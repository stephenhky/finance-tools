
import logging
import argparse
from datetime import datetime
from functools import partial

import numpy as np
from finsim.portfolio import DynamicPortfolioWithDividends, DynamicPortfolio
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
from finsim.data.preader import get_symbol_closing_price, get_yahoofinance_data


logging.basicConfig(level=logging.INFO)


def rewards(port, startdate, enddate, maxval, lambda1, lambda2, lambda3, cacheddir=None):
    df = port.get_portfolio_values_overtime(startdate, enddate, cacheddir=cacheddir)
    prices = np.array(df['value'])
    timestamps = np.array(df['TimeStamp'], dtype='datetime64[s]')

    r, sigma = fit_BlackScholesMerton_model(timestamps, prices)
    downside_risk = estimate_downside_risk(timestamps, prices, 0.)

    eachsymbol_prices = {
        symbol: nbshares*get_symbol_closing_price(symbol, enddate, cacheddir=cacheddir)
        for symbol, nbshares in port.symbols_nbshares.items()
    }
    totalprices = np.sum([val for val in eachsymbol_prices.values()])
    entropy = np.sum([
        val * (np.log(totalprices/val)) if val > 0 else 0.
        for val in eachsymbol_prices.values()
    ])
    entropy /= totalprices

    reward = r \
             - lambda1*sigma \
             + lambda2 * entropy / np.log(len(eachsymbol_prices)) \
             - lambda3 * downside_risk \
             + totalprices / maxval
    return reward


def randomly_rebalance_portfolio(orig_dynport, maxvalue, with_dividends=True):
    dynport_class = DynamicPortfolioWithDividends if with_dividends else DynamicPortfolio
    assert isinstance(orig_dynport, DynamicPortfolio)
    if not with_dividends:
        assert not isinstance(orig_dynport, DynamicPortfolioWithDividends)
    assert len(orig_dynport.timeseries) == 1

    olddynportdict = orig_dynport.generate_dynamic_portfolio_dict()
    startdate = orig_dynport.timeseries[0]['date']
    currentdate = orig_dynport.current_date
    cacheddir = orig_dynport.cacheddir
    dynport = dynport_class(orig_dynport.symbols_nbshares, startdate, cacheddir=cacheddir)

    buy_stocks = {}
    sell_stocks = {}

    rndnum = np.random.uniform()
    if rndnum < 0.33333:
        symbol_to_buy = np.random.choice(list(dynport.symbols_nbshares.keys()))
        buy_stocks[symbol_to_buy] = 1
    elif rndnum < 0.66667:
        symbol_to_sell = np.random.choice(list(dynport.symbols_nbshares.keys()))
        if dynport.symbols_nbshares[symbol_to_sell] >= 1:
            sell_stocks[symbol_to_sell] = 1
    else:
        symbols_to_exchange = np.random.choice(list(dynport.symbols_nbshares.keys()), 2)
        symbol_to_sell, symbol_to_buy = symbols_to_exchange
        selling_symbol_price = get_symbol_closing_price(symbol_to_sell, currentdate, cacheddir=cacheddir)
        buying_symbol_price = get_symbol_closing_price(symbol_to_buy, currentdate, cacheddir=cacheddir)

        if 0.5 < selling_symbol_price / buying_symbol_price < 2:
            sell_stocks[symbol_to_sell] = 1
            buy_stocks[symbol_to_buy] = 1
        elif selling_symbol_price / buying_symbol_price <= 0.5:
            sell_stocks[symbol_to_sell] = 1
            buy_stocks[symbol_to_buy] = selling_symbol_price / buying_symbol_price
        elif selling_symbol_price / buying_symbol_price >= 2:
            sell_stocks[symbol_to_sell] = 1
            buy_stocks[symbol_to_buy] = int(selling_symbol_price / buying_symbol_price)
        else:
            pass

    dynport.trade(currentdate, buy_stocks=buy_stocks, sell_stocks=sell_stocks)

    for symbol in sell_stocks:
        if dynport.symbols_nbshares[symbol] <= 0:
            return orig_dynport

    value = dynport.get_portfolio_value(currentdate)
    if value > maxvalue:
        dynport = dynport_class.load_from_dict(olddynportdict, cacheddir=cacheddir)
        dynport.move_cursor_to_date(currentdate)
    else:
        dynport = dynport_class(dynport.symbols_nbshares, startdate, cacheddir=cacheddir)
        dynport.move_cursor_to_date(currentdate)
    return dynport


def simulated_annealing(
        dynport,
        rewardfcn,
        maxval,
        initT=1000,
        factor=0.75,
        nbsteps=10000,
        temperaturechangestep=100,
        with_dividends=True
):
    olddynport = dynport
    olddynport_reward = rewardfcn(olddynport)
    temperature = initT
    for step in range(nbsteps):
        if step % temperaturechangestep == 0 and step > 0:
            temperature *= factor
            logging.info('Step {}'.format(step))
            logging.info('Temperature: {}'.format(temperature))
            logging.info('Reward: {}'.format(olddynport_reward))
            logging.info(olddynport.symbols_nbshares)

        newdynport = randomly_rebalance_portfolio(olddynport, maxval, with_dividends=with_dividends)

        if olddynport == newdynport:
            continue

        newdynport_reward = rewardfcn(newdynport)

        if newdynport_reward <= olddynport_reward:
            continue
        else:
            rndnum = np.random.uniform()
            if rndnum < np.exp((newdynport_reward - olddynport_reward) / temperaturechangestep):
                olddynport = newdynport
                olddynport_reward = newdynport_reward

    return olddynport


def get_argparser():
    argparser = argparse.ArgumentParser(description='Compute the optimized financial portfolio using simulated annealing.')
    argparser.add_argument('startdate', help='start date')
    argparser.add_argument('enddate', help='end date')
    argparser.add_argument('maxval', type=float, help='maximum value of the portfolio')
    argparser.add_argument('--symbols', nargs='+', help='symbols to be considered')
    argparser.add_argument('--nbsteps', default=10000, type=int, help='number of steps (default: 10000)')
    argparser.add_argument('--initT', default=1000, type=float, help='initial temperature (default: 1000)')
    argparser.add_argument('--changestep', default=100, type=int, help='change of temperature after a certain step (default: 100)')
    argparser.add_argument('--nodividends', default=False, action='store_true', help='no dividends')
    argparser.add_argument('--cacheddir', default=None, help='cached directory for symbols')
    argparser.add_argument('--lambda1', default=0.3, type=float, help='risk tolerance (default: 0.3)')
    argparser.add_argument('--lambda2', default=0.01, type=float, help='uniformity constant (default: 0.01)')
    argparser.add_argument('--lambda3', default=0.0, type=float, help='downside risk tolerance (default: 0.0)')
    argparser.add_argument('--index', default='DJI', help='index to calculate beta (default: DJI)')
    return argparser


if __name__ == '__main__':
    # parsing argument
    args = get_argparser().parse_args()
    startdate = args.startdate
    enddate = args.enddate
    maxval = args.maxval
    symbols = args.symbols
    nbsteps = args.nbsteps
    init_temperature = args.initT
    temperaturechange_step = args.changestep
    with_dividends = not args.nodividends
    cacheddir = args.cacheddir
    lambda1 = args.lambda1
    lambda2 = args.lambda2
    lambda3 = args.lambda3
    indexsymbol = args.index

    logging.info('Portfolio Optimization Using Simulated Annealing')
    logging.info('Symbols: {}'.format(', '.join(symbols)))
    logging.info('Start date: {}'.format(startdate))
    logging.info('End date: {}'.format(enddate))
    logging.info('Maximum value of the portfolio: {}'.format(maxval))
    logging.info('Number of steps: {}'.format(nbsteps))
    logging.info('Initial temperature: {} (decreased every {} steps)'.format(init_temperature, temperaturechange_step))
    logging.info('Consider dividends? {}'.format(with_dividends))
    logging.info('Cached directory: {}'.format(cacheddir))
    logging.info('lambda1: {}'.format(lambda1))
    logging.info('lambda2: {}'.format(lambda2))
    logging.info('indexsymbol: {}'.format(indexsymbol))

    # initializing the porfolio
    dynport = DynamicPortfolioWithDividends({symbol: 1 for symbol in symbols}, startdate, cacheddir=cacheddir)
    dynport.move_cursor_to_date(enddate)
    current_val = dynport.get_portfolio_value(enddate)
    if current_val > maxval:
        raise ValueError('Too many symbols (or maximum portfolio value too small). Value ({}) > maxval ({})'.format(current_val, maxval))

    # simulated annealing
    rewardfcn = partial(rewards,
                        startdate=startdate,
                        enddate=enddate,
                        maxval=maxval,
                        lambda1=lambda1,
                        lambda2=lambda2,
                        lambda3=lambda3,
                        cacheddir=cacheddir)
    optimized_dynport = simulated_annealing(
        dynport,
        rewardfcn,
        maxval,
        initT=init_temperature,
        factor=0.75,
        nbsteps=nbsteps,
        temperaturechangestep=temperaturechange_step,
        with_dividends=True
    )

    # display results
    print('Optimized Portfolio ({} dividends)'.format('with' if with_dividends else 'without'))
    for symbol, nbshares in optimized_dynport.symbols_nbshares.items():
        print('{}: {}'.format(symbol, nbshares))

    # further imformation
    print('reward function: {}'.format(rewardfcn(optimized_dynport)))
    df = optimized_dynport.get_portfolio_values_overtime(startdate, enddate, cacheddir=cacheddir)
    # df['TimeStamp'] = df['TimeStamp'].map(lambda item: datetime.strftime(item, '%Y-%m-%d'))
    indexdf = get_yahoofinance_data(indexsymbol, startdate, enddate, cacheddir=cacheddir)
    indexdf['TimeStamp'] = indexdf['TimeStamp'].map(lambda item: datetime.strftime(item, '%Y-%m-%d'))
    indexdf.index = list(indexdf['TimeStamp'])
    df = df.join(indexdf, on='TimeStamp', how='left', rsuffix='2')
    df['Close'] = df['Close'].ffill()
    timestamps = np.array(df['TimeStamp'], dtype='datetime64[s]')
    prices = np.array(df['value'])
    r, sigma = fit_BlackScholesMerton_model(timestamps, prices)
    print('Yield: {}'.format(r))
    print('Volatility: {}'.format(sigma))
    downside_risk = estimate_downside_risk(timestamps, prices, 0.)
    print('Downside risk: {}'.format(downside_risk))
    upside_risk = estimate_upside_risk(timestamps, prices, 0.)
    print('Upside risk: {}'.format(upside_risk))
    beta = estimate_beta(timestamps, prices, np.array(df['Close']))
    print('Beta (relative to {}): {}'.format(indexsymbol, beta))
