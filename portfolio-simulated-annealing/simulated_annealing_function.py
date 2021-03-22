
import logging
import json
from datetime import datetime
import time

import numpy as np
from finsim.portfolio import DynamicPortfolioWithDividends, DynamicPortfolio
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
from finsim.data.preader import get_symbol_closing_price, get_yahoofinance_data
import boto3


logging.basicConfig(level=logging.INFO)


def rewards(port, startdate, enddate, maxval, lambda1, lambda2, cacheddir=None):
    df = port.get_portfolio_values_overtime(startdate, enddate, cacheddir=cacheddir)
    prices = np.array(df['value'])
    timestamps = np.array(df['TimeStamp'], dtype='datetime64[s]')

    r, sigma = fit_BlackScholesMerton_model(timestamps, prices)

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

    reward = r - lambda1*sigma + lambda2 * entropy / np.log(len(eachsymbol_prices)) + totalprices / maxval
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

    for symbol in sell_stocks:
        if dynport.symbols_nbshares[symbol] <= 0:
            return orig_dynport

    dynport.trade(currentdate, buy_stocks=buy_stocks, sell_stocks=sell_stocks)
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
        startdate,
        enddate,
        maxval,
        lambda1,
        lambda2,
        initT=1000,
        factor=0.75,
        nbsteps=10000,
        temperaturechangestep=100,
        cacheddir=None,
        with_dividends=True
):
    olddynport = dynport
    olddynport_reward = rewards(olddynport, startdate, enddate, maxval, lambda1, lambda2, cacheddir=cacheddir)
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

        newdynport_reward = rewards(newdynport, startdate, enddate, maxval, lambda1, lambda2, cacheddir=cacheddir)

        if newdynport_reward <= olddynport_reward:
            continue
        else:
            rndnum = np.random.uniform()
            if rndnum < np.exp((newdynport_reward - olddynport_reward) / temperaturechangestep):
                olddynport = newdynport
                olddynport_reward = newdynport_reward

    return olddynport


def simulated_annealing_handler(event, context):
    # parsing argument
    query = event['body']
    startdate = query['startdate']
    enddate = query['enddate']
    maxval = query['maxval']
    symbols = query['symbols']
    nbsteps = query.get('nbsteps', 10000)
    init_temperature = query.get('init_temperature', 1000.)
    decfactor = query.get('decfactor', 0.75)
    temperaturechange_step = query.get('temperaturechange_step', 100)
    with_dividends = query.get('with_dividends', True)
    lambda1 = query.get('lambda1', 0.3)
    lambda2 = query.get('lambda2', 0.01)
    indexsymbol = query.get('index', 'DJI')
    call_wrapper = False
    if 'email' in query:
        assert 'sender_email' in query
        assert 'filebasename' in query
        call_wrapper = True

    # making caching directory
    cacheddir = '/tmp/cacheddir'

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
        return {
            'statusCode': 400,
            'body': json.dumps('Too many symbols (or maximum portfolio value too small). Value ({}) > maxval ({})'.format(current_val, maxval))
        }

    # simulated annealing
    starttime = time.time()
    optimized_dynport = simulated_annealing(
        dynport,
        startdate,
        enddate,
        maxval,
        lambda1,
        lambda2,
        initT=init_temperature,
        factor=decfactor,
        nbsteps=nbsteps,
        temperaturechangestep=temperaturechange_step,
        cacheddir=cacheddir,
        with_dividends=True
    )
    endtime = time.time()

    logging.info('final reward function: {}'.format(rewards(optimized_dynport, startdate, enddate, maxval, lambda1, lambda2, cacheddir=cacheddir)))
    df = optimized_dynport.get_portfolio_values_overtime(startdate, enddate, cacheddir=cacheddir)
    indexdf = get_yahoofinance_data(indexsymbol, startdate, enddate, cacheddir=cacheddir)
    indexdf['TimeStamp'] = indexdf['TimeStamp'].map(lambda item: datetime.strftime(item, '%Y-%m-%d'))
    indexdf.index = list(indexdf['TimeStamp'])
    df = df.join(indexdf, on='TimeStamp', how='left', rsuffix='2')
    df['Close'] = df['Close'].ffill()
    timestamps = np.array(df['TimeStamp'], dtype='datetime64[s]')
    prices = np.array(df['value'])
    r, sigma = fit_BlackScholesMerton_model(timestamps, prices)
    downside_risk = estimate_downside_risk(timestamps, prices, 0.)
    upside_risk = estimate_upside_risk(timestamps, prices, 0.)
    beta = estimate_beta(timestamps, prices, np.array(df['Close']))

    result = {
        'r': r,
        'sigma': sigma,
        'downside_risk': downside_risk,
        'upside_risk': upside_risk,
        'beta': beta,
        'portfolio': optimized_dynport.generate_dynamic_portfolio_dict(),
        'runtime': endtime-starttime
    }

    if call_wrapper:
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:409029738116:function:portfolio-simulated-annealing-wrapper',
            InvocationType='Event',
            Payload=json.dumps({'body': json.dumps({'query': query, 'result': result})})
        )

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
