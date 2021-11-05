
import logging
import json
import time
from datetime import datetime

import numpy as np
import boto3
from finsim.portfolio.create import get_optimized_portfolio_on_mpt_entropy_costfunction
from finsim.portfolio.dynamic import DynamicPortfolioWithDividends
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
from finsim.data.preader import get_yahoofinance_data


def portfolio_handler(event, context):
    # getting query
    query = event['body']
    print(query)

    # getting parameter
    rf = query['rf']
    symbols = query['symbols']
    totalworth = query['totalworth']
    presetdate = query['presetdate']
    estimating_startdate = query['estimating_startdate']
    estimating_enddate = query['estimating_enddate']
    riskcoef = query.get('riskcoef', 0.3)
    homogencoef = query.get('homogencoef', 0.1)
    V = query.get('V', 10.0)
    index = query.get('index', 'DJI')
    include_dividends = query['include_dividends']
    call_wrapper = False
    if 'email' in query:
        assert 'sender_email' in query
        assert 'filebasename' in query
        call_wrapper = True
    print('call wrapper? {}'.format(call_wrapper))
    query['riskcoef'] = riskcoef
    query['homogencoef'] = homogencoef
    print('Including Dividends: {}'.format(include_dividends))

    logging.info('Portfolio Optimization Using Modern Portfolio Theory (MPT)')
    logging.info('Symbols: {}'.format(', '.join(symbols)))
    logging.info('Total worth: {:.2f}'.format(totalworth))
    logging.info('Date: {}'.format(presetdate))
    logging.info('Estimating start date: {}'.format(estimating_startdate))
    logging.info('Estimating end date: {}'.format(estimating_enddate))
    logging.info('Risk coefficient: {}'.format(riskcoef))
    logging.info('Homogeneity coefficient: {}'.format(homogencoef))
    logging.info('V: {}'.format(V))
    logging.info('Including Dividends: {}'.format(include_dividends))

    # Optimization
    starttime = time.time()
    optimized_portfolio = get_optimized_portfolio_on_mpt_entropy_costfunction(
        rf,
        symbols,
        totalworth,
        presetdate,
        estimating_startdate,
        estimating_enddate,
        riskcoef,
        homogencoef,
        V=V,
        include_dividends=include_dividends
    )
    endtime = time.time()

    portfolio_summary = optimized_portfolio.portfolio_summary
    corr = portfolio_summary['correlation']
    portfolio_summary['correlation'] = [
        [
            corr[i, j] for j in range(corr.shape[1])
        ]
        for i in range(corr.shape[0])
    ]
    event['portfolio'] = portfolio_summary
    event['symbols_nbshares'] = optimized_portfolio.symbols_nbshares
    event['runtime'] = endtime - starttime

    # calculate dynamic portfolio
    dynport = DynamicPortfolioWithDividends(optimized_portfolio.symbols_nbshares, estimating_startdate)
    df = dynport.get_portfolio_values_overtime(estimating_startdate, estimating_enddate)
    indexdf = get_yahoofinance_data(index, estimating_startdate, estimating_enddate)
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
    event['estimates'] = {
        'r': float(r),
        'sigma': float(sigma),
        'downside_risk': float(downside_risk),
        'upside_risk': float(upside_risk),
        'beta': float(beta) if beta is not None else None
    }


    if call_wrapper:
        print('Sending e-mail')
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:409029738116:function:financial-portfolio-mpt-wrapper',
            InvocationType='Event',
            Payload=json.dumps({
                'body': json.dumps({
                    'query': query,
                    'result': {
                        'portfolio': event['portfolio'],
                        'symbols_nbshares': event['symbols_nbshares'],
                        'runtime': event['runtime']
                    },
                    'estimates': {
                        'r': float(r),
                        'sigma': float(sigma),
                        'downside_risk': float(downside_risk),
                        'upside_risk': float(upside_risk),
                        'beta': float(beta) if beta is not None else None
                    }
                })
            })
        )

    # reference of a lambda output to API gateway: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-output-format
    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        # 'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(event)
    }
    return req_res
