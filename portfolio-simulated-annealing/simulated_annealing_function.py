
import logging
import json
from datetime import datetime
import time
from functools import partial

import numpy as np
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
from finsim.data.preader import get_yahoofinance_data
import boto3
from portfolio_annealing import simulated_annealing, rewards, init_dynport

# Note: decoupling the modeling code and the Lambda function code that calls it
#       the file portfolio_annealing.py is an EXACT COPY of the model training code
#       in another repository

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
    lambda3 = query.get('lambda3', 0.0)
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
    logging.info('lambda3: {}'.format(lambda3))
    logging.info('indexsymbol: {}'.format(indexsymbol))

    # initializing the porfolio
    try:
        dynport = init_dynport(maxval, symbols, startdate, enddate, cacheddir=cacheddir)
    except ValueError:
        return {
            'statusCode': 400,
            'body': json.dumps('Too many symbols (or maximum portfolio value too small). Value ({}) > maxval ({})'.format(current_val, maxval))
        }

    # simulated annealing
    starttime = time.time()
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
        factor=decfactor,
        nbsteps=nbsteps,
        temperaturechangestep=temperaturechange_step,
        with_dividends=True
    )
    endtime = time.time()

    logging.info('final reward function: {}'.format(rewardfcn(optimized_dynport)))
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
