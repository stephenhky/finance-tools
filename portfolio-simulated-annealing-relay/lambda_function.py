
import json
import logging
from datetime import datetime

import numpy as np
import boto3
from finsim.data.preader import get_yahoofinance_data
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
from finsim.portfolio import DynamicPortfolioWithDividends


def lambda_handler(event, context):
    # get configuration
    eventbody = event['body']
    logging.info(eventbody)
    print(eventbody)
    query = eventbody['query']
    result = eventbody.get('result', {})
    remaining_nbsteps = query['remaining_nbsteps']
    current_temperature = query['current_temperature']
    runtime = query.get('runtime', 0.) + result.get('runtime', 0.)
    query['runtime'] = runtime
    query['init_temperature'] = current_temperature
    query['init_portfolio'] = result.get('portfolio', None)
    print('Number of remaining steps: {}'.format(remaining_nbsteps))

    lambda_client = boto3.client('lambda')
    if remaining_nbsteps > 0:
        # invoking simulated annealing Lambda
        lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:409029738116:function:portfolio-simulated-annealing',
            InvocationType='Event',
            Payload=json.dumps({'body': query})
        )
        return {
            'statusCode': 200,
            'body': 'Continue to run'
        }
    else:
        print('Finishing...')
        # parsing arguments
        indexsymbol = query.get('index', 'DJI')
        startdate = query['startdate']
        enddate = query['enddate']
        optimized_dynport = DynamicPortfolioWithDividends.load_from_dict(result['portfolio'])

        # making caching directory
        cacheddir = '/tmp/cacheddir'

        # call wrapper
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
            'r': float(r),
            'sigma': float(sigma),
            'downside_risk': float(downside_risk),
            'upside_risk': float(upside_risk),
            'beta': float(beta) if beta is not None else None,
            'portfolio': optimized_dynport.generate_dynamic_portfolio_dict(),
            'runtime': runtime
        }
        logging.info(result)
        print(result)

        lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:409029738116:function:portfolio-simulated-annealing-wrapper',
            InvocationType='Event',
            Payload=json.dumps({'body': json.dumps({'query': query, 'result': result})})
        )
        return {
            'statusCode': 200,
            'body': 'called wrapper'
        }
