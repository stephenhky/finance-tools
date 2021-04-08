
import logging
import json
import time

import boto3
from finsim.portfolio.create import get_optimized_portfolio_on_mpt_entropy_costfunction


def portfolio_handler(event, context):
    # getting query
    query = event['body']

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
    call_wrapper = False
    if 'email' in query:
        assert 'sender_email' in query
        assert 'filebasename' in query
        call_wrapper = True
    print('call wrapper? {}'.format(call_wrapper))
    query['riskcoef'] = riskcoef
    query['homogencoef'] = homogencoef

    logging.info('Portfolio Optimization Using Modern Portfolio Theory (MPT)')
    logging.info('Symbols: {}'.format(', '.join(symbols)))
    logging.info('Total worth: {:.2f}'.format(totalworth))
    logging.info('Date: {}'.format(presetdate))
    logging.info('Estimating start date: {}'.format(estimating_startdate))
    logging.info('Estimating end date: {}'.format(estimating_enddate))
    logging.info('Risk coefficient: {}'.format(riskcoef))
    logging.info('Homogeneity coefficient: {}'.format(homogencoef))
    logging.info('V: {}'.format(V))

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
        lazy=False
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
