
import logging
import json

from finsim.portfolio.create import get_optimized_portfolio_on_mpt_entropy_costfunction


def portfolio_handler(event, context):
    # getting query
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])
    # query = event['body']

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

    portfolio_summary = optimized_portfolio.portfolio_summary
    corr = portfolio_summary['correlation']
    portfolio_summary['correlation'] = [
        [
            corr[i, j] for j in range(corr.shape[1])
        ]
        for i in range(corr.shape[0])
    ]
    event['portfolio'] = portfolio_summary

    # reference of a lambda output to API gateway: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-output-format
    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        # 'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(event)
    }
    return req_res
