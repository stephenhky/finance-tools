
import logging
import json
from time import sleep
from socket import timeout
from urllib.error import URLError

import numpy as np
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk
from finsim.data import get_yahoofinance_data


def waiting_get_yahoofinance_data(symbol, startdate, enddate, waittime=1):
    done = False
    while not done:
        try:
            symdf = get_yahoofinance_data(symbol, startdate, enddate)
            done = True
        except ConnectionError:
            sleep(10)
        except URLError as error:
            if isinstance(error, timeout):
                sleep(waittime)
    return symdf


def symbol_handler(event, context):
    # getting info
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])

    # getting user inputs
    symbol = query['symbol']
    startdate = query['startdate']
    enddate = query['enddate']
    waittime = query.get('waittime', 1)

    # getting stock data
    symdf = waiting_get_yahoofinance_data(symbol, startdate, enddate, waittime=waittime)

    # estimation
    isrownull = symdf['Close'].isnull()
    r, sigma = fit_BlackScholesMerton_model(
        np.array(symdf.loc[~isrownull, 'TimeStamp']),
        np.array(symdf.loc[~isrownull, 'Close'])
    )
    downside_risk = estimate_downside_risk(
        np.array(symdf.loc[~isrownull, 'TimeStamp']),
        np.array(symdf.loc[~isrownull, 'Close']),
        0.0
    )
    upside_risk = estimate_upside_risk(
        np.array(symdf.loc[~isrownull, 'TimeStamp']),
        np.array(symdf.loc[~isrownull, 'Close']),
        0.0
    )
    estimations = {
        'symbol': symbol,
        'r': r,
        'vol': sigma,
        'downside_risk': downside_risk,
        'upside_risk': upside_risk,
        'data_startdate': symdf['TimeStamp'][0].date().strftime('%Y-%m-%d'),
        'data_enddate': symdf['TimeStamp'][-1].date().strftime('%Y-%m-%d'),
        'nbrecs': len(symdf.loc[~isrownull, :]),
    }

    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        # 'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(estimations)
    }

    return req_res
