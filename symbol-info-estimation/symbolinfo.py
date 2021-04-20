
import logging
import json
from time import sleep
from socket import timeout
from urllib.error import URLError

import numpy as np
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
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
    index = query.get('index', 'DJI')   # DJI index as the base.

    # getting stock data
    symdf = waiting_get_yahoofinance_data(symbol, startdate, enddate, waittime=waittime)

    # getting index
    indexdf = waiting_get_yahoofinance_data(index, startdate, enddate, waittime=waittime)

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
    try:
        beta = estimate_beta(
            np.array(symdf.loc[~isrownull, 'TimeStamp']),
            np.array(symdf.loc[~isrownull, 'Close']),
            np.array(indexdf.loc[~isrownull, 'Close']),
        )
    except:
        logging.warning('Index {} failed to be integrated.'.format(index))
        beta = None

    estimations = {
        'symbol': symbol,
        'r': float(r),
        'vol': float(sigma),
        'downside_risk': float(downside_risk),
        'upside_risk': float(upside_risk),
        'beta': float(beta) if beta is not None else None,
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
