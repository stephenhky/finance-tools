
import logging
import json
from time import sleep
from socket import timeout
from urllib.error import URLError
from math import sqrt

import numpy as np
from finsim.data import get_yahoofinance_data
from finsim.estimate.fit import fit_multivariate_BlackScholesMerton_model


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


def symbolcorr_handler(event, context):
    # getting info
    logging.info(event)
    print(event)
    logging.info(context)
    query = json.loads(event['body'])

    # getting user inputs
    symbol1 = query['symbol1']
    symbol2 = query['symbol2']
    startdate = query['startdate']
    enddate = query['enddate']

    # get symbols' prices
    print('grabbing stiuff')
    sym1df = waiting_get_yahoofinance_data(symbol1, startdate, enddate)
    sym2df = waiting_get_yahoofinance_data(symbol2, startdate, enddate)
    combined_df = sym1df[['TimeStamp', 'Close']].rename(columns={'Close': 'Close1'}).\
        merge(
            sym2df[['TimeStamp', 'Close']].rename(columns={'Close': 'Close2'}),
            on='TimeStamp', how='inner'
        )
    print(combined_df)
    rarray, covmat = fit_multivariate_BlackScholesMerton_model(
        combined_df['TimeStamp'].to_numpy(),
        np.array([
            combined_df['Close1'].to_numpy(),
            combined_df['Close2'].to_numpy()
        ])
    )

    results = {
        'symbol1': symbol1,
        'symbol2': symbol2,
        'r1': float(rarray[0]),
        'r2': float(rarray[1]),
        'std1': sqrt(covmat[0, 0]),
        'std2': sqrt(covmat[1, 1]),
        'cov': covmat[1, 0],
        'correlation': covmat[1, 0] / sqrt(covmat[0, 0] * covmat[1, 1])
    }

    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        'body': json.dumps(results)
    }

    return req_res
