
import logging
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from finsim.data import get_yahoofinance_data
from lppl.fit import LPPLModel


def lambda_handler(event, context):
    # getting info
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])

    # getting user inputs
    symbol = query.get('symbol', '^GSPC')
    startdate = query.get('startdate', (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d'))
    enddate = query.get('enddate', datetime.today().strftime('%Y-%m-%d'))

    # getting data
    logging.info('Getting symbol {} data'.format(symbol))
    symdf = get_yahoofinance_data(symbol, startdate, enddate)

    # fitting
    logging.info('Model fitting')
    fitted_lppl_model = LPPLModel()
    fitted_lppl_model.fit(symdf['TimeStamp'].map(lambda ts: ts.timestamp()), symdf['Close'])

    # gathering output info
    model_parameters = fitted_lppl_model.dump_model_parameters()
    output_dict = {
        'symbol': symbol,
        'startdate': startdate,
        'enddate': enddate,
        'estimated_crash_date': pd.Timestamp.fromtimestamp(model_parameters['tc']).strftime('%Y-%m-%d'),
        'estimated_crash_time': str(pd.Timestamp.fromtimestamp(model_parameters['tc'])),
        'model_param': model_parameters
    }

    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        'body': json.dumps(output_dict)
    }

    return req_res
