
import argparse
from socket import timeout
import sys
import os
from urllib.error import URLError
import logging
import asyncio

from tqdm import tqdm
import numpy as np
import pandas as pd
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk
from finsim.data import get_yahoofinance_data


logginglevel_dict = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


def get_argparser():
    argparser = argparse.ArgumentParser(description='Extract and calculate interest rate and volatility of symbols.')
    argparser.add_argument('startdate', help='start date')
    argparser.add_argument('enddate', help='end date')
    argparser.add_argument('outputfile', help='path of output Excel file')
    argparser.add_argument('--localsymdf', default=os.path.dirname(__file__), help='location of allsymdf.h5')
    argparser.add_argument('--cacheddir', default=None, help='Cached directory (Default: None, meaning no caching)')
    argparser.add_argument('--logginglevel', default='info', help='Logging level (default: info, options: {})'.format(', '.join(logginglevel_dict.keys())))
    argparser.add_argument('--waittime', default=1, type=int, help='waiting time (sec) for time-out error (default: 1)')
    argparser.add_argument('--batchslice', default=50, type=int, help='number of symbols to be queried in parallel at the same time (default: 50)')
    return argparser


async def waiting_get_yahoofinance_data(symbol, startdate, enddate, cacheddir=None, waittime=1):
    done = False
    while not done:
        try:
            symdf = get_yahoofinance_data(symbol, startdate, enddate, cacheddir=cacheddir)
            done = True
        except ConnectionError:
            await asyncio.sleep(10)
        except URLError as error:
            if isinstance(error, timeout):
                await asyncio.sleep(waittime)
    return symdf


async def async_compute_symbol_info(symbol, startdate, enddate, cacheddir=None, waittime=1):
    symdf = await waiting_get_yahoofinance_data(symbol, startdate, enddate, cacheddir=cacheddir, waittime=waittime)
    if len(symdf) > 0:
        try:
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
                'startdate': symdf['TimeStamp'][0].date().strftime('%Y-%m-%d'),
                'enddate': symdf['TimeStamp'][-1].date().strftime('%Y-%m-%d'),
                'nbrecs': len(symdf.loc[~isrownull, :]),
                'description': list(concernedsymdf['description'][concernedsymdf['symbol'] == symbol])[0],
                'type': list(concernedsymdf['type'][concernedsymdf['symbol'] == symbol])[0]
            }
        except ZeroDivisionError:
            logging.warning('Division by zero error for symbol {}; skipping.'.format(symbol))
            estimations = {}
        except TypeError:
            logging.error('TypeError: symbol: {}; df: {}'.format(symbol, len(symdf)))
            estimations = {}
    else:
        estimations = {}

    return estimations


def sliced_estimate_all_symbols_from_yahoo(symbols, startdate, enddate, cacheddir=None, waittime=1, slicesize=50):
    all_estimations = {}
    nbsymbols = len(symbols)
    
    for startidx in tqdm(range(0, nbsymbols, slicesize)):
        loop = asyncio.get_event_loop()
        async_this_estimations = asyncio.gather(*[
            async_compute_symbol_info(symbol, startdate, enddate, cacheddir=cacheddir, waittime=waittime)
            for symbol in symbols[startidx:min(startidx+slicesize, nbsymbols)]
        ])
        completed_this_estimations = loop.run_until_complete(async_this_estimations)
        this_estimations = {
            item['symbol']: item
            for item in completed_this_estimations
            if len(item) > 0
        }
        all_estimations.update(this_estimations)
    
    return all_estimations


if __name__ == '__main__':
    argparser = get_argparser()
    args = argparser.parse_args()

    startdate = args.startdate
    enddate = args.enddate
    outputfile = args.outputfile
    symdfloc = os.path.join(args.localsymdf, 'allsymdf.h5')
    cacheddir = args.cacheddir
    logginglevel = args.logginglevel
    waittime = args.waittime
    slicesize = args.batchslice

    # logging level
    logging.basicConfig(level=logginglevel_dict[logginglevel])

    # read table
    # print('Symbol types: {}'.format(', '.join(symtypes)))
    print('Reading all symbols from {}'.format(symdfloc), file=sys.stderr)
    allsymdf = pd.read_hdf(symdfloc, 'fintable')

    # extracting
    # concernedsymdf = allsymdf[allsymdf['type'].isin(symtypes)]
    concernedsymdf = allsymdf
    # concernedsymdf = allsymdf[allsymdf['type']!='']

    # extracting Yahoo Finance Data
    all_estimations = sliced_estimate_all_symbols_from_yahoo(
            list(concernedsymdf['symbol']), 
            startdate, 
            enddate, 
            cacheddir=cacheddir,
            waittime=waittime,
            slicesize=slicesize
    )
 
    # Writing out
    print('Writing to {}'.format(outputfile))
    pd.DataFrame([
        {'symbol': key, **values} for key, values in all_estimations.items()
    ]).to_excel(outputfile)
    