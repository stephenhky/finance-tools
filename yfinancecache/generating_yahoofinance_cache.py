
import argparse
import os
import sys
import time
import logging

import pandas as pd
import finsim.data

logginglevel_dict = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


def get_argparser():
    argparser = argparse.ArgumentParser(description='Caching Yahoo finance data to disk.')
    argparser.add_argument('startdate', type=str, help='start date')
    argparser.add_argument('enddate', type=str, help='end data')
    argparser.add_argument('cacheddir', type=str, help='cached directory')
    argparser.add_argument('--slicebatch', type=int, default=50, help='batch size for each online retrieval')
    argparser.add_argument('--localsymdf', default=os.path.dirname(__file__), help='location of allsymdf.h5')
    argparser.add_argument('--waittime', default=5, type=int, help='wait time when connection fails')
    argparser.add_argument('--logginglevel', default='info', help='Logging level (default: info, options: {})'.format(
        ', '.join(logginglevel_dict.keys())))
    return argparser


if __name__ == '__main__':
    # argument parsing
    argparser = get_argparser()
    args = argparser.parse_args()

    startdate = args.startdate
    enddate = args.enddate
    cacheddir = args.cacheddir
    slicebatch = args.slicebatch
    symdfloc = os.path.join(args.localsymdf, 'allsymdf.h5')
    waittime = args.waittime
    logginglevel = args.logginglevel

    # logging level
    logging.basicConfig(level=logginglevel_dict[logginglevel])

    # Read symbols
    print('Reading all symbols from {}'.format(symdfloc), file=sys.stderr)
    allsymdf = pd.read_hdf(symdfloc, 'fintable')
    concernedsymdf = allsymdf[allsymdf['type'] != '']

    # Generating cache
    starttime = time.time()
    finsim.data.preader.generating_cached_yahoofinance_data(
        list(concernedsymdf['symbol']),
        startdate,
        enddate,
        cacheddir,
        slicebatch=slicebatch,
        waittime=waittime,
        threads=False
    )
    endtime = time.time()
    print('Time elapsed: {} sec'.format(endtime - starttime))
