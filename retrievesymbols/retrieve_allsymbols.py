
import os
import argparse

import pandas as pd
from finsim.data.finnhub import FinnHubStockReader


def get_argparser():
    argparser = argparse.ArgumentParser(description='Grab all symbols from Finnhub.')
    argparser.add_argument('outputpath', help='Output path. (Extensions: ".h5", ".json", ".xlsx".)')
    argparser.add_argument('--tokenpath', default='./finnhub.tokens', help='Path of the Finnhub tokens.')
    return argparser


if __name__ == '__main__':
    argparser = get_argparser()
    args = argparser.parse_args()

    finnhub_token_filepath = args.tokenpath

    finnreader = FinnHubStockReader(open(finnhub_token_filepath, 'r').read().strip())

    allsym = finnreader.get_all_US_symbols()
    allsymdf = pd.DataFrame(allsym)

    extension = os.path.splitext(args.outputpath)[-1]
    if extension == '.h5':
        allsymdf.to_hdf(args.outputpath, 'fintable')
    elif extension == '.json':
        allsymdf.to_json(args.outputpath, orient='records')
    elif extension == '.xlsx':
        allsymdf.to_excel(args.outputpath)
    elif extension == '.csv':
        allsymdf.to_csv(args.outputpath)
    elif extension == '.pickle' or extension == '.pkl':
        allsymdf.to_pickle(args.outputpath)
    else:
        raise IOError('Extension {} not recognized.'.format(extension))
