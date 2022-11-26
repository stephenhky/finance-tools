
import json
import logging
import os
from time import time

import boto3

from mnbutils import SymbolMultinomialNaiveBayesExtractor


model_filenames = [
    'hyperparameters.json',
    'feature2idx.json',
    'symbols.json',
    'symbols_weight_info.json',
    'multinomialnb.joblib'
]

s3_bucket = os.environ.get('BUCKET')
modeldir = os.environ.get('MODELDIR')


def lambda_handler(event, context):
    # getting query
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])
    starttime = time()

    # load data
    querystring = query.get('querystring', None)
    maxedits = query.get('max_edit_distance_considered', 1)
    alpha = query.get('alpha', None)
    if alpha is not None:
        try:
            assert isinstance(alpha, float)
        except AssertionError:
            return {
                'isBase64Encoded': False,
                'statusCode': 400,
                'body': 'alpha has to be float'
            }
    gamma = query.get('gamma', None)
    if gamma is not None:
        try:
            assert isinstance(gamma, float)
        except AssertionError:
            return {
                'isBase64Encoded': False,
                'statusCode': 400,
                'body': 'gamma has to be float'
            }
    topn = query.get('topn', 10)
    if topn is not None:
        try:
            assert isinstance(topn, int)
        except AssertionError:
            return {
                'isBase64Encoded': False,
                'statusCode': 400,
                'body': 'topn has to be int'
            }

    # copy model files from S3
    logging.info('Copying model')
    print('Copying model')
    modelload_starttime = time()
    os.makedirs(os.path.join('/', 'tmp', modeldir))
    s3_client = boto3.client('s3')
    for model_filename in model_filenames:
        s3_client.download_file(
            s3_bucket,
            modeldir+'/'+model_filename,
            os.path.join('/', 'tmp', modeldir, model_filename)
        )
    modelload_endtime = time()
    logging.info('time elapsed: {:.3f} sec'.format(modelload_endtime-modelload_starttime))
    print('time elapsed: {:.3f} sec'.format(modelload_endtime - modelload_starttime))

    # load the model
    logging.info('Initializing the model')
    print('Initializing the model')
    extractor = SymbolMultinomialNaiveBayesExtractor.load_model(os.path.join('/', 'tmp', modeldir))
    if alpha is not None and alpha != extractor.alpha:
        extractor.alpha = alpha
    if gamma is not None and gamma != extractor.gamma:
        extractor.gamma = gamma

    # predictions
    logging.info('Searching: {}'.format(querystring))
    print('Searching: {}'.format(querystring))
    ans = extractor.predict_proba(querystring, max_edit_distance_considered=maxedits)
    returned_results = [
        {symbol: proba for symbol, proba in sorted(ans.items(), key=lambda item: item[1], reverse=True)[:topn]}
    ]
    endtime = time()
    logging.info('total time elapsed: {:.3f} sec'.format(endtime-starttime))
    print('total time elapsed: {:.3f} sec'.format(endtime - starttime))
    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        'body': json.dumps({
            'queryresults': returned_results
        })
    }
    return req_res

