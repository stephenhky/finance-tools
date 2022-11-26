
import json
import logging
import os

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

    # load data
    querystring = query.get('querystring', None)
    maxedits = query.get('max_edit_distance_considered', 1)
    alpha = query.get('alpha', None)
    try:
        assert isinstance(alpha, float)
    except AssertionError:
        return {
            'isBase64Encoded': False,
            'statusCode': 400,
            'body': 'alpha has to be float'
        }
    gamma = query.get('gamma', None)
    try:
        assert isinstance(gamma, float)
    except AssertionError:
        return {
            'isBase64Encoded': False,
            'statusCode': 400,
            'body': 'gamma has to be float'
        }
    topn = query.get(topn, 10)
    try:
        assert isinstance(topn, int)
    except AssertionError:
        return {
            'isBase64Encoded': False,
            'statusCode': 400,
            'body': 'topn has to be int'
        }

    # copy model files from S3
    os.makedirs([os.path.join('/', 'tmp', modeldir)])
    s3_client = boto3.client('s3')
    for model_filename in model_filenames:
        s3_client.download_file(
            s3_bucket,
            modeldir+'/'+model_filename,
            os.path.join('/', 'tmp', modeldir, model_filename)
        )

    # load the model
    extractor = SymbolMultinomialNaiveBayesExtractor.load_model(os.path.join('/', 'tmp', modeldir))
    if alpha is not None and alpha != extractor.alpha:
        extractor.alpha = alpha
    if gamma is not None and gamma != extractor.gamma:
        extractor.gamma = gamma

    # predictions
    ans = extractor.predict_proba(querystring, max_edit_distance_considered=maxedits)
    returned_results = [
        {symbol: proba for symbol, proba in sorted(ans.items(), key=lambda item: item[1], reverse=True)[:topn]}
    ]
    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        'body': json.dumps({
            'queryresults': returned_results
        })
    }
    return req_res

