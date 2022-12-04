
import json
import logging
import os
from time import time

from mnbutils import SymbolMultinomialNaiveBayesExtractor


s3_bucket = os.environ.get('BUCKET')
efsmodeldir = os.environ.get('EFSMODELDIR')


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

    # load the model
    logging.info('Initializing the model')
    print('Initializing the model')
    modelloadstarttime = time()
    extractor = SymbolMultinomialNaiveBayesExtractor.load_model(os.path.join('/', 'mnt', 'efs', efsmodeldir))
    if alpha is not None and alpha != extractor.alpha:
        extractor.alpha = alpha
    if gamma is not None and gamma != extractor.gamma:
        extractor.gamma = gamma
    modelloadendtime = time()
    logging.info('Model load time: {:.3f} sec'.format(modelloadendtime-modelloadstarttime))
    print('Model load time: {:.3f} sec'.format(modelloadendtime - modelloadstarttime))

    # predictions
    logging.info('Searching: {}'.format(querystring))
    print('Searching: {}'.format(querystring))
    ans = extractor.predict_proba(querystring, max_edit_distance_considered=maxedits)
    returned_results = [
        {'symbol': symbol, 'prob': proba}
        for symbol, proba in sorted(ans.items(), key=lambda item: item[1], reverse=True)[:topn]
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

