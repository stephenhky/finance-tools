
import os
import json
import logging
from time import time
import shutil

import boto3


model_filenames = [
    'hyperparameters.json',
    'feature2idx.json',
    'symbols.json',
    'symbols_weight_info.json',
    'multinomialnb.joblib'
]

s3_bucket = os.environ.get('BUCKET')
efsmodeldir = os.environ.get('EFSMODELDIR')


def lambda_handler(event, context):
    # getting query
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])

    # getting paramemeters
    modelfoldername = query.get('modelfolder')

    # copy
    logging.info('Start copying')
    print('Start copying')
    starttime = time()
    if not os.path.isdir(os.path.join('/', 'mnt', 'efs', efsmodeldir)):
        os.makedirs(os.path.join('/', 'mnt', 'efs', efsmodeldir))
    s3_client = boto3.client('s3')
    for model_filename in model_filenames:
        s3_client.download_file(
            s3_bucket,
            modelfoldername+'/'+model_filename,
            os.path.join('/', 'tmp', efsmodeldir, model_filename)
        )
    movetime = time()
    logging.info('Copying to epheremel storage: {:.3f} sec'.format(movetime-starttime))
    for model_filename in model_filenames:
        shutil.move(
            os.path.join('/', 'tmp', efsmodeldir, model_filename),
            os.path.join('/', 'mnt', 'efs', efsmodeldir, model_filename)
        )
    endtime = time()
    logging.info('Time elapsed: {:.3f} sec'.format(endtime-starttime))
    print('Time elapsed: {:.3f} sec'.format(endtime - starttime))

    return {
        'statusCode': 200,
        'body': 'Copied'
    }