
import os
import json
import logging
from time import time

import boto3
import botocore

model_filenames = [
    'feature_engineer_hyperparameters.json',
    'hyperparameters.json',
    'feature2idx.json',
    'symbols.json',
    'symbols_weight_info.json',
    'multinomialnb.joblib',
    'allsymdf.json'
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

    s3_client = boto3.client('s3', 'us-east-1', config=botocore.config.Config(s3={'addressing_style': 'path'}))

    logging.info('Copying to EFS directly...')
    print('Copying to EFS directly...')
    for model_filename in model_filenames:
        logging.info('Source: {}'.format(modelfoldername + '/' + model_filename))
        logging.info(' --> Destination: {}'.format(os.path.join('/', 'mnt', 'efs', efsmodeldir, model_filename)))
        print('Source: {}'.format(modelfoldername + '/' + model_filename))
        print(' --> Destination: {}'.format(os.path.join('/', 'mnt', 'efs', efsmodeldir, model_filename)))
        s3_client.download_file(
            s3_bucket,
            modelfoldername + '/' + model_filename,
            os.path.join('/', 'mnt', 'efs', efsmodeldir, model_filename)
        )

    endtime = time()
    logging.info('Time elapsed: {:.3f} sec'.format(endtime - starttime))
    print('Time elapsed: {:.3f} sec'.format(endtime - starttime))

    return {
        'statusCode': 200,
        'body': 'Copied'
    }

# Because EFS is connected to the Lambda function, the Lambda function has to be
# in a VPC. In order to connected to S3, you must create a Gateway endpoint for that VPC.
# See: https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html
