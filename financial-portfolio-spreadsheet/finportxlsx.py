
import logging
import os
import json
import random
from datetime import datetime

import boto3
import pandas as pd


def generate_filename():
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20))
    timestr = datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M%SUTC%z')
    return '{}_{}'.format(timestr, name)


def spreadsheet_handler(event, context):
    # getting config
    config = json.load(open('config.json', 'r'))

    # getting query
    logging.info(event)
    logging.info(context)
    portfolio = json.loads(event['body'])
    filebasename = portfolio.get('filebasename')

    # generate pandas dataframe
    df = pd.DataFrame(portfolio['components'])[['symbol', 'yield', 'volatility', 'weight', 'nbshares']]
    df = df.sort_values(by=['weight'], ascending=False)

    # generate Excel file
    filename = generate_filename() if filebasename is None else filebasename
    filename = filename+'.xlsx'
    filepath = os.path.join('/', 'tmp', filename)
    df.to_excel(filepath, index=False)

    # copy to S3
    s3_bucket = config['bucket']
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(filepath, s3_bucket, filename)

    event['filename'] = filename
    event['url'] = 'https://{}.s3.amazonaws.com/{}'.format(s3_bucket, filename)
    event['response'] = response

    # reference of a lambda output to API gateway: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-output-format
    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        'body': json.dumps(event)
    }
    return req_res


# References:
# Writing files to S3: https://stackoverflow.com/questions/62522227/create-new-file-in-s3-using-aws-lambda-function
#                      https://stackoverflow.com/questions/44233777/use-aws-lambda-to-upload-video-into-s3-with-download-url
