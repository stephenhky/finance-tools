
import logging
import os
import json
import random
from datetime import datetime

import boto3
from finsim.portfolio import Portfolio
from matplotlib import pyplot as plt


def generate_filename():
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20))
    timestr = datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M%SUTC%z')
    return '{}_{}'.format(timestr, name)


def plot_handler(event, context):
    # getting config
    config = json.load(open('config.json', 'r'))

    # getting query
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])
    startdate = query['startdate']
    enddate = query['enddate']
    filebasename = query.get('filebasename')
    filename = generate_filename() if filebasename is None else filebasename
    filename = filename + '.png'
    filepath = os.path.join('/', 'tmp', filename)

    # generate pandas dataframe
    logging.info('Calculating worth over time')
    print('Calculating worth over time')
    portfolio = Portfolio(query['components'])
    worthdf = portfolio.get_portfolio_values_overtime(startdate, enddate)

    # plot
    logging.info('plot')
    print('plot')
    f = plt.figure()
    f.set_figwidth(10)
    f.set_figheight(8)
    plt.xticks(rotation=90)
    plt.xlabel('Date')
    plt.ylabel('Portfolio Value')
    plt.plot(worthdf['TimeStamp'], worthdf['value'])
    plt.savefig(filepath)

    # copy to S3
    logging.info('copying to S3')
    print('copying to S3')
    s3_bucket = config['bucket']
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(filepath, s3_bucket, filename)

    event['filename'] = filename
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
