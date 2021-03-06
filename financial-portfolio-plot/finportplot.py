
import logging
import os
import json
import random
from datetime import datetime

import boto3
# import pandas as pd
from finsim.portfolio import Portfolio, DynamicPortfolioWithDividends
from matplotlib import pyplot as plt


def generate_filename():
    name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=20))
    timestr = datetime.strftime(datetime.utcnow(), '%Y%m%d%H%M%SUTC%z')
    return '{}_{}'.format(timestr, name)


def construct_portfolio(portdict, startdate):
    if portdict.get('name', '') == 'DynamicPortfolio':
        return DynamicPortfolioWithDividends.load_from_dict(portdict)
    else:
        return DynamicPortfolioWithDividends({
            'date': startdate,
            'portfolio': Portfolio(portdict)
        })


def plot_handler(event, context):
    # getting config
    config = json.load(open('config.json', 'r'))

    # getting query
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])
    startdate = query['startdate']
    logging.info('start date: {}'.format(startdate))
    enddate = query['enddate']
    logging.info('end date: {}'.format(enddate))
    filebasename = query.get('filebasename')
    filename = generate_filename() if filebasename is None else filebasename
    imgfilename = filename + '.png'
    imgfilepath = os.path.join('/', 'tmp', imgfilename)
    xlsxfilename = filename + '.xlsx'
    xlsxfilepath = os.path.join('/', 'tmp', xlsxfilename)

    # generate pandas dataframe
    logging.info('Calculating worth over time')
    portfolio = construct_portfolio(query['components'], startdate)
    worthdf = portfolio.get_portfolio_values_overtime(startdate, enddate)
    # pd.set_option('display.max_rows', len(worthdf))
    # print(pd.DataFrame.from_records(portfolio.cashtimeseries))
    # print(worthdf)
    # pd.reset_option('display.max_rows')

    # plot
    logging.info('plot')
    f = plt.figure()
    f.set_figwidth(10)
    f.set_figheight(8)
    plt.xlabel('Date')
    plt.ylabel('Portfolio Value')
    stockline, = plt.plot(worthdf['TimeStamp'], worthdf['stock_value'], label='stock')
    totalline, = plt.plot(worthdf['TimeStamp'], worthdf['value'], label='stock+dividend')
    xticks, _ = plt.xticks(rotation=90)
    step = len(xticks) // 10
    plt.xticks(xticks[::step])
    plt.legend([stockline, totalline], ['stock', 'stock+dividend'])
    plt.savefig(imgfilepath)

    # making spreadsheet
    worthdf.to_excel(xlsxfilepath)

    # copy to S3
    logging.info('copying to S3')
    s3_bucket = config['bucket']
    s3_client = boto3.client('s3')
    imgresponse = s3_client.upload_file(imgfilepath, s3_bucket, imgfilename)
    xlsxresponse = s3_client.upload_file(xlsxfilepath, s3_bucket, xlsxfilename)

    event['plot'] = {
        'filename': imgfilename,
        'url': 'https://{}.s3.amazonaws.com/{}'.format(s3_bucket, imgfilename),
        'response': imgresponse
    }
    event['spreadsheet'] = {
        'filename': xlsxfilename,
        'url': 'https://{}.s3.amazonaws.com/{}'.format(s3_bucket, xlsxfilename),
        'response': xlsxresponse
    }

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
