
from datetime import datetime
import json
import os
import logging

from dotenv import load_dotenv
import boto3
import pandas as pd
from finsim.data.preader import get_yahoofinance_data
from finsim.tech.ma import get_movingaverage_price_data
from plotnine import ggplot, aes, geom_line, theme, element_text, scale_x_datetime, labs, ggtitle
from mizani.breaks import date_breaks


logging.basicConfig(level=logging.INFO)

load_dotenv()

lambda_client = boto3.client('lambda')


def generate_filename():
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:generate_filename',
        InvocationType='RequestResponse',
        Payload=json.dumps("")
    )
    response_payload = json.load(response['Payload'])
    return response_payload['body']


def get_optimal_daybreaks(startdate, enddate):
    timediff = datetime.strptime(enddate, '%Y-%m-%d') - datetime.strptime(startdate, '%Y-%m-%d')
    nbdaysdiff = timediff.days
    if nbdaysdiff > 730:
        return '1 year'
    elif nbdaysdiff > 365:
        return '3 months'
    elif nbdaysdiff > 30:
        return '1 month'
    else:
        return '1 day'


def plot_handler(event, context):
    # getting query
    s3_bucket = os.getenv('S3BUCKET')
    logging.info(event)
    logging.info(context)
    query = json.loads(event['body'])
    symbol = query['symbol']
    logging.info('symbol: {}'.format(symbol))
    startdate = query['startdate']
    logging.info('start date: {}'.format(startdate))
    enddate = query['enddate']
    logging.info('end date: {}'.format(enddate))
    dayswindow = query['dayswindow']
    logging.info('days window: {}'.format(dayswindow))
    if not isinstance(dayswindow, list):
        dayswindow = [dayswindow]
    filebasename = query.get('filebasename')
    filename = generate_filename() if filebasename is None else filebasename
    imgfilename = filename + '.png'
    imgfilepath = os.path.join('/', 'tmp', imgfilename)
    xlsxfilename = filename + '.xlsx'
    xlsxfilepath = os.path.join('/', 'tmp', xlsxfilename)
    title = query.get('title', symbol)

    # get data
    df = get_yahoofinance_data(symbol, startdate, enddate)
    madfs = {
        daywindow: get_movingaverage_price_data(symbol, startdate, enddate, daywindow)
        for daywindow in dayswindow
    }

    # convert dataframe for plotting using plotnine
    plotdf = pd.DataFrame({
        'TimeStamp': df['TimeStamp'],
        'value': df['Close'],
        'plot': 'price'
    })
    for daywindow, madf in madfs.items():
        plotdf = pd.concat([
            plotdf,
            pd.DataFrame({
                'TimeStamp': madf['TimeStamp'],
                'value': madf['MA'],
                'plot': '{}-day MA'.format(daywindow)
            })
        ])

    # plot
    logging.info('plot')
    plot_date_interval = get_optimal_daybreaks(startdate, enddate)
    plt = (ggplot(plotdf)
           + geom_line(aes('TimeStamp', 'value', color='plot'))
           + theme(axis_text_x=element_text(rotation=90, hjust=1))
           + scale_x_datetime(breaks=date_breaks("{} days".format(plot_date_interval)))
           + labs(x='Date', y='value')
           + ggtitle(title)
           )
    plt.save(imgfilepath)

    # making spreadsheet
    outputdf = df[['TimeStamp', 'Close']]
    outputdf = outputdf.rename(columns={'Close': 'Price'})
    for daywindow, madf in madfs.items():
        outputdf = pd.merge(outputdf, madf, on='TimeStamp')
        outputdf = outputdf.rename(columns={'MA': '{}-day Moving Average'.format(daywindow)})
    outputdf['TimeStamp'] = outputdf['TimeStamp'].map(lambda ts: ts.date().strftime('%Y-%m-%d'))
    outputdf = outputdf.rename(columns={'TimeStamp': 'Date'})
    outputdf.to_excel(xlsxfilepath, engine='openpyxl')

    # copy to S3
    logging.info('copying to S3')
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
    event['data'] = outputdf.to_dict(orient='records')

    # reference of a lambda output to API gateway: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-output-format
    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        'body': json.dumps(event)
    }
    return req_res
