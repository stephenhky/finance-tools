
import logging
import os
import json

import boto3
import pandas as pd
from finsim.portfolio import DynamicPortfolioWithDividends
from plotnine import ggplot, aes, geom_line, theme, element_text, scale_x_datetime, labs
from mizani.breaks import date_breaks


lambda_client = boto3.client('lambda')


def generate_filename():
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:generate_filename',
        InvocationType='RequestResponse',
        Payload=json.dumps("")
    )
    response_payload = json.load(response['Payload'])
    return response_payload['body']


def construct_portfolio(portdict, startdate, enddate):
    if portdict.get('name', '') == 'DynamicPortfolio':
        return DynamicPortfolioWithDividends.load_from_dict(portdict)
    else:
        portfolio = DynamicPortfolioWithDividends(portdict, startdate)
        portfolio.move_cursor_to_date(enddate)
        return portfolio


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
    portfolio = construct_portfolio(query['components'], startdate, enddate)
    print(portfolio.symbols_nbshares)
    print(portfolio)
    worthdf = portfolio.get_portfolio_values_overtime(startdate, enddate)

    # convert dataframe for plotting using plotnine
    plotdf = pd.concat([
        pd.DataFrame({
            'TimeStamp': worthdf['TimeStamp'],
            'value': worthdf['stock_value'],
            'plot': 'stock price'}),
        pd.DataFrame({
            'TimeStamp': worthdf['TimeStamp'],
            'value': worthdf['value'],
            'plot': 'stock price+dividend'
        })
    ])

    # plot
    logging.info('plot')
    plt = (ggplot(plotdf)
           + geom_line(aes('TimeStamp', 'value', color='plot', group=1))
           + theme(axis_text_x=element_text(rotation=90, hjust=1))
           + scale_x_datetime(breaks=date_breaks('3 months'))
           + labs(x='Date', y='value')
           )
    plt.save(imgfilepath)

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
    event['data'] = worthdf.to_dict(orient='records')

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
