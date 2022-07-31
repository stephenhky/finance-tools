
import logging
import json
import os

import boto3
import pandas as pd


lambda_client = boto3.client('lambda')


def generate_filename():
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:generate_filename',
        InvocationType='RequestResponse',
        Payload=json.dumps("")
    )
    response_payload = json.load(response['Payload'])
    return response_payload['body']


def lambda_handler(event, context):
    # getting config
    config = json.load(open('config.json', 'r'))

    # getting query
    logging.info(event)
    logging.info(context)
    event = json.loads(event['body'])
    dfdict = event['dataframe']
    filebasename = event.get('filebasename')

    df = pd.DataFrame(dfdict)

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
