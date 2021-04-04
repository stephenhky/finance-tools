
import json
import boto3


lambda_client = boto3.client('lambda')


def generate_filename():
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:generate_filename',
        InvocationType='RequestResponse',
        Payload=json.dumps("")
    )
    response_payload = json.load(response['Payload'])
    return response_payload['body']


def call_mpt(query):
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:finport',
        InvocationType='RequestResponse',
        Payload=json.dumps({'body': query})
    )
    response_payload = json.load(response['Payload'])
    return response_payload['body']


def lambda_handler(event, context):
    query = json.loads(event['body'])

    response = call_mpt(query)

    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        # 'headers': {'Content-Type': 'application/json'},
        'body': response
    }
    return req_res