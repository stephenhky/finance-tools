
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


def call_mpt(query, call_wrapper):
    if not call_wrapper:
        response = lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:409029738116:function:finport',
            InvocationType='RequestResponse',
            Payload=json.dumps({'body': query})
        )
        response_payload = json.load(response['Payload'])
        return response_payload['body']
    else:
        lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:409029738116:function:finport',
            InvocationType='Event',
            Payload=json.dumps({'body': query})
        )
        return json.dumps(query)


def lambda_handler(event, context):
    query = json.loads(event['body'])
    call_wrapper = False
    query['estimating_startdate'] = query['startdate']
    query['estimating_enddate'] = query['enddate']
    query['riskcoef'] = query.get('riskcoef', 0.3)
    query['homogencoef'] = query.get('homogencoef', 0.1)
    query['V'] = query.get('V', 10.0)
    query['index'] = query.get('index', 'DJI')
    if 'email' in query:
        query['sender_email'] = 'finportlag@gmail.com'
        call_wrapper = True

    if call_wrapper and 'filebasename' not in query:
        query['filebasename'] = generate_filename()

    response = call_mpt(query, call_wrapper)

    req_res = {
        'isBase64Encoded': False,
        'statusCode': 200,
        # 'headers': {'Content-Type': 'application/json'},
        'body': response
    }
    return req_res
