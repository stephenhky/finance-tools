
import json
import boto3


lambda_client = boto3.client('lambda')


def run_simulated_annealing(parameters):
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:portfolio-simulated-annealing',
        InvocationType='RequestResponse',
        Payload=json.dumps({'body': parameters})
    )
    response_payload = json.load(response['Payload'])
    return response_payload


def overseer_handler(event, context):
    query = event['body']

    simulated_annealing_result = run_simulated_annealing(query)

    return {
        'statusCode': 200,
        'body': json.dumps(simulated_annealing_result)
    }

