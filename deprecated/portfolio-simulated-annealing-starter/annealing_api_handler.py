
import json
import logging

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


def send_email(sender_email, recipient_email, subject, html):
    lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:send_finport_email',
        InvocationType='Event',
        Payload=json.dumps({
            "sender": sender_email,
            "recipient": recipient_email,
            "subject": subject,
            "html": html
        })
    )


def lambda_handler(event, context):
    # parsing argument
    queryjson = event['body']
    query = json.loads(queryjson)
    startdate = query['startdate']
    enddate = query['enddate']
    maxval = query['maxval']
    symbols = query['symbols']
    nbsteps = query.get('nbsteps', 10000)
    each_lambda_nbsteps = query.get('each_lambda_nbsteps', nbsteps)
    init_temperature = query.get('init_temperature', 1000.)
    decfactor = query.get('decfactor', 0.75)
    temperaturechange_step = query.get('temperaturechange_step', 100)
    with_dividends = query.get('with_dividends', True)
    lambda1 = query.get('lambda1', 0.3)
    lambda2 = query.get('lambda2', 0.01)
    lambda3 = query.get('lambda3', 0.0)
    indexsymbol = query.get('index', 'DJI')
    user_email = query['email']
    filebasename = generate_filename()
    sender_email = 'finportlag@gmail.com'

    # check validity
    logging.info('{} % {} = {}'.format(
        each_lambda_nbsteps,
        temperaturechange_step,
        each_lambda_nbsteps % temperaturechange_step)
    )
    assert each_lambda_nbsteps % temperaturechange_step == 0
    assert nbsteps % each_lambda_nbsteps == 0

    # sending e-mail
    notification_email_body = open('notification_email.html', 'r').read().format(
        symbols=', '.join(sorted(symbols)),
        startdate=startdate,
        enddate=enddate,
        maxval=maxval,
        nbsteps=nbsteps,
        init_temperature=init_temperature,
        temperaturechange_step=temperaturechange_step,
        decfactor=decfactor,
        with_dividends='Yes' if with_dividends else 'No',
        lambda1=lambda1,
        lambda2=lambda2,
        lambda3=lambda3,
        indexsymbol=indexsymbol,
        filebasename=filebasename
    )

    send_email(sender_email, user_email, "Portfolio Optimization - Computation Started", notification_email_body)

    # invoking Lambda
    query['series_nbsteps'] = nbsteps
    query['series_init_temperature'] = init_temperature
    query['remaining_nbsteps'] = nbsteps
    query['current_temperature'] = init_temperature
    query['each_lambda_nbsteps'] = each_lambda_nbsteps

    query['nbsteps'] = each_lambda_nbsteps
    query['init_temperature'] = init_temperature
    query['decfactor'] = decfactor
    query['temperaturechange_step'] = temperaturechange_step
    query['with_dividends'] = with_dividends
    query['lambda1'] = lambda1
    query['lambda2'] = lambda2
    query['lambda3'] = lambda3
    query['index'] = indexsymbol

    query['filebasename'] = filebasename
    query['sender_email'] = sender_email

    lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:portfolio-simulated-annealing-relay',
        InvocationType='Event',
        Payload=json.dumps({'body': {'query': query}})
    )

    reply = query.copy()
    reply['email_body'] = notification_email_body

    return {
        'statusCode': 200,
        'body': json.dumps(reply)
    }
