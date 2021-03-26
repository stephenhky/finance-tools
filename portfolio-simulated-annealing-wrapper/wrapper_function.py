
import os
import json
from operator import itemgetter
import logging
from math import exp

import boto3


lambda_client = boto3.client('lambda')


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


def convert_portfolio_to_table(portfolio_dict):
    html_string = '<table style="width:100%">'
    html_string += '<tr><th>Symbol</th><th>Number of Shares</th>'
    for symbol, nbshares in sorted(
        portfolio_dict['timeseries'][0]['portfolio'].items(),
        key=itemgetter(0)
    ):
        html_string += "<tr><th><a href='https://finance.yahoo.com/quote/{symbol:}/'>{symbol:}</a></th><th>{nbshares:}</th></tr>".format(symbol=symbol, nbshares=nbshares)

    html_string += '</table>'
    return html_string


def lambda_handler(event, context):
    # getting config
    config = json.load(open('config.json', 'r'))

    # parsing argument
    eventbody = json.loads(event['body'])
    logging.info(eventbody)
    query = eventbody['query']
    result = eventbody['result']
    startdate = query['startdate']
    enddate = query['enddate']
    maxval = query['maxval']
    symbols = query['symbols']
    nbsteps = query['nbsteps']
    init_temperature = query['init_temperature']
    decfactor = query['decfactor']
    temperaturechange_step = query['temperaturechange_step']
    with_dividends = query['with_dividends']
    lambda1 = query['lambda1']
    lambda2 = query['lambda2']
    indexsymbol = query['index']
    user_email = query['email']
    filebasename = query['filebasename']
    sender_email = query['sender_email']
    r = result['r']
    sigma = result['sigma']
    downside_risk = result['downside_risk']
    upside_risk = result['upside_risk']
    beta = result['beta']
    runtime = result['runtime']

    runtime_minutes = int(runtime // 60)
    runtime_seconds = runtime % 60

    # resulting portfolio
    portfolio_dict = result['portfolio']

    # plot
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:finportplot',
        InvocationType='RequestResponse',
        Payload=json.dumps({'body': json.dumps({
            'startdate': startdate,
            'enddate': enddate,
            'components': portfolio_dict
        })})
    )
    finportplot_response_payload = json.load(response['Payload'])
    logging.info(finportplot_response_payload)
    finportplot_body = json.loads(finportplot_response_payload['body'])
    image_url = finportplot_body['plot']['url']
    xlsx_url = finportplot_body['spreadsheet']['url']

    # sending e-mail
    string_components_portfolio = convert_portfolio_to_table(portfolio_dict)
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
        indexsymbol=indexsymbol,
        r=r,
        annual_yield=100*(exp(r)-1),
        sigma=sigma,
        downside_risk=downside_risk,
        upside_risk=upside_risk,
        beta=beta,
        runtime_minutes=runtime_minutes,
        runtime_seconds=runtime_seconds,
        image_url=image_url,
        xlsx_url=xlsx_url,
        filebasename=filebasename,
        string_components_portfolio=string_components_portfolio
    )

    send_email(sender_email, user_email, "Portfolio Optimization - Computation Result", notification_email_body)

    # making json to S3
    eventbody['email_body'] = notification_email_body
    eventbody['portfolio_values_over_time'] = finportplot_body['data']
    jsonname = '{}.json'.format(filebasename)
    jsonpath = os.path.join('/', 'tmp', jsonname)
    json.dump(eventbody, open(jsonpath, 'w'))
    s3_bucket = config['bucket']
    s3_client = boto3.client('s3')
    jsonresponse = s3_client.upload_file(jsonpath, s3_bucket, jsonname)

    return {
        'statusCode': 200,
        'body': json.dumps(eventbody)
    }