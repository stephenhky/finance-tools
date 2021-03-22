
import json

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


def lambda_handler(event, context):
    # parsing argument
    eventbody = json.loads(event['body'])
    print(eventbody)
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
    print(finportplot_response_payload)
    finportplot_body = json.loads(finportplot_response_payload['body'])
    image_url = finportplot_body['plot']['url']
    xlsx_url = finportplot_body['spreadsheet']['url']

    # sending e-mail
    string_components_portfolio = '\n'.format([
        '{}\t{}'.format(symbol, nbshares)
        for symbol, nbshares in portfolio_dict['timeseries'][0]['portfolio'].items()
    ])
    notification_email_body = open('notification_email.html', 'r').read().format(
        symbols=', '.format(symbols),
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
        sigma=sigma,
        downside_risk=downside_risk,
        upside_risk=upside_risk,
        beta=beta,
        runtime=runtime,
        image_url=image_url,
        xlsx_url=xlsx_url,
        filebasename=filebasename,
        string_components_portfolio=string_components_portfolio
    )

    send_email(sender_email, user_email, "Portfolio Optimization - Computation Result", notification_email_body)

    event['email_body'] = notification_email_body

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }