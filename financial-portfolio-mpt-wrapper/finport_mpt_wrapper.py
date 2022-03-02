
import json
import logging
import asyncio
from math import exp
from operator import itemgetter
import os

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


def convert_expreturn_to_annualreturn(r):  # in the units of year
    return exp(r)-1


async def extract_symbol_info(symbol, startdate, enddate):
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:fininfoestimate',
        InvocationType='RequestResponse',
        Payload=json.dumps({
            'body': json.dumps({
                'symbol': symbol,
                'startdate': startdate,
                'enddate': enddate
            })
        })
    )
    response_payload = json.load(response['Payload'])
    return json.loads(response_payload['body'])


async def extract_symbols_info(symbols, startdate, enddate):
    symbols_info = await asyncio.gather(*[
            extract_symbol_info(symbol, startdate, enddate)
            for symbol in symbols
        ])
    print(symbols_info)
    symbols_info_dict = {info['symbol']: info for info in symbols_info}
    return symbols_info_dict


def convert_portfolio_to_table(portfolio_dict, startdate, enddate):
    symbols_info_dict = asyncio.run(extract_symbols_info(
        portfolio_dict['timeseries'][0]['portfolio'].keys(),
        startdate,
        enddate
    ))
    print(symbols_info_dict)

    html_string = '<table style="width:100%">'
    html_string += '<tr><th>Symbol</th>' + \
                   '<th>Number of Shares</th>' + \
                   '<th>Return Rate</th>' + \
                   '<th>Volatility</th>' + \
                   '<th>Downside Risk</th>' + \
                   '<th>Upside Risk</th>' + \
                   '<th>Record Start Date</th>' + \
                   '<th>Record End Date</th>' + \
                   '<th>Number of Records</th>' + \
                   '</tr>'

    row_html_template = "<tr><th><a href='https://finance.yahoo.com/quote/{symbol:}/'>{symbol:}</a></th>" + \
                        "<th>{nbshares:.2f}</th>" + \
                        "<th>{r:.4f} (annual: {annual_r:.2f}%)</th>" + \
                        "<th>{vol:.4f}</th>" + \
                        "<th>{downsiderisk:.4f}</th>" + \
                        "<th>{upsiderisk:.4f}</th>" + \
                        "<th>{rec_startdate:}</th>" + \
                        "<th>{rec_enddate:}</th>" + \
                        "<th>{nbrecs:}</th>" + \
                        "</tr>"

    for symbol, nbshares in sorted(
        portfolio_dict['timeseries'][0]['portfolio'].items(),
        key=itemgetter(0)
    ):
        html_string += row_html_template.format(
            symbol=symbol,
            nbshares=nbshares,
            r=symbols_info_dict[symbol]['r'],
            annual_r=100*convert_expreturn_to_annualreturn(symbols_info_dict[symbol]['r']),
            vol=symbols_info_dict[symbol]['vol'],
            downsiderisk=symbols_info_dict[symbol]['downside_risk'],
            upsiderisk=symbols_info_dict[symbol]['upside_risk'],
            rec_startdate=symbols_info_dict[symbol]['data_startdate'],
            rec_enddate=symbols_info_dict[symbol]['data_enddate'],
            nbrecs=symbols_info_dict[symbol]['nbrecs']
        )

    html_string += '</table>'
    return html_string


def lambda_handler(event, context):
    # getting config
    config = json.load(open('config.json', 'r'))

    # parsing argument
    eventbody = json.loads(event['body'])
    print(eventbody)
    query = eventbody['query']
    result = eventbody['result']
    rf = query['rf']
    symbols = query['symbols']
    totalworth = query['totalworth']
    presetdate = query['presetdate']
    startdate = query['estimating_startdate']
    enddate = query['estimating_enddate']
    riskcoef = query['riskcoef']
    homogencoef = query['homogencoef']
    V = query['V']
    timeweighted_scheme = query['timeweighted_scheme']
    yearscale = query.get('yearscale', 1000000.)
    include_dividends = query['include_dividends']
    indexsymbol = query['index']
    user_email = query['email']
    filebasename = query['filebasename']
    sender_email = query['sender_email']
    portfolio = result['portfolio']
    print(portfolio)
    symbols_nbshares = result['symbols_nbshares']
    print(symbols_nbshares)
    runtime = result['runtime']
    estimates = eventbody['estimates']

    runtime_minutes = int(runtime // 60)
    runtime_seconds = runtime % 60

    # constructing dynamic portfolio payload
    portfolio_dict = {
        'startdate': startdate,
        'enddate': enddate,
        'components': {
            'name': 'DynamicPortfolio',
            'current_date': presetdate,
            'timeseries': [
                {'date': startdate, 'portfolio': symbols_nbshares}
            ]
        }
    }

    # plot
    response = lambda_client.invoke(
        FunctionName='arn:aws:lambda:us-east-1:409029738116:function:finportplot',
        InvocationType='RequestResponse',
        Payload=json.dumps({'body': json.dumps(portfolio_dict)})
    )
    finportplot_response_payload = json.load(response['Payload'])
    logging.info(finportplot_response_payload)
    finportplot_body = json.loads(finportplot_response_payload['body'])
    image_url = finportplot_body['plot']['url']
    xlsx_url = finportplot_body['spreadsheet']['url']

    # sending e-mail
    string_components_portfolio = convert_portfolio_to_table(portfolio_dict['components'], startdate, enddate)
    notification_email_body = open('notification_email.html', 'r').read().format(
        symbols=', '.join(sorted(symbols)),
        runtime_minutes=runtime_minutes,
        runtime_seconds=runtime_seconds,
        startdate=startdate,
        enddate=enddate,
        totalworth=totalworth,
        string_components_portfolio=string_components_portfolio,
        mpt_r=portfolio['yield'],
        mpt_annual_yield=100*convert_expreturn_to_annualreturn(portfolio['yield']),
        mpt_sigma=portfolio['volatility'],
        r=estimates['r'],
        annual_yield=100*convert_expreturn_to_annualreturn(estimates['r']),
        sigma=estimates['sigma'],
        downside_risk=estimates['downside_risk'],
        upside_risk=estimates['upside_risk'],
        beta=estimates['beta'],
        image_url=image_url,
        xlsx_url=xlsx_url,
        rf=rf,
        riskcoef=riskcoef,
        homogencoef=homogencoef,
        indexsymbol=indexsymbol,
        V=V,
        include_dividends=include_dividends,
        filebasename=filebasename,
        timeweightedstr='None' if timeweighted_scheme is None else 'exponential with yearscale = {:.4f}'.format(yearscale)
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
