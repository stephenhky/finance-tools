
import json
from datetime import date
from math import exp

import asyncio
import pandas as pd
import requests
import streamlit as st
from matplotlib import pyplot as plt



def convert_expreturn_to_annualreturn(r):  # in the units of year
    return exp(r)-1


async def get_symbol_estimations(symbol, startdate, enddate, index='DJI'):
    url = "https://1phrvfsc16.execute-api.us-east-1.amazonaws.com/default/fininfoestimate"

    payload = json.dumps({
        "symbol": symbol,
        "startdate": startdate,
        "enddate": enddate,
        "index": index
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return json.loads(response.text)


async def get_symbol_plot_data(symbol, startdate, enddate):
    url = "https://ed0lbq7vph.execute-api.us-east-1.amazonaws.com/default/finportplot"

    payload = json.dumps({
        'startdate': startdate,
        'enddate': enddate,
        'components': {symbol: 1}
    })
    headers = {
        'Content-Type': 'text/plain'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    result = json.loads(response.text)
    data = result['data']
    plot_url = result['plot']['url']
    return pd.DataFrame.from_records(data), plot_url


# load symbols
allsymbol_info = json.load(open('allsymdf.json', 'r'))
symbols = [item['symbol'] for item in allsymbol_info]
allsymbol_info = {item['symbol']: item for item in allsymbol_info}


st.sidebar.title('Symbols')
symbol = st.sidebar.selectbox(
    'Choose a symbol',
    symbols
)
i_startdate = st.sidebar.date_input('Start Date', value=date(2021, 1, 6))
i_enddate = st.sidebar.date_input('End Date', value=date.today())

index = 'VOO'
startdate = i_startdate.strftime('%Y-%m-%d')
enddate = i_enddate.strftime('%Y-%m-%d')

# starting asyncio
task_estimate_symbols = get_symbol_estimations(symbol, startdate, enddate, index)
task_values_over_time = get_symbol_plot_data(symbol, startdate, enddate)

# estimation
symbol_estimate = asyncio.run(task_estimate_symbols)
r = symbol_estimate['r']
sigma = symbol_estimate['vol']
downside_risk = symbol_estimate['downside_risk']
upside_risk = symbol_estimate['upside_risk']
beta = symbol_estimate['beta']

# making portfolio and time series
worthdf, plot_url = asyncio.run(task_values_over_time)

# display
col1, col2 = st.beta_columns((2, 1))

# plot
f = plt.figure()
f.set_figwidth(10)
f.set_figheight(8)
plt.xlabel('Date')
plt.ylabel('Portfolio Value')
stockline, = plt.plot(worthdf['TimeStamp'], worthdf['stock_value'], label='stock', linewidth=0.75)
totalline, = plt.plot(worthdf['TimeStamp'], worthdf['value'], label='stock+dividend', linewidth=0.75)
xticks, _ = plt.xticks(rotation=90)
step = len(xticks) // 10
plt.xticks(xticks[::step])
plt.legend([stockline, totalline], ['stock', 'stock+dividend'])
col1.pyplot(f, height=30)

# inference
col2.title('Inference')
col2.text('yield = {:.4f} (annually {:.2f}%)'.format(r, convert_expreturn_to_annualreturn(r)*100))
col2.text('volatility = {:.4f}'.format(sigma))
col2.text('downside risk = {:.4f}'.format(downside_risk))
col2.text('upside risk = {:.4f}'.format(upside_risk))
if beta is not None:
    col2.text('beta (w.r.t. {}) = {:.4f}'.format(index, beta))
col2.text('Name: {}'.format(allsymbol_info[symbol]['description']))
col2.markdown('Symbol: [{sym:}](https://finance.yahoo.com/quote/{sym:})'.format(sym=symbol))
col2.markdown('[Download image]({})'.format(plot_url))

# Data display
st.title('Data')
st.dataframe(worthdf)
