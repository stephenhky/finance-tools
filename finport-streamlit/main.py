
import json

import requests
import streamlit as st


def call_MPT(rf, symbols, totalworth, startdate, enddate, riskcoef, homogencoef, email):
    url = "https://jkcspqhrah.execute-api.us-east-1.amazonaws.com/default/finport"

    payload = json.dumps({
        "rf": 0.0083,
        "symbols": symbolstext.split(','),
        "totalworth": totalworth,
        "presetdate": enddate,
        "startdate": startdate,
        "enddate": enddate,
        "riskcoef": riskcoef,
        "homogencoef": homogencoef,
        "email": email
    })

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response


# Streamlit Interface
st.text('Portfolio Optimization')

algorithm = st.selectbox('Algorithm', ['Modern Portfolio Theory', 'Simulated Annealing'])
symbolstext = st.text_input('Symbols (delimited by comma (,))')
totalworth = st.number_input('Total worth (USD)')
startdate = st.date_input('Start date')
enddate = st.date_input('End date')
email = st.text_input('E-mail')

riskcoef = 0.3
homogencoef = 0.1



