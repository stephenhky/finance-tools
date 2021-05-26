
import json
from datetime import date
import logging

import streamlit as st
import numpy as np
from finsim.estimate.fit import fit_BlackScholesMerton_model
from finsim.estimate.risk import estimate_downside_risk, estimate_upside_risk, estimate_beta
from finsim.data import get_yahoofinance_data


allsymbol_info = json.load(open('allsymdf.json', 'r'))
symbols = [item['symbol'] for item in allsymbol_info]


st.set_page_config(
    page_title="Symbol Inference",
    layout="wide"
)


symbol = st.sidebar.selectbox(
    'Choose a symbol',
     symbols
)
index = 'DJI'

i_startdate = st.date_input('Start Date', value=date(2021, 1, 6))
i_enddate = st.date_input('End Date', value=date.today())
startdate = i_startdate.strftime('%Y-%m-%d')
enddate = i_enddate.strftime('%Y-%m-%d')

# getting stock data
symdf = get_yahoofinance_data(symbol, startdate, enddate)

# getting index
indexdf = get_yahoofinance_data(index, startdate, enddate)

# estimation
isrownull = symdf['Close'].isnull()
r, sigma = fit_BlackScholesMerton_model(
    np.array(symdf.loc[~isrownull, 'TimeStamp']),
    np.array(symdf.loc[~isrownull, 'Close'])
)
downside_risk = estimate_downside_risk(
    np.array(symdf.loc[~isrownull, 'TimeStamp']),
    np.array(symdf.loc[~isrownull, 'Close']),
    0.0
)
upside_risk = estimate_upside_risk(
    np.array(symdf.loc[~isrownull, 'TimeStamp']),
    np.array(symdf.loc[~isrownull, 'Close']),
    0.0
)
try:
    beta = estimate_beta(
        np.array(symdf.loc[~isrownull, 'TimeStamp']),
        np.array(symdf.loc[~isrownull, 'Close']),
        np.array(indexdf.loc[~isrownull, 'Close']),
    )
except:
    logging.warning('Index {} failed to be integrated.'.format(index))
    beta = np.inf

st.text('yield = {:.4f}'.format(r))
st.text('volatility = {:.4f}'.format(sigma))
st.text('downside risk = {:.4f}'.format(downside_risk))
st.text('upside risk = {:.4f}'.format(upside_risk))
st.text('beta (w.r.t. {}) = {:.4f}'.format(index, beta))
