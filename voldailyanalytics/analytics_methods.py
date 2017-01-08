# coding: utf-8
import volibutils.sync_client as ib
from volsetup import config
import datetime as dt
import pandas as pd
import numpy as np
import swigibpy as sy
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volsetup.logger import logger
from swigibpy import Contract as IBcontract
from swigibpy import Order as IBOrder
import time
from voldailyanalytics import run_analytics as ra


def init_func():
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = globalconf.log
    client = None

    #this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    return client , log

def end_func(client):
    if not client is None:
        client.disconnect()


def COPP(df, n,close_nm='close'):
    """
    Coppock Curve
    """
    M = df[close_nm].diff(int(n * 11 / 10) - 1)
    N = df[close_nm].shift(int(n * 11 / 10) - 1)
    ROC1 = M / N
    M = df[close_nm].diff(int(n * 14 / 10) - 1)
    N = df[close_nm].shift(int(n * 14 / 10) - 1)
    ROC2 = M / N
    Copp = pd.Series(pd.Series.ewm(ROC1 + ROC2, span = n, min_periods = n,
                     adjust=True,ignore_na=False).mean(), name = 'Copp_' + str(n))
    #Copp = pd.Series(pd.ewma(ROC1 + ROC2, span=n, min_periods=n, adjust=True), name='Copp_' + str(n))
    df = df.join(Copp)
    return df

def print_coppock_diario(start_dt,end_dt,symbol="SPX"):
    client , log = init_func()
    #start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    #end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    start_dt1 = start_dt #+" 0:00:00"
    end_dt1 = end_dt #+" 23:59:59"
    df=ra.extrae_historical_underl(start_dt1,end_dt1,symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df=df.resample('1D', how=conversion)
    df=COPP(df,1)
    print df
    end_func(client)

def print_historical_underl(start_dt,end_dt,symbol):
    client , log = init_func()
    #start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    #end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    start_dt1 = start_dt #+" 0:00:00"
    end_dt1 = end_dt #+" 23:59:59"
    df=ra.extrae_historical_underl(start_dt1,end_dt1,symbol)
    print df
    end_func(client)


def print_historical_chain(start_dt,end_dt,symbol,strike,expiry,right,type):
    """
    Type should be bid, ask or trades
    """
    client , log = init_func()
    start_dt1 = start_dt #+" 0:00:00"
    end_dt1 = end_dt #+" 23:59:59"
    df=ra.extrae_historical_chain(start_dt1,end_dt1,symbol,strike,expiry,right)
    columns = [x for x in df.columns if type in x]
    """
    [u'WAP_trades', u'close_trades', u'count_trades', u'currency', u'expiry', u'hasGaps_trades', u'high_trades',
     u'load_dttm', u'low_trades', u'multiplier', u'open_trades', u'reqId_trades', u'right', u'secType', u'strike',
     u'symbol', u'volume_trades', u'WAP_ask', u'close_ask', u'count_ask', u'hasGaps_ask', u'high_ask', u'low_ask',
     u'open_ask', u'reqId_ask', u'volume_ask', u'WAP_bid', u'close_bid', u'count_bid', u'hasGaps_bid', u'high_bid',
     u'low_bid', u'open_bid', u'reqId_bid', u'volume_bid']
     """
    print df[columns]
    end_func(client)