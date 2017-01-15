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

from numpy.lib.stride_tricks import as_strided

import warnings
warnings.filterwarnings("ignore")

HISTORY_LIMIT = 20


def init_func():
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = globalconf.log
    client = None

    # this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    return client, log


def end_func(client):
    if client is not None:
        client.disconnect()


def windowed_view(x, window_size):
    """Creat a 2d windowed view of a 1d array.

    `x` must be a 1d numpy array.

    `numpy.lib.stride_tricks.as_strided` is used to create the view.
    The data is not copied.

    Example:

    >>> x = np.array([1, 2, 3, 4, 5, 6])
    >>> windowed_view(x, 3)
    array([[1, 2, 3],
           [2, 3, 4],
           [3, 4, 5],
           [4, 5, 6]])
    """
    y = as_strided(x, shape=(x.size - window_size + 1, window_size),
                   strides=(x.strides[0], x.strides[0]))
    return y


def rolling_max_dd(x, window_size, min_periods=1):
    """Compute the rolling maximum drawdown of `x`.

    `x` must be a 1d numpy array.
    `min_periods` should satisfy `1 <= min_periods <= window_size`.

    Returns an 1d array with length `len(x) - min_periods + 1`.
    """
    if min_periods < window_size:
        pad = np.empty(window_size - min_periods)
        pad.fill(x[0])
        x = np.concatenate((pad, x))
    y = windowed_view(x, window_size)
    running_max_y = np.maximum.accumulate(y, axis=1)
    dd = y - running_max_y
    return dd.min(axis=1)


def max_dd(ser):
    max2here = pd.expanding_max(ser)
    dd2here = (ser - max2here) / ser
    return dd2here.min()



def legend_coppock(copp,copp_shift1):
    #print copp,copp_shift1
    if copp >= 0:
        if copp > copp_shift1:
            legend = "Positive & Up"
        else:
            legend = "Positive & Down"
    else:
        if copp > copp_shift1:
            legend = "Negative & Up"
        else:
            legend = "Negative & Down"
    return legend

def COPP(df, a=11, b=14, n=50, close_nm='close'):
    """
    Coppock Curve
    """
    M = df[close_nm].diff(int(n * a / 10) - 1)
    N = df[close_nm].shift(int(n * a / 10) - 1)
    ROC1 = M / N
    M = df[close_nm].diff(int(n * b / 10) - 1)
    N = df[close_nm].shift(int(n * b / 10) - 1)
    ROC2 = M / N
    Copp = pd.Series(pd.Series.ewm(ROC1 + ROC2, span = n, min_periods = n,
                     adjust=True, ignore_na=False).mean(), name = 'Copp_' + str(n))
    #Copp = pd.Series(pd.ewma(ROC1 + ROC2, span=n, min_periods=n, adjust=True), name='Copp_' + str(n))
    df = df.join(Copp)
    df['Copp_' + str(n) + '_shift1'] = df['Copp_' + str(n)].shift(1)
    df['legend'] = df.apply(lambda row: legend_coppock(row['Copp_' + str(n)],row['Copp_' + str(n) + '_shift1']),axis=1)
    df=df.drop('Copp_' + str(n) + '_shift1',1)
    return df

def print_coppock_diario(symbol="SPX"):
    client , log_analytics = init_func()
    #start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    #end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    df=ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample('1D', how=conversion).dropna()
    # conf. semanal: StoCop (60,30,50) NO hay suficiente historico para configuracion semanal del copock
    # conf. diaria: StoCop (12,6,10)
    df = COPP(df, 12, 6, 10)
    print df.iloc[-HISTORY_LIMIT:] # pinta los ultimos 30 dias del coppock
    end_func(client)

def print_volatity(symbol):
    window=34.0
    year_days=252.0
    length = 20
    client, log_analytics = init_func()
    df = ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample('1D', how=conversion).dropna().rename(columns={'close': symbol})
    df['HV'] = pd.rolling_std(df[symbol],window=int(window),min_periods=int(window)) * np.sqrt(window / year_days)
    df=df.drop(['high','open','low'], 1)
    vix = ra.extrae_historical_underl("VIX")
    vix.index = pd.to_datetime(vix.index, format="%Y%m%d  %H:%M:%S")
    vix["date"] = vix.index
    vix[[u'close', u'high', u'open', u'low']]=vix[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    vix = vix.resample('1D', how=conversion).dropna().rename(columns={'close': 'vix'})['vix']
    vix_ewm = pd.Series(pd.Series.ewm(vix, span = length, min_periods = length,
                     adjust=True, ignore_na=False).mean(), name = 'vix_ema' + str(length))
    df = df.join(vix_ewm)
    df = df.join(vix)
    vix_std = pd.rolling_std(df['vix'], window=int(window), min_periods=int(window))
    VIX_BB_2SD_UP = vix_ewm + 2 * vix_std
    VIX_BB_2SD_DOWN = vix_ewm - 2 * vix_std
    VIX_BB_1SD_UP = vix_ewm +  vix_std
    VIX_BB_1SD_DOWN = vix_ewm -  vix_std
    df['ALERT_IV'] = np.where(( ( df['vix'] < VIX_BB_1SD_DOWN ) ) , "LOW","------")
    df['ALERT_IV'] = np.where(( ( df['vix'] > VIX_BB_1SD_UP ) ) , "HIGH",df['ALERT_IV'])
    df['ALERT_IV'] = np.where( (df['vix'] < VIX_BB_2SD_DOWN) , "EXTREME_LOW",df['ALERT_IV'])
    df['ALERT_IV'] = np.where(( ( df['vix'] > VIX_BB_2SD_UP ) ) , "EXTREME_HIGH",df['ALERT_IV'])
    print df.iloc[-HISTORY_LIMIT:]
    end_func(client)

def print_fast_move(symbol):
    length = 20.0
    num_dev_dn = -2.0
    num_dev_up = 2.0
    dbb_length = 120.0
    client, log_analytics = init_func()
    df = ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample('1D', how=conversion).dropna().rename(columns={'close': symbol})
    stddev = pd.rolling_std(df[symbol],window=int(length),min_periods=int(length))
    midline = pd.Series(pd.Series.ewm(df[symbol], span = int(length), min_periods = int(length),
              adjust=True, ignore_na=False).mean(), name = symbol + '_ema' + str(int(length)))
    lowerBand = midline + num_dev_dn * stddev
    upperBand = midline + num_dev_up * stddev
    dbb = np.sqrt((upperBand - lowerBand) / upperBand ) * length
    dbbmed = pd.Series(pd.Series.ewm(dbb, span = int(dbb_length), min_periods = int(dbb_length),
              adjust=True, ignore_na=False).mean(), name = 'dbb_ema' + str(int(dbb_length)))
    factor = dbbmed * 4.0 / 5.0
    atl = dbb - factor
    df = df.join(pd.DataFrame(atl))
    df['al1'] = np.where(((atl > 0.0)), np.nan , atl )

    """

    def factor = dbbmed * 4 / 5;
    def atl = dbb - factor;
    plot al1=if (atl>0) then Double.Nan else atl;
    al1.SetDefaultColor(Color.RED);
    al1.SetPaintingStrategy(PaintingStrategy.HISTOGRAM);
    def c1 = if atl > 0 and atl < Parameter then 1 else 0;
    def c2 = if atl[1] > 0 and atl[2] > 0 and atl[3] > 0 and atl[4] > 0 and atl[5] > 0 and atl[6] > 0 and atl[7] > 0 and atl[8] > 0 and atl[9] > 0 and atl[10] > 0 then 1 else 0;
    def c3 = c1 + c2 + base;
    plot al2 = if c3 == 3 then atl else Double.Nan;
    al2.SetDefaultColor(Color.DARK_RED);
    al2.SetPaintingStrategy(PaintingStrategy.HISTOGRAM);

    """

    print df #.iloc[-HISTORY_LIMIT:]
    end_func(client)

def print_emas(symbol="SPX"):
    client, log_analytics = init_func()
    df = ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample('1D', how=conversion).dropna()
    n = 50
    ema50 = pd.Series(pd.Series.ewm(df['close'], span = n, min_periods = n,
                     adjust=True, ignore_na=False).mean(), name = 'EMA_' + str(n))
    df = df.join(ema50)
    df['RSK_EMA50'] = np.where(df['close'] > df['EMA_' + str(n)], "-----", "ALERT")

    # sacar los canales de IV del historico del VIX
    vix = ra.extrae_historical_underl("VIX")
    vix.index = pd.to_datetime(vix.index, format="%Y%m%d  %H:%M:%S")
    vix["date"] = vix.index
    vix[[u'close', u'high', u'open', u'low']]=vix[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    vix = vix.resample('1D', how=conversion).dropna().rename(columns={'close': 'vix'})['vix']
    df = df.join(vix)
    df['lower_wk_iv_channel']=df['close'].shift(5) * (1 - (df['vix'].shift(5)*0.01 / np.sqrt(252/5)) )
    df['upper_wk_iv_channel'] = df['close'].shift(5) * (1 + (df['vix'].shift(5)*0.01 / np.sqrt(252/5)))
    df['lower_mo_iv_channel']= df['close'].shift(34) * (1 - (df['vix'].shift(34)*0.01 / np.sqrt(252/34)))
    df['upper_mo_iv_channel'] = df['close'].shift(34) * (1 + (df['vix'].shift(34)*0.01 / np.sqrt(252/34)))

    df['CANAL_IV_WK'] = np.where( (df['close'] < df['upper_wk_iv_channel']) &
                                  (df['close'] > df['lower_wk_iv_channel']), "-----", "ALERT")
    df['CANAL_IV_MO'] = np.where( (df['close'] < df['upper_mo_iv_channel']) &
                                   (df['close'] > df['lower_mo_iv_channel']), "-----", "ALERT")

    print df.iloc[-HISTORY_LIMIT:] # pinta los ultimos 30 dias del coppock
    end_func(client)

def print_historical_underl(start_dt, end_dt, symbol):
    client, log_analytics = init_func()
    # start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    # end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    start_dt1 = start_dt  # +" 0:00:00"
    end_dt1 = end_dt  # +" 23:59:59"
    df = ra.extrae_historical_underl(symbol,start_dt1,end_dt1)
    print df
    end_func(client)


def print_summary_underl(symbol):
    client, log_analytics = init_func()
    df = ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample('1D', how=conversion).dropna()
    df=df.drop(['high','open','low'], 1)
    GroupedYear = df.groupby(pd.TimeGrouper('A'))
    df["YTD"] = GroupedYear['close'].transform(lambda x: ( x / x.iloc[0] - 1.0))
    GroupedMonth = df.groupby([(df.index.year), (df.index.month)])
    df["MTD"] = GroupedMonth['close'].transform(lambda x: ( x / x.iloc[0] - 1.0))

    GroupedWeek = df.groupby([(df.index.year), (df.index.week)])
    df["WTD"] = GroupedWeek['close'].transform(lambda x: ( x / x.iloc[0] - 1.0))


    n = 100
    s = df['close']
    window_length = 252

    rolling_dd = pd.rolling_apply(s, window_length, max_dd, min_periods=0)
    df2 = pd.concat([s, rolling_dd], axis=1)
    df2.columns = ['s', 'rol_dd_%d' % window_length]
    my_rmdd = rolling_max_dd(s.values, window_length, min_periods=1)

    df = pd.concat([df,rolling_dd],axis=1)
    df.columns = ['close', 'YTD', 'MTD', 'WTD', 'rol_dd_%d' % window_length]
    #lastDayPrevMonth = dt.date.today().replace(day=1) - dt.timedelta(days=1)
    output = df.iloc[-HISTORY_LIMIT:].to_string(formatters={
                                    'YTD': '{:,.2%}'.format,
                                    'MTD': '{:,.2%}'.format,
                                    'WTD': '{:,.2%}'.format,
                                    'rol_dd_%d' % window_length: '{:,.2%}'.format,
                                    'close': '{:,.2f}'.format
                                })
    print(output)
    end_func(client)

def print_historical_chain(start_dt,end_dt,symbol,strike,expiry,right,type):
    """
    Type should be bid, ask or trades
    """
    client , log_analytics = init_func()
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


if __name__ == "__main__":
    #print_coppock_diario(start_dt="20160101", end_dt="20170303", symbol="SPX")
    #print_emas("SPX")
    #print_summary_underl("SPX")
    print_fast_move("SPX")