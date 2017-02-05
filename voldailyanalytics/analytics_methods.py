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
import json
from numpy.lib.stride_tricks import as_strided

import warnings

warnings.filterwarnings("ignore")
HISTORY_LIMIT = 20

def get_contract_details(symbol,conId=None):
    """
    In the future will get details fro DB given a ConId
    There will be a process that populate in background this table DB
    with all the potential contracts and the corresponding contract ID
    """
    db1={
        "ES":{"secType":"FOP","exchange":"GLOBEX","multiplier":"50","currency":"USD",
              "underlType":"FUT","underlCurrency":"XXX","underlExchange":"GLOBEX"},
        "SPY":{"secType":"OPT","exchange":"SMART","multiplier":"100","currency":"USD",
               "underlType":"STK","underlCurrency":"USD","underlExchange":"SMART"}
    }
    #if conId is None:
    return db1[symbol]



def init_func():
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = globalconf.log
    client = None
    # this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    return client, log, globalconf


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


def print_coppock_diario(symbol="SPX",period="1D"):
    client , log_analytics, globalconf = init_func()
    #start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    #end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    df=ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample(period, how=conversion).dropna()
    # conf. semanal: StoCop (60,30,50) NO hay suficiente historico para configuracion semanal del copock
    # conf. diaria: StoCop (12,6,10)
    df = COPP(df, 12, 6, 10)
    print df.iloc[-HISTORY_LIMIT:] # pinta los ultimos 30 dias del coppock
    end_func(client)


def print_volatity(symbol):
    window=34.0
    year_days=252.0
    length = 20
    client, log_analytics, globalconf = init_func()
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

    VIX_BB_2SD_UP.sort_index(inplace=True)
    VIX_BB_2SD_DOWN.sort_index(inplace=True)
    VIX_BB_1SD_UP.sort_index(inplace=True)
    VIX_BB_1SD_UP.sort_index(inplace=True)
    df.sort_index(inplace=True)
    try:
        df['ALERT_IV'] = np.where(( ( df['vix'] < VIX_BB_1SD_DOWN ) ) , "LOW","------")
        df['ALERT_IV'] = np.where(( ( df['vix'] > VIX_BB_1SD_UP ) ) , "HIGH",df['ALERT_IV'])
        df['ALERT_IV'] = np.where( (df['vix'] < VIX_BB_2SD_DOWN) , "EXTREME_LOW",df['ALERT_IV'])
        df['ALERT_IV'] = np.where(( ( df['vix'] > VIX_BB_2SD_UP ) ) , "EXTREME_HIGH",df['ALERT_IV'])
    except ValueError as e:
        print("ValueError raised [" + str(e) + "]  Missing rows for VIX needed to generate alerts ...")

    print df.iloc[-HISTORY_LIMIT:]
    end_func(client)

def print_fast_move(symbol):
    length = 20.0
    num_dev_dn = -2.0
    num_dev_up = 2.0
    dbb_length = 120.0
    client, log_analytics, globalconf = init_func()
    df = ra.extrae_historical_underl(symbol)
    df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
    df["date"] = df.index
    df[[u'close', u'high', u'open', u'low']]=df[[u'close', u'high',u'open',u'low']].apply(pd.to_numeric)
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',}
    df = df.resample('1H', how=conversion).dropna().rename(columns={'close': symbol})
    df = df.drop(['high', 'open', 'low'], 1)
    stddev = pd.rolling_std(df[symbol],window=int(length),min_periods=int(length))
    midline = pd.Series(pd.Series.ewm(df[symbol], span = int(length), min_periods = int(length),
              adjust=True, ignore_na=False).mean(), name = symbol + '_ema' + str(int(length)))
    df['lowerBand'] = midline + num_dev_dn * stddev
    df['upperBand'] = midline + num_dev_up * stddev
    df['dbb'] = np.sqrt((df['upperBand'] - df['lowerBand']) / df['upperBand'] ) * length
    df['dbbmed'] = pd.Series(pd.Series.ewm(df['dbb'], span = int(dbb_length), min_periods = int(dbb_length),
                        adjust=True, ignore_na=False).mean(), name = 'dbb_ema' + str(int(dbb_length)))
    df['factor'] = df['dbbmed'] * 4.0 / 5.0
    df['atl'] = df['dbb'] - df['factor']
    df['al1'] = np.where(((df['atl'] > 0.0)), np.nan , df['atl'] )

    # TODO: Finish this:
    """
    def c1 = if atl > 0 and atl < Parameter then 1 else 0;
    def c2 = if atl[1] > 0 and atl[2] > 0 and atl[3] > 0 and atl[4] > 0 and atl[5] > 0 and atl[6] > 0 and atl[7] > 0 and atl[8] > 0 and atl[9] > 0 and atl[10] > 0 then 1 else 0;
    def c3 = c1 + c2 + base;
    plot al2 = if c3 == 3 then atl else Double.Nan;
    al2.SetDefaultColor(Color.DARK_RED);
    al2.SetPaintingStrategy(PaintingStrategy.HISTOGRAM);
    """
    print df.iloc[-HISTORY_LIMIT:]
    end_func(client)


def print_emas(symbol="SPX"):
    client, log_analytics, globalconf = init_func()
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
    df['lower_wk_iv']=df['close'].shift(5) * (1 - (df['vix'].shift(5)*0.01 / np.sqrt(252/5)) )
    df['upper_wk_iv'] = df['close'].shift(5) * (1 + (df['vix'].shift(5)*0.01 / np.sqrt(252/5)))
    df['lower_mo_iv']= df['close'].shift(34) * (1 - (df['vix'].shift(34)*0.01 / np.sqrt(252/34)))
    df['upper_mo_iv'] = df['close'].shift(34) * (1 + (df['vix'].shift(34)*0.01 / np.sqrt(252/34)))

    df['CANAL_IV_WK'] = np.where( (df['close'] < df['upper_wk_iv']) &
                                  (df['close'] > df['lower_wk_iv']), "-----", "ALERT")
    df['CANAL_IV_MO'] = np.where( (df['close'] < df['upper_mo_iv']) &
                                   (df['close'] > df['lower_mo_iv']), "-----", "ALERT")

    output = df.iloc[-HISTORY_LIMIT:].to_string(formatters={
                                    'lower_wk_iv': '{:,.2f}'.format,
                                    'upper_wk_iv': '{:,.2f}'.format,
                                    'lower_mo_iv': '{:,.2f}'.format,
                                    'upper_mo_iv': '{:,.2f}'.format,
                                    'EMA_' + str(n): '{:,.2f}'.format
                                })
    print(output)
    end_func(client)

def print_account_snapshot(valuation_dt):
    """
    valuation dt = YYYY-MM-DD-HH
    :param valuation_dt:
    :return:
    """
    date1=valuation_dt.split("-")
    client, log_analytics, globalconf = init_func()
    accountid = globalconf.get_accountid()
    x  = dt.datetime(year=int(date1[0]),month=int(date1[1]),day=int(date1[2]),hour=int(date1[3]),minute=59,second=59)
    t_margin, t_prem = ra.extrae_account_snapshot(valuation_dttm=x, accountid=accountid,
                                                   scenarioMode="N", simulName="NA")

    t_margin = t_margin.rename(columns={    'RegTMargin_USD':'RegTM',
                                            'MaintMarginReq_USD':'MaintM',
                                            'InitMarginReq_USD':'IniM',
                                            'FullMaintMarginReq_USD':'FMaintM',
                                            'FullInitMarginReq_USD':'FIniM'
                              })

    output = t_margin.to_string(formatters={
                                    'RegTM': '{:,.2f}'.format,
                                    'MaintM': '{:,.2f}'.format,
                                    'IniM': '{:,.2f}'.format,
                                    'FMaintM': '{:,.2f}'.format,
                                    'FIniM': '{:,.2f}'.format
                                })
    t_prem = t_prem.rename(columns={    'TotalCashBalance_BASE':'TCashBASE',
                                        'TotalCashBalance_EUR':'TCashEUR',
                                        'TotalCashBalance_USD':'TCashUSD',
                                        'TotalCashValue_USD':'CashVUSD',
                                        'CashBalance_EUR':'CashEUR',
                                        'CashBalance_USD':'CashUSD',
                                        'TotalCashValue_C_USD':'Cash_C_USD',
                                        'TotalCashValue_S_USD':'Cash_S_USD',
                                        'CashBalance_BASE':'CashBASE',
                                        'ExchangeRate_EUR':'FXEUR'
                              })

    output2 = t_prem.to_string(formatters={
                                    'TCashBASE': '{:,.2f}'.format,
                                    'TCashEUR': '{:,.2f}'.format,
                                    'TCashUSD': '{:,.2f}'.format,
                                    'CashVUSD': '{:,.2f}'.format,
                                    'CashEUR': '{:,.2f}'.format,
                                    'CashUSD': '{:,.2f}'.format,
                                    'Cash_C_USD': '{:,.2f}'.format,
                                    'Cash_S_USD': '{:,.2f}'.format,
                                    'CashBASE': '{:,.2f}'.format,
                                    'FXEUR': '{:,.2f}'.format
                                })
    print("___MARGIN________________________________________________________________")
    print(output)
    print("___PREMIUM_______________________________________________________________")
    print(output2)

    temp_portfolio = ra.extrae_portfolio_positions(valuation_dttm=x,
                                                   symbol=None, expiry=None, secType=None,
                                                   accountid=accountid,
                                                   scenarioMode="N", simulName="NA")

    if not temp_portfolio is None:
        temp_portfolio = temp_portfolio.drop(['portfolio_current_datetime'], 1)
        temp_portfolio = temp_portfolio.rename(columns={    'portfolio_averageCost': 'Cost',
                                                            'portfolio_marketValue': 'MktVal',
                                                            'portfolio_multiplier': 'mult',
                                                            'portfolio_position': 'pos',
                                                            'portfolio_strike': 'str',
                                                            'portfolio_right': 'P_C',
                                                            'portfolio_symbol': 'sym',
                                                            'portfolio_expiry': 'exp',
                                                            'portfolio_precio_neto': 'NPrc',
                                                            'portfolio_load_dttm': 'LoDttm',
                                                            'portfolio_unrealizedPNL': 'PnL'
                                                            })

        output3 = temp_portfolio.to_string(formatters={
                                        'Cost': '{:,.2f}'.format,
                                        'MktVal': '{:,.2f}'.format,
                                        'mult': '{:,.0f}'.format,
                                        'pos': '{:,.0f}'.format,
                                        'str': '{:,.2f}'.format,
                                        'NPrc': '{:,.2f}'.format,
                                        'PnL': '{:,.2f}'.format
                                    })

        print("___PORTFOLIO_______________________________________________________________")
        print output3
    end_func(client)


def print_account_delta(valuation_dt):
    """
    valuation dt = YYYY-MM-DD-HH
    :param valuation_dt:
    :return:
    """
    date1=valuation_dt.split("-")
    client, log_analytics, globalconf = init_func()
    accountid = globalconf.get_accountid()
    x  = dt.datetime(year=int(date1[0]),month=int(date1[1]),day=int(date1[2]),hour=int(date1[3]),minute=0,second=0)
    t_margin, t_prem = ra.extrae_account_delta_new(valuation_dttm=x, accountid=accountid,
                                                   scenarioMode="N", simulName="NA")
    t_margin = t_margin.rename(columns={    'RegTMargin_USD':'RegTM',
                                            'MaintMarginReq_USD':'MaintM',
                                            'InitMarginReq_USD':'IniM',
                                            'FullMaintMarginReq_USD':'FMaintM',
                                            'FullInitMarginReq_USD':'FIniM'
                              })

    output = t_margin.to_string(formatters={
                                    'RegTM': '{:,.2f}'.format,
                                    'MaintM': '{:,.2f}'.format,
                                    'IniM': '{:,.2f}'.format,
                                    'FMaintM': '{:,.2f}'.format,
                                    'FIniM': '{:,.2f}'.format
                                })
    t_prem = t_prem.rename(columns={    'TotalCashBalance_BASE':'TCashBASE',
                                        'TotalCashBalance_EUR':'TCashEUR',
                                        'TotalCashBalance_USD':'TCashUSD',
                                        'TotalCashValue_USD':'CashVUSD',
                                        'CashBalance_EUR':'CashEUR',
                                        'CashBalance_USD':'CashUSD',
                                        'TotalCashValue_C_USD':'Cash_C_USD',
                                        'TotalCashValue_S_USD':'Cash_S_USD',
                                        'CashBalance_BASE':'CashBASE',
                                        'ExchangeRate_EUR':'FXEUR'
                              })


    output2 = t_prem.to_string(formatters={
                                    'TCashBASE': '{:,.2f}'.format,
                                    'TCashEUR': '{:,.2f}'.format,
                                    'TCashUSD': '{:,.2f}'.format,
                                    'CashVUSD': '{:,.2f}'.format,
                                    'CashEUR': '{:,.2f}'.format,
                                    'CashUSD': '{:,.2f}'.format,
                                    'Cash_C_USD': '{:,.2f}'.format,
                                    'Cash_S_USD': '{:,.2f}'.format,
                                    'CashBASE': '{:,.2f}'.format,
                                    'FXEUR': '{:,.2f}'.format
                                })
    print("__MARGIN___________________________________________________")
    print(output)
    print("__PREMIUM__________________________________________________")
    print(output2)
    print("__ORDERS___________________________________________________")
    store = globalconf.open_orders_store()
    node = store.get_node("/" + accountid)
    start_dt=x.replace(minute=0, second=0)
    end_dt=x.replace(minute=59, second=59)
    #coord1 = "times < " + end_dt + " & times > " + start_dt
    #c = store.select_as_coordinates(node._v_pathname,coord1)
    #df1 = store.select(node._v_pathname,where=c)
    df1 = store.select(node._v_pathname)
    df1['times'] = df1['times'].apply(lambda x: dt.datetime.strptime(x, '%Y%m%d %H:%M:%S'))
    df1 = df1[(df1.times <= end_dt) & (df1.times >=start_dt)].drop_duplicates(subset=['execid','times'])
    df1.sort_index(inplace=True,ascending=[True])
    print df1[['avgprice', 'conId', 'execid', 'expiry','localSymbol',
               'price','qty', 'right', 'shares', 'side', 'strike', 'symbol', 'times']]

    store.close()
    end_func(client)


def print_tic_report(symbol,expiry,history=1):
    """
    history: number of rows to print from the historical ABT
    """
    client, log_analytics, globalconf = init_func()
    store = globalconf.open_ib_abt_strategy_tic(scenarioMode="N")
    dataframe = pd.DataFrame(dtype=float)
    node = store.get_node("/" + symbol + "/" + expiry)
    accountid = globalconf.get_accountid()

    df1_abt = store.select(node._v_pathname,
                               where=['subyacente==' + symbol, 'expiry==' + expiry, 'accountid==' + accountid])
    store.close()
    dataframe = dataframe.append(df1_abt.iloc[-int(history):])
    df1 = dataframe[['MargenNeto','comisiones','PnL','unrealizedPNLfromPrices',
                           'linea_mercado','DshortPosition','GammaPosition','ThetaPosition',
                           'VegaPosition','AD1PCT','AD1SD1D','MaxDsAD1PCT','MaxDsAD1SD1D','MaxDs',
                           'BearCallMaxDeltaShortPos','BullPutMaxDeltaShortPos','coste_base','dit','dte','impacto_cash']]


    df2 = dataframe[['BearCallMaxIVShort',
                       'BullPutMaxIVShort',
                       'ImplVolATM',
                       'marketValueGross',
                       'marketValuefromPrices',
                       'max_profit',
                       'portfolio_marketValue',
                       'orders_precio_bruto',
                       'portfolio_precio_neto',
                       'portfolio_unrealizedPNL',
                       'prc_ajuste_1SD15D_dn',
                       'prc_ajuste_1SD15D_up',
                       'prc_ajuste_1SD21D_dn',
                       'prc_ajuste_1SD21D_up',
                       'precio_close_anterior_subyacente',
                       'precio_last_subyacente',
                       'precio_undl_inicio_strat']]

    """
    u'AD1PCT', u'AD1SD1D', u'BearCallMaxDShortJL', u'BearCallMaxDeltaShort', u'BearCallMaxDeltaShortPos',
    u'BearCallMaxIVShort', u'BullPutMaxDShortJL', u'BullPutMaxDeltaShort', u'BullPutMaxDeltaShortPos',
    u'BullPutMaxIVShort', u'DTMaxdatost', u'DTMaxdatost_1', u'DToperaciones', u'DshortPosition', u'GammaPosition',
    u'ImplVolATM', u'MargenNeto', u'MaxDs', u'MaxDsAD1PCT', u'MaxDsAD1SD1D', u'PnL', u'Pts1SD1D',
    u'Pts1SD5D', u'ThetaPosition', u'VegaPosition', u'VegaThetaRatio', u'accountid', u'comisiones',
    u'coste_base', u'dit', u'dte', u'dte_inicio_estrategia', u'e_v', u'expiry', u'impacto_cash',
    u'ini_1SD15D', u'ini_1SD21D', u'iv_atm_inicio_strat', u'iv_subyacente', u'lastUndPrice',
    u'linea_mercado', u'marketValueGross', u'marketValuefromPrices', u'max_profit', u'multiplier',
    u'orders_precio_bruto', u'pnl_margin_ratio', u'portfolio', u'portfolio_marketValue',
    u'portfolio_precio_neto', u'portfolio_unrealizedPNL', u'prc_ajuste_1SD15D_dn',
    u'prc_ajuste_1SD15D_up', u'prc_ajuste_1SD21D_dn',
   u'prc_ajuste_1SD21D_up', u'precio_close_anterior_subyacente', u'precio_last_subyacente',
   u'precio_undl_inicio_strat', u'puntos_desde_last_close', u'retorno_subyacente', u'subyacente',
   u'thetaDeltaRatio', u'thetaGammaRatio', u'unrealizedPNLfromPrices'

    """

    df1 = df1.rename(columns={'MargenNeto': 'NMargin',
                              'comisiones': 'Comm',
                              'unrealizedPNLfromPrices': 'PnL_prc',
                              'DshortPosition': 'Ds',
                              'GammaPosition': 'Gamma',
                              'ThetaPosition': 'Theta',
                              'VegaPosition': 'Vega',
                              'BearCallMaxDeltaShortPos': 'BCMaxDs',
                              'BullPutMaxDeltaShortPos': 'BPMaxDs',
                              'coste_base': 'CB',
                              'linea_mercado': 'LM',
                              'impacto_cash': 'Cash'
                              })

    output = df1.to_string(formatters={
                                    'NMargin': '{:,.2f}'.format,
                                    'Comm': '{:,.2f}'.format,
                                    'CB': '{:,.2f}'.format,
                                    'dit': '{:,.2f}'.format,
                                    'dte': '{:,.2f}'.format,
                                    'Cash': '{:,.2f}'.format,
                                    'PnL': '{:,.2f}'.format,
                                    'PnL_prc': '{:,.2f}'.format,
                                    'LM' : '{:,.2f}'.format,
                                    'Ds': '{:,.2f}'.format,
                                    'Gamma': '{:,.2f}'.format,
                                    'Theta': '{:,.2f}'.format,
                                    'Vega': '{:,.2f}'.format,
                                    'AD1PCT': '{:,.2f}'.format,
                                    'AD1SD1D': '{:,.2f}'.format,
                                    'MaxDsAD1PCT': '{:,.2f}'.format,
                                    'MaxDsAD1SD1D': '{:,.2f}'.format,
                                    'MaxDs': '{:,.2f}'.format,
                                    'BCMaxDs': '{:,.2f}'.format,
                                    'BPMaxDs': '{:,.2f}'.format
                                })

    df2 = df2.rename(columns={'BearCallMaxIVShort': 'BCMaxIV',
                              'BullPutMaxIVShort': 'BPMaxIV',
                              'ImplVolATM': 'IVATM',
                              'marketValueGross': 'G_MV',
                              'marketValuefromPrices': 'MVprc',
                              'max_profit': 'MaxPrft',
                              'portfolio_marketValue': 'P_MV',
                              'orders_precio_bruto': 'OrdGPrc',
                              'portfolio_precio_neto': 'PortNPrc',
                              'portfolio_unrealizedPNL': 'PortPnL',
                              'prc_ajuste_1SD15D_dn': '1sd15ddn',
                              'prc_ajuste_1SD15D_up': '1sd15dup',
                              'prc_ajuste_1SD21D_dn': '1sd21ddn',
                              'prc_ajuste_1SD21D_up': '1sd21dup',
                              'precio_close_anterior_subyacente': 'pclose',
                              'precio_last_subyacente': 'plast',
                              'precio_undl_inicio_strat': 'pini'
                              })


    output2 = df2.to_string(formatters={
                                    'BCMaxIV': '{:,.2f}'.format,
                                    'BPMaxIV': '{:,.2f}'.format,
                                    'IVATM': '{:,.2f}'.format,
                                    'G_MV': '{:,.2f}'.format,
                                    'MVprc': '{:,.2f}'.format,
                                    'MaxPrft': '{:,.2f}'.format,
                                    'P_MV': '{:,.2f}'.format,
                                    'OrdGPrc': '{:,.2f}'.format,
                                    'PortNPrc': '{:,.2f}'.format,
                                    'PortPnL': '{:,.2f}'.format,
                                    '1sd15ddn': '{:,.2f}'.format,
                                    '1sd15dup': '{:,.2f}'.format,
                                    '1sd21ddn': '{:,.2f}'.format,
                                    '1sd21dup': '{:,.2f}'.format,
                                    'pclose': '{:,.2f}'.format,
                                    'plast': '{:,.2f}'.format,
                                    'pini': '{:,.2f}'.format
                                })


    print(output)
    print("_____________________________________________________________")
    print(output2)
    print("_____________________________________________________________")

    for i in range(0,dataframe.__len__()):
        parsed=json.loads(dataframe['portfolio'][i])
        #print json.dumps(parsed, indent=4, sort_keys=True)
        print dataframe.index[i]
        print pd.DataFrame(parsed)
    end_func(client)


def print_historical_underl(start_dt, end_dt, symbol):
    client, log_analytics, globalconf = init_func()
    # start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    # end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    start_dt1 = start_dt  # +" 0:00:00"
    end_dt1 = end_dt  # +" 23:59:59"
    df = ra.extrae_historical_underl(symbol,start_dt1,end_dt1)
    print df
    end_func(client)


def print_summary_underl(symbol):
    client, log_analytics, globalconf = init_func()
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


def print_historical_option(start_dt,end_dt,symbol,lst_right_strike,expiry,type):
    """
    Type should be bid, ask or trades
    lst_right_strike like "P2200.0,P2225.0,C2300.0"
    """
    client , log_analytics, globalconf = init_func()
    start_dt1 = start_dt #+" 0:00:00"
    end_dt1 = end_dt #+" 23:59:59"
    conversion = {'open_'+type: 'first', 'high_'+type: 'max', 'low_'+type: 'min', 'close_'+type: 'last' }
    dataframe = pd.DataFrame()
    for x in lst_right_strike.split(","):
        df=ra.extrae_historical_chain(start_dt1,end_dt1,symbol,x[1:],expiry,x[:1])
        df.index = pd.to_datetime(df.index, format="%Y%m%d  %H:%M:%S")
        df["date"] = df.index
        df[[u'close_'+type, u'high_'+type, u'open_'+type, u'low_'+type]] \
            = df[[u'close_'+type, u'high_'+type, u'open_'+type, u'low_'+type]].apply(pd.to_numeric)
        df = df.resample('1H', how=conversion).dropna()
        dataframe[x] = df['close_'+type]

    #columns = [x for x in df.columns if type in x]
    """
    [u'WAP_trades', u'close_trades', u'count_trades', u'currency', u'expiry', u'hasGaps_trades', u'high_trades',
     u'load_dttm', u'low_trades', u'multiplier', u'open_trades', u'reqId_trades', u'right', u'secType', u'strike',
     u'symbol', u'volume_trades', u'WAP_ask', u'close_ask', u'count_ask', u'hasGaps_ask', u'high_ask', u'low_ask',
     u'open_ask', u'reqId_ask', u'volume_ask', u'WAP_bid', u'close_bid', u'count_bid', u'hasGaps_bid', u'high_bid',
     u'low_bid', u'open_bid', u'reqId_bid', u'volume_bid']
     """
    print dataframe
    end_func(client)


def print_volatility_cone(symbol):
    """
    Print valotility cone for symbol given as argument
    """
    client , log_analytics, globalconf = init_func()
    dataframe = pd.DataFrame()
    # use VIX to get the mean 30d 60d 90d and so on from underlying_hist_ib h5


    print dataframe
    end_func(client)


def print_quasi_realtime_chain(val_dt,symbol,call_d_range,put_d_range,expiry,type):
    """
    Type should be bid, ask or trades
    Call delta _range 10,15 Put delta_range -15,-10
    """
    client , log_analytics, globalconf = init_func()
    start_dt1 = val_dt +" 23:59:59"
    valuation_dttm=dt.datetime.strptime(start_dt1, '%Y%m%d %H:%M:%S')
    end_dt1 = "20991231" +" 23:59:59"
    c_range = call_d_range.split(",")
    p_range = put_d_range.split(",")
    dataframe=pd.DataFrame()
    df = ra.extrae_options_chain(valuation_dttm, symbol, expiry, get_contract_details(symbol)["secType"])
    df=df.rename(columns=lambda x: str(x)[7:]) # remove prices_
    df=df[( (df['modelDelta'] >= float(c_range[0])/100.0 ) & (df['modelDelta'] <= float(c_range[1])/100.0 ) & ( df['right'] == "C" ) )
            |
          ((df['modelDelta'] >= float(p_range[0])/100.0) & (df['modelDelta'] <= float(p_range[1])/100.0 ) & (df['right'] == "P"))
         ]
    #for x in range(int(c_range[0]),int(c_range[1])):
        #df=ra.extrae_historical_chain(start_dt1,end_dt1,symbol,str(x),expiry,"C")
        #df['right']="C"
        #df['strike']=x
        #dataframe=dataframe.append(df[:1])
    df = df.ix[np.max(df.index)]
    df = df.set_index(['symbol','expiry','right'],append=True)
    dataframe=dataframe.append(df)
    columns=[u'strike',u'bidPrice',
             u'bidSize',u'askPrice',u'askSize',u'modelDelta',u'modelImpliedVol',u'lastUndPrice']
    """
    [u'WAP_trades', u'close_trades', u'count_trades', u'currency', u'expiry', u'hasGaps_trades', u'high_trades',
     u'load_dttm', u'low_trades', u'multiplier', u'open_trades', u'reqId_trades', u'right', u'secType', u'strike',
     u'symbol', u'volume_trades', u'WAP_ask', u'close_ask', u'count_ask', u'hasGaps_ask', u'high_ask', u'low_ask',
     u'open_ask', u'reqId_ask', u'volume_ask', u'WAP_bid', u'close_bid', u'count_bid', u'hasGaps_bid', u'high_bid',
     u'low_bid', u'open_bid', u'reqId_bid', u'volume_bid']
     """
    dte = (dt.datetime.strptime(expiry, '%Y%m%d') -  dt.datetime.now())
    print ("DTE = %d " % (dte.days) )
    print ("____________________________________________________________________________________________________")
    print dataframe[columns]
    end_func(client)

def print_ecalendar():
    """
    Print economic calendar for present trading week
    """
    day = dt.datetime.today()
    start = day - dt.timedelta(days=day.weekday())
    end = start + dt.timedelta(days=6)
    print(start)
    print(end)

    client , log_analytics, globalconf = init_func()
    dataframe = ra.read_biz_calendar(start_dttm=start, valuation_dttm=end)
    print dataframe
    end_func(client)

if __name__ == "__main__":
    #print_coppock_diario(start_dt="20160101", end_dt="20170303", symbol="SPX")
    #print_emas("SPX")
    #print_summary_underl("SPX")
    #print_fast_move("SPX")
    #print_tic_report(symbol="ES", expiry="20161118",history=3)
    print_account_delta(valuation_dt="2017-01-31-20")
    #print_volatity("SPY")
