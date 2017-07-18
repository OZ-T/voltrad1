# coding: utf-8

"""Analytical methods useful for daily trading and strategy
"""

import datetime as dt
import json
import warnings

import numpy as np
import pandas as pd
from bokeh.embed import components
from bokeh.models.annotations import Title
from numpy import log, sqrt
from numpy.lib.stride_tricks import as_strided
from pylab import axhline, figure, legend, plot, show

import core.market_data_methods
from core import portfolio_and_account_data_methods as ra, market_data_methods as md
from core.market_data_methods import get_contract_details, read_graph_from_db, save_graph_to_db
from volsetup import config
from volsetup.logger import logger

warnings.filterwarnings("ignore")
HISTORY_LIMIT = 20


def init_func():
    """
    Initialization method.
    Initialize both global config object and display width for console printing
    """
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = globalconf.log
    client = None
    # this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    return client, log, globalconf


def end_func(client):
    """
    Clean up method to disconnect and free resources    
    """
    if client is not None:
        client.disconnect()


def windowed_view(x, window_size):
    """Creat a 2d windowed view of a 1d array.
    """
    #`x` must be a 1d numpy array.
    #`numpy.lib.stride_tricks.as_strided` is used to create the view.
    #The data is not copied.
    #Example:
    #>>> x = np.array([1, 2, 3, 4, 5, 6])
    #>>> windowed_view(x, 3)
    #array([[1, 2, 3],
    #       [2, 3, 4],
    #       [3, 4, 5],
    #       [4, 5, 6]])

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


import datetime
def coppock(globalconf, log_analytics, last_date, symbol="SPX",period="1D"):
    df = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist",symbol=symbol,expiry=None,
                                          last_date=last_date, num_days_back=100, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)
    df = COPP(df, 12, 6, 10)
    return df


def print_coppock_diario(symbol="SPX",period="1D"):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = coppock(globalconf, log_analytics, last_date, symbol, period)
    print(df.iloc[-HISTORY_LIMIT:]) # pinta los ultimos 30 dias del coppock
    end_func(client)


def graph_coppock(symbol="SPX",period="1D"):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = coppock(globalconf, log_analytics, last_date, symbol, period)
    from bokeh.plotting import figure
    df = df.reset_index()
    from math import pi
    mids = (df.open + df.close) / 2
    spans = abs(df.close - df.open)
    inc = df.close > df.open
    dec = df.open > df.close
    w = 12 * 60 * 60 * 1000  # half day in ms
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
    from bokeh.models import HoverTool, ColumnDataSource
    source = ColumnDataSource(df)
    TOOLS = "hover,save,pan,box_zoom,reset,wheel_zoom"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, toolbar_location="left",toolbar_sticky=False)
    p.toolbar_location="below"
    p.segment(df.date, df.high, df.date, df.low, color="black")
    p.rect(df.date[inc], mids[inc], w, spans[inc], fill_color="#D5E1DD", line_color="black")
    p.rect(df.date[dec], mids[dec], w, spans[dec], fill_color="#F2583E", line_color="black")

    p.title = Title(text="Daily Coppock (" + symbol + ")")
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3


    from bokeh.models.ranges import Range1d
    from bokeh.models import LinearAxis
    # Setting the second y axis range name and range
    p.y_range = Range1d(np.min(df.low - 0.01 *df.low), np.max(df.high + 0.01 *df.high))
    p.extra_y_ranges = {"foo": Range1d(start=np.min(df.Copp_10 - 0.01 * df.Copp_10), end=np.max(df.Copp_10 + 0.01 * df.Copp_10))}

    # Using the aditional y range named "foo" and "right" y axis here.
    p.line(df.date, df.Copp_10, y_range_name="foo")


    # Adding the second axis to the plot.
    p.add_layout(LinearAxis(y_range_name="foo"), 'right')

    p.select_one(HoverTool).tooltips = [
        ('date', '@date'),
        ("(open,high,low,close)", "(@open{(.00)},@high{(.00)},@low{(.00)},@close{(.00)})"),
    ]
    script, div = components(p)
    last_date1  = np.max(df.date).strftime("%Y%m%d")
    save_graph_to_db(globalconf=globalconf, log=log_analytics, script=script, div=div, symbol=symbol,
                     expiry="0", last_date=last_date1, num_days_back="-1", resample="NA",
                     estimator="COPPOCK", name="TREND")
    end_func(client)
    return p

def get_volatility_for_report(symbol,client,log_analytics,globalconf,last_date):
    window = 34.0
    year_days = 252.0
    length = 20
    df = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist", symbol=symbol, expiry=None,
                                          last_date=last_date, num_days_back=100, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)

    df = df.rename(columns={'close': symbol})
    df['HV'] = pd.rolling_std(df[symbol], window=int(window), min_periods=int(window)) * np.sqrt(window / year_days)
    df = df.drop(['high', 'open', 'low'], 1)

    vix = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                           db_type="underl_ib_hist", symbol="VIX", expiry=None,
                                           last_date=last_date, num_days_back=100, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol="VIX", last_date=last_date, last_record_stored=last_record_stored)
    vix = vix.append(df1)


    vix = vix.rename(columns={'close': 'vix'})['vix']
    vix_ewm = pd.Series(pd.Series.ewm(vix, span=length, min_periods=length,
                                      adjust=True, ignore_na=False).mean(), name='vix_ema' + str(length))
    df = df.join(vix_ewm)
    df = df.join(vix).fillna(method='ffill')
    vix_std = pd.rolling_std(df['vix'], window=int(window), min_periods=int(window))
    df['VIX_BB_2SD_UP'] = df['vix_ema' + str(length)] + 2 * vix_std
    df['VIX_BB_2SD_DOWN'] = df['vix_ema' + str(length)] - 2 * vix_std
    df['VIX_BB_1SD_UP'] = df['vix_ema' + str(length)] + vix_std
    df['VIX_BB_1SD_DOWN'] = df['vix_ema' + str(length)] - vix_std

    #df.VIX_BB_2SD_UP.sort_index(inplace=True)
    #df.VIX_BB_2SD_DOWN.sort_index(inplace=True)
    #df.VIX_BB_1SD_UP.sort_index(inplace=True)
    #df.VIX_BB_1SD_UP.sort_index(inplace=True)
    df.sort_index(inplace=True)
    try:
        df['ALERT_IV'] = np.where(((df['vix'] < df.VIX_BB_1SD_DOWN)), "LOW", "------")
        df['ALERT_IV'] = np.where(((df['vix'] > df.VIX_BB_1SD_UP)), "HIGH", df['ALERT_IV'])
        df['ALERT_IV'] = np.where((df['vix'] < df.VIX_BB_2SD_DOWN), "EXTREME_LOW", df['ALERT_IV'])
        df['ALERT_IV'] = np.where(((df['vix'] > df.VIX_BB_2SD_UP)), "EXTREME_HIGH", df['ALERT_IV'])
    except ValueError as e:
        print("ValueError raised [" + str(e) + "]  Missing rows for VIX needed to generate alerts ...")
    return df

def print_volatity(symbol):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = get_volatility_for_report(symbol,client,log_analytics,globalconf,last_date)

    print( df.iloc[-HISTORY_LIMIT:])
    end_func(client)

def graph_volatility(symbol):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = get_volatility_for_report(symbol,client,log_analytics,globalconf,last_date)
    df = df.reset_index()
    colors_list = ['orange', 'blue', 'pink', 'black', 'red', 'green']
    methods_list = ['x', 'diamond', 'x', 'square', 'inverted_triangle', 'inverted_triangle']
    line_dash_list = ['dotted', 'dotdash', 'dotted', 'solid', 'dashed', 'dashed']
    xs = [df.date, df.date, df.date, df.date, df.date, df.date]
    ys = [df.HV, df.vix, df.vix_ema20, df.VIX_BB_2SD_UP, df.VIX_BB_2SD_DOWN, df.VIX_BB_1SD_UP, df.VIX_BB_1SD_DOWN]
    legends_list = ["HV", "VIX", "vix_ema20", 'VIX_BB_2SD_UP', 'VIX_BB_2SD_DOWN', 'VIX_BB_1SD_UP','VIX_BB_1SD_DOWN']
    title = 'Volatility (' + symbol + ', daily from ' + last_date
    from bokeh.plotting import figure
    p = figure(x_axis_type="datetime",title=title, plot_width=700, plot_height=500, toolbar_sticky=False,
               x_axis_label="Dates", y_axis_label="Volatility", toolbar_location="below")
    legend_items = []

    for (colr, leg, x, y, method, line_dash) in zip(colors_list, legends_list, xs, ys, methods_list, line_dash_list):
        # call dynamically the method to plot line, circle etc...
        renderers = []
        if method:
            renderers.append(getattr(p, method)(x, y, color=colr, size=4))
        renderers.append(p.line(x, y, color=colr, line_dash=line_dash))
        legend_items.append((leg, renderers))
    # doesnt work: legend = Legend(location=(0, -30), items=legend_items)
    from bokeh.models.annotations import Legend
    legend = Legend(location=(0, -30), items=legend_items)
    p.add_layout(legend, 'right')
    last_date1  = np.max(df.date).strftime("%Y%m%d")
    script, div = components(p)
    save_graph_to_db(globalconf, log_analytics, script, div, symbol, "0", last_date1, 100, "1D", "Volatility", "TREND")

    end_func(client)
    return p

def graph_fast_move(symbol):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = get_fast_move_for_report(symbol,client, log_analytics, globalconf,last_date)
    df = df.reset_index()
    colors_list = ['black', 'blue', 'green']
    methods_list = ['x', 'diamond', 'square']
    line_dash_list = ['solid', 'dotted', 'dotdash']
    xs = [df.date, df.date, df.date]
    ys = [df[symbol], df.lowerBand, df.upperBand]
    legends_list = [symbol, "lowerBand", "upperBand"]
    title = 'Fast Move (' + symbol + ', daily from ' + last_date + ')'
    from bokeh.plotting import figure
    p = figure(x_axis_type="datetime",title=title, plot_width=700, plot_height=500, toolbar_sticky=False,
               x_axis_label="Dates", y_axis_label="Fast Move", toolbar_location="left")
    legend_items = []

    for (colr, leg, x, y, method, line_dash) in zip(colors_list, legends_list, xs, ys, methods_list, line_dash_list):
        # call dynamically the method to plot line, circle etc...
        renderers = []
        if method:
            renderers.append(getattr(p, method)(x, y, color=colr, size=4))
        renderers.append(p.line(x, y, color=colr, line_dash=line_dash))
        legend_items.append((leg, renderers))
    # doesn't work: legend = Legend(location=(0, -30), items=legend_items)
    from bokeh.models.annotations import Legend
    from sys import platform
    if "linux" in platform.lower():
        legend = Legend(location=(0, -30), items=legend_items)
    else:
        legend = Legend(location=(0, -30), legends=legend_items)

    from bokeh.models.ranges import Range1d
    from bokeh.models import LinearAxis
    # Setting the second y axis range name and range
    p.y_range = Range1d(np.min(df.lowerBand * 0.98), np.max(df.upperBand *1.02))
    p.extra_y_ranges = {"foo": Range1d(start=np.min(df['al1'] * 0.98), end=np.max(df['al1'] * 1.02))}

    # Using the aditional y range named "foo" and "right" y axis here.
    p.line(df.date, df['al1'], y_range_name="foo")
    # Adding the second axis to the plot.
    p.add_layout(LinearAxis(y_range_name="foo"), 'right')
    # TODO the following line breaks the code in Digital Ocean Server w Ubuntu & python 3
    # can't have 2 layouts in the same figure ??
    # p.add_layout(legend, 'right')
    last_date1  = np.max(df.date).strftime("%Y%m%d")
    script, div = components(p)
    save_graph_to_db(globalconf, log_analytics, script, div, symbol, "0", last_date1, 100, "1D", "FastMove", "TREND")
    print( df.iloc[-HISTORY_LIMIT:])
    end_func(client)
    return p


def get_fast_move_for_report(symbol,client, log_analytics, globalconf,last_date):
    length = 20.0
    num_dev_dn = -2.0
    num_dev_up = 2.0
    dbb_length = 120.0

    df = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist", symbol=symbol, expiry=None,
                                          last_date=last_date, num_days_back=250, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)

    df = df.drop(['high', 'open', 'low'], 1).rename(columns={'close': symbol})
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
    df['al1'] = - np.where(((df['atl'] > 0.0)), 0 , df['atl'] )

    # TODO: Finish this:
    """
    def c1 = if atl > 0 and atl < Parameter then 1 else 0;
    def c2 = if atl[1] > 0 and atl[2] > 0 and atl[3] > 0 and atl[4] > 0 and atl[5] > 0 and atl[6] > 0 and atl[7] > 0 and atl[8] > 0 and atl[9] > 0 and atl[10] > 0 then 1 else 0;
    def c3 = c1 + c2 + base;
    plot al2 = if c3 == 3 then atl else Double.Nan;
    al2.SetDefaultColor(Color.DARK_RED);
    al2.SetPaintingStrategy(PaintingStrategy.HISTOGRAM);
    """
    df = df.dropna()
    return df


def print_fast_move(symbol):

    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")

    df = get_fast_move_for_report(symbol,client, log_analytics, globalconf,last_date)

    print( df.iloc[-HISTORY_LIMIT:])
    end_func(client)


def print_emas(symbol="SPX"):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    n = 50
    df = get_emas(last_date, log_analytics, globalconf, symbol=symbol, n=n)
    output = df.iloc[-HISTORY_LIMIT:].to_string(formatters={
                                    'lower_wk_iv': '{:,.2f}'.format,
                                    'upper_wk_iv': '{:,.2f}'.format,
                                    'lower_mo_iv': '{:,.2f}'.format,
                                    'upper_mo_iv': '{:,.2f}'.format,
                                    'EMA_' + str(n): '{:,.2f}'.format
                                })
    print(output)
    end_func(client)

def get_emas(last_date, log_analytics, globalconf, symbol="SPX", n=50):
    df = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist",symbol=symbol,expiry=None,
                                          last_date=last_date, num_days_back=500, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)


    ema50 = pd.Series(pd.Series.ewm(df['close'], span = n, min_periods = n,
                     adjust=True, ignore_na=False).mean(), name = 'EMA_' + str(n))
    df = df.join(ema50)
    df['RSK_EMA50'] = np.where(df['close'] > df['EMA_' + str(n)], "-----", "ALERT")
    # sacar los canales de IV del historico del VIX
    vix = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist",symbol="VIX",expiry=None,
                                          last_date=last_date, num_days_back=100, resample="1D")


    last_record_stored = np.max(vix.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol="VIX", last_date=last_date, last_record_stored=last_record_stored)
    vix = vix.append(df1)

    vix = vix.rename(columns={'close': 'vix'})['vix'].dropna()
    df = df.join(vix)
    df['lower_wk_iv']=df['close'].shift(5) * (1 - (df['vix'].shift(5)*0.01 / np.sqrt(252/5)) )
    df['upper_wk_iv'] = df['close'].shift(5) * (1 + (df['vix'].shift(5)*0.01 / np.sqrt(252/5)))
    df['lower_mo_iv']= df['close'].shift(34) * (1 - (df['vix'].shift(34)*0.01 / np.sqrt(252/34)))
    df['upper_mo_iv'] = df['close'].shift(34) * (1 + (df['vix'].shift(34)*0.01 / np.sqrt(252/34)))

    df['CANAL_IV_WK'] = np.where( (df['close'] < df['upper_wk_iv']) &
                                  (df['close'] > df['lower_wk_iv']), "-----", "ALERT")
    df['CANAL_IV_MO'] = np.where( (df['close'] < df['upper_mo_iv']) &
                                   (df['close'] > df['lower_mo_iv']), "-----", "ALERT")
    return df


def graph_emas(symbol="SPX"):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    n = 50
    df = get_emas(last_date, log_analytics, globalconf, symbol=symbol, n=n).iloc[-50:]
    from bokeh.plotting import figure
    df = df.reset_index()
    from bokeh.models import ColumnDataSource
    source = ColumnDataSource(df)
    TOOLS = "hover,save,pan,box_zoom,reset,wheel_zoom"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, toolbar_location="left",toolbar_sticky=False)

    from math import pi
    mids = (df.open + df.close) / 2
    spans = abs(df.close - df.open)
    inc = df.close > df.open
    dec = df.open > df.close
    w = 12 * 60 * 60 * 1000  # half day in ms

    p.toolbar_location="below"
    p.segment(df.date, df.high, df.date, df.low, color="black")
    p.rect(df.date[inc], mids[inc], w, spans[inc], fill_color="#D5E1DD", line_color="black")
    p.rect(df.date[dec], mids[dec], w, spans[dec], fill_color="#F2583E", line_color="black")

    p.title = Title(text="EMA(50) (" + symbol + ")")
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3
    p.line(df.date, df.EMA_50)
    p.line(df.date, df.lower_wk_iv)
    p.line(df.date, df.upper_wk_iv)

    p.line(df.date, df.lower_mo_iv)
    p.line(df.date, df.upper_mo_iv)

    last_date1  = np.max(df.date).strftime("%Y%m%d")
    script, div = components(p)
    save_graph_to_db(globalconf=globalconf, log=log_analytics, script=script, div=div, symbol=symbol,
                     expiry="0", last_date=last_date1, num_days_back="500", resample="NA",
                     estimator="EMA50", name="TREND")
    end_func(client)
    return p


def graph_volatility_cone(symbol):
    """
    Old version using pyplot DON't use    
    Print valotility cone for symbol given as argument
    """
    client , log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist",symbol=symbol,expiry=None,
                                          last_date=last_date, num_days_back=500, resample="1D")


    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)


    df=df.pct_change(1).rename(columns={'close': symbol})[symbol].dropna()
    df=df.reset_index()

    # use VIX to get the mean 30d 60d 90d and so on from underlying_hist_ib h5
    vix = md.read_market_data_from_sqllite(globalconf=globalconf, log=log_analytics,
                                          db_type="underl_ib_hist",symbol="VIX",expiry=None,
                                          last_date=last_date, num_days_back=500, resample="1D")

    vix = vix.reset_index()
    df['vix'] = vix.dropna().rename(columns={'close': 'vix'})['vix']
    lst_exp = [30,60,90,120]
    for length in lst_exp:
        vix_ewm = pd.Series(pd.Series.ewm(df['vix'], span = length, min_periods = length,
                         adjust=True, ignore_na=False).mean(), name = 'vix_ema' + str(length))
        df = df.join(vix_ewm)
    df = df.fillna(method='ffill').dropna()
    close_data = df[symbol][-300:].values
    imp_vol_data_30d = df['vix_ema30'][-300:].values
    imp_vol_data_360d = df['vix_ema90'][-300:].values

    days_to_expiry = [20, 60, 120]

    lower = []
    means = []
    upper = []

    for expiry in days_to_expiry:
        print (expiry)
        np_lower, np_mean, np_upper = calc_sigmas(expiry, close_data)
        lower.append(np_lower)
        means.append(np_mean)
        upper.append(np_upper)

    historical_sigma_20d = calc_daily_sigma(20, close_data)
    historical_sigma_180d = calc_daily_sigma(180, close_data)

    limit = max(days_to_expiry)
    x = range(0, limit)

    fig = figure()
    ax1 = fig.add_subplot(3, 1, 1)
    plot(days_to_expiry, lower, color='red', label='Lower')
    plot(days_to_expiry, means, color='grey', label='Average')
    plot(days_to_expiry, upper, color='blue', label='Upper')
    axhline(lower[0], linestyle='dashed', color='red')
    axhline(lower[-1], linestyle='dashed', color='red')
    axhline(upper[0], linestyle='dashed', color='blue')
    axhline(upper[-1], linestyle='dashed', color='blue')
    ax1.set_title('Volatility Cones')
    legend(bbox_to_anchor=(1., 1.), loc=2)

    ax2 = fig.add_subplot(3, 1, 2)
    plot(x, historical_sigma_20d[-limit:], label='Historical')
    plot(x, imp_vol_data_30d[-limit:], label='Implied')
    axhline(lower[0], linestyle='dashed', color='red')
    axhline(upper[0], linestyle='dashed', color='blue')
    ax2.set_title('20 Day Volatilities')
    ax2.set_xlim(ax1.get_xlim())
    ax2.set_ylim(ax1.get_ylim())
    legend(bbox_to_anchor=(1., 1.), loc=2)

    # We only want to plot implied vol. where we have a value for historical
    #imp_vol_data_360d[np.where(np.isnan(historical_sigma_180d))] = np.nan

    ax3 = fig.add_subplot(3, 1, 3)
    plot(x, historical_sigma_180d[-limit:], label='Historical')
    plot(x, imp_vol_data_360d[-limit:], label='Implied')
    axhline(lower[-1], linestyle='dashed', color='red')
    axhline(upper[-1], linestyle='dashed', color='blue')
    ax3.set_title('180 Day Volatilities')
    ax3.set_xlim(ax1.get_xlim())
    ax3.set_ylim(ax1.get_ylim())
    legend(bbox_to_anchor=(1., 1.), loc=2)
    show()

    print( df )
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
        temp_portfolio['CB'] = temp_portfolio['portfolio_averageCost'] / temp_portfolio['portfolio_multiplier'] * \
                                    np.sign(temp_portfolio['portfolio_position'])
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
                                        'CB': '{:,.2f}'.format,
                                        'Cost': '{:,.2f}'.format,
                                        'MktVal': '{:,.2f}'.format,
                                        'mult': '{:,.0f}'.format,
                                        'pos': '{:,.0f}'.format,
                                        'str': '{:,.2f}'.format,
                                        'NPrc': '{:,.2f}'.format,
                                        'PnL': '{:,.2f}'.format
                                    })

        print("___PORTFOLIO_______________________________________________________________")
        print( output3)
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
    print( df1[['avgprice', 'conId', 'execid', 'expiry','localSymbol',
               'price','qty', 'right', 'shares', 'side', 'strike', 'symbol', 'times']])

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
        print( dataframe.index[i])
        print( pd.DataFrame(parsed))
    end_func(client)


def print_historical_underl(start_dt, end_dt, symbol):
    client, log_analytics, globalconf = init_func()
    # start_dt1 = dt.datetime.strptime(start_dt, '%Y%m%d')
    # end_dt1 = dt.datetime.strptime(end_dt, '%Y%m%d')
    start_dt1 = start_dt  # +" 0:00:00"
    end_dt1 = end_dt  # +" 23:59:59"
    df = ra.extrae_historical_underl(symbol,start_dt1,end_dt1)
    print( df)
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
    print( dataframe)
    end_func(client)



def calc_sigmas(N, X, period=20):
    start = 0
    end = N

    results = []

    while end <= len(X):
        sigma = calc_sigma(N, X[start:end])
        results.append(sigma)
        # print('N: {}, sigma: {}'.format(N, sigma))
        start += period
        end += period

    sigmas = np.array(results)
    mean = sigmas.mean()

    # Uncomment the following three lines to use z scores instead of minimum
    # and maximum sigma values
    #
    # z_score=2.0
    # interval = sigmas.std() * z_score
    # return mean - interval, mean, mean + interval
    #
    return sigmas.min(), mean, sigmas.max()


def calc_daily_sigma(lookback, data):
    results = np.zeros(len(data))
    start = 0
    end = lookback
    results[start:end] = np.nan
    while end < len(data):
        results[end] = calc_sigma(lookback, data[start:end])
        start += 1
        end += 1
    return results


def calc_sigma(N, X):
    return sqrt(sum((X)**2) / float(N - 1)) * sqrt(252.0)


def calculate_log_returns(pnl):
    lagged_pnl = lag(pnl)
    returns = log(pnl / lagged_pnl)

    # All values prior to our position opening in pnl will have a
    # value of inf. This is due to division by 0.0
    returns[np.isinf(returns)] = 0.
    # Additionally, any values of 0 / 0 will produce NaN
    returns[np.isnan(returns)] = 0.
    return returns


def lag(data):
    lagged = np.roll(data, 1)
    lagged[0] = 0.
    return lagged





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
    df = core.market_data_methods.extrae_options_chain(valuation_dttm, symbol, expiry, get_contract_details(symbol)["secType"])
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
    print (dataframe[columns])
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
    dataframe = core.market_data_methods.read_biz_calendar(start_dttm=start, valuation_dttm=end)
    print (dataframe)
    end_func(client)

if __name__ == "__main__":
    #print_coppock_diario(start_dt="20160101", end_dt="20170303", symbol="SPX")
    #print_emas("SPX")
    #print_summary_underl("SPX")
    #print_fast_move("SPX")
    #print_tic_report(symbol="SPY", expiry="20170317",history=3)
    #print_account_delta(valuation_dt="2017-01-31-20")
    #print_volatity("SPY")
    #print_volatility_cone(symbol="SPY")
    pass
    today = dt.date.today()
    last_date1 = today.strftime('%Y%m%d')
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = globalconf.log

    div, script = read_graph_from_db(globalconf=globalconf, log=log, symbol="SPX",
                                     last_date=last_date1, estimator="Coppock", name="TREND")

    print((div, script))
