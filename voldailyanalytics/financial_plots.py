"""@package docstring
"""


from volsetup import config
import datetime as dt
import pandas as pd
import numpy as np
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from time import sleep
from volibutils.sync_client import IBClient
import inspect
import time
from swigibpy import EPosixClientSocket, ExecutionFilter
from swigibpy import EWrapper
import swigibpy as sy
import sys
from volibutils.RequestOptionData import RequestOptionData
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.finance import candlestick2_ochl
from pandas import ExcelWriter

from math import pi
from bokeh.plotting import figure, show, output_file

from bokeh.plotting import figure, output_file, show
from bokeh.models import DatetimeTickFormatter

import bokeh


def plot_spy_vol():
    spy = web.DataReader('SPY',data_source='yahoo',start='3/14/2009',end='15/7/2016')
    spy['Log_ret'] = np.log(spy['Close'] / spy['Close'].shift(1))
    spy['Volatility'] = pd.rolling_std(spy['Log_ret'],window=252) * np.sqrt(252)

    # output to static HTML file
    output_file("spy_vol.html", title="SPY Price and volatility")

    # create a new plot with a title and axis labels
    p = figure(title="SPY", x_axis_label='t', y_axis_label='spy',plot_height=1000,plot_width=1600)

    # add a line renderer with legend and line thickness
    p.line(spy.index, spy['Volatility'], legend="Temp.", line_width=2)
    p.xaxis.formatter = DatetimeTickFormatter(formats=dict(
        hours=["%d %B %Y"],
        days=["%d %B %Y"],
        months=["%d %B %Y"],
        years=["%d %B %Y"],
    ))
    p.xaxis.major_label_orientation = np.pi / 4
    # show the results
    show(p)

def run_ohlc_plot(symbol,expiry=0):
    globalconf = config.GlobalConfig()
    f = globalconf.open_historical_store()
    path_h5 = "/" + str(symbol)
    if long(expiry) > 0:
        path_h5 = path_h5 + "/" + str(expiry)

    node = f.get_node(path_h5)
    if node:
        df = f.select(node._v_pathname)

    f.close()  # Close file

    df=df.reset_index().drop_duplicates(subset='date',keep='last')

    df["date"] = pd.to_datetime(df["date"])
    df.index = df["date"]
    conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    df1=df.resample('1D',how=conversion)
    df1=df1[['open','high','low','close','volume']]
    writer = ExcelWriter(datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/historical%Y_%m_%d_%H_%M_%S.xlsx'))
    df1.to_excel(writer,sheet_name=symbol+" 1D",)
    df.to_excel(writer, sheet_name=symbol)
    writer.save()


def kk_shit():
    output_file("candlestick.html", title="candlestick.py example")
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, toolbar_location="left")
    p.segment(df.date, df.high, df.date, df.low, color="black")
    p.title = "Candlestick"
    p.xaxis.major_label_orientation = pi/4
    p.grid.grid_line_alpha=0.3
    show(p)  # open a browser


if __name__=="__main__":
    #run_ohlc_plot(symbol="ES",expiry=20161216)
    run_ohlc_plot(symbol="SPY")