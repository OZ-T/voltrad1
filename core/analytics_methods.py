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

import persist.h5_methods
import persist.sqlite_methods
from persist import sqlite_methods as md, portfolio_and_account_data_methods as ra
from persist.sqlite_methods import read_graph_from_db, save_graph_to_db
from core.logger import logger

warnings.filterwarnings("ignore")
HISTORY_LIMIT = 20

from core import config
globalconf = config.GlobalConfig()
log = globalconf.log


def sma(period,df,symbol):
    df[symbol+'_SMA_'+str(period)] = df[symbol].rolling(period).mean()


def drawdown(period,df,symbol):    
    df[symbol+'_Drawdown_'+str(period)] = (df[symbol] - df[symbol].rolling(window=period, min_periods=1).max() ) / df[symbol].rolling(window=period, min_periods=1).max()
    

def ReturnsTD(df,symbol):    
    df['DateBoY'] = [ pd.to_datetime(r.date()) for r in df.index - pd.offsets.BYearBegin()]
    df['DateBoM'] = [ pd.to_datetime(r.date()) for r in df.index - pd.offsets.MonthBegin()]
    #df['DateBoW'] = [ pd.to_datetime(r.date()) for r in df.index - pd.offsets.BYearBegin() ???]    
    try:
        df[symbol+'_BoY'] = [ df.loc[df.index >= x,symbol].first('1D').item() for x in df['DateBoY'] ]
        df[symbol+'_BoM'] = [ df.loc[df.index >= x,symbol].first('1D').item() for x in df['DateBoM'] ]
        #df[symbol+'_BoW'] = [ df.loc[df.index >= x,symbol].first('1D').item() for x in df['DateBoW'] ]
    except KeyError:
        df[symbol+'_BoY'] = df[symbol]
        df[symbol+'_BoM'] = df[symbol]
    df[symbol+'_Return_BoY'] = (df[symbol] -  df[symbol+'_BoY']  ) / df[symbol+'_BoY']
    df[symbol+'_Return_BoM'] = (df[symbol] -  df[symbol+'_BoM']  ) / df[symbol+'_BoM']
    #df[symbol+'_Return_BoW'] = (df[symbol] -  df[symbol+'_BoW']  ) / df[symbol+'_BoW']
    #df.drop('DateBoY',axis=1,inplace=True)
    #df.drop('DateBoM',axis=1,inplace=True)
    #df.drop('DateBoW',axis=1,inplace=True)
    #df.drop(symbol+'_BoY',axis=1,inplace=True)
    #df.drop(symbol+'_BoM',axis=1,inplace=True)
    #df.drop(symbol+'_BoW',axis=1,inplace=True)



def sto_cycles_probability(price,deep):
    """
    Indicador StoCyclesProb: Capacidad de predicción en un mercado aleatorio. Basado en los cálculos de entropía de Shannon

        Deep = {default dos, uno, tres}

    """
    displace = 0
    Periodo =255

    """     #Calculo del nivel 1
        A = if close > close[1] then 1 else 0;
        B = if close < close[1] then 1 else 0;
        def Alcista = A + A[1] + A[2] + A[3] + A[4] + A[5] + A[6] + A[7] + A[8] + A[9];
        def Bajista = B + B[1] + B[2] + B[3] + B[4] + B[5] + B[6] + B[7] + B[8] + B[9];
        def ProbAlcista = Alcista / 10;
        def ProbBajista = Bajista / 10;
        def EntrProbAlcista = ProbAlcista * Lg(ProbAlcista) / Lg(2);
        def EntrProbBajista = ProbBajista * Lg(ProbBajista) / Lg(2);
        def SumaEntr = -(EntrProbAlcista + EntrProbBajista) * 100;
        def PrediccionN1 = 100 - SumaEntr;
        #Calculo del nivel 2
        def AA = if close[1] > close[2] and close > close[1] then 1 else 0;
        def AB = if close[1] > close[2] and close < close[1] then 1 else 0;
        def BB = if close[1] < close[2] and close < close[1] then 1 else 0;
        def BA = if close[1] < close[2] and close > close[1] then 1 else 0;
        def AlAl = AA + AA[1] + AA[2] + AA[3] + AA[4] + AA[5] + AA[6] + AA[7] + AA[8] + AA[9] + AA[10] + AA[11] + AA[12] + AA[13] + AA[14] + AA[15] + AA[16] + AA[17] + AA[18] + AA[19];
        def AlBa = AB + AB[1] + AB[2] + AB[3] + AB[4] + AB[5] + AB[6] + AB[7] + AB[8] + AB[9] + AB[10] + AB[11] + AB[12] + AB[13] + AB[14] + AB[15] + AB[16] + AB[17] + AB[18] + AB[19];
        def BaBa = BB + BB[1] + BB[2] + BB[3] + BB[4] + BB[5] + BB[6] + BB[7] + BB[8] + BB[9] + BB[10] + BB[11] + BB[12] + BB[13] + BB[14] + BB[15] + BB[16] + BB[17] + BB[18] + BB[19];
        def BaAl = BA + BA[1] + BA[2] + BA[3] + BA[4] + BA[5] + BA[6] + BA[7] + BA[8] + BA[9] + BA[10] + BA[11] + BA[12] + BA[13] + BA[14] + BA[15] + BA[16] + BA[17] + BA[18] + BA[19];
        def ProbAlAl = if AlAl == 0 then 0.00001 else AlAl / 20;
        def ProbAlBa = if AlBa == 0 then 0.00001 else AlBa / 20;
        def ProbBaBa = if BaBa == 0 then 0.00001 else BaBa / 20;
        def ProbBaAl = if BaAl == 0 then 0.00001 else BaAl / 20;
        def EntrProbAlAl = ProbAlAl * Lg(ProbAlAl) / Lg(2);
        def EntrProbAlBa = ProbAlBa * Lg(ProbAlBa) / Lg(2);
        def EntrProbBaBa = ProbBaBa * Lg(ProbBaBa) / Lg(2);
        def EntrProbBaAl = ProbBaAl * Lg(ProbBaAl) / Lg(2);
        def SumaEntrN2 = -(EntrProbAlAl + EntrProbAlBa + EntrProbBaBa + EntrProbBaAl) / 2 * 100;
        def PrediccionN2 = 100 - SumaEntrN2;
        #Calculo del nivel 3
        def AAA = if close[2] > close[3] and close[1] > close[2] and close > close[1] then 1 else 0;
        def AAB = if close[2] > close[3] and close[1] > close[2] and close < close[1] then 1 else 0;
        def ABA = if close[2] > close[3] and close[1] < close[2] and close > close[1] then 1 else 0;
        def ABB = if close[2] > close[3] and close[1] < close[2] and close < close[1] then 1 else 0;
        def BAA = if close[2] < close[3] and close[1] > close[2] and close > close[1] then 1 else 0;
        def BAB = if close[2] < close[3] and close[1] > close[2] and close < close[1] then 1 else 0;
        def BBA = if close[2] < close[3] and close[1] < close[2] and close > close[1] then 1 else 0;
        def BBB = if close[2] < close[3] and close[1] < close[2] and close < close[1] then 1 else 0;
        def AlAlAl = AAA + AAA[1] + AAA[2] + AAA[3] + AAA[4] + AAA[5] + AAA[6] + AAA[7] + AAA[8] + AAA[9] + AAA[10] + AAA[11] + AAA[12] + AAA[13] + AAA[14] + AAA[15] + AAA[16] + AAA[17] + AAA[18] + AAA[19] + AAA[20] + AAA[21] + AAA[22] + AAA[23] + AAA[24] + AAA[25] + AAA[26] + AAA[27] + AAA[28] + AAA[29] + AAA[30] + AAA[31] + AAA[32] + AAA[33] + AAA[34] + AAA[35] + AAA[36] + AAA[37] + AAA[38] + AAA[39];
        def AlAlBa = AAB + AAB[1] + AAB[2] + AAB[3] + AAB[4] + AAB[5] + AAB[6] + AAB[7] + AAB[8] + AAB[9] + AAB[10] + AAB[11] + AAB[12] + AAB[13] + AAB[14] + AAB[15] + AAB[16] + AAB[17] + AAB[18] + AAB[19] + AAB[20] + AAB[21] + AAB[22] + AAB[23] + AAB[24] + AAB[25] + AAB[26] + AAB[27] + AAB[28] + AAB[29] + AAB[30] + AAB[31] + AAB[32] + AAB[33] + AAB[34] + AAB[35] + AAB[36] + AAB[37] + AAB[38] + AAB[39];
        def AlBaAl = ABA + ABA[1] + ABA[2] + ABA[3] + ABA[4] + ABA[5] + ABA[6] + ABA[7] + ABA[8] + ABA[9] + ABA[10] + ABA[11] + ABA[12] + ABA[13] + ABA[14] + ABA[15] + ABA[16] + ABA[17] + ABA[18] + ABA[19] + ABA[20] + ABA[21] + ABA[22] + ABA[23] + ABA[24] + ABA[25] + ABA[26] + ABA[27] + ABA[28] + ABA[29] + ABA[30] + ABA[31] + ABA[32] + ABA[33] + ABA[34] + ABA[35] + ABA[36] + ABA[37] + ABA[38] + ABA[39];
        def AlBaBa = ABB + ABB[1] + ABB[2] + ABB[3] + ABB[4] + ABB[5] + ABB[6] + ABB[7] + ABB[8] + ABB[9] + ABB[10] + ABB[11] + ABB[12] + ABB[13] + ABB[14] + ABB[15] + ABB[16] + ABB[17] + ABB[18] + ABB[19] + ABB[20] + ABB[21] + ABB[22] + ABB[23] + ABB[24] + ABB[25] + ABB[26] + ABB[27] + ABB[28] + ABB[29] + ABB[30] + ABB[31] + ABB[32] + ABB[33] + ABB[34] + ABB[35] + ABB[36] + ABB[37] + ABB[38] + ABB[39];
        def BaAlAl = BAA + BAA[1] + BAA[2] + BAA[3] + BAA[4] + BAA[5] + BAA[6] + BAA[7] + BAA[8] + BAA[9] + BAA[10] + BAA[11] + BAA[12] + BAA[13] + BAA[14] + BAA[15] + BAA[16] + BAA[17] + BAA[18] + BAA[19] + BAA[20] + BAA[21] + BAA[22] + BAA[23] + BAA[24] + BAA[25] + BAA[26] + BAA[27] + BAA[28] + BAA[29] + BAA[30] + BAA[31] + BAA[32] + BAA[33] + BAA[34] + BAA[35] + BAA[36] + BAA[37] + BAA[38] + BAA[39];
        def BaAlBa = BAB + BAB[1] + BAB[2] + BAB[3] + BAB[4] + BAB[5] + BAB[6] + BAB[7] + BAB[8] + BAB[9] + BAB[10] + BAB[11] + BAB[12] + BAB[13] + BAB[14] + BAB[15] + BAB[16] + BAB[17] + BAB[18] + BAB[19] + BAB[20] + BAB[21] + BAB[22] + BAB[23] + BAB[24] + BAB[25] + BAB[26] + BAB[27] + BAB[28] + BAB[29] + BAB[30] + BAB[31] + BAB[32] + BAB[33] + BAB[34] + BAB[35] + BAB[36] + BAB[37] + BAB[38] + BAB[39];
        def BaBaAl = BBA + BBA[1] + BBA[2] + BBA[3] + BBA[4] + BBA[5] + BBA[6] + BBA[7] + BBA[8] + BBA[9] + BBA[10] + BBA[11] + BBA[12] + BBA[13] + BBA[14] + BBA[15] + BBA[16] + BBA[17] + BBA[18] + BBA[19] + BBA[20] + BBA[21] + BBA[22] + BBA[23] + BBA[24] + BBA[25] + BBA[26] + BBA[27] + BBA[28] + BBA[29] + BBA[30] + BBA[31] + BBA[32] + BBA[33] + BBA[34] + BBA[35] + BBA[36] + BBA[37] + BBA[38] + BBA[39];
        def BaBaBa = BBB + BBB[1] + BBB[2] + BBB[3] + BBB[4] + BBB[5] + BBB[6] + BBB[7] + BBB[8] + BBB[9] + BBB[10] + BBB[11] + BBB[12] + BBB[13] + BBB[14] + BBB[15] + BBB[16] + BBB[17] + BBB[18] + BBB[19] + BBB[20] + BBB[21] + BBB[22] + BBB[23] + BBB[24] + BBB[25] + BBB[26] + BBB[27] + BBB[28] + BBB[29] + BBB[30] + BBB[31] + BBB[32] + BBB[33] + BBB[34] + BBB[35] + BBB[36] + BBB[37] + BBB[38] + BBB[39];
        def ProbAlAlAl = if AlAlAl == 0 then 0.00001 else AlAlAl / 40;
        def ProbAlAlBa = if AlAlBa == 0 then 0.00001 else AlAlBa / 40;
        def ProbAlBaAl = if AlBaAl == 0 then 0.00001 else AlBaAl / 40;
        def ProbAlBaBa = if AlBaBa == 0 then 0.00001 else AlBaBa / 40;
        def ProbBaAlAl = if BaAlAl == 0 then 0.00001 else BaAlAl / 40;
        def ProbBaAlBa = if BaAlBa == 0 then 0.00001 else BaAlBa / 40;
        def ProbBaBaAl = if BaBaAl == 0 then 0.00001 else BaBaAl / 40;
        def ProbBaBaBa = if BaBaBa == 0 then 0.00001 else BaBaBa / 40;
        def EntrProbAlAlAl = ProbAlAlAl * Lg(ProbAlAlAl) / Lg(2);
        def EntrProbAlAlBa = ProbAlAlBa * Lg(ProbAlAlBa) / Lg(2);
        def EntrProbAlBaAl = ProbAlBaAl * Lg(ProbAlBaAl) / Lg(2);
        def EntrProbAlBaBa = ProbAlBaBa * Lg(ProbAlBaBa) / Lg(2);
        def EntrProbBaAlAl = ProbBaAlAl * Lg(ProbBaAlAl) / Lg(2);
        def EntrProbBaAlBa = ProbBaAlBa * Lg(ProbBaAlBa) / Lg(2);
        def EntrProbBaBaAl = ProbBaBaAl * Lg(ProbBaBaAl) / Lg(2);
        def EntrProbBaBaBa = ProbBaBaBa * Lg(ProbBaBaBa) / Lg(2);
        def SumaEntrN3 = -(EntrProbAlAlAl + EntrProbAlAlBa + EntrProbAlBaAl + EntrProbAlBaBa + EntrProbBaAlAl + EntrProbBaAlBa + EntrProbBaBaAl + EntrProbBaBaBa) / 3 * 100;
        def PrediccionN3 = 100 - SumaEntrN3;
        switch (Deep) {
        case dos:
            Prediccion = PrediccionN1;
        case uno:
            Prediccion = PrediccionN2;
        case tres:
            Prediccion = PrediccionN3;
        }
        def Predi=Prediccion;
        def Spread = (Predi - Average(Predi, Periodo));
        def sDev = StDev(data = Spread, Periodo);
        plot SpreadPred=Spread/sDev;
        SpreadPred.SetDefaultColor(Color.BLACK);
        SpreadPred.SetLineWeight(2);
        SpreadPred.SetPaintingStrategy(PaintingStrategy.LINE);
        SpreadPred.SetStyle(Curve.SHORT_DASH);
        plot UpperBand = 2;
        UpperBand.SetDefaultColor(GetColor(7));
        UpperBand.SetStyle(Curve.MEDIUM_DASH);
        UpperBand.SetLineWeight(3);
        plot UpperBandInt = 1;
        UpperBandInt.SetDefaultColor(GetColor(7));
        UpperBandInt.SetStyle(Curve.SHORT_DASH);
        UpperBandInt.SetLineWeight(2);
        plot CeroBand = 0;
        CeroBand.SetDefaultColor(GetColor(7));
        CeroBand.SetStyle(Curve.FIRM);
        CeroBand.SetLineWeight(1);
        plot LowerBandInt = - 1;
        LowerBandInt.SetDefaultColor(GetColor(7));
        LowerBandInt.SetStyle(Curve.SHORT_DASH);
        LowerBandInt.SetLineWeight(2);
        plot LowerBand = -2;
        LowerBand.SetDefaultColor(GetColor(7));
        LowerBand.SetStyle(Curve.MEDIUM_DASH);
        LowerBand.SetLineWeight(3);
        AddLabel(yes, “AV.PRED (SD) = "  +AsText(Average(Predi,Periodo), NumberFormat.TWO_DECIMAL_PLACES)+ " ("+AsText(Sdev, NumberFormat.TWO_DECIMAL_PLACES)+") "+ " Highest = "+AsText(Highest(Predi,Periodo), NumberFormat.TWO_DECIMAL_PLACES)+ " ("+AsText(Highest(Spread/Sdev,Periodo), NumberFormat.TWO_DECIMAL_PLACES)+ ") "+ " Lowest = "+AsText(Lowest(Predi,Periodo), NumberFormat.TWO_DECIMAL_PLACES)+ " ("+AsText(Lowest(Spread/Sdev,Periodo), NumberFormat.TWO_DECIMAL_PLACES)+ ") ", if SpreadPred >= 0 then CreateColor ( 43, 141, 80 ) else Color.RED);
    """
    return None

def init_func():
    """
    Initialization method.
    Initialize both global config object and display width for console printing
    """
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
def coppock(last_date, symbol="SPX",period="1D"):
    df = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol=symbol, expiry=None, last_date=last_date,
                                          num_days_back=100, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
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
    df['date'] = df.index
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
    save_graph_to_db(script=script, div=div, symbol=symbol, expiry="0", last_date=last_date1, num_days_back="-1",
                     resample="NA", estimator="COPPOCK", name="TREND")
    end_func(client)
    return p

def get_volatility_for_report(symbol,client,log_analytics,globalconf,last_date):
    window = 34.0
    year_days = 252.0
    length = 20
    df = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol=symbol, expiry=None, last_date=last_date,
                                          num_days_back=100, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)

    df = df.rename(columns={'close': symbol})
    df['HV'] = pd.rolling_std(df[symbol], window=int(window), min_periods=int(window)) * np.sqrt(window / year_days)
    df = df.drop(['high', 'open', 'low'], 1)

    vix = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol="VIX", expiry=None, last_date=last_date,
                                           num_days_back=100, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol="VIX", last_date=last_date, last_record_stored=last_record_stored)
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
    df['date'] = df.index
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
    save_graph_to_db(script, div, symbol, "0", last_date1, 100, "1D", "Volatility", "TREND")

    end_func(client)
    return p

def graph_fast_move(symbol):
    client, log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = get_fast_move_for_report(symbol,client, log_analytics, globalconf,last_date)
    df['date'] = df.index
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
    save_graph_to_db(script, div, symbol, "0", last_date1, 100, "1D", "FastMove", "TREND")
    print( df.iloc[-HISTORY_LIMIT:])
    end_func(client)
    return p


def get_fast_move_for_report(symbol,client, log_analytics, globalconf,last_date):
    length = 20.0
    num_dev_dn = -2.0
    num_dev_up = 2.0
    dbb_length = 120.0

    df = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol=symbol, expiry=None, last_date=last_date,
                                          num_days_back=250, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
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
    df = df.dropna(subset=[symbol])
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
    df = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol=symbol, expiry=None, last_date=last_date,
                                          num_days_back=500, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)


    ema50 = pd.Series(pd.Series.ewm(df['close'], span = n, min_periods = n,
                     adjust=True, ignore_na=False).mean(), name = 'EMA_' + str(n))
    df = df.join(ema50)
    df['RSK_EMA50'] = np.where(df['close'] > df['EMA_' + str(n)], "-----", "ALERT")
    # sacar los canales de IV del historico del VIX
    vix = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol="VIX", expiry=None, last_date=last_date,
                                           num_days_back=100, resample="1D")


    last_record_stored = np.max(vix.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol="VIX", last_date=last_date, last_record_stored=last_record_stored)
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
    df['date'] = df.index
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
    save_graph_to_db(script=script, div=div, symbol=symbol, expiry="0", last_date=last_date1, num_days_back="500",
                     resample="NA", estimator="EMA50", name="TREND")
    end_func(client)
    return p


def graph_volatility_cone(symbol):
    """
    Old version using pyplot DON't use    
    Print valotility cone for symbol given as argument
    """
    client , log_analytics, globalconf = init_func()
    last_date = datetime.datetime.today().strftime("%Y%m%d")
    df = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol=symbol, expiry=None, last_date=last_date,
                                          num_days_back=500, resample="1D")


    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    df1 = md.get_last_bars_from_rt(symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    df = df.append(df1)


    df=df.pct_change(1).rename(columns={'close': symbol})[symbol].dropna()
    df=df.reset_index()

    # use VIX to get the mean 30d 60d 90d and so on from underlying_hist_ib h5
    vix = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol="VIX", expiry=None, last_date=last_date,
                                           num_days_back=500, resample="1D")

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
    client, log, globalconf = init_func()
    accountid = globalconf.get_accountid()
    x  = dt.datetime(year=int(date1[0]),month=int(date1[1]),day=int(date1[2]),hour=int(date1[3]),minute=59,second=59)
    t_margin, t_prem = ra.extrae_account_snapshot(valuation_dttm=x, accountid=accountid,
                                                  log=log, globalconf=globalconf,
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

    temp_portfolio = ra.extrae_portfolio_positions(log=log, globalconf=globalconf,
                                                   valuation_dttm=x,
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

import sqlite3
def print_account_delta(valuation_dt):
    """
    valuation dt = YYYY-MM-DD-HH
    :param valuation_dt:
    :return:
    """
    date1=valuation_dt.split("-")
    client, log, globalconf = init_func()
    accountid = globalconf.get_accountid()
    x  = dt.datetime(year=int(date1[0]),month=int(date1[1]),day=int(date1[2]),hour=int(date1[3]),minute=0,second=0)
    t_margin, t_prem = ra.extrae_account_delta(globalconf=globalconf, log=log, valuation_dttm=x,
                                                   accountid=accountid, scenarioMode="N", simulName="NA")
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
    path = globalconf.config['paths']['data_folder']
    db_file = globalconf.config['sqllite']['orders_db']
    store = sqlite3.connect(path + db_file)
    sql = "select * from " + accountid + " where 1=1 "
    df1 = pd.read_sql_query(sql, store)

    start_dt=x.replace(minute=0, second=0)
    end_dt=x.replace(minute=59, second=59)
    #coord1 = "times < " + end_dt + " & times > " + start_dt
    #c = store.select_as_coordinates(node._v_pathname,coord1)
    #df1 = store.select(node._v_pathname,where=c)
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
    last_date = datetime.datetime.today().strftime("%Y%m%d")

    df = md.read_market_data_from_sqllite(db_type="underl_ib_hist", symbol=symbol, expiry=None, last_date=last_date,
                                          num_days_back=500, resample="1D")

    last_record_stored = np.max(df.index).replace(hour=0,minute=59, second=59)
    #df1 = md.get_last_bars_from_rt(globalconf=globalconf, log=log_analytics, symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
    #df = df.append(df1)


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
    df = pd.concat([df,df2],axis=1)
    df = df[['close', 'YTD', 'MTD', 'WTD', 'rol_dd_%d' % window_length]]
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
        df= persist.h5_methods.extrae_historical_chain(start_dt1, end_dt1, symbol, x[1:], expiry, x[:1])
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



from persist.sqlite_methods import read_market_data_from_sqllite

def print_chain(val_dt,symbol,call_d_range,put_d_range,expiry,type):
    """
    Type should be bid, ask or trades
    Call delta _range 10,15 Put delta_range -15,-10
    Example 
    "20170827" "SPY" "10,15" "-15,-10" "20170915"  "bid"
    """
    client , log, globalconf = init_func()
    start_dt1 = val_dt +" 23:59:59"
    valuation_dttm=dt.datetime.strptime(start_dt1, '%Y%m%d %H:%M:%S')
    end_dt1 = "20991231" +" 23:59:59"
    c_range = call_d_range.split(",")
    p_range = put_d_range.split(",")
    number_days_back = 10
    # max_expiry_available = max( get_expiries(globalconf=globalconf, dsId='optchain_ib_exp', symbol=symbol))
    expiry_file =  dt.datetime.strptime(expiry, "%Y%m%d").strftime("%Y-%m")
    df = read_market_data_from_sqllite(db_type="optchain_ib", symbol=symbol, expiry=expiry_file, last_date=val_dt,
                                       num_days_back=number_days_back, resample=None)

    # filtrar las opciones que tengan la delta dentro del rango seleccionado en la ultima cotización disponible
    # me quedo con la ultima cotizacion para cada option chain member
    filtro_df = df.loc[
        df.groupby(['symbol', 'strike', 'expiry', 'right'])['current_datetime'].agg(pd.Series.idxmax)]
    filtro_df=filtro_df[( (df['modelDelta'] >= float(c_range[0])/100.0 ) & (df['modelDelta'] <= float(c_range[1])/100.0 ) & ( df['right'] == "C" ) )
            |
          ((df['modelDelta'] >= float(p_range[0])/100.0) & (df['modelDelta'] <= float(p_range[1])/100.0 ) & (df['right'] == "P"))
         ]
    columns=[u'current_datetime',u'current_date',u'strike',u'bidPrice',u'expiry',u'right',u'symbol',
             u'bidSize',u'askPrice',u'askSize',u'modelDelta',u'modelImpliedVol',u'lastUndPrice']

    filtro_df = filtro_df[columns]
    df = df[columns]
    # g = df.groupby(['symbol','expiry','right'])['current_datetime'].max()
    # g = df.groupby(['symbol','expiry','right','current_date'])[df['current_datetime'] == df['current_datetime'].max()]
    # print(g)
    #df['current_datetime'] = df['current_datetime'].astype(float) # .apply(pd.to_numeric)

    keys = ['symbol', 'strike', 'expiry', 'right']
    i1 = df.set_index(keys).index
    i2 = filtro_df.set_index(keys).index
    df = df[i1.isin(i2)]


    # Get the latest quote per group
    df = df.loc[df.groupby(['symbol','strike','expiry','right','current_date'])['current_datetime'].agg(pd.Series.idxmax)]


    #for x in range(int(c_range[0]),int(c_range[1])):
        #df=ra.extrae_historical_chain(start_dt1,end_dt1,symbol,str(x),expiry,"C")
        #df['right']="C"
        #df['strike']=x
        #dataframe=dataframe.append(df[:1])
    #df = df.idxmax()
    df = df.sort_values(axis=0,by=['symbol','expiry','right','strike','current_date'], ascending=[True,True,True,True,True])
    df = df.set_index(['symbol','expiry','right','strike','current_date'],append=False) # .drop(['current_datetime'], 1)


    df['current_datetime']=pd.to_datetime(df['current_datetime'],format="%Y%m%d%H%M%S")
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
    print (df)
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
    dataframe = persist.h5_methods.read_biz_calendar(start_dttm=start, valuation_dttm=end)
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

    div, script = read_graph_from_db(symbol="SPX", last_date=last_date1, estimator="Coppock", name="TREND")

    print((div, script))
