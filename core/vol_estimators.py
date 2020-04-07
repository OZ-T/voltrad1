#A complete set of volatility estimators based on Euan Sinclair's Volatility Trading
#
# For each of the estimators, plot:
# * Probability cones
# * Rolling quantiles
# * Rolling extremes
# * Rolling descriptive statistics
# * Histogram
# * Comparison against arbirary comparable
# * Correlation against arbirary comparable
# * Regression against arbirary comparable
#
# Also creates a term sheet with all the metrics printed to a PDF

# #variables for the instances
# window=30
# windows=[30, 60, 90, 120]
# quantiles=[0.25, 0.75]
# bins=100
# normed=True
# bench='^GSPC'
# # call plt.show() on any of the below
# fig, plt = vol.cones(windows=windows, quantiles=quantiles)
# fig, plt = vol.rolling_quantiles(window=window, quantiles=quantiles)
# fig, plt = vol.rolling_extremes(window=window)
# fig, plt = vol.rolling_descriptives(window=window)
# fig, plt = vol.histogram(window=window, bins=bins, normed=normed)
# fig, plt = vol.benchmark_compare(window=window, bench=bench)
# fig, plt = vol.benchmark_correlation(window=window, bench=bench)
# # prints regression statistics
# print vol.benchmark_regression(window=window, bench=bench)
# # creates a pdf term sheet with all metrics
# vol.term_sheet(window, windows, quantiles, bins, normed, bench)

import datetime
import math
import os
import matplotlib
import numpy as np
import pandas
import statsmodels.api as sm

matplotlib.use('Agg') # Must be before importing matplotlib.pyplot or pylab!
# from matplotlib.pyplot import *
import matplotlib.mlab as mlab
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt, mpld3
import matplotlib.pyplot as pyplt
from persist.sqlite_methods import read_market_data_from_sqllite, save_graph_to_db, save_lineplot_data_to_db
from bokeh.models.annotations import Legend
from bokeh.embed import components
from bokeh.layouts import layout
import persist.sqlite_methods as md
# references
# http://stackoverflow.com/questions/4700614/how-to-put-the-legend-out-of-the-plot
# http://www.blog.pythonlibrary.org/2010/09/04/python-101-how-to-open-a-file-or-program/

from core import config
globalconf = config.GlobalConfig()
log = globalconf.log


class VolatilityEstimator(object):
    def __init__(self, db_type, symbol, expiry, last_date, num_days_back, resample, estimator, clean):
        if symbol is None or symbol == '':
            raise ValueError('symbol symbol required')
        if num_days_back is None or num_days_back == '':
            raise ValueError('num_days_back required')
        if last_date is None or last_date == '':
            raise ValueError('last_date required')
        if estimator not in ["GarmanKlass", "HodgesTompkins", "Kurtosis", "Parkinson", "Raw", "RogersSatchell", "Skew",
                             "YangZhang"]:
            raise ValueError('Acceptable volatility model is required')

        self._symbol = symbol
        self._num_days_back = num_days_back
        self._last_date = last_date
        self._estimator = estimator
        self._db_type = db_type
        self._expiry = expiry
        self._resample = resample
        self._clean = clean
        self._prices = read_market_data_from_sqllite(db_type=self._db_type, symbol=self._symbol, expiry=self._expiry,
                                                     last_date=self._last_date, num_days_back=self._num_days_back,
                                                     resample=self._resample)

        last_record_stored = np.max(self._prices.index).replace(hour=0, minute=59, second=59)
        df1 = md.get_last_bars_from_rt(symbol=symbol, last_date=last_date, last_record_stored=last_record_stored)
        self._prices = self._prices.append(df1)

        matplotlib.rc('image', origin='upper')
        matplotlib.rcParams['font.family'] = 'Times New Roman'
        matplotlib.rcParams['font.size'] = '11'
        matplotlib.rcParams['grid.color'] = 'lightgrey'
        matplotlib.rcParams['grid.linestyle'] = '-'
        matplotlib.rcParams['figure.subplot.left'] = 0.1
        matplotlib.rcParams['figure.subplot.bottom'] = 0.13
        matplotlib.rcParams['figure.subplot.right'] = 0.9
        matplotlib.rcParams['figure.subplot.top'] = 0.9

    def _get_estimator(self, window):
        """Selector for volatility estimator
        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        clean : boolean
            Set to True to remove the NaNs at the beginning of the series
        Returns
        -------
        y : pandas.DataFrame
            Estimator series values
        """
        if self._estimator is "GarmanKlass":
            return self.get_GarmanKlass_estimator(window=window)
        elif self._estimator is "HodgesTompkins":
            return self.get_HodgesTompkins_estimator(window=window)
        elif self._estimator is "Kurtosis":
            return self.get_Kurtosis_estimator(window=window)
        elif self._estimator is "Parkinson":
            return self.get_Parkinson_estimator(window=window)
        elif self._estimator is "Raw":
            return self.get_Raw_estimator(window=window)
        elif self._estimator is "RogersSatchell":
            return self.get_RogersSatchell_estimator(window=window)
        elif self._estimator is "Skew":
            return self.get_Skew_estimator(window=window)
        elif self._estimator is "YangZhang":
            return self.get_YangZhang_estimator(window=window)

    def get_GarmanKlass_estimator(self, window=30):
        prices = self._prices
        log_hl = (prices['high'] / prices['low']).apply(np.log)
        log_co = (prices['close'] / prices['open']).apply(np.log)
        rs = 0.5 * log_hl ** 2 - (2 * math.log(2) - 1) * log_co ** 2
        def f(v):
            return math.sqrt(252 * v.mean())
        result = rs.rolling(window=window, center=False).apply(func=f)
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_HodgesTompkins_estimator(self, window=30):
        prices = self._prices
        log_return = (prices['Close'] / prices['Close'].shift(1)).apply(np.log)
        vol = log_return.rolling(window=window, center=False).std() * math.sqrt(252)
        adj_factor = math.sqrt((1.0 / (1.0 - (window / (log_return.count() - (window - 1.0))) + (window ** 2 - 1.0) / (
        3.0 * (log_return.count() - (window - 1.0)) ** 2))))
        result = vol * adj_factor
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_Kurtosis_estimator(self, window=30):
        prices = self._prices
        log_return = (prices['Close'] / prices['Close'].shift(1)).apply(np.log)
        result = log_return.rolling(window=window, center=False).kurt()
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_Parkinson_estimator(self, window=30):
        prices = self._prices
        rs = (1 / (4 * math.log(2))) * ((prices['High'] / prices['Low']).apply(np.log)) ** 2
        def f(v):
            return math.sqrt(252 * v.mean())
        result = rs.rolling(window=window, center=False).apply(func=f)
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_Raw_estimator(self, window=30):
        prices = self._prices
        log_return = (prices['Close'] / prices['Close'].shift(1)).apply(np.log)
        result = log_return.rolling(window=window, center=False).std() * math.sqrt(252)
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_RogersSatchell_estimator(self, window=30):
        prices = self._prices
        log_ho = (prices['High'] / prices['Open']).apply(np.log)
        log_lo = (prices['Low'] / prices['Open']).apply(np.log)
        log_co = (prices['Close'] / prices['Open']).apply(np.log)
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
        def f(v):
            return math.sqrt(252 * v.mean())
        result = rs.rolling(window=window, center=False).apply(func=f)
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_Skew_estimator(self, window=30):
        prices = self._prices
        log_return = (prices['Close'] / prices['Close'].shift(1)).apply(np.log)
        result = log_return.rolling(window=window, center=False).skew()
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def get_YangZhang_estimator(self, window=30):
        prices = self._prices
        log_ho = (prices['High'] / prices['Open']).apply(np.log)
        log_lo = (prices['Low'] / prices['Open']).apply(np.log)
        log_co = (prices['Close'] / prices['Open']).apply(np.log)
        log_oc = (prices['Open'] / prices['Close'].shift(1)).apply(np.log)
        log_oc_sq = log_oc ** 2
        log_cc = (prices['Close'] / prices['Close'].shift(1)).apply(np.log)
        log_cc_sq = log_cc ** 2
        rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
        close_vol = log_cc_sq.rolling(window=window, center=False).sum() * (1.0 / (window - 1.0))
        open_vol = log_oc_sq.rolling(window=window, center=False).sum() * (1.0 / (window - 1.0))
        window_rs = rs.rolling(window=window, center=False).sum() * (1.0 / (window - 1.0))
        result = (open_vol + 0.164333 * close_vol + 0.835667 * window_rs).apply(np.sqrt) * math.sqrt(252)
        result[:window - 1] = np.nan
        if self._clean:
            return result.dropna()
        else:
            return result

    def cones_prepare_data(self, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75]):
        if len(windows) < 2:
            raise ValueError('Two or more window periods required')
        if len(quantiles) != 2:
            raise ValueError('A two element list of quantiles is required, lower and upper')
        if quantiles[0] + quantiles[1] != 1.0:
            raise ValueError('The sum of the quantiles must equal 1.0')
        if quantiles[0] > quantiles[1]:
            raise ValueError(
                'The lower quantiles (first element) must be less than the upper quantile (second element)')
        max = []
        min = []
        top_q = []
        median = []
        bottom_q = []
        realized = []
        data = []

        for w in windows:
            estimator = self._get_estimator(w)

            max.append(estimator.max())
            top_q.append(estimator.quantile(quantiles[1]))
            median.append(estimator.median())
            bottom_q.append(estimator.quantile(quantiles[0]))
            min.append(estimator.min())
            realized.append(estimator[-1])
            data.append(estimator)

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        return top_q, median, bottom_q, realized, min, max, f, data

    def cones_data(self, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75]):
        top_q, median, bottom_q, realized, min, max, f, data = self.cones_prepare_data(windows, quantiles)
        xs = [windows, windows, windows, windows, windows, windows]
        ys = [top_q, median, bottom_q, realized, min, max]
        ys_labels = ["top_q", "median", "bottom_q", "realized", "min", "max"]
        title = self._estimator + ' (' + self._symbol + ', daily from ' + self._last_date + ' days back ' + str(
            self._num_days_back) + ')'
        save_lineplot_data_to_db(xs=xs, ys=ys, ys_labels=ys_labels, title=title, symbol=self._symbol,
                                 expiry=self._expiry, last_date=self._last_date, num_days_back=self._num_days_back,
                                 resample=self._resample, estimator=self._estimator, name="VOLEST")


    def cones_bokeh(self, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75]):
        """Plots volatility cones
        Parameters
        ----------
        windows : [int, int, ...]
            List of rolling windows for which to calculate the estimator cones
        quantiles : [lower, upper]
            List of lower and upper quantiles for which to plot the cones
        """
        top_q, median, bottom_q, realized, min, max, f, data = self.cones_prepare_data(windows, quantiles)
        colors_list = ['orange','blue','pink','black','red','green']
        methods_list = ['x','diamond','x','square','inverted_triangle','inverted_triangle']
        line_dash_list = ['dotted', 'dotdash', 'dotted', 'solid', 'dashed', 'dashed']
        xs = [windows, windows, windows, windows, windows, windows]
        ys = [top_q, median, bottom_q, realized, min, max]
        legends_list = [str(int(quantiles[1] * 100)) + " Prctl", 'Median',
                        str(int(quantiles[0] * 100)) + " Prctl", 'Realized', 'Min', 'Max']
        title = self._estimator + ' (' + self._symbol + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')'
        p = figure(title=title, plot_width=700, plot_height=500,toolbar_sticky=False,
                   x_axis_label="Days",y_axis_label="Volatility",toolbar_location="below")
        legend_items = []
        for (colr, leg, x, y, method, line_dash) in zip(colors_list, legends_list, xs, ys, methods_list, line_dash_list):
            # call dynamically the method to plot line, circle etc...
            renderers = []
            if method:
                renderers.append( getattr(p, method)(x, y, color=colr,size=4) )
            renderers.append( p.line(x, y, color=colr, line_dash=line_dash) )
            legend_items.append((leg,renderers))
        # doesnt work: legend = Legend(location=(0, -30), items=legend_items)
        legend = Legend(location=(0, -30), items=legend_items)
        p.add_layout(legend, 'right')

        from bokeh.charts import BoxPlot
        df = pandas.DataFrame({"data": data[0], "group": 0})
        df = df.append(pandas.DataFrame({"data": data[1], "group": 1}))
        df = df.append(pandas.DataFrame({"data": data[2], "group": 2}))
        df = df.append(pandas.DataFrame({"data": data[3], "group": 3}))
        p2 = BoxPlot(df, values='data', label='group', title="Boxplot Summary (" + self._last_date + ") (" + self._symbol + ")" ,toolbar_location="below",
                     legend="bottom_right", plot_width=600, plot_height=400,toolbar_sticky=False)
        from bokeh.models.ranges import DataRange1d
        p2.y_range = DataRange1d(np.min(df['data']-0.01), np.max(df['data']+0.01))

        layout1 = layout([[p, p2]])
        script, div = components(layout1)
        save_graph_to_db(script, div, self._symbol, self._expiry, self._last_date, self._num_days_back, self._resample,
                         self._estimator, name="VOLEST")
        return layout1


    def cones(self, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75]):
        """Plots volatility cones
        Parameters
        ----------
        windows : [int, int, ...]
            List of rolling windows for which to calculate the estimator cones
        quantiles : [lower, upper]
            List of lower and upper quantiles for which to plot the cones
        """
        top_q, median, bottom_q, realized, min, max, f, data  = self.cones_prepare_data(windows, quantiles)

        fig = plt.figure(figsize=(8, 6))
        left, width = 0.07, 0.65
        bottom, height = 0.2, 0.7
        bottom_h = left_h = left + width + 0.02

        rect_cones = [left, bottom, width, height]
        rect_box = [left_h, bottom, 0.17, height]

        cones = plt.axes(rect_cones)
        box = plt.axes(rect_box)

        # set the plots
        cones.plot(windows, max, label="Max")
        cones.plot(windows, top_q, label=str(int(quantiles[1] * 100)) + " Prctl")
        cones.plot(windows, median, label="Median")
        cones.plot(windows, bottom_q, label=str(int(quantiles[0] * 100)) + " Prctl")
        cones.plot(windows, min, label="Min")
        cones.plot(windows, realized, 'r-.', label="Realized")

        # set the x ticks and limits
        cones.set_xticks((windows))
        cones.set_xlim((windows[0] - 5, windows[-1] + 5))

        # set and format the y-axis labels
        locs = cones.get_yticks()
        cones.set_yticklabels(map(f, locs))

        # turn on the grid
        cones.grid(True, axis='y', which='major', alpha=0.5)

        # set the title
        cones.set_title(self._estimator + ' (' + self._symbol + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')

        # set the legend
        pos = cones.get_position()  #
        cones.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=3)

        #
        # box plot args
        #

        # set the plots
        box.boxplot(data, notch=1, sym='+')
        box.plot([i for i in range(1, len(windows) + 1)], realized, color='r', marker='*', markeredgecolor='k')

        # set and format the y-axis labels
        locs = box.get_yticks()
        box.set_yticklabels(map(f, locs))

        # move the y-axis ticks on the right side
        box.yaxis.tick_right()

        # turn on the grid
        box.grid(True, axis='y', which='major', alpha=0.5)

        return fig, plt

    def rolling_quantiles(self, window=30, quantiles=[0.25, 0.75]):
        """Plots rolling quantiles of volatility

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        quantiles : [lower, upper]
            List of lower and upper quantiles for which to plot
        """
        if len(quantiles) != 2:
            raise ValueError('A two element list of quantiles is required, lower and upper')
        if quantiles[0] + quantiles[1] != 1.0:
            raise ValueError('The sum of the quantiles must equal 1.0')
        if quantiles[0] > quantiles[1]:
            raise ValueError(
                'The lower quantiles (first element) must be less than the upper quantile (second element)')

        estimator = self._get_estimator(window)
        date = estimator.index

        top_q = estimator.rolling(window=window, center=False).quantile(quantiles[1])
        median = estimator.rolling(window=window, center=False).median()
        bottom_q = estimator.rolling(window=window, center=False).quantile(quantiles[0])
        realized = estimator
        last = estimator[-1]

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        #
        # figure args
        #

        fig = plt.figure(figsize=(8, 6))

        left, width = 0.07, 0.65
        bottom, height = 0.2, 0.7
        bottom_h = left_h = left + width + 0.02

        rect_cones = [left, bottom, width, height]
        rect_box = [left_h, bottom, 0.17, height]

        cones = plt.axes(rect_cones)
        box = plt.axes(rect_box)

        #
        # cones plot args
        #

        # set the plots
        cones.plot(date, top_q, label=str(int(quantiles[1] * 100)) + " Prctl")
        cones.plot(date, median, label="Median")
        cones.plot(date, bottom_q, label=str(int(quantiles[0] * 100)) + " Prctl")
        cones.plot(date, realized, 'r-.', label="Realized")

        # set and format the y-axis labels
        locs = cones.get_yticks()
        cones.set_yticklabels(map(f, locs))

        # turn on the grid
        cones.grid(True, axis='y', which='major', alpha=0.5)

        # set the title
        cones.set_title(self._estimator + ' (' + self._symbol + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')

        # set the legend
        cones.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=3)

        #
        # box plot args
        #

        # set the plots
        box.boxplot(realized, notch=1, sym='+')
        box.plot(1, last, color='r', marker='*', markeredgecolor='k')

        # set and format the y-axis labels
        locs = box.get_yticks()
        box.set_yticklabels(map(f, locs))

        # move the y-axis ticks on the right side
        box.yaxis.tick_right()

        # turn on the grid
        box.grid(True, axis='y', which='major', alpha=0.5)

        return fig, plt

    def rolling_extremes(self, window=30):
        """Plots rolling max and min of volatility estimator

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        """
        estimator = self._get_estimator(window)
        date = estimator.index
        max = estimator.rolling(window=window, center=False).max()
        min = estimator.rolling(window=window, center=False).min()
        realized = estimator
        last = estimator[-1]

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        #
        # Figure args
        #

        fig = plt.figure(figsize=(8, 6))

        left, width = 0.07, 0.65
        bottom, height = 0.2, 0.7
        bottom_h = left_h = left + width + 0.02

        rect_cones = [left, bottom, width, height]
        rect_box = [left_h, bottom, 0.17, height]

        cones = plt.axes(rect_cones)
        box = plt.axes(rect_box)

        #
        # Cones plot args
        #

        # set the plots
        cones.plot(date, max, label="Max")
        cones.plot(date, min, label="Min")
        cones.plot(date, realized, 'r-.', label="Realized")

        # set and format the y-axis labels
        locs = cones.get_yticks()
        cones.set_yticklabels(map(f, locs))

        # turn on the grid
        cones.grid(True, axis='y', which='major', alpha=0.5)

        # set the title
        cones.set_title(self._estimator + ' (' + self._symbol + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')

        # set the legend
        cones.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=3)

        #
        # Box plot args
        #

        # set the plots
        box.boxplot(realized, notch=1, sym='+')
        box.plot(1, last, color='r', marker='*', markeredgecolor='k')

        # set and format the y-axis labels
        locs = box.get_yticks()
        box.set_yticklabels(map(f, locs))

        # move the y-axis ticks on the right side
        box.yaxis.tick_right()

        # turn on the grid
        box.grid(True, axis='y', which='major', alpha=0.5)

        return fig, plt

    def rolling_descriptives(self, window=30):
        """Plots rolling first and second moment of volatility estimator

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        """
        estimator = self._get_estimator(window)
        date = estimator.index
        mean = estimator.rolling(window=window, center=False).mean()
        std = estimator.rolling(window=window, center=False).std()
        z_score = (estimator - mean) / std

        realized = estimator
        last = estimator[-1]

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        #
        # Figure args
        #

        fig = plt.figure(figsize=(8, 6))

        left, width = 0.07, 0.65
        bottom, height = 0.1, .8
        bottom_h = left_h = left + width + 0.02

        rect_cones = [left, 0.35, width, 0.55]
        rect_box = [left_h, 0.15, 0.17, 0.75]
        rect_z = [left, 0.15, width, 0.15]

        cones = plt.axes(rect_cones)
        box = plt.axes(rect_box)
        z = plt.axes(rect_z)

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        #
        # Cones plot args
        #

        # set the plots
        cones.plot(date, mean, label="Mean")
        cones.plot(date, std, label="Std. Dev.")
        cones.plot(date, realized, 'r-.', label="Realized")

        # set and format the y-axis labels
        locs = cones.get_yticks()
        cones.set_yticklabels(map(f, locs))

        # turn on the grid
        cones.grid(True, axis='y', which='major', alpha=0.5)

        # set the title
        cones.set_title(self._estimator + ' (' + self._symbol + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')

        # shrink the plot up a bit and set the legend
        pos = cones.get_position()  #
        cones.set_position([pos.x0, pos.y0 + pos.height * 0.1, pos.width, pos.height * 0.9])  #
        cones.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=3)

        #
        # Box plot args
        #

        # set the plots
        box.boxplot(realized, notch=1, sym='+')
        box.plot(1, last, color='r', marker='*', markeredgecolor='k')

        # set and format the y-axis labels
        locs = box.get_yticks()
        box.set_yticklabels(map(f, locs))

        # move the y-axis ticks on the right side
        box.yaxis.tick_right()

        # turn on the grid
        box.grid(True, axis='y', which='major', alpha=0.5)

        #
        # Z-Score plot args
        #

        # set the plots
        z.plot(date, z_score, 'm-', label="Z-Score")

        # turn on the grid
        z.grid(True, axis='y', which='major', alpha=0.5)

        # create a horizontal line at y=0
        z.axhline(0, 0, 1, linestyle='-', linewidth=1.0, color='black')

        # set the legend
        z.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=3)

        return fig, plt

    def histogram(self, window=90, bins=100, normed=True):
        """

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        bins : int

        """
        estimator = self._get_estimator(window)
        mean = estimator.mean()
        std = estimator.std()
        last = estimator[-1]

        fig = plt.figure(figsize=(8, 6))

        n, bins, patches = plt.hist(estimator, bins, normed=normed, facecolor='blue', alpha=0.25)

        if normed:
            y = mlab.normpdf(bins, mean, std)
            l = plt.plot(bins, y, 'g--', linewidth=1)

        plt.axvline(last, 0, 1, linestyle='-', linewidth=1.5, color='r')

        plt.grid(True, axis='y', which='major', alpha=0.5)
        plt.title(
            'Distribution of ' + self._estimator + ' estimator values (' + self._symbol +
                ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')

        return fig, plt

    def benchmark_compare(self, window=90, bench='SPX'):
        """

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        bins : int

        """

        y = self._get_estimator(window)
        bench_vol = VolatilityEstimator(db_type=self._db_type, symbol=bench, expiry=None, last_date=self._last_date,
                                        num_days_back=self._num_days_back, resample=self._resample,
                                        estimator=self._estimator, clean=self._clean)

        x = bench_vol._get_estimator(window)
        date = y.index

        ratio = y / x

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        #
        # Figure args
        #

        fig = plt.figure(figsize=(8, 6))

        left, width = 0.07, .9

        rect_cones = [left, 0.4, width, .5]
        rect_box = [left, 0.15, width, 0.15]

        cones = plt.axes(rect_cones)
        box = plt.axes(rect_box)

        #
        # Cones plot args
        #

        # set the plots
        self._log.info (("x len",len(x)," y len ", len(y)))
        cones.plot(date, y, label=self._symbol.upper())
        cones.plot(date, x, label=bench.upper())

        # set and format the y-axis labels
        locs = cones.get_yticks()
        cones.set_yticklabels(map(f, locs))

        # turn on the grid
        cones.grid(True, axis='y', which='major', alpha=0.5)

        # set the title
        cones.set_title(
            self._estimator + ' (' + self._symbol + ' v. ' + bench.upper()
                + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')
        # shrink the plot up a bit and set the legend
        cones.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=3)

        #
        # Cones plot args
        #

        # set the plot
        box.plot(date, ratio, label=self._symbol.upper() + '/' + bench.upper())

        # set the y-limits
        box.set_ylim((ratio.min() - 0.05, ratio.max() + 0.05))

        # fill the area
        box.fill_between(date, ratio, 0, color='blue', alpha=0.25)

        # set the legend
        box.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=3)

        return fig, plt

    def benchmark_correlation(self, window=90, bench='^GSPC'):
        """

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        bins : int

        """

        y = self._get_estimator(window)

        bench_vol = VolatilityEstimator(db_type=self._db_type, symbol=bench, expiry=None, last_date=self._last_date,
                                        num_days_back=self._num_days_back, resample=self._resample,
                                        estimator=self._estimator, clean=self._clean)

        x = bench_vol._get_estimator(window)
        date = y.index

        corr = x.rolling(window=window).corr(other=y)

        if self._estimator is "Skew" or self._estimator is "Kurtosis":
            f = lambda x: "%i" % round(x, 0)
        else:
            f = lambda x: "%i%%" % round(x * 100, 0)

        #
        # Figure args
        #

        fig = plt.figure(figsize=(8, 6))
        cones = plt.axes()

        #
        # Cones plot args
        #

        # set the plots
        cones.plot(date, corr)

        # set the y-limits
        cones.set_ylim((corr.min() - 0.05, corr.max() + 0.05))

        # set and format the y-axis labels
        locs = cones.get_yticks()
        cones.set_yticklabels(map(f, locs))

        # turn on the grid
        cones.grid(True, axis='y', which='major', alpha=0.5)

        # set the title
        cones.set_title(
            self._estimator + ' (Correlation of ' + self._symbol + ' v. ' + bench.upper()
                + ', daily from ' + self._last_date + ' days back ' + str(self._num_days_back) + ')')
        return fig, plt

    def benchmark_regression(self, window=90, bench='^GSPC'):
        """

        Parameters
        ----------
        window : int
            Rolling window for which to calculate the estimator
        bins : int

        """
        y = self._get_estimator(window)

        bench_vol = VolatilityEstimator(db_type=self._db_type, symbol=bench, expiry=None, last_date=self._last_date,
                                        num_days_back=self._num_days_back, resample=self._resample,
                                        estimator=self._estimator, clean=self._clean)

        X = bench_vol._get_estimator(window)

        model = sm.OLS(y, X)
        results = model.fit()

        return results.summary()

    def term_sheet_to_pdf(self, window=30, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75], bins=100, normed=True,
                   bench='SPY'):

        cones_fig, cones_plt = self.cones(windows=windows, quantiles=quantiles)
        rolling_quantiles_fig, rolling_quantiles_plt = self.rolling_quantiles(window=window, quantiles=quantiles)
        rolling_extremes_fig, rolling_extremes_plt = self.rolling_extremes(window=window)
        rolling_descriptives_fig, rolling_descriptives_plt = self.rolling_descriptives(window=window)
        histogram_fig, histogram_plt = self.histogram(window=window, bins=bins, normed=normed)
        benchmark_compare_fig, benchmark_compare_plt = self.benchmark_compare(window=window, bench=bench)
        benchmark_corr_fig, benchmark_corr_plt = self.benchmark_correlation(window=window, bench=bench)
        benchmark_regression = self.benchmark_regression(window=window, bench=bench)

        filename = self._symbol.upper() + '_termsheet_' + datetime.datetime.today().strftime("%Y%m%d") + '.pdf'
        fn = os.path.abspath(os.path.join(filename))
        pp = PdfPages(fn)

        pp.savefig(cones_fig)
        pp.savefig(rolling_quantiles_fig)
        pp.savefig(rolling_extremes_fig)
        pp.savefig(rolling_descriptives_fig)
        pp.savefig(histogram_fig)
        pp.savefig(benchmark_compare_fig)
        pp.savefig(benchmark_corr_fig)

        fig = plt.figure()
        plt.text(0.01, 0.01, benchmark_regression, fontsize=12)
        plt.axis('off')
        pp.savefig(fig)

        pp.close()

        print (filename + ' output complete')


    def define_fig(self,name,ext):
        web_server = self._globalconf.config['paths']['nginx_static_folder']
        filename = self._symbol.upper() + '_'+ name + ext
        fn = os.path.abspath(os.path.join(web_server,"volest", filename))
        return fn


    def term_sheet_to_html(self, window=30, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75], bins=100, normed=True,
                   bench='SPY', open=False):
        ext = '.html'
        cones_fig, cones_plt = self.cones(windows=windows, quantiles=quantiles)
        rolling_quantiles_fig, rolling_quantiles_plt = self.rolling_quantiles(window=window, quantiles=quantiles)
        rolling_extremes_fig, rolling_extremes_plt = self.rolling_extremes(window=window)
        rolling_descriptives_fig, rolling_descriptives_plt = self.rolling_descriptives(window=window)
        histogram_fig, histogram_plt = self.histogram(window=window, bins=bins, normed=normed)
        mpld3.save_html(cones_fig, self.define_fig("cones",ext))
        mpld3.save_html(rolling_quantiles_fig, self.define_fig("rolling_quantiles",ext))
        mpld3.save_html(rolling_extremes_fig, self.define_fig("rolling_extremes",ext))
        mpld3.save_html(rolling_descriptives_fig, self.define_fig("rolling_desc",ext))
        mpld3.save_html(histogram_fig, self.define_fig("histogram",ext))
        pyplt.close(cones_fig)
        pyplt.close(rolling_quantiles_fig)
        pyplt.close(rolling_extremes_fig)
        pyplt.close(rolling_descriptives_fig)
        pyplt.close(histogram_fig)


    def term_sheet_to_db(self, window=30, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75], bins=100, normed=True,
                   bench='SPY', open=False):
        ext = '.json'
        cones_fig, cones_plt = self.cones(windows=windows, quantiles=quantiles)
        rolling_quantiles_fig, rolling_quantiles_plt = self.rolling_quantiles(window=window, quantiles=quantiles)
        rolling_extremes_fig, rolling_extremes_plt = self.rolling_extremes(window=window)
        rolling_descriptives_fig, rolling_descriptives_plt = self.rolling_descriptives(window=window)
        histogram_fig, histogram_plt = self.histogram(window=window, bins=bins, normed=normed)
        cones_fig_dict = mpld3.fig_to_dict(cones_fig)
        rolling_quantiles_fig_dict = mpld3.fig_to_dict(rolling_quantiles_fig)
        rolling_extremes_fig_dict = mpld3.fig_to_dict(rolling_extremes_fig)
        rolling_descriptives_fig_dict = mpld3.fig_to_dict(rolling_descriptives_fig)
        histogram_fig_dict = mpld3.fig_to_dict(histogram_fig)
        pyplt.close(cones_fig)
        pyplt.close(rolling_quantiles_fig)
        pyplt.close(rolling_extremes_fig)
        pyplt.close(rolling_descriptives_fig)
        pyplt.close(histogram_fig)


    def term_sheet_to_json(self, window=30, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75], bins=100, normed=True,
                   bench='SPY', open=False):
        ext = '.json'
        cones_fig, cones_plt = self.cones(windows=windows, quantiles=quantiles)
        rolling_quantiles_fig, rolling_quantiles_plt = self.rolling_quantiles(window=window, quantiles=quantiles)
        rolling_extremes_fig, rolling_extremes_plt = self.rolling_extremes(window=window)
        rolling_descriptives_fig, rolling_descriptives_plt = self.rolling_descriptives(window=window)
        histogram_fig, histogram_plt = self.histogram(window=window, bins=bins, normed=normed)
        mpld3.save_json(cones_fig, self.define_fig("cones",ext))
        mpld3.save_json(rolling_quantiles_fig, self.define_fig("rolling_quantiles",ext))
        mpld3.save_json(rolling_extremes_fig, self.define_fig("rolling_extremes",ext))
        mpld3.save_json(rolling_descriptives_fig, self.define_fig("rolling_desc",ext))
        mpld3.save_json(histogram_fig, self.define_fig("histogram",ext))
        pyplt.close(cones_fig)
        pyplt.close(rolling_quantiles_fig)
        pyplt.close(rolling_extremes_fig)
        pyplt.close(rolling_descriptives_fig)
        pyplt.close(histogram_fig)



    def term_sheet_to_png(self, window=30, windows=[30, 60, 90, 120], quantiles=[0.25, 0.75], bins=100, normed=True):
        ext = '.png'
        cones_fig, cones_plt = self.cones(windows=windows, quantiles=quantiles)
        rolling_quantiles_fig, rolling_quantiles_plt = self.rolling_quantiles(window=window, quantiles=quantiles)
        rolling_extremes_fig, rolling_extremes_plt = self.rolling_extremes(window=window)
        rolling_descriptives_fig, rolling_descriptives_plt = self.rolling_descriptives(window=window)
        histogram_fig, histogram_plt = self.histogram(window=window, bins=bins, normed=normed)

        cones_fig.savefig(self.define_fig("cones",ext), bbox_inches='tight')
        rolling_quantiles_fig.savefig(self.define_fig("rolling_quantiles",ext), bbox_inches='tight')
        rolling_extremes_fig.savefig(self.define_fig("rolling_extremes",ext), bbox_inches='tight')
        rolling_descriptives_fig.savefig(self.define_fig("rolling_desc",ext), bbox_inches='tight')
        histogram_fig.savefig(self.define_fig("histogram",ext), bbox_inches='tight')
        pyplt.close(cones_fig)
        pyplt.close(rolling_quantiles_fig)
        pyplt.close(rolling_extremes_fig)
        pyplt.close(rolling_descriptives_fig)
        pyplt.close(histogram_fig)


if __name__ =="__mainKK__":
    from core import config
    from core.logger import logger
    log = logger("some testing ...")
    globalconf = config.GlobalConfig()

    last_date = "20170706"
    vol = VolatilityEstimator(db_type="underl_ib_hist", symbol="SPY", expiry=None, last_date=last_date,
                              num_days_back=200, resample="1D", estimator="GarmanKlass", clean=True)
    window=30
    windows=[30, 60, 90, 120]
    quantiles=[0.25, 0.75]
    bins=100
    normed=True
    p = vol.cones_bokeh(windows=windows, quantiles=quantiles)
    from bokeh.plotting import show
    show(p)
    from bokeh.plotting import figure, show

if __name__ == "__main__":
    from mainteinance import config
    from core.logger import logger
    log = logger("some testing ...")
    globalconf = config.GlobalConfig()

    last_date = "20170708"

