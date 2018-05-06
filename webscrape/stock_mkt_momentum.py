# https://www.barchart.com/stocks/momentum
# Number of stocks trading above their 20 Day Moving Average.
# When you analyze the historical behavior of this statistic, you clearly notice how extreme readings eventually end
# up signaling a market reversal in the short term (For longer term readings you use longer term moving averages,
# but all we care about here is the short term. That is, what is likely to happen in the next few weeks)

from bs4 import BeautifulSoup
import datetime as dt
from core.logger import logger
import persist.document_methods as da
import persist.sqlite_methods as sql
from core import config
from textblob import TextBlob
import locale
from time import sleep
import core.misc_utilities as utils
from webscrape.utilities import download
import pandas as pd
log = logger("wrap download")
import numpy as np
import re

def run_reader(now1 = None):
    """
    Run with "NA" string to get today's

    :param now1:
    :return:
    """
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    now = dt.datetime.now()  # + dt.timedelta(days=-4)
    mydate = utils.expiry_date(now1)
    if mydate:
        now = mydate
    weekday = now.strftime("%A").lower()
    log.info(("Getting data from https://www.barchart.com/stocks/momentum ... ",now))
    #if (  weekday in ("saturday","sunday")  or
    #      now.date() in utils.get_trading_close_holidays(dt.datetime.now().year)):
    #    log.info("This is a US Calendar holiday or weekend. Ending process ... ")
    #    return

    base = "https://www.barchart.com/stocks/momentum"
    globalconf = config.GlobalConfig()
    url = base
    log.info ('[%s] Downloading: %s' % (str(now) , url ) )
    b_html= download(url=url, log=log,
                     user_agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
                     )
    if  not b_html is None:
        soup = BeautifulSoup(b_html, 'html.parser')
        fixed_html = soup.prettify()
        tables = soup.find_all('table')

        # Summary Of Stocks With New Highs And Lows
        table = tables[1]
        df1= pd.read_html(str(table))[0]
        #df1.columns = df1.columns.astype(str)
        #df1 = df1.add_prefix('data_')
        df1.columns = ['description','data_5days','data_1month','data_3months','data_6months','data_52weeks','data_ytd']
        df1 = df1.replace(np.nan, 'Default', regex=True)
        df1['load_dttm'] = now
        df1.set_index(keys=['load_dttm'], drop=True, inplace=True)
        sql.write_momentum_to_sqllite(df1,"stocks_hi_lo_summary")

        # Market Performance Indicator
        #   The percentage of stocks in $BCMM above their individual Moving Average per period.
        #
        # Barchart Market Momentum Index ($BCMM) is an exclusive index used as an indicator of change
        #   in overall markets. It reflects the movement of stocks who fit the following criteria: must have
        #   current SEC filings, must have traded for a minimum of 6-months, and must be trading above $2.
        table = tables[2]
        df2= pd.read_html(str(table))[0]
        #df1.columns = df1.columns.astype(str)
        #df1 = df1.add_prefix('data_')
        df2.columns = ['description','5day_ma','20day_ma','50day_ma','100day_ma','150day_ma','200day_ma']
        df2 = df2.replace(np.nan, 'Default', regex=True)
        df2['load_dttm'] = now
        df2.set_index(keys=['load_dttm'], drop=True, inplace=True)
        sql.write_momentum_to_sqllite(df2,"stocks_bcmm_above_ma_summary")



if __name__=="__main__":
    date1 = None
    #run_reader("20180501")
    run_reader(now1=date1)
