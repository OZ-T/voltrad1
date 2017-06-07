"""
File to run automatically by cron each day to gather options data

/etc/crontab

entry:

IMPORTANTE
Ejecutar al principio de semana con los valores Actual de la tabla a missing (--)
y ejecutarlo otra vez al final de la semana con todos los resultados economicos de dicha semana
La existencia de estos valores duplicados debe manejarse en los programas analiticos subsiguientes

50 23 * * 1,2,3,4,5 python /home/david/python/voltrad1/volquotes/yahoo_biz_economic_calendar_us.py

"""
import platform
import datetime as dt
import numpy as np
import urllib
import time
import pandas as pd
import os
from volsetup import config
import sys
from BeautifulSoup import BeautifulSoup
import re
from datetime import datetime
from time import sleep
from volsetup.logger import logger
import requests
import bs4

# This is broken
# usar esta URL
#
#
#
#
# https://finance.yahoo.com/calendar/economic?from=2017-06-04&to=2017-06-10&day=2017-06-06
#




def run_reader():
    globalconf = config.GlobalConfig()
    log = logger("yahoo biz eco calendar download")
    log.info("Getting calendar from yahoo ... ")

    wait_secs = 10
    now = dt.datetime.now()  # Get current time
    c_year = str(now.year)
    #c_year = '2015'
    f = globalconf.open_economic_calendar_h5_store()  # open database file
    load_dttm = now.strftime('%Y-%m-%d %H:%M:%S')
    # Esto es para la carga incremental inicial
    #weeks = ["%.2d" % i for i in range(35)]
    weeks = [ datetime.today().strftime("%W") ]

    dataframe = pd.DataFrame()

    for week in weeks:
        log.info ("yahoo economic calendar downloader [%s] week=[%s]" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),week))
        html = requests.get("https://biz.yahoo.com/c/ec/{}{}.html".format(c_year,week), headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0"}).text
        soup = bs4.BeautifulSoup(html, "lxml")

        for tr in soup.findAll("tr"):
            if len(tr.contents) > 3:
                if str(tr.contents[0]).strip():
                    if str(tr.contents[0].text) <> 'Date':
                        temp = {'Date':tr.contents[0].text, 'Time_ET':tr.contents[1].text, 'Statistic':tr.contents[2].text ,
                                'For':tr.contents[3].text,  'Actual':tr.contents[4].text, 'Briefing_Forecast': tr.contents[5].text,
                                'Market_Expects':tr.contents[6].text, 'Prior':tr.contents[7].text , 'Revised_From':tr.contents[8].text }
                        temp1 = pd.DataFrame(temp,index=[str(tr.contents[0].text)])
                        temp1['week'] = c_year + week
                        #print temp1
                        dataframe = dataframe.append(temp1)
        sleep(wait_secs)

    dataframe = dataframe.drop_duplicates()
    dataframe['load_dttm'] = load_dttm

    # Fix the error "TypeError: [unicode] is not implemented as a table column" when runing to_hdf
    types = dataframe.apply(lambda x: pd.api.types.infer_dtype(x.values))
    for col in types[types == 'unicode'].index:
        dataframe[col] = dataframe[col].astype(str)
    dataframe.columns = [str(c) for c in dataframe.columns]
    log.info("storing calendar in hdf5 ...")
    try:
        f.append(    c_year , dataframe, data_columns=dataframe.columns)
    except ValueError as e:
        print ("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
        aux = globalconf.open_economic_calendar_h5_store_error()
        aux.append( c_year, dataframe, data_columns=dataframe.columns)
        aux.close()
    f.close()  # Close file

    """ TODO:
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings('error')
            try:
                answer = 1 / 0
            except Warning as e:
                print('error found:', e)
    """

def get_earning_data(date):
    html = requests.get("https://biz.yahoo.com/research/earncal/{}.html".format(date), headers={"User-Agent": "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0"}).text
    soup = bs4.BeautifulSoup(html)
    quotes = []
    for tr in soup.find_all("tr"):
        if len(tr.contents) > 3:
            if len(tr.contents[1].contents) > 0:
                if tr.contents[1].contents[0].name == "a":
                    if tr.contents[1].contents[0]["href"].startswith("http://finance.yahoo.com/q?s="):
                        quotes.append({     "name"  : tr.contents[0].text
                                           ,"symbol": tr.contents[1].contents[0].text
                                           ,"url"   : tr.contents[1].contents[0]["href"]
                                           ,"eps"   : tr.contents[2].text if len(tr.contents) == 6 else u'N/A'
                                           ,"time"  : tr.contents[3].text if len(tr.contents) == 6 else tr.contents[2].text
                                       })
    return quotes
#print(get_earning_data("20150309"))


if __name__=="__main__":
    run_reader()


