# coding: utf-8

#27-nov-2016 Fix error:
#  ValueError: Trying to store a value with len [XX] in [CallOI??] column but
#  this column has a limit of [XXX]!

import glob
import fnmatch
import os
import sys
import core.config as config
import pandas as pd
import datetime as dt
import numpy as np
import persist.sqlite_methods as sql
import sqlite3

# path = 'C:/Users/David/data/'
# path = '/home/david/data/'

globalconf = config.GlobalConfig()
path = globalconf.config['paths']['data_folder']

def run_ib(symbol,expiry):
    dataframe = sql.get_ib_option_dataframe(symbol, expiry, None, None)
    dataframe['expiry'] = pd.to_datetime(dataframe['expiry'], format='%Y%m%d')
    dataframe.drop(['current_date','current_datetime'], axis=1, inplace=True)
    dataframe=dataframe.rename(columns={'index': 'load_dttm'})
    con, meta = globalconf.connect_sqldb()
    # Valores muy pequenos hacen crash al load en postgresql
    # num = dataframe._get_numeric_data() (this one cannot use abs() unfortunately)
    # num[num < 1e-12] = 0
    dataframe.loc[abs(dataframe['bidGamma']) < 1e-12, 'bidGamma'] = 0
    dataframe.loc[abs(dataframe['askGamma']) < 1e-12, 'askGamma'] = 0
    dataframe.loc[abs(dataframe['modelVega']) < 1e-12, 'modelVega'] = 0
    dataframe.loc[abs(dataframe['modelTheta']) < 1e-12, 'modelTheta'] = 0
    dataframe.loc[abs(dataframe['modelGamma']) < 1e-12, 'modelGamma'] = 0
    dataframe.loc[abs(dataframe['modelDelta']) < 1e-12, 'modelDelta'] = 0
    dataframe.loc[abs(dataframe['askVega']) < 1e-12, 'askVega'] = 0
    dataframe.loc[abs(dataframe['bidDelta']) < 1e-12, 'bidDelta'] = 0
    dataframe.loc[abs(dataframe['askDelta']) < 1e-12, 'askDelta'] = 0
    dataframe.loc[abs(dataframe['bidVega']) < 1e-12, 'bidVega'] = 0
    dataframe.loc[abs(dataframe['askTheta']) < 1e-12, 'askTheta'] = 0
    dataframe.loc[abs(dataframe['bidTheta']) < 1e-12, 'bidTheta'] = 0
    dataframe.loc[abs(dataframe['modelOptPrice']) < 1e-12, 'modelOptPrice'] = 0

    # path = globalconf.config['paths']['data_folder']
    # store = sqlite3.connect(path + "AAAA.db")
    # dataframe.to_sql("AAAAA", store, if_exists='append')
    #print(list(dataframe))
    dataframe.to_sql(name="OPTIONS_CHAIN_IB", con=con, if_exists='append', chunksize=50, index=False)


if __name__ == "__main__":
    run(symbol="ES", expiry="2017-04")