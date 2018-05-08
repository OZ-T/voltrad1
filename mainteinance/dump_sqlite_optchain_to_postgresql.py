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
log = globalconf.get_logger()
path = globalconf.config['paths']['data_folder']

def run_ib(symbol,expiry):
    dataframe = sql.get_ib_option_dataframe(symbol, expiry, None, None)

    if dataframe.empty:
        log.info('DataFrame is empty!')
        return

    dataframe['expiry'] = pd.to_datetime(dataframe['expiry'], format='%Y%m%d')
    #ataframe['expiry'] = dataframe['expiry'].dt.date
    drop_lst = []
    if 'Halted' in dataframe.columns:
        drop_lst.append('Halted')
    if 'current_date' in dataframe.columns:
        drop_lst.append('current_date')
    if 'current_datetime' in dataframe.columns:
        drop_lst.append('current_datetime')
    if 'secType' in dataframe.columns:
        drop_lst.append('secType')
    if 'exchange' in dataframe.columns:
        drop_lst.append('exchange')
    if 'currency' in dataframe.columns:
        drop_lst.append('currency')
    if 'comboLegsDescrip' in dataframe.columns:
        drop_lst.append('comboLegsDescrip')
    if 'bidPvDividend' in dataframe.columns:
        drop_lst.append('bidPvDividend')
    if 'askPvDividend' in dataframe.columns:
        drop_lst.append('askPvDividend')
    if 'lastPvDividend' in dataframe.columns:
        drop_lst.append('lastPvDividend')
    if 'modelPvDividend' in dataframe.columns:
        drop_lst.append('modelPvDividend')

    dataframe.drop(drop_lst, axis=1, inplace=True)
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

    # INFO
    #duplicated = pd.concat(g for _, g in dataframe.groupby(["load_dttm","symbol", "expiry","strike", "right"]) if len(g) > 1)
    #print (duplicated)

    # keep the last if there are duplicates
    log.info(("len before removing dups",len(dataframe)))
    dataframe = dataframe.drop_duplicates(subset=["load_dttm","symbol", "expiry","strike", "right"], keep='last')
    log.info(("len after removing dups", len(dataframe)))

    # path = globalconf.config['paths']['data_folder']
    # store = sqlite3.connect(path + "AAAA.db")
    # dataframe.to_sql("AAAAA", store, if_exists='append')
    #print(list(dataframe))
    dataframe.to_sql(name="OPTIONS_CHAIN_IB", con=con, if_exists='append', chunksize=50, index=False)


def run_yhoo(symbol,expiry):
    try:
        dataframe = sql.get_yahoo_option_dataframe(symbol, expiry, None, None)

        if dataframe.empty:
            log.info('DataFrame is empty!')
            return

        dataframe['Expiry_txt'] = pd.to_datetime(dataframe['Expiry_txt'], format="%Y-%m-%d %H:%M:%S")
        dataframe=dataframe.rename(columns={'Expiry_txt': 'Expiry'})
        drop_lst = []
        if 'Quote_Time_txt' in dataframe.columns:
            drop_lst.append('Quote_Time_txt')
        if 'Last_Trade_Date_txt' in dataframe.columns:
            drop_lst.append('Last_Trade_Date_txt')
        if 'JSON' in dataframe.columns:
            drop_lst.append('JSON')
        if 'Symbol' in dataframe.columns:
            drop_lst.append('Symbol')
        if 'Root' in dataframe.columns:
            drop_lst.append('Root')
        if 'IsNonstandard' in dataframe.columns:
            drop_lst.append('IsNonstandard')

        # only one char for call/put
        dataframe.Type = dataframe.Type.str[:1]
        dataframe.drop(drop_lst, axis=1, inplace=True)
        con, meta = globalconf.connect_sqldb()
        # path = globalconf.config['paths']['data_folder']
        # store = sqlite3.connect(path + "AAAA.db")
        # dataframe.to_sql("AAAAA", store, if_exists='append')
        #print(list(dataframe))

        # keep the last if there are duplicates
        log.info(("len before removing dups",len(dataframe)))
        dataframe = dataframe.drop_duplicates(subset=["Quote_Time","Underlying", "Expiry_txt","Strike", "Type"], keep='last')
        log.info(("len after removing dups", len(dataframe)))

        dataframe.to_sql(name="OPTIONS_CHAIN_YHOO", con=con, if_exists='append', chunksize=50, index=False)
    except pd.io.sql.DatabaseError as e:
        log.info("No table for this symbol and expiry : " + str(e) )

if __name__ == "__main__":
    run_ib(symbol="SPY", expiry="2017-10")