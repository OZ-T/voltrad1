import pandas as pd
import json
import datetime as dt
import sqlite3
import volibutils.sync_client as ib
from volsetup.logger import logger
from volutils import utils as utils
import glob
import os
import volsetup.config as config
import time
import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError
import sys


def get_market_db_file(globalconf,db_type,expiry):
    if db_type == "optchain_ib":
        return1 = globalconf.config['sqllite']['optchain_ib'].format(expiry)
    elif db_type == "optchain_yhoo":
        return1 = globalconf.config['sqllite']['optchain_yhoo'].format(expiry)
    elif db_type == "optchain_ib_hist":
        return1 = globalconf.config['sqllite']['optchain_ib_hist_db'].format(expiry)
    elif db_type == "underl_ib_hist":
        return1 = globalconf.config['sqllite']['underl_ib_hist_db']

    return return1


def get_partition_names(db_type):
    """
    Returns the way to filter the input dataset expiry variable text, format of that variable symbol variable
     and sorting criteria
        which will be used for the name of the tables inside the sqlite DB
    """
    if db_type == "optchain_ib":
        return1 = ['expiry',"%Y%m%d","symbol","current_datetime"]
    elif db_type == "optchain_yhoo":
        return1 = ["Expiry_txt","%Y-%m-%d  %H:%M:%S","Underlying","Quote_Time_txt"]
    elif db_type == "optchain_ib_hist":
        return1 = []
    elif db_type == "underl_ib_hist":
        return1 = ["expiry","NO_EXPIRY","symbol","load_dttm"]

    return return1

def formated_string_for_file(expiry, expiry_in_format):
    """
    Return formated string to be used to name the DB file for each expiry date
    """
    return dt.datetime.strptime(expiry, expiry_in_format).strftime('%Y-%m')

def write_market_data_to_sqllite(globalconf, log, dataframe, db_type):
    """
    Write to sqllite the market data snapshot passed as argument
    """
    log.info("Appending market data to sqllite ... ")
    path = globalconf.config['paths']['data_folder']
    criteria = get_partition_names(db_type)
    expiries = dataframe[criteria[0]].unique().tolist()
    log.info(("These are the expiries included in the data to be loaded: ", expiries))
    # expiries_file = map(lambda i: formated_string_for_file(i,criteria[1]) , expiries)
    # remove empty string expiries (bug in H5 legacy files)
    expiries = [x for x in expiries if x]

    for expiry in expiries:
        if criteria[1] == "NO_EXPIRY":
            expiry_file = ""
        else:
            expiry_file = formated_string_for_file(expiry, criteria[1])
        db_file = get_market_db_file(globalconf,db_type,expiry_file)
        store = sqlite3.connect(path + db_file)
        symbols = dataframe[criteria[2]].unique().tolist()
        log.info(("For expiry: ",expiry," these are the symbols included in the data to be loaded:  ", symbols))
        # remove empty string symbols (bug in H5 legacy files)
        symbols = [x for x in symbols if x]
        for name in symbols:
            name = name.replace("^", "")
            joe = dataframe.loc[ (dataframe[criteria[2]] == name) & (dataframe[criteria[0]] == expiry) ]
            #joe.sort(columns=['current_datetime'], inplace=True)  DEPRECATED
            # remove this field which is not to be used
            if 'Halted' in joe.columns:
                joe = joe.drop(['Halted'], axis=1)
            joe = joe.sort_values(by=[criteria[3]])
            if 'Expiry' in joe.columns:
                joe = joe.drop(['Expiry'], axis=1)
            joe.to_sql(name, store, if_exists='append')
        store.close()



def store_optchain_yahoo_to_db():
    globalconf = config.GlobalConfig()
    optchain_def = globalconf.get_tickers_optchain_yahoo()
    source1 = globalconf.config['use_case_yahoo_options']['source']
    log = logger("yahoo options chain")
    log.info("Getting options chain data from yahoo w pandas_datareader ... [%s]"
             % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') ))

    for symbol,row in optchain_def.iterrows():
        log.info("Init yahoo quotes downloader symbol=%s" % (symbol) )
        try:
            option = web.Options(symbol,source1)
            for j in option.expiry_dates:
                log.info("expiry=%s" % (str(j)))
                try:
                    joe = pd.DataFrame()
                    call_data = option.get_call_data(expiry=j)
                    put_data = option.get_put_data(expiry=j)
                    if call_data.values.any():
                        joe = joe.append(call_data)
                    if put_data.values.any():
                        joe = joe.append(put_data)
                    joe = joe.sort_index()
                    joe['Quote_Time_txt'] = joe['Quote_Time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                    joe['Last_Trade_Date_txt'] = joe['Last_Trade_Date'].dt.strftime("%Y-%m-%d %H:%M:%S")
                    joe = joe.reset_index().set_index("Quote_Time")
                    joe['Expiry_txt'] = joe['Expiry'].dt.strftime("%Y-%m-%d %H:%M:%S")
                    if 'JSON' in joe.columns:
                        joe['JSON'] = ""

                    write_market_data_to_sqllite(globalconf, log, joe, "optchain_yhoo")

                except KeyError as e:
                    log.warn("KeyError raised [" + str(e) + "]...")

        except (RemoteDataError,TypeError) as err:
            log.info("No information for ticker [%s] Error=[%s] sys_info=[%s]" % (str(symbol) , str(err) , sys.exc_info()[0] ))
            continue


def CustomParser(data):
    # eval convert unicode string to dictionary
    j1 = eval(data)
    return j1


