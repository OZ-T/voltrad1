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
        return1 = ['Expiry']
    elif db_type == "optchain_yhoo":
        return1 = ["Expiry_txt","%Y-%m-%d  %H:%M:%S","Underlying","Quote_Time_txt"]
    elif db_type == "optchain_ib_hist":
        return1 = []
    elif db_type == "underl_ib_hist":
        return1 = []

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

    for expiry in expiries:
        expiry_file = formated_string_for_file(expiry, criteria[1])
        db_file = get_market_db_file(globalconf,db_type,expiry_file)
        store = sqlite3.connect(path + db_file)
        symbols = dataframe[criteria[2]].unique().tolist()
        log.info(("These are the symbols included in the data to be loaded:  ", symbols))
        for name in symbols:
            name = name.replace("^", "")
            joe = dataframe.loc[ (dataframe[criteria[2]] == name) & (dataframe[criteria[0]] == expiry) ]
            #joe.sort(columns=['current_datetime'], inplace=True)  DEPRECATED
            joe = joe.sort_values(by=[criteria[3]])
            joe.to_sql(name, store, if_exists='append')
        store.close()


def CustomParser(data):
    # eval convert unicode string to dictionary
    j1 = eval(data)
    return j1

def partition_data_set_by_option_expiry():
    """
         
    """
    globalconf = config.GlobalConfig()
    log = logger("partition_data_set_by_option_expiry")
    hdf5_path = "C:/Users/David/data/optchain_yahoo_db_expiry_2017-03.h5"
    store_file = pd.HDFStore(hdf5_path)
    root1 = store_file.root
    # todos los nodos hijos de root que son los underlying symbols
    list = [x._v_pathname for x in root1]
    log.info(("Root pathname of the input store: ", root1._v_pathname))

    store_file.close()
    log.info(("List of symbols: " + str(list)))
    for symbol in list:
        store_file = pd.HDFStore(hdf5_path)
        node1 = store_file.get_node(symbol)
        if node1:
            df1 = store_file.select(node1._v_pathname)
            # Unfortunately th JSON field doesnt contain more info that the already present fields
            # df1['json_dict'] = df1['JSON'].apply(CustomParser)
            # following line converts dict column into n columns for the dataframe:
            # https://stackoverflow.com/questions/20680272/reading-a-csv-into-pandas-where-one-column-is-a-json-string
            # df1 = pd.concat([df1.drop(['json_dict','JSON'], axis=1), df1['json_dict'].apply(pd.Series)], axis=1)
            # df1 = df1.drop(['JSON'], axis=1)
            # Expiry is already in the index
            df1 = df1.drop(['Expiry'], axis=1)
            write_market_data_to_sqllite(globalconf, log, df1, "optchain_yhoo")
        store_file.close()


if __name__ == "__main__":
    partition_data_set_by_option_expiry()