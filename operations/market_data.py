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
    # remove empty string expiries (bug in H5 legacy files)
    expiries = [x for x in expiries if x]

    for expiry in expiries:
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

                    write_market_data_to_sqllite(globalconf, log, joe, "optchain_yhoo")

                except KeyError as e:
                    log.warn("KeyError raised [" + str(e) + "]...")

        except RemoteDataError as err:
            log.info("No information for ticker [%s] RemoteDataError=[%s] sys_info=[%s]" % (str(symbol) , str(err) , sys.exc_info()[0] ))
            continue


def CustomParser(data):
    # eval convert unicode string to dictionary
    j1 = eval(data)
    return j1


def migrate_h5_to_sqllite_optchain_yahoo():
    """
    migrate_h5_to_sqllite_optchain_yahoo
    """
    hdf5_pattern = "optchain_yahoo_*.h5*"
    h5_db_alias = "optchain_yhoo"
    migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias,True)


def migrate_h5_to_sqllite_optchain_ib():
    """
    migrate_h5_to_sqllite_optchain_ib
    """
    hdf5_pattern = "optchain_ib_*.h5*"
    h5_db_alias = "optchain_ib"
    migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias,False)


def migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias,drop_expiry):
    """
         
    """
    globalconf = config.GlobalConfig()
    log = logger("migrate_h5_to_sqllite_optchain")
    path = globalconf.config['paths']['data_folder']
    lst1 = glob.glob(path + hdf5_pattern)
    if not lst1:
        log.info("No h5 files to append ... ")
    else:
        log.info(("List of h5 files that will be appended: ", lst1))
        time.sleep(1)
        try:
            input("Press Enter to continue...")
        except SyntaxError:
            pass

        for hdf5_path in lst1:
            store_file = pd.HDFStore(hdf5_path)
            root1 = store_file.root
            # todos los nodos hijos de root que son los underlying symbols
            list = [x._v_pathname for x in root1]
            log.info(("Processing file: ", hdf5_path))

            store_file.close()
            log.info(("List of symbols: " + str(list)))
            for symbol in list:
                store_file = pd.HDFStore(hdf5_path)
                node1 = store_file.get_node(symbol)
                if node1:
                    log.info(("Symbol: " + symbol))
                    df1 = store_file.select(node1._v_pathname)
                    # Unfortunately th JSON field doesnt contain more info that the already present fields
                    # df1['json_dict'] = df1['JSON'].apply(CustomParser)
                    # following line converts dict column into n columns for the dataframe:
                    # https://stackoverflow.com/questions/20680272/reading-a-csv-into-pandas-where-one-column-is-a-json-string
                    # df1 = pd.concat([df1.drop(['json_dict','JSON'], axis=1), df1['json_dict'].apply(pd.Series)], axis=1)
                    # df1 = df1.drop(['JSON'], axis=1)
                    # Expiry is already in the index
                    if drop_expiry == True:
                        df1 = df1.drop(['Expiry'], axis=1)
                    write_market_data_to_sqllite(globalconf, log, df1, h5_db_alias)
                store_file.close()


if __name__ == "__main__":
    #migrate_h5_to_sqllite_optchain_yahoo()
    migrate_h5_to_sqllite_optchain_ib()