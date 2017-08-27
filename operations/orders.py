""" Module with orders related methods.
"""

import datetime as dt
import sqlite3

import pandas as pd

import volibutils.sync_client as ib
import volsetup.config as config
from core import misc_utilities as utils
from volsetup.logger import logger


def write_orders_to_sqllite(globalconf, log, dataframe):
    """
    Write to sqllite the orders snapshot passed as argument
    """
    log.info("Appending orders data to sqllite ... ")
    db_file = globalconf.config['sqllite']['orders_db']
    path = globalconf.config['paths']['data_folder']
    store = sqlite3.connect(path + db_file)
    # get a list of names
    names = dataframe['account'].unique().tolist()
    for name in names:
        # now we can perform a lookup on a 'view' of the dataframe
        log.info("Storing " + name + " in ABT ...")
        joe = dataframe.loc[dataframe['account'] == name]
        #joe.sort(columns=['current_datetime'], inplace=True)  DEPRECATED
        joe = joe.sort_values(by=['current_datetime'])
        joe.to_sql(name, store, if_exists='append')
    store.close()


def store_orders_from_ib_to_db():
    """
    Method to retrieve orders -everything from the last business day-, intended for batch usage     
    """
    log=logger("store_orders_from_ib_to_sqllite")
    if dt.datetime.now().date() in utils.get_trading_close_holidays(dt.datetime.now().year):
        log.info("This is a US Calendar holiday. Ending process ... ")
        return

    log.info("Getting orders data from IB ... ")
    globalconf = config.GlobalConfig()
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    ## Get the executions (gives you everything for last business day)
    execlist = client.get_executions(10)
    client.disconnect()
    log.info("execlist length = [%d]" % ( len(execlist) ))
    if execlist:
        dataframe = pd.DataFrame.from_dict(execlist).transpose()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        log.info("Appending orders to sqllite store ...")
        dataframe=dataframe.sort_values(by=['account'])
        dataframe.set_index(keys=['execid'], drop=True, inplace=True)
        write_orders_to_sqllite(globalconf, log, dataframe)
    else:
        log.info("No orders to append ...")
