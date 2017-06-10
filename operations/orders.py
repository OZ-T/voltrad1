""" Module with orders related methods.
"""

import volibutils.sync_client as ib
from volsetup.logger import logger
from volutils import utils as utils
import glob
import os
import volsetup.config as config
import pandas as pd
import datetime as dt
import sqlite3

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
        # sort the dataframe
        #dataframe.sort(columns=['account'], inplace=True) DEPRECATED
        dataframe=dataframe.sort_values(by=['account'])
        # set the index to be this and don't drop
        dataframe.set_index(keys=['account'], drop=False, inplace=True)
        write_orders_to_sqllite(globalconf, log, dataframe)
    else:
        log.info("No orders to append ...")



def consolidate_anciliary_h5_orders():
    globalconf = config.GlobalConfig()
    path = globalconf.config['paths']['data_folder']
    # path = '/home/david/data/'
    # http://stackoverflow.com/questions/2225564/get-a-filtered-list-of-files-in-a-directory
    os.chdir(path)
    orders_orig = 'orders_db.h5'
    pattern_orders = 'orders_db.h5*'
    orders_out = 'orders_db_new.h5'
    lst1 = glob.glob(pattern_orders)
    lst1.remove(orders_orig)
    print (lst1)
    dataframe = pd.DataFrame()
    for x in lst1:
        store_in1 = pd.HDFStore(path + x)
        root1 = store_in1.root
        print (root1._v_pathname)
        for lvl1 in root1:
            print (lvl1._v_pathname)
            if lvl1:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                print ("store_in1", len(df1), x)
        store_in1.close()

    store_in1 = pd.HDFStore(path + orders_orig)
    store_out = pd.HDFStore(path + orders_out)
    root1 = store_in1.root
    print (root1._v_pathname)
    for lvl1 in root1:
        print (lvl1._v_pathname)
        if lvl1:
            df1 = store_in1.select(lvl1._v_pathname)
            dataframe = dataframe.append(df1)
            print ("store_in1", len(df1), orders_orig)
    store_in1.close()

    dataframe.sort_index(inplace=True,ascending=[True])
    names = dataframe['account'].unique().tolist()
    for name in names:
        print ("Storing " + name + " in ABT ..." + str(len(dataframe)))
        joe = dataframe[dataframe.account == name]
        joe=joe.sort_values(by=['current_datetime'])
        store_out.append("/" + name, joe, data_columns=True)
    store_out.close()


def store_orders_from_ib_to_h5():
    """
    Method to retrieve orders -everything from the last business day-, intended for batch usage     
    """
    log=logger("store_orders_from_ib_to_h5")
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
        f = globalconf.open_orders_store()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        log.info("Appending orders to HDF5 store ...")
        # sort the dataframe
        #dataframe.sort(columns=['account'], inplace=True) DEPRECATED
        dataframe=dataframe.sort_values(by=['account'])
        # set the index to be this and don't drop
        dataframe.set_index(keys=['account'], drop=False, inplace=True)
        # get a list of names
        names = dataframe['account'].unique().tolist()

        for name in names:
            # now we can perform a lookup on a 'view' of the dataframe
            log.info("Storing " + name + " in ABT ...")
            joe = dataframe.loc[dataframe['account'] == name]
            #joe.sort(columns=['current_datetime'], inplace=True)  DEPRECATED
            joe = joe.sort_values(by=['current_datetime'])
            try:
                f.append("/" + name, joe, data_columns=joe.columns)
            except ValueError as e:
                log.warn("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
                aux = globalconf.open_orders_store_value_error()
                aux.append("/" + name, joe, data_columns=True)
                aux.close()
        f.close()
    else:
        log.info("No orders to append ...")


if __name__=="__main__":
    store_orders_from_ib_to_h5()

