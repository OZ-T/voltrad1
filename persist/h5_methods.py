import datetime
import datetime as dt
import glob
import os
import time
from datetime import datetime, timedelta
from time import sleep

import numpy as np
import pandas as pd
from pytz import timezone
from tables.exceptions import NaturalNameWarning

import persist.sqlite_methods
import persist.sqlite_methods as mkt
import operations.crud as acc
import ibutils.sync_client as ib
from core import misc_utilities, config
from ibutils.RequestUnderlyingData import RequestUnderlyingData
from ibutils.sync_client import IBClient
from core.logger import logger
from persist.portfolio_and_account_data_methods import globalconf, OPT_NUM_FIELDS_LST, log
from persist.sqlite_methods import log, globalconf, OPT_NUM_FIELDS_LST
from valuations.opt_pricing_methods import bsm_mcs_euro


def migrate_h5(what_to_migrate,filter_symbol):
    """
    what_to_migrate -> ["portfolio"|"acc_summary"|"optchain_yahoo"|"underly_hist"|"optchain_ib"|"orders"||]
    filter_symbol only required for yhoo method
    """
    if what_to_migrate == "portfolio":
        migrate_h5_to_sqllite_portfolio()
    elif what_to_migrate == "acc_summary":
        migrate_h5_to_sqllite_acc_summary()
    elif what_to_migrate == "optchain_yahoo":
        migrate_h5_to_sqllite_optchain_yahoo(filter_symbol)
    elif what_to_migrate == "underly_hist":
        migrate_h5_to_sqllite_underly_hist()
    elif what_to_migrate == "optchain_ib":
        migrate_h5_to_sqllite_optchain_ib()
    elif what_to_migrate == "orders":
        migrate_h5_to_sqllite_orders()


def migrate_h5_to_sqllite_optchain_yahoo(filter_symbol):
    """
    migrate_h5_to_sqllite_optchain_yahoo
    """
    hdf5_pattern = "optchain_yahoo_*.h5*"
    h5_db_alias = "optchain_yhoo"
    migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias, True, filter_symbol)


def migrate_h5_to_sqllite_underly_hist():
    """
    migrate_h5_to_sqllite_optchain_yahoo
    """
    hdf5_pattern = "underly_ib_hist_*.h5*"
    h5_db_alias = "underl_ib_hist"
    migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias, False, "ALL")


def migrate_h5_to_sqllite_optchain_ib():
    """
    migrate_h5_to_sqllite_optchain_ib
    """
    hdf5_pattern = "optchain_ib_*.h5*"
    h5_db_alias = "optchain_ib"
    migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias, False, "ALL")


def migrate_h5_to_sqllite_optchain(hdf5_pattern, h5_db_alias, drop_expiry, filter_symbol):
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
            # only migrate the symbol indicated if available
            if filter_symbol != "ALL":
                list = [filter_symbol]
            store_file.close()
            log.info(("List of symbols: " + str(list)))
            for symbol in list:
                store_file = pd.HDFStore(hdf5_path)
                node1 = store_file.get_node(symbol)
                if node1:
                    log.info(("Symbol: " + symbol))
                    # Unfortunately th JSON field doesnt contain more info that the already present fields
                    # df1['json_dict'] = df1['JSON'].apply(CustomParser)
                    # following line converts dict column into n columns for the dataframe:
                    # https://stackoverflow.com/questions/20680272/reading-a-csv-into-pandas-where-one-column-is-a-json-string
                    # df1 = pd.concat([df1.drop(['json_dict','JSON'], axis=1), df1['json_dict'].apply(pd.Series)], axis=1)
                    # df1 = df1.drop(['JSON'], axis=1)


                    # this is a specifc case for underlying hisotry
                    if symbol == "/ES" and h5_db_alias == "underl_ib_hist":
                        for lvl1 in node1:
                            log.info(("Level 1 pathname in the root if the H5: ", lvl1._v_pathname))
                            if lvl1:
                                df1 = store_file.select(lvl1._v_pathname)
                                df1['expiry'] = lvl1._v_pathname
                                mkt.write_market_data_to_sqllite(df1, h5_db_alias)
                    else:
                        df1 = store_file.select(node1._v_pathname)
                    # Expiry is already in the index
                    if drop_expiry == True:
                        df1 = df1.drop(['Expiry'], axis=1)
                    mkt.write_market_data_to_sqllite(df1, h5_db_alias)
                store_file.close()


def migrate_h5_to_sqllite_orders():
    """
    migrate_h5_to_sqllite_orders
    """
    hdf5_pattern = "orders_db*.h5*"
    globalconf = config.GlobalConfig()
    log = logger("migrate_h5_to_sqllite_orders")
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
            # todos los nodos hijos de root que son los account ids
            list = [x._v_pathname for x in root1]
            log.info(("Root pathname of the input store: ", root1._v_pathname))

            store_file.close()
            log.info(("List of account ids: " + str(list)))
            for accountid in list:
                store_file = pd.HDFStore(hdf5_path)
                node1 = store_file.get_node(accountid)
                if node1:
                    log.info(("accountid: " + accountid))
                    df1 = store_file.select(node1._v_pathname)
                    df1.set_index(keys=['execid'], drop=True, inplace=True)
                    persist.sqlite_methods.write_orders_to_sqllite(df1)
                store_file.close()



def migrate_h5_to_sqllite_portfolio():
    """
    migrate_h5_to_sqllite_portfolio
    """
    hdf5_pattern = "portfolio_db*.h5*"
    globalconf = config.GlobalConfig()
    log = logger("migrate_h5_to_sqllite_portfolio")
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
            # todos los nodos hijos de root que son los account ids
            list = [x._v_pathname for x in root1]
            log.info(("Root pathname of the input store: ", root1._v_pathname))

            store_file.close()
            log.info(("List of account ids: " + str(list)))
            for accountid in list:
                store_file = pd.HDFStore(hdf5_path)
                node1 = store_file.get_node(accountid)
                if node1:
                    log.info(("accountid: " + accountid))
                    df1 = store_file.select(node1._v_pathname)
                    df1.set_index(keys=['conId'], drop=True, inplace=True)
                    persist.sqlite_methods.write_portfolio_to_sqllite(df1)
                store_file.close()


def migrate_h5_to_sqllite_acc_summary():
    """
    migrate_h5_to_sqllite_acc_summary
    """
    hdf5_pattern = "account_db*.h5*"
    globalconf = config.GlobalConfig()
    log = logger("migrate_h5_to_sqllite_acc_summary")
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
            # todos los nodos hijos de root que son los account ids
            list = [x._v_pathname for x in root1]
            log.info(("Root pathname of the input store: ", root1._v_pathname))

            store_file.close()
            log.info(("List of account ids: " + str(list)))
            for accountid in list:
                store_file = pd.HDFStore(hdf5_path)
                node1 = store_file.get_node(accountid)
                if node1:
                    log.info(("accountid: " + accountid))
                    df1 = store_file.select(node1._v_pathname)
                    persist.sqlite_methods.write_acc_summary_to_sqllite(df1)
                store_file.close()


def write_portfolio_to_h5(globalconf, log, dataframe, store):
    """
    Write to h5 the portfolio snapshot passed as argument
    """
    log.info("Appending portfolio data to HDF5 ... ")
    names=dataframe['accountName'].unique().tolist()
    for name in names:
        joe = dataframe.loc[dataframe['accountName']==name]
        try:
            store.append("/" + name, joe, data_columns=True)
        except NaturalNameWarning as e:
            log.warn("NaturalNameWarning raised [" + str(e))
        except (ValueError) as e:
            log.warn("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
            aux = globalconf.portfolio_store_error()
            aux.append("/" + name, joe, data_columns=True)
            aux.close()
        store.close()


def read_historical_portfolio_from_h5(globalconf, log, accountid):
    """
    Read from h5 the complete history of the portfolio and returns as dataframe
    """
    store = globalconf.portfolio_store()
    node = store.get_node("/" + accountid)
    df1 = store.select(node._v_pathname)
    df1['date1']=df1.index.map(lambda x: x.date())
    df1= df1.drop_duplicates(subset=['date1'],keep='last')
    store.close()
    return df1


def read_historical_acc_summary_from_h5(globalconf, log, accountid):
    """
    Read from h5 the complete history of the account summary and returns as dataframe
    """
    store = globalconf.account_store_new()
    node = store.get_node("/" + accountid)
    df1 = store.select(node._v_pathname)
    df1['date1']=df1.index.map(lambda x: x.date())
    df1= df1.drop_duplicates(subset=['date1'],keep='last')
    store.close()
    return df1


def write_acc_summary_to_h5(globalconf, log, dataframe2,store_new):
    """
    Write to h5 the account summary passed as argument
    """
    # get a list of names
    names=dataframe2['AccountCode_'].unique().tolist()
    for name in names:
        # now we can perform a lookup on a 'view' of the dataframe
        joe = dataframe2.loc[dataframe2['AccountCode_']==name]
        node=store_new.get_node("/" + name)
        if node:
            log.info("Getting columns names in account store HDF5 ... ")
            dftot = store_new.select(node._v_pathname)
            cols = list(dftot.columns.values)
            cols.sort()
            colsjoe=list(joe.columns.values)
            colsfinal = list(set(cols).intersection(colsjoe))
            joe = joe[colsfinal]

        log.info("Appending account data to HDF5 ... ")
        # Following 3 lines is to fix following error when storing in HDF5:
        #       [unicode] is not implemented as a table column
        types = joe.apply(lambda x: pd.lib.infer_dtype(x.values))
        for col in types[types == 'unicode'].index:
            joe[col] = joe[col].astype(str)
        #print joe.dtypes
        try:
            store_new.append("/" + name, joe, data_columns=True)
        except NaturalNameWarning as e:
            log.warn("NaturalNameWarning raised [" + str(e))
        except (ValueError) as e:
            log.warn("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
            aux = globalconf.account_store_new_error()
            aux.append("/" + name, joe, data_columns=True)
            aux.close()
        store_new.close()


def consolidate_anciliary_h5_portfolio():
    """
    Used as command to consolidate in the main h5 anciliary h5 generated due to column length exceptions
    """
    globalconf = config.GlobalConfig()
    log = logger("consolidate_anciliary_h5_portfolio")
    path = globalconf.config['paths']['data_folder']
    os.chdir(path)
    if not os.path.exists(path + "/portfolio_backups"):
        os.makedirs(path + "/portfolio_backups")

    port_orig = 'portfolio_db.h5'
    pattern_port = 'portfolio_db.h5*'
    port_out = 'portfolio_db_complete.h5'

    lst1 = glob.glob(pattern_port)
    lst1.remove(port_orig)
    dataframe = pd.DataFrame()
    old_format = False
    if not lst1:
        log.info("No ancilliary files to append ... ")
    else:
        log.info(("List of ancilliary files that will be appended: ", lst1))
        for x in lst1:
            store_in1 = pd.HDFStore(path + x)
            root1 = store_in1.root
            log.info(("Root pathname of the input store: ", root1._v_pathname))
            for lvl1 in root1:
                log.info(("Level 1 pathname in the root if the H5: ", x, lvl1._v_pathname))
                if lvl1:
                    try:
                        df1 = store_in1.select(lvl1._v_pathname)
                        dataframe = dataframe.append(df1)
                        log.info(("Store_in1", len(df1), x))
                    except (TypeError) as e:
                        log.info("This is the old format of the portfolio file...")
                        old_format = True
                        break
            if old_format:
                for lvl1 in root1:
                    for lvl2 in store_in1.get_node(lvl1._v_pathname):
                        for lvl3 in store_in1.get_node(lvl2._v_pathname):
                            for lvl4 in store_in1.get_node(lvl3._v_pathname):
                                for lvl5 in store_in1.get_node(lvl4._v_pathname):
                                    log.info(("Pathname level 5: ", x, lvl5._v_pathname))
                                    if lvl5:
                                        df1 = store_in1.select(lvl5._v_pathname)
                                        dataframe = dataframe.append(df1)

            store_in1.close()
            os.rename(path + x, path + "/portfolio_backups/" + x)

    store_in1 = pd.HDFStore(path + port_orig)
    store_out = pd.HDFStore(path + port_out)
    root1 = store_in1.root
    root2 = store_out.root
    old_format = False
    log.info(("Root pathname of the input store: ", root1._v_pathname, " and output the store: ", root2._v_pathname))
    for lvl1 in root1:
        log.info(("Level 1 pathname in the root if the H5: ", port_orig, lvl1._v_pathname))
        if lvl1:
            try:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                log.info(("Store_in1", len(df1), port_orig))
            except (TypeError) as e:
                log.info("This is the old format of the portfolio file...")
                old_format = True
                break
    if old_format:
        for lvl1 in root1:
            for lvl2 in store_in1.get_node(lvl1._v_pathname):
                for lvl3 in store_in1.get_node(lvl2._v_pathname):
                    for lvl4 in store_in1.get_node(lvl3._v_pathname):
                        for lvl5 in store_in1.get_node(lvl4._v_pathname):
                            log.info(("Pathname level 5: ", port_orig, lvl5._v_pathname))
                            if lvl5:
                                df1 = store_in1.select(lvl5._v_pathname)
                                dataframe = dataframe.append(df1)

    store_in1.close()
    os.rename(path + port_orig, path + "/portfolio_backups/" + datetime.now().strftime('%Y%m%d%H%M%S') + port_orig)
    dataframe.sort_values(by=['current_datetime'], inplace=True)
    dataframe = dataframe.dropna(subset=['current_datetime'])
    dataframe.drop('multiplier', axis=1, inplace=True)
    # dataframe.drop('multiplier', axis=1, inplace=True)
    write_portfolio_to_h5(globalconf, log, dataframe, store_out)
    store_out.close()
    os.rename(path + port_out, path + port_orig)


def consolidate_anciliary_h5_account():
    """
    Used as command to consolidate in the main h5 anciliary h5 generated due to column length exceptions
    """
    globalconf = config.GlobalConfig(level=logger.DEBUG)
    log = globalconf.log
    path = globalconf.config['paths']['data_folder']
    os.chdir(path)
    if not os.path.exists(path + "/account_backups"):
        os.makedirs(path + "/account_backups")

    acc_orig = 'account_db_new.h5'
    pattern_acc = 'account_db_new.h5*'
    acc_out = 'account_db_complete.h5'

    lst1 = glob.glob(pattern_acc)
    lst1.remove(acc_orig)
    if not lst1:
        log.info("No ancilliary files to append, exiting ... ")
        return

    log.info(("List of ancilliary files that will be appended: ", lst1))
    dataframe = pd.DataFrame()
    for x in lst1:
        store_in1 = pd.HDFStore(path + x)
        root1 = store_in1.root
        log.info(("Root pathname of the input store: ", root1._v_pathname))
        for lvl1 in root1:
            log.info(("Level 1 pathname in the root if the H5: ", lvl1._v_pathname))
            if lvl1:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                log.info(("Store_in1", len(df1), x))
        store_in1.close()
        os.rename(path + x, path + "/account_backups/" + x)

    store_in1 = pd.HDFStore(path + acc_orig)
    store_out = pd.HDFStore(path + acc_out)
    root1 = store_in1.root
    log.info(("Root pathname of the input store: ", root1._v_pathname))
    root2 = store_out.root
    log.info(("Root pathname of the output store: ", root2._v_pathname))

    for lvl1 in root1:
        print (lvl1._v_pathname)
        if lvl1:
            df1 = store_in1.select(lvl1._v_pathname)
            dataframe = dataframe.append(df1)
            log.info(("Store_in1 length and name", len(df1), acc_orig))
    store_in1.close()
    os.rename(path + acc_orig, path + "/account_backups/" + datetime.now().strftime('%Y%m%d%H%M%S') + acc_orig)
    dataframe.sort_index(inplace=True,ascending=[True])
    write_acc_summary_to_h5(globalconf, log, dataframe, store_out)
    store_out.close()
    os.rename(path + acc_out, path + acc_orig)


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
    if dt.datetime.now().date() in misc_utilities.get_trading_close_holidays(dt.datetime.now().year):
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



def historical_data_loader():
    """
    ADVERTENCIA USO DATOS HISTORICOS:

    Se inserta un registro duplicado en cada carga incremental. Es decir:
        se vuelve a insertar la barra de la ultima media hora que estaba cargada ya en el hdf5
        y tipicamente el close de esta barra es distinto al cargado inicialmente.
        La analitica que se haga sobre esta tabla debe contemplar eliminar primero de los registros duplicados
        porque asumimos que el segundo es el valido (dado que es igual al open de la siguiente barra de media hora
        como se hgha observado)
        este error se puede eliminar o mitigar si solamente se piden los datos histoticos con el mercado cerrado
        que es lo que se hace en el modo automatico (crontab) Validar esto.
    :return:
    """
    log=logger("historical data loader")
    log.info("Getting historical underlying data from IB ... ")

    globalconf = config.GlobalConfig()
    underly_def = globalconf.get_tickers_historical_ib()
    client = IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    dt_now=datetime.now()
    endDateTime =  dt_now.strftime('%Y%m%d %H:%M:%S')
    # lo mas que se puede pedir para barras de 30 min es un mes historico
    # barSizeSetting = "30 mins"
    barSizeSetting = "1 min"
    whatToShow = "TRADES"
    useRTH = 1
    formatDate = 1
    wait_secs = 40
    f = globalconf.open_historical_store()

    for index, row_req in underly_def.iterrows():
        log.info("underl=[%s] [%s] [%s] [%d] [%s] [%s] [%s] [%s] [%d]"
                        % ( str(row_req['symbol']), str(row_req['underl_type']),
                            str(row_req['underl_expiry']), 0, '', '',
                            str(row_req['underl_ex']), str(row_req['underl_curr']), int(index) ) )

        ticker = RequestUnderlyingData(str(row_req['symbol']), str(row_req['underl_type']), str(row_req['underl_expiry']), 0, '', '',
                                       str(row_req['underl_ex']), str(row_req['underl_curr']), int(index))

        path_h5 = "/" + str(row_req['symbol'])
        if long(row_req['underl_expiry']) > 0:
            path_h5 = path_h5 + "/" + str(row_req['underl_expiry'])
        last_record_stored = 0
        node = f.get_node(path_h5)
        if node:
            df1 = f.select(node._v_pathname)
            df1= df1.reset_index()['date']
            last_record_stored = datetime.strptime(str(df1.max()), '%Y%m%d %H:%M:%S')
            # no se debe usar .seconds que da respuesta incorrecta debe usarse .total_seconds()
            #days= int(round( (dt_now - last_record_stored).total_seconds() / 60 / 60 / 24  ,0))
            # lo anterior no sirve porque debe considerarse la diferencia en business days
            #days = np.busday_count(last_record_stored.date(), dt_now.date())
            bh= misc_utilities.BusinessHours(last_record_stored, dt_now, worktiming=[15, 21], weekends=[6, 7])
            days = bh.getdays()
            durationStr = str( days ) + " D"
        else:
            durationStr = "30 D"
            barSizeSetting = "30 mins"

        if str(row_req['symbol']) in ['NDX','SPX','VIX']:
            barSizeSetting = "30 mins"

        log.info( "last_record_stored=[%s] endDateTime=[%s] durationStr=[%s] barSizeSetting=[%s]"
                  % ( str(last_record_stored), str(endDateTime) , durationStr, barSizeSetting) )

        historicallist = client.get_historical(ticker, endDateTime, durationStr, barSizeSetting,whatToShow, useRTH,formatDate)
        #print historicallist
        dataframe = pd.DataFrame()
        if historicallist:
            for reqId, request in historicallist.items():
                for date, row in request.items():
                    # print ("date [%s]: row[%s]" % (date, str(row)))
                    temp1 = pd.DataFrame(row, index=[0])
                    temp1['symbol'] = str(row_req['symbol'])
                    temp1['expiry'] = str(row_req['underl_expiry'])
                    temp1['type'] = str(row_req['underl_type'])
                    temp1['load_dttm'] = endDateTime
                    dataframe = dataframe.append(temp1.reset_index().drop('index', 1))

            dataframe = dataframe.sort_values(by=['date']).set_index('date')
            log.info( "appending data in hdf5 ...")
            f.append(path_h5, dataframe, data_columns=dataframe.columns)
        log.info("sleeping [%s] secs ..." % (str(wait_secs)))
        sleep(wait_secs)

    client.disconnect()
    f.close()  # Close file


def extrae_options_chain2(start_dttm,end_dttm,symbol,expiry,secType):
    """
        extraer de la hdf5 los datos de cotizaciones entre dos fechas
        imputa valores ausente con el metodo ffill de pandas dataframe dentro del dia
    """
    store = globalconf.open_ib_h5_store()
    store_txt = store.filename
    log.info("extrae_options_chain2 [%s]: start [%s] end [%s] " % (str(store_txt), str(start_dttm), str(end_dttm)))
    dataframe = pd.DataFrame()
    sym1= store.get_node("/"+symbol)
    where1=['symbol==' + symbol,
            'secType==' + secType,
            'current_date>'     + str(start_dttm.year)
                                 + str(start_dttm.month).zfill(2)
                                 + str(start_dttm.day).zfill(2),
            'current_date<=' + str(end_dttm.year)
                                 + str(end_dttm.month).zfill(2)
                                 + str(end_dttm.day).zfill(2)
            ]
    df1 = store.select(sym1._v_pathname, where=where1)
    log.info("Number of rows loaded from h5 option chain file: [%d] where=[%s]" % ( len(df1) , str(where1)))
    df1['load_dttm'] = pd.to_datetime(df1['current_datetime'], errors='coerce')  # DEPRECATED remove warning coerce=True)
    df1['current_datetime_txt'] = df1.index.strftime("%Y-%m-%d %H:%M:%S")
    log.info("append data frame ... ")
    dataframe = dataframe.append(df1)
    log.info("close store h5 ... ")
    store.close()

    dataframe[OPT_NUM_FIELDS_LST] = dataframe[OPT_NUM_FIELDS_LST].apply(pd.to_numeric)
    dataframe['load_dttm'] = dataframe['load_dttm'].apply(pd.to_datetime)
    # imputar valores ausentes con el valor justo anterior (para este dia)
    log.info("drop_duplicates ... ")
    dataframe = dataframe.drop_duplicates(subset=['right','strike','expiry','load_dttm'], keep='last')
    log.info("sort_values ... ")
    dataframe = dataframe.sort_values(by=['right','strike','expiry','load_dttm'],
                                      ascending=[True, True, True, True]).groupby(
                                        ['right','strike','expiry'], #,'load_dttm'],
                                      as_index=False).apply(lambda group: group.ffill())
    dataframe= dataframe.replace([-1],[0])

    localtz = timezone('Europe/Madrid')
    log.info("localize tz ... ")
    dataframe.index = dataframe.index.map(lambda x: localtz.localize(x))
    dataframe.index = dataframe.index.map(lambda x: x.replace(tzinfo=None))

    return dataframe


def read_biz_calendar(start_dttm, valuation_dttm,log,globalconf):
    # leer del h5 del yahoo biz calendar
    log.info("read_biz_calendar: [%s] " % (str(valuation_dttm)))
    year= str(valuation_dttm.year)     # "2016"
    store = globalconf.open_economic_calendar_h5_store()
    sym1= store.get_node("/"+year)
    dataframe = pd.DataFrame()
    df1 = store.select(sym1._v_pathname)
    store_txt = store.filename
    log.info("Number of rows loaded from h5 economic calendar[%s]: [%d]" % ( str(store_txt), len(df1)))
    dataframe = dataframe.append(df1)
    store.close()

    # construir un dataframe con los eventos y las fechas convertidas a datetime
    dataframe['event_datetime'] = dataframe.Date+" "+year+" "+dataframe.Time_ET
    dataframe['event_datetime']=dataframe['event_datetime'].apply(
                                        lambda x: datetime.strptime(x, '%b %d %Y %I:%M %p'))

    # convertir las hora que estan en el horario de la costa este de US creo (mirar en la web)
    localtz = timezone('US/Eastern')
    dataframe['event_datetime'] = dataframe['event_datetime'].apply(
                                        lambda x: localtz.localize(x))
    dataframe['event_datetime'] = dataframe['event_datetime'].apply(
                                        lambda x: x.astimezone(timezone("Europe/Madrid")).replace(tzinfo=None))

    # eliminar los duplicados (quedarse con los registros historicos que ya tienen el dato real
    dataframe = dataframe.reset_index().drop_duplicates(subset=['event_datetime','Briefing_Forecast','For',
                                                                'Statistic'],
                                                                keep='last').set_index('event_datetime', drop=0)

    dataframe.set_index(keys=['event_datetime'], drop=True, inplace=True)
    dataframe = dataframe[['Actual','Briefing_Forecast','For','Market_Expects',
                           'Prior','Revised_From','Statistic','load_dttm']]
    dataframe = dataframe.sort_index(ascending=[True])
    dataframe = dataframe[ (dataframe.index <= valuation_dttm) & (dataframe.index >= start_dttm) ]
    log.info("Number of rows filtered from h5 economic calendar: [%d]" % (len(dataframe)))
    return dataframe


def extrae_options_chain(valuation_dttm,symbol,expiry,secType):
    """
        extraer de la db los datos de cotizaciones para una fecha
        imputa valores ausente con el metodo ffill de pandas dataframe dentro del dia
    :param year:
    :param month:
    :param day:
    :param symbol:
    :param expiry:
    :param secType:
    :return:
    """
    log.info("extrae_options_chain: [%s] " % (str(valuation_dttm)))
    store = globalconf.open_ib_h5_store()
    #print "extrae_options_chain year=[%s] month=[%s] day=[%s]" % (str(year),month,str(day))
    dataframe = pd.DataFrame()
    #for hora in store.get_node("/" + str(year) + "/" + month + "/" + str(day)):
    #    for minuto in store.get_node(hora._v_pathname):
    #        df1 = store.select(minuto._v_pathname, where=['symbol==' + symbol, 'expiry==' + expiry, 'secType==' + secType])
    #        df1['load_dttm'] = datetime.strptime(minuto._v_pathname, '/%Y/%b/%d/%H/%M')
    #        dataframe = dataframe.append(df1)
    sym1= store.get_node("/"+symbol)
    where1=['symbol==' + symbol, 'expiry==' + expiry,
            'secType==' + secType, 'current_date==' + str(valuation_dttm.year) +  str(valuation_dttm.month).zfill(2)
            + str(valuation_dttm.day).zfill(2), 'current_datetime<=' + str(valuation_dttm.year)
            + str(valuation_dttm.month).zfill(2) + str(valuation_dttm.day).zfill(2)+str(valuation_dttm.hour).zfill(2)+"5959"]
    df1 = store.select(sym1._v_pathname, where=where1)
    log.info("Number of rows loaded from h5 option chain file: [%d] where=[%s]" % ( len(df1) , str(where1)))
    df1['load_dttm'] = pd.to_datetime(df1['current_datetime'], errors='coerce')  # DEPRECATED remove warning coerce=True)
    df1['current_datetime_txt'] = df1.index.strftime("%Y-%m-%d %H:%M:%S")
    dataframe = dataframe.append(df1)
    store.close()

    #cadena_opcs.columns
    dataframe[OPT_NUM_FIELDS_LST] = dataframe[OPT_NUM_FIELDS_LST].apply(pd.to_numeric)
    dataframe['load_dttm'] = dataframe['load_dttm'].apply(pd.to_datetime)
    # imputar valores ausentes con el valor justo anterior (para este dia)
    #dataframe = dataframe.ffill() PERO aqui hay varios strikes !!! ESTO NO VALE
    dataframe = dataframe.drop_duplicates(subset=['right','strike','expiry','load_dttm'], keep='last')

    dataframe = dataframe.sort_values(by=['right','strike','expiry','load_dttm'],
                                      ascending=[True, True, True, True]).groupby(
                                      ['right','strike','expiry'],
                                      as_index=False).apply(lambda group: group.ffill())
    dataframe= dataframe.replace([-1],[0])
    dataframe = dataframe.add_prefix("prices_")
    return dataframe


def create_opt_chain_abt(year="2016"):
    store = globalconf.open_ib_h5_store()

    dataframe = pd.DataFrame()

    for lvl1 in store.get_node("/"+year):
        #print lvl1._v_pathname #mes
        #if lvl1._v_pathname != "/2016/Jul":
        #    continue
        for lvl2 in store.get_node(lvl1._v_pathname):
            #print lvl2._v_pathname # dia
            #if lvl2._v_pathname != "/2016/Jul/21":
            #    continue
            for lvl3 in store.get_node(lvl2._v_pathname):
                # print lvl3 # hora
                for lvl4 in store.get_node(lvl3._v_pathname):
                    if lvl4:
                        print ("Reading " + str(lvl4._v_pathname) + " and appending to DataFrame ...")
                        df1 = store.select(lvl4._v_pathname)
                        dataframe = dataframe.append(df1)
    store.close()
    # sort the dataframe
    dataframe=dataframe[dataframe.current_datetime.notnull()]

    types = dataframe.apply(lambda x: pd.lib.infer_dtype(x.values))
    #print str(types)
    for col in types[types=='floating'].index:
        dataframe[col] = dataframe[col].map(lambda x: np.nan if x > 1e200 else x)

    dataframe['index']=dataframe['current_datetime'].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S'))



    dataframe.sort(columns=['index'], inplace=True)
    # set the index to be this and don't drop
    dataframe.set_index(keys=['index'], drop=True, inplace=True)
    # get a list of names
    names = dataframe['symbol'].unique().tolist()

    max_load_dttm = dataframe.reset_index().max()['current_datetime']
    df_load_dttm = pd.DataFrame()
    df_load_dttm = df_load_dttm.append({"load_dttm": max_load_dttm}, ignore_index=True)

    store_new = globalconf.open_ib_h5_store_abt()
    store_new.append("/load_dttm", df_load_dttm, data_columns=True)

    for name in names:
        # now we can perform a lookup on a 'view' of the dataframe
        print ("Storing " + name + " in ABT ...")
        joe = dataframe.loc[dataframe.symbol == name]
        joe.sort(columns=['symbol', 'current_datetime','expiry','strike','right'], inplace=True)
        #joe.to_excel(name+".xlsx")
        store_new.append("/" + name, joe, data_columns=True)
    store_new.close()


def extrae_historical_chain(start_dt,end_dt,symbol,strike,expiry,right):
    contract = symbol + expiry + right + strike
    log.info("extrae_historical_chain para : start_dt=%s end_dt=%s contract=%s " % (str(start_dt),str(end_dt),contract))
    store = globalconf.open_historical_optchain_store()
    dataframe = pd.DataFrame()
    node = store.get_node("/" + contract)
    coord1 = "index < " + end_dt + " & index > " + start_dt
    c = store.select_as_coordinates(node._v_pathname,coord1)
    df1 = store.select(node._v_pathname,where=c)
    df1.sort_index(inplace=True,ascending=[True])
    #df1 = df1[(df1.index < end_dt) & (df1.index > start_dt)]
    dataframe = dataframe.append(df1)
    store.close()
    return dataframe


def extrae_historical_chain_where(start_dt,end_dt,symbol,strike,expiry,right):
    """
    NOT IMPLEENTEDDD
    :param start_dt:
    :param end_dt:
    :param symbol:
    :param strike:
    :param expiry:
    :param right:
    :return:
    """
    contract = symbol + expiry + right + strike
    log.info("extrae_historical_chain para : start_dt=%s end_dt=%s contract=%s " % (str(start_dt),str(end_dt),contract))
    store = globalconf.open_historical_optchain_store()
    dataframe = pd.DataFrame()
    #pd.concat([store.select(node._v_pathname) for node in store.get_node('df')])
    #list =
    node = store.get_node("/" + contract)
    coord1 = "index < " + end_dt + " & index > " + start_dt
    c = store.select_as_coordinates(node._v_pathname,coord1)
    df1 = store.select(node._v_pathname,where=c)
    df1.sort_index(inplace=True,ascending=[True])
    #df1 = df1[(df1.index < end_dt) & (df1.index > start_dt)]
    dataframe = dataframe.append(df1)
    store.close()
    return dataframe


def extrae_fecha_inicio_estrategia(symbol,expiry,accountid,scenarioMode,simulName):
    """

    :param symbol:
    :param expiry:
    :param accountid:
    :return:
    """
    if scenarioMode == "N":
        f = globalconf.open_orders_store()
        node=f.get_node("/" + accountid)
        df1 = f.select(node._v_pathname,where=['symbol=='+symbol,'expiry=='+expiry])
        f.close()
    elif scenarioMode == "Y":
        df1 = globalconf.orders_dataframe_simulation(simulName=simulName)
        df1 = df1.set_index("index", drop=1)
    try:
        ret1=pd.to_datetime((df1.loc[df1.times == np.min(df1.times)]['times']).unique()[0])
    except IndexError:
        log.info("There are no operations for the strategy %s %s in the orders H5 db" % ( str(symbol) , str(expiry) )  )
        ret1 = datetime.now() + timedelta(days=99999)
    return ret1


def lista_options_chain_keys():
    f = globalconf.open_ib_h5_store()  # open database file
    lst = f.keys()
    f.close()
    return lst


def run_opt_pricing():
    df= extrae_options_chain(year=2016, month="Jul", day=17, hour=16, minute=40)

    # bsm_mcs_euro(s0,k,t,r,sigma,num_simulations)
    df['t'] = df.apply(lambda row: ( datetime.strptime(row['expiry'], "%Y%m%d") - datetime(year=2016,month=7,day=17) ).days / 365.25 , axis = 1)
    df['bsm_mcs_euro'] = df.apply(lambda row: bsm_mcs_euro(float(row['modelUndPrice']),float(row['strike']),row['t'],0.0,
                                                           float(row['modelImpliedVol']),100000,row['right']) , axis=1)

    df.to_excel(
        datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/core%Y_%m_%d_%H_%M_%S.xlsx'),
        sheet_name='chains')