import datetime as dt
import glob
import os
import time
from datetime import datetime
from time import sleep

import pandas as pd
from tables.exceptions import NaturalNameWarning

import core.market_data_methods as mkt
import operations.accounting as acc
import operations.orders as orde
import volibutils.sync_client as ib
from core import utils
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volibutils.sync_client import IBClient
from volsetup import config
from volsetup.logger import logger


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
                                mkt.write_market_data_to_sqllite(globalconf, log, df1, h5_db_alias)
                    else:
                        df1 = store_file.select(node1._v_pathname)
                    # Expiry is already in the index
                    if drop_expiry == True:
                        df1 = df1.drop(['Expiry'], axis=1)
                    mkt.write_market_data_to_sqllite(globalconf, log, df1, h5_db_alias)
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
                    orde.write_orders_to_sqllite(globalconf, log, df1)
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
                    acc.write_portfolio_to_sqllite(globalconf, log, df1)
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
                    acc.write_acc_summary_to_sqllite(globalconf, log, df1)
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
            bh= utils.BusinessHours(last_record_stored, dt_now, worktiming=[15, 21], weekends=[6, 7])
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
