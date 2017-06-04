""" package for accounting tasks
"""

import volibutils.sync_client as ib
from volsetup import config
import datetime as dt
import pandas as pd
from volsetup.logger import logger
from volutils import utils as utils
from tables.exceptions import NaturalNameWarning
import glob
import os
from datetime import datetime

def read_acc_summary_and_portfolio_from_ib(client, globalconf, log):
    """
    Read portfolio data and account summary from IB and returns two dataframes.
    """
    log.info("Getting portfolio and account data from IB ... ")
    # Get the executions (gives you everything for last business day)
    acclist, summarylist = client.get_potfolio_data(11)
    log.info("acclist length [%d] " % (len(acclist)))
    log.info("summarylist length [%d]" % (len(summarylist)))
    return acclist, summarylist


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


def print_10_days_acc_summary_and_current_positions():
    """ 
    print_10_days_acc_summary_and_current_positions
    Read portfolio data from IB and print to console.
    Summary information about account and portfolio for last 10 days is read from H5 and printed to console also.
    """
    days = 10
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = logger("print_10_days_acc_summary_and_current_positions")
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_orders'])
    client.connect(clientid1=clientid1)
    # this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    now = dt.datetime.now()  # Get current time
    acclist, summarylist = read_acc_summary_and_portfolio_from_ib(globalconf, log)
    print("Real time valuation: %s" % (str(now)))
    if acclist:
        dataframe = pd.DataFrame.from_dict(acclist).transpose()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        columns1 = [u'averageCost', u'conId', u'expiry', u'localSymbol', u'right',
                    u'marketPrice', u'marketValue', u'multiplier',
                    u'position', u'strike', u'symbol', u'unrealizedPNL']
        dataframe = dataframe[columns1]
        print("Portfolio = ")
        print(dataframe)
    if summarylist:
        dataframe2 = pd.DataFrame.from_dict(summarylist).transpose()
        dataframe2['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe2['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        # sort the dataframe
        dataframe2.sort_values(by=['AccountCode_'], inplace=True)
        # set the index to be this and don't drop
        dataframe2.set_index(keys=['AccountCode_'], drop=False,inplace=True)
        dataframe2 = dataframe2[[u'Cushion_',
                                 u'FullInitMarginReq_USD',
                                 u'FullMaintMarginReq_USD', u'GrossPositionValue_USD',
                                 u'NetLiquidation_USD', u'RegTEquity_USD',
                                 u'TotalCashBalance_BASE', u'UnrealizedPnL_BASE']]
        print("____________________________________________________________________________________________")
        print("Summary = ")
        print(dataframe2)
    client.disconnect()

    print("____________________________________________________________________________________________")
    print("Summary Account Last %d days valuation:" % (days))
    accountid = globalconf.get_accountid()
    df1 = read_historical_acc_summary_from_h5(globalconf, log, accountid)
    df1 = df1[[u'FullInitMarginReq_USD',
               u'FullMaintMarginReq_USD', u'GrossPositionValue_USD',
               u'RegTMargin_USD',
               u'TotalCashBalance_BASE', u'UnrealizedPnL_BASE']]
    df1 = df1.ix[-10:]
    print(df1)

def store_acc_summary_and_portfolio_from_ib_to_h5():
    """ Stores in HDF5 snapshot of portfolio data.
    Intended for batch run, Summary information about account and portfolio.
    """
    log=logger("store_acc_summary_and_portfolio_from_ib_to_h5")
    if dt.datetime.now().date() in utils.get_trading_close_holidays(dt.datetime.now().year):
        log.info("This is a US Calendar holiday. Ending process ... ")
        return
    globalconf = config.GlobalConfig()
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    acclist, summarylist = read_acc_summary_and_portfolio_from_ib(globalconf, log)
    log.info("acclist length [%d] " % ( len(acclist) ))
    log.info("summarylist length [%d]" % ( len(summarylist) ))

    if acclist:
        dataframe = pd.DataFrame.from_dict(acclist).transpose()
        store = globalconf.portfolio_store()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        write_portfolio_to_h5(globalconf, log, dataframe, store)
    else:
        log.info("Nothing to append to HDF5 ... ")


    if summarylist:
        store_new = globalconf.account_store_new()
        dataframe2 = pd.DataFrame.from_dict(summarylist).transpose()
        # print("dataframe = ",dataframe)
        dataframe2['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe2['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        dataframe2.sort_values(by=['current_datetime'], inplace=True)
        dataframe2.index=pd.to_datetime(dataframe2['current_datetime'], format="%Y%m%d%H%M%S")
        dataframe2.drop('current_datetime',axis=1,inplace=True)
        dataframe2['current_datetime_txt'] = dataframe2.index.strftime("%Y-%m-%d %H:%M:%S")

        write_acc_summary_to_h5(globalconf, log, dataframe2, store_new)
    else:
        log.info("Nothing to append to HDF5 ... ")

    client.disconnect()

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


if __name__=="__main__":
    consolidate_anciliary_h5_portfolio()
    #run_get_portfolio_data()
    #print_portfolio_from_ib()
    #print_10_days_acc_summary_and_current_positions()

"""
AccountCode_	AccountOrGroup_BASE	AccountOrGroup_EUR	AccountOrGroup_USD	AccountReady_	AccountType_
AccruedCash_BASE	AccruedCash_C_USD	AccruedCash_EUR	AccruedCash_S_USD	AccruedCash_USD	AccruedDividend_C_USD
AccruedDividend_S_USD	AccruedDividend_USD	AvailableFunds,_C_USD	AvailableFunds_S_USD	AvailableFunds_USD	Billable_C_USD
Billable_S_USD	Billable_USD	BuyingPower_USD	CashBalance_BASE	CashBalance_EUR	CashBalance_USD	CorporateBondValue_BASE
CorporateBondValue_EUR	CorporateBondValue_USD	Currency_BASE	Currency_EUR	Currency_USD	Cushion_
DayTradesRemainingTplus1_	DayTradesRemainingTplus2_	DayTradesRemainingTplus3_	DayTradesRemainingTplus4_
DayTradesRemaining_	EquityWithLoanValue_C_USD	EquityWithLoanValue_S_USD	EquityWithLoanValue_USD	ExcessLiquidity_C_USD
ExcessLiquidity_S_USD	ExcessLiquidity_USD	ExchangeRate_BASE	ExchangeRate_EUR	ExchangeRate_USD	FullAvailableFunds_C_USD
FullAvailableFunds_S_USD	FullAvailableFunds_USD	FullExcessLiquidity_C_USD	FullExcessLiquidity_S_USD
FullExcessLiquidity_USD	FullInitMarginReq_C_USD	FullInitMarginReq_S_USD	FullInitMarginReq_USD	FullMaintMarginReq_C_USD
FullMaintMarginReq_S_USD	FullMaintMarginReq_USD	FundValue_BASE	FundValue_EUR	FundValue_USD	FutureOptionValue_BASE
FutureOptionValue_EUR	FutureOptionValue_USD	FuturesPNL_BASE	FuturesPNL_EUR	FuturesPNL_USD	FxCashBalance_BASE
FxCashBalance_EUR	FxCashBalance_USD	GrossPositionValue_S_USD	GrossPositionValue_USD	IndianStockHaircut_C_USD
IndianStockHaircut_S_USD	IndianStockHaircut_USD	InitMarginReq_C_USD	InitMarginReq_S_USD	InitMarginReq_USD
IssuerOptionValue_BASE	IssuerOptionValue_EUR	IssuerOptionValue_USD	Leverage_S_	LookAheadAvailableFunds_C_USD
LookAheadAvailableFunds_S_USD	LookAheadAvailableFunds_USD	LookAheadExcessLiquidity_C_USD	LookAheadExcessLiquidity_S_USD
LookAheadExcessLiquidity_USD	LookAheadInitMarginReq_C_USD	LookAheadInitMarginReq_S_USD	LookAheadInitMarginReq_USD
LookAheadMaintMarginReq_C_USD	LookAheadMaintMarginReq_S_USD	LookAheadMaintMarginReq_USD	LookAheadNextChange_
MaintMarginReq_C_USD	MaintMarginReq_S_USD	MaintMarginReq_USD	MoneyMarketFundValue_BASE	MoneyMarketFundValue_EUR
MoneyMarketFundValue_USD	MutualFundValue_BASE	MutualFundValue_EUR	MutualFundValue_USD	NetDividend_BASE
NetDividend_EUR	NetDividend_USD	NetLiquidationByCurrency_BASE	NetLiquidationByCurrency_EUR	NetLiquidationByCurrency_USD
NetLiquidationUncertainty_USD	NetLiquidation_C_USD	NetLiquidation_S_USD	NetLiquidation_USD	OptionMarketValue_BASE
OptionMarketValue_EUR	OptionMarketValue_USD	PASharesValue_C_USD	PASharesValue_S_USD	PASharesValue_USD
PostExpirationExcess_C_USD	PostExpirationExcess_S_USD	PostExpirationExcess_USD	PostExpirationMargin_C_USD
PostExpirationMargin_S_USD	PostExpirationMargin_USD	PreviousDayEquityWithLoanValue_S_USD
PreviousDayEquityWithLoanValue_USD	RealCurrency_BASE	RealCurrency_EUR	RealCurrency_USD	RealizedPnL_BASE
RealizedPnL_EUR	RealizedPnL_USD	RegTEquity_S_USD	RegTEquity_USD	RegTMargin_S_USD	RegTMargin_USD	SMA_S_USD
SMA_USD	SegmentTitle_C_	SegmentTitle_S_	StockMarketValue_BASE	StockMarketValue_EUR	StockMarketValue_USD
TBillValue_BASE	TBillValue_EUR	TBillValue_USD	TBondValue_BASE	TBondValue_EUR	TBondValue_USD	TotalCashBalance_BASE
TotalCashBalance_EUR	TotalCashBalance_USD	TotalCashValue_C_USD	TotalCashValue_S_USD	TotalCashValue_USD
TradingType_S_	UnrealizedPnL_BASE	UnrealizedPnL_EUR	UnrealizedPnL_USD	WarrantValue_BASE	WarrantValue_EUR
WarrantValue_USD	WhatIfPMEnabled_	current_date	current_datetime
"""
