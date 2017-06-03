""" package for accounting tasks
"""

import volibutils.sync_client as ib
from volsetup import config
import datetime as dt
import pandas as pd
from volsetup.logger import logger
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volutils import utils as utils
from tables.exceptions import NaturalNameWarning
import glob
import os

def print_portfolio_data():
    """ Print portfolio data to console.
    Summary information about account and portfolio.
    """
    days = 10
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = globalconf.log
    #this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    log.info("Getting portfolio and account data from IB ... ")
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_orders'])
    client.connect(clientid1=clientid1)

    now = dt.datetime.now()  # Get current time
    ## Get the executions (gives you everything for last business day)
    acclist, summarylist = client.get_potfolio_data(11)
    client.disconnect()
    log.info("acclist length [%d] " % ( len(acclist) ))
    log.info("summarylist length [%d]" % ( len(summarylist) ))
    print("Real time valuation: %s" % (str(now)))
    if acclist:
        dataframe = pd.DataFrame.from_dict(acclist).transpose()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        columns1 = [u'averageCost', u'conId', u'expiry', u'localSymbol', u'right',
                               u'marketPrice', u'marketValue', u'multiplier',
                               u'position',u'strike', u'symbol', u'unrealizedPNL']
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
        dataframe2=dataframe2[[u'Cushion_',
                               u'FullInitMarginReq_USD',
                               u'FullMaintMarginReq_USD',u'GrossPositionValue_USD',
                               u'NetLiquidation_USD',u'RegTEquity_USD',
                               u'TotalCashBalance_BASE',u'UnrealizedPnL_BASE']]
        print("____________________________________________________________________________________________")
        print("Summary = ")
        print(dataframe2)
        # u'DayTradesRemaining_',u'AvailableFunds_USD',
    print("____________________________________________________________________________________________")
    print("Summary Account Last %d days valuation:" % (days))
    accountid = globalconf.get_accountid()
    store = globalconf.account_store_new()
    node = store.get_node("/" + accountid)
    df1 = store.select(node._v_pathname)
    df1['date1']=df1.index.map(lambda x: x.date())
    df1= df1.drop_duplicates(subset=['date1'],keep='last')
    df1 = df1[[u'FullInitMarginReq_USD',
               u'FullMaintMarginReq_USD',u'GrossPositionValue_USD',
               u'RegTMargin_USD',
               u'TotalCashBalance_BASE',u'UnrealizedPnL_BASE']]
    df1 = df1.ix[-10:]
    store.close()
    print(df1)

def run_get_portfolio_data():
    """ Stores in HDF5 snapshot of portfolio data.
    Intended for batch run, Summary information about account and portfolio.
    """
    log=logger("run_get_portfolio_data")
    if dt.datetime.now().date() in utils.get_trading_close_holidays(dt.datetime.now().year):
        log.info("This is a US Calendar holiday. Ending process ... ")
        return

    log.info("Getting portfolio and account data from IB ... ")
    globalconf = config.GlobalConfig()
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)
    months = globalconf.months
    now = dt.datetime.now()  # Get current time
    c_month = months[now.month]  # Get current month
    c_day = str(now.day)  # Get current day
    c_year = str(now.year)  # Get current year
    c_hour = str(now.hour)
    c_minute = str(now.minute)

    ## Get the executions (gives you everything for last business day)
    acclist, summarylist = client.get_potfolio_data(11)
    client.disconnect()
    log.info("acclist length [%d] " % ( len(acclist) ))
    log.info("summarylist length [%d]" % ( len(summarylist) ))
    if acclist:
        dataframe = pd.DataFrame.from_dict(acclist).transpose()
        #print("dataframe = ",dataframe)
        f = globalconf.portfolio_store()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        log.info("Appending portfolio data to HDF5 ... ")
        try:
            f.append(c_year + "/" + c_month + "/" + c_day + "/" + c_hour + "/" + c_minute, dataframe,
                    data_columns=dataframe.columns)
        except NaturalNameWarning as e:
            log.warn("NaturalNameWarning raised [" + str(e))
        except (ValueError) as e:
            log.warn("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
            aux = globalconf.portfolio_store_error()
            aux.append(c_year + "/" + c_month + "/" + c_day + "/" + c_hour + "/" + c_minute, dataframe,
                     data_columns=dataframe.columns)
            aux.close()
        f.close()  # Close file

    if summarylist:
        dataframe2 = pd.DataFrame.from_dict(summarylist).transpose()
        # print("dataframe = ",dataframe)
        dataframe2['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe2['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        # sort the dataframe
        dataframe2.sort_values(by=['AccountCode_'], inplace=True)
        # set the index to be this and don't drop
        dataframe2.set_index(keys=['AccountCode_'], drop=False,inplace=True)
        # get a list of names
        names=dataframe2['AccountCode_'].unique().tolist()
        store_new = globalconf.account_store_new()
        for name in names:
            # now we can perform a lookup on a 'view' of the dataframe
            joe = dataframe2.loc[dataframe2['AccountCode_']==name]
            joe.sort_values(by=['current_datetime'], inplace=True)
            joe.index=pd.to_datetime(joe['current_datetime'], format="%Y%m%d%H%M%S")
            joe.drop('current_datetime',axis=1,inplace=True)
            joe['current_datetime_txt'] = joe.index.strftime("%Y-%m-%d %H:%M:%S")
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


# globalconf = config.GlobalConfig()
# path = globalconf.config['paths']['data_folder']
#
# def fix_h5_account():
#     os.chdir(path)
#     orders_orig = 'account_db_new.h5'
#     pattern_orders = 'account_db_new.h5*'
#     orders_out = 'account_db_complete.h5'
#     lst1 = glob.glob(pattern_orders)
#     lst1.remove(orders_orig)
#     print (lst1)
#     dataframe = pd.DataFrame()
#     for x in lst1:
#         store_in1 = pd.HDFStore(path + x)
#         root1 = store_in1.root
#         print (root1._v_pathname)
#         for lvl1 in root1:
#             print (lvl1._v_pathname)
#             if lvl1:
#                 df1 = store_in1.select(lvl1._v_pathname)
#                 dataframe = dataframe.append(df1)
#                 print ("store_in1", len(df1), x)
#         store_in1.close()
#
#
#     store_in1 = pd.HDFStore(path + orders_orig)
#     store_out = pd.HDFStore(path + orders_out)
#     root1 = store_in1.root
#     print (root1._v_pathname)
#     for lvl1 in root1:
#         print (lvl1._v_pathname)
#         if lvl1:
#             df1 = store_in1.select(lvl1._v_pathname)
#             dataframe = dataframe.append(df1)
#             print ("store_in1", len(df1), orders_orig)
#     store_in1.close()
#
#     dataframe.sort_index(inplace=True,ascending=[True])
#     names = dataframe['account'].unique().tolist()
#     for name in names:
#         print ("Storing " + name + " in ABT ..." + str(len(dataframe)))
#         joe = dataframe[dataframe.account == name]
#         joe=joe.sort_values(by=['current_datetime'])
#         store_out.append("/" + name, joe, data_columns=True)
#     store_out.close()


if __name__=="__main__":
    run_get_portfolio_data()
    #print_portfolio_data()

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
