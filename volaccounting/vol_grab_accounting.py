import volibutils.sync_client as ib
from volsetup import config
import datetime as dt
import pickle
import pandas as pd
from volsetup.logger import logger
import swigibpy as sy
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData

def run_get_portfolio_data():
    log=logger("run_get_portfolio_data")
    log.info("Getting portfolio and account data from IB ... ")
    globalconf = config.GlobalConfig()
    client = ib.IBClient(globalconf)
    client.connect()
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
        f.append(c_year + "/" + c_month + "/" + c_day + "/" + c_hour + "/" + c_minute, dataframe,
                 data_columns=dataframe.columns)
        f.close()  # Close file

    if summarylist:
        dataframe2 = pd.DataFrame.from_dict(summarylist).transpose()
        # print("dataframe = ",dataframe)
        dataframe2['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe2['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        # sort the dataframe
        dataframe2.sort(columns=['AccountCode_'], inplace=True)
        # set the index to be this and don't drop
        dataframe2.set_index(keys=['AccountCode_'], drop=False,inplace=True)
        # get a list of names
        names=dataframe2['AccountCode_'].unique().tolist()
        store_new = globalconf.account_store_new()
        for name in names:
            # now we can perform a lookup on a 'view' of the dataframe
            joe = dataframe2.loc[dataframe2['AccountCode_']==name]
            joe.sort(columns=['current_datetime'], inplace=True)
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
            log.info("Appending account data to HDF5 ... ")
            store_new.append(    "/"+name , joe, data_columns=True)
        store_new.close()

if __name__=="__main__":
    run_get_portfolio_data()
