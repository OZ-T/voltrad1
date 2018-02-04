""" package for accounting tasks
"""

import datetime as dt
import pandas as pd
import ibutils.sync_client as ib
from core.misc_utilities import get_trading_close_holidays
from core import config
from core.logger import logger
from persist.sqlite_methods import read_historical_acc_summary_from_sqllite, write_portfolio_to_sqllite, \
    write_acc_summary_to_sqllite


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


def print_10_days_acc_summary_and_current_positions():
    """ Summary information about account and portfolio for last 10 days is read from db and printed to console also."""
    days = 10
    globalconf = config.GlobalConfig(level=logger.ERROR)
    log = logger("print_10_days_acc_summary_and_current_positions")
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_orders'])
    client.connect(clientid1=clientid1)
    # this is to try to fit in one line each row od a dataframe when printing to console
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 400)
    pd.set_option('display.width', 1000)
    now = dt.datetime.now()  # Get current time
    acclist, summarylist = read_acc_summary_and_portfolio_from_ib(client, globalconf, log)
    print("Real time valuation: %s" % (str(now)))
    if acclist:
        dataframe = pd.DataFrame.from_dict(acclist).transpose()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        dataframe = dataframe[[
                    u'averageCost',
                    u'conId',
                    u'expiry',
                    u'localSymbol',
                    u'right',
                    u'marketPrice',
                    u'marketValue',
                    u'multiplier',
                    u'position',
                    u'strike',
                    u'symbol',
                    u'unrealizedPNL'
                    ]]
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
        print(dataframe2.transpose())
    client.disconnect()

    print("____________________________________________________________________________________________")
    print("Summary Account Last %d days valuation:" % (days))
    accountid = globalconf.get_accountid()
    df1 = read_historical_acc_summary_from_sqllite(globalconf, log, accountid)
    df1 = df1[[u'current_date',
               u'current_datetime_txt', u'FullInitMarginReq_USD',
               u'FullMaintMarginReq_USD', u'GrossPositionValue_USD',
               u'RegTMargin_USD',
               u'TotalCashBalance_BASE', u'UnrealizedPnL_BASE']]

    df1 = df1.groupby('current_date').last()
    df1 = df1.ix[-10:]
    print(df1)

def store_acc_summary_and_portfolio_from_ib_to_db():
    """ Stores Snapshot Summary information about account and portfolio from IB into db """
    log=logger("store_acc_summary_and_portfolio_from_ib_to_db")
    if dt.datetime.now().date() in get_trading_close_holidays(dt.datetime.now().year):
        log.info("This is a US Calendar holiday. Ending process ... ")
        return
    globalconf = config.GlobalConfig()
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    acclist, summarylist = read_acc_summary_and_portfolio_from_ib(client, globalconf, log)
    log.info("acclist length [%d] " % ( len(acclist) ))
    log.info("summarylist length [%d]" % ( len(summarylist) ))

    if acclist:
        dataframe = pd.DataFrame.from_dict(acclist).transpose()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        dataframe.sort_values(by=['current_datetime'], inplace=True)
        dataframe.index=dataframe['current_datetime']
        dataframe.drop('current_datetime',axis=1,inplace=True)
        dataframe.drop('multiplier', axis=1, inplace=True)
        write_portfolio_to_sqllite(globalconf, log, dataframe)
    else:
        log.info("Nothing to append to sqlite ... ")

    if summarylist:
        dataframe2 = pd.DataFrame.from_dict(summarylist).transpose()
        # print("dataframe = ",dataframe)
        dataframe2['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe2['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        dataframe2.sort_values(by=['current_datetime'], inplace=True)
        dataframe2.index=pd.to_datetime(dataframe2['current_datetime'], format="%Y%m%d%H%M%S")
        dataframe2.drop('current_datetime',axis=1,inplace=True)
        dataframe2['current_datetime_txt'] = dataframe2.index.strftime("%Y-%m-%d %H:%M:%S")
        dataframe2.drop('Guarantee_C_USD', axis=1, inplace=True)
        write_acc_summary_to_sqllite(globalconf, log, dataframe2)
    else:
        log.info("Nothing to append to HDF5 ... ")

    client.disconnect()

if __name__=="__main__":
    pass
    #consolidate_anciliary_h5_portfolio()
    #run_get_portfolio_data()
    #print_portfolio_from_ib()
    #print_10_days_acc_summary_and_current_positions()
