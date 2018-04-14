"""@package docstring
"""

from datetime import datetime
from persist.sqlite_methods import read_historical_acc_summary_from_sqllite
import pandas as pd
# import pandas_datareader.data as web
import core.config as config
globalconf = config.GlobalConfig()
log = globalconf.get_logger()

OPT_NUM_FIELDS_LST = [u'CallOI', u'PutOI', u'Volume', u'askDelta', u'askGamma',
                   u'askImpliedVol', u'askOptPrice', u'askPrice', u'askPvDividend',
                   u'askSize', u'askTheta', u'askUndPrice', u'askVega', u'bidDelta',
                   u'bidGamma', u'bidImpliedVol', u'bidOptPrice', u'bidPrice',
                   u'bidPvDividend', u'bidSize', u'bidTheta', u'bidUndPrice', u'bidVega',
                   u'closePrice', u'highPrice', u'lastDelta', u'lastGamma', u'lastImpliedVol',
                   u'lastOptPrice', u'lastPrice', u'lastPvDividend', u'lastSize',
                   u'lastTheta', u'lastUndPrice', u'lastVega', u'lowPrice',
                   u'modelDelta', u'modelGamma', u'modelImpliedVol', u'modelOptPrice',
                   u'modelPvDividend', u'modelTheta', u'modelUndPrice', u'modelVega',
                   u'multiplier', u'strike']


def extrae_last_date_abt(con,table,field):
    """
    :param con:
    :return:
    """
    query = 'select max(' + field + ') as maxdate from ' + table
    ret1 = pd.read_sql_query(query, con=con)
    return ret1.maxdate[0].replace(tzinfo=None)


def extrae_account_snapshot(valuation_dttm,log, globalconf,accountid,scenarioMode,simulName):
    log.info("extrae_account_snapshot para : [%s] " % (str(valuation_dttm)))
    dataframe = pd.DataFrame()
    if scenarioMode == "N":
        df1 = read_historical_acc_summary_from_sqllite( globalconf, log, accountid )
        df1['current_datetime'] = pd.to_datetime(df1['current_datetime'], format="%Y-%m-%d %H:%M:%S")
        df1 = df1.set_index("current_datetime", drop=0)

    elif scenarioMode == "Y":
        df1 = globalconf.account_dataframe_simulation(simulName=simulName)
        df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
        df1 = df1.set_index("current_datetime", drop=0)

    df1.sort_index(inplace=True,ascending=[True])
    dfprev = df1[df1.index < valuation_dttm.replace(minute=0, second=0)]
    dfprev = dfprev.ix[-1:]
    dataframe = dataframe.append(dfprev)

    dataframe['load_dttm'] = valuation_dttm #  dataframe['current_datetime_txt']
    dataframe = dataframe.set_index('load_dttm')
    dataframe['fecha_operacion'] = str(valuation_dttm)
    t_margin = dataframe[['RegTMargin_USD', 'MaintMarginReq_USD', 'InitMarginReq_USD',
                          'FullMaintMarginReq_USD',
                          'FullInitMarginReq_USD']].apply(pd.to_numeric).dropna(subset=['FullInitMarginReq_USD'])


    t_prem = dataframe[['TotalCashBalance_BASE', 'TotalCashBalance_EUR', 'TotalCashBalance_USD', 'TotalCashValue_USD',
                        'CashBalance_EUR', 'CashBalance_USD', 'TotalCashValue_C_USD', 'TotalCashValue_S_USD',
                        'CashBalance_BASE',
                        'ExchangeRate_EUR']].apply(pd.to_numeric).dropna(subset=['TotalCashValue_USD'])

    return t_margin , t_prem


def extrae_account_delta(globalconf, log, valuation_dttm, accountid, scenarioMode,simulName):
    """
    Extrae la delta de las variables de account dado un datetime.
    Se trata que el inicio y fin de la delta este lo mas cerca posible de la datetime que se recibe como input

    :param valuation_dttm:
    :return:
    """
    log.info("extrae_account_delta_new para : [%s] " % (str(valuation_dttm)))
    dataframe = pd.DataFrame()
    if scenarioMode == "N":
        df1 = read_historical_acc_summary_from_sqllite( globalconf, log, accountid )
        df1['current_datetime'] = pd.to_datetime(df1['current_datetime'], format="%Y-%m-%d %H:%M:%S")
        df1 = df1.set_index("current_datetime", drop=0)
    elif scenarioMode == "Y":
        df1 = globalconf.account_dataframe_simulation(simulName=simulName)
        df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
        df1 = df1.set_index("current_datetime", drop=0)
    #df1=df1.sort_values(by=['current_datetime'] , ascending=[True])
    df1.sort_index(inplace=True,ascending=[True])
    dfprev = df1[df1.index < valuation_dttm.replace(minute=0, second=0)]
    #dfprev = df1[df1['current_datetime'] < str(year)+str(month).zfill(2)+str(day).zfill(2)+str(hour).zfill(2)+str(minute).zfill(2)]
    dfprev = dfprev.ix[-1:]
    dfpost = df1[df1.index > valuation_dttm.replace(minute=0, second=0)]
    #dfpost = df1[df1['current_datetime'] > str(year)+str(month).zfill(2)+str(day).zfill(2)+str(hour).zfill(2)+str(minute).zfill(2)]
    dfpost = dfpost.ix[:1]
    dataframe = dataframe.append(dfprev)
    dataframe = dataframe.append(dfpost)

    t_margin = dataframe[['RegTMargin_USD', 'MaintMarginReq_USD', 'InitMarginReq_USD',
                          'FullMaintMarginReq_USD',
                          'FullInitMarginReq_USD']].apply(pd.to_numeric).diff().dropna(subset=['FullInitMarginReq_USD'])


    t_prem = dataframe[['TotalCashBalance_BASE', 'TotalCashBalance_EUR', 'TotalCashBalance_USD', 'TotalCashValue_USD',
                        'CashBalance_EUR', 'CashBalance_USD', 'TotalCashValue_C_USD', 'TotalCashValue_S_USD',
                        'CashBalance_BASE',
                        'ExchangeRate_EUR']].apply(pd.to_numeric).diff().dropna(subset=['TotalCashValue_USD'])

    t_margin['load_dttm'] = valuation_dttm #  dataframe['current_datetime_txt']
    t_margin = t_margin.set_index('load_dttm')
    t_margin['fecha_operacion'] = str(valuation_dttm)

    t_prem['load_dttm'] = valuation_dttm #  dataframe['current_datetime_txt']
    t_prem = t_prem.set_index('load_dttm')
    t_prem['fecha_operacion'] = str(valuation_dttm)

    return t_margin , t_prem


def extrae_portfolio_positions(log, globalconf, valuation_dttm=None,symbol=None,expiry=None,secType=None,
                               accountid=None,scenarioMode="N",simulName="NA"):
    """
    Saca la foto del subportfolio tic (para un instrumento y expiracion) a una fecha dada

    :param valuation_dttm:
    :param symbol:
    :param expiry:
    :param secType:
    :return:
    """
    if symbol is None:
        where1 = None
    else:
        where1 = ['symbol=='+symbol,'expiry=='+expiry,'secType=='+secType,'accountName=='+accountid]
    log.info("extrae_portfolio_positions para : [%s] " % (str(valuation_dttm)) )
    dataframe = pd.DataFrame()

    path = globalconf.config['paths']['data_folder']
    db_file = globalconf.config['sqllite']['portfolio_db']

    if scenarioMode == "N":
        store = sqlite3.connect(path + db_file)
        sql = "select * from " + accountid + " where 1=1 "
        df1 = pd.read_sql_query(sql, store)
    elif scenarioMode == "Y":
        df1 = globalconf.portfolio_dataframe_simulation(simulName=simulName)

    df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
    df1['expiry'] = df1['expiry'].apply(lambda x: str(x))
    df1['current_datetime'] = df1['current_datetime'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d%H%M%S'))
    df1=df1.loc[ (df1.current_datetime <= valuation_dttm) & ( df1.current_date == valuation_dttm.strftime("%Y%m%d") ) ]
    dataframe = dataframe.append(df1)

    #dataframe = dataframe[[u'averageCost', u'conId', u'expiry', u'localSymbol', u'marketPrice', u'marketValue',
    #                       u'multiplier', u'position', u'realizedPNL',
    #                       u'right', u'strike', u'symbol', u'unrealizedPNL', u'current_datetime',
    #                       u'load_dttm']]
    if len(dataframe) == 0:
        log.info("No option data to analyze (t). Exiting ...")
        return

    dataframe = dataframe[[u'averageCost', u'expiry', u'marketValue',u'multiplier', u'position', u'right',
                           u'strike', u'symbol', u'unrealizedPNL', u'current_datetime',u'current_date']]
    #   unrealizedPNL = marketValue - (position * averageCost)
    #   marketValue = marketPrice * multiplier * position
    # se queda con la ultima foto cargada del dia (la h5 accounts se actualiza cada hora RTH)
    dataframe = dataframe.reset_index().drop_duplicates(subset='index', keep='last').set_index('index')
    dataframe['current_datetime'] = dataframe['current_datetime'].apply(pd.to_datetime)
    dataframe[[u'averageCost', u'marketValue', u'multiplier', u'position',
               u'strike', u'unrealizedPNL']] = dataframe[[u'averageCost', u'marketValue',
                                                          u'multiplier',  u'position',
                                                          u'strike', u'unrealizedPNL']].apply(pd.to_numeric)
    # el precio neto son las primas netas de comisiones tal y como se contabiliza en el portfolio en cada dttm
    dataframe['precio_neto']=dataframe['averageCost'] * dataframe['position']
    dataframe = dataframe.add_prefix("portfolio_")
    return dataframe


import sqlite3

def extrae_detalle_operaciones(valuation_dttm,globalconf, log, symbol,expiry,secType,accountid,scenarioMode,simulName):
    """
    Extrae el detalle de todas las ordenes ejecutadas para un simbolo y expiracion
    desde una fecha hacia atras
    :param valuation_dttm:
    :param symbol:
    :param expiry:
    :param secType:
    :return:
    """

    #valuation_dttm = datetime.strptime(str(year)+str(month)+str(day)+" 23:59", '%Y%b%d %H:%M')
    #valuation_dttm = datetime.strptime(str(year) + str(month) + str(day) + " " + str(hour) + ":59", '%Y%m%d %H:%M')
    #valuation_dttm_txt = str(year)+str(month)+str(day)+"235959"
    dataframe = pd.DataFrame()
    path = globalconf.config['paths']['data_folder']
    db_file = globalconf.config['sqllite']['orders_db']

    if scenarioMode == "N":
        store = sqlite3.connect(path + db_file)
        sql = "select * from " + accountid + " where 1=1 "
        sql = sql + " and symbol == '" + symbol + "'"
        sql = sql + " and expiry == '" + expiry + "'"
        df1 = pd.read_sql_query(sql, store)

        df1['load_dttm'] = df1['current_datetime'].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S'))
        df1 = df1[df1.load_dttm <= valuation_dttm]
        df1=df1.rename(columns={'execid':'index'})
        df1.set_index(keys=['index'], drop=True, inplace=True)
        dataframe = dataframe.append(df1)
        store.close()
    elif scenarioMode == "Y":
        df1 = globalconf.orders_dataframe_simulation(simulName=simulName)
        df1 = df1.set_index("index", drop=1)
        df1['expiry'] = df1['expiry'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
        df1['current_date'] = df1['current_datetime'].apply(lambda x: str(x)[:8])
        df1['load_dttm'] = df1['current_datetime'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d%H%M%S'))
        df1 = df1[df1.load_dttm <= valuation_dttm]
        dataframe = dataframe.append(df1)

    dataframe=dataframe[['localSymbol','avgprice','expiry','conId','current_datetime','symbol','load_dttm',
                             'multiplier','price','qty','right','side','strike','times','shares']]
    dataframe[['avgprice','multiplier','price','qty','strike','shares']]= \
                dataframe[['avgprice','multiplier','price','qty','strike','shares']].apply(pd.to_numeric)

    dataframe[['expiry','current_datetime','load_dttm','times']] = \
                dataframe[['expiry','current_datetime','load_dttm','times']].apply(pd.to_datetime)
    #dataframe.columns
    #dataframe.index
    # elimina duplicados causados por posibles recargas manuales de los ficheros h5 desde IB API
    dataframe=dataframe.reset_index().drop_duplicates(subset='index',keep='last').set_index('index')
    # el precio bruto son las primas brutas de comisiones de las ordenes ejecutadas
    dataframe['precio_bruto'] = dataframe['avgprice'] * dataframe['multiplier'] * dataframe['shares']
    dataframe=dataframe.add_prefix("orders_")
    return dataframe


if __name__ == "__main__":
    #print globalconf.config['use_case_ib_options']['hdf5_db_nm']
    #print globalconf.config['paths']['data_folder']
    #lista = lista_options_chain_keys()
    #print lista
    #run_opt_pricing()
    #testing_operaciones_bug()
    #plot_spy_vol()
    #create_opt_chain_abt(year="2016")
    #a= extrae_options_chain(year=2016, month=10, day=27, hour=20, symbol="ES", expiry="20161118",secType="FOP")
    #print extrae_account_snapshot_new(year=2016, month=10, day=27, hour=17, accountid="DU242089")
    pass
