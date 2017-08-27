"""@package docstring
"""

from datetime import datetime, timedelta
from operations.accounting  import read_historical_acc_summary_from_sqllite
import numpy as np
import pandas as pd
from pytz import timezone
# import pandas_datareader.data as web
from core.opt_pricing_methods import bsm_mcs_euro
from volsetup import config
from volsetup.logger import logger


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


def extrae_last_date_abt(con,table,field):
    """
    :param con:
    :return:
    """
    query = 'select max(' + field + ') as maxdate from ' + table
    ret1 = pd.read_sql_query(query, con=con)
    return ret1.maxdate[0].replace(tzinfo=None)


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


def testing_operaciones_bug():
    oper_series1=pd.DatetimeIndex(['2016-07-25 21:07:26+02:00','2016-08-15 19:08:26+02:00', '2016-08-15 19:19:08+02:00',
               '2016-08-15 20:32:21+02:00'])
    temp_margin = pd.DataFrame()
    temp_premium = pd.DataFrame()
    for x in oper_series1:
        temporal1 = extrae_account_delta(year=x.year, month=x.strftime("%B")[:3], day=x.day, hour=x.hour,minute=x.minute)
        temporal1 = temporal1.set_index('load_dttm')
        t_margin = temporal1[['RegTMargin_USD', 'MaintMarginReq_USD', 'InitMarginReq_USD',
                              'FullMaintMarginReq_USD', 'FullInitMarginReq_USD']].apply(pd.to_numeric)
        t_margin.diff()
        t_margin['fecha_operacion'] = str(x)
        temp_margin = temp_margin.append(t_margin)
        # cash primas y comisiones
        # impacto en comisiones de las operaciones
        # dado un datetime donde se han ejecutado operaciones hace la delta de las posiciones cash de la cuenta y eso
        # resulta en el valor de las comisiones de ese conjunto de operaciones (hay que considerar el dienro recibido de)
        # la venta de opciones y tambien si se tiene posiciones en divisa los cambios ????
        t_prem = temporal1[
            ['TotalCashBalance_BASE', 'TotalCashBalance_EUR', 'TotalCashBalance_USD', 'TotalCashValue_USD',
             'CashBalance_EUR', 'CashBalance_USD', 'TotalCashValue_C_USD', 'TotalCashValue_S_USD',
             'CashBalance_BASE', 'ExchangeRate_EUR']].apply(pd.to_numeric).diff()
        t_prem['fecha_operacion'] = str(x)
        temp_premium = temp_premium.append(t_prem)



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
