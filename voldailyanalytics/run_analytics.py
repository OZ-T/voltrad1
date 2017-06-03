"""@package docstring
"""


from volsetup import config
from datetime import datetime, timedelta
from pytz import timezone
import time
import pandas as pd
import numpy as np
# import pandas_datareader.data as web
from voldailyanalytics.opt_pricing_methods import bsm_mcs_euro
from volsetup.logger import logger

globalconf = config.GlobalConfig()
log = logger("Run Analytics module")

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


def timefunc(f):
    def f_timer(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        log.info( ' __TIMER__ ' + str(f.__name__) + ' took ' + str( end - start ) + ' time')
        return result
    return f_timer


# leer del h5 del yahoo biz calendar
def read_biz_calendar(start_dttm, valuation_dttm):
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


@timefunc
def extrae_options_chain(valuation_dttm,symbol,expiry,secType):
    """
        extraer de la hdf5 los datos de cotizaciones para una fecha
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

@timefunc
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


def extrae_account_snapshot_kk(year,month,day):
    """
    DEPRECATED
    Extrae los snapshots horarios de account para una fecha
    :param year:
    :param month:
    :param day:
    :return:
    """
    store = globalconf.account_store()
    dataframe = pd.DataFrame()
    for hora in store.get_node("/" + str(year) + "/" + month + "/" + str(day)):
        for minuto in store.get_node(hora._v_pathname):
            df1 = store.select(minuto._v_pathname)
            df1['load_dttm'] = datetime.strptime(minuto._v_pathname, '/%Y/%b/%d/%H/%M')
            dataframe = dataframe.append(df1)
    store.close()
    return dataframe

@timefunc
def extrae_account_snapshot_new(valuation_dttm,accountid,scenarioMode,simulName):
    """
    Extrae los snapshots horarios de account para una fecha
    :param year:
    :param month:
    :param day:
    :return:
    """
    log.info("extrae_account_snapshot_new para : [%s] " % (str(valuation_dttm)))
    df2 = pd.DataFrame()
    if scenarioMode == "N":
        #accountid = globalconf.get_accountid()
        store = globalconf.account_store_new()
        node=store.get_node("/" + accountid)
        df1 = store.select(node._v_pathname)
        #df2 = df1[df1['current_date'] == str(year)+str(month).zfill(2)+str(day).zfill(2)]
        df2 = df1[(df1.index <= valuation_dttm) & (df1['current_date'] == str(valuation_dttm.year)
                                                   + str(valuation_dttm.month).zfill(2) + str(valuation_dttm.day).zfill(2))]
        store.close()
    elif scenarioMode == "Y":
        df1 = globalconf.account_dataframe_simulation(simulName=simulName)
        df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
        df1 = df1.set_index("current_datetime",drop=1)
        df2 = df1[(df1.index <= valuation_dttm) & (df1['current_date'] == str(valuation_dttm.year)
                                                   + str(valuation_dttm.month).zfill(2) + str(valuation_dttm.day).zfill(2))]

    # se queda con la ultima foto cargada del dia (la h5 accounts se actualiza cada hora RTH)
    df2 = df2.reset_index().drop_duplicates(subset='current_date', keep='last').set_index('current_datetime',drop=0)
    df2['load_dttm'] = df2.index
    df2 = df2.add_prefix("account_")
    return df2

def extrae_account_delta(year,month,day,hour,minute):
    """
    DEPRECATED
    Extrae la delta de las variables de account dado un datetime.
    Se trata que el inicio y fin de la delta este lo mas cerca posible de la datetime que se recibe como input

    :param year:
    :param month:
    :param day:
    :param hour:
    :param minute:
    :return:
    """
    #old_stdout = sys.stdout
    #old_stderr = sys.stderr
    #log_file = open( datetime.now().strftime('/tmp/extrae_account_delta_%Y_%m_%d_%H_%M_%S.log'), "w")
    #sys.stdout = log_file
    #sys.stderr = log_file	
	
    input_dttm=datetime(year=year,month=time.strptime(month,'%b').tm_mon,day=day,hour=hour,minute=minute)
    store = globalconf.account_store()
    dataframe = pd.DataFrame()
    fake_dttm_start = datetime(year=1976,month=9,day=11,hour=23,minute=59)
    fake_dttm_end = datetime(year=2999,month=9,day=11,hour=23,minute=59)
    delta_dttm_start = fake_dttm_start - input_dttm
    delta_dttm_end = fake_dttm_end - input_dttm
    df1_start = pd.DataFrame()
    df1_end = pd.DataFrame()
    #print("delta_dttm_end=[%s] delta_dttm_start=[%s]" % (str(delta_dttm_end), str(delta_dttm_start)))

    for hora in store.get_node("/" + str(year) + "/" + month + "/" + str(day)):
        for minuto in store.get_node(hora._v_pathname):
            temp_dttm = datetime.strptime(minuto._v_pathname, '/%Y/%b/%d/%H/%M')
            if ( delta_dttm_end > (temp_dttm - input_dttm) ) & ( (temp_dttm - input_dttm).total_seconds() > 0 )  :
                delta_dttm_end = temp_dttm - input_dttm
                df1_end = store.select(minuto._v_pathname)
                df1_end['load_dttm'] = temp_dttm
            if ( delta_dttm_start < (temp_dttm - input_dttm) ) & ( (temp_dttm - input_dttm).total_seconds() < 0 )  :
                delta_dttm_start = temp_dttm - input_dttm
                df1_start = store.select(minuto._v_pathname)
                df1_start['load_dttm'] = temp_dttm
            #print("input_dttm=[%s] temp_dttm=[%s] delta_dttm_end=[%s] delta_dttm_start=[%s] candidate=[%s]"
            #     % ( str(input_dttm) , str(temp_dttm) , str(delta_dttm_end) , str(delta_dttm_start), str(input_dttm - temp_dttm) ) )

    #print("df1_start=[%s] df1_end=[%s] input_dttm=[%s]" %
    #      (str(df1_start.iloc[0]['load_dttm']), str(df1_end.iloc[0]['load_dttm']) , str(input_dttm) ))

    dataframe = dataframe.append(df1_start)
    dataframe = dataframe.append(df1_end)
    store.close()
    #log_file.close()	
    return dataframe

@timefunc
def extrae_historical_underl(symbol,start_dt="20160101",end_dt="29991231"):
    log.info("extrae_historical_underl para : start_dt=%s end_dt=%s symbol=%s " % (str(start_dt),str(end_dt),symbol))
    store = globalconf.open_historical_store()
    dataframe = pd.DataFrame(dtype=float)
    node = store.get_node("/" + symbol)
    coord1 = "index < " + end_dt + " & index > " + start_dt
    c = store.select_as_coordinates(node._v_pathname,coord1)
    df1 = store.select(node._v_pathname,where=c)
    df1.sort_index(inplace=True,ascending=[True])
    #df1 = df1[(df1.index < end_dt) & (df1.index > start_dt)]
    dataframe = dataframe.append(df1)
    store.close()
    return dataframe

@timefunc
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


@timefunc
def extrae_account_snapshot(valuation_dttm,accountid,scenarioMode,simulName):
    log.info("extrae_account_snapshot para : [%s] " % (str(valuation_dttm)))
    dataframe = pd.DataFrame()
    if scenarioMode == "N":
        store = globalconf.account_store_new()
        #accountid = globalconf.get_accountid()
        node=store.get_node("/" + accountid)
        df1 = store.select(node._v_pathname)
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

    if scenarioMode == "N":
        store.close()
    return t_margin , t_prem


@timefunc
def extrae_account_delta_new(valuation_dttm,accountid,scenarioMode,simulName):
    """
    Extrae la delta de las variables de account dado un datetime.
    Se trata que el inicio y fin de la delta este lo mas cerca posible de la datetime que se recibe como input

    :param valuation_dttm:
    :return:
    """
    log.info("extrae_account_delta_new para : [%s] " % (str(valuation_dttm)))
    dataframe = pd.DataFrame()
    if scenarioMode == "N":
        store = globalconf.account_store_new()
        #accountid = globalconf.get_accountid()
        node=store.get_node("/" + accountid)
        df1 = store.select(node._v_pathname)
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

    if scenarioMode == "N":
        store.close()
    return t_margin , t_prem


@timefunc
def extrae_portfolio_positions(valuation_dttm=None,symbol=None,expiry=None,secType=None,
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
    if scenarioMode == "N":
        store=globalconf.portfolio_store()
        node1=store.get_node("/"+str(valuation_dttm.year)+"/"+valuation_dttm.strftime("%B")[:3]+"/"+str(valuation_dttm.day))
        try:
            for hora in node1:
                #print datetime.strptime(hora._v_pathname, '/%Y/%b/%d/%H').hour , hour
                if int(datetime.strptime(hora._v_pathname, '/%Y/%b/%d/%H').hour) <= valuation_dttm.hour:
                    for minuto in store.get_node(hora._v_pathname):
                        if where1 is None:
                            df1=store.select(minuto._v_pathname)
                        else:
                            df1 = store.select(minuto._v_pathname, where=where1)
                        df1['load_dttm']=datetime.strptime(minuto._v_pathname, '/%Y/%b/%d/%H/%M')
                        dataframe = dataframe.append(df1)
        except TypeError:
            log.info("No option data to analyze (t). Exiting ...")
            store.close()
            return
        store.close()
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
                           u'strike', u'symbol', u'unrealizedPNL', u'current_datetime',u'load_dttm']]
    #   unrealizedPNL = marketValue - (position * averageCost)
    #   marketValue = marketPrice * multiplier * position
    # se queda con la ultima foto cargada del dia (la h5 accounts se actualiza cada hora RTH)
    dataframe = dataframe.reset_index().drop_duplicates(subset='index', keep='last').set_index('index')
    dataframe['load_dttm'] = dataframe['load_dttm'].apply(pd.to_datetime)
    dataframe[[u'averageCost', u'marketValue', u'multiplier', u'position',
               u'strike', u'unrealizedPNL']] = dataframe[[u'averageCost', u'marketValue',
                                                          u'multiplier',  u'position',
                                                          u'strike', u'unrealizedPNL']].apply(pd.to_numeric)
    # el precio neto son las primas netas de comisiones tal y como se contabiliza en el portfolio en cada dttm
    dataframe['precio_neto']=dataframe['averageCost'] * dataframe['position']
    dataframe = dataframe.add_prefix("portfolio_")
    return dataframe

@timefunc
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

@timefunc
def extrae_detalle_operaciones(valuation_dttm,symbol,expiry,secType,accountid,scenarioMode,simulName):
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

    if scenarioMode == "N":
        store = globalconf.open_orders_store()
        node=store.get_node("/" + accountid)
        df1 = store.select(node._v_pathname,where=['symbol=='+symbol,'expiry=='+expiry])
        df1['load_dttm'] = df1['current_datetime'].apply(lambda x: datetime.strptime(x, '%Y%m%d%H%M%S'))
        df1 = df1[df1.load_dttm <= valuation_dttm]
        df1=df1.rename(columns={'execid':'index'})
        df1.set_index(keys=['index'], drop=True, inplace=True)
        #for item in store.keys():
        #    date_object = datetime.strptime(str(item), '/%Y/%b/%d/%H/%M')
            # print("item=", str(item), date_object,valuation_dttm)
        #    if date_object <= valuation_dttm:
        #        df1=store.select(item,where=['symbol=='+symbol,'expiry=='+expiry])
        #        df1['load_dttm']=datetime.strptime(item, '/%Y/%b/%d/%H/%M')
        #        dataframe = dataframe.append(df1)

        # en cada trade se toma la foto del portfolio y de la account para tener de comisiones coste base y margin por
        # trade

        #print "extrae_detalle_operaciones " , df1
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


    #dataframe.to_excel("operaciones.xlsx")
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
    df=extrae_options_chain(year=2016, month="Jul", day=17, hour=16, minute=40)

    # bsm_mcs_euro(s0,k,t,r,sigma,num_simulations)
    df['t'] = df.apply(lambda row: ( datetime.strptime(row['expiry'], "%Y%m%d") - datetime(year=2016,month=7,day=17) ).days / 365.25 , axis = 1)
    df['bsm_mcs_euro'] = df.apply(lambda row: bsm_mcs_euro(float(row['modelUndPrice']),float(row['strike']),row['t'],0.0,
                                                           float(row['modelImpliedVol']),100000,row['right']) , axis=1)

    df.to_excel(
        datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/analytics%Y_%m_%d_%H_%M_%S.xlsx'),
        sheet_name='chains')


def pruebas_lectura_hdf5():
    #tic_portfolio_underl(symbol='ES',expiry='20160819')

    store=globalconf.open_ib_h5_store()
    #print store.keys()
    #print ("/2016/Jul/17" in store)
    dataframe = pd.DataFrame()
    for hora in store.get_node("/2016/Jul/20"):
        for minuto in store.get_node(hora._v_pathname):
            df1=store.select(minuto._v_pathname)
            df1['load_dttm']=minuto._v_pathname
            dataframe = dataframe.append(df1)
        #dataframe = dataframe.append( pd.concat([store.select(minuto._v_pathname) for minuto in store.get_node(hora._v_pathname)])  )

    dataframe.to_excel(
    datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/shark%Y_%m_%d_%H_%M_%S.xlsx'),
        sheet_name='shark')

    #print store.select(node._v_pathname)
    #df = pd.concat([store.select(node._v_pathname) for node in store.get_node('')])
    store.close()

    #store = pd.HDFStore('test.h5',mode='w')
    #store.append('df/foo1',pd.DataFrame(np.random.randn(10,2)))
    #store.append('df/foo2',pd.DataFrame(np.random.randn(10,2)))
    #pd.concat([ store.select(node._v_pathname) for node in store.get_node('df') ])

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
