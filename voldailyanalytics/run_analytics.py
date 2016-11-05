from volsetup import config
from datetime import datetime, timedelta

import time
import pandas as pd
import numpy as np
import pandas_datareader.data as web
from opt_pricing_methods import bsm_mcs_euro
import sys


globalconf = config.GlobalConfig()

def extrae_options_chain(year,month,day,hour,symbol,expiry,secType):
    """
        extraer de la hdf5 los datos de cotizaciones para una fecha
    :param year:
    :param month:
    :param day:
    :param symbol:
    :param expiry:
    :param secType:
    :return:
    """

    # TODO remove
    #store = globalconf.open_ib_h5_store()
    store = globalconf.open_ib_h5_store_original()
    #print "extrae_options_chain year=[%s] month=[%s] day=[%s]" % (str(year),month,str(day))
    dataframe = pd.DataFrame()
    #for hora in store.get_node("/" + str(year) + "/" + month + "/" + str(day)):
    #    for minuto in store.get_node(hora._v_pathname):
    #        df1 = store.select(minuto._v_pathname, where=['symbol==' + symbol, 'expiry==' + expiry, 'secType==' + secType])
    #        df1['load_dttm'] = datetime.strptime(minuto._v_pathname, '/%Y/%b/%d/%H/%M')
    #        dataframe = dataframe.append(df1)
    sym1= store.get_node("/"+symbol)
    where1=['symbol==' + symbol, 'expiry==' + expiry,
            'secType==' + secType, 'current_date==' + str(year) + str(month) + str(day),
            'current_datetime<=' + str(year) + str(month) + str(day)+str(hour)+"5959"]
    df1 = store.select(sym1._v_pathname, where=where1)
    print "Number of rows loaded from h5 option chain file: " , len(df1) , where1
    df1['load_dttm'] = pd.to_datetime(df1['current_datetime'], errors='coerce')  # DEPRECATED remove warning coerce=True)
    df1['current_datetime_txt'] = df1.index.strftime("%Y-%m-%d %H:%M:%S")
    dataframe = dataframe.append(df1)
    store.close()
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


def extrae_account_snapshot(year,month,day):
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


def extrae_account_snapshot_new(year,month,day,hour,accountid,scenarioMode,simulName):
    """
    Extrae los snapshots horarios de account para una fecha
    :param year:
    :param month:
    :param day:
    :return:
    """
    df2 = pd.DataFrame()
    if scenarioMode == "N":
        #accountid = globalconf.get_accountid()
        store = globalconf.account_store_new()
        node=store.get_node("/" + accountid)
        df1 = store.select(node._v_pathname)
        #df2 = df1[df1['current_date'] == str(year)+str(month).zfill(2)+str(day).zfill(2)]
        df2 = df1[(df1.index <= datetime(year=year, month=month, day=day, hour=hour, minute=59)) &
                  (df1['current_date'] == str(year) + str(month).zfill(2) + str(day).zfill(2))]
        # BUG pytables?? si hago el filtro de abajo (usando str(df1['current_date']) en vez de df1['current_date'] )
        # salen cero registros!!!!
        #       (str(df1['current_date']) == str(year) + str(month).zfill(2) + str(day).zfill(2))]
        #df2.to_excel("df2.xlsx")
        #df1.to_excel("df1.xlsx")
        store.close()
    elif scenarioMode == "Y":
        df1 = globalconf.account_dataframe_simulation(simulName=simulName)
        df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
        df1 = df1.set_index("current_datetime",drop=0)
        df2 = df1[(df1.index <= datetime(year=year, month=month, day=day, hour=hour, minute=59)) &
                  (str(df1['current_date']) == str(year) + str(month).zfill(2) + str(day).zfill(2))]
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


def extrae_account_delta_new(year, month, day, hour, minute,accountid,scenarioMode,simulName):
    """
    Extrae la delta de las variables de account dado un datetime.
    Se trata que el inicio y fin de la delta este lo mas cerca posible de la datetime que se recibe como input

    :param year:
    :param month:
    :param day:
    :param hour:
    :param minute:
    :return:
    """
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
    df1=df1.sort_values(by=['current_datetime'] , ascending=[True])
    dfprev = df1[df1.index < datetime(year, month, day, hour, minute, 0)]
    #dfprev = df1[df1['current_datetime'] < str(year)+str(month).zfill(2)+str(day).zfill(2)+str(hour).zfill(2)+str(minute).zfill(2)]
    dfprev = dfprev.ix[-1:]

    dfpost = df1[df1.index > datetime(year, month, day, hour, minute, 0)]
    #dfpost = df1[df1['current_datetime'] > str(year)+str(month).zfill(2)+str(day).zfill(2)+str(hour).zfill(2)+str(minute).zfill(2)]
    dfpost = dfpost.ix[:1]
    dataframe = dataframe.append(dfprev)
    dataframe = dataframe.append(dfpost)
    if scenarioMode == "N":
        store.close()
    return dataframe


def extrae_portfolio_positions(year,month,day,hour,symbol,expiry,secType,
                               accountid,scenarioMode,simulName):
    """
    Saca la foto del subportfolio tic (para un instrumento y expiracion) a una fecha dada

    :param year:
    :param month:
    :param day:
    :param symbol:
    :param expiry:
    :param secType:
    :return:
    """
    dataframe = pd.DataFrame()
    if scenarioMode == "N":
        store=globalconf.portfolio_store()
        for hora in store.get_node("/"+str(year)+"/"+month+"/"+str(day)):
            #print datetime.strptime(hora._v_pathname, '/%Y/%b/%d/%H').hour , hour
            if int(datetime.strptime(hora._v_pathname, '/%Y/%b/%d/%H').hour) <= hour:
                for minuto in store.get_node(hora._v_pathname):
                    df1=store.select(minuto._v_pathname,where=['symbol=='+symbol,'expiry=='+expiry,
                                                               'secType=='+secType,'accountName=='+accountid])
                    df1['load_dttm']=datetime.strptime(minuto._v_pathname, '/%Y/%b/%d/%H/%M')
                    dataframe = dataframe.append(df1)
        store.close()
    elif scenarioMode == "Y":
        df1 = globalconf.portfolio_dataframe_simulation(simulName=simulName)
        df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
        df1['expiry'] = df1['expiry'].apply(lambda x: str(x))
        df1['current_datetime'] = df1['current_datetime'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d%H%M%S'))
        dataframe = dataframe.append(df1)
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
        print "There are no operations for the strategy in the orders H5 db"
        ret1 = datetime.now() + timedelta(days=99999)
    return ret1

def extrae_detalle_operaciones(year,month,day,hour,symbol,expiry,secType,accountid,scenarioMode,simulName):
    """
    Extrae el detalle de todas las ordenes ejecutadas para un simbolo y expiracion
    desde una fecha hacia atras
    :param year:
    :param month:
    :param day:
    :param symbol:
    :param expiry:
    :param secType:
    :return:
    """
    #valuation_dttm = datetime.strptime(str(year)+str(month)+str(day)+" 23:59", '%Y%b%d %H:%M')
    valuation_dttm = datetime.strptime(str(year) + str(month) + str(day) + " " + str(hour) + ":59", '%Y%m%d %H:%M')
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
        df1['current_date'] = df1['current_date'].apply(lambda x: str(x))
        df1['current_datetime'] = df1['current_datetime'].apply(lambda x: datetime.strptime(str(x), '%Y%m%d%H%M%S'))
        dataframe = dataframe.append(df1)

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