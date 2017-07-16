import datetime as dt
import os
import sqlite3
import sys
from datetime import datetime
from time import sleep

import pandas as pd
import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError
from pytz import timezone

import volsetup.config as config
from core import utils as utils
from core.run_analytics import log, globalconf, timefunc, OPT_NUM_FIELDS_LST
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volibutils.sync_client import IBClient
from volsetup.logger import logger


def get_columns(name,store):
    sql = "SELECT * FROM "+name +" WHERE 0=1"
    df = pd.read_sql_query(sql, store)
    return list(df.columns)

from collections import defaultdict
from core.utils import make_dict, dictify
def get_optchain_datasources(globalconf):
    dict = get_optchain_datasource_files(globalconf)
    dict_out = defaultdict(make_dict)
    for db_type, db_files in dict.items():
        for db_name, db_file in db_files.items():
            store = sqlite3.connect(db_file)
            sql = "SELECT name FROM sqlite_master WHERE type='table'"
            # dict_out['symbols'].extend(list(pd.read_sql_query(sql, store).values.flatten()))
            for symbol in list(pd.read_sql_query(sql, store).values.flatten()):
                dict_out[db_type][symbol]['columns'] = get_columns(symbol, store)
                if not dict_out[db_type][symbol]['expiries']:
                    dict_out[db_type][symbol]['expiries'] = []
                dict_out[db_type][symbol]['expiries'].append(db_name[-10:-3])
    return dictify(dict_out)

def get_underlying_symbols(globalconf, db_type):
    # TODO: get this from configuration
    symbols = {
        "optchain_ib": ["ES", "SPY"],
        "optchain_yhoo": ["ES", "SPY", "USO", "SPX"],
        "optchain_ib_hist": ["ES", "SPY"],
        "underl_ib_hist": ["ES", "SPY", "GOOG"]
    }
    return symbols[db_type]


def get_expiries(globalconf, dsId, symbol):
    """
    get available expiries for options on the given underlying
    """

    from core.market_data_methods import get_optchain_datasources
    import volsetup.config as config
    dict = get_optchain_datasources(globalconf)

    expiries = dict[dsId][symbol]['expiries']

    return expiries


def get_market_db_file(globalconf,db_type,expiry):
    if db_type == "optchain_ib":
        return1 = globalconf.config['sqllite']['optchain_ib'].format(expiry)
    elif db_type == "optchain_yhoo":
        return1 = globalconf.config['sqllite']['optchain_yhoo'].format(expiry)
    elif db_type == "optchain_ib_hist":
        return1 = globalconf.config['sqllite']['optchain_ib_hist_db'].format(expiry)
    elif db_type == "underl_ib_hist":
        return1 = globalconf.config['sqllite']['underl_ib_hist_db']

    return return1


def get_partition_names(db_type):
    """
    Returns the way to filter the input dataset expiry variable text, format of that variable symbol variable
     and sorting criteria
        which will be used for the name of the tables inside the sqlite DB
    """
    if db_type == "optchain_ib":
        return1 = {'expiry':'expiry',
                   "format_expiry":"%Y%m%d",
                   "symbol":"symbol",
                   "sorting_var":"current_datetime",
                   "filtro_sqlite": "substr(current_datetime,1,8)",
                   'formato_filtro': '%Y%m%d'
                   }
    elif db_type == "optchain_yhoo":
        return1 = {'expiry':"Expiry_txt",
                   "format_expiry":"%Y-%m-%d  %H:%M:%S",
                   "symbol":"Underlying",
                   "sorting_var":"Quote_Time_txt",
                   "filtro_sqlite": "substr(Quote_time,1,10)",
                   'formato_filtro': '%Y-%m-%d'
                   }
    elif db_type == "optchain_ib_hist":
        return1 = {'expiry':"",
                   "format_expiry":"",
                   "symbol":"",
                   "sorting_var":"",
                   "filtro_sqlite": "",
                   'formato_filtro': ''
                   }
    elif db_type == "underl_ib_hist":
        return1 = {'expiry':"expiry",
                   "format_expiry":"NO_EXPIRY",
                   "symbol":"symbol",
                   "sorting_var":"date",
                   "filtro_sqlite": "substr(date,1,8)",
                   'formato_filtro': '%Y%m%d'
                   }

    return return1

def formated_string_for_file(expiry, expiry_in_format):
    """
    Return formated string to be used to name the DB file for each expiry date
    """
    return dt.datetime.strptime(expiry, expiry_in_format).strftime('%Y-%m')

def get_last_bars_from_rt(globalconf, log, symbol, last_date,number_days_back):
    max_expiry_available = max( get_expiries(globalconf=globalconf, dsId='optchain_ib_exp', symbol=symbol))
    df = read_market_data_from_sqllite(globalconf=globalconf, log=log,
                                          db_type="optchain_ib",symbol=symbol,expiry=max_expiry_available,
                                          last_date=last_date, num_days_back=number_days_back, resample=None)
    return df

def resample_and_improve_quality(dataframe, criteria, resample):
    dataframe = dataframe.drop_duplicates(subset=[criteria["sorting_var"], criteria["expiry"]], keep='last')
    dataframe = dataframe.sort_values(by=[criteria["sorting_var"]])

    dataframe[criteria["sorting_var"]] = pd.to_datetime(dataframe[criteria["sorting_var"]])
    dataframe.index = dataframe[criteria["sorting_var"]]
    if resample:
        conversion = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        dataframe = dataframe.resample(resample, how=conversion).dropna()
        dataframe['return'] = (dataframe['close'] / dataframe['close'].shift(1)) - 1
    log.info(("Resampled size ", len(dataframe)))
    return dataframe

def read_market_data_from_sqllite(globalconf, log, db_type,symbol,expiry,last_date,num_days_back,resample):
    path = globalconf.config['paths']['data_folder']
    log.info("Reading market data from sqllite ... ")
    criteria = get_partition_names(db_type)
    first_date = (dt.datetime.strptime(last_date, '%Y%m%d') - dt.timedelta(num_days_back)).strftime(
        criteria['formato_filtro'])
    db_file = get_market_db_file(globalconf, db_type, expiry)
    store = sqlite3.connect(path + db_file)
    sql = "select * from " + symbol + " where 1=1 "
    if last_date:
        sql = sql + " and " + criteria['filtro_sqlite'] + " between '" + first_date + "' and '" + last_date +"'"
    #if expiry:
    #    sql = sql + " and expiry = '" +str("/"+symbol+"/"+expiry)+ "'"

    dataframe = pd.read_sql_query(sql, store)

    if resample:
        dataframe = resample_and_improve_quality(dataframe, criteria, resample)

    return dataframe

def write_market_data_to_sqllite(globalconf, log, dataframe, db_type):
    """
    Write to sqllite the market data snapshot passed as argument
    """
    log.info("Appending market data to sqllite ... ")
    path = globalconf.config['paths']['data_folder']
    criteria = get_partition_names(db_type)
    expiries = dataframe[criteria["expiry"]].unique().tolist()
    log.info(("These are the expiries included in the data to be loaded: ", expiries))
    # expiries_file = map(lambda i: formated_string_for_file(i,criteria[1]) , expiries)
    # remove empty string expiries (bug in H5 legacy files)
    expiries = [x for x in expiries if x]

    for expiry in expiries:
        if criteria["format_expiry"] == "NO_EXPIRY":
            expiry_file = ""
        else:
            expiry_file = formated_string_for_file(expiry, criteria["format_expiry"])
        db_file = get_market_db_file(globalconf,db_type,expiry_file)
        store = sqlite3.connect(path + db_file)
        symbols = dataframe[criteria["symbol"]].unique().tolist()
        log.info(("For expiry: ",expiry," these are the symbols included in the data to be loaded:  ", symbols))
        # remove empty string symbols (bug in H5 legacy files)
        symbols = [x for x in symbols if x]
        for name in symbols:
            name = name.replace("^", "")
            joe = dataframe.loc[ (dataframe[criteria["symbol"]] == name) & (dataframe[criteria["expiry"]] == expiry) ]
            #joe.sort(columns=['current_datetime'], inplace=True)  DEPRECATED
            # remove this field which is not to be used
            if 'Halted' in joe.columns:
                joe = joe.drop(['Halted'], axis=1)
            log.info("columns "+str(joe.columns))
            #joe = joe.sort_values(by=[criteria["sorting_var"]])
            if 'Expiry' in joe.columns:
                joe = joe.drop(['Expiry'], axis=1)
            joe.to_sql(name, store, if_exists='append')
        store.close()

def store_underlying_ib_to_db():
    log=logger("historical data loader")
    log.info("Getting historical underlying data from IB ... ")
    globalconf = config.GlobalConfig()
    path = globalconf.config['paths']['data_folder']
    underly_def = globalconf.get_tickers_historical_ib()
    client = IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    dt_now=dt.datetime.now()
    endDateTime =  dt_now.strftime('%Y%m%d %H:%M:%S')
    # lo mas que se puede pedir para barras de 30 min es un mes historico
    # barSizeSetting = "30 mins"
    barSizeSetting = "1 min"
    whatToShow = "TRADES"
    useRTH = 1
    formatDate = 1
    wait_secs = 40

    db_file = get_market_db_file(globalconf=globalconf, db_type="underl_ib_hist", expiry="NONE")
    store = sqlite3.connect(path + db_file)

    for index, row_req in underly_def.iterrows():
        log.info("underl=[%s] [%s] [%s] [%d] [%s] [%s] [%s] [%s] [%d]"
                        % ( str(row_req['symbol']), str(row_req['underl_type']),
                            str(row_req['underl_expiry']), 0, '', '',
                            str(row_req['underl_ex']), str(row_req['underl_curr']), int(index) ) )

        ticker = RequestUnderlyingData(str(row_req['symbol']), str(row_req['underl_type']), str(row_req['underl_expiry']), 0, '', '',
                                       str(row_req['underl_ex']), str(row_req['underl_curr']), int(index))
        symbol_expiry = "/" + str(row_req['symbol']) + "/" + str(row_req['underl_expiry'])

        sql = "SELECT count(*) as count1 FROM sqlite_master WHERE type='table' AND name='" + str(row_req['symbol']) + "';"
        tbl_exist = pd.read_sql_query(sql, store)['count1'][0]
        log.info("Does this symbol has a table in sqlite? " + str(tbl_exist))

        if tbl_exist:
            if int(row_req['underl_expiry']) > 0:
                sql = "SELECT MAX(DATE) as max1 FROM " + str(row_req['symbol']) +" WHERE EXPIRY = '" + symbol_expiry + "' ;"
            else:
                sql = "SELECT MAX(DATE) as max1 FROM " + str(row_req['symbol']) + " ;"

            last_date = pd.read_sql_query( sql , store)['max1'][0]
            log.info("Last date in data store for symbol " + symbol_expiry + " is " + str(last_date) )
            if last_date is not None:
                last_record_stored = dt.datetime.strptime(str(last_date), '%Y%m%d %H:%M:%S')
                # no se debe usar .seconds que da respuesta incorrecta debe usarse .total_seconds()
                # days= int(round( (dt_now - last_record_stored).total_seconds() / 60 / 60 / 24  ,0))
                # lo anterior no sirve porque debe considerarse la diferencia en business days
                # days = np.busday_count(last_record_stored.date(), dt_now.date())
                bh = utils.BusinessHours(last_record_stored, dt_now, worktiming=[15, 21], weekends=[6, 7])
                days = bh.getdays()
                durationStr = str(days) + " D"
            else:
                last_record_stored = 0
                durationStr = "30 D"
                barSizeSetting = "30 mins"

            if str(row_req['symbol']) in ['NDX','SPX','VIX']:
                barSizeSetting = "30 mins"
        else:
            last_record_stored = 0
            durationStr = "30 D"
            barSizeSetting = "30 mins"

        log.info( "last_record_stored=[%s] endDateTime=[%s] durationStr=[%s] barSizeSetting=[%s]"
                  % ( str(last_record_stored), str(endDateTime) , durationStr, barSizeSetting) )

        historicallist = client.get_historical(ticker, endDateTime, durationStr, barSizeSetting,whatToShow, useRTH,formatDate)
        dataframe = pd.DataFrame()
        if historicallist:
            for reqId, request in historicallist.items():
                for date, row in request.items():
                    temp1 = pd.DataFrame(row, index=[0])
                    temp1['symbol'] = str(row_req['symbol'])
                    if int(row_req['underl_expiry']) > 0:
                        temp1['expiry'] = symbol_expiry
                    else:
                        temp1['expiry'] = str(row_req['underl_expiry'])
                    temp1['type'] = str(row_req['underl_type'])
                    temp1['load_dttm'] = endDateTime
                    dataframe = dataframe.append(temp1.reset_index().drop('index', 1))
            dataframe = dataframe.sort_values(by=['date']).set_index('date')
            log.info( "appending data to sqlite ...")
            write_market_data_to_sqllite(globalconf, log, dataframe, "underl_ib_hist")

        log.info("sleeping [%s] secs ..." % (str(wait_secs)))
        sleep(wait_secs)

    client.disconnect()
    store.close()

def store_optchain_yahoo_to_db():
    globalconf = config.GlobalConfig()
    optchain_def = globalconf.get_tickers_optchain_yahoo()
    source1 = globalconf.config['use_case_yahoo_options']['source']
    log = logger("yahoo options chain")
    log.info("Getting options chain data from yahoo w pandas_datareader ... [%s]"
             % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') ))

    for symbol,row in optchain_def.iterrows():
        log.info("Init yahoo quotes downloader symbol=%s" % (symbol) )
        try:
            option = web.Options(symbol,source1)
            for j in option.expiry_dates:
                log.info("expiry=%s" % (str(j)))
                try:
                    joe = pd.DataFrame()
                    call_data = option.get_call_data(expiry=j)
                    put_data = option.get_put_data(expiry=j)
                    if call_data.values.any():
                        joe = joe.append(call_data)
                    if put_data.values.any():
                        joe = joe.append(put_data)
                    joe = joe.sort_index()
                    joe['Quote_Time_txt'] = joe['Quote_Time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                    joe['Last_Trade_Date_txt'] = joe['Last_Trade_Date'].dt.strftime("%Y-%m-%d %H:%M:%S")
                    joe = joe.reset_index().set_index("Quote_Time")
                    joe['Expiry_txt'] = joe['Expiry'].dt.strftime("%Y-%m-%d %H:%M:%S")
                    if 'JSON' in joe.columns:
                        joe['JSON'] = ""

                    write_market_data_to_sqllite(globalconf, log, joe, "optchain_yhoo")

                except KeyError as e:
                    log.warn("KeyError raised [" + str(e) + "]...")

        except (RemoteDataError,TypeError) as err:
            log.info("No information for ticker [%s] Error=[%s] sys_info=[%s]" % (str(symbol) , str(err) , sys.exc_info()[0] ))
            continue


def CustomParser(data):
    # eval convert unicode string to dictionary
    j1 = eval(data)
    return j1


def get_contract_details(symbol, conId=None):
    """
    Get contract details method.
    In the future will get details from DB given a ConId
    There will be a process that populate in background this table DB
    with all the potential contracts and the corresponding contract ID
    """
    db1={
        "ES":{"secType":"FOP","exchange":"GLOBEX","multiplier":"50","currency":"USD",
              "underlType":"FUT","underlCurrency":"XXX","underlExchange":"GLOBEX"},
        "SPY":{"secType":"OPT","exchange":"SMART","multiplier":"100","currency":"USD",
               "underlType":"STK","underlCurrency":"USD","underlExchange":"SMART"}
    }
    #if conId is None:
    return db1[symbol]


def read_graph_from_db(globalconf,log,symbol, last_date, estimator,name):
    """
    return the last saved graph of that type
    """
    log.info("Reading Graph data from sqllite ... ")
    import sqlite3
    db_file = globalconf.config['sqllite']['graphs_db']
    path = globalconf.config['paths']['analytics_folder']
    store = sqlite3.connect(path + db_file)
    import pandas as pd
    df1 = pd.read_sql_query("SELECT div,script FROM " + name + " where symbol = '" + symbol + "'"
                            + " and last_date = '" + last_date + "'"
                            + " and estimator = '" + estimator + "' order by save_dttm desc ;"
                            , store)

    if df1.empty:
        df2 = pd.read_sql_query("SELECT max(last_date) as max1 from " + name, store)
        last_date = df2.iloc[0]['max1']
        df1 = pd.read_sql_query("SELECT div,script FROM " + name + " where symbol = '" + symbol + "'"
                                + " and last_date = '" + last_date + "'"
                                + " and UPPER(estimator) = UPPER('" + estimator + "') order by save_dttm desc ;"
                                , store)
        if df1.empty:
            return None
    store.close()

    return df1['div'].values[0], df1['script'].values[0]


def save_graph_to_db(globalconf,log,script, div, symbol, expiry, last_date, num_days_back, resample, estimator,name):
    # Embedding bokeh plots in web pages
    # http://bokeh.pydata.org/en/0.9.3/docs/user_guide/embed.html
    log.info("Appending Graph data to sqllite ... ")
    import datetime as dt
    import pandas as pd
    save_dttm = dt.datetime.now()
    import sqlite3
    db_file = globalconf.config['sqllite']['graphs_db']
    path = globalconf.config['paths']['analytics_folder']
    dict1 = dict([
        ['script', [script]],
        ['div', [div]],
        ['symbol', [symbol]],
        ['expiry', [expiry]],
        ['last_date', [last_date]],
        ['num_days_back', [num_days_back]],
        ['resample', [resample]],
        ['estimator', [estimator]],
        ['save_dttm', [save_dttm]]
    ])
    df = pd.DataFrame.from_dict(dict1, orient='columns')
    df.set_index(keys=['symbol', 'last_date', 'estimator'], drop=True, inplace=True)
    store = sqlite3.connect(path + db_file)
    df.to_sql(name, store, if_exists='append')
    store.close()


def read_biz_calendar(start_dttm, valuation_dttm):
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


def get_optchain_db_types(globalconf):
    db_types = ["optchain_ib_exp","optchain_yhoo","optchain_ib_hist"]
    return db_types


def get_optchain_datasource_files(globalconf):
    data_folder = globalconf.config['paths']['data_folder']
    dir = os.path.abspath(data_folder)
    exts = ".db"
    dict = {}
    db_types = get_optchain_db_types(globalconf)
    for x in db_types:
        dict[x] = {}
    for root, dirs, files in os.walk(dir):
        for name in files:
            type1 = [x for x in db_types if (x in name ) & ( name.lower().endswith(exts) )]
            if type1:
                # print(("XXXXX2: ", type1,name))
                filename = os.path.join(root, name)
                if filename.lower().endswith(exts):
                    dict[type1[0]][name] = filename
    # print(("XXXXX3: ",dict))
    return dict


@timefunc
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