
# coding: utf-8

# In[50]:

import run_analytics as ra
import pandas as pd
import numpy as np
import datetime as dt
from pandas import ExcelWriter
from scipy import stats
import datetime as dt
from volsetup import config

# Regular trading hours in US (CEST time)
# TODO: need to consider change saving time CEST vs CET
_rth = (15,16,17,18,19,20,21)

def prev_weekday(adate):
    _offsets = (3, 1, 1, 1, 1, 1, 2)
    return adate - dt.timedelta(days=_offsets[adate.weekday()])


# Analizar TIC activas para un subyacente y vencimiento dados para una datetime de valoracion dada
#   1.- se obtiene del chain option del h5 historico para la datetime de valoracion, los precios (opciones y subyacente),
#       IV, griegas, volumenes, OI,...
#       se le proporciona un date de valoracion y el datetime de valoracion corresponde a la ultima hora disponible para ese date
#       (hora_max)
#   2.- se obtiene lo mismo para (datetime de valoracion) - (1 dia)
#   3.- se obtiene la foto de las posiciones a datetime de valoración
#   4.- se obtiene todas las operaciones desde el inicio de la estrategia hasta el datetime de valoracion
#   5.- se obtiene del h5 account los impactos (deltas) en Cash (comisiones, primas cobradas) y margin en cada uno de los datetimes
#       en que se ha realizado operaciones (este historico se obtiene en el punto 4 anterior)
#
def run_shark_analytics(i_symbol,i_date,i_expiry,i_secType,accountid,scenarioMode,simulName,appendh5):
    print "Running for valuation date: " , i_date
    # fechas en que se calcula la foto de la estrategia
    # fecha que se utiliza para calcular retornos del subyacente y se convertira en hora mas cercana al cierre luego
    fecha_valoracion=i_date
    fecha_val_tminus1 = prev_weekday(fecha_valoracion)
    i_year=fecha_valoracion.year
    p_i_day_t0 = fecha_valoracion.day
    p_i_hour_t0 = fecha_valoracion.hour
    i_month=fecha_valoracion.strftime("%B")[:3]
    i_month_num=str(fecha_valoracion.month).zfill(2)
    i_day_t0= str(fecha_valoracion.day).zfill(2)
    i_month_num_tminus1 = str(fecha_val_tminus1.month).zfill(2)
    i_day_tminus1 = str(fecha_val_tminus1.day).zfill(2)

    ###################################################################################################################
    # PORTFOLIO en la fecha de valoracion
    # posiciones actuales en las opciones en el symbol y el expiry
    ###################################################################################################################
    posiciones=ra.extrae_portfolio_positions(year=i_year,month=i_month,day=p_i_day_t0,hour=p_i_hour_t0,
                                             symbol=i_symbol,expiry=i_expiry,secType=i_secType,accountid=accountid,
                                             scenarioMode=scenarioMode,simulName=simulName)
    #posiciones.to_excel("posiciones.xlsx")
    posiciones=posiciones[[u'averageCost',u'conId', u'expiry', u'localSymbol', u'marketPrice', u'marketValue',
                           u'multiplier', u'position', u'realizedPNL',
                           u'right', u'strike', u'symbol', u'unrealizedPNL', u'current_datetime',
                           u'load_dttm']]
    # se queda con la última foto cargada del dia (la h5 accounts se actualiza cada hora RTH)
    posiciones=posiciones.reset_index().drop_duplicates(subset='index',keep='last').set_index('index')
    posiciones['load_dttm'] = posiciones['load_dttm'].apply(pd.to_datetime)
    posiciones[[u'averageCost',u'marketPrice',u'marketValue', u'multiplier', u'position',
                u'realizedPNL', u'strike',u'unrealizedPNL']] = posiciones[[u'averageCost',u'marketPrice',
                                                            u'marketValue', u'multiplier', u'position',
                                                            u'realizedPNL', u'strike', u'unrealizedPNL']].apply(pd.to_numeric)

    ###################################################################################################################
    # ORDERS
    # se obtienen todas operaciones desde el inicio de la estrategia hasta el datetime de valoracion
    ###################################################################################################################
    operaciones=ra.extrae_detalle_operaciones(year=i_year,month=i_month_num,day=i_day_t0,hour=p_i_hour_t0,
                                              symbol=i_symbol,expiry=i_expiry,secType=i_secType,accountid=accountid,
                                              scenarioMode=scenarioMode, simulName=simulName)
    #operaciones.to_excel("operaciones.xlsx")
    operaciones=operaciones[['localSymbol','avgprice','expiry','conId','current_datetime','symbol','load_dttm',
                             'multiplier','price','qty','right','side','strike','times','shares']]
    operaciones[['avgprice','multiplier','price','qty','strike','shares']]= \
                    operaciones[['avgprice','multiplier','price','qty','strike','shares']].apply(pd.to_numeric)

    operaciones[['expiry','current_datetime','load_dttm','times']] = \
                    operaciones[['expiry','current_datetime','load_dttm','times']].apply(pd.to_datetime)
    #operaciones.columns
    #operaciones.index
    # elimina duplicados causados por posibles recargas manuales de los ficheros h5 desde IB API
    operaciones=operaciones.reset_index().drop_duplicates(subset='index',keep='last').set_index('index')

    ###################################################################################################################
    # OPTIONS CHAIN
    # precios de la cadena de opciones lo mas cerca posible de la fecha de valoracion
    # realmente saca todos los precios del dia anteriores a dicha fecha
    ###################################################################################################################
    cadena_opcs=ra.extrae_options_chain(year=i_year, month=i_month_num, day=i_day_t0,hour=p_i_hour_t0,
                                        symbol=i_symbol,expiry=i_expiry,secType=i_secType)

    #cadena_opcs.columns
    cadena_opcs[[u'CallOI',u'PutOI', u'Volume', u'askDelta', u'askGamma',
                   u'askImpliedVol', u'askOptPrice', u'askPrice', u'askPvDividend',
                   u'askSize', u'askTheta', u'askUndPrice', u'askVega', u'bidDelta',
                   u'bidGamma', u'bidImpliedVol', u'bidOptPrice', u'bidPrice',
                   u'bidPvDividend', u'bidSize', u'bidTheta', u'bidUndPrice', u'bidVega',
                   u'closePrice', u'highPrice', u'lastDelta', u'lastGamma', u'lastImpliedVol',
                   u'lastOptPrice', u'lastPrice', u'lastPvDividend', u'lastSize',
                   u'lastTheta', u'lastUndPrice', u'lastVega', u'lowPrice',
                   u'modelDelta', u'modelGamma', u'modelImpliedVol', u'modelOptPrice',
                   u'modelPvDividend', u'modelTheta', u'modelUndPrice', u'modelVega',
                   u'multiplier', u'strike']] = cadena_opcs[[u'CallOI',u'PutOI', u'Volume', u'askDelta', u'askGamma',
                   u'askImpliedVol', u'askOptPrice', u'askPrice', u'askPvDividend',
                   u'askSize', u'askTheta', u'askUndPrice', u'askVega', u'bidDelta',
                   u'bidGamma', u'bidImpliedVol', u'bidOptPrice', u'bidPrice',
                   u'bidPvDividend', u'bidSize', u'bidTheta', u'bidUndPrice', u'bidVega',
                   u'closePrice', u'highPrice', u'lastDelta', u'lastGamma', u'lastImpliedVol',
                   u'lastOptPrice', u'lastPrice', u'lastPvDividend', u'lastSize',
                   u'lastTheta', u'lastUndPrice', u'lastVega', u'lowPrice',
                   u'modelDelta', u'modelGamma', u'modelImpliedVol', u'modelOptPrice',
                   u'modelPvDividend', u'modelTheta', u'modelUndPrice', u'modelVega',
                   u'multiplier', u'strike']].apply(pd.to_numeric)
    cadena_opcs['load_dttm'] = cadena_opcs['load_dttm'].apply(pd.to_datetime)

    # cadena de opciones en el cierre del dia anterior a la fecha de valoracion
    # realmente saca todos los precios del dia anteriores a dicha fecha
    cadena_opcs_tminus1=ra.extrae_options_chain(year=i_year, month=i_month_num_tminus1, day=i_day_tminus1,hour=23, #cierre del dia anterior
                                                symbol=i_symbol,expiry=i_expiry,secType=i_secType)

    ###################################################################################################################
    # foto de la cuenta en la fecha de valoracion
    ###################################################################################################################
    account=ra.extrae_account_snapshot_new(year=i_year, month=fecha_valoracion.month, day=p_i_day_t0,
                                           hour=p_i_hour_t0,accountid=accountid,
                                           scenarioMode=scenarioMode, simulName=simulName)
    # foto de la cuenta en el cierre del dia anterior a la fecha de valoracion
    account=account.append(ra.extrae_account_snapshot_new(year=i_year, month=fecha_val_tminus1.month,
                                                          day=fecha_val_tminus1.day,hour=23,
                                                          accountid=accountid, #cierre del dia anterior
                                                          scenarioMode = scenarioMode, simulName = simulName))
    #account.to_excel("account.xlsx")

    ###################################################################################################################
    # 1.- PRECIO DEL SUBYACENTE EN valuation datetime
    #
    # sacar el precio del subyacente mas probable (la moda del lastUndPrice por cada load_dttm)
    # para cada load_dttm debe haber pequeñas diferencias en el precio del subyacente
    # por el modo asincrono en que se recuperan los datos.
    ###################################################################################################################
    underl_prc_df = cadena_opcs[['lastUndPrice','load_dttm']].groupby(['load_dttm']).agg(lambda x: stats.mode(x)[0][0])
    #print "underl_prc_df ",underl_prc_df
    # si el dataset esta vacio no se puede continuar en la analitica.
    if underl_prc_df.empty:
        print "No option data to analyze (t). Exiting ..."
        return
    # esto me da el ultimo precio disponible en la H5 del subyacente antes de la fecha de valoracion
    underl_prc = float(underl_prc_df.ix[np.max(underl_prc_df.index)]['lastUndPrice'])
    hora_max=np.max(underl_prc_df.index)

    ###################################################################################################################
    ## Lo mismo pero para t-1 (ayer)
    ###################################################################################################################
    underl_prc_df_tminus1 = cadena_opcs_tminus1[['lastUndPrice','load_dttm']].groupby(['load_dttm']).agg(lambda x: stats.mode(x)[0][0])
    if underl_prc_df_tminus1.empty:
        print "No option data to analyze (t-1). Exiting ..."
        return
    underl_prc_tminus1 = float(underl_prc_df_tminus1.ix[np.max(underl_prc_df_tminus1.index)]['lastUndPrice'])
    hora_max_tminus1=np.max(underl_prc_df_tminus1.index)

    ###################################################################################################################
    # calcular el retorno del subyacente entre el cierre de ayer y lo que llevamos de hoy
    ###################################################################################################################
    try:
        retorno_suby_dia = np.log(float(underl_prc_df[underl_prc_df.index == hora_max]['lastUndPrice'])
               / float(underl_prc_df_tminus1[underl_prc_df_tminus1.index == hora_max_tminus1]['lastUndPrice'] ) )
    except ZeroDivisionError:
        retorno_suby_dia = np.nan

    ###################################################################################################################
    # FILTRO la ultima cadena de opciones disponible para el subyacente y vencimiento dados en el datetime de valoracion
    ###################################################################################################################
    cadena_opcs = cadena_opcs[cadena_opcs['load_dttm'] == hora_max]
    #cadena_opcs.info()
    cadena_opcs.index = pd.RangeIndex(start=0, stop=len(cadena_opcs), step=1)
    cadena_opcs = cadena_opcs.dropna(subset=['askDelta'])
    cadena_opcs=cadena_opcs.drop_duplicates(subset=['strike','symbol','secType','right','load_dttm','expiry',
                                           'current_date','currency','exchange'])

    ###################################################################################################################
    # CALCULA LA IV ATM
    # saca los strikes mas cerca de ATM de la cadena de opciones del paso anterior
    ###################################################################################################################
    subset_df=cadena_opcs.ix[(cadena_opcs['strike'] - underl_prc).abs().argsort()[:4]]
    # cadena_opcs[(cadena_opcs['strike']-underl_prc).abs() < 0.5]
    # mejor usar modelImpliedVol que lastImpliedVol ???!!!
    #calcula la volimpl del subyacente como las medias de las vol impl de las opciones mas ATM
    impl_vol_suby = subset_df['modelImpliedVol'].mean()

    ###################################################################################################################
    # SACA FECHAS para las que hay TRADES Y JUNTA CON PRICES DE OPTIONS CHAIN
    # se obtiene la lista de todas las datetimes en que se han realizado operaciones
    ###################################################################################################################
    temp_ts1=operaciones.reset_index().set_index('times').tz_localize('Europe/Madrid')
    oper_series1=temp_ts1.index.unique()

    # Enriquezco las posiciones desde el inicio con las quotes de la cadena de opciones IV, griegas precio del subyacente etc
    # en cada date de todas las operaciones que se han realizado desde el inicio de la estrategia
    lista_dias_con_trades = set([x.date() for x in oper_series1])
    #print "list de dias con operaciones en la estrategia: " , lista_dias_con_trades
    cadena_opcs_orders= pd.DataFrame()
    for x in lista_dias_con_trades:
        temporal1 =ra.extrae_options_chain(year=x.year, month=str(x.month).zfill(2),  #x.strftime("%B")[:3],
                                           day=str(x.day).zfill(2),hour="23",
                                           symbol=i_symbol,expiry=i_expiry,secType=i_secType)
        cadena_opcs_orders=cadena_opcs_orders.append(temporal1)

    cadena_opcs_orders[[u'CallOI',u'PutOI', u'Volume', u'askDelta', u'askGamma',
           u'askImpliedVol', u'askOptPrice', u'askPrice', u'askPvDividend',
           u'askSize', u'askTheta', u'askUndPrice', u'askVega', u'bidDelta',
           u'bidGamma', u'bidImpliedVol', u'bidOptPrice', u'bidPrice',
           u'bidPvDividend', u'bidSize', u'bidTheta', u'bidUndPrice', u'bidVega',
           u'closePrice', u'highPrice', u'lastDelta', u'lastGamma', u'lastImpliedVol',
           u'lastOptPrice', u'lastPrice', u'lastPvDividend', u'lastSize',
           u'lastTheta', u'lastUndPrice', u'lastVega', u'lowPrice',
           u'modelDelta', u'modelGamma', u'modelImpliedVol', u'modelOptPrice',
           u'modelPvDividend', u'modelTheta', u'modelUndPrice', u'modelVega',
           u'multiplier', u'strike']] = cadena_opcs_orders[[u'CallOI',u'PutOI', u'Volume', u'askDelta', u'askGamma',
           u'askImpliedVol', u'askOptPrice', u'askPrice', u'askPvDividend',
           u'askSize', u'askTheta', u'askUndPrice', u'askVega', u'bidDelta',
           u'bidGamma', u'bidImpliedVol', u'bidOptPrice', u'bidPrice',
           u'bidPvDividend', u'bidSize', u'bidTheta', u'bidUndPrice', u'bidVega',
           u'closePrice', u'highPrice', u'lastDelta', u'lastGamma', u'lastImpliedVol',
           u'lastOptPrice', u'lastPrice', u'lastPvDividend', u'lastSize',
           u'lastTheta', u'lastUndPrice', u'lastVega', u'lowPrice',
           u'modelDelta', u'modelGamma', u'modelImpliedVol', u'modelOptPrice',
           u'modelPvDividend', u'modelTheta', u'modelUndPrice', u'modelVega',
           u'multiplier', u'strike']].apply(pd.to_numeric)
    cadena_opcs_orders[['expiry','load_dttm']] = cadena_opcs_orders[['expiry','load_dttm']].apply(pd.to_datetime)
    cadena_opcs_orders_temp=cadena_opcs_orders.rename(columns={'load_dttm':'times'})
    #cadena_opcs_orders_temp=cadena_opcs_orders_temp.reset_index().set_index(['times','strike','right','expiry','symbol'])
    #cadena_opcs_orders_temp.info()
    cadena_opcs_orders_temp['on']=cadena_opcs_orders_temp['times'].dt.strftime("%Y-%m-%d %H")
    cadena_opcs_orders_temp = cadena_opcs_orders_temp.drop_duplicates(subset=['expiry', 'strike', 'symbol', 'right', 'on'], keep='last')
    operaciones['on']=operaciones['times'].dt.strftime("%Y-%m-%d %H")

    # devuelve en ops_w_hist_opt_chain la lista de operaciones historicas con las cotizaciones de la cadena de opciones incluida
    ops_w_hist_opt_chain = pd.merge(operaciones, cadena_opcs_orders_temp, how='left', on=['expiry', 'strike','symbol','right','on']) #.fillna(method='ffill')
    #ops_w_hist_opt_chain.to_excel("real.xlsx")
    # Para cada datetime en que se han realizado operaciones (variable oper_series1) se extraen de cadena_opcs2 los precios mas adecuados
    # cruzar esta lista con las cotizacionon la IV
    ops_w_hist_opt_chain = ops_w_hist_opt_chain[['localSymbol','avgprice','expiry','conId','current_datetime_x','symbol','lastUndPrice',
                                    'modelImpliedVol','load_dttm',
                                    'multiplier_x','price','qty','shares','right','side','strike','times_x','times_y',
                                    'bidPrice','askPrice','lastPrice','current_datetime_y',
                                    'modelDelta','modelGamma','modelTheta','modelVega']]

    ops_w_hist_opt_chain= ops_w_hist_opt_chain.rename(columns={'multiplier_x': 'multiplier',  'times_x':'times_ops',  'times_y':'times_opt_chain',
                                         'current_datetime_x':'current_datetime_ops','current_datetime_y':'current_datetime_chain',
                                         'load_dttm_x': 'load_dttm_operaciones'})

    #caclulo dias a expiracion en el trade
    ops_w_hist_opt_chain['DTE'] = (ops_w_hist_opt_chain['expiry'] - ops_w_hist_opt_chain['times_ops'] ).astype('timedelta64[D]').astype(int)
    ## hay que calcular aqui la IV de las opciones ATM en el momento del trade, se saca de la cadena de opciones
    cadena_opcs_orders.index = cadena_opcs_orders[['strike','load_dttm','right']]

    # es necesario calcular esto aperturado por load_dttm (aqui se calcula un valor para todas las fechas de operaciones)
    # result2['ImplVolATM'] = cadena_opcs2.ix[(cadena_opcs2['strike'] - cadena_opcs2['lastUndPrice']).abs().argsort()[:16] ]['modelImpliedVol'].mean()
    temp1=cadena_opcs_orders.ix[(cadena_opcs_orders['strike'] - cadena_opcs_orders['lastUndPrice']).abs().argsort()[:64] ][['load_dttm','modelImpliedVol']]
    temp1=temp1.groupby(by=['load_dttm'])['load_dttm','modelImpliedVol'].mean()
    temp1=temp1.rename(columns={'modelImpliedVol':'ImplVolATM'})
    temp1=temp1.reset_index()
    temp1['on']=temp1['load_dttm'].dt.strftime("%Y-%m-%d %H")

    ops_w_hist_opt_chain=ops_w_hist_opt_chain[['times_opt_chain','load_dttm','times_ops','localSymbol','lastUndPrice','avgprice','multiplier',
                           'qty','side','DTE','modelDelta','modelGamma','modelTheta','modelVega','strike','shares']].drop_duplicates()

    ops_w_hist_opt_chain['on']=ops_w_hist_opt_chain['times_opt_chain'].dt.strftime("%Y-%m-%d %H")

    trade_summary = pd.merge(ops_w_hist_opt_chain, temp1, how='left', on=['on'])
    # calcular 1SD con el horizonte de dias DTE
    trade_summary['1SD'] = trade_summary['lastUndPrice'] * trade_summary['ImplVolATM'] / (365.0/((trade_summary['DTE']) ) )**0.5
    trade_summary['lastUndPrice_less1SD']=trade_summary['lastUndPrice'] - trade_summary['1SD']
    trade_summary['lastUndPrice_plus1SD']=trade_summary['lastUndPrice'] + trade_summary['1SD']
    trade_summary['DIT'] = (fecha_valoracion - ops_w_hist_opt_chain['times_ops'] ).astype('timedelta64[D]').astype(int) # days in trade
    trade_summary=trade_summary.rename(columns={'load_dttm_x':'load_dttm'})
    trade_summary = trade_summary.drop('load_dttm_y', 1)
    trade_summary = trade_summary.drop('on', 1)
    trade_summary['sign'] = 0
    trade_summary.loc[trade_summary['side'] == 'SLD', 'sign'] = -1
    trade_summary.loc[trade_summary['side'] == 'BOT', 'sign'] = 1
    trade_summary['CreditoBruto'] = trade_summary['avgprice'] * trade_summary['multiplier'] * trade_summary['shares'] * trade_summary['sign']
    #esto es util para la fase de apertura
    trade_summary['1SD15D'] = trade_summary['lastUndPrice'] * trade_summary['ImplVolATM'] / (365.0/((15.0) ) )**0.5
    trade_summary['1SD21D'] = trade_summary['lastUndPrice'] * trade_summary['ImplVolATM'] / (365.0/((21.0) ) )**0.5
    trade_summary=trade_summary.sort_values(by=['times_opt_chain', 'localSymbol'] , ascending=[False, True])

    # juntar las posiciones con la cadena de opciones para tener las griegas, bid ask volumen etc
    # el datetime de los dos conjutnos de datos tiene siempre un lag por ese motivo el market value de pos
    # no coincide con el last price que vienen de cadena_opcs
    positions_summary = pd.merge(posiciones, cadena_opcs, how='left', on=['expiry', 'strike','symbol','right'])
    positions_summary = positions_summary[['localSymbol','right','position','bidPrice','askPrice','lastPrice',
                                           'marketValue','averageCost','multiplier_x','modelDelta','modelGamma',
                                           'modelTheta','modelVega','load_dttm_y','load_dttm_x','modelImpliedVol',
                                           'expiry','lastUndPrice']]

    #cross check marketValue from portfolio and that derived from market prices
    positions_summary['marketValuefromPrices'] = positions_summary['position'] * positions_summary['multiplier_x'] \
                                                 * ( positions_summary['bidPrice'] + positions_summary['askPrice'] ) / 2.0
    positions_summary['expiry'] = positions_summary['expiry'].apply(pd.to_datetime)
    positions_summary.rename(columns={'multiplier_x': 'multiplier', 'load_dttm_y': 'load_dttm_cadena'
                                        , 'load_dttm_x': 'load_dttm_posiciones'}, inplace=True)
    """
    u'averageCost', u'conId', u'expiry', u'localSymbol', u'marketPrice',
           u'marketValue', u'multiplier_x', u'position', u'realizedPNL', u'right',
           u'strike', u'symbol', u'unrealizedPNL', u'current_datetime_x',
           u'load_dttm_x', u'CallOI', u'Halted', u'PutOI', u'Volume', u'askDelta',
           u'askGamma', u'askImpliedVol', u'askOptPrice', u'askPrice',
           u'askPvDividend', u'askSize', u'askTheta', u'askUndPrice', u'askVega',
           u'bidDelta', u'bidGamma', u'bidImpliedVol', u'bidOptPrice', u'bidPrice',
           u'bidPvDividend', u'bidSize', u'bidTheta', u'bidUndPrice', u'bidVega',
           u'closePrice', u'currency', u'current_date', u'current_datetime_y',
           u'exchange', u'highPrice', u'lastDelta', u'lastGamma',
           u'lastImpliedVol', u'lastOptPrice', u'lastPrice', u'lastPvDividend',
           u'lastSize', u'lastTheta', u'lastUndPrice', u'lastVega', u'load_dttm_y',
           u'lowPrice', u'modelDelta', u'modelGamma', u'modelImpliedVol',
           u'modelOptPrice', u'modelPvDividend', u'modelTheta', u'modelUndPrice',
           u'modelVega', u'multiplier_y', u'secType'
    """

    #caclulo dias a expiracion en el trade
    positions_summary['DTE'] = (positions_summary['expiry'] - fecha_valoracion ).astype('timedelta64[D]').astype(int)
    ## hay que calcular aqui la IV de las opciones ATM en el momento del trade, se saca de la cadena de opciones
    positions_summary['ImplVolATM'] = cadena_opcs.ix[(cadena_opcs['strike'] - cadena_opcs['lastUndPrice']).abs().argsort()[:4] ]['modelImpliedVol'].mean()
    # calcular 1SD con el horizonte de dias DTE
    positions_summary['1SD'] = positions_summary['lastUndPrice'] * positions_summary['ImplVolATM'] / (365.0/((positions_summary['DTE']) ) )**0.5
    positions_summary['lastUndPrice_less1SD']=positions_summary['lastUndPrice'] - positions_summary['1SD']
    positions_summary['lastUndPrice_plus1SD']=positions_summary['lastUndPrice'] + positions_summary['1SD']
    positions_summary['midPrice']= ( positions_summary['bidPrice'] + positions_summary['askPrice'] ) / 2
    positions_summary['ValCurrent'] = positions_summary['midPrice'] * np.sign(positions_summary['position'])
    positions_summary['costUnit'] = positions_summary['averageCost'] * np.sign(positions_summary['position']) / positions_summary['multiplier']
    positions_summary['Dshort'] = positions_summary['modelDelta'] * positions_summary['multiplier']
    positions_summary['DshortPosition'] = positions_summary['modelDelta'] * positions_summary['multiplier'] * positions_summary['position']
    positions_summary['GammaPosition'] = positions_summary['modelGamma'] * positions_summary['multiplier'] * positions_summary['position']
    positions_summary['ThetaPosition'] = positions_summary['modelTheta'] * positions_summary['multiplier'] * positions_summary['position']
    positions_summary['VegaPosition'] = positions_summary['modelVega'] * positions_summary['multiplier'] * positions_summary['position']
    positions_summary=positions_summary.sort_values(by=['right', 'localSymbol'] , ascending=[False, True])

    # agregar por spreads
    spread_summary = positions_summary.groupby(by=['right'])['ValCurrent','costUnit','marketValue','DshortPosition'].sum()
    # agregar total
    total_summary = positions_summary[['DshortPosition','GammaPosition','ThetaPosition','VegaPosition','marketValue']].sum()
    total_summary_risk = positions_summary[['lastUndPrice','ImplVolATM','multiplier']].mean()
    total_summary_risk['Pts1SD1D'] = total_summary_risk['lastUndPrice'] * total_summary_risk['ImplVolATM'] / np.sqrt(365.0)
    total_summary_risk['Pts1SD5D'] = total_summary_risk['lastUndPrice'] * total_summary_risk['ImplVolATM'] / np.sqrt(365.0/5.0)
    total_summary=total_summary.append(total_summary_risk)

    # impacto en el margen de la operacion
    # recibimos el date time que corresponde al time en que se hizo la lista de operaciones y que viene del dataframe
    # operaciones y se calcula la delta del margen en la cuenta. Las fechas de los snapshots de account será los dos más cercanos
    # de los que se disponga en el h5 accounts (este fichero se actualiza cada hora durante RTH)
    # si  en eseintervalo de tiempo se hiciesen operaciones en otros symbol-expirations el resultado de aqui sería incorrecto
    # lo esperable es que el margen sea igual a la diferencia entre los strikes del spread del iron condor mas ancho
    #ops.reset_index().drop_duplicates(subset='index',keep='last').set_index('index')
    #ops
    temp_margin=pd.DataFrame()
    temp_premium=pd.DataFrame()
    for x in oper_series1:
        #temporal1 =ra.extrae_account_delta_new(year=x.year, month=x.strftime("%B")[:3],day=x.day,hour=x.hour,minute=x.minute)
        temporal1 =ra.extrae_account_delta_new(year=x.year, month=x.month,day=x.day,hour=x.hour,
                                               minute=x.minute,accountid=accountid,
                                               scenarioMode = scenarioMode,simulName=simulName)
        temporal1['load_dttm'] = temporal1['current_datetime_txt']
        temporal1=temporal1.set_index('load_dttm')
        t_margin=temporal1[['RegTMargin_USD','MaintMarginReq_USD','InitMarginReq_USD',
                                'FullMaintMarginReq_USD','FullInitMarginReq_USD']].apply(pd.to_numeric)
        t_margin.diff()
        t_margin['fecha_operacion']=str(x)
        temp_margin = temp_margin.append(t_margin)
        # cash primas y comisiones
        # impacto en comisiones de las operaciones
        # dado un datetime donde se han ejecutado operaciones hace la delta de las posiciones cash de la cuenta y eso
        # resulta en el valor de las comisiones de ese conjunto de operaciones (hay que considerar el dienro recibido de)
        # la venta de opciones y tambien si se tiene posiciones en divisa los cambios ????
        t_prem =temporal1[['TotalCashBalance_BASE','TotalCashBalance_EUR','TotalCashBalance_USD','TotalCashValue_USD',
                          'CashBalance_EUR','CashBalance_USD','TotalCashValue_C_USD','TotalCashValue_S_USD',
                          'CashBalance_BASE','ExchangeRate_EUR']].apply(pd.to_numeric).diff()
        t_prem['fecha_operacion']=str(x)
        temp_premium = temp_premium.append(t_prem)
    #temporal1.to_excel("temporal1_simul.xlsx")
    # por cada fecha de operacion tendremos cuatro registros tipicamente en la tabla temp_margin
    # dos por cada spread de la tic (4 en total) y los dos por cada spread son el margen antes de la operacion
    # y el margen despues. Dado que el margen que coge el broker en una tic es por un spread solo
    #el margen incial final es para toda la cuenta, y si se resta da el efecto de la operacion
    # como los snapshots de la cuenta son por cada hora, si se abre una tic completa (dos spreads) dentro de una
    # misma hora aparecera el incremento del margen dos veces
    # sin embargo por lo dicho antes si ya se tiene un spread de la tic abierto el otro no incrementa el margen
    # a no ser que el ancho del nuevo spread sea mayor que el anterior.

    temp_margin=temp_margin.reset_index()[['load_dttm','FullInitMarginReq_USD']].drop_duplicates()
    temp_premium=temp_premium.reset_index()[['load_dttm','TotalCashValue_USD']].drop_duplicates()
    temp_margin2=temp_margin.rename(columns={'FullInitMarginReq_USD':'FullInitMarginReq_USD_actual'})
    temp_premium2=temp_premium.rename(columns={'TotalCashValue_USD':'Impacto_encash_ultimo_periodo1h'})
    temp_premium2=temp_premium2.dropna()

    temp_margin2['on']=pd.to_datetime(temp_margin2['load_dttm'], format="%Y-%m-%d %H:%M:%S")
    temp_premium2['on']=pd.to_datetime(temp_premium2['load_dttm'], format="%Y-%m-%d %H:%M:%S")

    temp_margin2['on'] = temp_margin2['on'].dt.strftime("%Y-%m-%d %H")
    temp_premium2['on'] = temp_premium2['on'].dt.strftime("%Y-%m-%d %H")

    temp_credbruto=trade_summary.groupby(by=['times_opt_chain'])['times_opt_chain','CreditoBruto'].sum()
    temp_credbruto=temp_credbruto.reset_index()
    temp_credbruto['on']=temp_credbruto['times_opt_chain'].dt.strftime("%Y-%m-%d %H")

    trade_summary_margin_cash = pd.merge(temp_margin2, temp_premium2, how='left', on=['on'])
    trade_summary_margin_cash = pd.merge(trade_summary_margin_cash, temp_credbruto, how='left', on=['on'])
    trade_summary_margin_cash = trade_summary_margin_cash.drop('on', 1)
    trade_summary_margin_cash = trade_summary_margin_cash.drop('load_dttm_y', 1)
    trade_summary_margin_cash = trade_summary_margin_cash.drop('times_opt_chain', 1)
    trade_summary_margin_cash=trade_summary_margin_cash.rename(columns={'load_dttm_x':'load_dttm'})
    trade_summary_margin_cash['Comisiones'] = np.abs(trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h'] + trade_summary_margin_cash['CreditoBruto'] )
    trade_summary_margin_cash['Margen_neto'] = 0
    trade_summary_margin_cash.loc[trade_summary_margin_cash['FullInitMarginReq_USD_actual'] <= 0, 'Margen_neto'] = 0
    trade_summary_margin_cash.loc[trade_summary_margin_cash['FullInitMarginReq_USD_actual'] > 0, 'Margen_neto'] = trade_summary_margin_cash['FullInitMarginReq_USD_actual'] - trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h']

    trade_summary_margin_cash['load_dttm']=pd.to_datetime(trade_summary_margin_cash['load_dttm'], format="%Y-%m-%d %H:%M:%S")

    trade_summary_margin_cash['BeneficioMaximoPct'] = 0
    trade_summary_margin_cash.loc[trade_summary_margin_cash['FullInitMarginReq_USD_actual'] > 0, 'BeneficioMaximoPct'] = trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h'] / trade_summary_margin_cash['Margen_neto']
    trade_summary_margin_cash=trade_summary_margin_cash.sort_values(by='load_dttm')
    #print "trade_summary_margin_cash " , trade_summary_margin_cash
    # calcular apalancamientos de la posicion completa
    # a continuacion es el margen neto que quedo tras la ultima operacion en la estrategia completa
    cb_tot = trade_summary_margin_cash [(trade_summary_margin_cash['load_dttm']) ==trade_summary_margin_cash.reset_index().max()['load_dttm'] ]  ['Margen_neto']

    total_summary['AD1PCT'] =np.abs( total_summary['multiplier'] * ( ( (np.abs(total_summary['DshortPosition']) + total_summary['GammaPosition'] )
                                                                       * total_summary['lastUndPrice'] / total_summary['multiplier'] ) - total_summary['GammaPosition'] )
                                                                    / cb_tot ).values[0]


    total_summary['AD1SD1D'] =np.abs( total_summary['multiplier'] * ( ( (np.abs(total_summary['DshortPosition']) + total_summary['GammaPosition'] )
                                                                       * total_summary['lastUndPrice'] * total_summary['ImplVolATM'] / np.sqrt(365.0) )
                                                                     - total_summary['GammaPosition'] ) / cb_tot ).values[0]

    total_summary['MaxDsAD1PCT'] = np.abs ( 3.0 * total_summary['DshortPosition'] / total_summary['AD1PCT'] )
    total_summary['MaxDsAD1SD1D'] = np.abs ( 3.0 * total_summary['DshortPosition'] / total_summary['AD1SD1D'] )
    total_summary['MaxDs'] = np.abs ( 0.04 * cb_tot / total_summary['Pts1SD1D'] ).values[0]
    total_summary['thetaDeltaRatio'] = np.abs ( total_summary['ThetaPosition'] / total_summary['DshortPosition'] )
    total_summary['thetaGammaRatio'] = np.abs ( total_summary['ThetaPosition'] / total_summary['GammaPosition'] )
    total_summary['VegaThetaRatio'] = np.abs ( total_summary['VegaPosition'] / total_summary['ThetaPosition'] )

    ## Resultado final
    # date time de los datos de option chain usados en los cálculos (hora_max)
    # underly (todos los valores son a fecha de valoracion / fecha actual):
    #   fecha valoracion, ticker , last price,
    # trade (todos los valores son a fecha de la operacion):
    #   fecha de operacion, impl vol, dias a expiracion
    #print "____________________Precios del subyacente almancenados de la fecha de valoracion _____________"
    #print underl_prc_df
    #print "____________________Con las siguientes se calcula la IV del subyacente (las IV de las opciones P C mas ATM) _____________"
    #print subset_df[['modelImpliedVol','strike','load_dttm','right']]
    #print "---------------------------------------- underlying ----------------------------------------"
    #print ({'hora max datos':hora_max, 'fecha valoracion':fecha_valoracion , 'subycente':i_symbol, 'precio last subyacente':underl_prc,
    #        'precio close anterior subyacente':underl_prc_tminus1, 'vol. implicita subyacente': impl_vol_suby ,'retorno subyacente': retorno_suby_dia})
    #print "---------------------------------------- Sumario Trade TIC ----------------------------------------"
    #print trade_summary # muestra el precio de subyacente en el momento de cada trade de la tic
    #print "---------------------------------------- Sumario Posiciones TIC ----------------------------------------"
    #print positions_summary
    #print "---------------------------------------- Sumario Margin y Cash ----------------------------------------"
    #print trade_summary_margin_cash
    #print "---------------------------------------- Sumario TIC por Spread  a fecha valoracion ----------------------------------------"
    # resultados por spread de la TIC
    #print spread_summary
    #print "---------------------------------------- Sumario TIC global a fecha valoracion ----------------------------------------"
    # resultados globales de la TIC
    #print total_summary

    #fecha_ini=dt.date(2016,8,1)
    #if np.min(trade_summary['times_ops']).date() >= fecha_ini:
    #    fecha_ini=np.min(trade_summary['times_ops']).date()

    #ivol=get_ivol_series(date_ini=fecha_ini.strftime("%Y-%m-%d"),date_end=fecha_valoracion.date().strftime("%Y-%m-%d"))
    #col=['IV_Index_call_52wk_hi','IV_Index_call_52wk_lo','IV_Index_call_52wk_hi','IV_Index_mean_52wk_hi',
    #     'IV_Index_mean_52wk_lo','IV_Index_put_52wk_hi','IV_Index_put_52wk_lo']
    #ivol.drop(col,axis=1,inplace=True)

    precio_undl_inicio_strat = \
        (trade_summary.loc[trade_summary.times_ops == np.min(trade_summary.times_ops)]['lastUndPrice']).unique()[0]
    iv_atm_inicio_strat = \
        (trade_summary.loc[trade_summary.times_ops == np.min(trade_summary.times_ops)]['ImplVolATM']).unique()[0]

    dte_ini = \
        (trade_summary.loc[trade_summary.times_ops == np.min(trade_summary.times_ops)]['DTE']).unique()[0]

    ini_1SD15D = \
        (trade_summary.loc[trade_summary.times_ops == np.max(trade_summary.times_ops)]['1SD15D']).unique()[0].item()

    ini_1SD21D = \
        (trade_summary.loc[trade_summary.times_ops == np.max(trade_summary.times_ops)]['1SD21D']).unique()[0].item()

    prc_ajuste_1SD21D_up = precio_undl_inicio_strat + ini_1SD21D
    prc_ajuste_1SD21D_dn = precio_undl_inicio_strat - ini_1SD21D

    prc_ajuste_1SD15D_up = precio_undl_inicio_strat + ini_1SD15D
    prc_ajuste_1SD15D_dn = precio_undl_inicio_strat - ini_1SD15D

    dte= positions_summary[['DTE']].mean().item()

    margen_neto= (trade_summary_margin_cash - trade_summary_margin_cash.shift())['FullInitMarginReq_USD_actual'].dropna().item()
    comisiones=trade_summary_margin_cash['Comisiones'].dropna().sum()

    impacto_cash = trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h'].dropna().sum()

    BearCallMaxDShortJL = positions_summary.loc[(positions_summary.right=="C")
                                                  & (positions_summary.position < 0),'Dshort'].max()

    BullPutMaxDShortJL = positions_summary.loc[(positions_summary.right=="P")
                                                  & (positions_summary.position < 0),'Dshort'].min() # because delta is <0 for puts

    BearCallMaxIVShort = positions_summary.loc[(positions_summary.right=="C")
                                                  & (positions_summary.position < 0),'modelImpliedVol'].max()

    BullPutMaxIVShort = positions_summary.loc[(positions_summary.right=="P")
                                                  & (positions_summary.position < 0),'modelImpliedVol'].max()

    BearCallMaxDeltaShort = positions_summary.loc[(positions_summary.right=="C")
                                                  & (positions_summary.position < 0),'modelDelta'].max()

    BullPutMaxDeltaShort = positions_summary.loc[(positions_summary.right=="P")
                                                  & (positions_summary.position < 0),'modelDelta'].min() # because delta is <0 for puts

    BearCallMaxDeltaShortPos = positions_summary.loc[(positions_summary.right=="C")
                                                  & (positions_summary.position < 0),'DshortPosition'].max()

    BullPutMaxDeltaShortPos = positions_summary.loc[(positions_summary.right=="P")
                                                  & (positions_summary.position < 0),'DshortPosition'].min() # because delta is <0 for puts

    p_n_l = positions_summary[['marketValue']].sum().item() -trade_summary[['CreditoBruto']].sum().item()
    coste_base = trade_summary[['CreditoBruto']].sum().item()
    #print p_n_l , dte
    #print "margen_neto " , margen_neto.item()
    #sumario_subyacente
    max_profit = abs(coste_base)/margen_neto
    pnl_margin_ratio = p_n_l / margen_neto
    puntos_desde_last_close = (underl_prc-underl_prc_tminus1)
    linea_mercado = (1-( (pnl_margin_ratio) / (max_profit) ))
    e_v = (max_profit)-(pnl_margin_ratio)
    dit = dte_ini - dte
    row_datos = pd.DataFrame({'subyacente':i_symbol,
                              'expiry':i_expiry,
                              'accountid':accountid,
                              'ini_1SD15D':ini_1SD15D,
                              'ini_1SD21D':ini_1SD21D,
                              'prc_ajuste_1SD21D_up':prc_ajuste_1SD21D_up,
                              'prc_ajuste_1SD21D_dn':prc_ajuste_1SD21D_dn,
                              'prc_ajuste_1SD15D_up':prc_ajuste_1SD15D_up,
                              'prc_ajuste_1SD15D_dn':prc_ajuste_1SD15D_dn,
                              'precio_last_subyacente':underl_prc,
                              'precio_close_anterior_subyacente':underl_prc_tminus1,
                              'precio_undl_inicio_strat':precio_undl_inicio_strat,
                              'puntos_desde_last_close':puntos_desde_last_close,
                              'iv_subyacente': impl_vol_suby ,
                              'iv_atm_inicio_strat': iv_atm_inicio_strat,
                              'dte_inicio_estrategia': dte_ini,
                              'BearCallMaxDeltaShort':BearCallMaxDeltaShort,
                              'BullPutMaxDeltaShort':BullPutMaxDeltaShort,
                              'BearCallMaxDShortJL':BearCallMaxDShortJL,
                              'BullPutMaxDShortJL':BullPutMaxDShortJL,
                              'BearCallMaxIVShort':BearCallMaxIVShort,
                              'BullPutMaxIVShort':BullPutMaxIVShort,
                              'BearCallMaxDeltaShortPos':BearCallMaxDeltaShortPos,
                              'BullPutMaxDeltaShortPos':BullPutMaxDeltaShortPos,
                              'dte': dte,
                              'dit': dit,
                              'PnL': p_n_l,
                              'coste_base':coste_base,
                              'max_profit':max_profit,
                              'pnl_margin_ratio': pnl_margin_ratio,
                              'linea_mercado':linea_mercado,
                              'e_v': e_v,
                              'comisiones':comisiones,
                              'impacto_cash':impacto_cash,
                              'portfolio': positions_summary[["localSymbol","right","position",
                                                              "marketValue"]].reset_index().drop('index',axis=1).to_json(orient='records'),
                              'retorno_subyacente': retorno_suby_dia,
                              'lastUndPrice':total_summary['lastUndPrice'],
                              'DshortPosition':total_summary['DshortPosition'],
                              'GammaPosition':total_summary['GammaPosition'],
                              'ThetaPosition':total_summary['ThetaPosition'],
                              'VegaPosition':total_summary['VegaPosition'],
                              'marketValue':total_summary['marketValue'],
                              'lastUndPrice':total_summary['lastUndPrice'],
                              'ImplVolATM':total_summary['ImplVolATM'],
                              'multiplier':total_summary['multiplier'],
                              'Pts1SD1D':total_summary['Pts1SD1D'],
                              'Pts1SD5D':total_summary['Pts1SD5D'],
                              'AD1PCT':total_summary['AD1PCT'],
                              'AD1SD1D':total_summary['AD1SD1D'],
                              'MaxDsAD1PCT':total_summary['MaxDsAD1PCT'],
                              'MaxDsAD1SD1D':total_summary['MaxDsAD1SD1D'],
                              'MaxDs':total_summary['MaxDs'],
                              'thetaDeltaRatio':total_summary['thetaDeltaRatio'],
                              'thetaGammaRatio':total_summary['thetaGammaRatio'],
                              'VegaThetaRatio':total_summary['VegaThetaRatio'],
                              'DTMaxdatost_1':hora_max_tminus1.strftime("%Y-%m-%d %H:%M:%S"),
                              'DTMaxdatost': hora_max.strftime("%Y-%m-%d %H:%M:%S"),
                              'DToperaciones':str(lista_dias_con_trades),
                              'MargenNeto': margen_neto
                              },index=[hora_max.strftime("%Y-%m-%d %H:%M:%S")])
    if appendh5 != 1:
        return
    globalconf = config.GlobalConfig()
    store = globalconf.open_ib_abt_strategy_tic(scenarioMode=scenarioMode)
    if scenarioMode == "N":
        loc1 = i_symbol
    elif scenarioMode == "Y":
        loc1 = simulName
    store.append("/" + loc1, row_datos, data_columns=True,
                 min_itemsize={'portfolio': 500})
    store.close()

    #trade_summary=trade_summary.dropna()
    #writer = ExcelWriter('dailyanalytics'+fecha_valoracion.strftime("%Y-%m-%d")+'.xlsx')
    #underl_prc_df.to_excel(writer,sheet_name='Precios Suby.')
    #ivol.to_excel(writer,sheet_name='IVolatility')
    #subset_df[['modelImpliedVol','strike','load_dttm','right']].to_excel(writer,sheet_name='Calculo IV ATM')
    #sumario_subyacente.to_excel(writer,sheet_name='Sumario Suby.')
    #trade_summary.to_excel(writer,sheet_name='Sumario Trades')
    #positions_summary.to_excel(writer,sheet_name='Sumario Posiciones')
    #trade_summary_margin_cash.to_excel(writer,sheet_name='Sumario Margin Primas')
    #spread_summary.to_excel(writer,sheet_name='Sumario Spreads')
    #total_summary.to_frame(name='Conceptos').to_excel(writer,sheet_name='Sumario estrategia')
    #writer.save()

    #writer = ExcelWriter('dailyanalytics.xlsx')
    #underl_prc_df.to_excel(writer,sheet_name='Precios Suby.')
    #ivol.to_excel(writer,sheet_name='IVolatility')
    #subset_df[['modelImpliedVol','strike','load_dttm','right']].to_excel(writer,sheet_name='Calculo IV ATM')
    #sumario_subyacente.to_excel(writer,sheet_name='Sumario Suby.')
    #trade_summary.to_excel(writer,sheet_name='Sumario Trades')
    #positions_summary.to_excel(writer,sheet_name='Sumario Posiciones')
    #trade_summary_margin_cash.to_excel(writer,sheet_name='Sumario Margin Primas')
    #spread_summary.to_excel(writer,sheet_name='Sumario Spreads')
    #total_summary.to_frame(name='Conceptos').to_excel(writer,sheet_name='Sumario estrategia')
    #writer.save()

def get_ivol_series(date_ini,date_end):
    globalconf = config.GlobalConfig()
    store = globalconf.open_ivol_h5_db()
    lvl1 = store.get_node("/IVOL")
    df1 = store.select(lvl1._v_pathname)
    return df1.ix[date_ini:date_end]

def get_strategy_start_date(symbol,expiry,accountid,scenarioMode,simulName):
    globalconf = config.GlobalConfig()
    # 1.- Comprobar primero si hay ya registros en la ABT para la estrategia
    store_abt = globalconf.open_ib_abt_strategy_tic(scenarioMode)
    try:
        node1=symbol
        if scenarioMode == "Y":
            node1=simulName
        node_abt = store_abt.get_node("/" + node1)
        df1_abt = store_abt.select(node_abt._v_pathname,
                                   where=['subyacente==' + symbol, 'expiry==' + expiry, 'accountid==' + accountid])
        ret1=pd.to_datetime((df1_abt.loc[df1_abt.DTMaxdatost == np.max(df1_abt.DTMaxdatost)]['DTMaxdatost']).unique()[0])
        store_abt.close()
        # add one hour to run from the next hour
        ret1 += dt.timedelta(hours=1)
        return ret1
    except (IndexError, AttributeError) :
        print "There are no rows for the TIC strategy in the ABT H5"
        store_abt.close()

    # 2.- si la estrategia no existe en la ABT se toma la fecha de inicio la fecha de la primera operacion
    ret1 = ra.extrae_fecha_inicio_estrategia(symbol=symbol,expiry=expiry,accountid=accountid,
                                             scenarioMode=scenarioMode,simulName=simulName)
    return ret1


def run_analytics(symbol, expiry, secType,accountid,valuation_dt,scenarioMode,simulName,appendh5):
    """
        Run analytics main method.
    :param symbol:
    :param expiry:
    :param secType:
    :param accountid:
    :param valuation_dt:
    :param scenarioMode: If the valuation analytics is run against real portfolio data (paper or real account)
                        or against simulated orders & portfolio. In the later case the portfolio and orders are
                        read not from the H5 db but from customized excel templates
                        The real historical market data is always used in both cases though
    :return:
    """
    start = get_strategy_start_date(symbol, expiry,accountid,scenarioMode,simulName)
    print "start " , start
    end = valuation_dt
    delta = dt.timedelta(hours=1)
    d = start
    diff = 0
    weekend = set([5, 6])
    while d <= end:
        #print "date while " , d
        if ( d.hour in _rth ) & ( d.weekday() not in weekend ) :
            diff += 1
            run_shark_analytics(i_symbol=symbol, i_date=d,i_expiry=expiry,i_secType=secType,
                                accountid=accountid,scenarioMode=scenarioMode,simulName=simulName,appendh5=appendh5)
        d += delta


if __name__=="__main__":
    #i_secType OPT para SPY FOP para ES
    #run_shark_analytics(i_symbol='SPY',i_year=2016,i_month_num=9,i_day_t0=2,i_day_tminus1=1,i_expiry='20161021',i_secType='OPT')
    globalconf = config.GlobalConfig()
    accountid = globalconf.get_accountid()
    fecha_valoracion = dt.datetime(year=2016, month=9, day=20, hour=15, minute=59, second=59)
    #fecha_valoracion=dt.datetime.now()
    run_analytics(symbol="SPY", expiry="20161021", secType="OPT", accountid=accountid,
                  valuation_dt=fecha_valoracion,scenarioMode="Y",simulName="spy1016dls",appendh5=0)
    #run_analytics(symbol="ES", expiry="20161118", secType="FOP", accountid=accountid,
    #             valuation_dt=fecha_valoracion,scenarioMode="N",simulName="NA",appendh5=1)

