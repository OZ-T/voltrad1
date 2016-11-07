
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
from volsetup.logger import logger

# Regular trading hours in US (CEST time)
# TODO: need to consider change saving time CEST vs CET
_rth = (15,16,17,18,19,20,21)

def prev_weekday_close(adate):
    _offsets = (3, 1, 1, 1, 1, 1, 2)
    return ( adate - dt.timedelta(days=_offsets[adate.weekday()]) ).replace(hour=23,minute=59, second=59)

log = logger("Blablio TIC Analytics")

datos_toxls=pd.DataFrame()

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
def run_shark_analytics(i_symbol,i_date,i_expiry,i_secType,accountid,scenarioMode,simulName,appendh5,toxls):
    log.info(" ------------- Running for valuation date: [%s] ------------- " % (str(i_date)) )

    # fechas en que se calcula la foto de la estrategia
    fecha_valoracion=i_date
    # fecha que se utiliza para calcular retornos del subyacente y se convierte en hora mas cercana al cierre
    fecha_val_tminus1 = prev_weekday_close(fecha_valoracion)

    ###################################################################################################################
    # ORDERS
    # se obtienen todas operaciones desde el inicio de la estrategia hasta el datetime de valoracion
    ###################################################################################################################
    operaciones=ra.extrae_detalle_operaciones(valuation_dttm=fecha_valoracion,
                                              symbol=i_symbol,expiry=i_expiry,secType=i_secType,accountid=accountid,
                                              scenarioMode=scenarioMode, simulName=simulName)

    ###################################################################################################################
    # SACA FECHAS para las que hay TRADES Y JUNTA CON PORTFOLIO ACCOUNT Y PRICES DE OPTIONS CHAIN
    # se obtiene la lista de todas las datetimes en que se han realizado operaciones
    ###################################################################################################################
    temp_ts1=operaciones.reset_index().set_index('orders_times') #.tz_localize('Europe/Madrid')
    oper_series1=temp_ts1.index.unique().to_pydatetime()
    #lista_dttm_con_trades = set([x.date() for x in oper_series1])
    lista_dttm_con_trades = set([x.replace(minute=59, second=59) for x in oper_series1])
    #x = x.replace(minute=59, second=59)  # to ensure we han updated data in portfolio after the trade
    log.info("dttm operaciones: [%s] " % (str(lista_dttm_con_trades)) )

    ###################################################################################################################
    # PORTFOLIO en la fecha de valoracion y en las fechas de los trades
    # La foto del portfolio mas actual se saca para hacer cross check con el market value y p&l que se clcula de los precios
    # en la ejecucion con SCENARIO=N. Para la ejecucion de SCENARIO no se usa
    # posiciones actuales en las opciones en el symbol y el expiry
    ###################################################################################################################
    if scenarioMode != "Y":
        # base cse
        posiciones = ra.extrae_portfolio_positions(valuation_dttm=fecha_valoracion,
                                                   symbol=i_symbol, expiry=i_expiry, secType=i_secType,
                                                   accountid=accountid,
                                                   scenarioMode=scenarioMode, simulName=simulName)
    else:
        # scenario case
        x=max(lista_dttm_con_trades)
        log.info("Extraer posiciones para fecha trade: [%s] " % (str(x)))
        posiciones = ra.extrae_portfolio_positions(valuation_dttm=x,
                                                   symbol=i_symbol, expiry=i_expiry, secType=i_secType,
                                                   accountid=accountid,
                                                   scenarioMode=scenarioMode, simulName=simulName)
        posiciones['marketValue'] = 0 # no se usa
        posiciones['unrealizedPNL'] = 0  # no se usa
        posiciones['current_datetime'] = fecha_valoracion
        posiciones['load_dttm'] = fecha_valoracion



    posiciones_trades_dates = pd.DataFrame()
    for x in lista_dttm_con_trades:
        log.info("Extraer posiciones para fecha trade: [%s] " % (str(x)))
        temp_portfolio =ra.extrae_portfolio_positions(valuation_dttm=x ,
                                           symbol=i_symbol,expiry=i_expiry,secType=i_secType,
                                           accountid = accountid,
                                           scenarioMode = scenarioMode, simulName = simulName)
        posiciones_trades_dates=posiciones_trades_dates.append(temp_portfolio)

    ###################################################################################################################
    # OPTIONS CHAIN
    # precios de la cadena de opciones lo mas cerca posible de la fecha de valoracion
    # realmente saca todos los precios del dia anteriores a dicha fecha
    ###################################################################################################################
    cadena_opcs=ra.extrae_options_chain(valuation_dttm=fecha_valoracion,symbol=i_symbol,expiry=i_expiry,secType=i_secType)

    # cadena de opciones en el cierre del dia anterior a la fecha de valoracion
    # realmente saca todos los precios del dia anteriores a dicha fecha
    cadena_opcs_tminus1=ra.extrae_options_chain(valuation_dttm=fecha_val_tminus1, #cierre del dia anterior
                                                symbol=i_symbol,expiry=i_expiry,secType=i_secType)

    ###################################################################################################################
    # 1.- PRECIO DEL SUBYACENTE EN valuation datetime
    #
    # sacar el precio del subyacente mas probable (la moda del lastUndPrice por cada load_dttm)
    # para cada load_dttm debe haber pequeñas diferencias en el precio del subyacente
    # por el modo asincrono en que se recuperan los datos.
    ###################################################################################################################
    underl_prc_df = cadena_opcs[['prices_lastUndPrice','prices_load_dttm']].groupby(['prices_load_dttm']).agg(lambda x: stats.mode(x)[0][0])
    #print "underl_prc_df ",underl_prc_df
    # si el dataset esta vacio no se puede continuar en la analitica.
    if underl_prc_df.empty:
        print "No option data to analyze (t). Exiting ..."
        return
    # esto me da el ultimo precio disponible en la H5 del subyacente antes de la fecha de valoracion
    underl_prc = float(underl_prc_df.ix[np.max(underl_prc_df.index)]['prices_lastUndPrice'])
    hora_max=np.max(underl_prc_df.index)

    ###################################################################################################################
    ## Lo mismo pero para t-1 (ayer)
    ###################################################################################################################
    underl_prc_df_tminus1 = cadena_opcs_tminus1[['prices_lastUndPrice',
                                                 'prices_load_dttm']].groupby(['prices_load_dttm']).agg(lambda x: stats.mode(x)[0][0])
    if underl_prc_df_tminus1.empty:
        print "No option data to analyze (t-1). Exiting ..."
        return
    underl_prc_tminus1 = float(underl_prc_df_tminus1.ix[np.max(underl_prc_df_tminus1.index)]['prices_lastUndPrice'])
    hora_max_tminus1=np.max(underl_prc_df_tminus1.index)

    ###################################################################################################################
    # calcular el retorno del subyacente entre el cierre de ayer y lo que llevamos de hoy
    ###################################################################################################################
    try:
        retorno_suby_dia = np.log(float(underl_prc_df[underl_prc_df.index == hora_max]['prices_lastUndPrice'])
               / float(underl_prc_df_tminus1[underl_prc_df_tminus1.index == hora_max_tminus1]['prices_lastUndPrice'] ) )
    except ZeroDivisionError:
        retorno_suby_dia = np.nan

    ###################################################################################################################
    # FILTRO la ultima cadena de opciones disponible para el subyacente y vencimiento dados en el datetime de valoracion
    ###################################################################################################################
    cadena_opcs = cadena_opcs[cadena_opcs['prices_load_dttm'] == hora_max]
    #cadena_opcs.info()
    cadena_opcs.index = pd.RangeIndex(start=0, stop=len(cadena_opcs), step=1)
    cadena_opcs = cadena_opcs.dropna(subset=['prices_askDelta'])
    cadena_opcs=cadena_opcs.drop_duplicates(subset=['prices_strike','prices_symbol','prices_secType','prices_right',
                                                    'prices_load_dttm','prices_expiry','prices_current_date',
                                                    'prices_currency','prices_exchange'])

    ###################################################################################################################
    # CALCULA LA IV ATM
    # saca los strikes mas cerca de ATM de la cadena de opciones del paso anterior
    ###################################################################################################################
    subset_df=cadena_opcs.ix[(cadena_opcs['prices_strike'] - underl_prc).abs().argsort()[:4]]
    # cadena_opcs[(cadena_opcs['strike']-underl_prc).abs() < 0.5]
    # mejor usar modelImpliedVol que lastImpliedVol ???!!!
    #calcula la volimpl del subyacente como las medias de las vol impl de las opciones mas ATM
    impl_vol_suby = subset_df['prices_modelImpliedVol'].mean()

    # Enriquezco las posiciones desde el inicio con las quotes de la cadena de opciones IV, griegas precio del subyacente etc
    # en cada date de todas las operaciones que se han realizado desde el inicio de la estrategia
    cadena_opcs_orders= pd.DataFrame()
    for x in lista_dttm_con_trades:
        log.info("Extraer options chain para fecha trade: [%s] " % (str(x)))
        temporal1 =ra.extrae_options_chain(valuation_dttm=x,
                                           symbol=i_symbol,expiry=i_expiry,secType=i_secType)
        cadena_opcs_orders=cadena_opcs_orders.append(temporal1)

    cadena_opcs_orders[['prices_expiry']] = cadena_opcs_orders[['prices_expiry']].apply(pd.to_datetime)

    #cadena_opcs_orders_temp=cadena_opcs_orders_temp.reset_index().set_index(['times','strike','right','expiry','symbol'])
    #cadena_opcs_orders_temp.info()
    cadena_opcs_orders['on']=cadena_opcs_orders['prices_load_dttm'].dt.strftime("%Y-%m-%d %H")
    cadena_opcs_orders = cadena_opcs_orders.drop_duplicates(subset=['prices_expiry', 'prices_strike',
                                                                    'prices_symbol', 'prices_right', 'on'], keep='last')

    operaciones['on']=operaciones['orders_times'].dt.strftime("%Y-%m-%d %H")

    # devuelve en trade_summary la lista de operaciones historicas con las cotizaciones de la cadena de opciones incluida
    trade_summary = pd.merge(operaciones, cadena_opcs_orders, how='left',
                             left_on=['orders_expiry', 'orders_strike','orders_symbol','orders_right','on'],
                             right_on=['prices_expiry', 'prices_strike','prices_symbol','prices_right','on']
                             ) #.fillna(method='ffill')

    # Para cada datetime en que se han realizado operaciones (variable oper_series1) se extraen de cadena_opcs2 los precios mas adecuados
    # cruzar esta lista con las cotizacionon la IV
    trade_summary = trade_summary[['orders_localSymbol','orders_avgprice','orders_expiry','orders_current_datetime',
                                   'orders_symbol','orders_load_dttm','orders_multiplier','orders_price','orders_qty',
                                   'orders_shares','orders_right','orders_side','orders_strike','orders_times',
                                   'prices_lastUndPrice','prices_modelImpliedVol','prices_bidPrice','prices_askPrice',
                                   'prices_lastPrice','prices_current_datetime','prices_modelDelta','prices_modelGamma',
                                   'prices_modelTheta','prices_modelVega']]

    #caclulo dias a expiracion en el trade
    trade_summary['DTE'] = (trade_summary['orders_expiry'] - trade_summary['orders_times'] ).astype('timedelta64[D]').astype(int)
    ######################################################################################################################
    # Calculo IV ATM en cada trade
    ## hay que calcular aqui la IV de las opciones ATM en el momento del trade, se saca de la cadena de opciones
    cadena_opcs_orders.index = cadena_opcs_orders[['prices_strike','prices_load_dttm','prices_right']]
    # es necesario calcular esto aperturado por load_dttm (aqui se calcula un valor para todas las fechas de operaciones)
    # result2['ImplVolATM'] = cadena_opcs2.ix[(cadena_opcs2['strike'] - cadena_opcs2['lastUndPrice']).abs().argsort()[:16] ]['modelImpliedVol'].mean()
    temp1=cadena_opcs_orders.ix[(cadena_opcs_orders['prices_strike']
                                 - cadena_opcs_orders['prices_lastUndPrice']).abs().argsort()[:64] ][['prices_load_dttm',
                                                                                                      'prices_modelImpliedVol']]
    temp1=temp1.groupby(by=['prices_load_dttm'])['prices_load_dttm','prices_modelImpliedVol'].mean()
    temp1=temp1.rename(columns={'prices_modelImpliedVol':'ImplVolATM'})
    temp1=temp1.reset_index()
    temp1['on']=temp1['prices_load_dttm'].dt.strftime("%Y-%m-%d %H")

    trade_summary=trade_summary[['orders_avgprice',
                                 'orders_load_dttm',
                                 'orders_times',
                                 'orders_localSymbol',
                                 'prices_lastUndPrice',
                                 'orders_multiplier',
                                 'orders_qty',
                                 'orders_side',
                                 'DTE',
                                 'prices_modelGamma',
                                 'prices_modelTheta',
                                 'prices_modelVega',
                                 'orders_strike',
                                 'orders_shares']].drop_duplicates()

    trade_summary['on']=trade_summary['orders_times'].dt.strftime("%Y-%m-%d %H")
    trade_summary = pd.merge(trade_summary, temp1, how='left', on=['on'])

    # calcular 1SD con el horizonte de dias DTE
    trade_summary['1SD'] = trade_summary['prices_lastUndPrice'] * trade_summary['ImplVolATM'] / (365.0/((trade_summary['DTE']) ) )**0.5
    trade_summary['lastUndPrice_less1SD']=trade_summary['prices_lastUndPrice'] - trade_summary['1SD']
    trade_summary['lastUndPrice_plus1SD']=trade_summary['prices_lastUndPrice'] + trade_summary['1SD']
    trade_summary['DIT'] = (fecha_valoracion - trade_summary['orders_times'] ).astype('timedelta64[D]').astype(int) # days in trade
    trade_summary=trade_summary.rename(columns={'orders_load_dttm':'load_dttm'})
    trade_summary = trade_summary.drop('on', 1)
    trade_summary['sign'] = 0
    trade_summary.loc[trade_summary['orders_side'] == 'SLD', 'sign'] = -1
    trade_summary.loc[trade_summary['orders_side'] == 'BOT', 'sign'] = 1
    trade_summary['CreditoBruto'] = trade_summary['orders_avgprice'] * trade_summary['orders_multiplier'] \
                                    * trade_summary['orders_shares'] * trade_summary['sign']
    #esto es util para la fase de apertura
    trade_summary['1SD15D'] = trade_summary['prices_lastUndPrice'] * trade_summary['ImplVolATM'] / (365.0/((15.0) ) )**0.5
    trade_summary['1SD21D'] = trade_summary['prices_lastUndPrice'] * trade_summary['ImplVolATM'] / (365.0/((21.0) ) )**0.5
    trade_summary=trade_summary.sort_values(by=['orders_times', 'orders_localSymbol'] , ascending=[False, True])

    # calculo las primas brutas de comisiones de todas las operaciones hasta la fecha de valoracion
    operaciones['group'] = operaciones['orders_expiry'].dt.strftime("%Y%m%d")+operaciones['orders_strike'].map(str) \
                           + operaciones['orders_symbol'].map(str)+operaciones['orders_right'].map(str)
    #las primas brutas o precio bruto son todas las primas de todas las ordenes hasta la valuation dttm
    todate_precio_bruto=operaciones.groupby(['group'])['group',
                                                        'orders_precio_bruto'
                                                      ].agg({'orders_precio_bruto':np.sum}).reset_index()
    todate_precio_bruto.columns=['group','orders_precio_bruto']
    #todate_precio_bruto.columns=todate_precio_bruto.columns.droplevel()
    # en el caso de ser una simulacion no tengo necesariamente registros de posiciones en la fecha de valoracion
    # tampoco los necesito, dado que el MtM del protfolio lo derivo de las ordenes, comisiones y precios del mercado
    # en el caso de caso base (no escenario) si recupero el portfolio en valuation dttm principalmente para
    # validar y que cuadre rezonablemente.
    posiciones['group'] = posiciones['portfolio_expiry'].map(str)+posiciones['portfolio_strike'].map(str) \
                          +posiciones['portfolio_symbol'].map(str)+posiciones['portfolio_right'].map(str)
    posiciones = pd.merge(posiciones,todate_precio_bruto, how='left',left_on=['group'],right_on=['group'])

    #en real tengo el porfolio en la fecha de valoracion pero en simulacion lo que meto es unicamente el portfolio
    # en cada fecha de trade, necesito sacar el portfolio_averageCost en la ultima operacion antes de la fecha de
    # valoracion
    posiciones_trades_dates=posiciones_trades_dates.loc[posiciones_trades_dates.portfolio_load_dttm
                                                        == np.max(posiciones_trades_dates.portfolio_load_dttm)]
    posiciones_trades_dates['group'] = posiciones_trades_dates['portfolio_expiry'].map(str)\
                                       +posiciones_trades_dates['portfolio_strike'].map(str) \
                                       +posiciones_trades_dates['portfolio_symbol'].map(str)\
                                       +posiciones_trades_dates['portfolio_right'].map(str)
    #el precio neto son las primas netas de comisiones tal y como se contabiliza en el portfolio tras la ultima orden
    #  de la estrategia
    todate_precio_neto=posiciones_trades_dates.groupby(['group'])['group',
                                                        'portfolio_precio_neto'
                                                      ].agg({'portfolio_precio_neto':np.sum}).reset_index()
    todate_precio_neto.columns = ['group', 'portfolio_precio_neto']
    posiciones=posiciones.drop('portfolio_precio_neto',1)
    posiciones = pd.merge(posiciones, todate_precio_neto, how='left', left_on=['group'],
                                                            right_on=['group']).drop('group', 1)
    # juntar las posiciones con la cadena de opciones para tener las griegas, bid ask volumen etc
    # el datetime de los dos conjutnos de datos tiene siempre un lag por ese motivo el market value de pos
    # no coincide con el last price que vienen de cadena_opcs
    positions_summary = pd.merge(posiciones, cadena_opcs, how='left',
                                 left_on=['portfolio_expiry', 'portfolio_strike', 'portfolio_symbol', 'portfolio_right'],
                                 right_on=['prices_expiry', 'prices_strike', 'prices_symbol', 'prices_right'])

    #cross check marketValue from portfolio and that derived from market prices
    # el market value gross se calcula a partir de los precios de mercado de la cadena de opciones
    positions_summary['marketValueGross'] = positions_summary['portfolio_position'] \
                                             * positions_summary['portfolio_multiplier'] \
                                             * (positions_summary['prices_bidPrice'] \
                                                + positions_summary['prices_askPrice']) / 2.0

    positions_summary['orders_precio_bruto']= np.sign(positions_summary['portfolio_position']) \
                                               * positions_summary['orders_precio_bruto']

    positions_summary = positions_summary[['portfolio_averageCost',
                                           'portfolio_expiry',
                                           'portfolio_position',
                                           'portfolio_multiplier',
                                           'portfolio_strike',
                                           'portfolio_symbol',
                                           'portfolio_right',
                                           'portfolio_marketValue',
                                           'portfolio_unrealizedPNL',
                                           'portfolio_current_datetime',
                                           'portfolio_load_dttm',
                                           'marketValueGross',
                                           'orders_precio_bruto',
                                           'portfolio_precio_neto',
                                           'prices_lastUndPrice',
                                           'prices_modelImpliedVol',
                                           'prices_bidPrice',
                                           'prices_askPrice',
                                           'prices_modelDelta',
                                           'prices_modelGamma',
                                           'prices_modelTheta',
                                           'prices_modelVega'
                                           ]]

    # este es el precio de mercado neto de comisiones calculado y que debe ser parecido al
    # que se contabiliza a nivel de portfolio en el caso base (portfolio_marketValue)
    positions_summary['marketValuefromPrices'] =  positions_summary['marketValueGross'] \
                                                 + np.sign(positions_summary['portfolio_position']) \
                                                 * ( positions_summary['orders_precio_bruto'] \
                                                    - (positions_summary['portfolio_precio_neto']) )

    positions_summary['unrealizedPNLfromPrices'] = positions_summary['marketValuefromPrices'] \
                                                   - positions_summary['portfolio_precio_neto']

    #positions_summary['portfolio_expiry'] = positions_summary['portfolio_expiry'].apply(pd.to_datetime)
    positions_summary['portfolio_expiry'] = pd.to_datetime(positions_summary['portfolio_expiry'],format="%Y%m%d")

    #caclulo dias a expiracion en el trade
    positions_summary['DTE'] = (positions_summary['portfolio_expiry'] - fecha_valoracion ).astype('timedelta64[D]').astype(int)
    ## hay que calcular aqui la IV de las opciones ATM en el momento del trade, se saca de la cadena de opciones
    positions_summary['ImplVolATM'] = cadena_opcs.ix[(cadena_opcs['prices_strike'] - cadena_opcs['prices_lastUndPrice']).abs().argsort()[:4] ]['prices_modelImpliedVol'].mean()
    # calcular 1SD con el horizonte de dias DTE
    positions_summary['1SD'] = positions_summary['prices_lastUndPrice'] * positions_summary['ImplVolATM'] / (365.0/((positions_summary['DTE']) ) )**0.5
    positions_summary['lastUndPrice_less1SD']=positions_summary['prices_lastUndPrice'] - positions_summary['1SD']
    positions_summary['lastUndPrice_plus1SD']=positions_summary['prices_lastUndPrice'] + positions_summary['1SD']
    positions_summary['prices_midPrice']= ( positions_summary['prices_bidPrice'] + positions_summary['prices_askPrice'] ) / 2
    positions_summary['ValCurrent'] = positions_summary['prices_midPrice'] * np.sign(positions_summary['portfolio_position'])
    positions_summary['costUnit'] = positions_summary['portfolio_averageCost'] * np.sign(positions_summary['portfolio_position']) / positions_summary['portfolio_multiplier']
    positions_summary['Dshort'] = positions_summary['prices_modelDelta'] * positions_summary['portfolio_multiplier']
    positions_summary['DshortPosition'] = positions_summary['prices_modelDelta'] * positions_summary['portfolio_multiplier'] * positions_summary['portfolio_position']
    positions_summary['GammaPosition'] = positions_summary['prices_modelGamma'] * positions_summary['portfolio_multiplier'] * positions_summary['portfolio_position']
    positions_summary['ThetaPosition'] = positions_summary['prices_modelTheta'] * positions_summary['portfolio_multiplier'] * positions_summary['portfolio_position']
    positions_summary['VegaPosition'] = positions_summary['prices_modelVega'] * positions_summary['portfolio_multiplier'] * positions_summary['portfolio_position']
    positions_summary=positions_summary.sort_values(by=['portfolio_right','portfolio_symbol','portfolio_expiry',
                                                        'portfolio_strike'] , ascending=[False, True, True, True])

    # agregar por spreads
    spread_summary = positions_summary.groupby(by=['portfolio_right'])['ValCurrent','costUnit','marketValuefromPrices','DshortPosition'].sum()
    # agregar total
    total_summary = positions_summary[['DshortPosition','GammaPosition','ThetaPosition','VegaPosition','marketValuefromPrices']].sum()
    total_summary_risk = positions_summary[['prices_lastUndPrice','ImplVolATM','portfolio_multiplier']].mean()
    total_summary_risk['Pts1SD1D'] = total_summary_risk['prices_lastUndPrice'] * total_summary_risk['ImplVolATM'] / np.sqrt(365.0)
    total_summary_risk['Pts1SD5D'] = total_summary_risk['prices_lastUndPrice'] * total_summary_risk['ImplVolATM'] / np.sqrt(365.0/5.0)
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
    for x in lista_dttm_con_trades:
        #temporal1 =ra.extrae_account_delta_new(year=x.year, month=x.strftime("%B")[:3],day=x.day,hour=x.hour,minute=x.minute)
        t_margin,t_prem =ra.extrae_account_delta_new(valuation_dttm=x,accountid=accountid,
                                               scenarioMode = scenarioMode,simulName=simulName)
        temp_margin = temp_margin.append(t_margin)
        # cash primas y comisiones
        # impacto en comisiones de las operaciones
        # dado un datetime donde se han ejecutado operaciones hace la delta de las posiciones cash de la cuenta y eso
        # resulta en el valor de las comisiones de ese conjunto de operaciones (hay que considerar el dienro recibido de)
        # la venta de opciones y tambien si se tiene posiciones en divisa los cambios ????
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
    temp_margin=temp_margin.rename(columns={'FullInitMarginReq_USD':'FullInitMarginReq_USD_actual'})
    temp_premium=temp_premium.rename(columns={'TotalCashValue_USD':'Impacto_encash_ultimo_periodo1h'}).dropna()

    #temp_margin['on']=pd.to_datetime(temp_margin['load_dttm'], format="%Y-%m-%d %H:%M:%S").dt.strftime("%Y-%m-%d %H")
    #temp_premium['on']=pd.to_datetime(temp_premium['load_dttm'], format="%Y-%m-%d %H:%M:%S").dt.strftime("%Y-%m-%d %H")
    temp_margin['on']=temp_margin['load_dttm'].dt.strftime("%Y-%m-%d %H")
    temp_premium['on']=temp_premium['load_dttm'].dt.strftime("%Y-%m-%d %H")

    temp_credbruto=trade_summary.reset_index()
    temp_credbruto['on']=temp_credbruto['orders_times'].dt.strftime("%Y-%m-%d %H")
    temp_credbruto=temp_credbruto.groupby(by=['on'])['on','CreditoBruto'].sum().reset_index()

    trade_summary_margin_cash = pd.merge(temp_margin, temp_premium, how='left', on=['on'])
    trade_summary_margin_cash = pd.merge(trade_summary_margin_cash, temp_credbruto, how='left', on=['on'])
    trade_summary_margin_cash = trade_summary_margin_cash.drop('on', 1)
    trade_summary_margin_cash = trade_summary_margin_cash.rename(columns={'load_dttm_x': 'load_dttm'})
    trade_summary_margin_cash = trade_summary_margin_cash.drop('load_dttm_y', 1)
    trade_summary_margin_cash['Comisiones'] = np.abs(trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h']
                                                     + trade_summary_margin_cash['CreditoBruto'] )
    trade_summary_margin_cash['Margen_neto'] = 0
    trade_summary_margin_cash.loc[trade_summary_margin_cash['FullInitMarginReq_USD_actual'] <= 0,
                                                            'Margen_neto'] = 0
    trade_summary_margin_cash.loc[trade_summary_margin_cash['FullInitMarginReq_USD_actual'] > 0,
                                'Margen_neto'] = trade_summary_margin_cash['FullInitMarginReq_USD_actual'] \
                                                 - trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h']

    trade_summary_margin_cash['load_dttm']=pd.to_datetime(trade_summary_margin_cash['load_dttm'],
                                                          format="%Y-%m-%d %H:%M:%S")

    trade_summary_margin_cash['BeneficioMaximoPct'] = 0
    trade_summary_margin_cash.loc[trade_summary_margin_cash['FullInitMarginReq_USD_actual'] > 0,
                                  'BeneficioMaximoPct'] = trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h'] \
                                                          / trade_summary_margin_cash['Margen_neto']
    trade_summary_margin_cash=trade_summary_margin_cash.sort_values(by='load_dttm')
    #print "trade_summary_margin_cash " , trade_summary_margin_cash
    # calcular apalancamientos de la posicion completa
    # a continuacion es el margen neto que quedo tras la ultima operacion en la estrategia completa
    cb_tot = trade_summary_margin_cash [(trade_summary_margin_cash['load_dttm']) ==
                                        trade_summary_margin_cash.reset_index().max()['load_dttm'] ]  ['Margen_neto']

    total_summary['AD1PCT'] =np.abs( total_summary['portfolio_multiplier'] * ( ( (np.abs(total_summary['DshortPosition'])
                                                                        + total_summary['GammaPosition'] )
                                                                        * total_summary['prices_lastUndPrice']
                                                                        / total_summary['portfolio_multiplier'] )
                                                                        - total_summary['GammaPosition'] )
                                                                        / cb_tot ).values[0]

    total_summary['AD1SD1D'] =np.abs( total_summary['portfolio_multiplier'] * ( ( (np.abs(total_summary['DshortPosition']) + total_summary['GammaPosition'] )
                                                                       * total_summary['prices_lastUndPrice'] * total_summary['ImplVolATM'] / np.sqrt(365.0) )
                                                                     - total_summary['GammaPosition'] ) / cb_tot ).values[0]

    total_summary['MaxDsAD1PCT'] = np.abs ( 3.0 * total_summary['DshortPosition'] / total_summary['AD1PCT'] )
    total_summary['MaxDsAD1SD1D'] = np.abs ( 3.0 * total_summary['DshortPosition'] / total_summary['AD1SD1D'] )
    total_summary['MaxDs'] = np.abs ( 0.04 * cb_tot / total_summary['Pts1SD1D'] ).values[0]
    total_summary['thetaDeltaRatio'] = np.abs ( total_summary['ThetaPosition'] / total_summary['DshortPosition'] )
    total_summary['thetaGammaRatio'] = np.abs ( total_summary['ThetaPosition'] / total_summary['GammaPosition'] )
    total_summary['VegaThetaRatio'] = np.abs ( total_summary['VegaPosition'] / total_summary['ThetaPosition'] )

    #ivol=get_ivol_series(date_ini=fecha_ini.strftime("%Y-%m-%d"),date_end=fecha_valoracion.date().strftime("%Y-%m-%d"))
    #col=['IV_Index_call_52wk_hi','IV_Index_call_52wk_lo','IV_Index_call_52wk_hi','IV_Index_mean_52wk_hi',
    #     'IV_Index_mean_52wk_lo','IV_Index_put_52wk_hi','IV_Index_put_52wk_lo']
    #ivol.drop(col,axis=1,inplace=True)

    precio_undl_inicio_strat = \
        (trade_summary.loc[trade_summary.orders_times == np.min(trade_summary.orders_times)]['prices_lastUndPrice']).unique()[0]
    iv_atm_inicio_strat = \
        (trade_summary.loc[trade_summary.orders_times == np.min(trade_summary.orders_times)]['ImplVolATM']).unique()[0]

    dte_ini = \
        (trade_summary.loc[trade_summary.orders_times == np.min(trade_summary.orders_times)]['DTE']).unique()[0]

    ini_1SD15D = \
        (trade_summary.loc[trade_summary.orders_times == np.max(trade_summary.orders_times)]['1SD15D']).unique()[0].item()

    ini_1SD21D = \
        (trade_summary.loc[trade_summary.orders_times == np.max(trade_summary.orders_times)]['1SD21D']).unique()[0].item()

    prc_ajuste_1SD21D_up = precio_undl_inicio_strat + ini_1SD21D
    prc_ajuste_1SD21D_dn = precio_undl_inicio_strat - ini_1SD21D

    prc_ajuste_1SD15D_up = precio_undl_inicio_strat + ini_1SD15D
    prc_ajuste_1SD15D_dn = precio_undl_inicio_strat - ini_1SD15D

    dte= positions_summary[['DTE']].mean().item()

    #margen_neto= (trade_summary_margin_cash - trade_summary_margin_cash.shift())['FullInitMarginReq_USD_actual'].dropna().item()
    margen_neto = (trade_summary_margin_cash)['FullInitMarginReq_USD_actual'].dropna().sum()
    comisiones=trade_summary_margin_cash['Comisiones'].dropna().sum()

    impacto_cash = trade_summary_margin_cash['Impacto_encash_ultimo_periodo1h'].dropna().sum()

    BearCallMaxDShortJL = positions_summary.loc[(positions_summary.portfolio_right=="C")
                                                  & (positions_summary.portfolio_position < 0),'Dshort'].max()
    BullPutMaxDShortJL = positions_summary.loc[(positions_summary.portfolio_right=="P")
                                                  & (positions_summary.portfolio_position < 0),'Dshort'].min() # because delta is <0 for puts
    BearCallMaxIVShort = positions_summary.loc[(positions_summary.portfolio_right=="C")
                                                  & (positions_summary.portfolio_position < 0),'prices_modelImpliedVol'].max()
    BullPutMaxIVShort = positions_summary.loc[(positions_summary.portfolio_right=="P")
                                                  & (positions_summary.portfolio_position < 0),'prices_modelImpliedVol'].max()
    BearCallMaxDeltaShort = positions_summary.loc[(positions_summary.portfolio_right=="C")
                                                  & (positions_summary.portfolio_position < 0),'prices_modelDelta'].max()
    BullPutMaxDeltaShort = positions_summary.loc[(positions_summary.portfolio_right=="P")
                                                  & (positions_summary.portfolio_position < 0),'prices_modelDelta'].min() # because delta is <0 for puts
    BearCallMaxDeltaShortPos = positions_summary.loc[(positions_summary.portfolio_right=="C")
                                                  & (positions_summary.portfolio_position < 0),'DshortPosition'].max()
    BullPutMaxDeltaShortPos = positions_summary.loc[(positions_summary.portfolio_right=="P")
                                                  & (positions_summary.portfolio_position < 0),'DshortPosition'].min() # because delta is <0 for puts
    # this is only for the base case
    p_n_l = positions_summary[['portfolio_marketValue']].sum().item() -trade_summary[['CreditoBruto']].sum().item()
    coste_base = trade_summary[['CreditoBruto']].sum().item()

    #marketValueGross = positions_summary[['marketValueGross']].sum().item()
    #portfolio_precio_neto = positions_summary[['portfolio_precio_neto']].sum().item()
    #orders_precio_bruto = positions_summary[['orders_precio_bruto']].sum().item()
    #portfolio_marketValue = positions_summary[['portfolio_marketValue']].sum().item()
    #portfolio_unrealizedPNL = positions_summary[['portfolio_unrealizedPNL']].sum().item()
    #marketValuefromPrices = positions_summary[['marketValuefromPrices']].sum().item()
    #unrealizedPNLfromPrices = positions_summary[['unrealizedPNLfromPrices']].sum().item()
    marketValueGross = positions_summary['marketValueGross'].sum()
    portfolio_precio_neto = positions_summary['portfolio_precio_neto'].sum()
    orders_precio_bruto = positions_summary['orders_precio_bruto'].sum()
    portfolio_marketValue = positions_summary['portfolio_marketValue'].sum()
    portfolio_unrealizedPNL = positions_summary['portfolio_unrealizedPNL'].sum()
    marketValuefromPrices = positions_summary['marketValuefromPrices'].sum()
    unrealizedPNLfromPrices = positions_summary['unrealizedPNLfromPrices'].sum()

    #sumario_subyacente
    max_profit = abs(coste_base)/margen_neto
    pnl_margin_ratio = unrealizedPNLfromPrices / margen_neto
    puntos_desde_last_close = (underl_prc-underl_prc_tminus1)
    linea_mercado = (1-( (pnl_margin_ratio) / (max_profit) ))
    e_v = (max_profit)-(pnl_margin_ratio)
    dit = dte_ini - dte
    row_datos = pd.DataFrame({'subyacente':i_symbol,
                              'expiry':i_expiry,
                              'accountid':accountid,
                              'portfolio_unrealizedPNL': portfolio_unrealizedPNL,
                              'marketValuefromPrices': marketValuefromPrices,
                              'marketValueGross':marketValueGross,
                              'portfolio_precio_neto':portfolio_precio_neto,
                              'orders_precio_bruto':orders_precio_bruto,
                              'portfolio_marketValue':portfolio_marketValue,
                              'unrealizedPNLfromPrices':unrealizedPNLfromPrices,
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
                              'portfolio':
                              positions_summary[['portfolio_strike',
                                                 'portfolio_right',
                                                 'prices_bidPrice',
                                                 'prices_askPrice']].rename(columns={'portfolio_strike': 'Str',
                                                                                     'portfolio_right': 'T',
                                                                                     'prices_bidPrice': 'Bid',
                                                                                     'prices_askPrice': 'Ask'})
                                                                    .reset_index().drop('index',axis=1)
                                                                    .to_json(orient='records'),
                              'retorno_subyacente': retorno_suby_dia,
                              'lastUndPrice':total_summary['prices_lastUndPrice'],
                              'DshortPosition':total_summary['DshortPosition'],
                              'GammaPosition':total_summary['GammaPosition'],
                              'ThetaPosition':total_summary['ThetaPosition'],
                              'VegaPosition':total_summary['VegaPosition'],
                              'lastUndPrice':total_summary['prices_lastUndPrice'],
                              'ImplVolATM':total_summary['ImplVolATM'],
                              'multiplier':total_summary['portfolio_multiplier'],
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
                              'DToperaciones':str(lista_dttm_con_trades),
                              'MargenNeto': margen_neto
                              },index=[hora_max.strftime("%Y-%m-%d %H:%M:%S")])
    if toxls ==1:
        global datos_toxls
        datos_toxls=datos_toxls.append(row_datos)


    if appendh5 != 1:
        return
    globalconf = config.GlobalConfig()
    store = globalconf.open_ib_abt_strategy_tic(scenarioMode=scenarioMode)
    if scenarioMode == "N":
        loc1 = i_symbol
    elif scenarioMode == "Y":
        loc1 = simulName
    store.append("/" + loc1, row_datos, data_columns=True,
                 min_itemsize={'portfolio': 500,
                               'DToperaciones': 500})
    store.close()

def get_ivol_series(date_ini,date_end):
    globalconf = config.GlobalConfig()
    store = globalconf.open_ivol_h5_db()
    lvl1 = store.get_node("/IVOL")
    df1 = store.select(lvl1._v_pathname)
    return df1.ix[date_ini:date_end]

def get_strategy_start_date(symbol,expiry,accountid,scenarioMode,simulName,timedelta1):
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
        ret1 += dt.timedelta(hours=timedelta1)
        return ret1
    except (IndexError, AttributeError) :
        log.info("There are no rows for the TIC strategy in the ABT H5")
        store_abt.close()

    # 2.- si la estrategia no existe en la ABT se toma la fecha de inicio la fecha de la primera operacion
    ret1 = ra.extrae_fecha_inicio_estrategia(symbol=symbol,expiry=expiry,accountid=accountid,
                                             scenarioMode=scenarioMode,simulName=simulName)
    ret1=ret1.replace(minute=59, second=59) # el datetime de valoracion siemrpe ha ser el ultimo minuto para asegurar coger todos los trades
    return ret1


def run_analytics(symbol, expiry, secType,accountid,valuation_dt,scenarioMode,simulName,appendh5,toxls,timedelta1):
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
    start = get_strategy_start_date(symbol, expiry,accountid,scenarioMode,simulName,timedelta1)
    log.info("Starting date to use: [%s] " % (str(start)) )
    expiry_dt = dt.datetime.strptime(expiry, '%Y%m%d')
    end = min(valuation_dt,expiry_dt)
    delta = dt.timedelta(hours=timedelta1)
    d = start
    diff = 0
    weekend = set([5, 6])
    while d <= end:
        #print "date while " , d
        if ( d.hour in _rth ) & ( d.weekday() not in weekend ) :
            diff += 1
            run_shark_analytics(i_symbol=symbol, i_date=d,i_expiry=expiry,i_secType=secType,
                                accountid=accountid,scenarioMode=scenarioMode,simulName=simulName,appendh5=appendh5,toxls=toxls)
        d += delta

    if toxls == 1:
        datos_toxls.to_excel(symbol+"_"+valuation_dt.strftime("%Y%m%d_%H")+"_scn"+scenarioMode+".xlsx");


if __name__=="__main__":
    #i_secType OPT para SPY FOP para ES
    #run_shark_analytics(i_symbol='SPY',i_year=2016,i_month_num=9,i_day_t0=2,i_day_tminus1=1,i_expiry='20161021',i_secType='OPT')
    globalconf = config.GlobalConfig()
    accountid = globalconf.get_accountid()
    #fecha_valoracion = dt.datetime(year=2016, month=10, day=17, hour=21, minute=59, second=59)
    fecha_valoracion=dt.datetime.now()
    run_analytics(symbol="SPY", expiry="20161021", secType="OPT", accountid=accountid,
                  valuation_dt=fecha_valoracion,scenarioMode="Y",simulName="spy1016wild",appendh5=1,toxls=0,timedelta1=24)
    #run_analytics(symbol="ES", expiry="20161118", secType="FOP", accountid=accountid,
    #             valuation_dt=fecha_valoracion,scenarioMode="N",simulName="NA",appendh5=1)

