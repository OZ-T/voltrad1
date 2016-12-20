# -*- coding: UTF-8 -*-

from volsetup import config
from datetime import datetime, timedelta
from pytz import timezone
import run_analytics as ra
import time
import pandas as pd
import numpy as np
import pandas_datareader.data as web
from opt_pricing_methods import bsm_mcs_euro
import sys
from volsetup.logger import logger
import matplotlib.pyplot as plt
import statsmodels.api as sm
from scipy.stats import ttest_ind

globalconf = config.GlobalConfig()
log = logger("Gekko index module")

PCT_DOWN_CHAIN = 0.08
PCT_UP_CHAIN = 0.06
DAY_BEFORE_EXPIRY_ROLL = 10

def read_h5_source_data(start1,end1):
    df1=ra.read_biz_calendar(start_dttm=start1,valuation_dttm=end1)
    # sacar del historical options chain P y C ATM y OTM volumenes y precios bid ask
    df2=ra.extrae_options_chain2(start_dttm=start1, end_dttm=end1, symbol="ES", expiry="", secType="FOP")
    return df1,df2

def keep_some_strikes(ds,pct):
    return ds.ix[(ds.strike - (1 + pct) * ds.lastUndPrice).abs().argsort()[:1]]

def keep_some_strikes2(ds,pct,right1):
    ds=ds.ix[(ds.strike - (1 + pct) * ds.lastUndPrice).abs().argsort()[:1]]
    return ds.ix[(ds.expiry == min(ds.expiry)) & (ds.right == right1)]

# habría que agrupar por la fecha del indice para quedarse sólamente con la cotización de la expiración que cumpla esas condiciones
def keep_closer_expiry(ds,right1):
    # cuando se acerca la expiracion hay que pasar al expiry siguiente con algunos dias de ANTELACION
    # si no tenemos varias expiraiones en los datos no aplico el primer filtro porque me quedaria con cero si quedan menos de 10 días para vencimiento
    if len(ds) > 2:
        ds = ds.ix[(ds.index + timedelta(days=DAY_BEFORE_EXPIRY_ROLL) <= ds.expiry.apply(lambda x: datetime.strptime(x, '%Y%m%d'))) ]
    ds = ds.ix[(ds.expiry == min(ds.expiry)) & (ds.right == right1)]
    return ds


def extract_abt(events,optchain,start1,end1):
    df1=events
    df2=optchain
    # el precio mas probable en cada datetime del subyacente
    # Meter como eventos el tercer friday de cada mes - 10 dias para que el salto de psar de una expiracion a otra
    # no se identifique como un evento tradeable
    rng = pd.date_range(start1, end1, freq='WOM-3FRI', normalize=True) - timedelta(days=DAY_BEFORE_EXPIRY_ROLL)
    df_optroll = pd.DataFrame({'Statistic': 'OptionRollover'}, index=rng)
    df1 = df1.append(df_optroll).sort_index(inplace=False, ascending=[True])
    und_prc = df2[['lastUndPrice', ]].groupby(df2.index, as_index=True).apply(lambda group: group.mean())
    df2 = df2[['askImpliedVol', 'bidImpliedVol', 'expiry', 'modelImpliedVol', 'right', 'strike',
               'symbol', 'askOptPrice', 'askSize', 'bidOptPrice', 'bidSize']]
    full_df = pd.merge(df2, und_prc, how='left', left_index=True, right_index=True)
    # filtrar los strikes ATM C y P , el P 8% por debajo y el C 6% por encima
    atm_df = full_df.groupby([full_df.index, 'right', 'expiry'],
                             as_index=False).apply(lambda group: keep_some_strikes(group, 0.0)).reset_index().drop(
        'level_0', 1)
    atm_df = atm_df.rename(columns={'level_1': 'index'})
    atm_df = atm_df[['index', 'askImpliedVol', 'bidImpliedVol', 'modelImpliedVol', 'lastUndPrice', 'expiry'
        , 'right', 'askOptPrice', 'askSize', 'bidOptPrice', 'bidSize']].set_index('index')
    # atm_df.to_sql(name='atm_df',con=con, if_exists = 'replace')
    atm_df = atm_df.groupby(atm_df.index, as_index=False).apply(lambda group: keep_closer_expiry(group,
                                                                                                 right1='C')).reset_index().drop(
        ['level_0', 'expiry', 'right'], 1)
    atm_df = atm_df.rename(columns={'level_1': 'index'}).set_index('index').add_prefix('atm_')
    # filtrar solo los puts
    otm_put_df = full_df.groupby([full_df.index, 'right', 'expiry'],
                                 as_index=False).apply(
        lambda group: keep_some_strikes(group, -PCT_DOWN_CHAIN)).reset_index().drop('level_0', 1)
    otm_put_df = otm_put_df.rename(columns={'level_1': 'index'})
    otm_put_df = otm_put_df[['index', 'askImpliedVol', 'bidImpliedVol', 'modelImpliedVol', 'expiry'
        , 'right', 'askOptPrice', 'askSize', 'bidOptPrice', 'bidSize']].set_index('index')
    otm_put_df = otm_put_df.groupby(otm_put_df.index, as_index=False).apply(lambda group: keep_closer_expiry(group,
                                                                                                             right1='P')).reset_index().drop(
        ['level_0', 'expiry', 'right'], 1)
    otm_put_df = otm_put_df.rename(columns={'level_1': 'index'}).set_index('index').add_prefix('otm_put_')
    # filtrar solo los calls
    otm_call_df = full_df.groupby([full_df.index, 'right', 'expiry'],
                                  as_index=False).apply(
        lambda group: keep_some_strikes2(group, +PCT_UP_CHAIN, 'C')).reset_index().drop('level_0', 1)
    otm_call_df = otm_call_df.rename(columns={'level_1': 'index'})
    otm_call_df = otm_call_df[['index', 'askImpliedVol', 'bidImpliedVol', 'modelImpliedVol', 'expiry'
        , 'right', 'askOptPrice', 'askSize', 'bidOptPrice', 'bidSize']].set_index('index')
    otm_call_df = otm_call_df.groupby(otm_call_df.index, as_index=False).apply(lambda group: keep_closer_expiry(group,
                                                                                                                right1='C')).reset_index().drop(
        ['level_0', 'expiry', 'right'], 1)
    otm_call_df = otm_call_df.rename(columns={'level_1': 'index'}).set_index('index').add_prefix('otm_call_')
    # juntarlos y poner los datos en columnas
    final_df = pd.merge(atm_df, otm_put_df, how='left', left_index=True, right_index=True)
    final_df = pd.merge(final_df, otm_call_df, how='left', left_index=True, right_index=True)
    # juntar con el df de eventos
    # print final_df.columns
    # convertir a serie temporal (equiespaciada en t) frecuencia horaria
    out2 = final_df.resample('H', label='right').mean().dropna()
    # out2.to_excel("out2.xlsx")
    # create indicator variables for statistics

    # normalizar los eventos porque algunos se difenrencian en espacios, ademas juntar en uno los que son tienen sufijo
    #  Prel de preliminary, Rev de Revision, etc. y se llaman igual
    df1.Statistic=df1.Statistic.apply(lambda x: ''.join(x.split()).lower())

    # ''.join(s.split()).lower()
    # Ejemplo de fuzzy matching names:
    #df2.index = df2.index.map(lambda x: difflib.get_close_matches(x, df1.index)[0])

    out1 = pd.get_dummies(df1.Statistic).resample('H', label='right').sum().dropna()
    # out1.to_excel("out1.xlsx")
    # juntar con el dataframe de los eventos join
    # final_df=out1.join(out2, how='outer')
    # final_df=out1.merge(out2, how='outer', left_index=True, right_index=True)
    final_df = pd.merge(out1, out2, how='outer', left_index=True, right_index=True)

    # hacer ffil de las cotizaciones de opciones
    res1 = [k for k in final_df.columns if 'atm_' in k]
    res2 = [k for k in final_df.columns if 'otm_' in k]
    final_df[res1+res2] = final_df[res1+res2].fillna(method="bfill")


    # sacar el retorno de todos los precios IV y sizes
    # sacar diferencias con el peridodo anterior en cada fecha
    final_df[res1 + res2]= final_df[res1+res2].pct_change()

    # TODO: probar con varios periodos para la diferencia (para ajustar el impacto del evento y medirlo)
    ##

    # imputar los missing a cero de los eventos
    final_df = final_df.fillna(value=0)
    final_df = final_df.sort_index(inplace=False, ascending=[True])
    return final_df

def incremental_feed_abt():
    """"
    feed SQL ABT incrementally from source H5 files
    """
    end1 = datetime.now()
    start1 = datetime(year=2016, month=7, day=21, hour=15, minute=59, second=59)

    # TODO: recuperar el start1 del ultimo registro disponible en el SQL ABT
    #end1 = datetime(year=2016, month=8, day=9, hour=15, minute=59, second=59)
    df1, df2 = read_h5_source_data(start1=start1,end1=end1)
    log.info("connecting to sql db... ")
    con , meta = globalconf.connect_sqldb()

    final_df = extract_abt(events=df1,optchain=df2,start1=start1,end1=end1)
    final_df.to_sql(name='gekko_data', con=con, if_exists='replace', chunksize=50, index=True)

def ols_analysis_abt():
    # cross tab con estadísticas de retornos por cada estadístico
    # modelo de regresion con las variables dummy y como el target el retorno del ES respecto al periodo anterio

    con, meta = globalconf.connect_sqldb()
    df=pd.read_sql_table('gekko_data',con=con)
    xdat=df['optionrollover']
    ydat=df['atm_modelImpliedVol']
    model = sm.OLS(ydat, xdat)
    res = model.fit()
    print(res.summary())

    plt.plot(xdat,ydat,'r.')
    ax = plt.axis()

def ttest_mean_stat_signif():
    #  http://docs.scipy.org/doc/scipy/reference/stats.html
    con, meta = globalconf.connect_sqldb()
    df=pd.read_sql_table('gekko_data',con=con)

    cat1 = df[df['durableorders'] == 1]
    cat2 = df[df['durableorders'] == 0]
    ttest = ttest_ind(cat1['atm_modelImpliedVol'], cat2['atm_modelImpliedVol'])

    # TODO: hacer el ttest por cada variable pero elimiar antes los registros con optionrollover == 1
    # y los registros que son de apertura del mercado (hora 16:00)
    #hacer el t-test en un bucle para todas las variables que no sean atm_ y otm_
    # EJEMPLO Ttest_indResult(statistic=0.096742317171181577, pvalue=0.92295547692306734)
    # la variable objetivo que sean las que son otm_ y/o atm_ (bucle anidado
    # para aquellas que el p-valor salga significativo calcular la media y eso es la "predicción" de la modifición
    # del movimiento del subyacente, del movimiento de la IV , del movimiento del precio de las opciones OTM, etc.

    print ttest

if __name__ == "__main__":
    #incremental_feed_abt()
    #ols_analysis_abt()
    ttest_mean_stat_signif()



# agregar por tipo de evento

# clasificar los eventos entre si excedieron o decepcionaron las expectativas de analistas mercado

#
# almacenar este ranking en un h5

# sacar el listado de eventos de los proximos 7 dias del h5 de biz yahoo calendar

# en funcion de los eventos de la semana que viene (por cada uno de los 7 proximos dias)
# dar un forecast en base a los eventos anunciados y el ranking de eventos calculado1
# la prediccción se basa tambien en los aciertos /errores que se alamancenan en el h5 de abajo que guarda los forecasts


# alamcenar este forecast en otro h5 en data que es lo que consulta desde el API json de flask
# la predicccion se basa tambien en los aciertos /errores que se alamancenan aqui


# del api de json se obtiene el forecast para cada dia de la semana en curso
# el martes por ejemplo se compara el forecast del modelo con la realidad (se muestra si el modelo
# acerto o no al predecir el impacto de los eventos

