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

globalconf = config.GlobalConfig()
log = logger("Gekko index module")

PCT_DOWN_CHAIN = 0.08
PCT_UP_CHAIN = 0.06

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
        ds = ds.ix[(ds.index + timedelta(days=10) <= ds.expiry.apply(lambda x: datetime.strptime(x, '%Y%m%d'))) ]
    ds = ds.ix[(ds.expiry == min(ds.expiry)) & (ds.right == right1)]
    return ds

if __name__ == "__main__":
    now = datetime.now()
    start1 = datetime(year=2016, month=1, day=1, hour=21, minute=0, second=0)
    end1 = datetime(year=2016, month=8, day=9, hour=15, minute=59, second=59)
    df1, df2 = read_h5_source_data(start1=start1,end1=now)
    con , meta = globalconf.connect_sqldb()

    # el precio mas probable en cada datetime del subyacente
    und_prc = df2[['lastUndPrice', ]].groupby(df2.index, as_index=True).apply(lambda group: group.mean())

    df2 = df2[['askImpliedVol', 'bidImpliedVol', 'expiry', 'modelImpliedVol', 'right', 'strike',
               'symbol','askOptPrice','askSize','bidOptPrice','bidSize']]
    full_df = pd.merge(df2, und_prc, how='left', left_index=True,right_index=True)

    # filtrar los strikes ATM C y P , el P 8% por debajo y el C 6% por encima
    atm_df=full_df.groupby( [full_df.index,'right','expiry'],
                            as_index=False).apply(lambda group: keep_some_strikes(group,0.0)).reset_index().drop('level_0',1)

    atm_df = atm_df.rename(columns={'level_1': 'index'})
    atm_df=atm_df[['index','askImpliedVol','bidImpliedVol','modelImpliedVol','lastUndPrice','expiry'
        ,'right','askOptPrice','askSize','bidOptPrice','bidSize']].set_index('index')

    #atm_df.to_sql(name='atm_df',con=con, if_exists = 'replace')

    atm_df = atm_df.groupby(atm_df.index,as_index=False).apply(lambda group: keep_closer_expiry(group,
                                                    right1='C')).reset_index().drop(['level_0','expiry','right'],1)
    atm_df = atm_df.rename(columns={'level_1': 'index'}).set_index('index').add_prefix('atm_')
    # filtrar solo los puts
    otm_put_df=full_df.groupby( [full_df.index,'right','expiry'],
                            as_index=False).apply(lambda group: keep_some_strikes(group,-PCT_DOWN_CHAIN)).reset_index().drop('level_0',1)

    otm_put_df = otm_put_df.rename(columns={'level_1': 'index'})
    otm_put_df = otm_put_df[['index','askImpliedVol','bidImpliedVol','modelImpliedVol','expiry'
        ,'right','askOptPrice','askSize','bidOptPrice','bidSize']].set_index('index')

    otm_put_df = otm_put_df.groupby(otm_put_df.index,as_index=False).apply(lambda group: keep_closer_expiry(group,
                                                    right1='P')).reset_index().drop(['level_0','expiry','right'],1)
    otm_put_df = otm_put_df.rename(columns={'level_1': 'index'}).set_index('index').add_prefix('otm_put_')

    # filtrar solo los calls
    otm_call_df=full_df.groupby( [full_df.index,'right','expiry'],
                            as_index=False).apply(lambda group: keep_some_strikes2(group,+PCT_UP_CHAIN,'C')).reset_index().drop('level_0',1)

    otm_call_df = otm_call_df.rename(columns={'level_1': 'index'})
    otm_call_df = otm_call_df[['index','askImpliedVol','bidImpliedVol','modelImpliedVol','expiry'
        ,'right','askOptPrice','askSize','bidOptPrice','bidSize']].set_index('index')

    otm_call_df = otm_call_df.groupby(otm_call_df.index,as_index=False).apply(lambda group: keep_closer_expiry(group,
                                                    right1='C')).reset_index().drop(['level_0','expiry','right'],1)
    otm_call_df = otm_call_df.rename(columns={'level_1': 'index'}).set_index('index').add_prefix('otm_call_')

    # juntarlos y poner los datos en columnas
    final_df = pd.merge(atm_df, otm_put_df, how='left', left_index=True, right_index=True)
    final_df = pd.merge(final_df, otm_call_df, how='left', left_index=True, right_index=True)
    #juntar con el df de eventos
    #print final_df.columns

    # convertir a serie temporal (equiespaciada en t) frecuencia horaria
    out2=final_df.resample('H', label='right').mean().dropna()
    #out2.to_excel("out2.xlsx")

    # create indicator variables for statistics
    out1=pd.get_dummies(df1.Statistic).resample('H', label='right').sum().dropna()
    #out1.to_excel("out1.xlsx")

    # juntar con el dataframe de los eventos join
    #final_df=out1.join(out2, how='outer')
    #final_df=out1.merge(out2, how='outer', left_index=True, right_index=True)
    final_df = pd.merge(out1, out2, how='outer', left_index=True, right_index=True)
    final_df = final_df.sort_index(inplace=False,ascending=[True])

    con , meta = globalconf.connect_sqldb()

    final_df.to_sql(name='gekko_data',con=con, if_exists = 'replace')


    #modelo de regresion con las variables dummy y como el target el retorno del ES respecto al periodo anterio

# sacar diferencias con el peridodo anterior en cada fecha

# probar con varios periodos para la diferencia (para ajustar el impacto del evento y medirlo)

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

