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

def read_h5_source_data():
    df1=ra.read_biz_calendar(valuation_dttm=now)
    # sacar del historical options chain P y C ATM y OTM volumenes y precios bid ask
    df2=ra.extrae_options_chain2(start_dttm=start1, end_dttm=now, symbol="ES", expiry="", secType="FOP")
    return df1,df2

def keep_some_strikes(ds,pct):
    return ds.ix[(ds.strike - (1 + pct) * ds.lastUndPrice).abs().argsort()[:1]]

def keep_some_strikes2(ds,pct,right1):
    ds=ds.ix[(ds.strike - (1 + pct) * ds.lastUndPrice).abs().argsort()[:1]]
    return ds.ix[(ds.expiry == min(ds.expiry)) & (ds.right == right1)]


if __name__ == "__main__":
    now = datetime.now()
    start1 = datetime(year=2016, month=6, day=2, hour=21, minute=59, second=59)

    df1, df2 = read_h5_source_data()

    # el precio mas probable en cada datetime del subyacente
    und_prc = df2[['lastUndPrice', ]].groupby(df2.index, as_index=True).apply(lambda group: group.mean())

    df2 = df2[['askImpliedVol', 'bidImpliedVol', 'expiry', 'modelImpliedVol', 'right', 'strike', 'symbol']]
    full_df = pd.merge(df2, und_prc, how='left', left_index=True,right_index=True)

    # filtrar los strikes ATM C y P , el P 8% por debajo y el C 6% por encima
    atm_df=full_df.groupby( [full_df.index,'right','expiry'],
                            as_index=False).apply(lambda group: keep_some_strikes(group,0.0)).reset_index().drop('level_0',1)
    # TODO: cuando se acerca la expiracion hay que pasar al expiry siguiente con algunos dias de ANTELACION
    # atm_df = atm_df.ix[(atm_df.index + 10 dias (timedelta) formatear a fecha <= atm_df.expiry) ]
    atm_df=atm_df.ix[(atm_df.expiry == min(atm_df.expiry)) & (atm_df.right == 'C')]
    atm_df=atm_df[['index','askImpliedVol','bidImpliedVol','modelImpliedVol','lastUndPrice']].set_index('index').add_prefix('atm_')
    # filtrar solo los puts
    otm_put_df=full_df.groupby( [full_df.index,'right','expiry'],
                            as_index=False).apply(lambda group: keep_some_strikes(group,-PCT_DOWN_CHAIN)).reset_index().drop('level_0',1)
    otm_put_df = otm_put_df.ix[(otm_put_df.expiry == min(otm_put_df.expiry)) & (otm_put_df.right == 'P')]
    otm_put_df = otm_put_df[['index','askImpliedVol','bidImpliedVol','modelImpliedVol']].set_index('index').add_prefix('otm_put_')
    # filtrar solo los calls
    otm_call_df=full_df.groupby( [full_df.index,'right','expiry'],
                            as_index=False).apply(lambda group: keep_some_strikes2(group,+PCT_UP_CHAIN,'C')).reset_index().drop('level_0',1)
    otm_call_df = otm_call_df.ix[(otm_call_df.expiry == min(otm_call_df.expiry)) & (otm_call_df.right == 'C')]
    otm_call_df = otm_call_df[['index','askImpliedVol','bidImpliedVol','modelImpliedVol']].set_index('index').add_prefix('otm_call_')
    # juntarlos y poner los datos en columnas
    final_df = pd.merge(atm_df, otm_put_df, how='left', left_index=True, right_index=True)
    final_df = pd.merge(final_df, otm_call_df, how='left', left_index=True, right_index=True)
    #juntar con el df de eventos
    # juntar con el dataframe de los eventos join
    final_df=df1.join(final_df, how='outer')


    # convertir a serie temporal (equiespaciada en t) frecuencia horaria
    # Todo : mirar porque salen datos raros en la agregacion
    out2=final_df.resample('H', label='right').sum()
    out2.to_excel("out2.xlsx")

    # create indicator variables for statistics
    out1=pd.get_dummies(final_df.Statistic)
    out1.to_excel("out1.xlsx")


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

