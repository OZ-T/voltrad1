from volsetup import config
import datetime as dt
import pandas as pd
import numpy as np
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from time import sleep
from volibutils.sync_client import IBClient
import inspect
import time
from swigibpy import EPosixClientSocket, ExecutionFilter
from swigibpy import EWrapper
import swigibpy as sy
import sys
from volibutils.RequestOptionData import RequestOptionData
from datetime import datetime, timedelta
import numpy as np
from volsetup.logger import logger
import math
from volutils import utils

def run_reader():
    """
    ADVERTENCIA USO DATOS HISTORICOS:

    Se inserta un registro duplicado en cada carga incremental. Es decir:
        se vuelve a insertar la barra de la ultima media hora que estaba cargada ya en el hdf5
        y tipicamente el close de esta barra es distinto al cargado inicialmente.
        La analitica que se haga sobre esta tabla debe contemplar eliminar primero de los registros duplicados
        porque asumimos que el segundo es el valido (dado que es igual al open de la siguiente barra de media hora
        como se hgha observado)
        este error se puede eliminar o mitigar si solamente se piden los datos histoticos con el mercado cerrado
        que es lo que se hace en el modo automatico (crontab) Validar esto.
    :return:
    """
    log=logger("historical data loader")
    log.info("Getting historical underlying data from IB ... ")

    globalconf = config.GlobalConfig()
    underly_def = globalconf.get_tickers_historical_ib()
    client = IBClient(globalconf)
    client.connect()
    dt_now=datetime.now()
    endDateTime =  dt_now.strftime('%Y%m%d %H:%M:%S')
    # lo mas que se puede pedir para barras de 30 min es un mes historico
    # barSizeSetting = "30 mins"
    barSizeSetting = "1 min"
    whatToShow = "TRADES"
    useRTH = 1
    formatDate = 1
    wait_secs = 40
    f = globalconf.open_historical_store()

    for index, row_req in underly_def.iterrows():
        log.info("underl=[%s] [%s] [%s] [%d] [%s] [%s] [%s] [%s] [%d]"
                        % ( str(row_req['symbol']), str(row_req['underl_type']),
                            str(row_req['underl_expiry']), 0, '', '',
                            str(row_req['underl_ex']), str(row_req['underl_curr']), int(index) ) )

        ticker = RequestUnderlyingData(str(row_req['symbol']), str(row_req['underl_type']), str(row_req['underl_expiry']), 0, '', '',
                                       str(row_req['underl_ex']), str(row_req['underl_curr']), int(index))

        path_h5 = "/" + str(row_req['symbol'])
        if long(row_req['underl_expiry']) > 0:
            path_h5 = path_h5 + "/" + str(row_req['underl_expiry'])
        last_record_stored = 0
        node = f.get_node(path_h5)
        if node:
            df1 = f.select(node._v_pathname)
            df1= df1.reset_index()['date']
            last_record_stored = datetime.strptime(str(df1.max()), '%Y%m%d %H:%M:%S')
            # no se debe usar .seconds que da respuesta incorrecta debe usarse .total_seconds()
            #days= int(round( (dt_now - last_record_stored).total_seconds() / 60 / 60 / 24  ,0))
            # lo anterior no sirve porque debe considerarse la diferencia en business days
            #days = np.busday_count(last_record_stored.date(), dt_now.date())
            bh=utils.BusinessHours(last_record_stored,dt_now,worktiming=[15,21],weekends=[6,7])
            days = bh.getdays()
            durationStr = str( days ) + " D"
        else:
            durationStr = "30 D"
            barSizeSetting = "30 mins"

        if str(row_req['symbol']) in ['NDX','SPX','VIX']:
            barSizeSetting = "30 mins"

        log.info( "last_record_stored=[%s] endDateTime=[%s] durationStr=[%s] barSizeSetting=[%s]"
                  % ( str(last_record_stored), str(endDateTime) , durationStr, barSizeSetting) )

        historicallist = client.get_historical(ticker, endDateTime, durationStr, barSizeSetting,whatToShow, useRTH,formatDate)
        #print historicallist
        dataframe = pd.DataFrame()
        if historicallist:
            for reqId, request in historicallist.iteritems():
                for date, row in request.iteritems():
                    # print ("date [%s]: row[%s]" % (date, str(row)))
                    temp1 = pd.DataFrame(row, index=[0])
                    temp1['symbol'] = str(row_req['symbol'])
                    temp1['expiry'] = str(row_req['underl_expiry'])
                    temp1['type'] = str(row_req['underl_type'])
                    temp1['load_dttm'] = endDateTime
                    dataframe = dataframe.append(temp1.reset_index().drop('index', 1))

            dataframe = dataframe.sort_values(by=['date']).set_index('date')
            log.info( "appending data in hdf5 ...")
            f.append(path_h5, dataframe, data_columns=dataframe.columns)
        log.info("sleeping [%s] secs ..." % (str(wait_secs)))
        sleep(wait_secs)

    client.disconnect()
    f.close()  # Close file

if __name__=="__main__":
    run_reader()