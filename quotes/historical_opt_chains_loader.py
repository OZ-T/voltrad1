from copy import deepcopy
from datetime import datetime
from time import sleep

import pandas as pd

from core import misc_utilities, config
from ibutils.RequestOptionData import RequestOptionData
from ibutils.RequestUnderlyingData import RequestUnderlyingData
from ibutils.sync_client import IBClient
from core.logger import logger


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
    log=logger("historical optchain data loader")
    log.info("Getting historical option chains data from IB ... ")

    globalconf = config.GlobalConfig()
    underly_def = globalconf.get_tickers_optchain_ib()
    client = IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    dt_now=datetime.now() #- timedelta(days=4)
    endDateTime =  dt_now.strftime('%Y%m%d %H:%M:%S')

    underl = {}
    underl_under = {}
    for index , row in underly_def.iterrows():
        log.info("underl=[%s] [%s] [%s] [%d] [%s] [%s] [%s] [%s] [%d]" %
                 (row['symbol'], row['type'],str(row['Expiry']),0,'','', row['Exchange'] , row['Currency'] , int(index)) )
        underl.update({ int(index) : RequestOptionData( row['symbol'], row['type'],
                                                   str(row['Expiry']),0,'','', row['Exchange'] , row['Currency'] , int(index) ) })
        underl_under.update({int(index): RequestUnderlyingData(row['symbol'], row['underl_type'],
                                                               str(row['underl_expiry']), 0, '', '', row['underl_ex'],
                                                               row['underl_curr'], int(index))})
    log.info("Getting underlying price from IB ... ")
    underl_prc = client.getMktData(underl_under)
    row1 = 0
    opt_chain_ranges = {}
    for reqId, request in underl_prc.items():
        if reqId in request.get_out_data().keys():
            row1 += 1
            if "closePrice" in request.get_out_data()[reqId]:
                log.info("Requestid [%d]: Option[%s] Results len [%s]" % ( reqId , str(request.get_in_data()), len(request.get_out_data()) ))
                opt_chain_ranges.update({ request.get_in_data()["symbol"] : request.get_out_data()[reqId]["closePrice"]  })
                #print ("Requestid [%d]: modelVega=%0.5f" % ( reqId, request.get_out_data()[reqId]['modelVega'] ) )
    log.info("opt_chain_ranges = [%s]" % ( str(opt_chain_ranges) ) )

    log.info("Getting Options chain definitions from IB ... ")
    # get options chain
    list_results = client.getOptionsChain(underl)

    pct_range_opt_chain = float(globalconf.config['ib_api']['pct_range_opt_chain'])

    log.info("Number of requests [%d]" % (len(list_results)) )
    for reqId, request in list_results.items():
        log.info("Requestid [%d]: Option[%s] Len Results [%d]" % ( reqId , str(request.get_in_data()), len(request.optionsChain) ))

    contr = {}
    num = 100

    # lo mas que se puede pedir para barras de 30 min es un mes historico
    # barSizeSetting = "30 mins"
    barSizeSetting = "30 mins"
    useRTH = 1
    formatDate = 1
    wait_secs = 3
    df1 = pd.DataFrame()
    for reqId, request in list_results.items():
        log.info("Requestid [%d]: Chain size [%d]" % ( reqId , len( request.optionsChain ) ))
        for opt1 in request.optionsChain:
            if opt1["symbol"] in opt_chain_ranges:
                filtro_strikes = opt_chain_ranges[opt1["symbol"]]
                if opt1["strike"] >= (filtro_strikes * (1 - pct_range_opt_chain)) \
                        and opt1["strike"] <= (filtro_strikes * (1 + pct_range_opt_chain)):

                    path_h5 = "/" + str(opt1['symbol']) + "/" + str(opt1['expiry']) + "/" + \
                              str(opt1['right']) + "/" + str(opt1['strike'])
                    f = globalconf.open_historical_optchain_store()
                    node = f.get_node(path_h5)
                    last_record_stored = 0
                    if node:
                        df1 = f.select(node._v_pathname)
                        df1= df1.reset_index()['date']
                        last_record_stored = datetime.strptime(str(df1.max()), '%Y%m%d %H:%M:%S')
                        # no se debe usar .seconds que da respuesta incorrecta debe usarse .total_seconds()
                        #days= int(round( (dt_now - last_record_stored).total_seconds() / 60 / 60 / 24  ,0))
                        # lo anterior no sirve porque debe considerarse la diferencia en business days
                        #days = np.busday_count(last_record_stored.date(), dt_now.date())
                        #days= int(math.ceil( utils.office_time_between(last_record_stored,dt_now).total_seconds() / 60.0 / 60.0 / 7.0 ))

                        bh = misc_utilities.BusinessHours(last_record_stored, dt_now, worktiming=[15, 21], weekends=[6, 7])
                        days = bh.getdays()
                        barSizeSetting =   "5 mins" #"30 mins"
                        durationStr = str( days ) + " D"
                        if days > 3:
                            barSizeSetting = "30 mins"
                        #continue
                    else:
                        barSizeSetting = "30 mins" #"30 mins"
                        durationStr = "10 D"

                    log.info("path_h5=[%s] last_record_stored=[%s] endDateTime=[%s] durationStr=[%s] barSizeSetting=[%s]"
                          % ( path_h5 , str(last_record_stored),
                              str(endDateTime) , durationStr , barSizeSetting ) )
                    dataframeBID = pd.DataFrame(columns=['symbol','expiry','secType','strike',
                                                            'right','multiplier','currency','load_dttm',
                                                         'WAP_bid', 'close_bid', 'count_bid',
                                                         'hasGaps_bid', 'high_bid', 'low_bid', 'open_bid',
                                                         'reqId_bid', 'volume_bid'])
                    dataframeBID = dataframeBID.astype(float)

                    numBID=10000+num
                    contr[numBID] = RequestOptionData(opt1["symbol"],opt1["secType"],opt1["expiry"],opt1["strike"],opt1["right"],
                                                   opt1["multiplier"],opt1["exchange"],opt1["currency"],numBID)
                    historicallistBID = client.get_historical(contr[numBID], endDateTime, durationStr, barSizeSetting, "BID", useRTH,formatDate)
                    log.info("historicallistBID=[%d]" % (len(historicallistBID)))
                    if historicallistBID:
                        log.info("Appending BID...")
                        keysBID=historicallistBID.keys()
                        #for reqIdBID, requestBID in historicallistBID.iteritems():
                        for reqIdBID in keysBID:
                            temp_requestBID = deepcopy(historicallistBID[reqIdBID])
                            for dateBID, rowBID in temp_requestBID.items():
                                # print ("date2 [%s]: row2[%s]" % (date2, str(row2)))
                                tempBID = pd.DataFrame(rowBID, index=[0])
                                tempBID = tempBID.add_suffix('_bid')
                                tempBID['symbol'] = str(opt1['symbol'])
                                tempBID['expiry'] = str(opt1['expiry'])
                                tempBID['secType'] = str(opt1['secType'])
                                tempBID['strike'] = str(opt1['strike'])
                                tempBID['right'] = str(opt1['right'])
                                tempBID['multiplier'] = str(opt1['multiplier'])
                                tempBID['currency'] = str(opt1['currency'])
                                tempBID['load_dttm'] = endDateTime
                                tempBID = tempBID.rename(columns={'date_bid': 'date'})
                                dataframeBID = dataframeBID.append(tempBID.reset_index().drop('index', 1))

                    log.info("sleeping [%s] secs ..." % (str(wait_secs)))
                    sleep(wait_secs)
                    dataframeASK = pd.DataFrame(columns=['symbol','expiry','secType','strike',
                                                            'right','multiplier','currency','load_dttm',
                                                            'WAP_ask', 'close_ask','count_ask',
                                                         'hasGaps_ask', 'high_ask', 'low_ask', 'open_ask',
                                                         'reqId_ask', 'volume_ask'])
                    dataframeASK = dataframeASK.astype(float)

                    numASK = 20000 + num
                    contr[numASK] = RequestOptionData(opt1["symbol"],opt1["secType"],opt1["expiry"],opt1["strike"],opt1["right"],
                                                   opt1["multiplier"],opt1["exchange"],opt1["currency"],numASK)
                    historicallistASK = client.get_historical(contr[numASK], endDateTime, durationStr, barSizeSetting, "ASK", useRTH,formatDate)
                    log.info("historicallistASK=[%d]" % (len(historicallistASK)))
                    if historicallistASK:
                        log.info("Appending ASK...")
                        keysASK = historicallistASK.keys()
                        #for reqIdASK, requestASK in historicallistASK.iteritems():
                        for reqIdASK in keysASK:
                            temp_requestASK = deepcopy(historicallistASK[reqIdASK])
                            for dateASK, rowASK in temp_requestASK.items():
                                # print ("date2 [%s]: row2[%s]" % (date2, str(row2)))
                                tempASK = pd.DataFrame(rowASK, index=[0])
                                tempASK = tempASK.add_suffix('_ask')
                                tempASK['symbol'] = str(opt1['symbol'])
                                tempASK['expiry'] = str(opt1['expiry'])
                                tempASK['secType'] = str(opt1['secType'])
                                tempASK['strike'] = str(opt1['strike'])
                                tempASK['right'] = str(opt1['right'])
                                tempASK['multiplier'] = str(opt1['multiplier'])
                                tempASK['currency'] = str(opt1['currency'])
                                tempASK['load_dttm'] = endDateTime
                                tempASK = tempASK.rename(columns={'date_ask': 'date'})
                                tempASK = tempASK.reset_index().drop('index', 1)
                                dataframeASK = dataframeASK.append(tempASK.reset_index().drop('index', 1))

                    log.info("sleeping [%s] secs ..." % (str(wait_secs)))
                    sleep(wait_secs)
                    dataframeTRADES = pd.DataFrame(columns=['symbol','expiry','secType','strike',
                                                            'right','multiplier','currency','load_dttm',
                                                            'WAP_trades','close_trades','count_trades',
                                                            'hasGaps_trades','high_trades','low_trades','open_trades',
                                                            'reqId_trades','volume_trades'])

                    dataframeTRADES = dataframeTRADES.astype(float)

                    contr[num] = RequestOptionData(opt1["symbol"],opt1["secType"],opt1["expiry"],opt1["strike"],opt1["right"],
                                                   opt1["multiplier"],opt1["exchange"],opt1["currency"],num)
                    historicallistTRADES = client.get_historical(contr[num], endDateTime, durationStr, barSizeSetting,"TRADES", useRTH,formatDate)
                    log.info("historicallistTRADES=[%d]" % (len(historicallistTRADES)))
                    if historicallistTRADES:
                        log.info("Appending TRADES...")
                        keysTRADES = historicallistTRADES.keys()
                        #for reqId2, request2 in historicallistTRADES.iteritems():
                        for reqIdTRADES in keysTRADES:
                            temp_request2=deepcopy(historicallistTRADES[reqIdTRADES])
                            for date2, row2 in temp_request2.items():
                                #print ("date2 [%s]: row2[%s]" % (date2, str(row2)))
                                temp1 = pd.DataFrame(row2, index=[0])
                                temp1 = temp1.add_suffix('_trades')
                                temp1['symbol'] = str(opt1['symbol'])
                                temp1['expiry'] = str(opt1['expiry'])
                                temp1['secType'] = str(opt1['secType'])
                                temp1['strike'] = str(opt1['strike'])
                                temp1['right'] = str(opt1['right'])
                                temp1['multiplier'] = str(opt1['multiplier'])
                                temp1['currency'] = str(opt1['currency'])
                                temp1['load_dttm'] = endDateTime
                                temp1 = temp1.rename(columns={'date_trades': 'date'})
                                temp1=temp1.reset_index().drop('index', 1)
                                dataframeTRADES = dataframeTRADES.append(temp1.reset_index().drop('index', 1))

                    if historicallistTRADES:
                        dataframeTRADES = dataframeTRADES.sort_values(by=['date']).set_index('date')
                    if historicallistBID:
                        dataframeBID = dataframeBID.sort_values(by=['date']).set_index('date')
                    if historicallistASK:
                        dataframeASK = dataframeASK.sort_values(by=['date']).set_index('date')

                    if historicallistASK or historicallistBID or historicallistTRADES:
                        t_dataframe = pd.merge(dataframeTRADES,dataframeASK,how='outer',left_index=True, right_index=True,
                                             on=['symbol','expiry','secType','strike','right','multiplier',
                                                 'currency','load_dttm'])
                        dataframe = pd.merge(t_dataframe,dataframeBID,how='outer',left_index=True, right_index=True,
                                             on=['symbol','expiry','secType','strike','right','multiplier',
                                                 'currency','load_dttm'])
                        #print dataframe.index , dataframe.columns
                        log.info( "appending data in hdf5 ...")
                        f.append(path_h5, dataframe, data_columns=True)
                    f.close()  # Close file
                    log.info ("sleeping [%s] secs ..." % (str(wait_secs)))
                    sleep(wait_secs)
                    num += 1

    client.disconnect()

if __name__=="__main__":
    run_reader()
