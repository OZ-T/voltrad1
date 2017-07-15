import datetime as dt

import numpy as np
import pandas as pd

import volibutils.sync_client as ib
from core import utils as utils
from core.market_data_methods import write_market_data_to_sqllite
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volsetup import config


def run_reader():
    globalconf = config.GlobalConfig()
    log = globalconf.log
    optchain_def = globalconf.get_tickers_optchain_ib()
    source1 = globalconf.config['use_case_ib_options']['source']

    if dt.datetime.now().date() in utils.get_trading_close_holidays(dt.datetime.now().year):
        log.info("This is a US Calendar holiday. Ending process ... ")
        return

    log.info("Getting realtime option chains data from IB ... ")

    underl = {}
    for index , row in optchain_def.iterrows():
        log.info("underl=[%s] [%s] [%s] [%s] [%s] [%d]"
                 % ( row['symbol'], row['type'], str(row['Expiry']),row['Exchange'],row['Currency'],int(index)))
        underl.update({ int(index) : RequestOptionData( row['symbol'], row['type'],
                                                   str(row['Expiry']),0,'','', row['Exchange'] , row['Currency'] , int(index) ) })

    underl_under = {}
    for index , row in optchain_def.iterrows():
        log.info("underl_under=[%s] [%s] [%s] [%s] [%s] [%d]"
                 % (row['symbol'], row['underl_type'],str(row['underl_expiry']),row['underl_ex'] , row['underl_curr'] , int(index)) )
        underl_under.update({ int(index) : RequestUnderlyingData( row['symbol'], row['underl_type'],
                                                   str(row['underl_expiry']),0,'','', row['underl_ex'] , row['underl_curr'] , int(index) ) })

    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)

    #causa error en ubuntu : porque pone a isDOne la primera ::
    #   print("Get time from server [%s] isConnected? [%s] " % ( str(client.getTime()) , str(client.isConnected() )  ) )

    underl_prc = client.getMktData(underl_under)
    row1 = 0
    opt_chain_ranges = {}
    for reqId, request in underl_prc.items():
        if reqId in request.get_out_data().keys():
            row1 += 1
            if "closePrice" in request.get_out_data()[reqId]:
                log.info ("Requestid [%d]: Option[%s] Results length [%d]"
                          % ( reqId , str(request.get_in_data()), len(request.get_out_data()) ))
                opt_chain_ranges.update({ request.get_in_data()["symbol"] : request.get_out_data()[reqId]["closePrice"]  })
                #print ("Requestid [%d]: modelVega=%0.5f" % ( reqId, request.get_out_data()[reqId]['modelVega'] ) )
    log.info("opt_chain_ranges = [%s] " %  (str(opt_chain_ranges)))


    # get options chain
    list_results = client.getOptionsChain(underl)

    log.info("Number of requests [%d]" % (len(list_results)) )
    for reqId, request in list_results.items():
        log.info ("Requestid [%d]: Option[%s] Len Results [%d]" % ( reqId , str(request.get_in_data()), len(request.optionsChain) ))

    contr = {}
    num = 100
    pct_range_opt_chain = float(globalconf.config['ib_api']['pct_range_opt_chain'])
    for reqId, request in list_results.items():
        #print ("Requestid [%d]: Chain size [%d] detail [%s]"
        #       % ( reqId , len( request.optionsChain ) , str(request.optionsChain)  ))
        for opt1 in request.optionsChain:
            if opt1["symbol"] in opt_chain_ranges:
                filtro_strikes = opt_chain_ranges[ opt1["symbol"] ]
                #print ("filtro_strikes = " , filtro_strikes , "opt1 = " , opt1 )
                if  opt1["strike"] >= (filtro_strikes * ( 1 - pct_range_opt_chain ) ) \
                        and opt1["strike"]<= (filtro_strikes * ( 1 + pct_range_opt_chain ) ):
                    contr[num] = RequestOptionData(opt1["symbol"],opt1["secType"],opt1["expiry"],opt1["strike"],opt1["right"],
                                                   opt1["multiplier"],opt1["exchange"],opt1["currency"],num)
                    num += 1

    list_results2= client.getMktData(contr)
    client.disconnect()
    dataframe = pd.DataFrame()
    row1 = 0

    for reqId, request in list_results2.items():
        row1 += 1
        #print ("Requestid [%d]: Option[%s] Results [%s]" % ( reqId , str(request.get_in_data()), str(request.get_out_data()) ))
        #print ("Requestid [%d]: modelVega=%0.5f" % ( reqId, request.get_out_data()[reqId]['modelVega'] ) )
        dict1 = request.get_in_data().copy()
        if reqId in request.get_out_data():
            dict1.update(request.get_out_data()[reqId])
        for key in dict1.keys():
            dataframe.loc[row1,key] = dict1[key]

    log.info(str(list(dataframe.columns.values)))
    dataframe['current_date']=dt.datetime.now().strftime('%Y%m%d')
    dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    #dataframe.to_hdf(f,"IB_IDX",format='table')
    # c_day da un warning
    #   NaturalNameWarning: object name is not a valid Python identifier: '9'; it does not match the pattern
    #   ``^[a-zA-Z_][a-zA-Z0-9_]*$``; you will not be able to use natural naming to access this object; using
    #   ``getattr()`` will still work, though   NaturalNameWarning)
    #f.append(    c_year+"/"+c_month+"/"+c_day+"/"+c_hour+"/"+c_minute , dataframe, data_columns=dataframe.columns)
    #f.close()  # Close file

    # sort the dataframe
    dataframe = dataframe[dataframe.current_datetime.notnull()]
    types = dataframe.apply(lambda x: pd.lib.infer_dtype(x.values))
    # print str(types)
    for col in types[types == 'floating'].index:
        dataframe[col] = dataframe[col].map(lambda x: np.nan if x > 1e200 else x)

    dataframe['index'] = dataframe['current_datetime'].apply(lambda x: dt.datetime.strptime(x, '%Y%m%d%H%M%S'))

    dataframe=dataframe.sort_values(by=['index'], inplace=False)
    # set the index to be this and don't drop
    dataframe.set_index(keys=['index'], drop=True, inplace=True)
    # get a list of names
    names = dataframe['symbol'].unique().tolist()

    for name in names:
        # now we can perform a lookup on a 'view' of the dataframe
        log.info("Storing " + name + " in ABT ...")
        joe = dataframe[dataframe.symbol == name]
        joe=joe.sort_values(by=['symbol', 'current_datetime', 'expiry', 'strike', 'right'])
        # joe.to_excel(name+".xlsx")
        write_market_data_to_sqllite(globalconf, log, joe, "optchain_ib")



if __name__=="__main__":
    run_reader()
