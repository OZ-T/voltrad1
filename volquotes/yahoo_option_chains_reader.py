"""
File to run automatically by cron each day to gather options data

/etc/crontab

entry:

50 23 * * 1,2,3,4,5 python /home/david/python/vol_tradblotter/vol_grab_optchain_pandas_yhoo1.py

"""
import platform
import datetime as dt
import pandas as pd
import numpy as np
import h5py as h5
import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError
#import pandas.io as web
import os
from datetime import datetime, timedelta
from volsetup import config
import sys
from volsetup.logger import logger

def clean_cols_for_hdf2(df):
    types = df.apply(lambda x: pd.lib.infer_dtype(x.values))
    for col in types[types=='mixed'].index:
        df[col] = df[col].astype(str)
    for col in types[types=='unicode'].index:
        df[col] = df[col].astype(str)
    df.columns = [str(c) for c in df.columns]
    return df

globalconf = config.GlobalConfig()
optchain_def = globalconf.get_tickers_optchain_yahoo()
source1 = globalconf.config['use_case_yahoo_options']['source']
log = logger("yahoo options chain")
log.info("Getting options chain data from yahoo w pandas_datareader ... [%s]"
         % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') ))
num_ticks = len(optchain_def.index)  # used to print status
months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
#log.info("option chain definition [%s]" % (str(optchain_def)))
f = globalconf.open_yahoo_h5_store()  # open database file
datelist = pd.date_range(pd.datetime.today(), freq='M', periods=5).tolist()

#i="SPY"
for symbol,row in optchain_def.iterrows():
    log.info("Init yahoo quotes downloader symbol=%s" % (symbol) )
    #log.info(i,row[0])
    try:
        option = web.Options(symbol,source1)
        #log.info("option=[%s] dir(options)=[%s]" % (str(option),str(dir(option)) ))
        for j in option.expiry_dates:
            #j=datetime.date(2016,6,17)
            log.info("expiry=%s" % (str(j)))
            try:
                joe = pd.DataFrame()
                call_data = option.get_call_data(expiry=j)
                put_data = option.get_put_data(expiry=j)
                #call_data.Expiry = j   fix error reading dates in get_call_data pandas code
                #put_data.Expiry = j   fix error reading dates in get_call_data pandas code
                if call_data.values.any():
                    joe = joe.append(call_data)
                if put_data.values.any():
                    joe = joe.append(put_data)
                #joe = joe.sort_values(by=['Symbol', 'Quote_Time'])
                joe = joe.sort_index()
                joe['Quote_Time_txt'] = joe['Quote_Time'].dt.strftime("%Y-%m-%d %H:%M:%S")
                joe['Last_Trade_Date_txt'] = joe['Last_Trade_Date'].dt.strftime("%Y-%m-%d %H:%M:%S")
                joe = joe.reset_index().set_index("Quote_Time")
                joe['Expiry_txt'] = joe['Expiry'].dt.strftime("%Y-%m-%d %H:%M:%S")
                joe = clean_cols_for_hdf2(joe)

                try:
                    f.append("/" + symbol, joe, data_columns=True)
                #             ,min_itemsize={'JSON': 420,'Symbol': 25,'Root': 10})
                # ,'OpenInt':10,'Vol':10}) PENDIENTE recrear el hdf5 con longitudes por defecto
                except ValueError as e:
                    log.warn("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
                    aux = globalconf.open_yahoo_h5_store_error()
                    aux.append("/" + symbol, joe, data_columns=True)
                    #           ,min_itemsize={'JSON': 500,'Symbol': 25,'Root': 10})
                    aux.close()
            except KeyError as e:
                log.warn("KeyError raised [" + str(e) + "]...")

    except RemoteDataError as err:
        log.info("No information for ticker [%s] RemoteDataError=[%s] sys_info=[%s]" % (str(symbol) , str(err) , sys.exc_info()[0] ))
        continue
f.close()  # Close file
