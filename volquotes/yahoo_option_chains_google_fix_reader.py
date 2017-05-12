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
import os
import datetime
from volsetup import config
from google_fix import *

if __name__ == "__main__":
    globalconf = config.GlobalConfig()
    optchain_def = globalconf.get_tickers_optchain_yahoo()
    source1 = globalconf.config['use_case_yahoo_options']['source']

    num_ticks = len(optchain_def.index)  # used to print status

    months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
              7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    print("option chain definition [%s]" % (str(optchain_def)))
    now = dt.datetime.now()  # Get current time
    c_month = months[now.month]  # Get current month
    c_day = str(now.day)  # Get current day
    c_year = str(now.year)  # Get current year
    c_hour = str(now.hour)
    c_minute = str(now.minute)

    f = globalconf.open_yahoo_h5_db()  # open database file

    ######

    # https://github.com/pydata/pandas-datareader/issues/212


    #i="SPY"
    num = 0
    current_dttm = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print ("Init yahoo quotes downloader [%s]" % ( current_dttm  ) )
    for i,row in optchain_def.iterrows():
        print(i,row[0])
        calls , puts = Options(i)
        ticker1 = hour.require_group(i)
        calls['current_dttm'] =  current_dttm
        puts['current_dttm'] =  current_dttm
        f.append(    c_year+"/"+c_month+"/"+c_day+"/"+c_hour+"/"+c_minute+"/calls" , calls , data_columns=calls.columns)
        f.append(    c_year+"/"+c_month+"/"+c_day+"/"+c_hour+"/"+c_minute+"/puts" , puts , data_columns=puts.columns)

    f.close()  # Close file
