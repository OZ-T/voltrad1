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
import datetime
from volsetup import config
import sys

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

f = globalconf.open_yahoo_h5_db()  # open database file
year = f.require_group(c_year)  # Make hdf5 group for year
month = year.require_group(c_month)  # Make hdf5 group for month
day = month.require_group(c_day)  # Make hdf5 group for day
hour = day.require_group(c_hour)

######

# https://github.com/pydata/pandas-datareader/issues/212


#i="SPY"
num = 0
for i,row in optchain_def.iterrows():
    print ("Init yahoo quotes downloader [%s]" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') ) )
    print(i,row[0])
    option = web.Options(i,source1)
    ticker1 = hour.require_group(i)
    print("option=[%s] dir(options)=[%s]" % (str(option),str(dir(option)) ))
    try:
    	for j in option.expiry_dates:
           #print(j)
	   try:
           	#j=datetime.date(2016,6,17)
           	call_data = option.get_call_data(expiry=j).reset_index()
           	put_data = option.get_put_data(expiry=j).reset_index()
           	call_data.Expiry = j  # fix error reading dates in get_call_data pandas code
           	put_data.Expiry = j  # fix error reading dates in get_call_data pandas code
           	if call_data.values.any():
               		#try:
                   	datac = call_data[['Strike','Last','Bid','Ask','Chg','PctChg','Vol','Open_Int','IV']]
                   	#datac = call_data[['Strike','Last','Vol']]
                   	call_ds = ticker1.require_dataset(i+str(j)+"C",datac.shape,float)
                   	#datac.Vol = datac.Vol.str.replace(',', '')
                   	call_ds.attrs['source']=source1
                   	call_ds.attrs['symbol']=i
                   	call_ds.attrs['name']=row[0]
                   	call_ds.attrs['category']=row[1]
                   	call_ds.attrs['type']="call"
                   	call_ds.attrs['fields']="Strike,Last,Bid,Ask,Chg,PctChg,Vol,Open_Int,IV"
                   	call_ds.attrs['expiry']=str(j)
                   	call_ds[...] = datac.astype(np.float32)
               	#except:
               	#    print ("Error saving dataset in hdf5 file (%s) " % (i+str(j)+"C"))
           	if put_data.values.any():
               		#try:
                   	datap = put_data[['Strike','Last','Bid','Ask','Chg','PctChg','Vol','Open_Int','IV']]
                   	#datap = put_data[['Strike','Last''Vol']]
                   	put_ds = ticker1.require_dataset(i+str(j)+"P",datap.shape,float)
                   	#datap.Vol = datap.Vol.str.replace(',', '')
                   	put_ds.attrs['source']=source1
                   	put_ds.attrs['symbol']=i
                   	put_ds.attrs['name']=row[0]
                   	put_ds.attrs['category']=row[1]
			put_ds.attrs['type']="put"
        	        put_ds.attrs['fields']="Strike,Last,Bid,Ask,Chg,PctChg,Vol,Open_Int,IV"
        	        put_ds.attrs['expiry']=str(j)
        	        put_ds[...] = datap.astype(np.float32)
        	        #except:
        	        #    print ("Error saving dataset in hdf5 file (%s) " % (i+str(j)+"P"))
	   except:
        	   print ("Error saving dataset in hdf5 file (%s) " % (i+str(j)))
		
    	# status update
    	num += 1
    except RemoteDataError as err:
	print("No information for ticker [%s] RemoteDataError=[%s] sys_info=[%s]" % (str(i) , str(err) , sys.exc_info()[0] ))
	continue	
    print ("Completed %s [%s of %s]" % (str(i), str(num), str(num_ticks)))

f.close()  # Close file

