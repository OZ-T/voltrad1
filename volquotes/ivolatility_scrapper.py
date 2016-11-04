import urllib2
from bs4 import BeautifulSoup
import h5py as h5
from volsetup import config
import datetime as dt
import numpy as np
import pandas as pd
from volsetup.logger import logger

log = logger("ivolatility download")
log.info("Getting volatility data from ivolatility.com ... ")

def download(url, user_agent='wswp', num_retries=2):
    log.info ('[%s] Downloading: %s' % (str(dt.datetime.now()) , url ) )
    headers = {'User-agent': user_agent}
    request = urllib2.Request(url, headers=headers)
    try:
        html = urllib2.urlopen(request).read()
    except urllib2.URLError as e:
        print 'Download error:', e.reason
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # retry 5XX HTTP errors
                return download(url, user_agent, num_retries-1)
    return html

globalconf = config.GlobalConfig()

b_html=download("http://www.ivolatility.com/options.j?ticker=SPX&R=0",
                user_agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
                )

soup = BeautifulSoup(b_html, 'html.parser')
fixed_html = soup.prettify()
ul = soup.find_all('font', attrs={'class':'s2'})
#type(ul)
#for i in ul:
#    print i.text

#historical volatility
HV_10d_current = float(ul[7].text.strip('%')) / 100.0
HV_10d_1wk = float(ul[8].text.strip('%')) / 100.0
HV_10d_1mo = float(ul[9].text.strip('%')) / 100.0
HV_10d_52wk_hi = (ul[10].text)
HV_10d_52wk_lo = (ul[11].text)

HV_20d_current = float(ul[13].text.strip('%')) / 100.0
HV_20d_1wk = float(ul[14].text.strip('%')) / 100.0
HV_20d_1mo = float(ul[15].text.strip('%')) / 100.0
HV_20d_52wk_hi = (ul[16].text)
HV_20d_52wk_lo = (ul[17].text)

HV_30d_current = float(ul[19].text.strip('%')) / 100.0
HV_30d_1wk = float(ul[20].text.strip('%')) / 100.0
HV_30d_1mo = float(ul[21].text.strip('%')) / 100.0
HV_30d_52wk_hi = (ul[22].text)
HV_30d_52wk_lo = (ul[23].text)

#Impl. vol.
IV_Index_call_current = float(ul[26].text.strip('%')) / 100.0
IV_Index_call_1wk = float(ul[27].text.strip('%')) / 100.0
IV_Index_call_1mo = float(ul[28].text.strip('%')) / 100.0
IV_Index_call_52wk_hi = (ul[29].text)
IV_Index_call_52wk_lo = (ul[30].text)

IV_Index_put_current = float(ul[32].text.strip('%')) / 100.0
IV_Index_put_1wk = float(ul[33].text.strip('%')) / 100.0
IV_Index_put_1mo = float(ul[34].text.strip('%')) / 100.0
IV_Index_put_52wk_hi = (ul[35].text)
IV_Index_put_52wk_lo = (ul[36].text)

IV_Index_mean_current = float(ul[38].text.strip('%')) / 100.0
IV_Index_mean_1wk = float(ul[39].text.strip('%')) / 100.0
IV_Index_mean_1mo = float(ul[40].text.strip('%')) / 100.0
IV_Index_mean_52wk_hi = (ul[41].text)
IV_Index_mean_52wk_lo = (ul[42].text)

ivol_data=  np.array([
    HV_10d_current,
    HV_10d_1wk,
    HV_10d_1mo,
    HV_20d_current,
    HV_20d_1wk,
    HV_20d_1mo,
    HV_30d_current,
    HV_30d_1wk,
    HV_30d_1mo,
    IV_Index_call_current,
    IV_Index_call_1wk,
    IV_Index_call_1mo,
    IV_Index_put_current,
    IV_Index_put_1wk,
    IV_Index_put_1mo,
    IV_Index_mean_current,
    IV_Index_mean_1wk,
    IV_Index_mean_1mo
])


months = globalconf.months

now = dt.datetime.now()  # Get current time
c_month = months[now.month]  # Get current month
c_day = str(now.day)  # Get current day
c_year = str(now.year)  # Get current year
c_hour = str(now.hour)
lvl3="/"+c_year+"/"+c_month+"/"+c_day+"/"+c_hour
store = globalconf.open_ivol_h5_db()  # open database file
temp1=ivol_data
dataframe = pd.DataFrame(
    {
        'HV_10d_current': temp1[0]
        , 'HV_10d_1wk': temp1[1]
        , 'HV_10d_1mo': temp1[2]
        , 'HV_20d_current': temp1[3]
        , 'HV_20d_1wk': temp1[4]
        , 'HV_20d_1mo': temp1[5]
        , 'HV_30d_current': temp1[6]
        , 'HV_30d_1wk': temp1[7]
        , 'HV_30d_1mo': temp1[8]
        , 'IV_Index_call_current': temp1[9]
        , 'IV_Index_call_1wk': temp1[10]
        , 'IV_Index_call_1mo': temp1[11]
        , 'IV_Index_put_current': temp1[12]
        , 'IV_Index_put_1wk': temp1[13]
        , 'IV_Index_put_1mo': temp1[14]
        , 'IV_Index_mean_current': temp1[15]
        , 'IV_Index_mean_1wk': temp1[16]
        , 'IV_Index_mean_1mo': temp1[17]
        , 'HV_10d_52wk_hi': HV_10d_52wk_hi.encode('utf-8').strip()
        , 'HV_10d_52wk_lo': HV_10d_52wk_lo.encode('utf-8').strip()
        , 'HV_20d_52wk_hi': HV_20d_52wk_hi.encode('utf-8').strip()
        , 'HV_20d_52wk_lo': HV_20d_52wk_lo.encode('utf-8').strip()
        , 'HV_30d_52wk_hi': HV_30d_52wk_hi.encode('utf-8').strip()
        , 'HV_30d_52wk_lo': HV_30d_52wk_lo.encode('utf-8').strip()
        , 'IV_Index_call_52wk_hi': IV_Index_call_52wk_hi.encode('utf-8').strip()
        , 'IV_Index_call_52wk_lo': IV_Index_call_52wk_lo.encode('utf-8').strip()
        , 'IV_Index_mean_52wk_hi': IV_Index_mean_52wk_hi.encode('utf-8').strip()
        , 'IV_Index_mean_52wk_lo': IV_Index_mean_52wk_lo.encode('utf-8').strip()
        , 'IV_Index_put_52wk_hi': IV_Index_put_52wk_hi.encode('utf-8').strip()
        , 'IV_Index_put_52wk_lo': IV_Index_put_52wk_lo.encode('utf-8').strip()
        , 'load_dttm': dt.datetime.strptime(str(lvl3), "/%Y/%b/%d/%H")
    }
    , index={0})


dataframe = dataframe.reset_index().set_index('load_dttm')
dataframe = dataframe.drop('index', 1)
dataframe['load_dttm_txt'] = dataframe.index.strftime("%Y-%m-%d %H:%M:%S")
types = dataframe.apply(lambda x: pd.lib.infer_dtype(x.values))
for col in types[types == 'unicode'].index:
    dataframe[col] = dataframe[col].astype(str)
log.info("Storing data in HDF5 ...")
store.append('/IVOL', dataframe, data_columns=True)
store.close()



"""
year = f.require_group(c_year)  # Make hdf5 group for year
month = year.require_group(c_month)  # Make hdf5 group for month
day = month .require_group(c_day)  # Make hdf5 group for day
hour = day.require_group(c_hour)

call_ds = hour.require_dataset("IVOL_SPX",ivol_data.shape,float)
call_ds.attrs['source']="ivolatility"
call_ds.attrs['symbol']="SPX"
call_ds.attrs['fields'] = HV_10d_current,HV_10d_1wk,HV_10d_1mo,HV_20d_current,HV_20d_1wk,HV_20d_1mo,
HV_30d_current,HV_30d_1wk,HV_30d_1mo,IV_Index_call_current,IV_Index_call_1wk,
IV_Index_call_1mo,IV_Index_put_current,IV_Index_put_1wk,IV_Index_put_1mo,
IV_Index_mean_current,IV_Index_mean_1wk,IV_Index_mean_1mo
call_ds.attrs['HV_10d_52wk_hi']=HV_10d_52wk_hi
call_ds.attrs['HV_10d_52wk_lo']=HV_10d_52wk_lo
call_ds.attrs['HV_20d_52wk_hi']=HV_20d_52wk_hi
call_ds.attrs['HV_20d_52wk_lo']=HV_20d_52wk_lo
call_ds.attrs['HV_30d_52wk_hi']=HV_30d_52wk_hi
call_ds.attrs['HV_30d_52wk_lo']=HV_30d_52wk_lo
call_ds.attrs['IV_Index_call_52wk_hi']=IV_Index_call_52wk_hi
call_ds.attrs['IV_Index_call_52wk_lo']=IV_Index_call_52wk_lo
call_ds.attrs['IV_Index_put_52wk_hi']=IV_Index_put_52wk_hi
call_ds.attrs['IV_Index_put_52wk_lo']=IV_Index_put_52wk_lo
call_ds.attrs['IV_Index_mean_52wk_hi']=IV_Index_mean_52wk_hi
call_ds.attrs['IV_Index_mean_52wk_lo']=IV_Index_mean_52wk_lo
call_ds[...] = ivol_data.astype(np.float32)

"""




