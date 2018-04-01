"""
File to run automatically by cron each day to gather options data

/etc/crontab

entry:

50 23 * * 1,2,3,4,5 python /home/david/python/voltrad1/volquotes/yahoo_option_chains_reader_fix_beautifulsoup.py

"""
import datetime as dt
import urllib
import pandas as pd
from core import config
from BeautifulSoup import BeautifulSoup
from core.logger import logger

if __name__ == "__main__":

    globalconf = config.GlobalConfig()
    optchain_def = globalconf.get_tickers_optchain_yahoo()
    source1 = globalconf.config['use_case_yahoo_options']['source']
    log = logger("yahoo options chain")
    log.info("Getting options chain data from yahoo w beautifulsoup ... ")

    num_ticks = len(optchain_def.index)  # used to print status

    months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
              7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    #log.info("option chain definition [%s]" % (str(optchain_def)))
    now = dt.datetime.now()  # Get current time
    c_month = months[now.month]  # Get current month
    c_day = str(now.day)  # Get current day
    c_year = str(now.year)  # Get current year
    c_hour = str(now.hour)
    c_minute = str(now.minute)
    f = globalconf.open_yahoo_h5_store_fix()  # open database file
    load_dttm = now.strftime('%Y-%m-%d-%H-%M-%S')
    datelist = pd.date_range(pd.datetime.today(), freq='M', periods=5).tolist()

    dataframe = pd.DataFrame()
    # i="SPY"
    for symbol, row in optchain_def.iterrows():
        #log.info ("Init yahoo quotes downloader [%s]" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        #log.info(symbol, row[0])

        datelist = pd.date_range(pd.datetime.today(), freq='M', periods=5).tolist()
        #print datelist
        headers = ['Strike', 'Symbol', 'Last', 'Chg', 'Bid', 'Ask', 'Vol', 'OpenInt', ]

        for i in datelist:
            #symbol = 'SPY'
            expiry = i.strftime('%Y-%m')
            url = 'https://ca.finance.yahoo.com/q/op?s={}&m={}'.format(symbol, expiry)
            # url= 'http://finance.yahoo.com/quote/SPY/options?p=SPY'
            # https://ca.finance.yahoo.com/q/op?s=SPY&m=2016-09

            response = urllib.urlopen(url)
            input = response.read()
            soup = BeautifulSoup(input)
            temp_optionsTable = [
                [str(x.text) for x in y.parent.contents]
                for y in soup.findAll('td', attrs={'class': 'yfnc_h'})
                ]
            #print ("expiry [%s] temp_optionsTable [%s] length [%d] " % (expiry,str(temp_optionsTable),len(temp_optionsTable)))
            if len(temp_optionsTable) > 1:
                temp1 = pd.DataFrame(temp_optionsTable, columns=headers)
                temp1['expiry'] = expiry
                temp1['symbol'] = symbol
                # temp1=temp1[temp1['Strike'].str.isnumeric()]
                dataframe = dataframe.append(temp1)

    dataframe=dataframe[dataframe['Strike'] <> '&nbsp;&nbsp;&nbsp;']
    # la extraccion de beutifulsoup de ca finance yahoo saca duplicados en la option chain
    dataframe=dataframe.drop_duplicates()
    dataframe['load_dttm'] = load_dttm

    # sort the dataframe
    #dataframe.sort(columns=['symbol'], inplace=True) DEPRECATED
    dataframe=dataframe.sort_values(by=['symbol'])
    # set the index to be this and don't drop
    dataframe.set_index(keys=['symbol'], drop=False,inplace=True)
    # get a list of names
    names=dataframe['symbol'].unique().tolist()

    for name in names:
        # now we can perform a lookup on a 'view' of the dataframe
        #node=f.get_node("/"+name)
        #df1=f.select(node._v_pathname)
        #del f["/"+name]
        log.info( "storing =[%s]" % (  name ))
        joe = dataframe.loc[dataframe.symbol==name]
        #joe=joe.append(df1)
        #joe.sort(columns=['Symbol','load_dttm'], inplace=True) DEPRECATED
        joe = joe.sort_values(by=['Symbol','load_dttm'])
        try:
            f.append( "/"+name , joe, data_columns=True, min_itemsize={'Last': 10})
            # ,'OpenInt':10,'Vol':10}) PENDIENTE recrear el hdf5 con longitudes por defecto
        except ValueError as e:
            log.warn("ValueError raised [" + str(e) + "]  Creating ancilliary file ...")
            aux = globalconf.open_yahoo_h5_store_fix_error()
            aux.append("/" + name, joe, data_columns=True, min_itemsize={'Last': 10})
            aux.close()

    f.close()  # Close file


