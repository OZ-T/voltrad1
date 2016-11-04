import volibutils.sync_client as ib
from volsetup import config
import datetime as dt
import pickle
import pandas as pd
import swigibpy as sy
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volsetup.logger import logger

def run_get_orders():
    log=logger("run_get_orders")
    log.info("Getting orders data from IB ... ")
    globalconf = config.GlobalConfig()
    client = ib.IBClient(globalconf)
    client.connect()
    months = globalconf.months
    now = dt.datetime.now()  # Get current time
    c_month = months[now.month]  # Get current month
    c_day = str(now.day)  # Get current day
    c_year = str(now.year)  # Get current year
    c_hour = str(now.hour)
    c_minute = str(now.minute)

    ## Get the executions (gives you everything for last business day)
    execlist = client.get_executions(10)
    client.disconnect()
    log.info("execlist length = [%d]" % ( len(execlist) ))
    if execlist:
        dataframe = pd.DataFrame.from_dict(execlist).transpose()
        #print("dataframe = ",dataframe)
        f = globalconf.open_orders_store()
        dataframe['current_date'] = dt.datetime.now().strftime('%Y%m%d')
        dataframe['current_datetime'] = dt.datetime.now().strftime('%Y%m%d%H%M%S')
        log.info("Appending orders to HDF5 store ...")
        #f.append(c_year + "/" + c_month + "/" + c_day + "/" + c_hour + "/" + c_minute, dataframe,
        #         data_columns=dataframe.columns)
        #f.close()  Close file

        # sort the dataframe
        dataframe.sort(columns=['account'], inplace=True)
        # set the index to be this and don't drop
        dataframe.set_index(keys=['account'], drop=False, inplace=True)
        # get a list of names
        names = dataframe['account'].unique().tolist()

        for name in names:
            # now we can perform a lookup on a 'view' of the dataframe
            joe = dataframe.loc[dataframe['account'] == name]
            joe.sort(columns=['current_datetime'], inplace=True)
            f.append("/" + name, joe, data_columns=joe.columns)
        f.close()

    else:
        log.info("No orders to append ...")

if __name__=="__main__":
    run_get_orders()