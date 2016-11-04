
#20-oct-2016 Fix error: 
#  ValueError: Trying to store a string with len [3] in [multiplier] column but
#  this column has a limit of [2]!
import sys
import config as config
import pandas as pd
import datetime as dt
import numpy as np
sys.path.append('/home/david/python/voltrad1')

globalconf = config.GlobalConfig()
store_in = globalconf.open_historical_optchain_store()
store_out = pd.HDFStore("/home/david/Dropbox/proyectos/data/" + "historical_optchain_ib_db_new.h5")
root = store_in.root
print root._v_pathname
#dataframe = pd.DataFrame()
for lvl1 in root:
    print lvl1._v_pathname 
    if lvl1:
        df1 = store_in.select(lvl1._v_pathname)
        #dataframe = dataframe.append(df1)
        store_out.append( lvl1._v_pathname , df1, data_columns=True, min_itemsize={'multiplier': 5})
store_in.close()
store_out.close()


