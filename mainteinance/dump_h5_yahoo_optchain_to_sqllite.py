# coding: utf-8

#27-nov-2016 Fix error:
#  ValueError: Trying to store a value with len [XX] in [CallOI??] column but
#  this column has a limit of [XXX]!

import glob
import fnmatch
import os
import sys
import config as config
import pandas as pd
import datetime as dt
import numpy as np

# path = 'C:/Users/David/data/'
# path = '/home/david/data/'

globalconf = config.GlobalConfig()
path = globalconf.config['paths']['data_folder']

def run():
    os.chdir(path)
    optchain_orig = 'optchain_yahoo_db_expiry_2018-03.h5'
    pattern_optchain = 'optchain_yahoo_db_expiry_2018-03.h5*'
    optchain_out = 'optchain_yahoo_db_expiry_2018-03.db'
    lst1 = glob.glob(pattern_optchain)
    lst1.remove(optchain_orig)
    print( lst1)
    dataframe = pd.DataFrame()
    for x in lst1:
        store_in1 = pd.HDFStore(path + x)
        root1 = store_in1.root
        print (root1._v_pathname)
        for lvl1 in root1:
            print (lvl1._v_pathname)
            if lvl1:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                print ("store_in1", len(df1), x)
        store_in1.close()

    store_in1 = pd.HDFStore(path + optchain_orig)

    root1 = store_in1.root
    print (root1._v_pathname)
    for lvl1 in root1:
        print (lvl1._v_pathname)
        if lvl1:
            df1 = store_in1.select(lvl1._v_pathname)
            dataframe = dataframe.append(df1)
            print ("store_in1", len(df1), optchain_orig)
    store_in1.close()

    store_out = globalconf.connect_sqllite(path + optchain_out)

    #dataframe = dataframe.reset_index().set_index("Quote_Time")
    #dataframe.sort_index(inplace=True,ascending=[True])
    names = dataframe['Underlying'].unique().tolist()
    for name in names:
        joe = dataframe[dataframe.Underlying == name]
        print ("Storing " + name + " in ABT ..." + str(len(joe)))
        joe=joe.sort_values(by=['Symbol','Quote_Time_txt'])
        joe.to_sql(name,store_out,if_exists='append')
    store_out.close()

if __name__ == "__main__":
    run()