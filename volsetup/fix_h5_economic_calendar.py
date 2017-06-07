# coding: utf-8

#27-nov-2016 Fix error:
#  ValueError: Trying to store a value with len [XX] in [CallOI??] column but
#  this column has a limit of [XXX]!

import glob
import fnmatch
import os
import sys
import volsetup.config as config
import pandas as pd
import datetime as dt
import numpy as np

# path = 'C:/Users/David/data/'
# path = '/home/david/data/'

globalconf = config.GlobalConfig()
path = globalconf.config['paths']['data_folder']

def run():
    os.chdir(path)
    optchain_orig = 'economic_calendar_db.h5'
    pattern_optchain = 'economic_calendar_db.h5*'
    optchain_out = 'economic_calendar_db_new.h5'
    lst1 = glob.glob(pattern_optchain)
    lst1.remove(optchain_orig)
    print (lst1)
    dataframe = pd.DataFrame()
    for x in lst1:
        store_in1 = pd.HDFStore(path + x,mode='w')
        root1 = store_in1.root
        print (root1._v_pathname)
        for lvl1 in root1:
            print (lvl1._v_pathname)
            if lvl1:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                print ("store_in1", len(df1), x)
        store_in1.close()


    store_in1 = pd.HDFStore(path + optchain_orig,mode='w')
    store_out = pd.HDFStore(path + optchain_out,mode='w')
    root1 = store_in1.root
    print (root1._v_pathname)
    for lvl1 in root1:
        print (lvl1._v_pathname)
        if lvl1:
            df1 = store_in1.select(lvl1._v_pathname)
            dataframe = dataframe.append(df1)
            print ("store_in1", len(df1), optchain_orig)
    store_in1.close()

    dataframe.sort_index(inplace=True,ascending=[True])
    names = dataframe['week'].apply(lambda x: x[:4]).unique().tolist()
    for name in names:
        print ("Storing " + name + " in ABT ..." + str(len(dataframe)))
        joe = dataframe[dataframe['week'].apply(lambda x: x[:4]) == name]
        joe=joe.sort_values(by=['week', 'Time_ET', 'load_dttm'])
        store_out.append("/" + name, joe, data_columns=True)
    store_out.close()

if __name__ == "__main__":
    run()

