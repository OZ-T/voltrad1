# coding: utf-8

import glob
import fnmatch
import os
import sys
import config as config
import pandas as pd
import datetime as dt
import numpy as np

# path = 'C:\Users\David\Dropbox\proyectos\data'
path = '/home/david/data/'

# http://stackoverflow.com/questions/2225564/get-a-filtered-list-of-files-in-a-directory


def run():
    os.chdir(path)
    orders_orig = 'orders_db.h5'
    pattern_orders = 'orders_db.h5*'
    orders_out = 'orders_db_new.h5'
    lst1 = glob.glob(pattern_orders)
    lst1.remove(orders_orig)
    print lst1

    for x in lst1:
        store_in1 = pd.HDFStore(path + x)
        store_out = pd.HDFStore(path + orders_out)
        root1 = store_in1.root
        print root1._v_pathname
        dataframe = pd.DataFrame()
        for lvl1 in root1:
            print lvl1._v_pathname
            if lvl1:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                print "store_in1", len(df1)
        store_in1.close()

        dataframe.sort_index(inplace=True,ascending=[True])

        names = dataframe['symbol'].unique().tolist()

        #for name in names:
        #    print ("Storing " + name + " in ABT ...")
        #    joe = dataframe[dataframe.symbol == name]
        #    joe=joe.sort_values(by=['symbol', 'current_datetime', 'expiry', 'strike', 'right'])
        #    store_out.append("/" + name, joe, data_columns=True)

        #store_out.close()


if __name__ == "__main__":
    run()
