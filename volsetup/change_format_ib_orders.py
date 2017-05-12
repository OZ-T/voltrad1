"""
Once time script for fixing orders Hdf5 file
"""
import sys
import config as config
import pandas as pd
import datetime as dt
import numpy as np

if __name__ == "__main__":
    sys.path.append('/home/david/python/voltrad1')
    #20-oct-2016 : pone el fichero ib orders h5 en formato con jerarquia de cuenta en lugar de
    #  jerarquia con la fecha
    globalconf = config.GlobalConfig()
    store = globalconf.open_orders_store()

    dataframe = pd.DataFrame()

    for lvl1 in store.get_node("/2016"):
        #print lvl1 #mes
        for lvl2 in store.get_node(lvl1._v_pathname):
            #print lvl2 # dia
            for lvl3 in store.get_node(lvl2._v_pathname):
                #print lvl3 # hora
                for lvl4 in store.get_node(lvl3._v_pathname):
                    print lvl4._v_pathname # minuto
                    if lvl4:
                        df1 = store.select(lvl4._v_pathname)
                        dataframe = dataframe.append(df1)
    store.close()
    # sort the dataframe
    dataframe=dataframe[dataframe.current_datetime.notnull()]

    dataframe.sort(columns=['account'], inplace=True)
    # set the index to be this and don't drop
    dataframe.set_index(keys=['account'], drop=False,inplace=True)
    # get a list of names
    names=dataframe['account'].unique().tolist()

    store_new = pd.HDFStore("/home/david/Dropbox/proyectos/data/" + "orders_db_new.h5")
    for name in names:
        # now we can perform a lookup on a 'view' of the dataframe
        joe = dataframe.loc[dataframe['account']==name]
        joe.sort(columns=['current_datetime'], inplace=True)
        store_new.append(    "/"+name , joe, data_columns=joe.columns)

    store_new.close()

