
#27-nov-2016 Fix error:
#  ValueError: Trying to store a value with len [XX] in [CallOI??] column but
#  this column has a limit of [XXX]!
import sys
import config as config
import pandas as pd
import datetime as dt
import numpy as np

store_in1 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db_full.h5')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161201215146')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161201215147')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161208175213')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161208175215')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161212215218')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161212215220')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161213205229')
#store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161213215230')
store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h520161213215234')
#
#
#

store_in1 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db_full2.h5')
store_in2 = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + 'optchain_ib_hist_db.h5')

store_out = pd.HDFStore("C:/Users/David/Dropbox/proyectos/data/" + "optchain_ib_hist_db_full2.h5")
root1 = store_in1.root
print root1._v_pathname
dataframe = pd.DataFrame()
for lvl1 in root1:
    print lvl1._v_pathname
    if lvl1:
        df1 = store_in1.select(lvl1._v_pathname)
        dataframe = dataframe.append(df1)
        print "store_in1" , len(df1)
store_in1.close()

root2 = store_in2.root
print root2._v_pathname
for lvl1 in root2:
    print lvl1._v_pathname
    if lvl1:
        df1 = store_in2.select(lvl1._v_pathname)
        dataframe = dataframe.append(df1)
        print "store_in2" , len(df1)
store_in2.close()

dataframe.sort_index(inplace=True,ascending=[True])

names = dataframe['symbol'].unique().tolist()

#for name in names:
#    print ("Storing " + name + " in ABT ...")
#    joe = dataframe[dataframe.symbol == name]
#    joe=joe.sort_values(by=['symbol', 'current_datetime', 'expiry', 'strike', 'right'])
#    store_out.append("/" + name, joe, data_columns=True)

#store_out.close()


