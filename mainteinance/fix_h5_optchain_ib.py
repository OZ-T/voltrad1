# coding: utf-8

#27-nov-2016 Fix error:
#  ValueError: Trying to store a value with len [XX] in [CallOI??] column but
#  this column has a limit of [XXX]!

import glob
import os
import core.config as config
import pandas as pd
from datetime import datetime
from core.logger import logger


def run():
    """
    Used as command to consolidate in the main h5 anciliary h5 generated due to column length exceptions
    """
    globalconf = config.GlobalConfig()
    path = globalconf.config['paths']['data_folder']
    log = logger("fix_h5 optchain")
    os.chdir(path)
    if not os.path.exists(path + "/optchain_ib_backups"):
        os.makedirs(path + "/optchain_ib_backups")

    optchain_orig = 'optchain_ib_hist_db.h5'
    pattern_optchain = 'optchain_ib_hist_db.h5*'
    optchain_out = 'optchain_ib_hist_db_complete.h5'
    lst1 = glob.glob(pattern_optchain)
    lst1.remove(optchain_orig)
    if not lst1:
        log.info("No ancilliary files to append, exiting ... ")
        return
    log.info(("List of ancilliary files that will be appended: ", lst1))
    dataframe = pd.DataFrame()
    for x in lst1:
        store_in1 = pd.HDFStore(path + x)
        root1 = store_in1.root
        log.info(("Root pathname of the input store: ", root1._v_pathname))
        for lvl1 in root1:
            log.info(("Level 1 pathname in the root if the H5: ", lvl1._v_pathname))
            if lvl1:
                df1 = store_in1.select(lvl1._v_pathname)
                dataframe = dataframe.append(df1)
                log.info(("Store_in1", len(df1), x))
        store_in1.close()
        os.rename(path + x, path + "/optchain_ib_backups/" + x)


    store_in1 = pd.HDFStore(path + optchain_orig)
    store_out = pd.HDFStore(path + optchain_out)
    root1 = store_in1.root
    root2 = store_out.root
    log.info(("Root pathname of the input store: ", root1._v_pathname))
    log.info(("Root pathname of the output store: ", root2._v_pathname))
    for lvl1 in root1:
        print (lvl1._v_pathname)
        if lvl1:
            df1 = store_in1.select(lvl1._v_pathname)
            dataframe = dataframe.append(df1)
            log.info(("Store_in1 length and name", len(df1), optchain_orig))
    store_in1.close()
    os.rename(path + optchain_orig, path + "/optchain_ib_backups/" + datetime.now().strftime('%Y%m%d%H%M%S') + optchain_orig)

    dataframe.sort_index(inplace=True,ascending=[True])
    names = dataframe['symbol'].unique().tolist()
    for name in names:
        print ("Storing " + name + " in ABT ..." + str(len(dataframe)))
        joe = dataframe[dataframe.symbol == name]
        joe=joe.sort_values(by=['symbol', 'current_datetime', 'expiry', 'strike', 'right'])
        store_out.append("/" + name, joe, data_columns=True,min_itemsize={'comboLegsDescrip': 25})
    store_out.close()
    os.rename(path + optchain_out, path + optchain_orig)

if __name__ == "__main__":
    run()

