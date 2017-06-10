
#20-oct-2016 Fix error: 
#  ValueError: Trying to store a string with len [3] in [multiplier] column but
#  this column has a limit of [2]!
import sys
import volsetup.config as config
import pandas as pd
import datetime as dt
import numpy as np
#sys.path.append('/home/david/python/voltrad1')

globalconf = config.GlobalConfig()
path = globalconf.config['paths']['data_folder']


def run():
    store_in = globalconf.open_historical_optchain_store()
    store_out = pd.HDFStore(path + "historical_optchain_ib_db_new.h5")
    root = store_in.root
    print (root._v_pathname)
    #dataframe = pd.DataFrame()
    for lvl1 in root:
        print (lvl1._v_pathname)
        if lvl1:
            df1 = store_in.select(lvl1._v_pathname)
            symbols = df1['symbol'].unique().tolist()
            for symbol in symbols:
                joe = df1[df1.symbol == symbol]
                expiries = joe['expiry'].unique().tolist()
                for expiry in expiries:
                    joe2 = joe[joe.expiry == expiry]
                    rights = joe2['right'].unique().tolist()
                    for right in rights:
                        joe3 = joe2[joe2.right == right]
                        strikes = joe3['strike'].unique().tolist()
                        for strike in strikes:
                            joe4 = joe3[joe3.strike == strike]
                            joe4 = joe4.sort_values(by=['symbol', 'expiry', 'load_dttm'])
                            store_out.append("/" + symbol + "/" + expiry + "/" + right
                                             + "/" + strike , joe4, data_columns=True)

    store_in.close()
    store_out.close()


if __name__ == "__main__":
    run()
