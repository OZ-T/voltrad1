from operations.market_data import get_market_db_file
import volsetup.config as config
from volsetup.logger import logger
from operations.market_data import read_market_data_from_sqllite

import unittest

# http://python-guide-pt-br.readthedocs.io/en/latest/writing/tests/

class MarketData_tests(unittest.TestCase):
    def test_get_market_db_file(self):
        globalconf = config.GlobalConfig()
        db_type = "optchain_ib"
        expiry = "222222"
        db_file = get_market_db_file(globalconf, db_type, expiry)
        print(db_file)
        self.assertEqual(db_file, "KKKK")
    def test_market_data_from_sqllite(self):
        log = logger("Testing ...")
        globalconf = config.GlobalConfig()
        df = read_market_data_from_sqllite(globalconf, log, db_type="underl_ib_hist", symbol="USO",
                                           expiry=None, last_date="20170623", num_days_back=50, resample="1D")
        print(df)
        self.assertEqual(len(df), 100)

import core.vol_estimators as volest
if __name__ == "__main__":
    log = logger("Testing ...")
    globalconf = config.GlobalConfig()
    vol = volest.VolatilityEstimator(globalconf=globalconf,log=log,db_type="underl_ib_hist",symbol="USO",expiry="",
                                     last_date="20170623", num_days_back=200, resample="1D",estimator="GarmanKlass",clean=True)

    #fig, plt = vol.cones(windows=[30, 60, 90, 120], quantiles=[0.25, 0.75])
    #plt.show()

    window=30
    windows=[30, 60, 90, 120]
    quantiles=[0.25, 0.75]
    bins=100
    normed=True
    bench='SPY'
    # creates a pdf term sheet with all metrics
    # vol.term_sheet_to_pdf(window, windows, quantiles, bins, normed, bench)
    # vol.term_sheet_to_png(window, windows, quantiles, bins, normed, bench)
    vol.term_sheet_to_html(window, windows, quantiles, bins, normed, bench)