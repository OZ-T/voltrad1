import datetime as dt
import unittest

import volsetup.config as config
from core.market_data_methods import get_market_db_file
from core.market_data_methods import read_market_data_from_sqllite
from volsetup.logger import logger


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
    def test_business_hours(self):
        import datetime as dt
        dt_now = dt.datetime.now()  # - timedelta(days=4)
        last_record_stored = dt.datetime.strptime("2017-06-22 22:59:00", '%Y-%m-%d %H:%M:%S')
        bh = utils.BusinessHours(last_record_stored, dt_now, worktiming=[15, 21], weekends=[6, 7])
        days = bh.getdays()
        print(days)
        self.assertEqual(100, 100)

    def test_volestimator(self):
        import core.vol_estimators as volest
        log = logger("Testing ...")
        globalconf = config.GlobalConfig()
        today =  dt.date.today()
        vol = volest.VolatilityEstimator(globalconf=globalconf,log=log,db_type="underl_ib_hist",symbol="AAPL",expiry=None,
                                         last_date=today.strftime('%Y%m%d'), num_days_back=200, resample="1D",estimator="GarmanKlass",clean=True)
        #fig, plt = vol.cones(windows=[30, 60, 90, 120], quantiles=[0.25, 0.75])
        #plt.show()

        window=30
        windows=[30, 60, 90, 120]
        quantiles=[0.25, 0.75]
        bins=100
        normed=True
        bench='SPY'
        # creates a pdf term sheet with all metrics
        vol.term_sheet_to_pdf(window, windows, quantiles, bins, normed, bench)
        # vol.term_sheet_to_png(window, windows, quantiles, bins, normed, bench)
        # vol.term_sheet_to_html(window, windows, quantiles, bins, normed, bench)
        self.assertEqual(100, 100)


if __name__ == "__main__":
    globalconf = config.GlobalConfig()
    from core import analytics_methods as am, utils

    #am.print_coppock_diario(symbol="SPX",period="1D")
    #p = am.graph_coppock(symbol="SPX",period="1D")
    #p = am.graph_emas(symbol="SPY")
    #p = am.graph_volatility(symbol="SPY")
    #p = am.graph_fast_move(symbol="SPY")

    #show(p)
    am.graph_volatility_cone(symbol='SPY')
    #print (md.get_datasources(globalconf))