import core.config as config
from core.logger import logger
import datetime

globalconf = config.GlobalConfig()
log = logger("Testing ...")
last_date = datetime.datetime.today().strftime("%Y%m%d")
#df = get_last_bars_from_rt(globalconf=globalconf, log=log, symbol="ES", last_date=last_date,number_days_back=4)
#import core.analytics_methods as am
#df = am.coppock(globalconf=globalconf, log_analytics=log, last_date=last_date, symbol="SPY",period="1D")

#print (df)


import datetime as dt
symbol = "SPY"
expiry = "20170317"
secType = "OPT"
import core.config as config
globalconf = config.GlobalConfig()
accountid = globalconf.get_accountid()
fecha_valoracion = dt.datetime.now()  # dt.datetime(year=2017,month=2,day=2) #
scenarioMode = "N"
simulName = "spy0317dls"
appendh5 = 1
appendpgsql = 0
toxls = 0
timedelta1 = 1
#posiciones = ra.extrae_portfolio_positions(valuation_dttm=fecha_valoracion,
#                                           symbol=symbol, expiry=expiry, secType=secType,
#                                           accountid=accountid,
#                                           scenarioMode=scenarioMode, simulName=simulName)
#print (posiciones)


#am.print_summary_underl(symbol="SPY")

val_dt = "20170827"
symbol = "SPY"
expiry = "20171020"
call_d_range = "10,15"
put_d_range = "-15,-10"
type = "bid"
#print_chain(val_dt,symbol,call_d_range,put_d_range,expiry,type)

valuation_dt = "2017-08-25-17"
# print_account_delta(valuation_dt=valuation_dt)

#print_account_snapshot(valuation_dt=valuation_dt)


from core.vol_estimators import VolatilityEstimator
from core import config
from core.logger import logger
log = logger("some testing ...")
globalconf = config.GlobalConfig()
last_date = "20170706"
vol = VolatilityEstimator(globalconf=globalconf, log=log, db_type="underl_ib_hist", symbol="SPY", expiry=None,
                          last_date=last_date, num_days_back=200, resample="1D", estimator="GarmanKlass", clean=True)
window = 30
windows = [30, 60, 90, 120]
quantiles = [0.25, 0.75]
bins = 100
normed = True
vol.cones_data(windows=windows, quantiles=quantiles)
from persist.sqlite_methods import read_lineplot_data_from_db
estimator="GarmanKlass"
dict = read_lineplot_data_from_db(globalconf,log,symbol, last_date, estimator)
