from core.market_data_methods import get_last_bars_from_rt
import volsetup.config as config
from volsetup.logger import logger
import datetime
import numpy as np
globalconf = config.GlobalConfig()
log = logger("Testing ...")
last_date = datetime.datetime.today().strftime("%Y%m%d")
#df = get_last_bars_from_rt(globalconf=globalconf, log=log, symbol="ES", last_date=last_date,number_days_back=4)
import core.analytics_methods as am
df = am.coppock(globalconf=globalconf, log_analytics=log, last_date=last_date, symbol="SPY",period="1D")

print (df)


import core.portfolio_and_account_data_methods as ra
import datetime as dt
symbol = "SPY"
expiry = "20170317"
secType = "OPT"
import volsetup.config as config
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