import pandas as pd

import core.config as config
from core.logger import logger
import datetime

from persist.portfolio_and_account_data_methods import extrae_account_delta

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
vol = VolatilityEstimator(db_type="underl_ib_hist", symbol="SPY", expiry=None, last_date=last_date, num_days_back=200,
                          resample="1D", estimator="GarmanKlass", clean=True)
window = 30
windows = [30, 60, 90, 120]
quantiles = [0.25, 0.75]
bins = 100
normed = True
vol.cones_data(windows=windows, quantiles=quantiles)
from persist.sqlite_methods import read_lineplot_data_from_db
estimator="GarmanKlass"
dict = read_lineplot_data_from_db(symbol, last_date, estimator)


def testing_operaciones_bug():
    oper_series1=pd.DatetimeIndex(['2016-07-25 21:07:26+02:00','2016-08-15 19:08:26+02:00', '2016-08-15 19:19:08+02:00',
               '2016-08-15 20:32:21+02:00'])
    temp_margin = pd.DataFrame()
    temp_premium = pd.DataFrame()
    for x in oper_series1:
        temporal1 = extrae_account_delta(year=x.year, month=x.strftime("%B")[:3], day=x.day, hour=x.hour,minute=x.minute)
        temporal1 = temporal1.set_index('load_dttm')
        t_margin = temporal1[['RegTMargin_USD', 'MaintMarginReq_USD', 'InitMarginReq_USD',
                              'FullMaintMarginReq_USD', 'FullInitMarginReq_USD']].apply(pd.to_numeric)
        t_margin.diff()
        t_margin['fecha_operacion'] = str(x)
        temp_margin = temp_margin.append(t_margin)
        # cash primas y comisiones
        # impacto en comisiones de las operaciones
        # dado un datetime donde se han ejecutado operaciones hace la delta de las posiciones cash de la cuenta y eso
        # resulta en el valor de las comisiones de ese conjunto de operaciones (hay que considerar el dienro recibido de)
        # la venta de opciones y tambien si se tiene posiciones en divisa los cambios ????
        t_prem = temporal1[
            ['TotalCashBalance_BASE', 'TotalCashBalance_EUR', 'TotalCashBalance_USD', 'TotalCashValue_USD',
             'CashBalance_EUR', 'CashBalance_USD', 'TotalCashValue_C_USD', 'TotalCashValue_S_USD',
             'CashBalance_BASE', 'ExchangeRate_EUR']].apply(pd.to_numeric).diff()
        t_prem['fecha_operacion'] = str(x)
        temp_premium = temp_premium.append(t_prem)