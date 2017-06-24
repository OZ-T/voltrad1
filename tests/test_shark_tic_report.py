# coding: utf-8

from core import shark_tic_report as vd
import datetime as dt
from volsetup import config
from volsetup.logger import logger

globalconf = config.GlobalConfig()
log = logger("Test Sharks module")

if __name__ == "__main__":
    fecha_valoracion = dt.datetime(year=2016,month=12,day=30,hour=17,minute=15,second=0)
    accountid = globalconf.get_accountid()
    vd.run_analytics(symbol="ES", expiry="20170120", secType="FOP", accountid=accountid,
                    valuation_dt=fecha_valoracion,scenarioMode="N",simulName="NA",appendh5=1,
                    appendsql=0,toxls=0,timedelta1=1, log=log,globalconf=globalconf)

