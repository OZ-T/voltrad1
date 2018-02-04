"""

"""
import datetime as dt
import pandas as pd
from core import config
from datetime import datetime
from time import sleep
from core.logger import logger

def run_reader():
    globalconf = config.GlobalConfig()
    log = logger("yahoo biz eco calendar download")
    log.info("Getting calendar from yahoo ... ")

    wait_secs = 10
    now = dt.datetime.now()  # Get current time
    c_year = str(now.year)

    #f = globalconf.open_economic_calendar_h5_store()  # open database file
    load_dttm = now.strftime('%Y-%m-%d %H:%M:%S')
    # Esto es para la carga incremental inicial
    #weeks = ["%.2d" % i for i in range(35)]
    weeks = [ datetime.today().strftime("%W") ]

    dataframe = pd.DataFrame()

    for week in weeks:
        log.info ("yahoo economic calendar downloader [%s] week=[%s]" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),week))


        sleep(wait_secs)

    dataframe = dataframe.drop_duplicates()
    dataframe['load_dttm'] = load_dttm
    log.info("storing calendar in sqlite3 ...")


if __name__=="__main__":
    run_reader()


