"""
File to run automatically by cron each day to gather options data
IMPORTANTE
Ejecutar al principio de semana con los valores Actual de la tabla a missing (--)
y ejecutarlo otra vez al final de la semana con todos los resultados economicos de dicha semana
La existencia de estos valores duplicados debe manejarse en los programas analiticos subsiguientes

Entry en /etc/crontab
50 23 * * 1,2,3,4,5 python /home/david/python/voltrad1/volquotes/yahoo_biz_economic_calendar_us.py

"""
import datetime as dt
import pandas as pd
from core import config
from datetime import datetime
from time import sleep
from core.logger import logger
import requests
import bs4

# usar esta URL
# https://finance.yahoo.com/calendar/economic?from=2017-06-04&to=2017-06-10&day=2017-06-06
#


def run_reader():
    globalconf = config.GlobalConfig()
    log = logger("yahoo biz eco calendar download")
    log.info("Getting calendar from yahoo ... ")
    wait_secs = 10
    now = dt.datetime.now()  # Get current time
    c_year = str(now.year)
    #c_year = '2015'
    f = globalconf.open_economic_calendar_h5_store()  # open database file
    load_dttm = now.strftime('%Y-%m-%d %H:%M:%S')
    # Esto es para la carga incremental inicial
    #weeks = ["%.2d" % i for i in range(35)]
    weeks = [ datetime.today().strftime("%W") ]
    dataframe = pd.DataFrame()

    for week in weeks:
        log.info ("yahoo economic calendar downloader [%s] week=[%s]" % (dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),week))
        dataframe = dataframe.append(temp1)
        sleep(wait_secs)

    dataframe = dataframe.drop_duplicates()
    dataframe['load_dttm'] = load_dttm

# write_economic_to_sqllite(globalconf, log, dataframe)

if __name__=="__main__":
    run_reader()


'''
Yahoo! Economic Calendar scraper
'''
import datetime
import json
import logging
import requests

BASE_URL = 'http://finance.yahoo.com/calendar/economic'

# Logging config
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)


class YahooEconomicCalendar(object):
    """
    This is the class for fetching earnings data from Yahoo! Finance
    """

    def _get_data_dict(self, url):
        page = requests.get(url)
        page_content = page.content
        page_data_string = [row for row in page_content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]
        page_data_string = page_data_string.split('root.App.main = ', 1)[1]
        return json.loads(page_data_string)

    def economic_on(self, date):
        """Gets economic calendar data from Yahoo! on a specific date.
        Args:
            date: A datetime.date instance representing the date of economic data to be fetched.
        Returns:
            An array of economic calendar data on date given. E.g.,
            [
                {
                ...
                },
                ...
            ]
        Raises:
            TypeError: When date is not a datetime.date object.
        """
        if not isinstance(date, datetime.date):
            raise TypeError(
                'Date should be a datetime.date object')
        date_str = date.strftime('%Y-%m-%d')
        logger.debug('Fetching earnings data for %s', date_str)
        dated_url = '{0}?day={1}'.format(BASE_URL, date_str)
        page_data_dict = self._get_data_dict(dated_url)
        return page_data_dict['context']['dispatcher']['stores']['ScreenerResultsStore']['results']['rows']

    def economic_between(self, from_date, to_date):
        """Gets economic calendar data from Yahoo! in a date range.
        Args:
            from_date: A datetime.date instance representing the from-date (inclusive).
            to_date: A datetime.date instance representing the to-date (inclusive).
        Returns:
            An array of economic calendar data of date range. E.g.,
            [
                {
                 ...
                },
                ...
            ]
        Raises:
            ValueError: When from_date is after to_date.
            TypeError: When either from_date or to_date is not a datetime.date object.
        """
        if from_date > to_date:
            raise ValueError(
                'From-date should not be after to-date')
        if not (isinstance(from_date, datetime.date) and
                isinstance(to_date, datetime.date)):
            raise TypeError(
                'From-date and to-date should be datetime.date objects')
        economic_data = []
        current_date = from_date
        delta = datetime.timedelta(days=1)
        while current_date <= to_date:
            economic_data += self.economic_on(current_date)
            current_date += delta
        return economic_data


if __name__ == '__main__':
    date_from = datetime.datetime.strptime(
        'May 5 2017  10:00AM', '%b %d %Y %I:%M%p')
    date_to = datetime.datetime.strptime(
        'May 8 2017  1:00PM', '%b %d %Y %I:%M%p')
    yec = YahooEconomicCalendar()
    print yec.economic_on(date_from)
    print yec.economic_between(date_from, date_to)

