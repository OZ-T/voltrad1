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
import datetime
import json
import logging
import requests
import locale
import persist.sqlite_methods as da
from core import misc_utilities as utils, config
from requests.exceptions import ContentDecodingError

'''
Yahoo! Economic Calendar scraper
'''

globalconf = config.GlobalConfig()
log = globalconf.get_logger()
BASE_URL = 'http://finance.yahoo.com/calendar/economic'

class YahooEconomicCalendar(object):
    """
    This is the class for fetching earnings data from Yahoo! Finance
    """
    def _get_data_dict(self, url):
        page_data_string = None
        try:
            page = requests.get(url)
            page_content = page.content
            page_data_string = [row for row in page_content.decode('utf8').split('\n') if row.startswith('root.App.main = ')][0][:-1]
            page_data_string = page_data_string.split('root.App.main = ', 1)[1]
        except ContentDecodingError:
            print("ContentDecodingError")
            pass

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
        log.info('Fetching earnings data for ' + date_str)
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


def first_run():
    date1 = dt.datetime(year=2018,month=3,day=1,hour=23,minute=55)
    end = dt.datetime.now()
    while date1 < end:
        run_reader(now1=date1)
        date1 = date1 + dt.timedelta(days=1)
        sleep(3)

def batch_run_reader():
    run_reader(now1=None)

def run_reader(now1 = None):
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    now = dt.datetime.now()  # + dt.timedelta(days=-4)
    if now1:
        now = now1
    weekday = now.strftime("%A").lower()
    log.info(("Getting data from yahoo ... ",now))
    yec = YahooEconomicCalendar()

    # Earnings data get updated slowly so to be sure we have the real actual value an not only the estimate
    # we should retrieve from previous days
    # we'll have duplicates in DB
    from datetime import timedelta
    from time import sleep
    for i in [0,5]:
        date_load = now - timedelta(days=i)
        dataframe = pd.DataFrame(yec.economic_on(date_load))
        da.write_ecocal_to_sqllite(dataframe)
        sleep(5)



if __name__ == '__main__':
    batch_run_reader()
    #first_run()

    #date_from = datetime.datetime.strptime(
    #    'Apr 1 2018  10:00AM', '%b %d %Y %I:%M%p')
    #date_to = datetime.datetime.strptime(
    #    'Apr 7 2018  1:00PM', '%b %d %Y %I:%M%p')
    #yec = YahooEconomicCalendar()
    #print (yec.economic_on(date_from))
    #print (yec.economic_between(date_from, date_to))







