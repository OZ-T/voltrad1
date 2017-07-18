"""
 Utility classes and methods.
"""

import math
import datetime
import datetime as dt
import time

from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
    USLaborDay, USThanksgivingDay

from collections import defaultdict

from core.portfolio_and_account_data_methods import log




def timefunc(f):
    def f_timer(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        log.info( ' __TIMER__ ' + str(f.__name__) + ' took ' + str( end - start ) + ' time')
        return result
    return f_timer

def make_dict():
    """
    returns a nested dictionary of arbitrary depth
    Usage:
        d=defaultdict(make_dict)
        d["food"]["meat"]="beef"
        d["food"]["veggie"]="corn"
        d["food"]["sweets"]="ice cream"
        d["animal"]["pet"]["dog"]="collie"
        d["animal"]["pet"]["cat"]="tabby"
        d["animal"]["farm animal"]="chicken"

    """

    return defaultdict(make_dict)


def iter_all(d,depth=1):
    """
    Iterate through everything in NESTED DICTIONARY
    """
    for k,v in d.items():
        print (("-"*depth,k))
        if type(v) is defaultdict:
            iter_all(v,depth+1)
        else:
            print (("-"*(depth+1),v))

def dictify(d):
    """
    Convert NESTED DICT to normal dict
    """
    for k,v in d.items():
        if isinstance(v,defaultdict):
            d[k] = dictify(v)
    return dict(d)



def expiry_date(expiry_ident):
    """
    Translates an expiry date which could be "20150305" or "201505" into a datetime
    :param expiry_ident: Expiry to be processed
    :type days: str or datetime.datetime
    :returns: datetime.datetime or datetime.date
    >>> expiry_date('201503')
    datetime.datetime(2015, 3, 1, 0, 0)
    >>> expiry_date('20150305')
    datetime.datetime(2015, 3, 5, 0, 0)
    >>> expiry_date(datetime.datetime(2015,5,1))
    datetime.datetime(2015, 5, 1, 0, 0)
    """

    if isinstance(expiry_ident, str):
        # do string expiry calc
        if len(expiry_ident) == 6:
            expiry_date = datetime.datetime.strptime(expiry_ident, "%Y%m")
        elif len(expiry_ident) == 8:
            expiry_date = datetime.datetime.strptime(expiry_ident, "%Y%m%d")
        else:
            raise Exception("")

    elif isinstance(expiry_ident, datetime.datetime) or isinstance(
            expiry_ident, datetime.date):
        expiry_date = expiry_ident

    else:
        raise Exception(
            "expiry_date needs to be a string with 4 or 6 digits, ")

    # 'Natural' form is datetime
    return expiry_date




class USTradingCalendar(AbstractHolidayCalendar):
    """
    !@brief This class is representing a US Calendar standard includes holidays.
    
     @param self
    """
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]


def get_trading_close_holidays(year):
    """
    Returns a curve with holidays in the year passed as argument
    :param year: 
    :return: 
    """
    inst = USTradingCalendar()

    return inst.holidays(dt.datetime(year-1, 12, 31), dt.datetime(year, 12, 31))



class BusinessHours:
    """
    !@brief This class used for calculating business hours between two datetimes
    @param self
    """
    def __init__(self, datetime1, datetime2, worktiming=[9, 17],weekends=[6, 7]):
        self.weekends = weekends
        self.worktiming = worktiming
        self.datetime1 = datetime1
        self.datetime2 = datetime2
        self.day_minutes = (self.worktiming[1]-self.worktiming[0])*3600

    def getdays(self):
        """
        Return the difference in days.
        """
        days = (self.datetime2-self.datetime1).days
        # exclude any day in the week marked as holiday (ex: saturday , sunday)
        # use integer division // required by Python 3
        noofweeks = days // 7
        extradays = days % 7
        startday = self.datetime1.isoweekday()
        days = days - (noofweeks * self.weekends.__len__())
        for weekend in self.weekends:
            if(startday == weekend):
                days = days - 1
            else:
                if(weekend >= startday):
                    if(startday+extradays >= weekend):
                        days = days - 1
                else:
                    if(7-startday+extradays >= weekend):
                        days = days - 1
        return days

    def gethours(self):
        return int(self.getminutes() // 60)

    def getminutes(self):
        """
        Return the difference in minutes.
        """
        # Set initial default variables
        dt_start = self.datetime1  # datetime of start
        dt_end = self.datetime2    # datetime of end
        worktime = 0               # remaining minutes after full days

        if dt_start.date() == dt_end.date():
            # starts and ends on same workday
            full_days = 0
            if self.is_weekend(dt_start):
                return 0
            else:
                if dt_start.hour < self.worktiming[0]:
                    # set start time to opening hour
                    dt_start = datetime.datetime(
                        year=dt_start.year,
                        month=dt_start.month,
                        day=dt_start.day,
                        hour=self.worktiming[0],
                        minute=0)
                if dt_start.hour >= self.worktiming[1] or \
                        dt_end.hour < self.worktiming[0]:
                    return 0
                if dt_end.hour >= self.worktiming[1]:
                    dt_end = datetime.datetime(
                        year=dt_end.year,
                        month=dt_end.month,
                        day=dt_end.day,
                        hour=self.worktiming[1],
                        minute=0)
                worktime = (dt_end-dt_start).total_seconds()
        elif (dt_end-dt_start).days < 0:
            # ends before start
            return 0
        else:
            current_day = dt_start  # marker for counting workdays
            while not current_day.date() == dt_end.date():
                if not self.is_weekend(current_day):
                    if current_day == dt_start:
                        # increment hours of first day
                        if current_day.hour < self.worktiming[0]:
                            worktime += self.day_minutes  # add 1 day
                        elif current_day.hour >= self.worktiming[1]:
                            pass  # no time on first day
                        else:
                            dt_currentday_close = datetime.datetime(
                                year=dt_start.year,
                                month=dt_start.month,
                                day=dt_start.day,
                                hour=self.worktiming[1],
                                minute=0)
                            worktime += (dt_currentday_close
                                         - dt_start).total_seconds()
                    else:
                        # increment one full day
                        worktime += self.day_minutes
                current_day += datetime.timedelta(days=1)  # next day
            # Time on the last day
            if not self.is_weekend(dt_end):
                if dt_end.hour >= self.worktiming[1]:  # finish after close
                    # Add a full day
                    worktime += self.day_minutes
                elif dt_end.hour < self.worktiming[0]:  # close before opening
                    pass  # no time added
                else:
                    # Add time since opening
                    dt_end_open = datetime.datetime(
                        year=dt_end.year,
                        month=dt_end.month,
                        day=dt_end.day,
                        hour=self.worktiming[0],
                        minute=0)
                    worktime += (dt_end-dt_end_open).total_seconds()
        return int(worktime // 60)

    def is_weekend(self, datetime):
        """
        Returns True if datetime lands on a weekend.
        """
        for weekend in self.weekends:
            if datetime.isoweekday() == weekend:
                return True
        return False


if __name__=="__main__kk":
    a=datetime.datetime(year=2016, month=9, day=19,hour=22,minute=0)
    b = datetime.datetime(year=2016, month=9, day=23, hour=4,minute=20)

    bh=BusinessHours(a,b,worktiming=[15,21],weekends=[6,7])
    print (bh.gethours())
    print (bh.getdays())



    #c= int(math.ceil(  office_time_between(a,b,start = timedelta(hours = 15),stop = timedelta(hours = 21)).total_seconds() / 60.0 / 60.0 / 7.0 ))
    #print str(a),str(b),c,"ffffff",office_time_between(a,b,start = timedelta(hours = 15),stop = timedelta(hours = 21))


if __name__ == '__main__':
    print(get_trading_close_holidays(2017))
    #    DatetimeIndex(['2016-01-01', '2016-01-18', '2016-02-15', '2016-03-25',
    #                   '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24',
    #                   '2016-12-26'],
    #                  dtype='datetime64[ns]', freq=None)
    print (dt.datetime.now().date() + dt.timedelta(days=1))

    if (dt.datetime.now().date()+ dt.timedelta(days=1)) in get_trading_close_holidays(dt.datetime.now().year):
        print ("Holidayyyyy!!!!")


