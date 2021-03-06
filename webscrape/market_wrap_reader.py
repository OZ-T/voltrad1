from bs4 import BeautifulSoup
import datetime as dt
from core.logger import logger
import persist.document_methods as da
from core import config
from textblob import TextBlob
import locale
from time import sleep
import core.misc_utilities as utils
from webscrape.utilities import download

log = logger("wrap download")


def first_run(now1 = None):
    date1 = dt.datetime(year=2019,month=6,day=5,hour=23,minute=55)
    end = dt.datetime.now()
    while date1 < end:
        run_reader(now1=date1)
        date1 = date1 + dt.timedelta(days=1)
        sleep(3)


def run_reader(now1 = None):
    """
    Run with "NA" string to get today's

    :param now1:
    :return:
    """
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    now = dt.datetime.now()  # + dt.timedelta(days=-4)
    mydate = utils.expiry_date(now1)
    if mydate:
        now = mydate
    weekday = now.strftime("%A").lower()
    log.info(("Getting data from www.itpm.com ... ",now))
    if (  weekday in ("saturday","sunday")  or
          now.date() in utils.get_trading_close_holidays(dt.datetime.now().year)):
        log.info("This is a US Calendar holiday or weekend. Ending process ... ")
        return

    base = "https://www.itpm.com/wraps_post/"
    month = now.strftime("%B").lower()  # Changed format update 27DEC2019 nov --> november
    #daymonth = now.strftime("%d").zfill(2)
    # Fix URL change format: without leading zeroes
    daymonth = str(now.day)
    year = now.strftime("%Y")
    wrapTypes = ("opening","closing")
    globalconf = config.GlobalConfig()
    # example https://www.itpm.com/wraps_post/closing-wrap-friday-nov-17-2017/
    # Changed format update 27DEC2019: https://www.itpm.com/wraps_post/closing-wrap-friday-december-20-2019/
    for wrapType in wrapTypes:
        url = base + wrapType + "-wrap-" + weekday + "-" + month + "-" + daymonth + "-" + year + "/"
        log.info ('[%s] Downloading: %s' % (str(now) , url ) )
        b_html= download(url=url, log=log,
                         user_agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
                         )
        if b_html is None:
            continue
        soup = BeautifulSoup(b_html, 'html.parser')
        fixed_html = soup.prettify()
        #print(fixed_html)
        #ul = soup.find_all('p', attrs={'class':'s2'})
        wrap = {
            'datetime': now,
            'type': wrapType
        }
        ul = soup.find_all('p')
        type(ul)
        #n =1
        for i in ul:
            #print((str(i.text),n))
            if "European Equity Markets" in str(i.text):
                wrap.update({"euro_equity_comment" : str(i.text)})
                wrap.update({"euro_equity_sentiment": get_sentiment(str(i.text)) })
            elif "Currency Markets" in str(i.text):
                wrap.update({"fx_comment" : str(i.text)})
                wrap.update({"fx_comment_sentiment": get_sentiment(str(i.text))})
            elif "Commodities Markets" in str(i.text):
                wrap.update({"commodities_comment" : str(i.text)})
                wrap.update({"commodities_sentiment": get_sentiment(str(i.text))})
            elif "US Equity Markets" in str(i.text):
                wrap.update({"us_equity_comment" : str(i.text)})
                wrap.update({"us_equity_sentiment": get_sentiment(str(i.text))})
            elif "Bond Markets" in str(i.text):
                wrap.update({"bonds_comment" : str(i.text)})
                wrap.update({"bonds_sentiment": get_sentiment(str(i.text))})
            #n=n+1
        #print(wrap)
        da.save_docs_to_db(globalconf, log, wrap, collection_name="itrm-wraps")

        # HV_10d_current = float(ul[7].text.strip('%')) / 100.0
        # HV_10d_1wk = float(ul[8].text.strip('%')) / 100.0
        # HV_10d_1mo = float(ul[9].text.strip('%')) / 100.0
        # HV_10d_52wk_hi = (ul[10].text)
        # HV_10d_52wk_lo = (ul[11].text)

def get_sentiment(statement):
    sentiment = TextBlob(statement)
    return { "polarity": sentiment.sentiment.polarity ,
             "subjectivity": sentiment.sentiment.subjectivity,
             "assessments": sentiment.sentiment_assessments
    }

if __name__=="__main__":
    #date1 = dt.datetime(year=2018, month=5, day=3, hour=23, minute=55)
    date1 = None
    #run_reader("20180501")
    run_reader(now1=date1)
    #first_run()




