from urllib.request import Request, urlopen
import urllib.error
from bs4 import BeautifulSoup
from volsetup import config
import datetime as dt
import numpy as np
import pandas as pd
from volsetup.logger import logger
import persist.data_access as da
from core import misc_utilities as utils
from textblob import TextBlob
import locale
from time import sleep

log = logger("wrap download")

def download(url,log, user_agent='wswp',  num_retries=2):
    try:
        q = Request(url)
        q.add_header('User-agent', user_agent)
        html = urlopen(q).read()
    except urllib.error.HTTPError as e:
        log.error( 'Download error: %s' % (str(e.reason)) )
        html = None
        if num_retries > 0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                # retry 5XX HTTP errors
                return download(url, log, user_agent, num_retries-1)
        else:
            return None
    return html

def first_run():
    date1 = dt.datetime(year=2017,month=3,day=22,hour=23,minute=55)
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
    log.info(("Getting data from www.itpm.com ... ",now))
    if (  weekday in ("saturday","sunday")  or
          now.date() in utils.get_trading_close_holidays(dt.datetime.now().year)):
        log.info("This is a US Calendar holiday or weekend. Ending process ... ")
        return

    base = "https://www.itpm.com/wraps_post/"
    month = now.strftime("%b").lower()
    daymonth = now.strftime("%d").zfill(2)
    year = now.strftime("%Y")
    wrapTypes = ("opening","closing")
    globalconf = config.GlobalConfig()
    # example https://www.itpm.com/wraps_post/closing-wrap-friday-nov-17-2017/
    for wrapType in wrapTypes:
        url = base + wrapType + "-wrap-" + weekday + "-" + month + "-" + daymonth + "-" + year + "/"
        log.info ('[%s] Downloading: %s' % (str(now) , url ) )
        b_html=download(url=url,log=log,
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
    run_reader()
    #first_run()




