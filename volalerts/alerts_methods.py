# -*- coding: UTF-8 -*-

"""@package docstring
"""
import smtplib
import datetime


def email(tradingSymbol, link):
    today = datetime.datetime.today()
    today = today.strftime('%m/%d/%Y %I:%M %p')
    smtpObj = smtplib.SMTP('smtp.gmail.com', 587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login('******************@gmail.com', '***************')
    print(smtpObj.sendmail('******************@gmail.com',\
                     '******************@gmail.com',\
                     'Subject: ' + str(today) + ' | Stock order: ' + str(tradingSymbol) + '.\nBuy this stock and heres the address + ' + str(link) + '\n'))
    smtpObj.quit()

