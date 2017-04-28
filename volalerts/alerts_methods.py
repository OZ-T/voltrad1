# -*- coding: UTF-8 -*-

""" Methods useful for sending alerts by mail and so on.
"""

import smtplib
import datetime
import logging.handlers
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def email(tradingSymbol, link):
    """
    Send an alert using email
    """
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

class GmailHandler():
    """ sends a gmail email with the logging message as the subject """
    def emit(self, record):
        """     Gmail handler emit method  """
        message = MIMEText('') # no body text
        message['Subject'] = record
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        addr = 'delucas.david@gmail.com'
        server.login("delucas.david@gmail.com", "duyqyphhpiodrsvr")
        server.sendmail(addr, [addr,], message.as_string())
        server.quit()

    def emit_w_body(self, subject,body):
        """     Gmail handler emit method  """
        addr = 'delucas.david@gmail.com'
        message =  MIMEMultipart()
        message['Subject'] = subject
        message['From'] = addr
        message['To'] = addr
        message.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login("delucas.david@gmail.com", "duyqyphhpiodrsvr")
        server.sendmail(addr, [addr,], message.as_string())
        server.quit()


if __name__ == "__main__":
	mail1= GmailHandler()
	mail1.emit(record="Test mail python mail")
    #mail1.emit_w_body(subject="Test mail python mail",body="This is the body of the message")
