"""@package docstring
"""

import logging.handlers
import smtplib
from email.mime.text import MIMEText

class GmailHandler():
	""" sends a gmail email with the logging message as the subject """
	def emit(self, record):
		message = MIMEText('') # no body text
		message['Subject'] = record
		server = smtplib.SMTP('smtp.gmail.com', 587)
		server.starttls()
		addr = 'delucas.david@gmail.com'
		server.login("delucas.david@gmail.com", "duyqyphhpiodrsvr")
		server.sendmail(addr, [addr,], message.as_string())
		server.quit()

if __name__ == "__main__":
	mail1= GmailHandler()
	mail1.emit(record="Test mail python mail")
