
import logging.handlers
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class GmailHandler():
    """ sends a gmail email with the logging message as the subject """
    def emit(self, subject,body):
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
    mail1.emit(subject="Test mail python mail",body="This is the body of the message")
