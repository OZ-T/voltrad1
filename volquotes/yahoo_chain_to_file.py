import urllib
import time
import datetime

if __name__ == "__main__":
    url = 'https://ca.finance.yahoo.com/q/op?s=AAPL'

    systemTime = time.time()
    # print ("systemTime "+systemTime)

    timeStamp = datetime.datetime.fromtimestamp(systemTime).strftime('%Y-%m-%d-%H-%M-%S')
    print ("timeStamp = "+timeStamp)

    response = urllib.urlopen(url)
    webContent = response.read()


    f = open('C:/Temp/'+timeStamp +'.html', 'wb')
    f.write(webContent)
    f.close
