import sys
import os
import datetime
from volsetup import config
from volibutils import IBClient
#sys.path.append("/home/david/python")
#sys.path.insert(0, os.path.abspath('..'))
#print(sys.path)


if __name__=="__main__":
	
    """
    This simple example returns the time
    """
    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)

    print ("Local datetime [%s] IB datetime [%d]" % (datetime.datetime.now(),client.speaking_clock()))
