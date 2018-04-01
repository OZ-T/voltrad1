from core import config
from ibutils.sync_client import IBClient
import pandas as pd
#sys.path.append("/home/david/python")
#sys.path.insert(0, os.path.abspath('..'))
#print(sys.path)


if __name__=="__main__":
	
    """
    This simple example returns the time
    """
    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_data'])
    client.connect(clientid1=clientid1)
    dataframe = pd.DataFrame({'ibtime': client.getTime()}, index=[0])
    print (dataframe)
    client.disconnect()





