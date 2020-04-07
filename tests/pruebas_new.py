#%%
import pandas as pd
import core.config as config
from core.logger import logger
import datetime
globalconf = config.GlobalConfig()
log = logger("Testing ...")
last_date = datetime.datetime.today().strftime("%Y%m%d")
import persist.sqlite_methods as psm

import os
try:
	os.chdir(os.path.join(os.getcwd(), 'test'))
	print(os.getcwd())
except:
	pass
from IPython import get_ipython

get_ipython().magic('matplotlib inline')
import sys
sys.path.append("../")


#%%
df = psm.get_yahoo_option_dataframe("SPY","2018-06","2018-06","P")

df