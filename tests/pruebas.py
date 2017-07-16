from core.market_data_methods import get_last_bars_from_rt
import volsetup.config as config
from volsetup.logger import logger
import datetime
import numpy as np
globalconf = config.GlobalConfig()
log = logger("Testing ...")
last_date = datetime.datetime.today().strftime("%Y%m%d")
df = get_last_bars_from_rt(globalconf=globalconf, log=log, symbol="ES", last_date=last_date,number_days_back=4)

df = df[abs(df.strike - df.modelUndPrice) == np.min(abs(df.strike - df.modelUndPrice)) ]
df = df[['modelImpliedVol','modelUndPrice','lastImpliedVol','lastUndPrice','askImpliedVol','askUndPrice','bidImpliedVol','bidUndPrice','current_datetime']]

print (df)