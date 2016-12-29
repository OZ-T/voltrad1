import volibutils.sync_client as ib
from volsetup import config
import datetime as dt
import pandas as pd
import numpy as np
import swigibpy as sy
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volsetup.logger import logger
from swigibpy import Contract as IBcontract
from swigibpy import Order as IBOrder
import time

def bs_resolve(x):
    if x<0:
        return 'SELL'
    if x>0:
        return 'BUY'
    if x==0:
        raise Exception("trying to trade with zero")

def place_spread_order(expiry,symbol,right,strike,orderType,quantity,lmtPrice):
    globalconf = config.GlobalConfig()
    log=logger("ib place order")
    log.info("placing order ")
    client = ib.IBClient(globalconf)
    client.connect()

    ibcontract = IBcontract()
    ibcontract.secType = "FOP"
    ibcontract.expiry=expiry
    ibcontract.symbol=symbol
    ibcontract.exchange="SMART"
    ibcontract.right=right
    ibcontract.strike=strike
    ibcontract.multiplier="100"
    ibcontract.currency="USD"

    iborder = IBOrder()
    iborder.action = bs_resolve(quantity)
    iborder.lmtPrice = lmtPrice
    iborder.orderType = orderType
    iborder.totalQuantity = abs(quantity)
    iborder.tif = 'DAY'
    iborder.transmit = True

    orderid1 = client.place_new_IB_order(ibcontract, iborder, orderid=None)
    print orderid1
    ## Short wait so dialog is in order, not neccessary
    time.sleep(5)

    order_structure = client.get_open_orders()

    print order_structure

if __name__=="__main__":
    place_spread_order("20170120","ES","C",2200.0,"MKT",1,0.0)
