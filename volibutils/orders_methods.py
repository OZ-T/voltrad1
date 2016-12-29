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

log=logger("order methods")
globalconf = config.GlobalConfig()
client = ib.IBClient(globalconf)

def bs_resolve(x):
    if x<0:
        return 'SELL'
    if x>0:
        return 'BUY'
    if x==0:
        raise Exception("trying to trade with zero")

def place_spread_order(expiry,symbol,right,strike,orderType,quantity,lmtPrice):
    log.info("placing order ")
    client.connect()
    ibcontract = IBcontract()
    ibcontract.secType = "FOP"
    ibcontract.expiry=expiry
    ibcontract.symbol=symbol
    ibcontract.exchange="GLOBEX"
    ibcontract.right=right
    ibcontract.strike=strike
    ibcontract.multiplier="50"
    ibcontract.currency="USD"

    iborder = IBOrder()
    iborder.action = bs_resolve(quantity)
    iborder.lmtPrice = lmtPrice
    iborder.orderType = orderType
    iborder.totalQuantity = abs(quantity)
    iborder.tif = 'DAY'
    iborder.transmit = True

    orderid1 = client.place_new_IB_order(ibcontract, iborder, orderid=None)
    log.info("orderid [%s] " % (str(orderid1)))

    client.disconnect()

def list_open_orders():
    log.info("list orders ")
    client.connect()
    order_structure = client.get_open_orders()
    log.info("order structure [%s]" % (str(order_structure)))
    client.disconnect()

def modify_open_order():
    pass

def cancel_open_order():
    pass

def cancel_all_open_orders():
    pass

def list_prices_before_trade():
    client.connect()
    ctrt = { 1000:RequestOptionData('ES','FOP','20170120',2200.0,'C','50','GLOBEX','USD',1000)}
    ctrt_prc = client.getMktData(ctrt)
    log.info("price [%s]" % (str(ctrt_prc)))
    client.disconnect()


if __name__=="__main__":
    place_spread_order("20170120","ES","C",2200.0,"LMT",2,5.0)
    list_open_orders()
    #list_prices_before_trade()
