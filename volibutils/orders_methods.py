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

def place_plain_order(client,expiry,symbol,right,strike,orderType,quantity,lmtPrice):
    log.info("placing order ")
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


def place_spread_order(client,expiry,symbol,right,strike,orderType,quantity,lmtPrice):
    log.info("placing order ")

    leg1 = sy.ComboLeg()
    leg2 = sy.ComboLeg()
    #sy.Contract.comboLegs

    #Contract   contract = new     Contract();
    #contract.Symbol = "DBK";
    #contract.SecType = "BAG";
    #contract.Currency = "EUR";
    #contract.Exchange = "DTB";
    #ComboLeg    leg1 = new    ComboLeg();
    #leg1.ConId = 197397509; // DBK    JUN    15    '18 C
    #leg1.Ratio = 1;
    #leg1.Action = "BUY";
    #leg1.Exchange = "DTB";
    #ComboLeg    leg2 = new    ComboLeg();
    #leg2.ConId = 197397584; // DBK    JUN    15    '18 P
    #leg2.Ratio = 1;
    #leg2.Action = "SELL";
    #leg2.Exchange = "DTB";
    #contract.ComboLegs = new    List < ComboLeg > ();
    #contract.ComboLegs.Add(leg1);
    #contract.ComboLegs.Add(leg2);

    ibcontract = IBcontract()
    ibcontract.comboLegs = sy.ComboLegList([leg1, leg2])

    ibcontract.symbol = "EOE"  # abitrary value only combo orders
    ibcontract.secType = "BAG"  # BAG is the security type for COMBO order

    ibcontract.exchange="GLOBEX"
    ibcontract.right=right
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


def list_open_orders(client):
    log.info("list orders ")

    order_structure = client.get_open_orders()
    log.info("order structure [%s]" % (str(order_structure)))

def modify_open_order(client):
    # http://interactivebrokers.github.io/tws-api/modifying_orders.html#gsc.tab=0
    """
    Modification of an open order through the API can be achieved by the same client which placed the original order.
    In the case of orders placed manually in TWS, the order can be modified by the client with ID 0.

    To modify an order, simply call the IBApi.EClient.placeOrder function again with the same parameters used to place
    the original order, except for the changed parameter. This includes the IBApi.Order.OrderId, which must match the
    IBApi.Order.OrderId of the original. It is not generally recommended to try to change order parameters other than
    the order price and order size. To change other parameters, it might be preferable to cancel the original order
    and place a new order.

    """

    pass

def cancel_open_order(client):
    pass

def cancel_all_open_orders(client):
    pass

def list_prices_before_trade(client):
    ctrt = { 1000:RequestOptionData('ES','FOP','20170120',2200.0,'C','50','GLOBEX','USD',1000)}
    ctrt_prc = client.getMktData(ctrt)
    log.info("price [%s]" % (str(ctrt_prc)))

if __name__=="__main__":
    clientid1 = int(globalconf.config['ib_api']['clientid_orders'])
    client.connect(clientid1=clientid1)

    place_plain_order(client=client,expiry="20170120",symbol="ES",right="C",strike=2200.0,orderType="LMT",quantity=2,lmtPrice=5.0)
    list_open_orders(client=client)
    list_prices_before_trade(client=client)

    client.disconnect()
