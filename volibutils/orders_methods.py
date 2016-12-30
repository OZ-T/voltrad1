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

globalconf = config.GlobalConfig(level=logger.ERROR)
log = globalconf.log
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


def place_or_modif_spread_order(client,expiry,symbol,right,strike_l,strike_s,orderType,quantity,lmtPrice,orderId):
    """
    Place new order or modify existing order

    :param client:
    :param expiry:
    :param symbol:
    :param right:
    :param strike_l:
    :param strike_s:
    :param orderType:
    :param quantity:
    :param lmtPrice:
    :return:
    """
    log.info("placing order ")

    underl = {
            1001:RequestOptionData(symbol,'FOP',expiry,strike_l,right,'50','GLOBEX','USD',1001),
            1002: RequestOptionData(symbol, 'FOP',expiry, strike_s, right, '50', 'GLOBEX', 'USD', 1002)
    }
    action1 = {1001:"BUY",1002:"SELL"}
    list_results = client.getOptionsChain(underl)
    legs = []
    log.info("Number of requests [%d]" % (len(list_results)) )
    for reqId, request in list_results.iteritems():
        log.info ("Requestid [%d]: Option[%s] Results [%d]" % ( reqId , str(request.get_in_data()), len(request.optionsChain) ))
        for opt1 in request.optionsChain:
            leg1 = sy.ComboLeg()
            leg1.conId = opt1['conId']
            leg1.ratio = 1
            leg1.action = action1[reqId]
            leg1.exchange = opt1['exchange']
            legs.append(leg1)

    #sy.Contract.comboLegs

    ibcontract = IBcontract()
    ibcontract.comboLegs = sy.ComboLegList(legs)

    ibcontract.symbol = symbol
    ibcontract.secType = "BAG"  # BAG is the security type for COMBO order
    ibcontract.exchange="GLOBEX"
    ibcontract.currency="USD"

    iborder = IBOrder()
    iborder.action = bs_resolve(quantity)
    iborder.lmtPrice = lmtPrice
    iborder.orderType = orderType
    iborder.totalQuantity = abs(quantity)
    iborder.tif = 'GTC'
    iborder.transmit = True

    orderid1 = client.place_new_IB_order(ibcontract, iborder, orderid=orderId)

    print("orderid [%s] " % (str(orderid1)))


def list_open_orders(client):
    log.info("list orders ")

    order_structure = client.get_open_orders()
    for idx, x in order_structure.iteritems():
        print("[%s]" % (str(x)))

def modify_open_order(client,orderId):
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

def list_prices_before_trade(client,symbol,expiry,query):
    ctrt = {}
    for idx, x in enumerate(query):
        ctrt[idx] = RequestOptionData(symbol,'FOP',expiry,float(x[1:]),x[:1],'50','GLOBEX','USD',idx)

    ctrt_prc = client.getMktData(ctrt)
    #log.info("[%s]" % (str(ctrt_prc)))
    for id, req1 in ctrt_prc.iteritems():
        subset_dic = {k: req1.get_in_data()[k] for k in ('strike', 'right', 'expiry','symbol')}
        subset_dic2 = {k: req1.get_out_data()[id][k] for k in ('bidPrice', 'bidSize', 'askPrice', 'askSize') }
        print subset_dic,subset_dic2

def list_spread_prices_before_trade(client,symbol,expiry,query):
    underl = {}

    for idx, x in enumerate(query):
        underl[idx] = RequestOptionData(symbol,'FOP',expiry,float(x[1:]),x[:1],'50','GLOBEX','USD',idx,comboLegs=None)
    action1 = {0:"BUY",1:"SELL"}
    list_results = client.getOptionsChain(underl)
    legs = []
    log.info("Number of requests [%d]" % (len(list_results)) )
    for reqId, request in list_results.iteritems():
        log.info ("Requestid [%d]: Option[%s] Results [%d]" % ( reqId , str(request.get_in_data()), len(request.optionsChain) ))
        for opt1 in request.optionsChain:
            leg1 = sy.ComboLeg()
            leg1.conId = opt1['conId']
            leg1.ratio = 1
            leg1.action = action1[reqId]
            leg1.exchange = opt1['exchange']
            legs.append(leg1)

    ibcontract = IBcontract()
    ibcontract.comboLegs = sy.ComboLegList(legs)
    ibcontract.symbol = symbol
    ibcontract.secType = "BAG"  # BAG is the security type for COMBO order
    ibcontract.exchange="GLOBEX"
    ibcontract.currency="USD"

    ctrt = {}
    ctrt[100] = RequestOptionData(symbol,'FOP',expiry,float(x[1:]),x[:1],'50','GLOBEX','USD',100,comboLegs=None,contract=ibcontract)


    ctrt_prc = client.getMktData(ctrt)
    #log.info("[%s]" % (str(ctrt_prc)))
    for id, req1 in ctrt_prc.iteritems():
        subset_dic = {k: req1.get_in_data()[k] for k in ('secType','symbol')}
        subset_dic2 = {k: req1.get_out_data()[id][k] for k in ('bidPrice', 'bidSize', 'askPrice', 'askSize') }
        print subset_dic,subset_dic2



if __name__=="__main__":
    clientid1 = int(globalconf.config['ib_api']['clientid_orders'])
    client.connect(clientid1=clientid1)
    #list_prices_before_trade(client=client,symbol="ES",expiry="20170120",query=['C2300.0','C2350.0','P2100.0','P2150.0'])
    list_spread_prices_before_trade(client=client,symbol="ES",expiry="20170120",query=['C2300.0','C2350.0'])
    #place_plain_order(client=client,expiry="20170120",symbol="ES",right="C",strike=2200.0,orderType="LMT",quantity=2,lmtPrice=5.0)
    place_or_modif_spread_order(client=client,expiry="20170120",symbol="ES",right="C",strike_l=2300.0,
                       strike_s=2350.0,orderType="LMT",quantity=-1,lmtPrice=3.7,orderId=None)
    list_open_orders(client=client)

    client.disconnect()
