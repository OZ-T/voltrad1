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

def init_func():
    globalconf = config.GlobalConfig(level=logger.DEBUG)
    log = globalconf.log
    client = ib.IBClient(globalconf)
    clientid1 = int(globalconf.config['ib_api']['clientid_orders'])
    client.connect(clientid1=clientid1)

    return client , log

def end_func(client):
    client.disconnect()

def bs_resolve(x):
    if x<0:
        return 'SELL'
    if x>0:
        return 'BUY'
    if x==0:
        raise Exception("trying to trade with zero")

def place_plain_order(expiry,symbol,right,strike,orderType,quantity,lmtPrice):
    """
    Place a sinlge option order
    :param expiry:
    :param symbol:
    :param right:
    :param strike:
    :param orderType:
    :param quantity:
    :param lmtPrice:
    :return:
    """
    client, log = init_func()
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
    end_func(client=client)

def place_or_modif_spread_order(expiry,symbol,right,strike_l,strike_s,orderType,quantity,lmtPrice,orderId):
    """
    Place new option spread order or modify existing order

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
    client, log = init_func()
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
    end_func(client=client)

def list_open_orders():
    """
    List all currently open orders for this client
    :return:
    """
    client, log = init_func()
    log.info("list orders ")

    order_structure = client.get_open_orders()
    for idx, x in order_structure.iteritems():
        print("[%s]" % (str(x)))
    end_func(client=client)

def modify_open_order(orderId):
    """
    Modification of an open order through the API can be achieved by the same client which placed the original order.
    In the case of orders placed manually in TWS, the order can be modified by the client with ID 0.

    To modify an order, simply call the IBApi.EClient.placeOrder function again with the same parameters used to place
    the original order, except for the changed parameter. This includes the IBApi.Order.OrderId, which must match the
    IBApi.Order.OrderId of the original. It is not generally recommended to try to change order parameters other than
    the order price and order size. To change other parameters, it might be preferable to cancel the original order
    and place a new order.
    """
    # http://interactivebrokers.github.io/tws-api/modifying_orders.html#gsc.tab=0
    client, log = init_func()
    end_func(client=client)

def cancel_open_order():
    """

    :return:
    """
    client, log = init_func()
    end_func(client=client)


def cancel_all_open_orders():
    """

    :return:
    """
    client, log = init_func()
    end_func(client=client)


def list_prices_before_trade(symbol,expiry,query):
    """
    List prices before trade
    :param symbol:
    :param expiry:
    :param query:
    :return:
    """
    client, log = init_func()
    ctrt = {}
    for idx, x in enumerate(query):
        ctrt[idx] = RequestOptionData(symbol,'FOP',expiry,float(x[1:]),x[:1],'50','GLOBEX','USD',idx)
    log.info("[%s]" % (str(ctrt)))
    ctrt_prc = client.getMktData(ctrt)
    log.info("[%s]" % (str(ctrt_prc)))
    for id, req1 in ctrt_prc.iteritems():
        subset_dic = {k: req1.get_in_data()[k] for k in ('strike', 'right', 'expiry','symbol')}
        subset_dic2 = {k: req1.get_out_data()[id][k] for k in ('bidPrice', 'bidSize', 'askPrice', 'askSize') }
        print subset_dic,subset_dic2
    end_func(client=client)

def list_spread_prices_before_trade(client,symbol,expiry,query):
    """
    List option spread prices before trade
    :param client:
    :param symbol:
    :param expiry:
    :param query:
    :return:
    """
    client, log = init_func()
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
#
    end_func(client=client)

if __name__=="__main__":
    #list_prices_before_trade(symbol="ES",expiry="20170120",query=['C2300.0','C2350.0','P2100.0','P2150.0'])
    #list_spread_prices_before_trade(symbol="ES",expiry="20170120",query=['C2300.0','C2350.0'])
    #place_plain_order(expiry="20170120",symbol="ES",right="C",strike=2200.0,orderType="LMT",quantity=2,lmtPrice=5.0)
    place_or_modif_spread_order(expiry="20170120",symbol="ES",right="C",strike_l=2300.0,
                       strike_s=2350.0,orderType="LMT",quantity=-1,lmtPrice=3.7,orderId=None)
    #list_open_orders()

