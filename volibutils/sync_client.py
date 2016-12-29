import datetime as dt
import inspect
import time
from swigibpy import EPosixClientSocket, ExecutionFilter
from swigibpy import EWrapper
import swigibpy as sy
import sys
import pandas as pd
from time import sleep
from volibutils.RequestOptionData import RequestOptionData
from volibutils.RequestUnderlyingData import RequestUnderlyingData
from volsetup import config
from datetime import datetime
from copy import deepcopy
from volsetup.logger import logger

import random


## This is the reqId IB API sends when a fill is received
FILL_CODE=-1


class syncEWrapper(EWrapper):
    def __init__(self):
        ## variables to store requested data
        self.isDone = False
        self.isSnapshot = False
        self.time = None
        self.bar  = None
        self.requests = {}
        self.req_chains = {}
        self.log=logger("syncEWrapper")
        super(syncEWrapper, self).__init__()

    def print_this_func_info(self):
        """
        This function extract info from the calling function:
        name of the function and args names and values received
        """
        calling_frame = sys._getframe(1)
        if inspect.getframeinfo(calling_frame)[2] in ["error"]:
            args, _, _, values = inspect.getargvalues(calling_frame)
            # sys.stdout.write('function name "%s"' % inspect.getframeinfo(calling_frame)[2])
            #self.log.error('function name "%s"' % inspect.getframeinfo(calling_frame)[2])
            # for i in args:
            # sys.stdout.write("    %s = %s" % (i, values[i]))
            #    print("    %s = %s" % (i, values[i]))
            #    print("")
            # return [(i, values[i]) for i in args]

    def init_fill_data(self):
        setattr(self, "data_fill_data", {})
        setattr(self, "flag_fill_data_finished", False)
        setattr(self, "flag_comm_data_finished", False)

    def init_news_data(self):
        setattr(self, "data_news_data", {})
        setattr(self, "flag_news_data_finished", False)

    def init_historical_data(self):
        setattr(self, "data_historical_data", {})
        setattr(self, "flag_historical_data_finished", False)

    def init_account_data(self):
        setattr(self, "data_account_data", {})
        setattr(self, "summary_account_data", {})
        setattr(self, "flag_account_data_finished", False)

    def init_nextvalidid(self):
        setattr(self, "data_brokerorderid", None)

    def init_openorders(self):
        setattr(self, "data_order_structure", {})
        setattr(self, "flag_order_structure_finished", False)

    def add_order_data(self, orderdetails):
        if "data_order_structure" not in dir(self):
            orderdata={}
        else:
            orderdata=self.data_order_structure

        orderid=orderdetails['orderid']
        orderdata[orderid]=orderdetails

        setattr(self, "data_order_structure", orderdata)


    def add_news_data(self, msgId, msgType, message, origExch ):
        if "data_news_data" not in dir(self):
            newsdata={}
        else:
            newsdata=self.data_news_data

        if msgId not in newsdata.keys():
            newsdata[msgId]={}

        newsdata[msgId]={"msgType":str(msgType),"message":str(message),"origExch":str(origExch)}
        setattr(self, "data_news_data", newsdata)


    def add_historical_data(self, reqId, historical_detail ):
        if "data_historical_data" not in dir(self):
            histdata={}
        else:
            histdata=self.data_historical_data

        if reqId not in histdata.keys():
            histdata[reqId]={}
        histdate=historical_detail['date']
        histdata[reqId][histdate]=historical_detail
        setattr(self, "data_historical_data", histdata)


    def add_fill_data(self, reqId, execdetails):
        if "data_fill_data" not in dir(self):
            filldata={}
        else:
            filldata=self.data_fill_data

        if reqId not in filldata.keys():
            filldata[reqId]={}

        execid=execdetails['execid']
        filldata[reqId][execid]=execdetails
        setattr(self, "data_fill_data", filldata)

    def add_account_data(self, accName,conId,accdetails):
        if "data_account_data" not in dir(self):
            accdata={}
        else:
            accdata=self.data_account_data

        if accName not in accdata.keys():
            accdata[accName]={}
        accdata[accName][conId]=accdetails
        setattr(self, "data_account_data", accdata)

    def add_summary_account_data(self, accName, key,value):
        if "summary_account_data" not in dir(self):
            accdata={}
        else:
            accdata=self.summary_account_data

        if accName not in accdata.keys():
            accdata[accName]={}
        accdata[accName][key]=value
        setattr(self, "summary_account_data", accdata)



    def init_error(self):
        setattr(self, "flag_iserror", False)
        setattr(self, "error_msg", "")


    def store_tick(self,tickerId, type, price):
        preffix = ""
        #tickPrice callback
        if (str(type) == "1"):
            preffix = "bidPrice"
        elif (str(type) == "2"):
            preffix = "askPrice"
        elif (str(type) == "4"):
            preffix = "lastPrice"
        elif (str(type) == "6"):
            preffix = "highPrice"
        elif (str(type) == "7"):
            preffix = "lowPrice"
        elif (str(type) == "9"):
            preffix = "closePrice"
        elif (str(type) == "37"):
            preffix = "MarkPrice"
        #tickSize callback
        elif (str(type) == "0"):
            preffix = "bidSize"
        elif (str(type) == "3"):
            preffix = "askSize"
        elif (str(type) == "5"):
            preffix = "lastSize"
        elif (str(type) == "8"):
            preffix = "Volume"
        elif (str(type) == "21"):
            preffix = "AvgVolume"
        elif (str(type) == "22"):
            preffix = "OpenInterest"
        elif (str(type) == "27"):
            preffix = "CallOI"
        elif (str(type) == "28"):
            preffix = "PutOI"
        elif (str(type) == "29"):
            preffix = "CallVolume"
        elif (str(type) == "30"):
            preffix = "PutVolume"

        #tickGeneric callback
        elif (str(type) == "23"):
            preffix = "OptHistoVol"
        elif (str(type) == "24"):
            preffix = "OptImplVol"
        elif (str(type) == "54"):
            preffix = "TradeCount"
        elif (str(type) == "55"):
            preffix = "TradeRate"
        elif (str(type) == "56"):
            preffix = "VolumeRate"
        # ??
        elif (str(type) == "49"):
            preffix = "Halted"
        else:
            preffix = str(type)
        # 20161113 dls: do not store bad prices (gt 1e200)
        if not (preffix == "") and not (price > 1e200):
            self.requests[tickerId].add_out_data(tickerId, preffix, price)

    def check_if_done(self,reqId):
        #print ("check_if_done: reqId=[%d] pending=[%s] retrieved=[%s] snapShot=[%s]"
        #       % ( reqId
        #           , str(set(self.requests[reqId].getOutputList()).difference(self.requests[reqId].get_out_data()[reqId].keys()))
        #           , str( self.requests[reqId].get_out_data()[reqId].keys() )
        #           , str(self.isSnapshot) ))
        if ( self.isSnapshot == True ):
            if (set(self.requests[reqId].getOutputListSnap()).issubset(self.requests[reqId].get_out_data()[reqId].keys())):
                self.requests[reqId].isDone = True
        if ( self.isDone == False ):
            if ( set(self.requests[reqId].getOutputList()).issubset(self.requests[reqId].get_out_data()[reqId].keys()) ):
                self.requests[reqId].isDone = True
                #print("check_if_done True reqId=[%d]" % (reqId))

    ## overwridden listener functions
    def tickPrice(self, tickerId, type, price, canAutoExecute):
        self.print_this_func_info()
        self.store_tick(tickerId, type, price)
        self.check_if_done(tickerId)

    def tickSize(self, tickerId, type, size):
        self.print_this_func_info()
        self.store_tick(tickerId, type, size)
        self.check_if_done(tickerId)

    def tickOptionComputation(self, tickerId,
                                    type,
                                    impliedVol,
                                    delta,
                                    optPrice,
                                    pvDividend,
                                    gamma,
                                    vega,
                                    theta,
                                    undPrice):
        self.print_this_func_info()
        preffix = ""
        if (str(type) == "10"):
            preffix = "bid"
        elif (str(type) == "11"):
            preffix = "ask"
        elif (str(type) == "12"):
            preffix = "last"
        elif (str(type) == "13"):
            preffix = "model"
        else:
            preffix = "opt" + str(type)
        if not (preffix == ""):
            self.requests[tickerId].add_out_data(tickerId, preffix+"ImpliedVol", impliedVol)
            self.requests[tickerId].add_out_data(tickerId, preffix+"Delta", delta)
            self.requests[tickerId].add_out_data(tickerId, preffix+"OptPrice", optPrice)
            self.requests[tickerId].add_out_data(tickerId, preffix+"PvDividend", pvDividend)
            self.requests[tickerId].add_out_data(tickerId, preffix+"Gamma", gamma)
            self.requests[tickerId].add_out_data(tickerId, preffix+"Vega", vega)
            self.requests[tickerId].add_out_data(tickerId, preffix+"Theta", theta)
            self.requests[tickerId].add_out_data(tickerId, preffix+"UndPrice", undPrice)
        self.check_if_done(tickerId)

    def tickGeneric(self, tickerId, type, value):
        self.print_this_func_info()
        self.store_tick(tickerId, type, value)
        self.check_if_done(tickerId)

    def tickString(self, tickerId, tickType, value):
        self.print_this_func_info()
    def tickEFP(self, tickerId,
                      tickType,
                      basisPoints,
                      formattedBasisPoints,
                      impliedFuture,
                      holdDays,
                      futureExpiry,
                      dividendImpact,
                      dividendsToExpiry):
        self.print_this_func_info()
    def orderStatus(self, orderId,
                          status,
                          filled,
                          remaining,
                          avgFillPrice,
                          permId,
                          parentId,
                          lastFillPrice,
                          clientId,
                          whyHeld):
        self.print_this_func_info()

    def openOrder(self, orderID, contract, order, orderState):
        """
        Tells us about any orders we are working now
        """
        self.print_this_func_info()
        ## Get a selection of interesting things about the order
        orderdetails=dict(symbol=contract.symbol , expiry=contract.expiry,  qty=int(order.totalQuantity) ,
                       side=order.action , orderid=int(orderID), clientid=order.clientId )

        self.add_order_data(orderdetails)

    def openOrderEnd(self):
        """
        Finished getting open orders
        """
        setattr(self, "flag_order_structure_finished", True)



    def updatePortfolio(self, contract,
                              position,
                              marketPrice,
                              marketValue,
                              averageCost,
                              unrealizedPNL,
                              realizedPNL,
                              accountName):
        self.print_this_func_info()

        symbol = contract.symbol
        expiry = contract.expiry
        combolegs=str(contract.comboLegs)
        comboLegsDescrip=contract.comboLegsDescrip
        conId =int(contract.conId)
        localSymbol=contract.localSymbol
        primaryExchange=contract.primaryExchange
        multiplier=contract.multiplier
        right=contract.right
        secId=contract.secId
        secIdType=contract.secIdType
        secType=contract.secType
        strike=float(contract.strike)
        underComp=contract.underComp

        portfoliodetails = dict(symbol=str(symbol),
                                expiry=str(expiry),
                                combolegs=str(combolegs),
                                comboLegsDescrip=str(comboLegsDescrip),
                                conId=str(conId),
                                localSymbol=str(localSymbol),
                                primaryExchange=str(primaryExchange),
                                multiplier=str(multiplier),
                                right=str(right),
                                secId=str(secId),
                                secIdType=str(secIdType),
                                secType=str(secType),
                                strike=str(strike),
                                underComp=str(underComp),
                                position=str(position),
                                marketPrice=str(marketPrice),
                                marketValue=str(marketValue),
                                averageCost=str(averageCost),
                                unrealizedPNL=str(unrealizedPNL),
                                realizedPNL=str(realizedPNL),
                                accountName=str(accountName)
                           )
        #print("conId = [%d] portfoliodetails = [%s]" % (conId, str(portfoliodetails)))
        ## This is just execution data we've asked for
        self.add_account_data(str(accountName),conId,portfoliodetails)

    def updateAccountTime(self, timeStamp):
        self.print_this_func_info()
        #print("updateAccountTime timeStamp = [%s]" % (str(timeStamp)))

    def updateAccountValue(self, key, value, currency, accountName):
        self.print_this_func_info()
        key1 = str(key).replace("-","_").replace("+","plus")
        #print("updateAccountValue key = [%s] value=[%s] currency=[%s] accountName=[%s]"
        #      % (str(key),str(value),str(currency),str(accountName)))
        self.add_summary_account_data(str(accountName), str(key1)+"_"+str(currency), str(value))

    def accountDownloadEnd(self,accountName):
        self.print_this_func_info()
        #print("accountDownloadEnd accountName = [%s]" % (str(accountName)))
        setattr(self, "flag_account_data_finished", True)


    def nextValidId(self, orderId):
        """
        Give the next valid order id

        Note this doesn't 'burn' the ID; if you call again without executing the next ID will be the same
        """

        self.print_this_func_info()
        self.data_brokerorderid = orderId


    def contractDetails(self, reqId, contractDetails):
        self.print_this_func_info()
        opt1 = contractDetails.summary
        temp1 = {"symbol":opt1.symbol,"secType":opt1.secType,"expiry":opt1.expiry,"strike":opt1.strike,"right":opt1.right,
                  "multiplier":opt1.multiplier,"exchange":opt1.exchange,"currency":opt1.currency}
        #print ("contractDetails reqId=[%d] contractdetails=[%s]" % ( reqId , str(temp1)) )
        if reqId in self.req_chains:
            self.req_chains[reqId].optionsChain.append(temp1)
        #for kk, pedo in self.req_chains.iteritems():
        #    print(">>contractDetails reqId=[%d] contractdetails=[%s]" % (kk, str(pedo.optionsChain)))

    def bondContractDetails(self, reqId, contractDetails):
        self.print_this_func_info()
    def contractDetailsEnd(self, reqId):
        self.print_this_func_info()
        self.log.info("contractDetailsEnd=[%s]" % ( str(reqId)) )
        self.req_chains[reqId].isDone = True
        #print("contractDetailsEnd reqId=[%d] contractdetails=[%s]" % ( reqId , str(self.req_chains[reqId].optionsChain) ))

    def commissionReport(self, commissionReport):
        self.log.info("CommisionReport is being called ...")
        setattr(self, "flag_comm_data_finished", True)

    def position(self,account,contract,position):
        pass


    def execDetails(self, reqId, contract, execution):
        """
        This is called if

        a) we have submitted an order and a fill has come back
        b) We have asked for recent fills to be given to us

        We populate the filldata object and also call action_ib_fill in case we need to do something with the
          fill data

        See API docs, C++, SocketClient Properties, Contract and Execution for more details
        """
        self.print_this_func_info()
        reqId=int(reqId)

        execid=execution.execId
        exectime=execution.time
        thisorderid=int(execution.orderId)
        account=execution.acctNumber
        exchange=execution.exchange
        permid=int(execution.permId)
        price=float(execution.price)
        cumQty=int(execution.cumQty)
        clientid=execution.clientId
        liquidation=int(execution.liquidation)
        avgprice=float(execution.avgPrice)
        evRule=execution.evRule
        evMultiplier=float(execution.evMultiplier)
        side=execution.side
        shares = int(execution.shares)
        symbol = contract.symbol
        expiry = contract.expiry
        combolegs=str(contract.comboLegs)
        comboLegsDescrip=contract.comboLegsDescrip
        conId =int(contract.conId)
        localSymbol=contract.localSymbol
        primaryExchange=contract.primaryExchange
        multiplier=contract.multiplier
        right=contract.right
        secId=contract.secId
        secIdType=contract.secIdType
        secType=contract.secType
        strike=float(contract.strike)
        underComp=contract.underComp
        execdetails=dict(side=str(side), times=str(exectime), orderid=str(thisorderid), qty=str(cumQty),
                         price=str(price), symbol=str(symbol), expiry=str(expiry), clientid=str(clientid),
                         execid=str(execid), account=str(account), exchange=str(exchange), permid=str(permid),
                         shares=str(shares),liquidation=str(liquidation),avgprice=str(avgprice),
                         evmultiplier=str(evMultiplier),evrule=str(evRule ),combolegs=str(combolegs),
                         comboLegsDescrip=str(comboLegsDescrip),conId=str(conId),localSymbol=str(localSymbol),
                         primaryExchange=str(primaryExchange),multiplier=str(multiplier),right=str(right),secId=str(secId),
                         secIdType=str(secIdType),strike=str(strike),underComp=str(underComp))
        self.log.info("reqId = [%d] execdetails = [%s]" % (reqId,str(execdetails)))
        if reqId==FILL_CODE:
            ## This is a fill from a trade we've just done
            pass
            #action_ib_fill(execdetails)

        else:
            ## This is just execution data we've asked for
            self.add_fill_data(reqId, execdetails)


    def execDetailsEnd(self, reqId):
        """
        No more orders to look at if execution details requested
        """
        self.print_this_func_info()
        setattr(self, "flag_fill_data_finished", True)



    def updateMktDepth(self, tickerId,
                             position,
                             operation,
                             side,
                             price,
                             size):
        self.print_this_func_info()
    def updateMktDepthL2(self, tickerId,
                               position,
                               marketMaker,
                               operation,
                               side,
                               price,
                               size):
        self.print_this_func_info()
    def updateNewsBulletin(self, msgId, msgType, message, origExchange):
        self.print_this_func_info()
        msgId1=str(msgId)
        msgType1=str(msgType)
        message1=str(message)
        origExch1=str(origExchange)
        self.log.info("msgId1 = [%d] message1 = [%s]" % (msgId1,str(message1)))
        self.add_news_data(self, msgId1, msgType1, message1, origExch1 )

    def managedAccounts(self, accountsList):
        self.print_this_func_info()
    def receiveFA(self, faDataType, xml):
        self.print_this_func_info()

    def historicalDataEnd(self,reqId,start,end):
        self.print_this_func_info()

    def historicalData(self, reqId,
                             date,
                             open,
                             high,
                             low,
                             close,
                             volume,
                             count,
                             WAP,
                             hasGaps):
        self.print_this_func_info()
        reqId1=int(reqId)
        date1=str(date)
        open1=float(open)
        high1=float(high)
        low1=float(low)
        close1=float(close)
        volume1=int(volume)
        count1=int(count)
        WAP1=float(WAP)
        hasGaps1=int(hasGaps)
        historical_details=dict(reqId=reqId1,
                                date=date1,
                                open=open1,
                                high=high1,
                                low=low1,
                                close=close1,
                                volume=volume1,
                                count=count1,
                                WAP=WAP1,
                                hasGaps=hasGaps1)

        #print("reqId = [%d] historical_details = [%s]" % (reqId,str(historical_details)))
        if "finished" in date1:
            setattr(self, "flag_historical_data_finished", True)
            self.log.info("Historical data request finished ...")
        else:
            self.add_historical_data(reqId, historical_details)


    def scannerParameters(self, xml):
        self.print_this_func_info()
    def scannerData(self, reqId,
                          rank,
                          contractDetails,
                          distance,
                          benchmark,
                          projection,
                          legsStr):
        self.print_this_func_info()
    def scannerDataEnd(self, reqId):
        self.print_this_func_info()
    def realtimeBar(self, reqId,
                          time,
                          open,
                          high,
                          low,
                          close,
                          volume,
                          wap,
                          count):
        self.print_this_func_info()
        self.bar = (time, open, high, low, close, volume)
        self.isDone = True
    def currentTime(self, time):
        self.print_this_func_info()
        self.time = dt.datetime.fromtimestamp(time)
        self.isDone = True
        self.log.info("currentTime=[%s]" % ( str(self.time) ) )
    def fundamentalData(self, reqId, data):
        self.print_this_func_info()
    def error(self,*err):
        if err[1] <> 300:
            self.print_this_func_info()
        ## Any errors not on this list we just treat as information
        ERRORS_TO_TRIGGER=[502, 504, 326]
        #ERRORS_TO_TRIGGER=[201, 103, 502, 504, 509, 200, 162, 420, 2105, 1100, 478, 201, 399]
        if err[1] in ERRORS_TO_TRIGGER:
            self.isDone = True
            self.log.error("Error =[%d] [%s] [%s]" % ( err[1], str(err[2]) ,str(err[0]) ) )
        else:
            self.log.info("Info =[%d] [%s] [%s]" % (err[1], str(err[2]), str(err[0])))

    def tickSnapshotEnd(self, reqId):
        self.print_this_func_info()
        self.requests[reqId].isDone = True

class IBClient():
    """
        Clase para conectarse como cliente a IB API con python usando swigibpy

    """
    def __init__(self,config1):
        self.config = config1
        self.myEWrapper = syncEWrapper()
        self.myEClientSocket = EPosixClientSocket(self.myEWrapper)
        self.start_time = 0
        self.log = logger("IBClient")

    def connect(self):
        port1= int(self.config.config['ib_api']['port'])
        host1 = str(self.config.config['ib_api']['host'])
        #clientid1 = int(self.config.config['ib_api']['clientid'])
        # This change is to allow more than one client in the VPS
        clientid1 = random.randint(1, 2000)
        self.log.info("Calling connection port=%d host=%s clientid=%d" % (port1 , host1 , clientid1))
        self.myEClientSocket.eConnect(host1, port1 , clientid1)

    def disconnect(self):
        self.myEClientSocket.eDisconnect()

    def wait(self):
        inc_secs = .2 # .2 a fifth of a second
        while not self.myEWrapper.isDone:
            li = []
            for reqId, request in self.myEWrapper.requests.iteritems():
                li.append( self.myEWrapper.requests[reqId].isDone )
                if ( self.myEWrapper.requests[reqId].isDone and not self.myEWrapper.requests[reqId].isCancelled ):
                    self.log.info("Cancelling MktData [%s] for reqId=[%d] [%d]"
                          % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),reqId, request.getRequestId()))
                    self.myEClientSocket.cancelMktData(id=request.getRequestId())
                    self.myEWrapper.requests[reqId].isCancelled = True
            #if  int((time.time() - self.start_time) % 30 ):
            #    print("is really done? li=[%s]" % (li))
            if (all(li)):
                self.myEWrapper.isDone = True

            if (time.time() - self.start_time) > \
                    min ( int(self.config.config['ib_api']['max_wait']) ,
                          int(self.config.config['ib_api']['wait_per_request']) *  len(self.myEWrapper.requests) ) :
                self.myEWrapper.isDone =True

            #if  int((time.time() - self.start_time) % 100 )
            #   print("Status of query after %d secs: %s " % ( int(time.time() - self.start_time),str(li)))
            sleep(inc_secs)
        self.myEWrapper.isDone = False

    def wait_chain(self):
        inc_secs = .2 # .2 a fifth of a second
        while not self.myEWrapper.isDone:
            li = []
            for reqId, request in self.myEWrapper.req_chains.iteritems():
                li.append( self.myEWrapper.req_chains[reqId].isDone )
                #print("Status of query after %d secs: isDone=%s reqId=%d maxWait=%d"
                #      % (float(time.time() - self.start_time)
                #         , str(self.myEWrapper.req_chains[reqId].isDone), reqId
                #         ,int(self.config.config['ib_api']['max_wait'])))

            if (all(li)):
                #print("Check li =[%s]" % ( str(li) ))
                self.myEWrapper.isDone = True

            time1 = time.time() - self.start_time
            time2 = int(self.config.config['ib_api']['max_wait'])
            if ( time1  >  time2 ):
                #print("Timing time1=[%0.2f] time2=[%0.2f]" % ( time1 , time2 ))
                self.myEWrapper.isDone =True

            #if  int((time.time() - self.start_time) % 100 )
            sleep(inc_secs)
        #print("Wait_chain end isDone=[%s]" % ( str(self.myEWrapper.isDone) ) )
        self.myEWrapper.isDone = False


    def getTime(self):
        self.start_time=time.time()
        self.myEClientSocket.reqCurrentTime()
        self.wait()
        return self.myEWrapper.time

    def getRTBarData(self, contract):
        self.myEClientSocket.reqRealTimeBars(id=1,
                                             contract=contract.getInstance,
                                             barSize=5,
                                             whatToShow='MIDPOINT',
                                             useRTH=0)
        self.wait()
        self.myEClientSocket.cancelRealTimeBars(tickerId=1)
        return self.myEWrapper.bar

    def getMktDataSnapshot(self, requests):
        self.start_time=time.time()
        self.log.info("getMktDataSnapshot [%s]" % ( datetime.now().strftime('%Y-%m-%d %H:%M:%S') ) )
        self.myEWrapper.isDone = False
        self.myEWrapper.isSnapshot = True
        self.myEWrapper.requests = requests
        for reqId, request in requests.iteritems():
            self.myEClientSocket.reqMktData(id=request.getRequestId(),
                                            contract=request.getInstance(),
                                            genericTicks="",
                                            snapshot=1)

        self.wait()
        return1 = self.myEWrapper.requests
        return return1

    def getMktData(self, requests):
        self.start_time=time.time()
        self.log.info("getMktData [%s]" % ( datetime.now().strftime('%Y-%m-%d %H:%M:%S') ) )
        self.myEWrapper.isDone = False
        self.myEWrapper.isSnapshot = False
        self.myEWrapper.requests = requests

        # to avoid max number of requests exceeded
        num1 = 0
        num2 = 0
        total = len(requests.keys())
        for reqId, request in requests.iteritems():
            num1 += 1
            num2 += 1
            self.log.info("Requesting MktData [%s] for reqId=[%d] [%d]"
                  % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),reqId, request.getRequestId()))
            self.myEClientSocket.reqMktData(id=request.getRequestId(),
                                            contract=request.getInstance(),
                                            genericTicks=request.getGenericTicks(),
                                            snapshot=0)
            if num1 >= 30:
                self.log.info("reqMktData requested [%d/%d] sleeping %d secs..." % (num2,total,6))
                sleep(10)
                num1=0
            # iterate all requests to cancel those already fulfilled
            for reqId2, request2 in self.myEWrapper.requests.iteritems():
                if (self.myEWrapper.requests[reqId2].isDone and not self.myEWrapper.requests[reqId2].isCancelled):
                    self.log.info("Cancelling MktData [%s] for reqId2=[%d] [%d]"
                          % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),reqId2, request2.getRequestId()))
                    self.myEClientSocket.cancelMktData(id=request2.getRequestId())
                    self.myEWrapper.requests[reqId2].isCancelled = True

        self.wait()
        return1 = self.myEWrapper.requests
        #self.myEClientSocket.cancelMktData(id=request.getRequestId())

        return return1

    def getOptionsChain(self,requests):
        self.start_time=time.time()
        self.log.info("getOptionsChain [%s]" % ( datetime.now().strftime('%Y-%m-%d %H:%M:%S') ) )
        return1 = {}
        for reqId, request in requests.iteritems():
            self.myEWrapper.isDone = False
            self.myEWrapper.req_chains = {reqId:request}
            self.myEClientSocket.reqContractDetails(reqId=request.getRequestId(),
                                                    contract=request.getInstance())

            self.wait_chain()
            return1.update(self.myEWrapper.req_chains)
            #print("Requested reqId[%d] instance[%s] return1[%s] datetime[%s]"
            #  % (request.getRequestId(), request.getInstance().symbol, str(return1[reqId].optionsChain)
            #     , datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        #for reqId1, request1 in return1.iteritems():
            #print("getOptionsChain 1 reqId1=[%d] len opt chain=[%d] self.myEWrapper.isDone=[%s]"
            #      % ( reqId1, len(request1.optionsChain) , str(self.myEWrapper.isDone) ) )
            #if reqId1 in self.myEWrapper.req_chains.keys():
            #    print("     getOptionsChain isDone=[%s]" % ( str(self.myEWrapper.req_chains[reqId1].isDone) ) )
            #for opt2 in request1.optionsChain:
            #    print("getOptionsChain 2 ", reqId1 , opt2)

        return return1

    def isConnected(self):
        return self.myEClientSocket.isConnected()

    def get_news(self, reqId):
        """
        Returns a list of all news today
        """
        self.myEWrapper.init_news_data()
        self.myEWrapper.init_error()
        allMsgs=True
        self.myEClientSocket.reqNewsBulletins(allMsgs)

        iserror = False
        finished = False

        start_time = time.time()

        while not finished and not iserror:
            finished = self.myEWrapper.flag_news_data_finished
            iserror = self.myEWrapper.flag_iserror
            if (time.time() - start_time) > \
                    min(int(self.config.config['ib_api']['max_wait']),
                    int(self.config.config['ib_api']['wait_per_request']) ):
                finished = True
            #pass
            #print str(self.myEWrapper.data_news_data)

        self.myEClientSocket.cancelNewsBulletins()

        if iserror:
            self.log.error(self.myEWrapper.error_msg)
            self.log.error("Problem getting news")

        if not self.myEWrapper.data_news_data: #check if today there are no news
            newslist = {}
        else:
            newsist = self.myEWrapper.data_news_data
        return newslist


    def get_historical(self, request,endDateTime,durationStr,barSizeSetting,whatToShow,useRTH,formatDate):
        """
        Returns a list of all news today
        """
        inc_secs=.2
        self.myEWrapper.init_historical_data()
        self.myEWrapper.init_error()
        self.log.info("get_historical request")
        #print ("id=",request.getRequestId(),"contract=",request.getInstance(),"endDateTime=",endDateTime,
        #      "durationStr=",durationStr,"barSizeSetting=",barSizeSetting,"whatToShow=",whatToShow,
        #      "useRTH=",useRTH,"formatDate=",formatDate)

        self.myEClientSocket.reqHistoricalData(id=request.getRequestId(),
                                               contract=request.getInstance(),
                                               endDateTime=endDateTime,
                                               durationStr=durationStr,
                                               barSizeSetting=barSizeSetting,
                                               whatToShow=whatToShow,
                                               useRTH=useRTH,
                                               formatDate=formatDate)
        iserror = False
        finished = False

        start_time = time.time()

        while not finished: #and not iserror:
            finished = self.myEWrapper.flag_historical_data_finished
            iserror = self.myEWrapper.flag_iserror
            if (time.time() - start_time) > int(self.config.config['ib_api']['wait_per_request']):
                    #min(int(self.config.config['ib_api']['max_wait']),
                    #int(self.config.config['ib_api']['wait_per_request']) * 2 ):
                self.log.info( "While loop timed out: max wait reached [%d] " % ( time.time() - start_time )  )
                self.myEClientSocket.cancelHistoricalData(tickerId=request.getRequestId())
                sleep(4)
                finished = True
            sleep(inc_secs)
            #self.log.info( str(self.myEWrapper.flag_historical_data_finished))



        if iserror:
            self.log.error(self.myEWrapper.error_msg)
            self.log.error("Problem getting historical data")

        if not self.myEWrapper.data_historical_data:
            histlist = {}
        else:
            histlist = deepcopy(self.myEWrapper.data_historical_data)
        return histlist


    def get_executions(self, reqId):
        """
        Returns a list of all executions done today
        """
        assert type(reqId) is int
        if reqId == FILL_CODE:
            raise Exception("Can't call get_executions with a reqId of %d as this is reserved for fills %d" % reqId)

        self.myEWrapper.init_fill_data()
        self.myEWrapper.init_error()

        ## We can change ExecutionFilter to subset different orders
        self.myEClientSocket.reqExecutions(reqId, ExecutionFilter())

        iserror = False
        finished = False
        inc_secs=.2
        start_time = time.time()

        while not finished and not iserror:
            finished = self.myEWrapper.flag_fill_data_finished and self.myEWrapper.flag_comm_data_finished
            iserror = self.myEWrapper.flag_iserror
            if (time.time() - start_time) > \
                    min(int(self.config.config['ib_api']['max_wait']),
                    int(self.config.config['ib_api']['wait_per_request']) ):
                finished = True
            sleep(inc_secs)

        if iserror:
            self.log.error(self.myEWrapper.error_msg)
            self.log.error("Problem getting executions")

        if not self.myEWrapper.data_fill_data: #check if today there are no orders
            execlist = {}
        else:
            execlist = self.myEWrapper.data_fill_data[reqId]
        return execlist

    def get_potfolio_data(self, reqId):
        """
        Returns account data
        """
        self.myEWrapper.init_account_data()
        self.myEWrapper.init_error()

        ## We can change ExecutionFilter to subset different orders
        self.myEClientSocket.reqAccountUpdates(subscribe=True,acctCode=str(self.config.config['ib_api']['accountid']) )

        iserror = False
        finished = False
        inc_secs=.2
        start_time = time.time()

        while not finished and not iserror:
            finished = self.myEWrapper.flag_account_data_finished
            iserror = self.myEWrapper.flag_iserror
            if (time.time() - start_time) > \
                    min(int(self.config.config['ib_api']['max_wait']),
                        int(self.config.config['ib_api']['wait_per_request'])):
                finished = True
            sleep(inc_secs)
        #unsubscribe
        self.myEClientSocket.reqAccountUpdates(subscribe=False, acctCode=str(self.config.config['ib_api']['accountid']))

        if iserror:
            self.log.error(self.myEWrapper.error_msg)
            self.log.error("Problem getting account data")

        #print ("data_account_data = [%s]"
        #       % (str(self.myEWrapper.data_account_data[str(self.config.config['ib_api']['accountid'])])))

        #print ("summary_account_data = [%s]"
        #       % ( str( self.myEWrapper.summary_account_data[str(self.config.config['ib_api']['accountid'])]  ) ))
        if not self.myEWrapper.data_account_data:
            acclist = {}
        else:
            acclist = self.myEWrapper.data_account_data[str(self.config.config['ib_api']['accountid'])]

        if not self.myEWrapper.summary_account_data:
            summarylist = {}
        else:
            dict1 = self.myEWrapper.summary_account_data[str(self.config.config['ib_api']['accountid'])]
            key1 = str(self.config.config['ib_api']['accountid'])
            summarylist = { key1 : dict1 }

        return acclist , summarylist

    def get_open_orders(self):
        """
        Returns a list of any open orders
        """
        self.myEWrapper.init_openorders()
        self.myEWrapper.init_error()

        start_time=time.time()
        self.myEClientSocket.reqAllOpenOrders()
        iserror=False
        finished=False

        while not finished and not iserror:
            finished=self.myEWrapper.flag_order_structure_finished
            iserror=self.myEWrapper.flag_iserror
            if (time.time() - start_time) > int(self.config.config['ib_api']['max_wait']) :
                ## You should have thought that IB would teldl you we had finished
                finished=True
            pass

        order_structure=self.myEWrapper.data_order_structure
        if iserror:
            self.log.error (self.myEWrapper.error_msg)
            self.log.error ("Problem getting open orders")

        return order_structure


    def get_next_brokerorderid(self):
        """
        Get the next brokerorderid
        """
        self.myEWrapper.init_error()
        self.myEWrapper.init_nextvalidid()
        start_time=time.time()
        ## Note for more than one ID change '1'
        self.myEClientSocket.reqIds(1)
        finished=False
        brokerorderid=None
        iserror=False
        while not finished and not iserror:
            brokerorderid=self.myEWrapper.data_brokerorderid
            finished=brokerorderid is not None
            iserror=self.myEWrapper.flag_iserror
            if (time.time() - start_time) > int(self.config.config['ib_api']['max_wait']):
                finished=True
            pass

        if brokerorderid is None or iserror:
            self.log.error(self.myEWrapper.error_msg)
            self.log.error("Problem getting next broker orderid")
            return None

        return brokerorderid



    def place_new_IB_order(self,ibcontract, iborder, orderid):
        if orderid is None:
            self.log.info("Getting orderid from IB")
            orderid=self.get_next_brokerorderid()
        if orderid is not None:
            self.log.info("Using order id of %d" % orderid)

        # Place the order
        self.myEClientSocket.placeOrder(
                orderid,                                    # orderId,
                ibcontract,                                   # contract,
                iborder                                       # order
            )

        return orderid


def run_test_opt_chain():
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    log_file = open( datetime.now().strftime('sync_client_%Y_%m_%d_%H_%M_%S.log'), "w")
    sys.stdout = log_file
    sys.stderr = log_file

    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)
    client.connect()
    print(client.getTime())

    underl = {
            1000:RequestOptionData('ES','FOP','20160819',0,'','','GLOBEX','',1000),
            1001: RequestOptionData('SPX', 'OPT', '20160818', 0, '', '', 'SMART', 'USD',1001),
            1002: RequestOptionData('SPY', 'OPT', '20160819', 0, '', '', 'SMART', 'USD', 1002)
    }

    underl_under = {
        1000: RequestUnderlyingData('ES', 'FUT', '20160916', 0, '', '', 'GLOBEX', 'XXX', 1000),
        1001: RequestUnderlyingData('SPX', 'IND', '0', 0, '', '', 'CBOE', 'XXX', 1001),
        1002: RequestUnderlyingData('SPY', 'STK', '0', 0, '', '', 'SMART', 'USD', 1002),
        1003: RequestUnderlyingData('IWM', 'STK', '0', 0, '', '', 'SMART', 'USD', 1003)
    }

    underl_prc = client.getMktData(underl_under)
    row1 = 0
    opt_chain_ranges = {}
    for reqId, request in underl_prc.iteritems():
        row1 += 1
        if "closePrice" in request.get_out_data()[reqId]:
            print ("Requestid [%d]: Option[%s] Results [%s]" % ( reqId , str(request.get_in_data()), str(request.get_out_data()) ))
            opt_chain_ranges.update({ request.get_in_data()["symbol"] : request.get_out_data()[reqId]["closePrice"]  })
            #print ("Requestid [%d]: modelVega=%0.5f" % ( reqId, request.get_out_data()[reqId]['modelVega'] ) )
    print("opt_chain_ranges = ", opt_chain_ranges)

    # get options chain
    list_results = client.getOptionsChain(underl)

    print("Number of requests [%d]" % (len(list_results)) )
    for reqId, request in list_results.iteritems():
        print ("Requestid [%d]: Option[%s] Len Results [%d]" % ( reqId , str(request.get_in_data()), len(request.optionsChain) ))

    contr = {}
    num = 100
    pct_range_opt_chain = float(globalconf.config['ib_api']['pct_range_opt_chain'])
    for reqId, request in list_results.iteritems():
        #print ("Requestid [%d]: Chain size [%d] detail [%s]"
        #       % ( reqId , len( request.optionsChain ) , str(request.optionsChain)  ))
        for opt1 in request.optionsChain:
            if opt1["symbol"] in opt_chain_ranges:
                filtro_strikes = opt_chain_ranges[ opt1["symbol"] ]
                #print ("filtro_strikes = " , filtro_strikes , "opt1 = " , opt1 )
                if  opt1["strike"] >= (filtro_strikes * ( 1 - pct_range_opt_chain ) ) \
                        and opt1["strike"]<= (filtro_strikes * ( 1 + pct_range_opt_chain ) ):
                    contr[num] = RequestOptionData(opt1["symbol"],opt1["secType"],opt1["expiry"],opt1["strike"],opt1["right"],
                                                   opt1["multiplier"],opt1["exchange"],opt1["currency"],num)
                    num += 1

    list_results2= client.getMktData(contr)
    client.disconnect()
    dataframe = pd.DataFrame()
    row1 = 0

    for reqId, request in list_results2.iteritems():
        row1 += 1
        print ("Requestid [%d]: Option[%s] Results [%s]" % ( reqId , str(request.get_in_data()), str(request.get_out_data()) ))
        #print ("Requestid [%d]: modelVega=%0.5f" % ( reqId, request.get_out_data()[reqId]['modelVega'] ) )
        dict1 = request.get_in_data().copy()
        if reqId in request.get_out_data():
            dict1.update(request.get_out_data()[reqId])
        for key in dict1.keys():
            dataframe.loc[row1,key] = dict1[key]

    #dataframe.to_hdf("C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/ib.h5",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    dataframe.to_excel(datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/ib%Y_%m_%d_%H_%M_%S.xlsx'),
                     sheet_name='option chains')
    log_file.close()

def run_test_get_time():
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    log_file = open( datetime.now().strftime('sync_client_%Y_%m_%d_%H_%M_%S.log'), "w")
    sys.stdout = log_file
    sys.stderr = log_file

    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)
    client.connect()
    dataframe = pd.DataFrame({'ibtime':client.getTime()},index=[0])

    client.disconnect()

    dataframe.to_excel(
        datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/ibtime%Y_%m_%d_%H_%M_%S.xlsx'),
        sheet_name='orders')
    log_file.close()

def run_test_get_orders():
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    log_file = open( datetime.now().strftime('sync_client_%Y_%m_%d_%H_%M_%S.log'), "w")
    sys.stdout = log_file
    sys.stderr = log_file

    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)
    client.connect()

    ## Get the executions (gives you everything for last business day)
    execlist = client.get_executions(10)
    client.disconnect()
    print ("execlist = [%s]" % ( str(execlist) ))
    if execlist:
        dataframe = pd.DataFrame.from_dict(execlist).transpose()
        #print("dataframe = ",dataframe)
        dataframe.to_excel(
            datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/iborders%Y_%m_%d_%H_%M_%S.xlsx'),
            sheet_name='orders')

    log_file.close()

def run_test_get_news():
    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)
    client.connect()
    newslist = client.get_news(33)
    print ("newslist = [%s]" % ( str(newslist) ))
    client.disconnect()

def run_test_get_historical():
    globalconf = config.GlobalConfig()
    client = IBClient(globalconf)
    client.connect()
    ticker = RequestUnderlyingData('ES', 'FUT', '20160916', 0, '', '', 'GLOBEX', 'XXX', 1000)
    endDateTime = "20160815 23:59:59"
    durationStr="30 D"
    barSizeSetting="30 mins"
    whatToShow="TRADES"
    useRTH=1
    formatDate=1
    historicallist = client.get_historical(ticker,endDateTime,durationStr,barSizeSetting,whatToShow,useRTH,formatDate)
    #print ("historicallist = [%s]" % ( str(historicallist) ))
    client.disconnect()
    dataframe = pd.DataFrame()
    if historicallist:
        for reqId, request in historicallist.iteritems():
            for date, row in request.iteritems():
                #print ("date [%s]: row[%s]" % (date, str(row)))
                temp1=pd.DataFrame(row,index=[0])
                dataframe=dataframe.append(temp1.reset_index().drop('index',1))

        dataframe=dataframe.sort_values(by=['date']).set_index('date')
        dataframe.to_excel(
            datetime.now().strftime('C:/Users/David/Dropbox/proyectos/Python/voltrad1/db/histdata%Y_%m_%d_%H_%M_%S.xlsx'),
            sheet_name='historical_data')


if __name__ == "__main__":
    #run_test_opt_chain()
    #run_test_get_orders()
    #run_test_get_news()
    run_test_get_historical()
