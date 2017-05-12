from swigibpy import EWrapper
import time
from swigibpy import EPosixClientSocket, ExecutionFilter
from volibutils.IButils import bs_resolve, action_ib_fill
import ConfigParser

FILL_CODE = 1234
MEANINGLESS_NUMBER = 999999
MAX_WAIT = 99999

def return_IB_connection_info():
    """
    Returns the tuple host, port, clientID required by eConnect
    """
    host = ""
    port = ""
    clientid = ""
    return (host, port, clientid)


class IBWrapper(EWrapper):
    """
    Callback object passed to TWS, these functions will be called directly by TWS.
    """
    def __init__(self, globalconfig):
        self.globalconfig=globalconfig
        self.nextValidId = 1

    def init_error(self):
        setattr(self, "flag_iserror", False)
        setattr(self, "error_msg", "")

    def init_time(self):
        setattr(self, "data_the_time_now_is", None)

    def init_fill_data(self):
        setattr(self, "data_fill_data", {})
        setattr(self, "data_comm_data", {})
        setattr(self, "flag_fill_data_finished", False)

    def error(self, id, errorCode, errorString):
        """
        This event is called when there is an error with the communication or when TWS wants to send a message to the client.
        """
        errors_to_trigger=self.globalconfig.get_list_errors_to_trigger_ib()
        if errorCode in errors_to_trigger:
            errormsg="IB error id %d errorcode %d string %s" %(id, errorCode, errorString)
            print (errormsg)
            setattr(self, "flag_iserror", True)
            setattr(self, "error_msg", errormsg)

    def currentTime(self, time_from_server):
        """
        This function receives the current system time on the server side.
        """
        setattr(self, "data_the_time_now_is", time_from_server)

    def nextValidId(self, orderId):
        """
        This function is called after a successful connection to TWS.
        """
        self.nextValidId=orderId

    def managedAccounts(self, openOrderEnd):
        """
        This function is called when a successful connection is made to an account.
        It is also called when the reqManagedAccts() function is invoked.
        """
        pass

    def orderStatus(self, reqid, status, filled, remaining, avgFillPrice, permId,
            parentId, lastFilledPrice, clientId, whyHeld):
        """
        This event is called whenever the status of an order changes. It is also fired after reconnecting
        to TWS if the client has any open orders.
        """
        pass

    def add_fill_data(self, reqId, execdetails):
        if "data_fill_data" not in dir(self):
            filldata={}
        else:
            filldata=self.data_fill_data
        if reqId not in filldata.keys():
            filldata[reqId]={}
        execid=execdetails['orderid']
        filldata[reqId][execid]=execdetails
        setattr(self, "data_fill_data", filldata)

    def add_comm_data(self, execId, commdetails):
        if "data_comm_data" not in dir(self):
            commdata={}
        else:
            commdata=self.data_comm_data
        if execId not in commdata.keys():
            commdata[execId]={}
        execid=commdetails['execId']
        commdata[execId][execId]=commdetails
        setattr(self, "data_comm_data", commdata)

    def commissionReport(self, commissionReport):
        print("CommisionReport is being called ...")
        commision=commissionReport.commision
        currency=commissionReport.currency
        execId=commissionReport.execId
        realizedPNL=commissionReport.realizedPNL
        thisyield=0 # ERROR PY yield is reserved word commissionReport.yield
        yieldRedemptionDate=commissionReport.yieldRedemptionDate
        commdetails=dict(commision=str(commision),currency=str(currency),
        execId=str(execId),realizedPNL=str(realizedPNL),theyield=str(thisyield),
        yieldRedemptionDate=str(yieldRedemptionDate))
        ## This is just execution data we've asked for
        self.add_comm_data(execId, commdetails)

    def execDetails(self, reqId, contract, execution):
        print("execDetails is being called ...")
        """
        This is called if
        a) we have submitted an order and a fill has come back
        b) We have asked for recent fills to be given to us
        We populate the filldata object and also call action_ib_fill in case we need to do something with the
          fill data
        See API docs, C++, SocketClient Properties, Contract and Execution for more details
        """
        reqId=int(reqId)
        execid=execution.execId
        exectime=execution.time
        thisorderid=int(execution.orderId)
        account=execution.acctNumber
        exchange=execution.exchange
        permid=execution.permId
        avgprice=execution.price
        cumQty=execution.cumQty
        clientid=execution.clientId
        symbol=contract.symbol
        expiry=contract.expiry
        side=execution.side
        execdetails=dict(side=str(side), times=str(exectime), orderid=str(thisorderid), qty=int(cumQty),
        price=float(avgprice), symbol=str(symbol), expiry=str(expiry), clientid=str(clientid),
        execid=str(execid), account=str(account), exchange=str(exchange), permid=int(permid))

        if reqId==FILL_CODE:
            ## This is a fill from a trade we've just done
            action_ib_fill(execdetails)

        else:
            ## This is just execution data we've asked for
            self.add_fill_data(reqId, execdetails)

    def execDetailsEnd(self, reqId):
        """
        No more orders to look at if execution details requested
        """
        setattr(self, "flag_fill_data_finished", True)

class IBclient(object):
    def __init__(self, callback,accountid="DU242089"):
        tws = EPosixClientSocket(callback)
        (host, port, clientid)=return_IB_connection_info()
        tws.eConnect(host, port, clientid)
        self.tws=tws
        self.accountid=accountid
        self.cb=callback


    def get_fills(self, reqId=MEANINGLESS_NUMBER):
        print("Getting fills...")
        assert type(reqId) is int

        self.cb.init_fill_data()
        self.cb.init_error()
        self.cb.init_time()

        self.tws.reqExecutions(reqId,ExecutionFilter())

        start_time=time.time()
        iserror=False
        finished=False

        while not finished and not iserror:
            finished=self.cb.flag_fill_data_finished
            iserror=self.cb.flag_iserror
            if (time.time() - start_time) > MAX_WAIT:
                finished=True
            pass

        if iserror:
            print ("Error happened")
            print (self.cb.error_msg)

        execlist=self.cb.data_fill_data[reqId]
        commlist=1 #self.cb.data_comm_data[reqId]
        return (execlist,commlist)


    def speaking_clock(self):
        print ("Getting the time... ")

        self.tws.reqCurrentTime()

        start_time=time.time()

        self.cb.init_error()
        self.cb.init_time()

        iserror=False
        not_finished=True


        while not_finished and not iserror:
            """
            The issue we have here is that the IB API is very much set up as an event driven process.
             So not like normal sequential code like function A calling function B and function A
             returning the answer back. No instead we have function A just kind of hanging around
             waiting for function B and then somehow by magic function B just happens.

            That is not the way I do stuff. I don't have to - I am running fairly slow trading
            systems not intraday high frequency stuff that needs to react to every tick for which
            an event driven system makes sense. Also it goes against my ethical beliefs. Why should
            function A have to wait for function B to run? What makes function B so special? Its
            just rudeness.

            So what we have to do is make the client-server relationship appear sequential, at
            least to anything sitting outside the wrapper module. That also means we need to handle
            the conditions of the thing not finishing in a reasonable time and finishing with an error.
            """
            not_finished=self.cb.data_the_time_now_is is None
            iserror=self.cb.flag_iserror

            if (time.time() - start_time) > MAX_WAIT:
                not_finished=False

            if iserror:
                not_finished=False

        if iserror:
            print ("Error happened")
            print (self.cb.error_msg)

        return self.cb.data_the_time_now_is