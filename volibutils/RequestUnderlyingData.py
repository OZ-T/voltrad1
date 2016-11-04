import swigibpy as sy

class RequestUnderlyingData():
    # indicates is the object data is completely stored for the option contract for this request
    # these are the tick types I'm requesting
    genericTicks = "100,101,106,165,225,233,236,258,411,456"
    # the following list is used to detect when I have all data requested
    ##  removed 'Halted' and 'lastSize' from this list because on RTH apparenlty are not provided by IB api
    outputListOld = ['lastOptPrice', 'modelPvDividend', 'lastPvDividend', 'bidImpliedVol', 'askImpliedVol',
                  'bidPrice', 'modelDelta', 'bidGamma', 'MarkPrice', 'modelOptPrice',
                  'closePrice', 'Volume', 'askSize', 'askPrice', 'bidSize']

    outputList = ['closePrice','bidPrice','askSize', 'askPrice', 'bidSize']
    # RequestUnderlyingData('SPX', 'IND', '', 0, '', '', 'CBOE', 'USD', 1001)
    def __init__(self,symbol,secType,expiry,strike,right,multiplier,exchange,currency,requestId):
        self.requestId = -9999
        self.isDone = False
        self.isCancelled = False
        self.c= sy.Contract()
        self.c.symbol=symbol.upper()
        self.c.secType=secType
        self.c.conId = 0
        self.c.includeExpired = False
        if expiry <> "" and expiry <> "0":
            self.c.expiry=expiry
        if int(strike) > 0:
            self.c.strike=int(strike)
        if right <> "":
            self.c.right=right
        if multiplier <> "":
            self.c.multiplier=multiplier
        if exchange <> "":
            self.c.exchange=exchange
        if currency <> "" and currency <> "XXX":
            self.c.currency=str(currency)
        self.requestId=requestId
        self.init_output_data()


    def getInstance(self):
        return self.c

    def getRequestId(self):
        return self.requestId

    def getGenericTicks(self):
        return RequestUnderlyingData.genericTicks

    def getOutputList(self):
        return RequestUnderlyingData.outputList

    def getOutputListSnap(self):
        return RequestUnderlyingData.outputListSnap

    def init_output_data(self):
        setattr(self, "output_data", {})
        setattr(self, "flag_output_data_finished", False)


    def add_out_data(self, reqId, field, outdetails):
        if "output_data" not in dir(self):
            outdata={}
        else:
            outdata=self.output_data
        if reqId not in outdata.keys():
            outdata[reqId]={}
        outdata[reqId][field]=outdetails
        #print("Current status of request (%d): keys (%s), (%s)" % ( reqId , str(outdata[reqId].keys()), str(outdata) ))
        #print("Current status of request (%d): num.elements (%d)" % ( reqId , len(outdata[reqId].keys())  ))
        setattr(self, "output_data", outdata)

    def get_in_data(self):
        indata={
                    "symbol":self.c.symbol,
                    "secType":self.c.secType,
                    "expiry":self.c.expiry,
                    "strike":self.c.strike,
                    "right":self.c.right,
                    "multiplier":self.c.multiplier,
                    "exchange":self.c.exchange,
                    "currency":self.c.currency
        }
        return indata

    def get_out_data(self):
        if "output_data" not in dir(self):
            outdata={}
        else:
            outdata=self.output_data
        return outdata
