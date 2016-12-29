import swigibpy as sy

class RequestOptionData():
    # these are the tick types I'm requesting
    genericTicks = "100,101,106,165,225,233,236,258,411,456"
    # these are ALL the valid tick types for FOP (options on futures, lie ES options)
    genericTicksExt = "100,101,105,106,107,125,165,166,225,232,233,236,258,291,293,294,295,318,370,370,377,377,381,384,384,387,388,391,405,407,411,428,439,439,456,459,460,499,506,511,512,513,514,515,516,517,521,545,576,577,578,584,585,587"
    # the following list is used to detect when I have all data requested
    ##  removed 'Halted' and 'lastSize' from this list because on RTH apparenlty are not provided by IB api
    ##     'lastOptPrice' , 'MarkPrice'
    outputList = [ 'modelPvDividend', 'lastPvDividend', 'bidImpliedVol', 'askImpliedVol',
                  'bidPrice', 'modelDelta', 'bidGamma', 'modelOptPrice',
                  'closePrice', 'lastGamma', 'askOptPrice', 'Volume', 'modelImpliedVol', 'lastTheta',
                  'askUndPrice', 'bidTheta', 'modelUndPrice', 'askGamma', 'bidDelta', 'askVega', 'bidVega',
                  'askSize', 'modelVega', 'askPrice', 'lastUndPrice', 'bidOptPrice', 'bidSize', 'lastDelta',
                  'askDelta', 'modelTheta', 'askPvDividend', 'bidPvDividend', 'askTheta', 'modelGamma', 'lastImpliedVol',
                  'CallOI', 'PutOI', 'bidUndPrice', 'lastVega']

    outputListSnap = ['bidPrice','askSize', 'askPrice', 'bidSize']

    def __init__(self,symbol,secType,expiry,strike,right,multiplier,exchange,currency,requestId):
        # indicates is the object data is completely stored for the option contract for this request
        self.requestId = -9999
        self.isDone = False
        self.isCancelled = False
        self.optionsChain = []
        self.c= sy.Contract()
        self.c.symbol=symbol.upper()
        self.c.secType=secType
        self.c.conId = 0
        self.c.includeExpired = False
        if expiry <> "":
            self.c.expiry=expiry
        self.c.strike=int(strike)
        if right <> "":
            self.c.right=right
        if multiplier <> "":
            self.c.multiplier=multiplier
        if exchange <> "":
            self.c.exchange=exchange
        if currency <> "":
            self.c.currency=str(currency)
        self.requestId=requestId
        self.init_output_data()


    def getInstance(self):
        return self.c

    def getRequestId(self):
        return self.requestId

    def getGenericTicks(self):
        return RequestOptionData.genericTicks

    def getOutputList(self):
        return RequestOptionData.outputList

    def getOutputListSnap(self):
        return RequestOptionData.outputListSnap

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

    def __str__(self):
        return str(self.get_in_data()) + " " + str(self.get_out_data())

    def __repr__(self):
        return str(self.get_in_data()) + " " + str(self.get_out_data())

