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

    genericTicksBAG = "100,101,106,165,225,233,258,411"
    """
    Error validating request:-'bm' : cause - Incorrect generic tick list of 100,101,106,165,225,233,236,258,411,456.
    Legal ones for (BAG) are:
    100(Option Volume),
    101(Option Open Interest),
    105(Average Opt Volume),
    106(impvolat),
    107(climpvlt),
    125(Bond analytic data),
    165(Misc. Stats),
    166(CScreen),
    221/220(Creditman Mark Price),
    225(Auction),232/221(Pl Price),
    233(RTVolume),236(inventory),
    258/47(Fundamentals),
    291(ivclose),
    293(TradeCount),
    294(TradeRate),
    295(VolumeRate),
    318(LastRTHTrade),
    370(ParticipationMonitor),
    370(ParticipationMonitor),
    375(RTTrdVolume),
    377(CttTickTag),
    377(CttTickTag),
    381(IB Rate),
    384(RfqTickRespTag),
    384(RfqTickRespTag),
    387(DMM),388(Issuer Fundamentals),
    391(IBWarrantImpVolCompeteTick),
    405(Index Capabilities),
    407(FuturesMargins),
    411(rthistvol),
    428(Monetary Close Price),
    439(MonitorTickTag),
    439(MonitorTickTag),
    459(RTCLOSE),
    460(Bond Factor Multiplier),
    499(Fee and Rebate Rate),
    506(midptiv),
    511(hvolrt10 (per-underlying)),
    512(hvolrt30 (per-underlying)),
    513(hvolrt50 (per-underlying)),
    514(hvolrt75 (per-underlying)),
    515(hvolrt100 (per-underlying)),
    516(hvolrt150 (per-underlying)),
    517(hvolrt200 (per-underlying)),
    521(fzmidptiv),545(vsiv),
    576(EtfNavBidAsk(navbidask)),
    577(EtfNavLast(navlast)),
    578(EtfNavClose(navclose)),
    584(Average Opening Vol.),
    585(Average Closing Vol.),
    587(Pl Price Delayed),
    588(Futures Open Interest),
    608(EMA N),
    614(EtfNavMisc(hight/low)),
    619(Creditman Slow Mark Price),
    623(EtfFrozenNavLast(fznavlast))] [100]

    """


    def __init__(self,symbol,secType,expiry,strike,right,multiplier,exchange,currency,requestId,comboLegs=None,contract=None):
        # indicates is the object data is completely stored for the option contract for this request
        self.requestId = -9999
        self.isDone = False
        self.isCancelled = False
        self.optionsChain = []
        if contract is None:
            self.c= sy.Contract()
            self.c.symbol = symbol.upper()
            self.c.secType = secType
            self.c.conId = 0
            self.c.includeExpired = False
            if expiry != "":
                self.c.expiry = expiry
            self.c.strike = int(strike)
            if right != "":
                self.c.right = right
            if multiplier != "":
                self.c.multiplier = multiplier
            if exchange != "":
                self.c.exchange = exchange
            if currency != "":
                self.c.currency = str(currency)
        else:
            self.c = contract
        if comboLegs is not None:
            self.c.comboLegs=comboLegs
        self.requestId=requestId
        self.init_output_data()


    def getInstance(self):
        return self.c

    def getRequestId(self):
        return self.requestId

    def getGenericTicks(self):
        if self.c.secType == "BAG":
            return1=RequestOptionData.genericTicksBAG
        else:
            return1=RequestOptionData.genericTicks
        return return1

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
                    "currency":self.c.currency,
                    "comboLegsDescrip":self.c.comboLegsDescrip
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

