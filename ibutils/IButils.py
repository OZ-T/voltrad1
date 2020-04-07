import pandas as pd
import numpy as np
import csv

from swigibpy import Contract

from core import config
globalconf = config.GlobalConfig()
log = globalconf.log

DEFAULT_VALUE=np.nan

class autodf(object):
    '''
    Object to make it easy to add data in rows and return pandas time series

    Initialise with autodf("name1", "name2", ...)
    Add rows with autodf.add_row(name1=..., name2=...., )
    To data frame with autodf.to_pandas
    '''

    def __init__(self, *args):


        storage=dict()
        self.keynames=args
        for keyname in self.keynames:
            storage[keyname]=[]

        self.storage=storage

    def add_row(self, **kwargs):

        for keyname in self.storage.keys():
            if keyname in kwargs:
                self.storage[keyname].append(kwargs[keyname])
            else:
                self.storage[keyname].append(DEFAULT_VALUE)

    def to_pandas(self, indexname=None):
        if indexname is not None:
            data=self.storage
            index=self.storage[indexname]
            data.pop(indexname)
            return pd.DataFrame(data, index=index)
        else:
            return pd.DataFrame(self.storage)

def bs_resolve(x):
    if x<0:
        return 'SELL'
    if x>0:
        return 'BUY'
    if x==0:
        raise Exception("trying to trade with zero")

def action_ib_fill(execlist):
    """
    Get fills (eithier ones that have just happened, or when asking for orders)

    Note that fills are cumulative, eg for an order of +10 first fill would be +3, then +9, then +10
    implying we got 3,6,1 lots in consecutive fills

    The price of each fill then is the average price for the order so far
    """

    print ("recived fill as follows:")
    print ("")
    print (execlist)
    print ("")

class OptionContract(Contract):
    def __init__(self,symbol,secType,expiry,strike,right,
                multiplier,tradingClass,exchange,primaryExchange,currency):
        super(OptionContract, self).__init__()
        self.symbol = symbol
        self.secType = secType
        self.expiry = expiry
        self.strike = float(strike)
        self.right = right
        self.exchange = exchange
        self.multiplier = multiplier
        self.currency = currency
        self.primaryExchange = primaryExchange
        self.tradingClass = tradingClass
    def __str__(self):
        return self.symbol # TO DO make a proper represnetation


def load_opt_chain_def(file):
    my_list= []
    with open(file,'r') as f:
        reader = csv.reader(f,delimiter=";")
        next(reader, None)# skip headers
        for row in reader:
            #print(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9])
            my_list.append(OptionContract(row[0],row[1],row[2],row[3],
                                            row[4],row[5],row[6],row[7],
                                            row[8],row[9]))
    return my_list
