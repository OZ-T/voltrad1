"""Volatility cones calculation methods
"""


from numpy import log, sqrt
import numpy as np
import pandas as pd
from pylab import axhline, figure, legend, plot, show

def main(ticker):
    """
    Main calculation method     
    """
    prices = pd.read_csv(ticker+'.csv', index_col=0, parse_dates=True)
    prices.sort_index(inplace=True)
if __name__ == '__main__':
    main(ticker='AAPL')