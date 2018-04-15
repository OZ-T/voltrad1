"""@package docstring
"""

# sacado del libro python for finance

import numpy as np

def bsm_mcs_euro(s0,k,t,r,sigma,num_simulations,type):
    # s0 initial index level, k strike, r interest rate, sigma, type C or P
    np.random.seed(1000)
    z = np.random.standard_normal(num_simulations)
    sT = s0 * np.exp((r - 0.5 * sigma ** 2) * t + sigma * np.sqrt(t) * z)
    if type == 'C':
        hT = np.maximum(sT - k , 0)
    elif type == 'P':
        hT = np.maximum(k  - sT , 0)
    return1 = np.exp(-r * t) * np.sum(hT) / num_simulations
    return return1

from scipy import stats
import math

# Option Parameters
S0 = 105.00 # initial index level
K = 100.00 # strike price
T = 1. # call option maturity
r = 0.05 # constant short rate
vola = 0.25 # constant volatility factor of diffusion
# Analytical Formula
def BSM_call_value(S0, K, T, r, vola):
    ''' Analytical European call option value for Black-Scholes-Merton (1973).
        Parameters
        ==========
        S0: float
        initial index level
        K: float
        strike price
        T: float
        time-to-maturity
        r: float
        constant short rate
        vola: float
        constant volatility factor
        Returns
        =======
        call_value: float
        European call option present value
    '''
    S0 = float(S0) # make sure to have float type
    d1 = (math.log(S0 / K) + (r + 0.5 * vola ** 2) * T) / (vola * math.sqrt(T))
    d2 = d1 - vola * math.sqrt(T)
    call_value = (S0 * stats.norm.cdf(d1, 0.0, 1.0) - K * math.exp(-r * T) * stats.norm.cdf(d2, 0.0, 1.0))
    return call_value



if __name__ == "__main__":
    c0=bsm_mcs_euro(s0=100, k=105, t=1.0, r=0.005, sigma=0.2, num_simulations=100000,type='C')
    print("Value of the European Call Option is %5.3f" % (c0))

