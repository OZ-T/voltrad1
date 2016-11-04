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

if __name__ == "__main__":
    c0=bsm_mcs_euro(s0=100, k=105, t=1.0, r=0.05, sigma=0.2, num_simulations=100000,type='C')
    print("Value of the European Call Option is %5.3f" % (c0))