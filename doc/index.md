# Documentation

*"CHECK ALT. DATA"* - **H. Clifford McBride** (astronaut), Ad Astra

## Trading plan

Operate when the prices are in **lateral ranges**. How to search for lateral range in prices in practice?

Don't seek to find out the price.

Risk control is fundamental for trading.

Estimate the **success probability for a strategy** (ventaja). How to estimate the sucess probability of a TIC strategy.
This is a clear task for a predictive model (NN scipy `tensorflow` of whatever or some kind of backtesting) based on the data provided
estimate the probability of sucess, maybe use python backtestesters like `zipline` or `zorro`?? (to be developed in JIRA VOL-18)


### Template to use for price levels:

- Price of underlying
- **SMA(20)** (to be developed in JIRA VOL-15)
- **SMA(50)** (to be developed in JIRA VOL-15)
- **SMA(200)** (to be developed in JIRA VOL-15)
- **pearson Pivot points monthly** (where to find this??) these are probably optional (to be developed in JIRA VOL-15)
- **pearson Pivot points weekly** (where to find this??) (to be developed in JIRA VOL-15)
- **probability cone** (to be developed in JIRA VOL-19)

For example SMA(50) is calculated as follows:

`df['SMA(50)'] = df.GLD.rolling(50).mean() `

Mean Implied Volatility for each underlying, how to calculate it? 
    
    --> use as proxy the IV for options with days to expirations between 30 and 45 days go to the ATM strikes and take the mean

    --> or use the VIX for SPX ES and SPY

Calculate the range using the volatility cones which is crucial for the operations.

Take all the risk measures and create indicator variables when those risk indicators are activated. for example SMA(50):

`df['SMA(50)'] = df.GLD.rolling(50).mean() `

if the price is below the SMA(50) then the risk indicator is activated.

this indicator is only applicable to stocks (it works for SPY but not for FXE for example) we can check that of course
so in stocks if the prices closes below the SMA(50) in a given day, the next trading day there is high prob thhat the market will have a negative return.

Search for the volatility cones code (dropbox folder or in this repo)


## Measures

The following measures are calculated:

   - Deltas, gammas, thetas, vegas are calculateed using quanlib python in the following function: [bsm_price_and_greeks]( ../valuations/opt_pricing_methods.py )
   - Prediction capability in a random market (Shannon) [sto_cycles_probability](../core/analytics_methods.py)
   - Coppock:
   - Mcclelan:
   - SMA(x): [sma](../core/analytics_methods.py)
   - Probability cones, volatility cones:
   - Pearson pivot:
   - Time to maturity:
   - *"In the money" Implied Volatility*: 
   - Mean IV of an underlying:
   - Sucess probability of an strategy: this is the key measure probably, in TOS is given by the platform.



## Operations

the file config/commandlist.yaml contains the commands to operate the system. The following is a description
of the workflow for operations with TIC:

### TREND ANALYSIS

1.- Check daily Coopock for SPX with command: . p coppock
        Check what coppock has to say about the market trend: positive&down, negative&up, ...

2.- Check Daily instrument summary (YTD, MTD, WTD, DD) for SPX: . p summary "SPX"

### RISK ANALYSIS

1.- Check EMAS and IV Channels for SPX: . p emas "SPX"
        Check if there are alerts activated for RSK_EMA50 or CANAL_IV_WK or CANAL_IV_MO
        price tend to be outside these channel a "short" time

2.- Check volatility levels: . p vol "SPX"
        Check if the level of IV (VIX) is inside or outside of bollinger bands (1sd and 2sd)
        so IV will be classified as high, low, extreme high or extreme low

3.- Check rapid movement indicator: . p fastmove "SPX"        

### PRE-TRADE TIC ANALYSIS

1.- Check DTE and find suitable expiry, also check options with deltas 10-15 accoridng to trend:
    Command:  . p lstchain "20170127" "ES" "10,15" "-15,-10" "20170421" "trades"
			  ['val_dt', 'symbol', 'call_d_range', 'put_d_range', 'expiry', 'type']
    This shows the DT for the expiry passed as parameter and filter option chain by deltas
    Check the size of bid ask 

2.- Once choosen suitable options candidates for trade based on DTE and deltas, check recent historical
    prices of the candidates to find/guest best time to open position
    Command:  	
                Type should be bid, ask or trades
                lst_right_strike like "P2200.0,P2225.0,C2300.0"
                Arguments:
                ['start_dt', 'end_dt', 'symbol', 'lst_right_strike', 'expiry', 'type']
     TODO: Possibly check the indraday coppock to search for an intraday down trend to enter

3.- Simulate what would look like the TIC is traded at last available Market prices

    Basically run a shark tic report with the simulated portfolio.
    The simulated portfolio is passed as a string "P237.0,P238.0,C240.0,C241.0"
    Command: . p simul_tic