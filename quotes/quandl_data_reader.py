import quandl

from core import config
globalconf = config.GlobalConfig()
log = globalconf.log


quandl.ApiConfig.api_key = globalconf.config['quandl']['key']

#df = quandl.get('URC/NASDAQ_UNCH')

df = quandl.get('URC/NASDAQ_UNCH', start_date='2018-05-02', end_date='2018-05-04')

print(df)

# CREAR EL MCCLLELAN oscilator con los datos de Quandl en
#   https://www.reddit.com/r/Python/comments/49wyzu/ive_been_trying_to_code_a_formula_pythonically/
#
# Datos de entrada de Unicorn Research Corporation
#   https://www.quandl.com/data/URC-Unicorn-Research-Corporation

# URC/NASDAQ_UNCH       Number of Stocks with Prices Unchanged
# URC/AMEX_UNCH         Number of Stocks with Prices Unchanged
# URC/AMEX_UNC        Number of Stocks with Prices Unchanged
# URC/NYSE_UNCH       Number of Stocks with Prices Unchanged
# URC/NYSE_UNC       Number of Stocks with Prices Unchanged
# URC/NASDAQ_UNC       Number of Stocks with Prices Unchanged
# URC/NASDAQ_DEC       number of stocks  w price declining
# URC/NYSE_DEC       number of stocks  w price declining
# URC/AMEX_DEC      number of stocks  w price declining
# URC/NASDAQ_ADV       number of stocks  w price advancing
# URC/AMEX_ADV       number of stocks  w price advancing
# URC/NYSE_ADV       number of stocks  w price advancing
# URC/NASDAQ_52W_LO     Number of Stocks Making 52-Week Lows
# URC/AMEX_52W_LO    Number of Stocks Making 52-Week Lows
# URC/NYSE_52W_LO    Number of Stocks Making 52-Week Lows
# URC/AMEX_52W_HI    Number of Stocks Making 52-Week highs
# URC/NASDAQ_52W_HI  Number of Stocks Making 52-Week Highs
# URC/NYSE_52W_HI  Number of Stocks Making 52-Week Highs
# URC/NASDAQ_UNCH_VOL   volume of stocks with price unchanged
# URC/AMEX_UNCH_VOL  volume of stocks with price unchanged
# URC/NYSE_UNCH_VOL  volume of stocks with price unchanged
# URC/NASDAQ_DEC_VOL    vol of stocks w prices declining
# URC/AMEX_DEC_VOL    vol of stocks w prices declining
# URC/NYSE_DEC_VOL    vol of stocks w prices declining
# URC/NASDAQ_ADV_VOL    vol of stocks w prices advancing
# URC/AMEX_ADV_VOL    vol of stocks w prices advancing
# URC/NYSE_ADV_VOL    vol of stocks w prices advancing



