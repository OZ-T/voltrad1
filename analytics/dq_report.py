import pandas as pd
import sqlite3
from volsetup.logger import logger
from volsetup import config
import os
from matplotlib.pyplot import *
import datetime as dt
import matplotlib as plt

def run_dq_report_market(hoy):
    plt.rcParams.update({'figure.max_open_warning': 0})
    ayer = (dt.datetime.strptime(hoy, '%Y%m%d') - dt.timedelta(1)).strftime('%Y%m%d')
    fecha = (hoy, ayer)

    globalconf = config.GlobalConfig()
    log = logger("dq_report")

    symbols = ['ES','SPY']
    expiries = ['2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01']
    db_type = "optchain_ib"
    path = globalconf.config['paths']['data_folder']
    web_server = globalconf.config['paths']['nginx_static_folder']

    variables = ['lastOptPrice', 'bidImpliedVol',
           'modelTheta', 'askImpliedVol', 'bidPrice',
           'modelDelta', 'bidGamma', 'modelOptPrice', 'closePrice', 'highPrice',
           'askSize', 'CallOI', 'askOptPrice', 'Volume', 'modelImpliedVol',
           'lastTheta', 'askUndPrice', 'bidTheta', 'modelUndPrice',
           'askGamma', 'bidDelta', 'askVega', 'bidVega', 'modelVega',
           'lastUndPrice', 'bidOptPrice',
           'askPrice', 'bidSize', 'lastDelta', 'askDelta',
            'askTheta', 'lowPrice',
           'modelGamma', 'lastImpliedVol', 'lastGamma', 'PutOI', 'bidUndPrice',
           'lastVega', 'lastSize', 'lastPrice' ]
    files = [x[2] for x in os.walk(path) if (x[2]) ][0]
    filtered_files = [x for x in files if x.startswith(db_type)]
    final_list = []
    for file in filtered_files:
        for expiry in expiries:
            if expiry in file:
                final_list.append(file)
    log.info(("final_list = ", final_list))
    for repo in final_list:
        log.info("repo = " + repo)
        store = sqlite3.connect(path + repo)
        for symbol in symbols:
            log.info("symbol = " + symbol)
            df1 = pd.read_sql_query("SELECT * FROM " + symbol
                                    + " where substr(current_datetime,1,8) in " + str(fecha), store)
            df1.sort_values(by=['current_datetime'], inplace=True)
            log.info("len(df1) = %d " % (len(df1)) )
            df1['optsymbol'] = df1.right.astype(str).str.cat(df1.strike.astype(str))
            for right in ['C','P']:
                df2 = df1[ (df1['right'] == right) ]
                for variable in variables:
                    df2 = df2.drop_duplicates(subset=['current_datetime', 'symbol', 'right', 'strike','expiry'], keep='last')
                    # transponer y poner en cada columna un precio
                    #                   C       ....
                    #                   2000.0  ...
                    #                   Ask    Bid IVBid IVAsk ...
                    df3 = df2.pivot(index='current_datetime', columns='optsymbol', values=variable)
                    df3.index = pd.to_datetime(df3.index, format="%Y%m%d%H%M%S")
                    df3 = df3.loc[:,:].apply(pd.to_numeric, errors='coerce')
                    if not df3.empty:
                        fig, ax = subplots()
                        xticks = df3.index
                        ax.set_xticklabels([x.strftime('%Y-%m-%d %H') for x in xticks])
                        ax.set_xticks(np.arange(len(df3)), minor=False)
                        t_plot = df3.plot(x=df3.index.to_series().dt.strftime('%Y-%m-%d %H'),
                                             figsize=(16,12),
                                             ax=ax,
                                             grid=True,
                                             # https://matplotlib.org/examples/color/colormaps_reference.html
                                             colormap='gist_ncar',
                                             # xticks=xticks.to_pydatetime(),
                                             # xticks=[x for x in df3.index.to_series().dt.strftime('%Y-%m-%d %H')],
                                             rot=45,
                                             title=str((variable,
                                                        right,
                                                        symbol,
                                                        repo))).legend(ncol=4,loc='center left',
                                                                                             bbox_to_anchor=(1, 0.5))

                        handles, labels = ax.get_legend_handles_labels()
                        lgd = ax.legend(handles, labels, loc='center left',ncol=4, bbox_to_anchor=(1, 0.5))
                        ax.grid('on')
                        fig = t_plot.get_figure()
                        fig.savefig( web_server + variable + symbol + right
                                     + repo.replace(".db","").replace("optchain_ib_expiry","") + ".png",
                                     bbox_extra_artists=(lgd,), bbox_inches='tight')




