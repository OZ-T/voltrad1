import pandas as pd
import sqlite3
from volsetup.logger import logger
from volsetup import config
import os
from matplotlib.pyplot import *
import datetime as dt
import matplotlib as plt

report_dict_ib = {
    "symbols":['ES','SPY'],
    "expiries":['2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01'],
    "db_type":"optchain_ib",
    "variables":['lastOptPrice', 'bidImpliedVol',
           'modelTheta', 'askImpliedVol', 'bidPrice',
           'modelDelta', 'bidGamma', 'modelOptPrice', 'closePrice', 'highPrice',
           'askSize', 'CallOI', 'askOptPrice', 'Volume', 'modelImpliedVol',
           'lastTheta', 'askUndPrice', 'bidTheta', 'modelUndPrice',
           'askGamma', 'bidDelta', 'askVega', 'bidVega', 'modelVega',
           'lastUndPrice', 'bidOptPrice',
           'askPrice', 'bidSize', 'lastDelta', 'askDelta',
            'askTheta', 'lowPrice',
           'modelGamma', 'lastImpliedVol', 'lastGamma', 'PutOI', 'bidUndPrice',
           'lastVega', 'lastSize', 'lastPrice' ],
    'current_datetime':'current_datetime',
    'symbol':'symbol',
    'right':'right',
    'strike':'strike',
    'expiry':'expiry',
    'format_index':"%Y%m%d%H%M%S",
    'valid_rights': ['P', 'C']
}

report_dict_yhoo = {
    "symbols":['USO','SPY'],
    "expiries":['2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01'],
    "db_type":"optchain_yhoo",
    "variables":['lastOptPrice', 'bidImpliedVol',
           'modelTheta', 'askImpliedVol', 'bidPrice',
           'modelDelta', 'bidGamma', 'modelOptPrice', 'closePrice', 'highPrice',
           'askSize', 'CallOI', 'askOptPrice', 'Volume', 'modelImpliedVol',
           'lastTheta', 'askUndPrice', 'bidTheta', 'modelUndPrice',
           'askGamma', 'bidDelta', 'askVega', 'bidVega', 'modelVega',
           'lastUndPrice', 'bidOptPrice',
           'askPrice', 'bidSize', 'lastDelta', 'askDelta',
            'askTheta', 'lowPrice',
           'modelGamma', 'lastImpliedVol', 'lastGamma', 'PutOI', 'bidUndPrice',
           'lastVega', 'lastSize', 'lastPrice' ],
    'current_datetime':'Quote_Time',
    'symbol':'Symbol',
    'right':'Type',
    'strike':'strike',
    'expiry':'expiry',
    'format_index':"%Y%m%d%H%M%S",
    'valid_rights':['put','call']
}


def run_dq_report_market(hoy):
    globalconf = config.GlobalConfig()
    dict1 = read_opt_chain_data(globalconf,hoy,report_dict_ib)
    save_image_plot_lines_multi_strike(globalconf, dict1)

def read_opt_chain_data(globalconf,hoy,r_dict):
    ayer = (dt.datetime.strptime(hoy, '%Y%m%d') - dt.timedelta(1)).strftime('%Y%m%d')
    fecha = (hoy, ayer)
    log = logger("dq_report")
    symbols = r_dict['symbols'] # ['ES','SPY']
    expiries = r_dict['expiries'] # ['2017-06','2017-07','2017-08','2017-09','2017-10','2017-11','2017-12','2018-01']
    db_type = r_dict['db_type'] # "optchain_ib"
    path = globalconf.config['paths']['data_folder']
    variables = r_dict['variables']
           #  ['lastOptPrice', 'bidImpliedVol',
           # 'modelTheta', 'askImpliedVol', 'bidPrice',
           # 'modelDelta', 'bidGamma', 'modelOptPrice', 'closePrice', 'highPrice',
           # 'askSize', 'CallOI', 'askOptPrice', 'Volume', 'modelImpliedVol',
           # 'lastTheta', 'askUndPrice', 'bidTheta', 'modelUndPrice',
           # 'askGamma', 'bidDelta', 'askVega', 'bidVega', 'modelVega',
           # 'lastUndPrice', 'bidOptPrice',
           # 'askPrice', 'bidSize', 'lastDelta', 'askDelta',
           #  'askTheta', 'lowPrice',
           # 'modelGamma', 'lastImpliedVol', 'lastGamma', 'PutOI', 'bidUndPrice',
           # 'lastVega', 'lastSize', 'lastPrice' ]
    files = [x[2] for x in os.walk(path) if (x[2]) ][0]
    filtered_files = [x for x in files if x.startswith(db_type)]
    final_list = []
    for file in filtered_files:
        for expiry in expiries:
            if expiry in file:
                final_list.append(file)
    log.info(("final_list = ", final_list))
    return_df_dict = {}
    for repo in final_list:
        log.info("repo = " + repo)
        store = sqlite3.connect(path + repo)
        for symbol in symbols:
            log.info("symbol = " + symbol)
            df1 = pd.read_sql_query("SELECT * FROM " + symbol
                                    + " where substr(current_datetime,1,8) in " + str(fecha), store)
            df1.sort_values(by=[ r_dict['current_datetime'] ], inplace=True)
            log.info("len(df1) = %d " % (len(df1)) )
            df1['optsymbol'] = df1[r_dict['right']].astype(str).str.cat(df1[r_dict['strike']].astype(str))
            for right in r_dict['valid_rights']:
                df2 = df1[ (df1[r_dict['right']] == right) ]
                for variable in variables:
                    df2 = df2.drop_duplicates(subset=[r_dict['current_datetime'], r_dict['symbol'],
                                                      r_dict['right'], r_dict['strike'],r_dict['expiry']], keep='last')
                    # transponer y poner en cada columna un precio
                    #                   C       ....
                    #                   2000.0  ...
                    #                   Ask    Bid IVBid IVAsk ...
                    df3 = df2.pivot(index=r_dict['current_datetime'], columns='optsymbol', values=variable)
                    df3.index = pd.to_datetime(df3.index, format=r_dict['format_index'])
                    df3 = df3.loc[:,:].apply(pd.to_numeric, errors='coerce')
                    title = str(variable+"_"+symbol+"_"+right+"_"+repo.replace(".db", "").replace("optchain_", ""))
                    return_df_dict[title] = df3
    return return_df_dict


def save_image_plot_lines_multi_strike(globalconf,dict_df):
    plt.rcParams.update({'figure.max_open_warning': 0})
    web_server = globalconf.config['paths']['nginx_static_folder']
    for title, df3 in dict_df.items():
        if not df3.empty:
            fig, ax = subplots()
            xticks = df3.index
            ax.set_xticklabels([x.strftime('%Y-%m-%d %H') for x in xticks])
            ax.set_xticks(np.arange(len(df3)), minor=False)
            t_plot = df3.plot(x=df3.index.to_series().dt.strftime('%Y-%m-%d %H'),
                              figsize=(16, 12),
                              ax=ax,
                              grid=True,
                              # https://matplotlib.org/examples/color/colormaps_reference.html
                              colormap='gist_ncar',
                              # xticks=xticks.to_pydatetime(),
                              # xticks=[x for x in df3.index.to_series().dt.strftime('%Y-%m-%d %H')],
                              rot=45,
                              title=title).legend(ncol=4, loc='center left',
                                                        bbox_to_anchor=(1, 0.5))

            handles, labels = ax.get_legend_handles_labels()
            lgd = ax.legend(handles, labels, loc='center left', ncol=4, bbox_to_anchor=(1, 0.5))
            ax.grid('on')
            fig = t_plot.get_figure()
            fig.savefig(web_server + title + ".png",
                        bbox_extra_artists=(lgd,), bbox_inches='tight')


