"""
Configuration classes and methods for Voltrad1
"""

import platform
import os
import pandas as pd
try:
        import configparser
except:
        from six.moves import configparser
from sys import platform
import sqlalchemy
from core.logger import logger
import sqlite3

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# This class implements the singleton pattern
#Python3
class GlobalConfig(object, metaclass=Singleton):
    def __init__(self):
        level = logger.DEBUG
        # first try to find config file in user home (ubuntu)
        self.log = logger("voltrad1", level)
        path1=os.path.expanduser("~")
        self.linux_user1=""
        if "linux" in platform.lower():
            #self.linux_user1=os.getenv("USER")
            self.linux_user1=os.popen('whoami').read().rstrip()

        if self.linux_user1.lower() == "root":
            path1="/var/www"
        config_file = os.path.join(path1, '.voltrad1')
        if not os.path.exists(config_file):
            # use the config file in the user folder (windows)
            config_file = os.path.join(os.path.expanduser("~"), 'voltrad1.ini')
        config = configparser.ConfigParser()
        config.read(config_file)
        self.config=config._sections
        self.months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

    def __str__(self):
        return repr(self)

    def get_logger(self):
        """
        return logger object        
        """
        return self.log

    def log_info(self,string):
        """
        log info message
        :param string: 
        :return: 
        """
        self.log.info(string)

    def get_config_csv(self,name,path,sep=','):
        """
        This is to load into a pandas DataFrame any of the csv with the list of contracts
        located in voltrad1/config folder.
        This method is only used inside this class GlobalConfig
        :param name: name of the csv
        :param path: path of the csv
        :param sep:
        :return:
        """
        file= path + name
        return  pd.DataFrame.from_csv( path =file, sep=sep )

    def get_tickers_optchain_yahoo(self):
        """
        This returns as a pandas dataframe the list of ETFs / indexes for yahoo finance download
        :return:
        """
        name=self.config['use_case_yahoo_options']['underlying_list']
        path=self.config['paths']['config_folder']
        return self.get_config_csv(name,path,sep=",")

    def get_tickers_optchain_ib(self):
        """
        get_tickers_optchain_ib
        :return: 
        """
        name=self.config['use_case_ib_options']['underlying_list']
        path=self.config['paths']['config_folder']
        return self.get_config_csv(name,path,sep=",")

    def get_tickers_historical_ib(self):
        """
        get_tickers_historical_ib
        :return: 
        """
        name=self.config['use_case_historical_ib']['underlying_list']
        path=self.config['paths']['config_folder']
        return self.get_config_csv(name,path,sep=",")



    def open_historical_optchain_store(self):
        """
        open_historical_optchain_store
        :return: 
        """
        name = self.config['db']['hdf5_historical_chain_db']
        path=self.config['paths']['data_folder']
        return pd.HDFStore(path + name)

    def open_ib_abt_strategy_tic(self,scenarioMode):
        """
        open_ib_abt_strategy_tic
        :param scenarioMode: If the valuation core is run against real portfolio data (paper or real account)
                                or against simulated orders & portfolio. In the later case the portfolio and orders are
                                read not from the H5 db but from customized excel templates
                                The real historical market data is always used in both cases though
        :return:
        """
        if scenarioMode == "N":
            name = self.config['sqllite']['strategy_tic']
        elif scenarioMode == "Y":
            name = self.config['sqllite']['strategy_tic_simul']
        path=self.config['paths']['analytics_folder']

        return pd.HDFStore(path + name)

    def open_economic_calendar_h5_store(self):
        """
        open_economic_calendar_h5_store
        :return: 
        """
        name=self.config['db']['hdf5_economic_calendar_db']
        path=self.config['paths']['data_folder']
        return pd.HDFStore(path + name)


    def portfolio_dataframe_simulation(self,simulName):
        name=self.config['simulation']['simulation_template']
        path=self.config['simulation']['data_folder']
        dataframe = pd.read_excel(open(path + name, 'rb'), sheetname='portfolio_'+simulName)
        return dataframe

    def account_dataframe_simulation(self,simulName):
        name=self.config['simulation']['simulation_template']
        path=self.config['simulation']['data_folder']
        dataframe = pd.read_excel(open(path + name, 'rb'), sheetname='account_'+simulName)
        return dataframe


    def orders_dataframe_simulation(self,simulName):
        name=self.config['simulation']['simulation_template']
        path=self.config['simulation']['data_folder']
        dataframe = pd.read_excel(open(path + name, 'rb'), sheetname='orders_'+simulName)
        return dataframe


    def open_ivol_h5_db(self):
        name="optchain_ivol_hist_db_new.h5"  #self.config['use_case_ivolatility']['hdf5_db_nm']
        path=self.config['paths']['data_folder']
        #return h5.File(path + name)
        return pd.HDFStore(path + name)

    def get_accountid(self):
        name=self.config['ib_api']['accountid']
        return str(name)

    def return_IB_connection_info(self):
        """
        Returns the tuple host, port, clientID required by eConnect
        """
        host=self.config['ib_api']['host']
        port=self.config['ib_api']['port']
        clientid=self.config['ib_api']['clientid']
        return (host, port, clientid)

    def get_list_data_columns_ib(self):
        return (self.config['use_case_ib_options']['data_columns']).split(',')

    def get_list_errors_to_trigger_ib(self):
        """
        Any errors not on this list we just treat as information
        """
        return (self.config['ib_api']['errors_to_trigger']).split(',')


    def connect_sqldb(self):
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        user = self.config['sqldb']['user']
        password = self.config['sqldb']['password']
        host = self.config['sqldb']['host']
        db = self.config['sqldb']['db']
        port = self.config['sqldb']['port']
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)
        # The return value of create_engine() is our connection object
        con = sqlalchemy.create_engine(url, client_encoding='utf8')
        # We then bind the connection to MetaData()
        meta = sqlalchemy.MetaData(bind=con, reflect=True)
        return con, meta

    def connect_sqllite(self,name):
        """
        Returns a connection to a sqllite DB
        :return:
        """
        conn = sqlite3.connect(name)
        return conn

if __name__ == "__main__":
    object = GlobalConfig();
    #datacols = (object.config['use_case_ib_options']['data_columns']).split(',')
    #print(datacols)
    #print(object.get_tickers_optchain_ib())
    #print(object.get_tickers_optchain_yahoo())
    print( object.get_list_errors_to_trigger_ib())
