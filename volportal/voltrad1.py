from flask import Flask, request, render_template
from voldailyanalytics import run_analytics as ra
from flask_restful import reqparse, abort, Api, Resource
import os,sys
import pandas as pd
import json
from volsetup import config
from datetime import datetime

from pandas.io.json import json_normalize

globalconf = config.GlobalConfig()

app = Flask(__name__)
api = Api(app)

DATA_DIR = globalconf.config['paths']['data_folder'] # "/home/david/Dropbox/proyectos/data"
exts= (".hdf",".hdf5",".h5",".he5")

parser = reqparse.RequestParser()
parser.add_argument('task')

class H5Dir(Resource):
    def get(self):
        dir=os.path.abspath(DATA_DIR)
        print (dir)
        #walk through data directory
        list = []
        for root, dirs, files in os.walk(dir):
            for name in files:
                filename=os.path.join(root,name)
                if filename.lower().endswith(exts):
                    print "filename" , filename
                    list.append(name)
        return json.dumps(list)

class H5AbtOptChain(Resource):
    def get(self,node,date_ini,date_end,localsymbol):
        dini=datetime.strptime(date_ini, '%Y_%m_%d')
        dend=datetime.strptime(date_end, '%Y_%m_%d')
        DIR = os.path.abspath(DATA_DIR)
        store = globalconf.open_table("abt_optchain_ib_hist_db.h5")
        nodeh5 = store.get_node(node)
        df1 = store.select(nodeh5._v_pathname,where=['index < TODO '])
        return df1.to_json(orient='records')


class H5Gekko(Resource):
    def get(self):
        dir=os.path.abspath(DATA_DIR)
        xls = pd.ExcelFile(dir+'/gekkomock.xlsx')
        df1 = xls.parse('test')

        return df1.to_json(orient='records')

class H5Table(Resource):
    def get(self,name,start,end,node):
        DIR = os.path.abspath(DATA_DIR)
        store=globalconf.open_table(name+".h5")
        #account_db_new
        nodeh5=store.get_node(node) #globalconf.config['ib_api']['accountid'])
        df1=store.select(nodeh5._v_pathname) # ,columns=['AccountCode_','AvailableFunds_USD','current_datetime_txt']
        return df1.iloc[start:end].to_json(orient='records')
        """
        orient : string
        Series
        default is index
        allowed values are: {split,records,index}
        DataFrame
        default is columns
        allowed values are: {split,records,index,columns,values}
        The format of the JSON string
        split : dict like {index -> [index], columns -> [columns], data -> [values]}
        records : list like [{column -> value}, ... , {column -> value}]
        index : dict like {index -> {column -> value}}
        columns : dict like {column -> {index -> value}}
        values : just the values array

        """

#@app.route('/tic')
#def index():
#	 return render_template("index.html")

# localsymbol is underlying symbol / Call/Put (right) / Expiry / Strike eg ES20161021C2025
api.add_resource(H5AbtOptChain, '/tic/h5abtoptchain/<node>/<date_ini>/<date_end>/<localsymbol>')  # http://localhost:9001/h5abtoptchain/ES/2016_08_23/2016_09_23
api.add_resource(H5Dir, '/tic/h5dir') #http://localhost:9001/h5dir
api.add_resource(H5Table, '/tic/h5table/<name>/<int:start>/<int:end>/<node>') # http://localhost:9001/h5table/optchain_ivol_hist_db_new/395/396/IVOL
# otro ejemplo https://blablio.tech/tic/h5table/economic_calendar_db/0/3/2016

#Gekko index API service
api.add_resource(H5Gekko, '/tic/gekko')

@app.route("/analytics")
def analytics():
	dataframe=ra.extrae_account_snapshot_new(year=2016,month=10,day=11,accountid="???")
	return dataframe.to_html()#
	
if __name__ == '__main__':
	app.run(host="0.0.0.0", port=9001,debug=True)
