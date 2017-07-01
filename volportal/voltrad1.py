from flask import Flask, request, render_template
from core import run_analytics as ra
from flask_restful import reqparse, abort, Api, Resource
import os
import pandas as pd
import json
from volsetup import config
from datetime import datetime
from pandas.io.json import json_normalize
from flask import jsonify
from flask import Response
from flask.json import JSONEncoder
from operations import market_data as md


class MiniJSONEncoder(JSONEncoder):
    """Minify JSON output."""
    item_separator = ','
    key_separator = ':'


globalconf = config.GlobalConfig()
DATA_DIR = globalconf.config['paths']['nginx_static_folder']
exts = (".png")
parser = reqparse.RequestParser()
parser.add_argument('task')
app = Flask(__name__)
app.json_encoder = MiniJSONEncoder
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
api = Api(app)


class Test(Resource):
    def get(self):
        return "Hello!"


class WebDirectory(Resource):
    def get(self,name):
        dir = os.path.abspath(DATA_DIR + "/" + name)
        # print (dir)
        # walk through data directory
        list = []
        for root, dirs, files in os.walk(dir):
            for name in files:
                filename = os.path.join(root,name)
                if filename.lower().endswith(exts):
                    # print ("filename" , filename)
                    list.append(name)
        data = jsonify(result=list)
        # js = json.dumps({"result":list})
        # print("js: " + js)
        # print("data: " + str(data))
        # resp = Response(js, status=200, mimetype='application/json')
        return data
        # return resp


class DataSources(Resource):
    """
    List available data sources
    """
    def get(self):
        # list available data sources
        data = md.get_db_types(globalconf)
        return jsonify(result=data)


class UnderlySymbols(Resource):
    def get(self, dsId):
        # list available underlying symbols in the data source
        data = md.get_underlying_symbols(globalconf, dsId)
        return jsonify(result=data)


class Expiries(Resource):
    def get(self, dsId, symbolExpiry):
        # list available expiries for the underlying 
        data = md.get_expiries(globalconf, dsId, symbolExpiry)
        return jsonify(result=data)


class MarketDates(Resource):
    def get(self, dsId, symbolExpiry, expiry):
        # list available dates with data for the expiry
        data = str(expiry)
        return jsonify(result=data)

class OptionChains(Resource):
    def get(self, dsId, symbolExpiry, expiry, dateIni, dateEnd):
        # list available dates with data for the expiry
        dini=datetime.strptime(dateIni, '%Y%m%d')
        dend=datetime.strptime(dateEnd, '%Y%m%d')
        data = str(expiry)
        return jsonify(result=data)


class H5Gekko(Resource):
    def get(self):
        dir = os.path.abspath(DATA_DIR)
        xls = pd.ExcelFile(dir+'/gekkomock.xlsx')
        df1 = xls.parse('test')
        return df1.to_json(orient='records')

api.add_resource(WebDirectory, '/tic/dir/<name>')  # http://localhost:9001/h5dir
# Gekko index API service
api.add_resource(H5Gekko, '/tic/gekko')
api.add_resource(Test, '/tic/test1')

# new stuff
api.add_resource(DataSources, '/tic/data/')
api.add_resource(UnderlySymbols, '/tic/data/<dsId>/')
api.add_resource(Expiries, '/tic/data/<dsId>/<symbolExpiry>/')
api.add_resource(MarketDates, '/tic/data/<dsId>/<symbolExpiry>/<expiry>/')
api.add_resource(OptionChains, '/tic/data/<dsId>/<symbolExpiry>/<expiry>/<dateIni>/<dateEnd>')


if __name__ == '__main__':

    app.run(host="0.0.0.0", port=9001,debug=True)
