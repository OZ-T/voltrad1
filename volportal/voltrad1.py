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
from core import vol_estimators as ve
from volsetup.logger import logger

class MiniJSONEncoder(JSONEncoder):
    """Minify JSON output."""
    item_separator = ','
    key_separator = ':'


globalconf = config.GlobalConfig()
log = logger("Flask REST API ...")
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
    def get(self, name):
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


class OptChainMarketDataInfo(Resource):
    def get(self):
        # return JSON with symbol items availablewith description of the data
        # available including: source, expiries, columns, time periods
        #   ES/20170616
        #       source =ibopt,ibund,yhooopt
        #       expiries
        #       columns

        data = md.get_optchain_datasources(globalconf)
        JSONP_data = jsonify(result=data)
        return JSONP_data
        # return data.to_json(orient='records')
        #return data

class OptChainMarketData(Resource):
    def get(self, underlySymbol):
        #       dates
        data = {}
        return jsonify(result=data)
    def post(self, underlySymbol):
        # returns the result of a query to the market data databases
        # the query itself is a json in the body of the request
        # {symbol:ES/20170616,sources:[ibopt,ibund],dates:... }
        # Get the parsed contents of the form data
        json = request.json
        print(json)
        # Render template
        return jsonify(json)

class VolGraph(Resource):
    def get(self,symbol, last_date, estimator):

        div,script = ve.read_graph_from_db(globalconf,log,symbol, last_date, estimator)
        JSONP_data = jsonify({"div":div,"script":script})
        return JSONP_data


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

# Better version
api.add_resource(OptChainMarketDataInfo, '/tic/optchain_data/')
api.add_resource(OptChainMarketData, '/tic/optchain_data/<underlySymbol>')

api.add_resource(VolGraph, '/tic/graph/<symbol>/<last_date>/<estimator>')

if __name__ == '__main__':

    app.run(host="0.0.0.0", port=9001,debug=True)
