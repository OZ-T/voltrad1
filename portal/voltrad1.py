import datetime as dt
import os
import pandas as pd
from flask import Flask
from flask import jsonify
from flask.json import JSONEncoder
from flask_restful import reqparse, Api, Resource
import persist.sqlite_methods
from core import config
from persist import sqlite_methods as md
from core.logger import logger
import graphene
from flask_graphql import GraphQLView


class MiniJSONEncoder(JSONEncoder):
    """Minify JSON output."""
    item_separator = ','
    key_separator = ':'

globalconf = config.GlobalConfig()
log = logger("Flask REST API ...")
DATA_DIR = globalconf.config['paths']['nginx_static_folder']
parser = reqparse.RequestParser()
parser.add_argument('task')

app = Flask(__name__)
app.json_encoder = MiniJSONEncoder
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
api = Api(app)


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
    def get(self,symbol, last_date, estimator,name):
        today = dt.date.today()
        last_date1 = today.strftime('%Y%m%d')

        div,script = persist.sqlite_methods.read_graph_from_db(globalconf=globalconf, log=log, symbol=symbol,
                                                               last_date=last_date1, estimator=estimator, name=name)
        JSONP_data = jsonify({"div":div,"script":script})
        return JSONP_data

class VolLinePoints(Resource):
    def get(self,symbol, last_date, estimator,name):
        today = dt.date.today()
        last_date1 = today.strftime('%Y%m%d')

        dict = persist.sqlite_methods.read_lineplot_data_from_db(globalconf=globalconf, log=log, symbol=symbol,
                                                                 last_date=last_date1, estimator=estimator)
        JSONP_data = jsonify(dict)
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

api.add_resource(VolGraph, '/tic/graph/<symbol>/<last_date>/<name>/<estimator>')
api.add_resource(VolLinePoints, '/tic/linpoints/<symbol>/<last_date>/<name>/<estimator>')



class Query(graphene.ObjectType):
    hello = graphene.String(description='Hello')
    def resolve_hello(self, args, context, info):
        return 'World'

view_func = GraphQLView.as_view('graphql', schema=graphene.Schema(query=Query))
app.add_url_rule('/tic/graphql', view_func=view_func)

from portal.accesscontroldecorator import *
import persist.document_methods as da
dir = os.path.abspath(DATA_DIR)

class Timers(Resource):
    #@crossdomain(origin='*')
    def post(self):
        json = request.json
        newTimer = {
            'title': json['title'],
            'project': json['project'],
            'id': json['id'],
            'elapsed': 0,
            'runningSince': None,
        }
        return1 = da.save_docs_to_db(globalconf, log,newTimer,collection_name="timers")
        return {'inserted_ids':return1}

    def put(self):
        # update timers
        timer = request.json
        return1 = da.update_timers_to_db(globalconf, log,timer,action=None,collection_name="timers")
        return {'updated_ids':return1}


    def delete(self):
        # delete timers
        timer = request.json
        return1 = da.delete_doc_to_db(globalconf, log,timer,collection_name="timers")
        return {'deleted_ids':return1}


    #@crossdomain(origin='*')
    def get(self):
        results1 = da.read_docs_from_db(globalconf,log,collection_name="timers")
        results = {
            'results':results1
        }
        JSONP_data = jsonify( results )
        return JSONP_data

     #### TODO get distinct timers associated   with a project
     ###  https://stackoverflow.com/questions/28155857/mongodb-find-query-return-only-unique-values-no-duplicates
     ### https://docs.mongodb.com/manual/reference/method/db.collection.distinct/
        
class TimersAction(Resource):
    def post(self, action):
        timer = request.json
        #print(action)
        return1 = da.update_timers_to_db(globalconf, log,timer,action,collection_name="timers")
        return {'updated_ids':return1}


api.add_resource(Timers, '/tic/api/timers')
api.add_resource(TimersAction, '/tic/api/timers/<action>')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9001,debug=True)





