from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
import os,sys
import pandas as pd
import json
from volsetup import config

globalconf = config.GlobalConfig()

app = Flask(__name__)
api = Api(app)

TODOS = {
    'todo1': {'task': 'build an API'},
    'todo2': {'task': '?????'},
    'todo3': {'task': 'profit!'},
}
DATA_DIR = globalconf.config['paths']['data_folder'] # "/home/david/Dropbox/proyectos/data"
exts= (".hdf",".hdf5",".h5",".he5")

def abort_if_todo_doesnt_exist(todo_id):
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exist".format(todo_id))

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

class H5Table(Resource):
    def get(self,name,start,end):
        DIR = os.path.abspath(DATA_DIR)
        store=globalconf.open_table(name+".h5")
        #account_db_new
        node=store.get_node(globalconf.config['ib_api']['accountid'])
        df1=store.select(node._v_pathname,start=start,end=end,columns=['AccountCode_','AvailableFunds_USD','current_datetime_txt'])

        return df1[start:end].reset_index().to_json(orient='index')

# Todo
# shows a single todo item and lets you delete a todo item
class Todo(Resource):
    def get(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        return TODOS[todo_id]

    def delete(self, todo_id):
        abort_if_todo_doesnt_exist(todo_id)
        del TODOS[todo_id]
        return '', 204

    def put(self, todo_id):
        args = parser.parse_args()
        task = {'task': args['task']}
        TODOS[todo_id] = task
        return task, 201


# TodoList
# shows a list of all todos, and lets you POST to add new tasks
class TodoList(Resource):
    def get(self):
        return TODOS

    def post(self):
        args = parser.parse_args()
        todo_id = int(max(TODOS.keys()).lstrip('todo')) + 1
        todo_id = 'todo%i' % todo_id
        TODOS[todo_id] = {'task': args['task']}
        return TODOS[todo_id], 201

##
## Actually setup the Api resource routing here
##
api.add_resource(TodoList, '/todos')
api.add_resource(Todo, '/todos/<todo_id>')
api.add_resource(H5Dir, '/h5dir')
api.add_resource(H5Table, '/h5table/<name>/<int:start>/<int:end>')


if __name__ == '__main__':
    app.run(debug=True)
