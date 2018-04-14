from flask import *
import pandas as pd
app = Flask(__name__)
from persist.sqlite_methods import *
from core import misc_utilities as utils, config as config
from flask import jsonify
from flask_graphql import GraphQLView

globalconf = config.GlobalConfig()
log = globalconf.get_logger()

@app.route("/files")
def show_files():
    return jsonify(get_data_files())


@app.route("/table/<name>")
def show_table(name):
    return jsonify(name)


app.add_url_rule(
    '/graphql', view_func=GraphQLView.as_view('graphql',schema=schema,graphiql=True # for having the GraphiQL interface
    )
)



if __name__ == "__main__":
    app.run(debug=True)