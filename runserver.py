from os import environ
from Modules.routes import app

app.run(host='0.0.0.0',port='8082')
# import flask

# app = flask.Flask(__name__)
# app.config["DEBUG"] = True


# @app.route('/', methods=['GET'])
# def home():
#     return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for distant reading of science fiction novels.</p>"

# app.run()