from flask import Flask

#monkey patching , see https://stackoverflow.com/a/67523704
import werkzeug
werkzeug.cached_property = werkzeug.utils.cached_property
import flask.scaffold
flask.helpers._endpoint_from_view_func = flask.scaffold._endpoint_from_view_func

from flask_restplus import Api, Resource

#from locations import Locations

from .namespaces import api

app = Flask(__name__)
api.init_app(app)


#app = Flask(__name__)
#api = Api(app)

# @api.route('/hello/')
# class HelloWorld(Resource):
#     def get(self):
#         return "Hello World"

# @api.route('/locations/')
# Locations

    
if __name__ == '__main__':
    app.run()