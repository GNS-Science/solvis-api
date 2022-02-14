import logging
from flask import Flask, g, request
#from flask.logging import default_handler

from flask_cors import CORS
from api.namespaces import api#, blueprint

from api.datastore import model
from api.config import IS_OFFLINE, IS_TESTING, LOGGING_CFG

#from api.datastore.datastore import get_datastore
import logging.config
import os
import yaml


def create_app():
    """
    Function that creates our Flask application.
    This function creates the Flask app, Flask-RESTful API,

    :param locations_data_module: module path
    :return: Initialized Flask app
    """
    log = logging.getLogger(__name__)

    app = Flask(__name__)
    api.init_app(app)
    CORS(app)

    #set up the datastore config
    #datastore = get_datastore()

    if IS_OFFLINE and not IS_TESTING:
        model.set_local_mode()

    app.before_first_request(model.migrate)

    # @app.before_request
    # def hook():
    #     log.debug('before_request endpoint: %s, url: %s, path: %s' % (
    #         request.endpoint,
    #         request.url,
    #         request.path))

    return app

app = create_app()

"""
Setup logging configuration
ref https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

"""

if os.path.exists(LOGGING_CFG):
    with open(LOGGING_CFG, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=logging.INFO)
