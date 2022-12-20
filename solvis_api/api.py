import logging

# from solvis_api.datastore.datastore import get_datastore
import logging.config
import os

import yaml
from flask import Flask
from flask_cors import CORS
from solvis_store import model

from solvis_api.config import IS_OFFLINE, IS_TESTING, LOGGING_CFG
from solvis_api.namespaces import api  # , blueprint

# from flask.logging import default_handler


def create_app():
    """
    Function that creates our Flask application.
    This function creates the Flask app, Flask-RESTful API,

    :param locations_data_module: module path
    :return: Initialized Flask app
    """
    app = Flask(__name__)
    api.init_app(app)
    CORS(app)

    # set up the datastore config
    # datastore = get_datastore()

    if IS_OFFLINE and not IS_TESTING:
        model.set_local_mode()

    if not IS_TESTING:
        model.migrate()

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
