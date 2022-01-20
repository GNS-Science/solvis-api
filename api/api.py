import logging
from flask import Flask, g, request
from api.namespaces import api#, blueprint

from api.datastore import model
from api.config import IS_OFFLINE

#from api.datastore.datastore import get_datastore

def create_app():
    """
    Function that creates our Flask application.
    This function creates the Flask app, Flask-RESTful API,

    :param locations_data_module: module path
    :return: Initialized Flask app
    """

    logging.basicConfig(level=logging.DEBUG)
    log = logging.getLogger(__name__)

    app = Flask(__name__)
    api.init_app(app)

    #set up the datastore config
    #datastore = get_datastore()

    if IS_OFFLINE:
        model.set_local_mode()

    app.before_first_request(model.migrate)

    @app.before_request
    def hook():
        log.info('before_request endpoint: %s, url: %s, path: %s' % (
            request.endpoint,
            request.url,
            request.path))

    return app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)