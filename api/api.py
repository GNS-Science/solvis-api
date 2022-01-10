from flask import Flask, request
from .namespaces import api#, blueprint

from api.model import set_local_mode, migrate
from api.config import LOCAL_MODE

app = Flask(__name__)

#app.register_blueprint(blueprint)

#app.config["APPLICATION_ROOT"] = "/spam"

api.init_app(app)

if LOCAL_MODE:
    set_local_mode()

app.before_first_request(migrate)

@app.before_request
def hook():
    # request - flask.request
    print('before_request endpoint: %s, url: %s, path: %s' % (
        request.endpoint,
        request.url,
        request.path))
    # just do here everything what you need...

if __name__ == '__main__':
    app.run()