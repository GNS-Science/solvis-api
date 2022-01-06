from flask import Flask
from .namespaces import api

from api.model import set_local_mode, migrate

app = Flask(__name__)
# app.config.from_object(Config)

api.init_app(app)


set_local_mode() #make this pick up the config
app.before_first_request(migrate)

if __name__ == '__main__':
    # Register table creation callback

    app.run()