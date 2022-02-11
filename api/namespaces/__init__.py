from flask_restx import Api, Namespace

from flask import Blueprint
from flask import request

from .locations import locations, location_lists
from .radii import api as radii
from .solutions import api as solution_analysis
#from .solution_analysis_geojson import api as solution_analysis_geojson


#blueprint = Blueprint('api', __name__, url_prefix='/')

#api = Api(blueprint, doc='/doc/')


"""
The main entry point for the application.
You need to initialize it with a Flask Application: ::

>>> app = Flask(__name__)
>>> api = Api(app)

Alternatively, you can use :meth:`init_app` to set the Flask application
after it has been constructed.

The endpoint parameter prefix all views and resources:

    - The API root/documentation will be ``{endpoint}.root``
    - A resource registered as 'resource' will be available as ``{endpoint}.resource``

:param flask.Flask|flask.Blueprint app: the Flask application object or a Blueprint
:param str version: The API version (used in Swagger documentation)
:param str title: The API title (used in Swagger documentation)
:param str description: The API description (used in Swagger documentation)
:param str terms_url: The API terms page URL (used in Swagger documentation)
:param str contact: A contact email for the API (used in Swagger documentation)
:param str license: The license associated to the API (used in Swagger documentation)
:param str license_url: The license page URL (used in Swagger documentation)
:param str endpoint: The API base endpoint (default to 'api).
:param str default: The default namespace base name (default to 'default')
:param str default_label: The default namespace label (used in Swagger documentation)
:param str default_mediatype: The default media type to return
:param bool validate: Whether or not the API should perform input payload validation.
:param bool ordered: Whether or not preserve order models and marshalling.
:param str doc: The documentation path. If set to a false value, documentation is disabled.
            (Default to '/')
:param list decorators: Decorators to attach to every resource
:param bool catch_all_404s: Use :meth:`handle_error`
    to handle 404 errors throughout your app
:param dict authorizations: A Swagger Authorizations declaration as dictionary
:param bool serve_challenge_on_401: Serve basic authentication challenge with 401
    responses (default 'False')
:param FormatChecker format_checker: A jsonschema.FormatChecker object that is hooked into
    the Model validator. A default or a custom FormatChecker can be provided (e.g., with custom
    checkers), otherwise the default action is to not enforce any format validation.
:param url_scheme: If set to a string (e.g. http, https), then the specs_url and base_url will explicitly use this
    scheme regardless of how the application is deployed. This is necessary for some deployments behind a reverse
    proxy.
"""

# see https://stackoverflow.com/questions/52087743/flask-restful-noauthorizationerror-missing-authorization-header/52208185
# see https://github.com/flask-restful/flask-restful/issues/280#issuecomment-567186686
class ErrorPropagatingApi(Api):
    """Flask-Restful has its own error handling facilities, this propagates errors to flask"""

    def error_router(self, original_handler, e):
        print('error_router : %s, url: %s, path: %s err:%s' % (
            request.endpoint,
            request.url,
            request.path,
            e))
        return original_handler(e)

#api = ErrorPropagatingApi(
api = Api(default='',
    #blueprint,
    #prefix='/api',
    #doc='/api/',
    title='Solvis API',
    version='0.1',
    description='A solvis Rest API',
    catch_all_404s=True
)

# sol_ns = Api()#"Solutions", path="/sol")
# #Api(title='Solution analysis Geojson', default='sol')
# sol_ns.add_namespace(solution_analysis_geojson)
# sol_ns.add_namespace(solution_analysis)

api.add_namespace(locations)
api.add_namespace(location_lists)
api.add_namespace(radii)
api.add_namespace(solution_analysis)
#api.add_namespace(solution_analysis_geojson)