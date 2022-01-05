from flask_restplus import Api

from .locations import api as locations

api = Api(
    title='Solvis API',
    version='0.1',
    description='A solvis Rest API',
    # All API metadatas
)

api.add_namespace(locations)