# #!python
import logging

from flask import current_app, g
from flask_restx import Namespace, Resource, fields

# from solvis_api.resources import LOCATIONS, LOCATION_LISTS
from solvis_api.datastore.datastore import get_ds

log = logging.getLogger(__name__)

locations = Namespace('locations', description='Location related operations')
location_lists = Namespace('location_lists', description='Location list related operations')

location_list_model = location_lists.model(
    'Location-List',
    {
        'id': fields.String(required=True, description='The location list identifier'),
        'name': fields.String(required=True, description='The location list name'),
        'locations': fields.List(fields.String, Required=True, description='List of location IDs'),
    },
)

location_model = locations.model(
    'Location',
    {
        'id': fields.String(
            required=True, description='The location identifier (https://service.unece.org/trade/locode/nz.htm) '
        ),
        'name': fields.String(required=True, description='The location name'),
        'latitude': fields.Float(required=True, description='The location\'s latitude'),
        'longitude': fields.Float(required=True, description='The location\'s longitude'),
        'population': fields.Integer(required=True, description='The location\'s population'),
    },
)


@locations.route('/')
class LocationList(Resource):
    @locations.doc('list_locations')
    @locations.marshal_list_with(location_model)
    def get(self):
        '''List all locations'''
        datastore = get_ds()
        return datastore.resources.LOCATIONS


@location_lists.route('/<location_list_id>/locations')
@locations.param('location_list_id', 'The location list identifier')
@locations.response(404, 'Location not found')
class Location(Resource):
    @locations.doc('get_location_lists location data')
    @locations.marshal_with(location_model)
    def get(self, location_list_id):
        '''Fetch a location_list's locations given its identifier'''
        locs = []
        datastore = get_ds()
        for loc_list in datastore.resources.LOCATION_LISTS:
            if loc_list['id'] == location_list_id:
                for location in datastore.resources.LOCATIONS:
                    if location['id'] in loc_list['locations']:
                        locs.append(location)
        if locs == []:
            locations.abort(404)
        else:
            return locs


@locations.route('/<location_id>')
@locations.param('location_id', 'The location identifier')
@locations.response(404, 'Location not found')
class Location(Resource):
    @locations.doc('get_location')
    @locations.marshal_with(location_model)
    def get(self, location_id):
        '''Fetch a location given its identifier'''
        datastore = get_ds()
        log.info("Location.get_location %s" % location_id)
        log.info('datastore %s' % datastore.resources.LOCATIONS)
        for loc in datastore.resources.LOCATIONS:
            if loc['id'] == location_id:
                return loc
        locations.abort(404)


@location_lists.route('/')
class LocationListList(Resource):
    @location_lists.doc('list_location_lists')
    @location_lists.marshal_list_with(location_list_model)
    def get(self):
        '''List all location lists'''
        datastore = get_ds()
        return datastore.resources.LOCATION_LISTS


@location_lists.route('/<location_list_id>')
@location_lists.param('location_list_id', 'The location list identifier')
@location_lists.response(404, 'Location not found')
class Location(Resource):
    @location_lists.doc('get_location_list_contents')
    @location_lists.marshal_with(location_list_model)
    def get(self, location_list_id):
        '''Fetch a location_list given its identifier'''
        datastore = get_ds()
        for loc_list in datastore.resources.LOCATION_LISTS:
            if loc_list['id'] == location_list_id:
                return loc_list
        location_lists.abort(404)
