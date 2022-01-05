# #!python

from flask_restplus import Namespace, Resource, fields

api = Namespace('locations', description='Location related operations')

location_model = api.model('Location', {
    'id': fields.String(required=True, description='The location identifier (https://service.unece.org/trade/locode/nz.htm) '),
    'name': fields.String(required=True, description='The location name'),
	'latitude': fields.Float(required=True, description='The location\'s latitude'),
	'longitude': fields.Float(required=True, description='The location\'s longitude'),
	'population': fields.Integer(required=True, description='The location\'s population'),
})

LOCATIONS = [
{'id': 'WLG', 'name': 'Wellington', 'latitude': -41.276825, 'longitude': 174.777969, 'population': 200000.0},
{'id': 'GIS', 'name': 'Gisborne', 'latitude': -38.662334, 'longitude': 178.017654, 'population': 50000.0},
{'id': 'CHC', 'name': 'Christchurch', 'latitude': -43.52565, 'longitude': 172.639847, 'population': 300000.0},
{'id': 'IVC', 'name': 'Invercargill', 'latitude': -46.413056, 'longitude': 168.3475, 'population': 80000.0},
{'id': 'DUD', 'name': 'Dunedin', 'latitude': -45.8740984, 'longitude': 170.5035755, 'population': 100000.0},
{'id': 'NPE', 'name': 'Napier', 'latitude': -39.4902099, 'longitude': 176.917839, 'population': 80000.0},
{'id': 'NPL', 'name': 'New Plymouth', 'latitude': -39.0579941, 'longitude': 174.0806474, 'population': 80000.0},
{'id': 'PMR', 'name': 'Palmerston North', 'latitude': -40.356317, 'longitude': 175.6112388, 'population': 70000.0},
{'id': 'NSN', 'name': 'Nelson', 'latitude': -41.2710849, 'longitude': 173.2836756, 'population': 80000.0},
{'id': 'BHE', 'name': 'Blenheim', 'latitude': -41.5118691, 'longitude': 173.9545856, 'population': 50000.0},
{'id': 'WHK', 'name': 'Whakatane', 'latitude': -37.9519223, 'longitude': 176.9945977, 'population': 50000.0},
{'id': 'GMN', 'name': 'Greymouth', 'latitude': -42.4499469, 'longitude': 171.2079875, 'population': 30000.0},
{'id': 'ZQN', 'name': 'Queenstown', 'latitude': -45.03, 'longitude': 168.66, 'population': 15000.0},
{'id': 'AKL', 'name': 'Auckland', 'latitude': -36.848461, 'longitude': 174.763336, 'population': 2000000.0},
{'id': 'ROT', 'name': 'Rotorua', 'latitude': -38.1446, 'longitude': 176.2378, 'population': 77000.0},
{'id': 'TUO', 'name': 'Taupo', 'latitude': -38.6843, 'longitude': 176.0704, 'population': 26000.0},
{'id': 'WRE', 'name': 'Whangarei', 'latitude': -35.7275, 'longitude': 174.3166, 'population': 55000.0},
{'id': 'LVN', 'name': 'Levin', 'latitude': -40.6218, 'longitude': 175.2866, 'population': 19000.0},
{'id': 'TMZ', 'name': 'Tauranga', 'latitude': -37.687, 'longitude': 176.1654, 'population': 130000.0},
{'id': 'TIU', 'name': 'Timaru', 'latitude': -44.3904, 'longitude': 171.2373, 'population': 28000.0},
{'id': 'OAM', 'name': 'Oamaru', 'latitude': -45.0966, 'longitude': 170.9714, 'population': 14000.0},
{'id': 'PUK', 'name': 'Pukekohe', 'latitude': -37.2004, 'longitude': 174.901, 'population': 27000.0},
{'id': 'HLZ', 'name': 'Hamilton', 'latitude': -37.7826, 'longitude': 175.2528, 'population': 165000.0},
{'id': 'LYJ', 'name': 'Lower Hutt', 'latitude': -41.2127, 'longitude': 174.8997, 'population': 112000.0}]


@api.route('/')
class LocationList(Resource):
    @api.doc('list_locations')
    @api.marshal_list_with(location_model)
    def get(self):
        '''List all locations'''
        return LOCATIONS

@api.route('/<id>')
@api.param('id', 'The location identifier')
@api.response(404, 'Location not found')
class Location(Resource):
    @api.doc('get_location')
    @api.marshal_with(location_model)
    def get(self, id):
        '''Fetch a location given its identifier'''
        for loc in LOCATIONS:
            if loc['id'] == id:
                return loc
        api.abort(404)

