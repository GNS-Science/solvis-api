#!python

from flask_restplus import Namespace, Resource, fields

api = Namespace('locations', description='Location related operations')

location_model = api.model('Location', {
    'id': fields.String(required=True, description='The location identifier (https://service.unece.org/trade/locode/nz.htm) '),
    'name': fields.String(required=True, description='The location name'),
	#    'latitude'
	#	'longitude'
	#	'population'
})

LOCATIONS = [
	{'id':'WLG', 'name': "Wellington"},
	{'id':'GIS', 'name':"Gisborne"}
]

"""
    WLG = ["Wellington", -41.276825, 174.777969, 2e5],
    GIS = ["Gisborne", -38.662334, 178.017654, 5e4],
    CHC = ["Christchurch", -43.525650, 172.639847, 3e5],
    IVC = ["Invercargill", -46.413056, 168.3475, 8e4],
    DUD = ["Dunedin", -45.8740984, 170.5035755, 1e5],
    NPE = ["Napier", -39.4902099, 176.917839, 8e4],
    NPL = ["New Plymouth", -39.0579941, 174.0806474, 8e4],
    PMR = ["Palmerston North", -40.356317, 175.6112388, 7e4],
    NSN = ["Nelson", -41.2710849, 173.2836756, 8e4],
    BHE = ["Blenheim", -41.5118691, 173.9545856, 5e4],
    WHK = ["Whakatane", -37.9519223, 176.9945977, 5e4],
    GMN = ["Greymouth", -42.4499469, 171.2079875, 3e4],
    ZQN = ["Queenstown", -45.03, 168.66, 15e3],
    AKL = ["Auckland", -36.848461, 174.763336, 2e6],
    ROT = ["Rotorua", -38.1446, 176.2378, 77e3],
    TUO = ["Taupo", -38.6843, 176.0704, 26e3],
    WRE = ["Whangarei", -35.7275, 174.3166, 55e3],
    LVN = ["Levin", -40.6218, 175.2866, 19e3],
    TMZ = ["Tauranga", -37.6870, 176.1654, 130e3],
    TIU = ['Timaru', -44.3904, 171.2373, 28e3],
    OAM = ["Oamaru", -45.0966, 170.9714, 14e3],
    PUK = ["Pukekohe", -37.2004, 174.9010, 27e3],
    HLZ = ["Hamilton", -37.7826, 175.2528, 165e3],
    LYJ = ["Lower Hutt", -41.2127, 174.8997, 112e3]
"""


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