from flask_restx import Namespace, Resource, fields

api = Namespace('radii', description='Radii')

radii_model = api.model('Radii', {
    'id': fields.String(Required=True, description='Radii list number'),
    'radii': fields.List(fields.Integer, Required=True, description='Radii')
})

RADII = [{'id': '1', 'radii': [10e3]},
         {'id': '2', 'radii': [10e3,20e3]},
         {'id': '3', 'radii': [10e3,20e3,30e3]},
         {'id': '4', 'radii': [10e3,20e3,30e3,40e3]},
         {'id': '5', 'radii': [10e3,20e3,30e3,40e3,50e3]},
         {'id': '6', 'radii': [10e3,20e3,30e3,40e3,50e3,100e3]},]

@api.route('/')
class RadiiList(Resource):
    @api.doc('list_all_radii')
    @api.marshal_list_with(radii_model)
    def get(self):
        '''List all radii'''
        return RADII

@api.route('/<radius>')
@api.param('radius', 'the radius list id')
@api.response(404, 'radius not found')
class RadiiById(Resource):    
    @api.doc('get_one_radii')
    @api.marshal_list_with(radii_model)
    def get(self, radius):
        '''List one radii by id'''
        for rad in RADII:
            if rad['id'] == radius:
                return rad
        api.abort(404)
