#!python

from flask_restplus import Namespace, Resource, fields
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL)
from api.solvis import multi_city_events
# from api.namespaces.locations import Location

import pandas as pd
from solvis import InversionSolution

api = Namespace('solutions', description='Solution analysis related operations')

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

solution_analysis_model = api.model('Solution Analysis', dict(
    solution_id = fields.String(required=True, description='The solution identifier (ref Toshi API '),
    locations_list_id = fields.String(required=True, description='The locations_list ID (see /location_lists)'),
    radii_list_id = fields.String(required=True, description='The radii_list ID (see /radii_lists)'),
))

SOLUTION_ANALYSES = [
    dict(solution_id='SW52ZXJzaW9uU29sdXRpb246MTc1NTQuMDdzQ3d1', locations_list_id="NZ", radii_list_id="1"),
]

@api.route('/')
class SolutionAnalysisList(Resource):
    @api.doc('list_solution')
    @api.marshal_list_with(solution_analysis_model)
    def get(self):
        '''List all solution'''
        return SOLUTION_ANALYSES

@api.route('/<solution_id>/<locations_list_id>/<radii_list_id>')
@api.param('solution_id', 'The solution analysis identifier')
@api.response(404, 'solution analysis not found')
class SolutionAnalysis(Resource):
    @api.doc('get_solution_analysis ')
    @api.marshal_with(solution_analysis_model)
    def get(self, solution_id, locations_list_id, radii_list_id):
        '''Fetch a solution_analysis_model given its identifiers'''
        for loc in SOLUTION_ANALYSES:
            if loc['solution_id'] == solution_id and \
                 loc['locations_list_id'] == locations_list_id and \
                 loc['radii_list_id'] == radii_list_id:
                return loc
        api.abort(404)

    @api.doc('create_solution_analysis ')
    @api.expect(solution_analysis_model)
    @api.marshal_with(solution_analysis_model)
    @api.response(404, 'solution id not found')
    def post(self, solution_id, locations_list_id, radii_list_id):

        WORK_PATH = "./tmp"
        #fetch the solution
        filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
        solution = InversionSolution().from_archive(filename)

        #ref https://service.unece.org/trade/locode/nz.htm
        cities = dict(
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
        )

        combos = multi_city_events.city_combinations(cities, 0, 5)
        print(f"city combos: {len(combos)}")

        radii = [10e3,20e3,30e3,40e3,50e3,100e3] #AK could be larger ??

        rupture_radius_site_sets = multi_city_events.main(solution, cities, combos, radii)

        #now export for some science analysis
        df = pd.DataFrame.from_dict(rupture_radius_site_sets, orient='index')
        df = df.rename(columns=dict(zip(radii, [f'r{int(r/1000)}km' for r in radii])))

        print(df)
        ruptures = df.join(solution.ruptures_with_rates)
        print(ruptures)


        ##Save the dataframe

