#!python

import io
import json
from functools import lru_cache
import pandas as pd
import geopandas as gpd

from flask import current_app
from flask_restx import reqparse
from flask_restx import Namespace, Resource, fields
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SNS_TOPIC_ARN, IS_OFFLINE)
from api.solvis import multi_city_events

#from api.datastore.datastore import get_ds

from solvis import InversionSolution, new_sol, section_participation
from api.datastore.solvis_db_query import matched_rupture_sections_gdf

import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

api = Namespace('solution_analysis', description='Solution analysis related operations')

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

# @lru_cache(maxsize=32)
# def get_inversion_solution(solution_id):
#     WORK_PATH = "./tmp"
#     log.info(f'get_inversion_solution: {solution_id}')
#     filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
#     return InversionSolution().from_archive(filename)
# =======
# @lru_cache(maxsize=32)
# def get_inversion_solution(solution_id):
#     if IS_OFFLINE:
#         WORK_PATH = "./tmp"
#     else:
#         WORK_PATH = "/tmp"
#     log.info(f'get_inversion_solution: {solution_id}')
#     filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
#     return InversionSolution().from_archive(filename)
# >>>>>>> feature/CORS

# @lru_cache(maxsize=32)
# def get_solution_dataframe_result(_id):
#     datastore = get_ds()
#     solutions = datastore.resources.solutions
#     try:
#         log.info(f'get_solution_dataframe_result: {_id}')
#         result = solutions.get(_id) #     5cd8df26-2370-4bbb-8e3d-03e6453e0c65

#         pickle_bytes = io.BytesIO()
#         pickle_bytes.write(result.dataframe)
#         pickle_bytes.seek(0)

#         result.dataframe = pd.read_pickle(pickle_bytes, 'xz').to_json(indent=2)

#         return result
#     except (solutions.DoesNotExist):
#         api.abort(404)

# @lru_cache(maxsize=32)
# def get_solution_dataframe(_id):
#     return pd.read_json(get_solution_dataframe_result(_id).dataframe)

# def filter_dataframe(_id, location_id: str, radius_km: int, rate_threshold: float):

#     df = get_solution_dataframe(_id) #cached

#     def get_radii_map(df):
#         rad_cols = {}
#         for c in df.columns:
#             if c[0] == 'r' and c[-2:] == 'km':
#                 rad_cols[int(c[1:-2])] = c
#         return rad_cols

#     def where_city_in_radius(df: pd.DataFrame, radius_column: str, location_id: str):
#         return df[radius_column].apply(lambda x: location_id in x if x else False) == True

#     #get the radius column for first filter layer
#     radius_map = get_radii_map( df )
#     if not radius_km in radius_map.keys():
#         api.abort(404, f'radius {radius_km} is not in {[k for k in radius_map.keys()]}')
#     radius_column = radius_map[radius_km]

#     print(get_radii_map( df ), radius_column)

#     df2 = df[where_city_in_radius(df, radius_column, location_id)]
#     if rate_threshold > 0:
#         df2 = df2[df2['Annual Rate']>= rate_threshold]

#     print(df2.shape)
#     return df2

# @lru_cache(maxsize=32)
# def get_filtered_ruptures(dataframe_id, location_id, radius_km, annual_rate_threshold):
#     filtered = filter_dataframe(dataframe_id, location_id, int(radius_km), float(annual_rate_threshold))
#     return filtered


solution_analysis_composite_geojson_model = api.model('Solution Analysis Geojson', dict(
    solution_id = fields.String(required=True, description='The unique identifier for the Inversion Solution.'),
    location_ids = fields.String(required=True, description='The location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'),
    radius_km = fields.String(required=True, description='The rupture/location intersection radius in km.'),
    rupture_count = fields.Integer(),
    created = fields.DateTime(description='The created timestamp'),
    ruptures = fields.String(description='All the ruptures'),
    error_message = fields.String(description='Any problems')
    ))

@api.route('/<string:solution_id>/loc/<string:location_ids>/rad/<int:radius_km>')
@api.param('solution_id', 'The inversion solution  identifier')
@api.param('location_ids', 'The location identifiers, comma-delmited e.g. `WLG,PMR,ZQN`')
@api.param('radius_km', 'The rupture/location intersection radius in km')
@api.response(404, 'solution analysis not found')
class SolutionAnalysisGeojsonComposite(Resource):
    """
    SolutionAnalysisGeojsonComposite handlers
    """
    @api.doc('get_solution_analysis_geojson ')
    @api.marshal_with(solution_analysis_composite_geojson_model)
    def get(self, solution_id, location_ids, radius_km):
        """
        GET handler
        """
        #TODO fix this it's not working
        parser = reqparse.RequestParser()
        parser.add_argument('rate_above', type=str,
            help='include ruptures with an Annual Rate above this value  (default = 0)')
        args = parser.parse_args() or {}
        print ('args', args, self)
        annual_rate_threshold=float(args.get('rate_above') or 0)

        rupture_sections_gdf = matched_rupture_sections_gdf(solution_id, location_ids, radius_km*1000, minimum_rate=1e-12)

        error_message = None
        geojson = json.dumps(dict(type="FeatureCollection", features= []))
        if (not rupture_sections_gdf is None) and (rupture_sections_gdf.shape[0] > 1e4):
            error_message = "Too many ruptures satisfy the query, please try a more selective query."
        elif (rupture_sections_gdf is None) or  (rupture_sections_gdf.shape[0] == 0):
            error_message = "No ruptures satisfy the query."
        else:
            geojson = rupture_sections_gdf.to_json()

        result = dict(
            solution_id = solution_id,
            location_ids = location_ids,
            radius_km = radius_km,
            ruptures = geojson,
            rupture_count = int(rupture_sections_gdf.shape[0]) if not rupture_sections_gdf is None else 0,
            error_message = error_message
            )
        return result

# geojson_rupture_model = api.model('Geojson Layer', dict(
#         rupture_id = fields.Integer(required=True),
#         layer_name = fields.String(required=True),
#         magnitude = fields.Float(),
#         section_count = fields.Integer(),
#         length = fields.Float(),
#         annual_rate = fields.Float(),
#         area = fields.Float(),
#         geojson = fields.String(description='The layer geojson')),
#     description='A single rupture including metadata and the fault section geojson'
# )

# solution_analysis_ruptures_geojson_model = api.model('Solution Analysis Geojson',
#     dict(
#         id = fields.String(description='The unique identifier for this analysis'),
#         location_id = fields.String(required=True),
#         radius_km = fields.String(required=True),
#         rupture_count = fields.Integer(),
#         created = fields.DateTime(description='The created timestamp'),
#         ruptures = fields.List(fields.Nested(geojson_rupture_model),
#             description='list of rupture models')
#     ),
#     description='A solution analysis model'
# )


# def get_rupture_geojsons(solution, filtered_df):
#     layers = []
#     #print(filtered_df)
#     for row in filtered_df.itertuples():
#         #Pandas(Index=426115, r10km='WLG', r20km='WLG', _3=426115, Magnitude=7.7557805759, _5=-17.7493085896, _6=3595708870.845804, _7=147379.5207102174, _8=1.395e-07)
#         rupt = dict(rupture_id=row[0], magnitude=row[4], area=row[6], length=row[7], annual_rate=row[8])
#         sp0 = section_participation(solution, [rupt['rupture_id'],])
#         rupt['geojson'] = gpd.GeoDataFrame(sp0).to_json()
#         rupt['layer_name'] = f"{sp0['FaultName'].values[0]} to {sp0['FaultName'].values[1]}"
#         rupt['section_count'] = sp0['FaultName'].size
#         layers.append(rupt)
#     return layers

# @api.route('/<string:sa_id>/loc/<string:location_id>/rad/<int:radius_km>/ruptures')
# @api.param('sa_id', 'The solution analysis identifier')
# @api.param('location_id', 'The location identifier')
# @api.param('radius_km', 'The rupture/location intersection radius in km')
# @api.response(404, 'solution analysis not found')
# class SolutionAnalysisGeojsonRuptures(Resource):
#     """
#     SolutionAnalysisGeojsonRuptures handlers
#     """
#     @api.doc('get_solution_analysis_geojson ')
#     @api.marshal_with(solution_analysis_ruptures_geojson_model)
#     def get(self, sa_id, location_id, radius_km):
#         """
#         GET handler
#         """
#         #TODO fix this it's not working
#         parser = reqparse.RequestParser()
#         parser.add_argument('rate_above', type=str,
#             help='include ruptures with an Annual Rate above this value  (default = 0)')
#         args = parser.parse_args() or {}
#         print ('args', args, self)
#         annual_rate_threshold=float(args.get('rate_above') or 0)

#         result = get_solution_dataframe_result(sa_id) #cached
#         sol = get_inversion_solution(result.solution_id) #cached

#         #apply filters, build a new solution, extract rupture geojson
#         rupture_df = get_filtered_ruptures(sa_id, location_id, radius_km, annual_rate_threshold)
#         rupture_idxs = list(rupture_df['Rupture Index'])
#         filtered_solution = new_sol(sol, rupture_idxs)

#         result.location_id = location_id
#         result.radius_km = radius_km
#         result.ruptures = get_rupture_geojsons(filtered_solution, rupture_df)
#         result.rupture_count = len(result.ruptures)
#         return result

