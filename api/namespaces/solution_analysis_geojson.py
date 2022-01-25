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

solution_analysis_composite_geojson_model = api.model('Solution Analysis Geojson', dict(
    solution_id = fields.String(required=True, description='The unique identifier for the Inversion Solution.'),
    location_ids = fields.String(required=True, description='The location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'),
    radius_km = fields.String(required=True, description='The rupture/location intersection radius in km.'),
    rupture_count = fields.Integer(),
    section_count = fields.Integer(),
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
    @api.param('min_rate', 'restrict ruptures to those having a annual rate above the value supplied.')
    @api.param('max_rate', 'restricts ruptures to those having a annual rate below the value supplied.')
    @api.param('min_mag', 'restricts ruptures to those having a magnitude above the value supplied.')
    @api.param('max_mag', 'restricts ruptures to those having a magnitude below the value supplied.')
    @api.marshal_with(solution_analysis_composite_geojson_model)
    def get(self, solution_id, location_ids, radius_km):
        """
        GET handler
        """

        parser = reqparse.RequestParser()
        parser.add_argument('min_rate', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')
        parser.add_argument('max_rate', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')
        parser.add_argument('min_mag', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')
        parser.add_argument('max_mag', type=float, default=None,
            help='include ruptures with an Annual Rate above this value')

        args = parser.parse_args() or {}
        print ('args', args, self)

        location_ids = ','.join(sorted(location_ids.split(',')))

        rupture_sections_gdf = matched_rupture_sections_gdf(solution_id, location_ids, radius_km*1000,
            min_rate=args.get('min_rate') or 1e-12,
            max_rate=args.get('max_rate'), min_mag=args.get('min_mag'), max_mag=args.get('max_mag'))

        rupture_count, section_count = 0, 0
        if not rupture_sections_gdf is None:
            # # Here we want to collapse all ruptures so we have just one feature for section. Each section can have the
            # count of ruptures, min, mean, max magnitudes & annual rates

            section_aggregates_gdf = rupture_sections_gdf.pivot_table(
                index= ['section_index'],
                aggfunc=dict(annual_rate=['sum', 'min', 'max'],
                     magnitude=['count', 'min', 'max']))

            #print(section_aggregates_gdf)
            #join the rupture_sections_gdf details
            section_aggregates_gdf.columns = [".".join(a) for a in section_aggregates_gdf.columns.to_flat_index()]
            section_aggregates_gdf = section_aggregates_gdf\
                .join (rupture_sections_gdf, 'section_index', how='inner', rsuffix='_R')\
                .drop(columns=['rupture_index','annual_rate', 'avg_rake',  'magnitude'])

            rupture_count = len(rupture_sections_gdf.rupture_index.unique()) if not rupture_sections_gdf is None else 0
            section_count = section_aggregates_gdf.shape[0]
            geojson = gpd.GeoDataFrame(section_aggregates_gdf).to_json()
        else:
            geojson = json.dumps(dict(type="FeatureCollection", features= []))

        error_message = None
        if section_count > 10e3:
            error_message = "Too many rupture sections satisfy the query, please try a more selective query."
        elif section_count == 0:
            error_message = "No ruptures satisfy the query."

        result = dict(
            solution_id = solution_id,
            location_ids = location_ids,
            radius_km = radius_km,
            ruptures = geojson,
            rupture_count = rupture_count,
            section_count = section_count,
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

