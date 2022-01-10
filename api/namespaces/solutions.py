#!python

import json
import io
import boto3
from functools import lru_cache
from uuid import uuid4

from flask_restx import reqparse
from flask_restx import Namespace, Resource, fields
from pandas.io import pickle
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SNS_TOPIC_ARN, LOCAL_MODE)
from api.solvis import multi_city_events

import pandas as pd
import geopandas as gpd
from solvis import InversionSolution, new_sol, section_participation
from api.model import SolutionLocationsRadiiDF

import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

api = Namespace('solutions', description='Solution analysis related operations')

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

@lru_cache(maxsize=32)
def get_inversion_solution(solution_id):
    WORK_PATH = "./tmp"
    log.info(f'get_inversion_solution: {solution_id}')
    filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
    return InversionSolution().from_archive(filename)

@lru_cache(maxsize=32)
def get_solution_dataframe_result(_id):
    try:
        log.info(f'get_solution_dataframe_result: {_id}')
        return SolutionLocationsRadiiDF.get(_id) #     5cd8df26-2370-4bbb-8e3d-03e6453e0c65
    except (SolutionLocationsRadiiDF.DoesNotExist):
        api.abort(404)

@lru_cache(maxsize=32)
def get_solution_dataframe(_id):
    return pd.read_json(get_solution_dataframe_result(_id).dataframe)

def filter_dataframe(id, location_id: str, radius_km: int, rate_threshold: float):

    df = get_solution_dataframe(id) #cached

    def get_radii_map(df):
        rad_cols = {}
        for c in df.columns:
            if c[0] == 'r' and c[-2:] == 'km':
                rad_cols[int(c[1:-2])] = c
        return rad_cols

    def where_city_in_radius(df: pd.DataFrame, radius_column: str, location_id: str):
        return df[radius_column].apply(lambda x: location_id in x if x else False) == True

    #get the radius column for first filter layer
    radius_map = get_radii_map( df )
    if not radius_km in radius_map.keys():
        api.abort(404, f'radius {radius_km} is not in {[k for k in radius_map.keys()]}')
    radius_column = radius_map[radius_km]

    print(get_radii_map( df ), radius_column)

    df2 = df[where_city_in_radius(df, radius_column, location_id)]
    if rate_threshold > 0:
        df2 = df2[df2['Annual Rate']>= rate_threshold]

    print(df2.shape)
    return df2

@lru_cache(maxsize=32)
def get_filtered_layers(dataframe_id, location_id, radius_km, annual_rate_threshold):
    filtered = filter_dataframe(dataframe_id, location_id, int(radius_km), float(annual_rate_threshold))

    result = get_solution_dataframe_result(dataframe_id) #cached
    sol = get_inversion_solution(result.solution_id) #cached

    rupture_idxs = list(filtered['Rupture Index'])

    rsol = new_sol(sol, rupture_idxs)

    layers = []
    for r in rupture_idxs:
        sp0 = section_participation(rsol)
        layers.append(dict(rupture_id=str(r), layer_name=str(r), geojson=gpd.GeoDataFrame(sp0).to_json()))
    return layers



#need to keep this in sync with pynamodb model.SolutionLocationsRadiiDF. Must be a better way
solution_analysis_model = api.model('Solution Analysis', dict(
    id = fields.String(description='The unique identifier for this analysis'),
    solution_id = fields.String(required=True, description='The solution identifier (ref Toshi API '),
    locations_list_id = fields.String(required=True, description='The locations_list ID (see /location_lists)'),
    radii_list_id = fields.String(required=True, description='The radii_list ID (see /radii_lists)'),
    created = fields.DateTime(description='The created timestamp'),
    dataframe = fields.String(description='The dataframe in json, only availble with filter or single get')
))

@api.route('/')
class SolutionAnalysisList(Resource):
    @api.doc('list_solution')
    @api.marshal_list_with(solution_analysis_model)
    def get(self):
        '''List all solution'''
        return [x for x in SolutionLocationsRadiiDF.scan(
            attributes_to_get=["id", "solution_id", "locations_list_id", "radii_list_id", "created"]
            )]


    @api.doc('create_solution_analysis ')
    @api.expect(solution_analysis_model)
    @api.marshal_with(solution_analysis_model)
    @api.response(404, 'solution id not found')
    def post(self):
        '''Create a solution_analysis SNS message, returning the id of the analysis'''

        log.debug(f"payload {api.payload}")

        solution_id = api.payload['solution_id']
        locations_list_id = api.payload['locations_list_id']
        radii_list_id = api.payload['radii_list_id']

        _id = str(uuid4())

        #TODO error handling here...
        solution = toshi_api.inversion_solution.get_file_download_url(solution_id)

        if LOCAL_MODE:
            AWS_REGION = 'ap-southeast-2'
            client = boto3.client('sns', endpoint_url="http://127.0.0.1:4002", region_name=AWS_REGION)
        else:
            client = boto3.client('sns')

        log.debug(f"SNS_TOPIC_ARN {SNS_TOPIC_ARN}")
        response = client.publish (
            TargetArn = SNS_TOPIC_ARN,
            Message = json.dumps({'default': json.dumps(
                    {'id': _id, 'solution_id': solution_id, 'locations_list_id': locations_list_id, 'radii_list_id': radii_list_id})
                }),
            MessageStructure = 'json'
        )

        log.debug(f"SNS reponse {response}")

        api.payload['id'] = _id
        return api.payload


@api.route('/<id>')
@api.param('id', 'The solution analysis identifier')
@api.response(404, 'solution analysis not found')
class SolutionAnalysis(Resource):
    @api.doc('get_solution_analysis ')
    @api.marshal_with(solution_analysis_model)
    def get(self, id):
        try:
            result = SolutionLocationsRadiiDF.get(id) #     5cd8df26-2370-4bbb-8e3d-03e6453e0c65
            pickle_bytes = io.BytesIO()
            pickle_bytes.write(result.dataframe)
            pickle_bytes.seek(0)
            result.dataframe = pd.read_pickle(pickle_bytes, 'zip').to_json(indent=2)
        except (SolutionLocationsRadiiDF.DoesNotExist):
            api.abort(404)
        return result

#need to keep this in sync with pynamodb model.SolutionLocationsRadiiDF. Must be a better way
geojson_layer_model = api.model('Geojson Layer', dict(
    rupture_id = fields.Integer(required=True),
    layer_name = fields.String(required=True),
    geojson = fields.String(description='The layer geojson')),
    description='A geojson layer representing either a rupture or related poygons'
)

solution_analysis_geojson_layers_model = api.model('Solution Analysis Geojson', dict(
    id = fields.String(description='The unique identifier for this analysis'),
    #solution_id = fields.String(required=True, description='The solution identifier (ref Toshi API '),
    location_id = fields.String(required=True),
    radius_km = fields.String(required=True),
    rupture_count = fields.Integer(),
    created = fields.DateTime(description='The created timestamp'),
    rupture_layers = fields.List(fields.Nested(geojson_layer_model), description='list of geojson models, one per rupture')
))

@api.route('/<id>/loc/<location_id>/rad/<int:radius_km>/geojson')
@api.param('id', 'The solution analysis identifier')
@api.param('location_id', 'The location identifier')
@api.param('radius_km', 'The rupture/location intersection radius in km')
@api.response(404, 'solution analysis not found')
class SolutionAnalysisGeojsonLayers(Resource):
    @api.doc('get_solution_analysis_geojson ')
    @api.marshal_with(solution_analysis_geojson_layers_model)
    def get(self, id, location_id, radius_km):

        #TODO fix this it's not working
        parser = reqparse.RequestParser()
        parser.add_argument('rate_above', type=str, help='include ruptures with an Annual Rate above this value  (default = 0)')
        args = parser.parse_args() or {}
        print ('args', args)
        annual_rate_threshold=float(args.get('rate_above') or 0)

        result = get_solution_dataframe_result(id) #cached

        layers = get_filtered_layers(id, location_id, radius_km, annual_rate_threshold) #cached

        result.location_id = location_id
        result.radius_km = radius_km
        result.rupture_count = len(layers)
        result.rupture_layers = layers
        return result


