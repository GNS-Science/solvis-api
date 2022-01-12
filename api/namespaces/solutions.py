#!python

import json
import io
import boto3
import logging

from functools import lru_cache
from uuid import uuid4

from flask_restx import reqparse
from flask_restx import Namespace, Resource, fields
from pandas.io import pickle
from api.toshi_api.toshi_api import ToshiApi

from api.config import (API_KEY, API_URL, S3_URL, SNS_TOPIC_ARN, IS_OFFLINE)
from api.solvis import multi_city_events

import pandas as pd
import geopandas as gpd
from solvis import InversionSolution, new_sol, section_participation
# from api.model import SolutionLocationsRadiiDF

from api.namespaces.solution_analysis_geojson import get_solution_dataframe_result
from api.namespaces.solution_analysis_geojson import api

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

from api.datastore.datastore import get_ds

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

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
        datastore = get_ds()
        solutions = datastore.resources.solutions
        return [x for x in solutions.scan(
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

        if IS_OFFLINE:
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


@api.route('/<sa_id>')
@api.param('sa_id', 'The solution analysis identifier')
@api.response(404, 'solution analysis not found')
class SolutionAnalysis(Resource):
    """
    SolutionAnalysis handlers
    """
    @api.doc('get_solution_analysis ')
    @api.marshal_with(solution_analysis_model)

    def get(self, sa_id):
        """
        GET handler
        """
        return get_solution_dataframe_result(sa_id)

