#!python

import json
import io

import logging

from functools import lru_cache
from uuid import uuid4

from functools import lru_cache

from flask_restx import reqparse
from flask_restx import Namespace, Resource, fields
from pandas.io import pickle
from api.toshi_api.toshi_api import ToshiApi

from api.config import (API_KEY, API_URL, S3_URL, SNS_TOPIC, IS_OFFLINE, IS_TESTING)
from api.solvis import multi_city_events

import pandas as pd
import geopandas as gpd
from solvis import InversionSolution, new_sol, section_participation
# from api.model import SolutionLocationsRadiiDF

#from api.namespaces.solution_analysis_geojson import get_solution_dataframe_result
from api.namespaces.solution_analysis_geojson import api

log = logging.getLogger(__name__)
#log.setLevel(logging.DEBUG)

from api.datastore.datastore import get_ds

from api.aws_util import publish_message

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)



#need to keep this in sync with pynamodb model.SolutionLocationsRadiiDF. Must be a better way
solution_analysis_model = api.model('Solution Analysis', dict(
    id = fields.String(description='The unique identifier for this analysis request'),
    solution_id = fields.String(required=True, description='The solution identifier (ref Toshi API '),
    locations_list_id = fields.String(required=True, description='The locations_list ID (see /location_lists)'),
    radii_list_id = fields.String(required=True, description='The radii_list ID (see /radii_lists)'),
    #created = fields.DateTime(description='The created timestamp'),
    #dataframe = fields.String(description='The dataframe in json, only availble with filter or single get')
))

@api.route('/inversion_solution')
class SolutionAnalysisList(Resource):
    # @api.doc('list_solution')
    # @api.marshal_list_with(solution_analysis_model)
    # def get(self):
    #     '''List all solution'''
    #     datastore = get_ds()
    #     solutions = datastore.resources.solutions
    #     return [x for x in solutions.scan(
    #         attributes_to_get=["id", "solution_id", "locations_list_id", "radii_list_id", "created"]
    #         )]


    # @api.doc('create_solution_analysis ')
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

        msg = {'id': _id, 'solution_id': solution_id, 'locations_list_id': locations_list_id, 'radii_list_id': radii_list_id}

        publish_message(msg)

        api.payload['id'] = _id
        return api.payload


general_task_model = api.model('Toshi API General Task Input ', dict(
    id = fields.String(description='The unique identifier for this analysis request'),
    general_task_id = fields.String(required=True, description='The General Task identifier (ref Toshi API '),
    locations_list_id = fields.String(required=True, description='The locations_list ID (see /location_lists)'),
    radii_list_id = fields.String(required=True, description='The radii_list ID (see /radii_lists)'),
    only_location_ids = fields.String(required=False, description='optional location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'),
    only_radii_kms = fields.String(required=True, description='optional list of radius in km. e.g. `10,20`'))
)

@api.route('/general_task')
class GeneralTaskAnalysisRequest(Resource):
    """
    Initiate Analysis processing for all Solutions in the given General Task
    """
    @api.expect(general_task_model)
    @api.marshal_with(general_task_model)
    @api.response(404, 'general_task id not found')
    def post(self):

        log.debug(f"GeneralTaskAnalysisRequest post payload {api.payload}")

        _id = str(uuid4())
        general_task_id = api.payload['general_task_id']

        print(f"general_task_id: {general_task_id}")

        # locations_list_id = api.payload['locations_list_id']
        # radii_list_id = api.payload['radii_list_id']
        # only_location_ids = api.payload.get('only_location_ids')
        # only_radii_kms = api.payload.get('only_radii_kms')

        gt = toshi_api.general_task.get_general_task(general_task_id)

        print(f"general_task_id: {gt}")

        api.payload['id'] = _id

        if not gt:
            api.abort(404, f'The requested General Task ({general_task_id}) was not found.')
        else:
            publish_message(api.payload)

        return api.payload