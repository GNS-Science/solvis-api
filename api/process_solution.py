import json
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SOLVIS_API_URL, SOLVIS_API_KEY, IS_OFFLINE)
#from api.solvis import multi_city_events
from api.datastore import model
from api.datastore.solvis_db import *

import pandas as pd
import io
import requests
import solvis
import logging

log = logging.getLogger('pynamodb')
log.setLevel(logging.INFO)

headers={"x-api-key":API_KEY}
solvis_headers={'x-api-key': SOLVIS_API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

if IS_OFFLINE:
    model.set_local_mode()

#ensure tables exist
model.migrate()

def process_event(evt):
    message = json.loads(evt['Sns']['Message'])

    _id = message['id']
    solution_id = message['solution_id']
    radii_list_id = message['radii_list_id']
    locations_list_id = message['locations_list_id']

    # WORK_PATH = "/tmp"
    #fetch the solution
    filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
    sol = solvis.InversionSolution().from_archive(filename)
    solution = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 1e-20)) #TODO: the solvis function above 0 isn't working
    
    locations = json.loads(requests.get(f'{SOLVIS_API_URL}/location_lists/{locations_list_id}/locations', headers=solvis_headers).text)

    raw_radii = json.loads(requests.get(f'{SOLVIS_API_URL}/radii/{radii_list_id}', headers=solvis_headers).text)
    radii = raw_radii['radii']

    save_solution_location_radii(solution_id, get_location_radius_rupture_models(solution_id, solution, locations=locations, radii=radii))
    save_solution_ruptures(solution_id, get_ruptures_with_rates(solution_id,solution))
    save_solution_fault_sections(solution_id, get_fault_section_models(solution_id,solution))

    #maybe return something :)

def handler(event, context):
        for evt in event['Records']:
            process_event(evt)
