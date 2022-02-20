import json
import requests
import solvis
import logging
import copy

from api.toshi_api.toshi_api import ToshiApi
from api.datastore import model
from api.datastore.solvis_db import *
from api.aws_util import publish_message
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SOLVIS_API_URL, SOLVIS_API_KEY, IS_OFFLINE)


log = logging.getLogger('pynamodb')
log.setLevel(logging.INFO)

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)


def process_solution_request(message):

    log.debug(f"process_solution_request {message}")

    solution_id = message.get('solution_id')
    radii_list_id = message['radii_list_id']
    locations_list_id = message['locations_list_id']
    only_location_ids = message.get('only_location_ids') #'optional location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'),
    only_radii_kms = message.get('only_radii_kms')       #'optional list of radius in km. e.g. `10,20`'))

    solvis_headers={'x-api-key': SOLVIS_API_KEY}
    locations = requests.get(f'{SOLVIS_API_URL}/location_lists/{locations_list_id}/locations', headers=solvis_headers).json()
    radii = requests.get(f'{SOLVIS_API_URL}/radii/{radii_list_id}', headers=solvis_headers).json()['radii']

    filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
    sol = solvis.InversionSolution().from_archive(filename)
    solution = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 1e-20))

    #apply list filters if they're defined
    if only_location_ids:
        only_location_ids = [w.strip() for w in only_location_ids.split(',')]
        locations = [loc for loc in locations if loc['id'] in only_location_ids]

    if only_radii_kms:
        only_radii_kms = [float(r) for r in only_radii_kms.split(',')]
        radii = [rad for rad in radii if rad in only_radii_kms]

    if IS_OFFLINE:
        model.set_local_mode()

    #ensure tables exist
    model.migrate()

    save_solution_location_radii(solution_id, get_location_radius_rupture_models(solution_id, solution, locations=locations, radii=radii))
    save_solution_ruptures(solution_id, get_ruptures_with_rates(solution_id,solution))
    save_solution_fault_sections(solution_id, get_fault_section_models(solution_id,solution))

    log.info("process_solution_request completed OK")

def process_general_task_request(general_task_id, message):
    gt = toshi_api.general_task.get_general_task_subtasks(general_task_id)

    for node in gt['children']['edges']:

        at = node['node']['child']
        solution = at.get('inversion_solution')

        #create a soluton message
        msg = copy.copy(message)
        msg['solution_id'] = solution.get('id')
        assert not msg.get('general_task_id')
        publish_message(msg)

def process_event(evt):

    print(evt)

    message = json.loads(evt['Sns']['Message'])

    _id = message['id']

    general_task_id = message.pop('general_task_id', None) #pop the GT id - we don't want a loop
    solution_id = message.get('solution_id')

    if general_task_id:
        process_general_task_request(general_task_id, message)
    elif solution_id:
        process_solution_request(message)
    else:
        raise ValueError("need one of solution_id or general_task_id")

def handler(event, context):
    for evt in event['Records']:
        process_event(evt)
    return True
