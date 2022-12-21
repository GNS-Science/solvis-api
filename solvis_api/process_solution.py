import json
import logging
import logging.config
import os

import requests
import solvis
import yaml
from solvis_store import model, solvis_db

from solvis_api.config import (
    API_KEY,
    API_URL,
    IS_OFFLINE,
    LOGGING_CFG,
    S3_URL,
    SOLVIS_API_KEY,
    SOLVIS_API_URL,
    WORK_PATH,
)
from solvis_api.toshi_api.toshi_api import ToshiApi

log = logging.getLogger(__name__)

headers = {"x-api-key": API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

if os.path.exists(LOGGING_CFG):
    with open(LOGGING_CFG, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=logging.INFO)


def process_solution_request(message):

    log.debug(f"process_solution_request {message}")

    solution_id = message.get('solution_id')
    radii_list_id = message['radii_list_id']
    locations_list_id = message['locations_list_id']
    only_location_ids = message.get(
        'only_location_ids'
    )  # 'optional location identifiers, comma-delmited list e.g. `WLG,PMR,ZQN`'),
    only_radii_kms = message.get('only_radii_kms')  # 'optional list of radius in km. e.g. `10,20`'))

    solvis_headers = {'x-api-key': SOLVIS_API_KEY}
    locations = requests.get(
        f'{SOLVIS_API_URL}/location_lists/{locations_list_id}/locations', headers=solvis_headers
    ).json()
    radii = requests.get(f'{SOLVIS_API_URL}/radii/{radii_list_id}', headers=solvis_headers).json()['radii']

    filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
    sol = solvis.InversionSolution().from_archive(filename)
    solution = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 1e-20))

    # apply list filters if they're defined
    if only_location_ids:
        only_location_ids = [w.strip() for w in only_location_ids.split(',')]
        locations = [loc for loc in locations if loc['id'] in only_location_ids]

    if only_radii_kms:
        only_radii_kms = [float(r) for r in only_radii_kms.split(',')]
        radii = [rad for rad in radii if rad in only_radii_kms]

    if IS_OFFLINE:
        model.set_local_mode()

    # ensure tables exist
    model.migrate()

    solvis_db.save_solution_location_radii(
        solution_id,
        solvis_db.get_location_radius_rupture_models(solution_id, solution, locations=locations, radii=radii),
    )
    solvis_db.save_solution_ruptures(solution_id, solvis_db.get_ruptures_with_rates(solution_id, solution))
    solvis_db.save_solution_fault_sections(solution_id, solvis_db.get_fault_section_models(solution_id, solution))

    log.info("process_solution_request completed OK")


def process_event(evt):

    print(evt)
    message = json.loads(evt['Sns']['Message'])
    solution_id = message.get('solution_id')

    if solution_id:
        process_solution_request(message)
    else:
        raise ValueError("need a solution_id")


def handler(event, context):

    for evt in event.get('Records', []):
        process_event(evt)
    return True
