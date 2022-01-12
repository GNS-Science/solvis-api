import json
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SOLVIS_API_URL, SOLVIS_API_KEY, IS_OFFLINE)
from api.solvis import multi_city_events
from api.datastore.model import SolutionLocationsRadiiDF,  set_local_mode


import pandas as pd
import io
import requests
from solvis import InversionSolution

headers={"x-api-key":API_KEY}
solvis_headers={'x-api-key': SOLVIS_API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

if IS_OFFLINE == '1':
    set_local_mode()

def process_event(evt):
    # print(evt)
    message = json.loads(evt['Sns']['Message'])

    _id = message['id']
    solution_id = message['solution_id']
    radii_list_id = message['radii_list_id']
    locations_list_id = message['locations_list_id']

    # WORK_PATH = "/tmp"
    #fetch the solution
    filename = toshi_api.inversion_solution.download_inversion_solution(solution_id, WORK_PATH)
    solution = InversionSolution().from_archive(filename)
    
    raw_cities = json.loads(requests.get(f'{SOLVIS_API_URL}/location_lists/{locations_list_id}/locations', headers=solvis_headers).text)
    cities = {location['id']: [location['name'], location['latitude'], location['longitude'], location['population']] for location in raw_cities}
    
    raw_radii = json.loads(requests.get(f'{SOLVIS_API_URL}/radii/{radii_list_id}', headers=solvis_headers).text)
    radii = raw_radii['radii']

    combos = multi_city_events.city_combinations(cities, 0, 5)
    print(f"city combos: {len(combos)}")

    rupture_radius_site_sets = multi_city_events.main(solution, cities, combos, radii)

    #now export for some science analysis
    df = pd.DataFrame.from_dict(rupture_radius_site_sets, orient='index')
    df = df.rename(columns=dict(zip(radii, [f'r{int(r/1000)}km' for r in radii])))

    ruptures = df.join(solution.ruptures_with_rates)
    binary_frame = io.BytesIO()
    ruptures.to_pickle(binary_frame, 'xz')
    binary_frame.seek(0)
    print('pickle size:', binary_frame.getbuffer().nbytes)
    
    ##Save the dataframe
    dataframe = SolutionLocationsRadiiDF(
        id = _id,
        solution_id = solution_id,
        locations_list_id = locations_list_id,
        radii_list_id =  radii_list_id,
        dataframe = binary_frame.getvalue())

    dataframe.save()

def handler(event, context):

        for evt in event['Records']:
            process_event(evt)
