import json
from api.toshi_api.toshi_api import ToshiApi

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, SOLVIS_API_URL)
from api.solvis import multi_city_events
from api.model import SolutionLocationsRadiiDF
from api.model import set_local_mode

import pandas as pd
import io
import requests
from solvis import InversionSolution

headers={"x-api-key":API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

# set_local_mode()

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

    #ref https://service.unece.org/trade/locode/nz.htm
    cities = dict(
        WLG = ["Wellington", -41.276825, 174.777969, 2e5],
        GIS = ["Gisborne", -38.662334, 178.017654, 5e4],
        HLZ = ["Hamilton", -37.7826, 175.2528, 165e3],
        LYJ = ["Lower Hutt", -41.2127, 174.8997, 112e3]
    )

    combos = multi_city_events.city_combinations(cities, 0, 5)
    print(f"city combos: {len(combos)}")

    radii = [10e3,20e3]#,30e3,40e3,50e3,100e3] #AK could be larger ??

    rupture_radius_site_sets = multi_city_events.main(solution, cities, combos, radii)

    #now export for some science analysis
    df = pd.DataFrame.from_dict(rupture_radius_site_sets, orient='index')
    df = df.rename(columns=dict(zip(radii, [f'r{int(r/1000)}km' for r in radii])))

    # print(df)
    ruptures = df.join(solution.ruptures_with_rates)
    binary_frame = io.BytesIO()
    ruptures.to_pickle(binary_frame, 'zip')
    binary_frame.seek(0)
    
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
