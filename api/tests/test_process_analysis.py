import unittest
import json
from unittest import mock
import datetime as dt

from moto import mock_dynamodb2
from api import process_solution

from api.config import SOLVIS_API_URL

LOCATION_LISTS = [
    {'id':'N1', 'name': "Default NZ locations", 'locations': ['LOC1', 'LOC2']},
    {'id':'N2', 'name': "Main Cities NZ", 'locations': ['LOC3',]},
]

LOCATIONS = [
    {'id': 'LOC1', 'name': 'Wellington', 'latitude': -41.276825, 'longitude': 174.777969, 'population': 200000.0},
    {'id': 'LOC2', 'name': 'Gisborne', 'latitude': -38.662334, 'longitude': 178.017654, 'population': 50000.0},
    {'id': 'LOC3', 'name': 'Gisborne', 'latitude': -38.662334, 'longitude': 178.017654, 'population': 50000.0},
]

RADII = [{'id': '1', 'radii': [10e3]},
         {'id': '2', 'radii': [10e3,20e3]},
         {'id': '3', 'radii': [10e3,20e3,30e3]},
         {'id': '4', 'radii': [10e3,20e3,30e3,40e3]},
         {'id': '5', 'radii': [10e3,20e3,30e3,40e3,50e3]},
         {'id': '6', 'radii': [10e3,20e3,30e3,40e3,50e3,100e3]}]


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code
        self.elapsed = dt.timedelta(seconds=1)

    # mock json() method always returns a specific testing dictionary
    def json(self):
        return self.json_data

def side_effect_get(*args, **kwargs):
    print(args[0], kwargs)

    if 'locations' in args[0]:
        return MockResponse(LOCATIONS, 200)
    elif 'radii' in args[0]:
        return MockResponse(RADII[-1], 200)
    else:
        return None #

# #@mock_dynamodb2
# class TestHandleGeneralTaskRequest(unittest.TestCase):

#     @mock.patch('api.process_solution.requests.get', side_effect=[MockResponse(LOCATIONS, 200),MockResponse(RADII[-1], 200)])
#     def test_process_without_gt_or_sol_id_raises_two(self, m1):
#         with self.assertRaises(ValueError) as cm:
#             msg = {'id': "ABABBA", 'solution_id': None, 'locations_list_id': "NZ", 'radii_list_id': "6"}
#             sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
#             process_solution.handler(sns_event, {})

#     @mock.patch('api.process_solution.requests.get', side_effect=[MockResponse(LOCATIONS, 200),MockResponse(RADII[-1], 200)])
#     @mock.patch('api.process_solution.process_general_task_request')
#     def test_process_gt_id(self, m1, m2):
#         msg = {'id': "ABABBA", 'general_task_id': "XGT", 'locations_list_id': "NZ", 'radii_list_id': "6"}
#         sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
#         process_solution.handler(sns_event, {})

#         m1.assert_called_once()
#         m1.assert_called_once_with("XGT", {'id': 'ABABBA', 'locations_list_id': 'NZ', 'radii_list_id': '6'})

#     @mock.patch('api.process_solution.requests.get', side_effect=[MockResponse(LOCATIONS, 200),MockResponse(RADII[-1], 200)])
#     @mock.patch('api.process_solution.process_general_task_request')
#     def test_process_gt_id_only_radii(self, m1, m2):
#         msg = {'id': "ABABBA", 'general_task_id': "XGT", 'locations_list_id': "NZ", 'radii_list_id': "6",
#                 'only_radii_kms' :"10e3"}
#         sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
#         process_solution.handler(sns_event, {})

#         m1.assert_called_once()
#         m1.assert_called_once_with("XGT", {'id': 'ABABBA', 'locations_list_id': 'NZ', 'radii_list_id': '6', 'only_radii_kms': '10e3'})

#     @mock.patch('api.process_solution.requests.get', side_effect=[MockResponse(LOCATIONS, 200),MockResponse(RADII[-1], 200)])
#     @mock.patch('api.process_solution.process_general_task_request')
#     def test_process_gt_id_only_loc(self, m1, m2):
#         msg = {'id': "ABABBA", 'general_task_id': "XGT", 'locations_list_id': "NZ", 'radii_list_id': "6",
#                 'only_location_ids': 'LOC1'}
#         sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
#         process_solution.handler(sns_event, {})

#         m1.assert_called_once()
#         m1.assert_called_once_with("XGT", {'id': 'ABABBA', 'locations_list_id': 'NZ', 'radii_list_id': '6', 'only_location_ids': 'LOC1'})

class TestHandleSolutionRequest(unittest.TestCase):

    @mock.patch('api.process_solution.requests.get', side_effect=[MockResponse(LOCATIONS, 200),MockResponse(RADII[-1], 200)])
    @mock.patch('api.process_solution.process_solution_request')
    def test_process_solution_id(self, m1, m2):
        msg = {'id': "ABABBA", 'solution_id': "XSI", 'locations_list_id': "NZ", 'radii_list_id': "6"}
        sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
        process_solution.handler(sns_event, {})

        m1.assert_called_once()
        m1.assert_called_once_with(msg)

    @mock.patch('api.process_solution.requests.get', side_effect=[MockResponse(LOCATIONS, 200),MockResponse(RADII[-1], 200)])
    @mock.patch('api.process_solution.process_solution_request')
    def test_process_solution_id_only_radii(self, m1, m2):
        msg = {'id': "ABABBA", 'solution_id': "XSI", 'locations_list_id': "NZ", 'radii_list_id': "6",
                'only_radii_kms' :"10e3"}
        sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
        process_solution.handler(sns_event, {})

        m1.assert_called_once()
        m1.assert_called_once_with(msg)