# from shutil import copy

import unittest
from moto import mock_dynamodb2

from api.api import create_app
from api.datastore.datastore import get_datastore

# Set up your local config, from environment variables, with some sone defaults
from api.config import (WORK_PATH, USE_API, API_KEY, API_URL, S3_URL, IS_OFFLINE)

class TestResources:
    LOCATION_LISTS = [
        {'id':'N1', 'name': "Default NZ locations", 'locations': ['LOC1', 'LOC2']},
        {'id':'N2', 'name': "Main Cities NZ", 'locations': ['LOC3',]},
    ]

    LOCATIONS = [
        {'id': 'LOC1', 'name': 'Wellington', 'latitude': -41.276825, 'longitude': 174.777969, 'population': 200000.0},
        {'id': 'LOC2', 'name': 'Gisborne', 'latitude': -38.662334, 'longitude': 178.017654, 'population': 50000.0},
        {'id': 'LOC3', 'name': 'Gisborne', 'latitude': -38.662334, 'longitude': 178.017654, 'population': 50000.0},
    ]

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        app = create_app()
        ds = get_datastore()
        ds.configure(TestResources)

        app.config["TESTING"] = True
        self.client =  app.test_client()


@mock_dynamodb2
class TestLocationList(BaseTestCase):

    def test_get_all(self):
        response = self.client.get('/location_lists/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, TestResources.LOCATION_LISTS)
        
    def test_get_one(self):
        response = self.client.get('/location_lists/N1')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, TestResources.LOCATION_LISTS[0])


@mock_dynamodb2
class TestLocation(BaseTestCase):

    def test_get_all(self):
        response = self.client.get('/locations/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, TestResources.LOCATIONS)

    def test_get_one(self):
        response = self.client.get('/locations/LOC2')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, TestResources.LOCATIONS[1])


@mock_dynamodb2
class TestKaikouraCase(unittest.TestCase):
    def setUp(self):
        app = create_app()
        self.client =  app.test_client()

    def test_get_one(self):
        response = self.client.get('/locations/KBZ')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['name'], 'Kaikoura')

    def test_is_in_defaul_list(self):
        response = self.client.get('/location_lists/NZ')
        self.assertEqual(response.status_code, 200)
        self.assertIn('KBZ', response.json['locations'])