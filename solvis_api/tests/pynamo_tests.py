import datetime
import unittest

from moto import mock_dynamodb2

from solvis_api.api import create_app
from solvis_api.datastore import model
from solvis_api.datastore.datastore import get_datastore

#
from solvis_api.datastore.solvis_db import get_location_radius_rupture_models
from solvis_api.datastore.solvis_db_query import get_rupture_ids


# from solvis_api.tests.test_api_location_list import TestResources
@mock_dynamodb2
class PynamoTest(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        # with self.app.app_context():
        model.set_local_mode()
        model.SolutionLocationRadiusRuptureSet.create_table(wait=True)
        print("Migrate created table: SolutionLocationRadiusRuptureSet")
        super(PynamoTest, self).setUp()

    def test_get_rupture_ids(self):

        # with self.app.app_context():
        dataframe = model.SolutionLocationRadiusRuptureSet(
            solution_id='test_solution_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            rupture_count=3,
        )
        dataframe.save()

        ids = get_rupture_ids(solution_id='test_solution_id', locations=['WLG'], radius=10000)

        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, set([1, 2, 3]))

    def test_table_exists(self):
        # with app.app_context():
        self.assertEqual(model.SolutionLocationRadiusRuptureSet.exists(), True)

    def tearDown(self):
        # with app.app_context():
        model.SolutionLocationRadiusRuptureSet.delete_table()
        return super(PynamoTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
