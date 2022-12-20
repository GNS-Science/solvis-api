import unittest

from moto import mock_dynamodb2

from solvis_api.datastore import model
from solvis_api.datastore.solvis_db_query import get_rupture_ids


@mock_dynamodb2
class RutupreIdEdgecaseTest(unittest.TestCase):
    def setUp(self):
        model.SolutionLocationRadiusRuptureSet.create_table(wait=True)

        model.SolutionLocationRadiusRuptureSet(
            solution_id='test_solution_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3000000],
            rupture_count=3,
        ).save()

    def test_get_model(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['WLG'], radius=10000, union=False)
        print(ids)
        # assert(0)
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, set([1, 2, 3000000]))  # WLG IDS
