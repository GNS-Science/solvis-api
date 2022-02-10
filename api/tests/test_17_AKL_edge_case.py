#!test_17_AKL_edge_case.py

import unittest
from moto import mock_dynamodb2
from api.datastore import model
from api.datastore.solvis_db_query import get_rupture_ids


# from api.tests.test_api_location_list import TestResources
@mock_dynamodb2
class EmptyLocationEdgecaseTest(unittest.TestCase):

    def setUp(self):
        model.SolutionLocationRadiusRuptureSet.create_table(wait=True)

        model.SolutionLocationRadiusRuptureSet(
            solution_id = 'test_solution_id',
            location_radius = 'WLG:10000',
            radius =  10000,
            location = 'WLG',
            ruptures = [1,2,3],
            rupture_count = 3
            ).save()

        model.SolutionLocationRadiusRuptureSet(
            solution_id = 'test_solution_id',
            location_radius = 'ZZZ:10000',
            radius =  10000,
            location = 'ZZZ',
            ruptures = [1,3,4],
            rupture_count = 2
            ).save()

        super(EmptyLocationEdgecaseTest, self).setUp()

    def test_get_WLG_rupture_ids(self):

        wlg_ids = get_rupture_ids(solution_id='test_solution_id', locations=['WLG'], radius=10000)
        self.assertEqual(len(wlg_ids), 3)
        self.assertEqual(wlg_ids, set([1,2,3]))

    def test_get_AKL_rupture_ids(self):

        ids = get_rupture_ids(solution_id='test_solution_id', locations=['AKL'], radius=10000)
        self.assertEqual(len(ids), 0)
        self.assertEqual(ids, set([]))

    def test_get_AKL_WLG_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['AKL','WLG'], radius=10000)
        print(ids)
        self.assertEqual(ids, set([]))

    def test_get_AKL_WLG_union_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['AKL','WLG'], radius=10000, union=True)
        print(ids)
        self.assertEqual(ids, set([1,2,3]))

    def test_get_ZZZ_WLG_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ZZZ','WLG'], radius=10000)
        print(ids)
        self.assertEqual(ids, set([1,3]))

    def test_get_ZZZ_WLG_union_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ZZZ','WLG'], radius=10000, union=True)
        print(ids)
        self.assertEqual(ids, set([1,2,3,4]))