#!test_17_AKL_edge_case.py

import unittest

from moto import mock_dynamodb
from solvis_store import model
from solvis_store.solvis_db_query import get_rupture_ids


# from solvis_api.tests.test_api_location_list import TestResources
@mock_dynamodb
class EmptyLocationEdgecaseTest(unittest.TestCase):
    def setUp(self):
        model.SolutionLocationRadiusRuptureSet.create_table(wait=True)

        model.SolutionLocationRadiusRuptureSet(
            solution_id='test_solution_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            rupture_count=3,
        ).save()

        model.SolutionLocationRadiusRuptureSet(
            solution_id='test_solution_id',
            location_radius='ZZZ:10000',
            radius=10000,
            location='ZZZ',
            ruptures=[1, 3, 4],
            rupture_count=2,
        ).save()

        model.SolutionLocationRadiusRuptureSet(
            solution_id='test_solution_id', location_radius='ROT:10000', radius=10000, location='ROT', rupture_count=0
        ).save()

        super(EmptyLocationEdgecaseTest, self).setUp()

    def test_get_WLG_rupture_ids(self):

        wlg_ids = get_rupture_ids(solution_id='test_solution_id', locations=['WLG'], radius=10000)
        self.assertEqual(len(wlg_ids), 3)
        self.assertEqual(wlg_ids, set([1, 2, 3]))

    def test_get_AKL_rupture_ids(self):

        ids = get_rupture_ids(solution_id='test_solution_id', locations=['AKL'], radius=10000)
        self.assertEqual(len(ids), 0)
        self.assertEqual(ids, set([]))

    def test_get_AKL_WLG_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['AKL', 'WLG'], radius=10000)
        print(ids)
        self.assertEqual(ids, set([]))

    def test_get_AKL_WLG_union_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['AKL', 'WLG'], radius=10000, union=True)
        print(ids)
        self.assertEqual(ids, set([1, 2, 3]))

    def test_get_ZZZ_WLG_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ZZZ', 'WLG'], radius=10000)
        print(ids)
        self.assertEqual(ids, set([1, 3]))

    def test_get_ZZZ_WLG_union_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ZZZ', 'WLG'], radius=10000, union=True)
        print(ids)
        self.assertEqual(ids, set([1, 2, 3, 4]))

    # https://fcx7tkv322.execute-solvis_api.ap-southeast-2.amazonaws.com/test/solution_analysis/SW52ZXJzaW9uU29sdXRpb246Mjc0OS4wMlhVTlA=/loc/ROT/rad/10?max_mag=10&min_mag=5&max_rate=1e0&min_rate=1e-20
    def test_get_model_with_empty_ruptures_attribute(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ROT'], radius=10000)
        print(ids)
        self.assertEqual(ids, set([]))

    def test_get_model_with_empty_and_nonempty_ruptures_attribute_and(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ROT', 'WLG'], radius=10000, union=False)
        print(ids)
        self.assertEqual(ids, set([]))

    def test_get_model_with_empty_and_nonempty_ruptures_attribute_or(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['ROT', 'WLG'], radius=10000, union=True)
        print(ids)
        # assert(0)
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, set([1, 2, 3]))  # WLG IDS
