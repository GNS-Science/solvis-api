import concurrent.futures
import itertools
import threading
from datetime import datetime as dt
from pathlib import PurePath

import pandas as pd
from solvis import *

lock = threading.Lock()


def sum_above(key_combo, cities, limit):
    for kc in key_combo:
        pop = 0
        for key in kc:
            pop += cities[key][3]  # sum population
        if pop >= limit:
            yield kc


def city_combinations(cities, pop_impacted=1e6, combo_max=5):
    combos = []
    for rng in range(1, min(len(cities), combo_max)):
        combos.extend(sum_above([c for c in itertools.combinations(cities, rng)], cities, pop_impacted))
    return combos


def pre_process(sol, cities, site_keys, radii):
    rupts_in_all_locs = set(sol.ruptures['Rupture Index'])

    locations = {}
    for sk in site_keys:
        locations[sk] = dict(info=cities[sk])

    for site_key, location in locations.items():
        location['radius'] = {}
        for radius in radii:
            location['radius'][radius] = {}
            polygon = circle_polygon(radius_m=radius, lat=location['info'][1], lon=location['info'][2])
            rupts = sol.get_ruptures_intersecting(polygon)
            # print(f"city: {site_key}, radius: {radius} , Pop: {location['info'][3]}, ruptures: {len(rupts)}")
            location['radius'][radius]['ruptures'] = rupts
    return locations


def process(args):
    sol, city_radius_ruptures, rupture_radius_site_sets, site_set, radius = args[:]
    events = set(sol.ruptures['Rupture Index'])
    if len(events) == 0:
        return

    for site in site_set:
        events = set(city_radius_ruptures[site]['radius'][radius]['ruptures']).intersection(events)

    if len(events):
        for rupture_idx in events:
            current = rupture_radius_site_sets.get(rupture_idx, {}).get(radius)
            if not current:
                pass
            elif len(current) < len(site_set):
                pass
                # print("update ", rupture_idx, radius, "from", current, "to", site_set)
            else:
                continue
            with lock:
                if not rupture_radius_site_sets.get(rupture_idx):
                    rupture_radius_site_sets[rupture_idx] = {}
                rupture_radius_site_sets[rupture_idx][radius] = "_".join(site_set)


def main(sol, cities, combos, radii):
    """ """
    t0 = dt.now()
    sol = new_sol(sol, rupt_ids_above_rate(sol, 0))
    # solutions = [("60hr-J0YWJU.zip", sol)]
    t1 = dt.now()

    city_radius_ruptures = pre_process(sol, cities, cities.keys(), radii)
    t2 = dt.now()

    print(f'pre-process events for {len(cities)} cities in {(t2-t1)}')

    rupture_radius_site_sets = {}
    site_set_rupts = {}

    def generate_args(sol, city_radius_ruptures, rupture_radius_site_sets):
        for site_set in combos:
            for radius in radii:
                yield (sol, city_radius_ruptures, rupture_radius_site_sets, site_set, radius)

    t3 = dt.now()
    with concurrent.futures.ThreadPoolExecutor(4) as executor:
        for res in executor.map(process, generate_args(sol, city_radius_ruptures, rupture_radius_site_sets)):
            pass

    t4 = dt.now()
    print(f'built city events by radius in {(t4-t3)}')

    return rupture_radius_site_sets
