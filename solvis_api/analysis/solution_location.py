"""
Solvis_db
"""

# import logging
# import geopandas as gpd
# import pandas as pd
# from typing import List, Iterator, Set


# from pathlib import PurePath
# import solvis
# name = "NZSHM22_InversionSolution-QXV0b21hdGlvblRhc2s6NTkzMHJ0YWJU.zip" #60hrs!

# 60hr
# WORK_PATH = "/home/chrisbc/DEV/GNS/opensha-modular/solvis"

# log = logging.getLogger()
# log.setLevel(logging.INFO)

locs = dict(
    WLG=["Wellington", -41.276825, 174.777969, 2e5],
    GIS=["Gisborne", -38.662334, 178.017654, 5e4],
    CHC=["Christchurch", -43.525650, 172.639847, 3e5],
    IVC=["Invercargill", -46.413056, 168.3475, 8e4],
    DUD=["Dunedin", -45.8740984, 170.5035755, 1e5],
    NPE=["Napier", -39.4902099, 176.917839, 8e4],
    NPL=["New Plymouth", -39.0579941, 174.0806474, 8e4],
    PMR=["Palmerston North", -40.356317, 175.6112388, 7e4],
    NSN=["Nelson", -41.2710849, 173.2836756, 8e4],
    BHE=["Blenheim", -41.5118691, 173.9545856, 5e4],
    WHK=["Whakatane", -37.9519223, 176.9945977, 5e4],
    GMN=["Greymouth", -42.4499469, 171.2079875, 3e4],
    ZQN=["Queenstown", -45.03, 168.66, 15e3],
    AKL=["Auckland", -36.848461, 174.763336, 2e6],
    ROT=["Rotorua", -38.1446, 176.2378, 77e3],
    TUO=["Taupo", -38.6843, 176.0704, 26e3],
    WRE=["Whangarei", -35.7275, 174.3166, 55e3],
    LVN=["Levin", -40.6218, 175.2866, 19e3],
    TMZ=["Tauranga", -37.6870, 176.1654, 130e3],
    TIU=['Timaru', -44.3904, 171.2373, 28e3],
    OAM=["Oamaru", -45.0966, 170.9714, 14e3],
    PUK=["Pukekohe", -37.2004, 174.9010, 27e3],
    HLZ=["Hamilton", -37.7826, 175.2528, 165e3],
    LYJ=["Lower Hutt", -41.2127, 174.8997, 112e3],
)


radii = [10e3, 20e3, 30e3, 40e3, 50e3, 100e3, 250e4]  # AK could be larger ??

# if __name__ == '__main__':

#     model.set_local_mode()

#     #part one in the analysis lambda
#     solution_id = "ZZZ"
#     sol = solvis.InversionSolution().from_archive(PurePath(WORK_PATH,  name))
#     sol = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 1e-20)) #TODO: the solvis function above 0 isn't working

#     clean_slate()
#     save_solution_location_radii(solution_id, get_location_radius_rupture_models(solution_id, sol, locations=locs, radii=radii)) # noqa
#     save_solution_ruptures(solution_id, get_ruptures_with_rates(solution_id, sol))
#     save_solution_fault_sections(solution_id, get_fault_section_models(solution_id, sol))

#     # part two in the actual API
#     #run this to simulate a bunch of user queries...
#     #for locs in [['WHK'], ['PMR'], ['WHK', 'PMR']]:
#     radius=50000
#     minimum_rate=1e-12
#     for locs in [['WLG'], ['WLG', 'NSN'], ['WLG', 'PMR', "NSN"]]:
#         rupture_sections_gdf = matched_rupture_sections_gdf(solution_id, locs, radius, minimum_rate)
#         if not rupture_sections_gdf is None:
#             solvis.export_geojson(rupture_sections_gdf, f"{'_'.join(locs)}_rad({radius})_rate({minimum_rate})_query_result.geojson")  # noqa
#     print('DONE')
