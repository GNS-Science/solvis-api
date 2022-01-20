import geopandas as gpd
import pandas as pd
from typing import List, Iterator, Set
import logging

from pathlib import PurePath
import solvis

from api.datastore import model
from api.datastore.solvis_db import *

name = "NZSHM22_InversionSolution-QXV0b21hdGlvblRhc2s6NTkzMHJ0YWJU.zip" #60hrs!

#60hr
WORK_PATH = "/home/chrisbc/DEV/GNS/opensha-modular/solvis"

log = logging.getLogger()
log.setLevel(logging.INFO)



# def query():

#     # for item in mSLR.query(f'{solution_id}',
#     #     mSLR.location_radius.startswith("WLG"),
#     #     #filter_condition=mSLR.rupture_count > 200,
#     #     limit=20):
#     #     print("Query returned item {0}".format(item), item.rupture_count)
#     # print()

#     # for rupt in mSR.query(f'{solution_id}',
#     #     #mRR.rupture_index == 238707,
#     #     filter_condition=mSR.magnitude > 8,
#     #     limit=20):
#     #     print("Query returned rupt {0}".format(rupt), rupt.magnitude, len(rupt.fault_sections))
#     # print()

#     for item in mSFS.query(f'{solution_id}',
#         #mSFS.section_index == 0, #list(rupt.fault_sections)
#         #filter_condition=mSFS.section_index.is_in(*list(rupt.fault_sections)),
#         #mSFS.section_index.is_in(*list(rupt.fault_sections)),
#         limit=20):
#         print("Query returned item {0}".format(item), item.rake, item.geometry)
#     print()

# QUERY operations for the API get endpoint(s)
def get_rupture_ids(solution_id:str, locations:List[str], radius:int, union:bool =True) -> Set[int]:
    ids = None
    for loc in locations:
        items = [i for i in mSLR.query(f'{solution_id}', mSLR.location_radius == f"{loc}:{radius}")]
        assert len(items) in [0,1]
        if len(items):
            item = items[0]
            print(f'SLR query item: {item}, ruptures: {item.ruptures}')
            if not item.ruptures:
                continue
            if union:
                ids = ids.union(item.ruptures) if ids else set(item.ruptures)
            else:
                ids = ids.intersection(item.ruptures) if ids else set(item.ruptures)
    return ids or set()

def get_ruptures(solution_id:str) -> gpd.GeoDataFrame:
    index = []
    values = []
    for item in mSR.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.rupture_index)
    return pd.DataFrame(values, index=index)

def get_fault_sections(solution_id:str) -> gpd.GeoDataFrame:
    index = []
    values = []
    for item in mSFS.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.section_index)

    df = pd.DataFrame(values, index)
    return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df.geometry))


def matched_rupture_sections_gdf(solution_id:str, locations:List[str], radius:int, union:bool=False):

    print('Intersection/Union')
    ids = get_rupture_ids(solution_id, locations, int(radius), union)
    if not ids:
        return

    ruptures_df = get_ruptures(solution_id)
    ruptures_df = ruptures_df[ruptures_df.rupture_index.isin(list(ids))]

    print("Build RuptureSections df")

    def build_rupture_sections_df(ruptures_df: pd.DataFrame) -> pd.DataFrame:
        table = []
        for row in ruptures_df.itertuples():
            rupture_id=row[0]
            fault_sections=row[4]
            for section_id in fault_sections:
                table.append(dict(rupture_index=rupture_id, section_index=section_id))

        return pd.DataFrame.from_dict(table)

    rupture_sections_df = build_rupture_sections_df(ruptures_df)

    print("get fault sections GDF")
    sections_gdf = get_fault_sections(solution_id)

    print("Assemble geojson")
    #join rupture details
    rupture_sections_df = rupture_sections_df\
        .join(ruptures_df, 'rupture_index', how='inner', rsuffix='_R')\
        .drop(columns=['fault_sections', 'rupture_index_R'])

    #join fault_section details as GeoDataFrame
    rupture_sections_gdf = gpd.GeoDataFrame(rupture_sections_df)\
        .join (sections_gdf, 'section_index', how='inner', rsuffix='_R')\
        .drop(columns=['section_index_R'])

    print(f'columns: {rupture_sections_gdf.columns}')
    print( rupture_sections_gdf[['rupture_index', 'section_index', 'fault_name', 'magnitude']] )

    return rupture_sections_gdf


locs = dict(
    WLG = ["Wellington", -41.276825, 174.777969, 2e5],
    GIS = ["Gisborne", -38.662334, 178.017654, 5e4],
    CHC = ["Christchurch", -43.525650, 172.639847, 3e5],
    IVC = ["Invercargill", -46.413056, 168.3475, 8e4],
    DUD = ["Dunedin", -45.8740984, 170.5035755, 1e5],
    NPE = ["Napier", -39.4902099, 176.917839, 8e4],
    NPL = ["New Plymouth", -39.0579941, 174.0806474, 8e4],
    PMR = ["Palmerston North", -40.356317, 175.6112388, 7e4],
    NSN = ["Nelson", -41.2710849, 173.2836756, 8e4],
    BHE = ["Blenheim", -41.5118691, 173.9545856, 5e4],
    WHK = ["Whakatane", -37.9519223, 176.9945977, 5e4],
    GMN = ["Greymouth", -42.4499469, 171.2079875, 3e4],
    ZQN = ["Queenstown", -45.03, 168.66, 15e3],
    AKL = ["Auckland", -36.848461, 174.763336, 2e6],
    ROT = ["Rotorua", -38.1446, 176.2378, 77e3],
    TUO = ["Taupo", -38.6843, 176.0704, 26e3],
    WRE = ["Whangarei", -35.7275, 174.3166, 55e3],
    LVN = ["Levin", -40.6218, 175.2866, 19e3],
    TMZ = ["Tauranga", -37.6870, 176.1654, 130e3],
    TIU = ['Timaru', -44.3904, 171.2373, 28e3],
    OAM = ["Oamaru", -45.0966, 170.9714, 14e3],
    PUK = ["Pukekohe", -37.2004, 174.9010, 27e3],
    HLZ = ["Hamilton", -37.7826, 175.2528, 165e3],
    LYJ = ["Lower Hutt", -41.2127, 174.8997, 112e3]
)


radii = [10e3,20e3,30e3,40e3,50e3,100e3,250e4] #AK could be larger ??

if __name__ == '__main__':

    mSLR = model.SolutionLocationRadiusRuptureSet
    mSR = model.SolutionRupture
    mSFS = model.SolutionFaultSection
    model.set_local_mode()

    solution_id = "ZZZ"
    sol = solvis.InversionSolution().from_archive(PurePath(WORK_PATH,  name))
    sol = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 1e-20)) #TODO: the solvis function above 0 isn't working
    # clean_slate()
    # save_solution_location_radii(solution_id, get_location_radius_rupture_models(solution_id, sol, locations=locs, radii=radii))
    # save_solution_ruptures(solution_id, get_ruptures_with_rates(solution_id, sol))
    # save_solution_fault_sections(solution_id, get_fault_section_models(solution_id, sol))


    # query()
    #run this to simulate a bunch of user queries...
    #for locs in [['WHK'], ['PMR'], ['WHK', 'PMR']]:
    radius=30000
    for locs in [['WLG'], ['WLG', 'NSN'], ['WLG', 'PMR', "NSN"]]:
        rupture_sections_gdf = matched_rupture_sections_gdf(solution_id, locs, radius)
        if not rupture_sections_gdf is None:
            solvis.export_geojson(rupture_sections_gdf, f"{'_'.join(locs)}_{radius}_query_result.geojson")
    print('DONE')
