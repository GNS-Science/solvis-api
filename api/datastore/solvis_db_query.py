import logging
import geopandas as gpd
import pandas as pd
from typing import List, Iterator, Set

#import solvis

from api.datastore import model

mSLR = model.SolutionLocationRadiusRuptureSet
mSR = model.SolutionRupture
mSFS = model.SolutionFaultSection

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

def matched_rupture_sections_gdf(solution_id:str, locations:List[str], radius:int, minimum_rate:int, union:bool=False) -> gpd.GeoDataFrame:

    print('Intersection/Union')
    ids = get_rupture_ids(solution_id, locations, int(radius), union)
    if not ids:
        return

    ruptures_df = get_ruptures(solution_id)
    ruptures_df = ruptures_df[ruptures_df.rupture_index.isin(list(ids))]

    if minimum_rate > 0:
        print("apply rate filter")
        ruptures_df = ruptures_df[ruptures_df.annual_rate > minimum_rate]

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

