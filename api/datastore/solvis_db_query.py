import logging
import geopandas as gpd
import pandas as pd
from datetime import datetime as dt
from typing import List, Iterator, Set

#import solvis
from functools import lru_cache
from api.datastore import model

mSLR = model.SolutionLocationRadiusRuptureSet
mSR = model.SolutionRupture
mSFS = model.SolutionFaultSection

# QUERY operations for the API get endpoint(s)
def get_rupture_ids(solution_id:str, locations:List[str], radius:int, union:bool =True) -> Set[int]:
    ids = None

    @lru_cache(maxsize=64)
    def query_fn(solution_id, loc, radius):
        return [i for i in mSLR.query(f'{solution_id}', mSLR.location_radius == f"{loc}:{radius}")]

    for loc in locations:
        items = query_fn(solution_id, loc, radius)
        assert len(items) in [0,1]
        if len(items):
            item = items[0]
            #print(f'SLR query item: {item}, ruptures: {item.ruptures}')
            if not item.ruptures:
                continue
            if union:
                ids = ids.union(item.ruptures) if ids else set(item.ruptures)
            else:
                ids = ids.intersection(item.ruptures) if ids else set(item.ruptures)
    return ids or set()

@lru_cache(maxsize=32)
def get_ruptures(solution_id:str) -> gpd.GeoDataFrame:
    index = []
    values = []
    for item in mSR.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.rupture_index)
    return pd.DataFrame(values, index=index)

@lru_cache(maxsize=32)
def get_fault_sections(solution_id:str) -> gpd.GeoDataFrame:
    index = []
    values = []
    for item in mSFS.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.section_index)

    df = pd.DataFrame(values, index)
    return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df.geometry))

@lru_cache(maxsize=32)
def matched_rupture_sections_gdf(solution_id:str, locations:str, radius:int, min_rate:float, max_rate: float, min_mag:float, max_mag: float, union:bool=False) -> gpd.GeoDataFrame:

    t0 = dt.utcnow()
    locations = locations.split(',')
    print(locations)

    print('Intersection/Union')
    ids = get_rupture_ids(solution_id, locations, int(radius), union)
    if not ids:
        return

    t1 = dt.utcnow()
    print(f'get_rupture_ids() (not cached), took {t1-t0}')

    ruptures_df = get_ruptures(solution_id)
    ruptures_df = ruptures_df[ruptures_df.rupture_index.isin(list(ids))]

    t2 = dt.utcnow()
    print(f'get_ruptures() (maybe cached), took {t2-t1}')

    if min_rate:
        print(f"apply min rate filter: min={min_rate}")
        ruptures_df = ruptures_df[ruptures_df.annual_rate > min_rate]

    if max_rate:
        print(f"apply max rate filter: max={max_rate}")
        ruptures_df = ruptures_df[ruptures_df.annual_rate < max_rate]

    if min_mag:
        print(f"apply min magnitude filter: min={min_mag}")
        ruptures_df = ruptures_df[ruptures_df.magnitude > min_mag]

    if max_mag:
        print(f"apply max magnitude filter: max={max_mag}")
        ruptures_df = ruptures_df[ruptures_df.magnitude < max_mag]

    t3 = dt.utcnow()
    print(f'apply filters  took {t3-t2}')

    if ruptures_df.empty:
        return None

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

    t4 = dt.utcnow()
    print(f'apply build_rupture_sections_df (not cached), took {t4-t3}')

    print("get fault sections GDF")
    sections_gdf = get_fault_sections(solution_id)

    t5 = dt.utcnow()
    print(f'apply get_fault_sections (maybe cached), took {t5-t4}')

    print("Assemble geojson")
    #join rupture details
    print(rupture_sections_df)
    print(ruptures_df)
    rupture_sections_df = rupture_sections_df\
        .join(ruptures_df, 'rupture_index', how='inner', rsuffix='_R')\
        .drop(columns=['fault_sections', 'rupture_index_R'])

    #join fault_section details as GeoDataFrame
    rupture_sections_gdf = gpd.GeoDataFrame(rupture_sections_df)\
        .join (sections_gdf, 'section_index', how='inner', rsuffix='_R')\
        .drop(columns=['section_index_R'])

    print(f'columns: {rupture_sections_gdf.columns}')
    print( rupture_sections_gdf[['rupture_index', 'section_index', 'fault_name', 'magnitude']] )

    t6 = dt.utcnow()
    print(f'Assemble geojson (not cached), took {t6-t5}')


    return rupture_sections_gdf.drop(columns = ['annual_rate', 'area_m2', 'length_m', 'magnitude',
        'parent_id', 'parent_name', 'section_index_rk', 'solution_id', 'solution_id_R'] )

"""
"Feature\", \"properties\": {
\"annual_rate\": 2.055428010806169e-06,
\"area_m2\": 23086300668.45861,
\"aseismic_slip_factor\": 0,
\"avg_rake\": -10.469866097808904,
\"coupling_coeff\": 1,
\"dip_degree\": 80,
\"dip_dir\": 130.8, \"fault_name
\": \"Alpine: Caswell, Subsection 0\",
 \"length_m\": 1019509.660978102,
 \"low_depth\": 22.7,
 \"magnitude\": 8.463349505790028,
 \"parent_id\": 8,
 \"parent_name\": \"Alpine: Caswell\",
 \"rake\": 0,
 \"rupture_index\": 70617,
 \"section_index\": 20,
 \"section_index_rk\": \"20\",
 \"slip_rate\": 31.4,
 \"slip_rate_std_dev\": 2.8,
 \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\",
 \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\",
 \"up_depth\": 0},
 \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"99\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 6.348522450816034e-06, \"area_m2\": 23378341839.866745, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.335223966086346, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 1030656.2705738322, \"low_depth\": 22.7, \"magnitude\": 8.46880885689296, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 70618, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"199\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 6.318460614488189e-09, \"area_m2\": 22485716610.601, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.757953895692369, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 996887.9699411752, \"low_depth\": 22.7, \"magnitude\": 8.45190190393534, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 71242, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"296\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 2.3507729040528737e-08, \"area_m2\": 22777757782.00915, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.615936068793474, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 1008034.5795369064, \"low_depth\": 22.7, \"magnitude\": 8.45750613516974, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 71243, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"394\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 4.131302556254752e-06, \"area_m2\": 23069798953.417297, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.477577637657475, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 1019181.1891326372, \"low_depth\": 22.7, \"magnitude\": 8.463038968452764, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 71244, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"493\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 9.328251332248747e-06, \"area_m2\": 23361840124.82543, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.342740516069568, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 1030327.7987283674, \"low_depth\": 22.7, \"magnitude\": 8.468502200143726, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 71245, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"593\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 2.847518028466272e-10, \"area_m2\": 22736874406.11083, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.635593873393702, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 1006341.6914000884, \"low_depth\": 22.7, \"magnitude\": 8.456725928507659, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 71819, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}}, {\"id\": \"691\", \"type\": \"Feature\", \"properties\": {\"annual_rate\": 2.811564035896247e-06, \"area_m2\": 23028915577.51897, \"aseismic_slip_factor\": 0, \"avg_rake\": -10.496731629556676, \"coupling_coeff\": 1, \"dip_degree\": 80, \"dip_dir\": 130.8, \"fault_name\": \"Alpine: Caswell, Subsection 0\", \"length_m\": 1017488.3009958192, \"low_depth\": 22.7, \"magnitude\": 8.462268647211136, \"parent_id\": 8, \"parent_name\": \"Alpine: Caswell\", \"rake\": 0, \"rupture_index\": 71820, \"section_index\": 20, \"section_index_rk\": \"20\", \"slip_rate\": 31.4, \"slip_rate_std_dev\": 2.8, \"solution_id\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"solution_id_R\": \"SW52ZXJzaW9uU29sdXRpb246MTk4MzcuMGZraHVq\", \"up_depth\": 0}, \"geometry\": {\"type\": \"LineString\", \"coordinates\": [[166.9821, -45.0393], [167.017, -45.0338], [167.0462, -45.0193], [167.0839, -44.9916], [167.0929467881779, -44.98315924182272]]}},

"""



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

