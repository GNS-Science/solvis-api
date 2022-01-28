import logging
import geopandas as gpd
import pandas as pd
from datetime import datetime as dt
from typing import List, Iterator, Set

from functools import lru_cache
from api.datastore import model

log = logging.getLogger(__name__)

mSLR = model.SolutionLocationRadiusRuptureSet
mSR = model.SolutionRupture
mSFS = model.SolutionFaultSection

# QUERY operations for the API get endpoint(s)
def get_rupture_ids(solution_id:str, locations:List[str], radius:int, union:bool =True) -> Set[int]:
    fullset = set(range(10000))
    ids = set() if union else fullset

    print(f'get_rupture_ids({locations}, {radius}, union: {union})')

    @lru_cache(maxsize=64)
    def query_fn(solution_id, loc, radius):
        return [i for i in mSLR.query(f'{solution_id}', mSLR.location_radius == (f"{loc}:{radius}"))]

    for loc in locations:
        items = query_fn(solution_id, loc, radius)
        assert len(items) in [0,1]
        loc_rupts = set()
        for item in items:
            if not item.ruptures:
                continue
            if item.radius > radius:
                continue

            print(f'SLR query item: {item} {item.location_radius}, ruptures: {len(item.ruptures)})')

            if union:
                ids = ids.union(item.ruptures)
            else:
                ids = ids.intersection(item.ruptures)

    #if no change to fullset (for intersection) return an empty set
    return set() if ids is fullset else ids

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
def matched_rupture_sections_gdf(solution_id:str, locations:str, radius:int,
    min_rate:float, max_rate: float, min_mag:float, max_mag: float, union:bool=False) -> gpd.GeoDataFrame:

    t0 = dt.utcnow()
    locations = locations.split(',')
    print(locations)

    print('Intersection/Union')
    ids = get_rupture_ids(solution_id, locations, int(radius), union)
    if not ids:
        return

    t1 = dt.utcnow()
    print(f'get_rupture_ids() (not cached), took {t1-t0}')

    try:
        ruptures_df = get_ruptures(solution_id)
        ruptures_df = ruptures_df[ruptures_df.rupture_index.isin(list(ids))]
    except Exception as err:
        log.error(err)
        raise

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

    sections_gdf = get_fault_sections(solution_id)

    t5 = dt.utcnow()
    print(f'apply get_fault_sections (maybe cached), took {t5-t4}')

    print("Assemble geojson")
    #join rupture details
    #print(rupture_sections_df)
    #print(ruptures_df)
    rupture_sections_df = rupture_sections_df\
        .join(ruptures_df, 'rupture_index', how='inner', rsuffix='_R')\
        .drop(columns=['fault_sections', 'rupture_index_R'])

    #join fault_section details as GeoDataFrame
    rupture_sections_gdf = gpd.GeoDataFrame(rupture_sections_df)\
        .join (sections_gdf, 'section_index', how='inner', rsuffix='_R')\
        .drop(columns=['section_index_R'])


    t6 = dt.utcnow()
    print(f'Assemble geojson (not cached), took {t6-t5}')

    rupture_sections_gdf = rupture_sections_gdf.drop(columns = ['area_m2', 'length_m',
        'parent_id', 'parent_name', 'section_index_rk', 'solution_id', 'solution_id_R'] )

    # # Here we want to collapse all ruptures so we have just one feature for section. Each section can have the
    # count of ruptures, min, mean, max magnitudes & annual rates

    section_aggregates_gdf = rupture_sections_gdf.pivot_table(
        index= ['section_index'],
        aggfunc=dict(annual_rate=['sum', 'min', 'max'],
             magnitude=['count', 'min', 'max']))

    #join the rupture_sections_gdf details
    section_aggregates_gdf.columns = [".".join(a) for a in section_aggregates_gdf.columns.to_flat_index()]
    section_aggregates_gdf = section_aggregates_gdf\
        .join (sections_gdf, 'section_index', how='inner', rsuffix='_R')

    return section_aggregates_gdf

