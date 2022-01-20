import geopandas as gpd
import pandas as pd


from pathlib import PurePath
import solvis
from api.datastore import model

name = "NZSHM22_InversionSolution-QXV0b21hdGlvblRhc2s6NTkzMHJ0YWJU.zip" #60hrs!

#60hr
WORK_PATH = "/home/chrisbc/DEV/GNS/opensha-modular/solvis"

def get_location_radius_rupture_models(solution_id, sol, locations, radii):

    for loc, item in locations.items():
        for radius in radii:
            polygon = solvis.circle_polygon(radius_m=radius, lat=item[1], lon=item[2])
            rupts = set(sol.get_ruptures_intersecting(polygon).tolist())

            print(loc, radius, len(rupts))

            yield model.SolutionLocationRadiusRuptureSet(
                location_radius = f"{loc}:{int(radius)}",
                solution_id = solution_id,
                radius = int(radius),
                location = loc,
                ruptures = rupts,
                rupture_count = len(rupts))


def clean_slate():
    model.drop_all()
    model.migrate()

def write1():
    with model.SolutionLocationRadiusRuptureSet.batch_write() as batch:
        for item in get_location_radius_rupture_models(solution_id, sol, locs, radii):
            #print(item)
            #item.save()
            batch.save(item)

def get_ruptures_with_rates(solution_id, sol):
    rs = sol.rupture_sections
    for row in sol.ruptures_with_rates.itertuples():
        sections = [int(x) for x in rs[rs.rupture==int(row[1])].section.tolist()]
        yield model.SolutionRupture(
            solution_id = solution_id,
            rupture_index = int(row[1]),
            magnitude = float(row[2]),     # Magnitude,
            avg_rake = float(row[3]),     # Average Rake (degrees),
            area_m2 = float(row[4]),       # Area (m^2),
            length_m = float(row[5]),      # Length (m),
            annual_rate = float(row[6]),   # Annual Rate
            fault_sections = sorted(sections)
        )

def write2():
    with model.SolutionRupture.batch_write() as batch:
        for item in get_ruptures_with_rates(solution_id, sol):
            batch.save(item)

def get_fault_section_models(solution_id, sol):
    for row in sol.fault_sections.itertuples():
        yield model.SolutionFaultSection(
            solution_id = solution_id,
            section_index_rk = str(row[1]),
            section_index = row[1],
            fault_name = row[2],
            dip_degree = float(row[3]),
            rake = float(row[4]),
            low_depth = float(row[5]),
            up_depth = float(row[6]),
            dip_dir = float(row[7]),
            aseismic_slip_factor = float(row[8]),
            coupling_coeff = float(row[9]),
            slip_rate = float(row[10]),
            parent_id = int(row[11]),
            parent_name = row[12],
            slip_rate_std_dev = float(row[13]),
            geometry = str(row[14])
        )


def write3():
    with model.SolutionFaultSection.batch_write() as batch:
        for item in get_fault_section_models(solution_id, sol):
            batch.save(item)

def query():

    # for item in mSLR.query(f'{solution_id}',
    #     mSLR.location_radius.startswith("WLG"),
    #     #filter_condition=mSLR.rupture_count > 200,
    #     limit=20):
    #     print("Query returned item {0}".format(item), item.rupture_count)
    # print()

    # for rupt in mSR.query(f'{solution_id}',
    #     #mRR.rupture_index == 238707,
    #     filter_condition=mSR.magnitude > 8,
    #     limit=20):
    #     print("Query returned rupt {0}".format(rupt), rupt.magnitude, len(rupt.fault_sections))
    # print()

    for item in mSFS.query(f'{solution_id}',
        #mSFS.section_index == 0, #list(rupt.fault_sections)
        #filter_condition=mSFS.section_index.is_in(*list(rupt.fault_sections)),
        #mSFS.section_index.is_in(*list(rupt.fault_sections)),
        limit=20):
        print("Query returned item {0}".format(item), item.rake, item.geometry)
    print()

# QUERY operations for the API get endpoint(s)

def get_rupture_ids(solution_id, locations, radius, union=True):
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

def get_ruptures(solution_id):
    index = []
    values = []
    for item in mSR.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.rupture_index)
    return pd.DataFrame(values, index=index)

def get_fault_sections(solution_id):
    index = []
    values = []
    for item in mSFS.query(f'{solution_id}'):
        values.append(item.attribute_values)
        index.append(item.section_index)

    df = pd.DataFrame(values, index)
    return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df.geometry))


def build_result(solution_id, locations=['WLG', 'GIS'], radius=20000, union=False):

    # aseismic_slip_factor  coupling_coeff  dip_degree  dip_dir fault_name geometry  ...  section_index  section_index_rk slip_rate  slip_rate_std_dev  solution_id up_dept
    sections_gdf = get_fault_sections(solution_id)
    print("Sections gdf")
    print(sections_gdf)
    """
    Sections gdf
      aseismic_slip_factor  coupling_coeff  dip_degree  dip_dir                   fault_name                                           geometry  ...  section_index  section_index_rk slip_rate  slip_rate_std_dev  solution_id up_dept
    h
    0                        0               1          60     94.3          Acton, Subsection 0  LINESTRING (168.3711 -45.4444, 168.3553 -45.47...  ...              0                 0      0.20               0.15          ZZZ
    0
    999                      0               1          70    113.6       Mascarin, Subsection 1  LINESTRING (175.050074630765 -40.3113896024160...  ...            999               999      0.10               1.62          ZZZ
    0

    [2088 rows x 16 columns]
    """

    # annual_rate       area_m2   avg_rake fault_sections length_m  magnitude  rupture_index solution_id
    ruptures_df = get_ruptures(solution_id)
    print("Ruptures df")
    print(ruptures_df.shape)
    """
    Ruptures df
             annual_rate       area_m2   avg_rake                                     fault_sections       length_m  magnitude  rupture_index solution_id
    881     5.547360e-07  4.544913e+09 -70.000000  {0, 1, 2, 367, 368, 369, 1139, 1140, 1332, 133...  148980.243321   7.857521            881         ZZZ
    447283  5.192884e-05  6.470956e+08 -70.000000                                 {2083, 2084, 2085}   27742.634758   7.010965         447283         ZZZ
    """

    print('Intersection')
    ids = get_rupture_ids(solution_id, locations, radius, union)
    print(ids)
    if not ids:
        return

    ruptures_df = ruptures_df[ruptures_df.rupture_index.isin(list(ids))]
    # if ruptures_df.shape[0] == 0:
    #     return
    #ruptures_df = ruptures_df[ruptures_df.rupture_index.isin([426156])]
    print("Ruptures df")
    #print(ruptures_df.shape)
    #print(ruptures_df.fault_sections.values)
    #assert 0

    table = []
    #for row in levins_rupts.itertuples():
    for row in ruptures_df.itertuples():
        #Pandas(Index=426115, r10km='WLG', r20km='WLG', _3=426115, Magnitude=7.7557805759, _5=-17.7493085896, _6=3595708870.845804, _7=147379.5207102174, _8=1.395e-07)
        rupture_id=row[0]
        fault_sections=row[4]
        for section_id in fault_sections:
            table.append(dict(rupture_index=rupture_id, section_index=section_id))

    rupture_sections_df = pd.DataFrame.from_dict(table)
    rupture_sections_df

    # print(ruptures_df.fault_sections.to_list())
    """
           rupture_id  section_id
    0             881           0
    25831      447283        2085
    """

    print("assemble geojson")
    rupture_sections_df = rupture_sections_df\
        .join(ruptures_df, 'rupture_index', how='inner', rsuffix='_R')\
        .drop(columns=['fault_sections', 'rupture_index_R'])

    # print(rupture_sections_df)
    # print(rupture_sections_df.shape)

    """
    print(f'columns: {rupture_sections_df.columns}')
    print( rupture_sections_df[['rupture_index', 'section_index', 'annual_rate', 'magnitude']] )
    print()
    print("Sections gdf")
    expected_sections = [5, 6, 1305, 1306, 1307, 1308, 1309, 1973, 1974, 1975, 1976, 1977, 1466, 1467, 1978, 1979, 1982, 1983, 1984, 1985, 1528, 1529, 1530, 1531, 1532]
    print(sections_gdf[sections_gdf['section_index'].isin(list(expected_sections))][["section_index", "geometry"]])
    """

    #join with section cols
    #rupture_sections_gdf = gpd.GeoDataFrame(rupture_sections_df[rupture_sections_df.rupture_index == 426156])\
    rupture_sections_gdf = gpd.GeoDataFrame(rupture_sections_df)\
        .join (sections_gdf, 'section_index', how='inner', rsuffix='_R')\
        .drop(columns=['section_index_R'])

    # sections_gdf\
    #         .join (levins_sections_df[levins_sections_df.rupture_index == 425773], 'section_index', how='inner', rsuffix='_R')\
    #         .drop(columns=['section_index_R', 'solution_id_R'])

    # levins_sections_df[levins_sections_df.rupture_index == 425773].join(sections_gdf, 'section_index', how='inner', rsuffix='_R')
    # print(rupture_sections_gdf.geometry)
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


radii = [10e3,20e3,30e3,40e3,50e3,100e3] #AK could be larger ??


if __name__ == '__main__':

    solution_id = "ZZZ"
    # sol = solvis.InversionSolution().from_archive(PurePath(WORK_PATH,  name))
    # sol = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 0))

    # print(sol.rs_with_rates)

    # print("Section Participtation")
    # print(solvis.section_participation(sol))
    # print()

    mSLR = model.SolutionLocationRadiusRuptureSet
    mSR = model.SolutionRupture
    mSFS = model.SolutionFaultSection

    model.set_local_mode()

    # clean_slate()
    # write1()
    # write2()
    # write3()

    # query()


    for locs in [['WHK'], ['PMR'], ['WHK', 'PMR']]:
        rupture_sections_gdf = build_result(solution_id, locs, radius=30000)
        if not rupture_sections_gdf is None:
            solvis.export_geojson(rupture_sections_gdf, f"{'_'.join(locs)}_query_result.geojson")
    print('DONE')

# #now export for some science analysis
# df = pd.DataFrame.from_dict(rupture_radius_site_sets, orient='index')
# df = df.rename(columns=dict(zip(radii, [f'r{int(r/1000)}km' for r in radii])))