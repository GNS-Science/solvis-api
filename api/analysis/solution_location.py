import geopandas as gpd
import pandas as pd


from pathlib import PurePath
import solvis
from api.datastore import model

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


def get_rupture_ids(solution_id, locations, radius, union=True):
    ids = None
    for loc in locations:
        for item in mSLR.query(f'{solution_id}',
            mSLR.location_radius == f"{loc}:{radius}"):
            if not item.ruptures:
                continue
            if union:
                ids = ids.union(item.ruptures) if ids else item.ruptures
            else:
                ids = ids.intersection(item.ruptures) if ids else item.ruptures
    return ids

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

    return gpd.GeoDataFrame(values, index=index)


if __name__ == '__main__':

    solution_id = "ZZZ"
    sol = solvis.InversionSolution().from_archive(PurePath(WORK_PATH,  name))
    sol = solvis.new_sol(sol, solvis.rupt_ids_above_rate(sol, 0))

    mSLR = model.SolutionLocationRadiusRuptureSet
    mSR = model.SolutionRupture
    mSFS = model.SolutionFaultSection

    model.set_local_mode()

    clean_slate()
    write1()
    write2()
    write3()

    query()
    sections_gdf = get_fault_sections(solution_id)
    print(sections_gdf)
    ruptures_df = get_ruptures(solution_id)
    print(ruptures_df)
    # ids = get_rupture_ids(solution_id, ['WLG', 'CHC'], radius=10000, union=True)
    # print(ids)

    print('Intersection')
    ids = get_rupture_ids(solution_id, ['WLG', 'CHC'], radius=10000, union=False)
    print(ids)

    print('DONE')

# #now export for some science analysis
# df = pd.DataFrame.from_dict(rupture_radius_site_sets, orient='index')
# df = df.rename(columns=dict(zip(radii, [f'r{int(r/1000)}km' for r in radii])))