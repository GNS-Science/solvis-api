# test_solution_location.py


expect_rs_with_rates = """
       rupture  section  Rupture Index  Magnitude  Average Rake (degrees)         Area (m^2)     Length (m)  Annual Rate
0          881        2            881   7.857521                   -70.0  4544912535.875902  148980.243321     0.000001
1          881        1            881   7.857521                   -70.0  4544912535.875902  148980.243321     0.000001
2          881        0            881   7.857521                   -70.0  4544912535.875902  148980.243321     0.000001
3          881     1149            881   7.857521                   -70.0  4544912535.875902  148980.243321     0.000001
4          881     1150            881   7.857521                   -70.0  4544912535.875902  148980.243321     0.000001
...        ...      ...            ...        ...                     ...                ...            ...          ...
25827   447154      741         447154   7.669141               71.880399  2945401320.547691  155258.640651     0.000033
25828   447154      740         447154   7.669141               71.880399  2945401320.547691  155258.640651     0.000033
25829   447283     2083         447283   7.010965                   -70.0   647095592.879773   27742.634758     0.000052
25830   447283     2084         447283   7.010965                   -70.0   647095592.879773   27742.634758     0.000052
25831   447283     2085         447283   7.010965                   -70.0   647095592.879773   27742.634758     0.000052
"""


# DEBUG:pynamodb.connection.base:Calling DescribeTable with arguments {'TableName': 'SolutionRupture'}
# DEBUG:pynamodb.connection.base:Calling Query with arguments {'TableName': 'SolutionRupture', 'KeyConditionExpression': '#0 = :0', 'ExpressionAttributeNames': {'#0': 'solution_id'}, 'ExpressionAttributeValues': {':0': {'S': 'ZZZ'}},
#  'ReturnConsumedCapacity': 'TOTAL'}
# DEBUG:pynamodb.connection.base: Query consumed 27.5 units


"""
         annual_rate       area_m2   avg_rake                                     fault_sections       length_m  magnitude  rupture_index solution_id
881     5.547360e-07  4.544913e+09 -70.000000  {0, 1, 2, 367, 368, 369, 1139, 1140, 1332, 133...  148980.243321   7.857521            881         ZZZ
933     2.354722e-05  4.434195e+09 -70.000000  {0, 1, 2, 1136, 1137, 1138, 1139, 1140, 1141, ...  143088.248056   7.846811            933         ZZZ
934     4.411243e-06  4.965519e+09 -70.000000  {0, 1, 2, 1091, 1092, 1136, 1137, 1138, 1139, ...  162340.966680   7.895961            934         ZZZ
2235    8.658790e-06  7.386711e+08 -49.480331                                 {2085, 2084, 5, 6}   33044.300693   7.068448           2235         ZZZ
2254    8.841627e-05  2.020679e+09 -70.000000                                {7, 8, 9, 536, 537}   57854.386398   7.505494           2254         ZZZ
...              ...           ...        ...                                                ...            ...        ...            ...         ...
442063  5.123474e-08  6.235171e+09 -38.696378  {2039, 61, 62, 63, 64, 89, 90, 91, 92, 93, 94,...  256415.708842   7.994844         442063         ZZZ
442189  5.641117e-11  5.942879e+09 -36.229453  {2039, 61, 62, 63, 64, 89, 90, 91, 92, 93, 94,...  244804.147980   7.973993         442189         ZZZ
446827  6.876582e-05  1.139196e+09 -70.000000                                 {2075, 2076, 2077}   34019.754824   7.256595         446827         ZZZ
447154  3.303941e-05  2.945401e+09  71.880399  {2055, 2056, 2057, 2078, 2079, 2080, 2081, 208...  155258.640651   7.669141         447154         ZZZ
447283  5.192884e-05  6.470956e+08 -70.000000                                 {2083, 2084, 2085}   27742.634758   7.010965         447283         ZZZ
"""


# >>> import solvis_api.analysis.solution_location as sl
# >>> sol = sl.solvis.InversionSolution().from_archive(PurePath(WORK_PATH,  name))
# >>> for row in sol.fault_sections.itertuples():
# ...  if row[1] == 5:
# ...   print(row[1], row[2], row[-1].wkt)
# ...
# 5 Akatarawa, Subsection 0 LINESTRING (175.0372 -41.1225, 175.0698 -41.1007, 175.0891467545942 -41.07156421641177)
# >>>

# sl.mSLR = sl.model.SolutionLocationRadiusRuptureSet
# sl.mSR = sl.model.SolutionRupture
# sl.mSFS = sl.model.SolutionFaultSection
# sl.model.set_local_mode()
