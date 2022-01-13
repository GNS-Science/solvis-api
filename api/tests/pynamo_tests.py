from api.api import create_app
import unittest
import datetime
from moto import mock_dynamodb2
from api.datastore.model import SolutionLocationsRadiiDF, set_local_mode
from api.namespaces.solution_analysis_geojson import get_solution_dataframe_result

mock_dataframe_binary = b"\xfd7zXZ\x00\x00\x04\xe6\xd6\xb4F\x02\x00!\x01\x16\x00\x00\x00t/\xe5\xa3\xe0\x02\xa0\x01\xb8]\x00@\x01Nw95\x99d1\x92\x8d\x8dz\xad\xb8\x08iZ\xeb(\x99\xb0\xa4L\xe9z\xb2\x11\xa0n>\xc6@uc\x03\xc3A\x0f*\x08\xc2\x9e@M3h\xdd\t\xb5\x97sg\x18\xce}k\xaf\x03]5\xb0\x90%\xebGQ\xef]+;\xce\xa3\x00k\x9b\xabFn\x0f\x8f\xe6?[u\x08\xb26`D\xaaN\x91N\xa7\xd7\x94\x80\xddU\xad\xd54M\x91'\xe0\x99\xd1w\xb8nC\xbcg\xdfq@\xe3\x1b\xe2\x7f6\xccql\tD\x82 \xcbZ\xb2\x82\xea\r\xadC\x9cI\x134\xbc\x9c\x94\x93\xf2/\x93\\\xfa\xac\xa0\xc7\x8f\xe6\x14Y=qy\x021o\xfd\xda\xd9F\xd8t\x90\xae\xb9\x13\xbe\xe7\xc7\xe5\xd29\x14\x08\xc9l\xa5\xe445\xf0_\xb4\x10\xf4\x1a\x03w\xb0P\xa5\xc8P\xea/\xd2s\x9e\ra\xa6\xe5ji\x9b\xfc\x97\x84\xfb\x96N\xfd\xdf\xb4n\xbe\x1e$S\xf2\xf8\xf8\x19/\x06\x8dZ\xf5\xbbTb\xfei=\x88?\x80\x9b7i\xe8'\xc1\xd9gR-}\x93VFv\x1f\x054b\x0b\x02\x7f\x92\xbd\xa8\xb0\xba-9@\xf1\xdcy\xebp\x08\x05\x12\xa9\x1e\xf4\x0b<\xd8\x02\xe4\xce\xd7c\n\x17\xe7\xf4\xa5\xb6\xea~\x83\x84L\xc2\x93\x0ej\x0f-q#\x94\xd9\xa3\r\xa48^-g\xfc\xebPv`\xb6\xeb\x0f\x1e-Q,t`\x13\x9e\n\x91\x8cM t\x10\xf4\xc6\x00\xf3.\x04\xfboa\x03\x14\xe3\x97\x1d[\xe4\xef\xa3\x91`\xb4\xf8OH\x145\n\xc8\xfb\xa4\xe0\xea\x05\xec\xb5e\xe4\x9f\x9d\xfb\xfd}\xa9L\xa92\x15\x0e\x13\xc5\xf8\x93\x8e\xd9W\xe5\xfe\xcc\x80\xa2\xfb\xe7-\xc5\xd0\xbf\x93\xd2b\x1aca\xd9\xdb\xcb@7\xb2\xc0\xe6\xd2.\xd8\x99\x87q\xc4'\x88\x14\xb0\xb8\x88\xc9N\x00\xec9,v]\xe8\xdc\xc0\x00\x01\xd4\x03\xa1\x05\x00\x00\xbfLF\xfe\xb1\xc4g\xfb\x02\x00\x00\x00\x00\x04YZ"
mock_dataframe = '{\n  "0":{\n    "key":"value"\n  }\n}'
table_description = {'AttributeDefinitions': [{'AttributeName': 'id', 'AttributeType': 'S'}], 'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 20, 'NumberOfDecreasesToday': 0}, 'TableSizeBytes': 0, 'TableName': 'solution_locations_radii_dataframes', 'TableStatus': 'ACTIVE', 'TableArn': 'arn:aws:dynamodb:us-east-1:123456789011:table/solution_locations_radii_dataframes', 'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}], 'ItemCount': 0, 'GlobalSecondaryIndexes': [], 'LocalSecondaryIndexes': []}
app = create_app()

@mock_dynamodb2
class DataframeTest(unittest.TestCase):
    
    def setUp(self):
        with app.app_context():
            SolutionLocationsRadiiDF.create_table(wait=True)
            set_local_mode()
            print("Migrate created table: SolutionLocationsRadiiDF")
            super(DataframeTest, self).setUp()
        
    def test_add_dataframe_and_get_by_id(self):
        with app.app_context():
            dataframe = SolutionLocationsRadiiDF(
                id = 'test_dataframe_id',
                solution_id = 'test_solution_id',
                locations_list_id = 'test_location_list_id',
                radii_list_id =  'test_radii_list_id',
                dataframe = mock_dataframe_binary)
            dataframe.save()
            
            dataframe_response = get_solution_dataframe_result('test_dataframe_id')
            self.assertEqual(dataframe.id, dataframe_response.id)
            self.assertEqual(dataframe.solution_id, dataframe_response.solution_id)
            self.assertEqual(dataframe.locations_list_id, dataframe_response.locations_list_id)
            self.assertEqual(dataframe.radii_list_id, dataframe_response.radii_list_id)
            self.assertEqual(mock_dataframe, dataframe_response.dataframe)
        
    def test_table_exists(self):
        with app.app_context():
            self.assertEqual(SolutionLocationsRadiiDF.exists(), True)
    
    def test_table_has_dataframe(self):
        with app.app_context():
            test_table_description = SolutionLocationsRadiiDF.describe_table()
            del(test_table_description['CreationDateTime'])
            self.assertEqual(test_table_description, table_description)
    
    def tearDown(self):
        with app.app_context():
            SolutionLocationsRadiiDF.delete_table()
            return super(DataframeTest, self).tearDown()

if __name__ == '__main__':
    unittest.main()