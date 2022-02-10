from datetime import datetime as dt
from hashlib import md5
from pathlib import Path

#import base64
#import json
import os
import requests

from nshm_toshi_client.toshi_client_base import ToshiClientBase

class ToshiApi(ToshiClientBase):

    def __init__(self, url, s3_url, auth_token, with_schema_validation=True, headers=None ):
        super(ToshiApi, self).__init__(url, auth_token, with_schema_validation, headers)
        self._s3_url = s3_url

        self.inversion_solution = InversionSolution(self)
        self.general_task = GeneralTask(self)

class GeneralTask(object):
    def __init__(self, api):
        self.api = api
        assert isinstance(api, ToshiClientBase)

    def get_general_task_subtasks(self, id):
        qry = '''
            query one_general ($id:ID!)  {
              node(id: $id) {
                __typename
                ... on GeneralTask {
                  id
                  title
                  description
                  created
                  swept_arguments
                  children {
                    #total_count
                    edges {
                      node {
                        child {
                          __typename
                          ... on AutomationTask {
                            inversion_solution {
                              id
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }'''

        # print(qry)
        input_variables = dict(id=id)
        executed = self.api.run_query(qry, input_variables)
        #print(executed)
        return executed['node']

class InversionSolution(object):

    def __init__(self, api):
        self.api = api
        assert isinstance(api, ToshiClientBase)

    def download_inversion_solution(self, solution_id, destination):
        folder = Path(destination)
        folder.mkdir(parents=True, exist_ok=True)

        info = self.get_file_download_url(solution_id)
        file_path = Path(folder, info['file_name'])

        if os.path.isfile(file_path) and os.path.getsize(file_path) == info['file_size']:
            print(f"Skip DL for existing file: {file_path}")
            return file_path

        # here we pull the file
        # print(info['file_url'])
        # r0 = requests.head(info['file_url'])
        r1 = requests.get(info['file_url'])
        with open(str(file_path), 'wb') as f:
            f.write(r1.content)
            print("downloaded input file:", file_path, f)
            assert os.path.getsize(file_path) == info['file_size']
        return file_path

    def get_file_download_url(self, id):
        qry = '''
        query download_file ($id:ID!) {
                node(id: $id) {
            __typename
            ... on Node {
              id
            }
            ... on FileInterface {
              file_name
              file_size
              file_url
            }
          }
        }'''

        print(qry)
        input_variables = dict(id=id)
        executed = self.api.run_query(qry, input_variables)
        return executed['node']
