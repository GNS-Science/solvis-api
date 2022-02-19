import unittest
import json
import boto3

from unittest import mock

from moto import mock_dynamodb2, mock_sns, mock_sts, mock_sqs

from api.api import create_app
from api.config import SNS_TOPIC
import logging

from api import process_solution

@mock_sqs
@mock_sns
def test_publish_subject():
    conn = boto3.client("sns", region_name="us-east-1")
    conn.create_topic(Name="TABCD")
    response = conn.list_topics()

    topic_arn = None
    for t in response.get('Topics'):
        if SNS_TOPIC in t['TopicArn']:
            topic_arn = t
            break

    if not topic_arn:
        conn.create_topic(Name=SNS_TOPIC)
        topic_arn = response["Topics"][0]["TopicArn"]
        response = conn.list_topics()
        print(response)

    topic_arn = response["Topics"][0]["TopicArn"]

    print(F"ARN {topic_arn}")
    response = conn.publish(
            TargetArn = topic_arn,
            Message = json.dumps({'default': json.dumps(["message"])}),
            MessageStructure = 'json'
    )
    print(response)
    # assert 0

@mock_dynamodb2
class TestGeneralTaskRequest(unittest.TestCase):

    def setUp(self):
        app = create_app()
        #
        self.client =  app.test_client()
        slog = logging.getLogger('root')
        slog.setLevel(logging.DEBUG)
        app.logger.setLevel(logging.DEBUG)

    @mock_sns
    @mock.patch('api.toshi_api.toshi_api.GeneralTask.get_general_task', lambda _self, id: dict(id='id1'))
    def test_get_vanilla(self):

        blog  = logging.getLogger('botocore')
        blog.setLevel(logging.INFO)

        headers = {'Content-Type': 'application/json'}
        data = dict(
            general_task_id = 'id1',
            locations_list_id = 'NZ',
            radii_list_id = '6'
            )

        response = self.client.post('/solution_analysis/general_task', data=json.dumps(data), headers=headers)
        print(response)
        self.assertEqual(response.status_code, 200)


# @mock_dynamodb2
# class TestHandleGeneralTaskRequest(unittest.TestCase):

#     def test_process_without_gt_or_sol_id_raises(self):
#         with self.assertRaises(ValueError) as ctx:
#             msg = {'id': "ABABBA", 'solution_id': None, 'locations_list_id': "NZ", 'radii_list_id': "6"}
#             sns_event = {"Records": [{'Sns': {'Message': json.dumps(msg)}}]}
#             process_solution.handler(sns_event, {})