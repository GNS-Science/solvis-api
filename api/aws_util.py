#!aws_util.py

from functools import lru_cache
import json
import logging
import boto3
from api.config import (SNS_TOPIC, IS_OFFLINE, IS_TESTING)

log = logging.getLogger(__name__)

def get_sns_client():
    AWS_REGION = 'ap-southeast-2'
    if IS_OFFLINE and not IS_TESTING:
        log.debug(f"**OFFLINE SETUP** SNS_TOPIC {SNS_TOPIC}")
        return boto3.client('sns', endpoint_url="http://127.0.0.1:4002", region_name=AWS_REGION)
    else:
        return boto3.client('sns', region_name=AWS_REGION)

@lru_cache(maxsize=1)
def get_sns_topic_arn():
    log.debug(f"get_sns_topic_arn for {SNS_TOPIC}")

    conn = get_sns_client()
    response = conn.list_topics()

    topic_arn = None
    for topic in response.get('Topics'):
        if SNS_TOPIC in topic['TopicArn']:
            return topic['TopicArn']

    #need to create the topic
    conn.create_topic(Name=SNS_TOPIC)
    response = conn.list_topics()
    topic_arn = response["Topics"][0]["TopicArn"]
    return topic_arn

def publish_message(message):
    log.debug(f"publish_message {message}")
    client = get_sns_client()
    topic_arn = get_sns_topic_arn()
    log.debug(f'TOPIC ARN {topic_arn}')
    try:
        response = client.publish (
            TargetArn = topic_arn,
            Message = json.dumps({'default': json.dumps(message)}),
            MessageStructure = 'json'
        )
        log.debug(f"SNS reponse {response}")
    except Exception as err:
        log.error(err)
        raise
    log.info(f"publish_message OK")


