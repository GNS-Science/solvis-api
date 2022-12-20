import copy
import json
import logging
import logging.config
import os

import requests
import solvis
import yaml

from solvis_api.aws_util import publish_message
from solvis_api.config import (
    API_KEY,
    API_URL,
    IS_OFFLINE,
    LOGGING_CFG,
    S3_URL,
    SOLVIS_API_KEY,
    SOLVIS_API_URL,
    USE_API,
    WORK_PATH,
)
from solvis_api.datastore import model
from solvis_api.datastore.solvis_db import *
from solvis_api.toshi_solvis_api.toshi_api import ToshiApi

log = logging.getLogger(__name__)

headers = {"x-api-key": API_KEY}
toshi_api = ToshiApi(API_URL, S3_URL, None, with_schema_validation=False, headers=headers)

if os.path.exists(LOGGING_CFG):
    with open(LOGGING_CFG, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=logging.DEBUG)


def process_general_task_request(general_task_id, message):
    gt = toshi_api.general_task.get_general_task_subtasks(general_task_id)

    for node in gt['children']['edges']:

        at = node['node']['child']
        solution = at.get('inversion_solution')

        if solution:
            # create a soluton message
            msg = copy.copy(message)
            msg['solution_id'] = solution.get('id')
            msg.pop('general_task_id')
            publish_message(msg)
        else:
            log.info(f"No solution for Automation Task: {at}")


def process_event(evt):

    print(evt)

    message = json.loads(evt['Sns']['Message'])

    _id = message['id']
    general_task_id = message.get('general_task_id', None)

    if general_task_id:
        process_general_task_request(general_task_id, message)
    else:
        raise ValueError("need a general_task_id")

    log.info("process_general_task_request completed OK")


def handler(event, context):

    for evt in event.get('Records', []):
        process_event(evt)

    return True
