"""
This module exports comfiguration forthe current system
and is imported  by the various run_xxx.py scripts
"""

import os
import enum
from pathlib import PurePath

# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:   
# https://aws.amazon.com/developers/getting-started/python/

import boto3
import base64
import enum
import urllib.parse
from botocore.exceptions import ClientError
import json

import solvis

def get_secret(secret_name, region_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            return base64.b64decode(get_secret_value_response['SecretBinary'])


class EnvMode(enum.IntEnum):
    AWS = 0
    LOCAL = 1

def boolean_env(environ_name, default='False'):
    return bool(os.getenv(environ_name, default).upper() in ["1", "Y", "YES", "TRUE"])

#API Setting are needed to sore job details for later reference
USE_API = boolean_env('NZSHM22_TOSHI_API_ENABLED')
API_URL  = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
S3_URL = os.getenv('NZSHM22_TOSHI_S3_URL',"http://localhost:4569")

# #Get API key from AWS secrets manager
# if USE_API and 'TEST' in API_URL.upper():
#     API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_TEST", "ap-southeast-2").get("NZSHM22_TOSHI_API_SECRET_TEST")
# elif USE_API and 'PROD' in API_URL.upper():
#     API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_PROD", "us-east-1").get("NZSHM22_TOSHI_API_KEY_PROD")
# else:
API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")

WORK_PATH = os.getenv('NZSHM22_SCRIPT_WORK_PATH', "/tmp")
SNS_IS_TOPIC  = os.getenv('SNS_IS_TOPIC', 'IS_TOPIC_unconfigured')
SNS_GT_TOPIC  = os.getenv('SNS_GT_TOPIC', 'GT_TOPIC_unconfigured')

IS_OFFLINE = boolean_env('SLS_OFFLINE') #set by serverless-wsgi plugin
IS_TESTING = boolean_env('TESTING', 'False')

if IS_OFFLINE:
   SOLVIS_API_URL = 'http://localhost:5000'
else:
   SOLVIS_API_URL = os.getenv('NZSHM22_SOLVIS_API_URL', 'https://ly86h01a86.execute-api.ap-southeast-2.amazonaws.com/dev/')
   
SOLVIS_API_KEY = os.getenv('NZSHM22_SOLVIS_API_KEY', '')

REGION = os.getenv('REGION', 'us-east-1')
DEPLOYMENT_STAGE = os.getenv('DEPLOYMENT_STAGE', 'LOCAL').upper()
LOGGING_CFG = os.getenv('LOGGING_CFG', 'api/logging.yaml')

CLOUDWATCH_APP_NAME = os.getenv('CLOUDWATCH_APP_NAME', 'CLOUDWATCH_APP_NAME_unconfigured')