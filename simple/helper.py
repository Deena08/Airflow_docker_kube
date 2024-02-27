import requests
import json
import time
#import boto3
#from botocore.exceptions import ClientError
import os
from io import StringIO
from datetime import datetime, date
from configs.pipeline_config.global_config import *
from airflow.models import Variable
import logging
import re


"""
Get integration based file path from S3 depending on the Environment
Dev setup can have Airflow variables created
Stage, UAT and Prod stores are pulled from S3 files
"""


def get_variables_file(integration_code):
    if os.environ['AIRFLOW_ENV'] == LOCAL_ENVIRONMENT_STORE_CODE:
        air_variables = Variable.get(integration_code, default_var='{}')
        return json.loads(air_variables)

    elif os.environ['AIRFLOW_ENV'] == STAGE_ENVIRONMENT_STORE_CODE or os.environ[
        'AIRFLOW_ENV'] == UAT_ENVIRONMENT_STORE_CODE or os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        s3 = boto3.client('s3')
        s3_file_key = os.environ['AIRFLOW_ENV'] + "/" + integration_code + ".json"
        try:
            result = s3.get_object(Bucket=CONFIGURATION_S3_BUCKET, Key=s3_file_key)
            air_variables = result["Body"].read().decode()
            if air_variables:
                return json.loads(air_variables)
            else:
                return dict()

        except ClientError as ex:
            print(ex)
            return dict()


    else:
        air_variables = Variable.get(integration_code, default_var='{}')
        return json.loads(air_variables)


"""
Write back integration based file path from S3 depending on the Environment
Dev setup can have Airflow variables created
Stage, UAT and Prod stores are pulled from S3 files
"""


def set_variables_file(data, integration_code):
    if os.environ['AIRFLOW_ENV'] == LOCAL_ENVIRONMENT_STORE_CODE:
        Variable.set(integration_code, data, serialize_json=True)

    elif os.environ['AIRFLOW_ENV'] == STAGE_ENVIRONMENT_STORE_CODE or os.environ[
        'AIRFLOW_ENV'] == UAT_ENVIRONMENT_STORE_CODE or os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        s3_vars = boto3.resource('s3')
        s3_file_key = os.environ['AIRFLOW_ENV'] + "/" + integration_code + ".json"
        s3_vars.Object(CONFIGURATION_S3_BUCKET, s3_file_key).put(Body=StringIO(json.dumps(data)).read())

    else:
        Variable.set(integration_code, data, serialize_json=True)


"""
Get S3 bucket name for data based on the environment
Stage, UAT and Prod files are pushed to different buckets
"""


def get_s3_bucket_name():
    if os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        return RAW_DATA_S3_BUCKET

    else:
        return RAW_DATA_S3_BUCKET_STAGE


"""
Get DynamoDB Table for data based on the environment
"""


def get_dynamodb_table():
    if os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        return DYNAMO_DB_TABLE_PROD

    else:
        return DYNAMO_DB_TABLE_STAGE


def batch(iterable, n=1):
    """Return batch wise data for given iterable

    Args:
        iterable (list)
        n (int, optional): Chunk size. Defaults to 1.

    Yields:
        list: chunked list
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def format_organisation_data(organisation_data):
    org_data = dict()
    for org_data_dict in organisation_data:
        org_data["store_code"] = org_data_dict["quadrant"]
    return org_data


def get_kinesis_stream_name():
    if os.environ['AIRFLOW_ENV'] == PROD_ENVIRONMENT_STORE_CODE:
        return KINESIS_STREAM_PROD
    else:
        return KINESIS_STREAM_STAGING
