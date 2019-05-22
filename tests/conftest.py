'''pytest makes all the fixtures in this file available to all other test files without having to import them.'''
import pytest
import os

from moto.s3 import mock_s3
import boto3

from .constants import (
    S3_BUCKET,
    POINT_JSON_SCHEMA, POLYGON_JSON_SCHEMA, 
    POINT_CSV, POLYGON_CSV,
)


FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'fixtures_data'
    )
SCHEMA_DIR = 'schemas'
STAGING_DIR = 'staging'

@pytest.fixture
def s3_client():
    with mock_s3():
        s3 = boto3.client('s3')
        yield s3

@pytest.fixture
def s3_bucket(s3_client):
    s3_client.create_bucket(Bucket=S3_BUCKET)
    return s3_client

@pytest.fixture
def s3_point_schema(s3_client, s3_bucket):
    with open(os.path.join(FIXTURES_DIR, SCHEMA_DIR, POINT_JSON_SCHEMA)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POINT_JSON_SCHEMA, Body=f.read())
    return s3_bucket

@pytest.fixture
def s3_polygon_schema(s3_client, s3_bucket):
    with open(os.path.join(FIXTURES_DIR, SCHEMA_DIR, POLYGON_JSON_SCHEMA)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POLYGON_JSON_SCHEMA, Body=f.read())
    return s3_bucket

@pytest.fixture
def s3_point_csv(s3_client, s3_bucket):
    with open(os.path.join(FIXTURES_DIR, STAGING_DIR, POINT_CSV)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POINT_CSV, Body=f.read())
    return s3_bucket

@pytest.fixture
def s3_polygon_csv(s3_client, s3_bucket):
    with open(os.path.join(FIXTURES_DIR, STAGING_DIR, POLYGON_CSV)) as f:
        s3_bucket.put_object(Bucket=S3_BUCKET, Key=POLYGON_CSV, Body=f.read())
    return s3_bucket