'''pytest makes all the fixtures in this file available to all other test files without having to import them.'''
import pytest
import os,sys

from moto.s3 import mock_s3
import boto3

from .constants import (
    S3_BUCKET,
    POINT_JSON_SCHEMA, POLYGON_JSON_SCHEMA, 
    POINT_CSV, POLYGON_CSV,
)

# Makes it so output doesn't get truncated
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999

FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'fixtures_data'
    )
SCHEMA_DIR = 'schemas'
STAGING_DIR = 'staging'

# Command line options to be used in our test python files
# Note: docs say this should only be in the conftest.py file.
def pytest_addoption(parser):
    parser.addoption("--user", action="store", default='GIS_TEST', help="db user name")
    parser.addoption("--host", action="store", default='some-host.gov', help="db host")
    parser.addoption("--password", action="store", default='password', help="db user password")
    parser.addoption("--database", action="store", default='adatabase',  help="db database name")
    parser.addoption("--ago_user", action="store", default='some_user',  help="user for AGO login")
    parser.addoption("--ago_password", action="store", default='some_user',  help="pw for AGO login")

# Necessary for our tests to access the parameters/args as specified
# Fixtures are just functions that return objects that can be used by
# multiple tests
# in conftest.py
@pytest.fixture
def user(pytestconfig):
    return pytestconfig.getoption("user")
@pytest.fixture
def host(pytestconfig):
    return pytestconfig.getoption("host")
@pytest.fixture
def password(pytestconfig):
    return pytestconfig.getoption("password")
@pytest.fixture
def database(pytestconfig):
    return pytestconfig.getoption("database")
@pytest.fixture
def ago_user(pytestconfig):
    return pytestconfig.getoption("ago_user")
@pytest.fixture
def ago_password(pytestconfig):
    return pytestconfig.getoption("ago_password")

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
