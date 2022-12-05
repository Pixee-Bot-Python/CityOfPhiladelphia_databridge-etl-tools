import os
from operator import eq

import pytest
from mock import patch

from .constants import S3_BUCKET
from databridge_etl_tools.oracle import Oracle


CONNECTION_STRING = 'connection_string'
TABLE_NAME        = 'table_name'
TABLE_SCHEMA      = 'schema'
S3_KEY            = 'mock_folder'

@pytest.fixture
def oracle():
    oracle_client = Oracle(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        table_schema=TABLE_SCHEMA,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY)
    return oracle_client
    
def test_schema_table_name(oracle):
    schema_table_name = oracle.schema_table_name
    assert schema_table_name == 'schema.table_name'

def test_csv_path(oracle):
    if os.name == 'nt':
        assert oracle.csv_path == 'table_name.csv'
    else:
        assert oracle.csv_path == '/tmp/table_name.csv'

#def test_load_csv_to_s3(oracle, s3_bucket):
#    oracle.load_csv_and_schema_to_s3()
#    assert os.path.isfile(oracle.csv_path)
