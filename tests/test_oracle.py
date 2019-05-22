import os
from operator import eq

import pytest

from databridge_etl_tools.oracle import Oracle


#CONNECTION_STRING = os.environ['TEST_ORACLE_CONNECTION_STRING']
CONNECTION_STRING = 'test'
TABLE_NAME        = 'li_imm_dang'
TABLE_SCHEMA      = 'lni'
S3_BUCKET         = 'S3_BUCKET'
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
    assert eq(schema_table_name, '{}.{}'.format(oracle.table_schema, oracle.table_name))

def test_load_csv_to_s3():
    pass