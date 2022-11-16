import pytest

from .constants import S3_BUCKET
from databridge_etl_tools.postgres import Postgres


TABLE_NAME         = 'testing'
TABLE_SCHEMA       = 'test'
CONNECTION_STRING  = 'connection_string'
S3_KEY         = 'csv.csv'
S3_KEY             = 'mock_folder'

@pytest.fixture
def postgres():
    postgres_client = Postgres(
        table_name=TABLE_NAME,
        table_schema=TABLE_SCHEMA,
        connection_string=CONNECTION_STRING,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY
    )
    return postgres_client

def test_table_schema_name(postgres):
    table_schema_name = postgres.table_schema_name
    assert table_schema_name == 'test.testing'
