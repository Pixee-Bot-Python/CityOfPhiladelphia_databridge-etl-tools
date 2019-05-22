import pytest

from .constants import S3_BUCKET
from databridge_etl_tools.postgres import Postgres


TABLE_NAME         = 'table_name'
TABLE_SCHEMA       = 'schema'
CONNECTION_STRING  = 'connection_string'
JSON_SCHEMA_S3_KEY = 'json_schema.json'
CSV_S3_KEY         = 'csv.csv'
S3_KEY             = 'mock_folder'

@pytest.fixture
def postgres():
    postgres_client = Postgres(
        table_name=TABLE_NAME,
        table_schema=TABLE_SCHEMA,
        connection_string=CONNECTION_STRING,
        s3_bucket=S3_BUCKET,
        json_schema_s3_key=JSON_SCHEMA_S3_KEY,
        csv_s3_key=CSV_S3_KEY
    )
    return postgres_client

def test_table_schema_name(postgres):
    table_schema_name = postgres.table_schema_name
    assert table_schema_name == 'schema.table_name'