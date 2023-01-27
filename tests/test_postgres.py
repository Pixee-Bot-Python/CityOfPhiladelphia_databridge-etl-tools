import pytest

from .constants import S3_BUCKET
from databridge_etl_tools.postgres import Postgres

@pytest.fixture
def postgres(user, password, host, database):
    conn_string = f'postgresql://{user}:{password}@{host}:5432/{database}'
    postgres_client = Postgres(
        table_name='multipolygon_table_2272',
        table_schema='citygeo',
        connection_string=conn_string,
        s3_bucket='airflow-testing-v2',
        s3_key='staging/test/multipolygon_table_2272.csv'
    )
    return postgres_client

def test_postgres_extract(postgres):
    postgres.extract()
