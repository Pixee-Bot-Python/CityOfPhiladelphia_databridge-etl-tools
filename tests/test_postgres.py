import pytest
from databridge_etl_tools.postgres.postgres import Postgres, Postgres_Connector

S3_BUCKET = 'citygeo-airflow-databridge2-testing'            
TABLE_NAME = 'point_table_2272'
TABLE_SCHEMA = 'citygeo'
S3_KEY_CSV = 'staging/citygeo/point_table_2272.csv'
S3_KEY_JSON = 'schemas/citygeo/point_table_2272.json'

# Remaining to-do: 
# Get point_table_2272 into this package, and write a fixture to write it to S3
# Remove .json files from /tmp
# Add useful comments in this file

@pytest.fixture
def connector(user, password, host, database): 
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector_obj: 
        yield connector_obj


@pytest.fixture
def pg(connector): 
    with Postgres(connector=connector,
                  table_name=TABLE_NAME,
                  table_schema=TABLE_SCHEMA,
                  s3_bucket=S3_BUCKET,
                  s3_key=S3_KEY_CSV, 
                  with_srid=True) as pg_obj:
        yield pg_obj        


def test_postgres_point_extract(pg):
    pg.extract()


def test_postgres_upsert(pg):
    pg.upsert('csv')


def test_postgres_load(pg):
    pg.truncate()
    pg.load()


def test_postgres_json_schema_extract(pg):
    pg.load_json_schema_to_s3()
