import pytest
from .constants import S3_BUCKET
from databridge_etl_tools.postgres.postgres import Postgres,Postgres_Connector

            
def test_postgres_point_extract(user, password, host, database):
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector:
        with Postgres(connector=connector,
            table_name='point_table_2272',
            table_schema='citygeo',
            s3_bucket='airflow-testing-v2',
            s3_key='staging/test/point_table_2272.csv') as pg:
                pg.extract()


def test_postgres_upsert(user, password, host, database):
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector:
        with Postgres(connector=connector,
            table_name='test_contractor_violations',
            table_schema='citygeo',
            s3_bucket='airflow-testing-v2',
            s3_key='staging/lni/contractor_violations.csv') as pg:
                pg.upsert('csv')


def test_postgres_load(user, password, host, database):
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector:
        with Postgres(connector=connector,
            table_name='test_contractor_violations',
            table_schema='citygeo',
            s3_bucket='airflow-testing-v2',
            s3_key='staging/lni/contractor_violations.csv') as pg:
                pg.truncate()
                pg.load()


def test_postgres_json_schema_extract(user, password, host, database):
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector:
        with Postgres(connector=connector,
            table_name='point_table_2272',
            table_schema='citygeo',
            s3_bucket='airflow-testing-v2',
            s3_key='schemas/citygeo/point_table_2272.json') as pg:
                pg.load_json_schema_to_s3()