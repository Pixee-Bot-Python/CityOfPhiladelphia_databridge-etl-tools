import pytest
import os
from databridge_etl_tools.postgres.postgres import Postgres, Postgres_Connector
from .constants import (S3_BUCKET, S3_KEY_CSV, TABLE_NAME, FIXTURES_DIR, STAGING_DIR, 
                        POINT_TABLE_2272_CSV)

TABLE_SCHEMA = 'citygeo'

@pytest.fixture(scope='module', autouse=True) # Use this fixture without anything calling it
def write_to_s3(s3_point_csv):
    '''Write the CSV contained in this package to S3 by calling fixture in conftest.py'''
    pass

@pytest.fixture(scope='module') # Only create a connection once for all tests below
def connector(user, password, host, database): # These parameters are defined in conftest.py
    '''Yield a Postgres Connector object'''
    with Postgres_Connector(connection_string=f'postgresql://{user}:{password}@{host}:5432/{database}') as connector_obj: 
        yield connector_obj

@pytest.fixture(scope='module') # Only CREATE TABLE once for all tests below
def create_table(connector): 
    '''CREATE TABLE IF NOT EXISTS'''
    with connector.conn.cursor() as cursor: 
        cursor.execute(f'''
    CREATE TABLE if not exists point_table_2272 (
        objectid int4 NOT NULL,
        textfield varchar(255) NULL,
        datefield timestamp NULL,
        numericfield numeric(38, 8) NULL,
        timezone timestamp NULL,
        "timestamp" timestamp NULL,
        newcol varchar(20) NULL,
        shape public.geometry NULL,
        CONSTRAINT enforce_srid_shape CHECK ((st_srid(shape) = 2272)),
        CONSTRAINT point_table_2272_pk PRIMARY KEY (objectid)
    );
    CREATE INDEX if not exists a298_ix1 ON point_table_2272 USING gist (shape);
    CREATE UNIQUE INDEX if not exists r496_sde_rowid_uk ON point_table_2272 USING btree (objectid) WITH (fillfactor='75');
        ''')
        print('Created table "point_table_2272"\n')

@pytest.fixture(scope='module', autouse=True)
def append_to_table(create_table, connector): # Only COPY data to table once for all tests below
    '''Delete any existing data in point_table_2272 and append test data'''
    with connector.conn.cursor() as cursor:
        cursor.execute('DELETE FROM point_table_2272;')     
        with open(os.path.join(FIXTURES_DIR, STAGING_DIR, POINT_TABLE_2272_CSV)) as f:
            stmt = 'COPY point_table_2272 FROM STDIN WITH (FORMAT csv, HEADER true)'
            cursor.copy_expert(stmt, f)
    connector.conn.commit()
    print('Appended test data to table point_table_2272\n')

@pytest.fixture
def pg(connector): 
    '''Yield a Postgres Table object'''
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
