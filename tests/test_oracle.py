import pytest
from .constants import S3_BUCKET
from databridge_etl_tools.oracle.oracle import Oracle

# Note: cli args are passed in via cli, see: conftest.py
@pytest.fixture(scope='module')
def conn_string(user, password, host, database): 
    return f'{user}/{password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT=1521))(CONNECT_DATA=(SID={database})))'

@pytest.fixture(scope='module')
def oracle_point(conn_string):
    # Initialize the class so we can run stuff on it, see the oracle python 
    # file itself to see what you can do.
    return Oracle(
        connection_string=conn_string,
        table_name='point_table_2272',
        table_schema='gis_test',
        s3_bucket=S3_BUCKET,
        s3_key='staging/test/point_table_2272.csv')

@pytest.fixture(scope='module')
def oracle_multipolygon(conn_string):
    return Oracle(
        connection_string=conn_string,
        table_name='multipolygon_table_2272',
        table_schema='gis_test',
        s3_bucket=S3_BUCKET,
        s3_key='staging/test/multipolygon_table_2272.csv')

def test_oracle_extract_and_assert_one_db_call_point_table(oracle_point):
    oracle_point.extract()
    assert oracle_point.times_db_called == 1

def test_oracle_extract_and_assert_one_db_call_multipolygon_table(oracle_multipolygon):
    oracle_multipolygon.extract()
    assert oracle_multipolygon.times_db_called == 1

# More tests are needed
