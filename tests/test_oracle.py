import os
from operator import eq

import pytest
from mock import patch

from .constants import S3_BUCKET
from databridge_etl_tools.oracle import Oracle


# Note: cli args are passed in via cli, see: conftest.py
@pytest.fixture
def oracle(user, password, host, database):
    conn_string = f'{user}/{password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT=1521))(CONNECT_DATA=(SID={database})))'
    # NOTE: can't do a table with dates, because when we run an etl.convert on the petl
    # dataframe, for some reason it loses it's geopetl functions and our tests will fail.
    # This only happens in pytest. I don't know why.
    table_name = 'multipolygon_table_2272'
    table_schema = 'gis_test'
    s3_bucket = 'airflow-testing-v2'
    s3_key = 'staging/test/multipolygon_table_2272.csv'


    # Initialize the class so we can run stuff
    # on it, see cli.py or the python file itself
    # to see what you can do.
    oracle_client = Oracle(
        connection_string=conn_string,
        table_name=table_name,
        table_schema=table_schema,
        s3_bucket=s3_bucket,
        s3_key=s3_key)
    return oracle_client

def test_oracle_extract(oracle):
    oracle.extract()
    # Assert db only called once
    # can't do separate test, object seems to be lost between tests?
    assert oracle.times_db_called == 1

