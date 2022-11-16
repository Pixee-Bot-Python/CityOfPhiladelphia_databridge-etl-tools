import pytest
import os

from databridge_etl_tools.carto_ import Carto
from .constants import (
    S3_BUCKET,  
    POINT_CSV, POLYGON_CSV,
)
from _pytest.assertion import truncate
truncate.DEFAULT_MAX_LINES = 9999
truncate.DEFAULT_MAX_CHARS = 9999



CONNECTION_STRING            = 'carto://user:apikey'
TABLE_NAME                   = 'table_name'
S3_KEY                   = 'csv.csv'
SELECT_USERS                 = 'publicuser'

@pytest.fixture
def carto():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY)
    return carto_client


@pytest.fixture
def carto_subfolder():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY)
    return carto_client

@pytest.fixture
def carto_point():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_key=POINT_CSV)
    return carto_client

@pytest.fixture
def carto_polygon():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_key=POLYGON_CSV)
    return carto_client

def test_user(carto):
    assert carto.user == 'user'

def test_api_key(carto):
    assert carto.api_key == 'apikey'

def test_temp_table_name(carto):
    assert carto.temp_table_name == 't_table_name'

def test_csv_path(carto):
    if os.name == 'nt':
        assert carto.csv_path == 'table_name.csv'
    else:
        assert carto.csv_path == '/tmp/table_name.csv'

def test_temp_csv_path(carto):
    if os.name == 'nt':
        assert carto.temp_csv_path == 'table_name_t.csv'
    else:
        assert carto.temp_csv_path == '/tmp/table_name_t.csv'

#def test_point_schema(carto_point, s3_point_schema):
#    expected_point_schema = ' objectid numeric, textfield text, datefield date, numericfield numeric, shape geometry (Point, 2272) '
#    assert carto_point.schema == expected_point_schema

#def test_polygon_schema(carto_polygon, s3_polygon_schema):
#    expected_polygon_schema = ' objectid numeric, textfield text, datefield date, numericfield numeric, shape geometry (MultiPolygon, 2272) '
#    assert carto_polygon.schema == expected_polygon_schema

#def test_geom_field(carto_point, s3_point_schema):
#    assert carto_point.geom_field == 'shape'

#def test_geom_srid(carto_point, s3_point_schema):
#    assert carto_point.geom_srid == 2272

def test_get_csv_from_s3(carto_point, s3_point_csv):
    carto_point.get_csv_from_s3()
    assert os.path.isfile(carto_point.csv_path)
