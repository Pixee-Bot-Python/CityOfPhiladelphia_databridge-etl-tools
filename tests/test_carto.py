import pytest
import os

from databridge_etl_tools.carto_ import Carto
from .constants import (
    S3_BUCKET, 
    POINT_JSON_SCHEMA, POLYGON_JSON_SCHEMA, 
    POINT_CSV, POLYGON_CSV,
)


CONNECTION_STRING            = 'carto://user:apikey'
TABLE_NAME                   = 'table_name'
JSON_SCHEMA_S3_KEY           = 'json_schema.json'
JSON_SCHEMA_S3_KEY_SUBFOLDER = 'subfolder/json_schema.json'
S3_KEY                   = 'csv.csv'
SELECT_USERS                 = 'publicuser'

@pytest.fixture
def carto():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        json_schema_s3_key=JSON_SCHEMA_S3_KEY,
        s3_key=S3_KEY,
        select_users=SELECT_USERS)
    return carto_client


@pytest.fixture
def carto_subfolder():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        json_schema_s3_key=JSON_SCHEMA_S3_KEY_SUBFOLDER,
        s3_key=S3_KEY,
        select_users=SELECT_USERS)
    return carto_client

@pytest.fixture
def carto_point():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        json_schema_s3_key=POINT_JSON_SCHEMA,
        s3_key=POINT_CSV,
        select_users=SELECT_USERS)
    return carto_client

@pytest.fixture
def carto_polygon():
    carto_client = Carto(
        connection_string=CONNECTION_STRING,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        json_schema_s3_key=POLYGON_JSON_SCHEMA,
        s3_key=POLYGON_CSV,
        select_users=SELECT_USERS)
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

def test_json_schema_file_name_no_subfolder(carto):
    assert carto.json_schema_file_name == 'json_schema.json'

def test_json_schema_file_name_with_subfolder(carto_subfolder):
    assert carto_subfolder.json_schema_file_name == 'json_schema.json'

def test_json_schema_path_no_subfolder(carto):
    if os.name == 'nt':
        assert carto.json_schema_path == 'json_schema.json'
    else:
        assert carto.json_schema_path == '/tmp/json_schema.json'

def test_json_schema_path_with_subfolder(carto_subfolder):
    if os.name == 'nt':
        assert carto_subfolder.json_schema_path == 'json_schema.json'
    else:
        assert carto_subfolder.json_schema_path == '/tmp/json_schema.json'

def test_get_json_schema_from_s3(carto_point, s3_point_schema):
    carto_point.get_json_schema_from_s3()
    assert os.path.isfile(carto_point.json_schema_path)

def test_point_schema(carto_point, s3_point_schema):
    expected_point_schema = ' objectid numeric, textfield text, datefield date, numericfield numeric, shape geometry (Point, 2272) '
    assert carto_point.schema == expected_point_schema

def test_polygon_schema(carto_polygon, s3_polygon_schema):
    expected_polygon_schema = ' objectid numeric, textfield text, datefield date, numericfield numeric, shape geometry (MultiPolygon, 2272) '
    assert carto_polygon.schema == expected_polygon_schema

def test_geom_field(carto_point, s3_point_schema):
    assert carto_point.geom_field == 'shape'

def test_geom_srid(carto_point, s3_point_schema):
    assert carto_point.geom_srid == 2272

def test_get_csv_from_s3(carto_point, s3_point_csv):
    carto_point.get_csv_from_s3()
    assert os.path.isfile(carto_point.csv_path)
