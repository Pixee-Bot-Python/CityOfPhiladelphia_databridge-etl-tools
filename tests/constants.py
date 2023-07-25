import os 
S3_BUCKET = 'citygeo-airflow-databridge2-testing'

POINT_JSON_SCHEMA = 'point.json'
POLYGON_JSON_SCHEMA = 'polygon.json'
POINT_CSV = 'point.csv'
POLYGON_CSV = 'polygon.csv'

POINT_TABLE_2272_NAME = 'test_point_table_2272'
POINT_TABLE_2272_CSV = 'point_table_2272.csv'
POINT_TABLE_2272_S3_KEY_CSV = 'staging/citygeo/point_table_2272.csv'
POINT_TABLE_2272_S3_KEY_JSON = 'schemas/citygeo/point_table_2272.json'

FIXTURES_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'fixtures_data'
    )
SCHEMA_DIR = 'schemas'
STAGING_DIR = 'staging'