import pytest

from .constants import S3_BUCKET
from databridge_etl_tools.ago import AGO

@pytest.fixture
def ago(ago_user, ago_password):
    ago_client = AGO(
        ago_org_url='https://phl.maps.arcgis.com',
        ago_item_name='POINT_TABLE_2272',
        ago_user=ago_user,
        ago_pw=ago_password,
        s3_bucket='airflow-testing-v2',
        s3_key='staging/test/point_table_2272.csv',
        in_srid=2272
    )
    return ago_client

def test_ago_truncate_append(ago):
    ago.get_csv_from_s3()
    ago.append(truncate=True)
    ago.verify_count()

