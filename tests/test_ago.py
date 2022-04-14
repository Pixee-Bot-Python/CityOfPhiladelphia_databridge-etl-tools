import pytest

from .constants import S3_BUCKET
from databridge_etl_tools.ago import AGO
import boto3


AGO_ORG_URL        = 'phl.maps.arcgis.com'
AGO_ITEM_NAME       = 'mock_item_name'
AGO_USER            = 'auser'
AGO_PW              = 'password'
S3_KEY         = 'csv.csv'
S3_BUCKET             = 'mock_folder'
IN_SRID             = 2272

@pytest.fixture
def ago():
    ago_client = AGO(
        ago_org_url=AGO_ORG_URL,
        ago_item_name=AGO_ITEM_NAME,
        ago_user=AGO_USER,
        ago_pw=AGO_PW,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        in_srid=IN_SRID
    )
    return ago_client

def test_ago_auth(ago):
    print('Testing ago auth which ensures we have our AWS key env vars configured...')
    client = boto3.setup_default_session(region_name='us-east-1')
    s3_client = boto3.client('s3')
    s3_client.list_objects(Bucket='airflow-testing-v2')
    print('Success.')
