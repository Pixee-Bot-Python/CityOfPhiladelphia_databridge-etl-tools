import pytest
import os

from databridge_etl_tools.carto.carto_ import Carto
from .constants import (
    S3_BUCKET,  
    POINT_CSV, POLYGON_CSV,
)

@pytest.fixture
def carto_point(carto_user, carto_password):
    carto_client = Carto(
        connection_string=f'carto://{carto_user}:{carto_password}',
        table_name='dbtools_testing',
        s3_bucket=S3_BUCKET,
        s3_key='staging/test/point_table_2272.csv',
        select_users='publicuser,tileuser',
        index_fields='textfield,shape')
    return carto_client

def test_carto_point_upload(carto_point):
    carto_point.run_workflow()
