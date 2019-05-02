import unittest
import os

from moto import mock_s3
import boto3
import botocore

from databridge_etl_tools.oracle import Oracle


#CONNECTION_STRING = os.environ['TEST_ORACLE_CONNECTION_STRING']
CONNECTION_STRING = 'test'
TABLE_NAME        = 'li_imm_dang'
TABLE_SCHEMA      = 'lni'
S3_BUCKET         = 'S3_BUCKET'
S3_KEY            = 'mock_folder'

@mock_s3
class BaseOracleTest(unittest.TestCase):

    def setUp(self):
        self.oracle = Oracle(
            connection_string=CONNECTION_STRING,
            table_name=TABLE_NAME,
            table_schema=TABLE_SCHEMA,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY)

        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            )
        try:
            s3 = boto3.resource(
                "s3",
                region_name="us-east-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
                )
            s3.meta.client.head_bucket(Bucket=S3_BUCKET)
        except botocore.exceptions.ClientError:
            pass
        else:
            err = "{bucket} should not exist.".format(bucket=S3_BUCKET)
            raise EnvironmentError(err)
        client.create_bucket(Bucket=S3_BUCKET)
        current_dir = os.path.dirname(__file__)

    def tearDown(self):
        s3 = boto3.resource(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
            )
        bucket = s3.Bucket(S3_BUCKET)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
    
    def test_schema_table_name(self):
        schema_table_name = self.oracle.schema_table_name
        self.assertEqual(schema_table_name, '{}.{}'.format(self.oracle.table_schema, self.oracle.table_name))

    def test_load_csv_to_s3(self):
        pass
        
if __name__ == '__main__':
    unittest.main()