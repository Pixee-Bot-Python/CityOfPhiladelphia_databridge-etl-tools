import os

import cx_Oracle
import petl as etl
import geopetl

from .abstract import BaseClient


class Oracle(BaseClient):

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, output_file):

        super(Oracle, self).__init__(
            connection_string=connection_string,
            table_name=table_name,
            table_schema=table_schema,
            s3_bucket=s3_bucket,
            output_file=output_file
        )

    @property
    def conn(self):
        if self._conn is None:
            self.logger.info('Trying to connect to Oracle database...')
            conn = cx_Oracle.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to database.')
        return self._conn

    def extract(self):
        self.logger.info('Starting extract from {}'.format(self.schema_table_name))

        etl.fromoraclesde(self.conn, self.schema_table_name, timestamp=True) \
           .tocsv(self.csv_path, encoding='latin-1')

        self.load_csv_to_s3()
        os.remove(self.csv_path)

        self.logger.info('Successfully extracted from {}'.format(self.schema_table_name))