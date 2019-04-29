import csv
import sys
import os
import re
import json

from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient
from carto.exceptions import CartoException
import petl as etl
import geopetl
import requests

from .postgres import Postgres


csv.field_size_limit(sys.maxsize)

TEST = os.environ.get('TEST', False)
USR_BASE_URL = "https://{user}.carto.com/"
CONNECTION_STRING_REGEX = r'^carto://(.+):(.+)'

class Carto(Postgres):

    _user = None
    _api_key = None
    _schema = None

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, select_users):

        super(Carto, self).__init__(
            connection_string=connection_string,
            table_name=table_name,
            table_schema=table_schema,
            s3_bucket=s3_bucket,
            select_users=select_users
        )

    @property
    def user(self):
        if self._user is None:
            creds = re.match(CONNECTION_STRING_REGEX, self.connection_string).groups()
            user = creds[0]
            self._user = user
        return self._user

    @property
    def api_key(self):
        if self._api_key is None:
            creds = re.match(CONNECTION_STRING_REGEX, self.connection_string).groups()
            api_key = creds[1]
            self._api_key = api_key
        return self._api_key

    @property
    def conn(self):
        if self._conn is None:
            self.logger.info('Making connection to Carto {} account...'.format(self.user))
            try:
                api_key = self.api_key
                base_url = USR_BASE_URL.format(user=self.user)
                auth_client = APIKeyAuthClient(api_key=api_key, base_url=base_url)
                conn = SQLClient(auth_client)
                self._conn = conn
                self.logger.info('Connected to Carto.\n')
            except CartoException as e:
                self.logger.error('Failed making connection to Carto {} account...'.format(self.user))
                raise e
        return self._conn

    def execute_sql(self, stmt, fetch='many'):
        self.logger.info('Executing: {}'.format(stmt))
        response = self.conn.send(stmt)
        return response

    def create_table(self):
        self.logger.info('Creating temp table...')
        stmt = '''DROP TABLE IF EXISTS {table_name}; 
                    CREATE TABLE {table_name} ({schema});'''.format(table_name=self.temp_table_name,
                                                                    schema=self.schema)
        self.execute_sql(stmt)
        check_table_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}');".format(self.temp_table_name)
        response = self.execute_sql(check_table_sql, fetch='many')
        exists = response['rows'][0]['exists']

        if not exists:
            message = '{} - Could not create table'.format(self.temp_table_name)
            self.logger.error(message)
            raise Exception(message)

        if self.index_fields:
            self.logger.info("Indexing fields: {}".format(self.index_fields))
            self.create_indexes()

        self.logger.info('Temp table created successfully.\n')

    def write(self):
        self.get_csv_from_s3()
        rows = etl.fromcsv(self.csv_path, encoding='latin-1') \
                    .cutout('etl_read_timestamp')
        header = rows[0]
        str_header = ''
        num_fields = len(header)
        self._num_rows_in_upload_file = rows.nrows()
        for i, field in enumerate(header):
            if i < num_fields - 1:
                str_header += field + ', '
            else:
                str_header += field

        self.logger.info('Writing to temp table...')
        # format geom field:
        if self.geom_field and geom_srid:
            rows = rows.convert(geom_field,
                                lambda c: 'SRID={srid};{geom}'.format(srid=geom_srid, geom=c) if c else '')
        write_file = self.temp_csv_path
        rows.tocsv(write_file)
        q = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
            table_name=self.temp_table_name, header=str_header)
        url = USR_BASE_URL.format(user=self.user) + 'api/v2/sql/copyfrom'
        with open(write_file, 'rb') as f:
            r = requests.post(url, params={'api_key': self.api_key, 'q': q}, data=f, stream=True)

            if r.status_code != 200:
                self.logger.error('Carto Write Error Response: {}'.format(r.text))
                self.logger.error('Exiting...')
                exit(1)
            else:
                status = r.json()
                self.logger.info('Carto Write Successful: {} rows imported.\n'.format(status['total_rows']))

    def verify_count(self):
        self.logger.info('Verifying row count...')

        data = self.execute_sql('SELECT count(*) FROM "{}";'.format(self.temp_table_name), fetch='many')
        num_rows_in_table = data['rows'][0]['count']
        num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
        # Carto does count the header
        num_rows_expected = self._num_rows_in_upload_file
        message = '{} - expected rows: {} inserted rows: {}.'.format(
            self.temp_table_name,
            num_rows_expected,
            num_rows_inserted
        )
        self.logger.info(message)
        if num_rows_in_table != num_rows_expected:
            self.logger.error('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
                    'DROP TABLE if exists "{}" cascade;'.format(temp_table_name) + \
                    'COMMIT;'
            execute_sql(stmt)
            exit(1)
        self.logger.info('Row count verified.\n')

    def cartodbfytable(self):
        self.logger.info('Cartodbfytable\'ing table: {}'.format(self.temp_table_name))
        self.execute_sql("select cdb_cartodbfytable('{}', '{}');".format(self.user, self.temp_table_name))
        self.logger.info('Successfully Cartodbyfty\'d table.\n')

    def swap_table(self):
        stmt = 'BEGIN;' + \
                'ALTER TABLE "{}" RENAME TO "{}_old";'.format(self.table_name, self.table_name) + \
                'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.table_name) + \
                'DROP TABLE "{}_old" cascade;'.format(self.table_name) + \
                self.generate_select_grants() + \
                'COMMIT;'
        self.logger.info('Swapping temporary and production tables...')
        self.logger.info(stmt)
        self.execute_sql(stmt)

    def run_workflow(self):
        if TEST:
            self.logger.info('THIS IS A TEST RUN, PRODUCTION TABLES WILL NOT BE AFFECTED!\n')
        try:
            self.create_table()
            self.write()
            self.verify_count()
            self.cartodbfytable()
            self.vacuum_analyze()
            if TEST:
                self.cleanup()
            else:
                self.swap_table()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed, reverting...')
            self.cleanup()
            raise e