import logging
import csv
import sys
import os
import re
import json

from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient
from carto.exceptions import CartoException
import boto3
import requests
import petl as etl

import pandas as pd
import numpy as np
import dateutil.parser


csv.field_size_limit(sys.maxsize)

USR_BASE_URL = "https://{user}.carto.com/"
CONNECTION_STRING_REGEX = r'^carto://(.+):(.+)'


class Carto():

    _conn = None
    _logger = None
    _user = None
    _api_key = None
    _geom_field = None
    _geom_srid = None
    _geom_type = None
    _schema = None

    def __init__(self, 
                 connection_string, 
                 table_name, 
                 s3_bucket, 
                 s3_key,
                 **kwargs):
        self.connection_string = connection_string
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.select_users = kwargs.get('select_users', None)
        self.index_fields = kwargs.get('index_fields', None)
        self.override_datatypes = kwargs.get('override_datatypes', None)

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

    @property
    def temp_table_name(self):
        if not self.table_name:
            self.logger.error("Can't get table name, exiting...")
            exit(1)
        return 't_' + self.table_name

    @property
    def csv_path(self):
        csv_file_name = self.table_name
        # On Windows, save to current directory
        if os.name == 'nt':
            csv_path = '{}.csv'.format(csv_file_name)
        # On Linux, save to tmp folder
        else:
            csv_path = '/tmp/{}.csv'.format(csv_file_name)
        return csv_path

    @property
    def temp_csv_path(self):
        temp_csv_path = self.csv_path.replace('.csv', '_t.csv')
        return temp_csv_path

    @property
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger


    def geometric_detection(self):
        if self._geom_field is None:
            # Only read in the first 1000 rows into memory for faster detection
            # in case the CSV is really really big
            # Hopefully the first 5000 rows don't all contain null geometry
            df = pd.read_csv(self.csv_path, nrows=5000)

            # Field names
            fields = df.columns.values.tolist()

            candidate_shape = None
            if 'shape' in fields:
                candidate_shape = 'shape' 
            elif 'geom' in fields:
                candidate_shape = 'geom' 


            if candidate_shape:
                # Create a new df that drops rows with a null shape
                non_null_df = df.dropna(subset='shape', how='any')

                # If we still have data..
                if not non_null_df.empty:
                    # Extract a shape value
                    example_shape = non_null_df.loc[1]['shape']
                    if example_shape is not None:
                        # Extract the SRID
                        #print(example_shape.split(';'))
                        self._geom_field = candidate_shape
                        if 'SRID=' in example_shape:
                            self._geom_srid = example_shape.split(';')[0].replace('SRID=','')

                        # Extract the geom_type
                        if ';' in example_shape:
                            asplit = example_shape.split(';')[1].split('(')
                            self._geom_type = asplit[0].strip()
            else:
                self._geom_field = None
                self._geom_type = None
                self._geom_srid = None

    @property
    def schema(self):
        '''
        Auto detect data types by using pandas dtype detection and some
        custom methods such as attempting to parse dates and trying to 
        figure out the top length of a string field.
        We only pull in the first 5000 rows so this doesn't take forever
        on larger datasets.
        '''
        if self._schema is None:
            # Only read in the first 5000 rows into memory for faster detection
            # in case the CSV is really really big
            # Hopefully the first 5000 rows don't all contain null geometry
            df = pd.read_csv(self.csv_path, nrows=5000)
            # Get all our fields and possible types in a dictionary to loop through
            type_dict = df.dtypes.to_dict()
            #print('\nPrinting dtypes...')
            # Parse any possible override data type args we got
            if self.override_datatypes:
                self.override_datatypes = self.override_datatypes.strip('\'')
                self.override_datatypes = self.override_datatypes.strip('\"')

            schema = ''
            # In batch sometimes the var gets communicated with extra quotes
            # Loop through fields/columns
            for k,v in type_dict.items():
                # Check to see if we got passed a direct datatype for this one
                atype = None
                if self.override_datatypes:
                    overrides = self.override_datatypes.split(',')
                    for o in overrides:
                        #print(f"{k} == {o.split(':')[0]} ?")
                        if k == o.split(':')[0]:
                            atype = o.split(':')[1]
                if atype == None:
                    #print(f'field: {k},  detected type: {type(v)}, first value: {df[k].loc[1]}')
                    if v == np.int32:
                        atype = 'int4'
                    elif v == np.int64:
                        atype = 'int8'
                    elif v == np.float:
                        atype = 'float4'
                    elif v == np.object:
                        # if object, check if it's a datetime
                        non_null_df = df.dropna(subset=k, how='any')
                        #print(f'DEBUG: {k}')
                        ex_val = non_null_df.loc[1][k]
                        if isinstance(ex_val, str):
                            # Account for odd values like "9:30 PM" by putting a length lmit.
                            # The date parser will parse this, but when we attempt to upload to carto
                            # it won't accept it as a date.
                            if len(ex_val) > 8:
                                try:
                                    adate = dateutil.parser.parse(ex_val)
                                    print(f'Detected a date: {adate}, field: {k}')
                                    if adate.tzinfo is not None:
                                        atype = 'timestamp with time zone'
                                    if adate.tzinfo is None:
                                        atype = 'timestamp without time zone'
                                    #if not adate.year:
                                    #    atype = 'time'
                                    #if '+' in ex_val or '-' in ex_val:
                                        #atype = 'timestamp with time zone'
                                except dateutil.parser._parser.ParserError as e:
                                    pass
                        # if we still don't have a type, assume string and check length
                        # Also determine varchar length
                        if atype is None:
                            # Drop all null values for this column
                            non_null_df = df.dropna(subset=k, how='any')
                            max_len = non_null_df[k].str.len().max()
                            atype = f'varchar({max_len+50})'

                if atype:
                    schema = schema + f'{k} {atype}, '
                elif not atype:
                    raise TypeError(f'Could not determine a data type for field {k}!! Please pass an override via the override_datatypes argument (see its help text in cli.py)')
                    #print('DEBUG ', atype)
            # strip last two characters
            schema = schema[:-2]
            print(f'Devised schema: {schema}')
            self._schema = schema
        return self._schema


    @property
    def geom_field(self):
        return self._geom_field

    @property
    def geom_type(self):
        return self._geom_type

    @property
    def geom_srid(self):
        return self._geom_srid


    def get_csv_from_s3(self):
        print('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)

        print('CSV successfully downloaded.\n'.format(self.s3_bucket, self.s3_key))

    def execute_sql(self, stmt, fetch='many'):
        self.logger.info('Executing: {}'.format(stmt))
        response = self.conn.send(stmt)
        return response

    def create_table(self):
        self.logger.info('Creating temp table...')
        stmt = f'''DROP TABLE IF EXISTS {self.temp_table_name}; 
                    CREATE TABLE {self.temp_table_name} ({self.schema});'''
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
        
    def create_indexes(self):
        self.logger.info('Creating indexes on {}: {}'.format(
            self.temp_table_name,
            self.index_fields)
        )
        
        indexes = self.index_fields.split(',')
        stmt = ''
        for indexes_field in indexes:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=self.temp_table_name,
                                                                                      field=indexes_field)
        self.execute_sql(stmt)
        self.logger.info('Indexes created successfully.\n')

    def extract(self):
        raise NotImplementedError

    def write(self):
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            self.logger.info("Exception encountered trying to import rows with utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
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
        if self.geom_field and self.geom_srid:
            rows = rows.convert(self.geom_field,
                                lambda c: 'SRID={srid};{geom}'.format(srid=self.geom_srid, geom=c) if c else '')
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

    def vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.temp_table_name))
        self.execute_sql('VACUUM ANALYZE "{}";'.format(self.temp_table_name))
        self.logger.info('Vacuum analyze complete.\n')

    def generate_select_grants(self):
        grants_sql = ''
        if self.select_users:
            select_users = self.select_users.split(',')
            for user in select_users:
                self.logger.info('{} - Granting SELECT to {}'.format(self.table_name, user))
                grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(self.table_name, user)
            return grants_sql
        else: 
            return ''

    def cleanup(self):
        self.logger.info('Attempting to drop any temporary tables: {}'.format(self.temp_table_name))
        stmt = '''DROP TABLE IF EXISTS {} cascade'''.format(self.temp_table_name)
        self.execute_sql(stmt)
        self.logger.info('Temporary tables dropped successfully.\n')

        self.logger.info('Attempting to drop temp files...')

        for f in [self.csv_path, self.temp_csv_path]:
            if os.path.isfile(f):
                os.remove(f)

        self.logger.info('Successfully removed temp files.')

    def swap_table(self):
        stmt = 'BEGIN;' + \
                'ALTER TABLE IF EXISTS "{}" RENAME TO "{}_old";'.format(self.table_name, self.table_name) + \
                'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.table_name) + \
                'DROP TABLE IF EXISTS "{}_old" cascade;'.format(self.table_name) + \
                self.generate_select_grants() + \
                'COMMIT;'
        self.logger.info('Swapping temporary and production tables...')
        self.logger.info(stmt)
        self.execute_sql(stmt)

    def run_workflow(self):
        try:
            self.get_csv_from_s3()
            # Run our initial detection function using the CSV
            self.geometric_detection()
            self.create_table()
            self.write()
            self.verify_count()
            self.cartodbfytable()
            self.vacuum_analyze()
            self.swap_table()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed, reverting...')
            raise e
        finally:
            self.cleanup()
