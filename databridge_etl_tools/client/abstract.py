from abc import ABC, abstractmethod
import logging
import os
import sys
import json

import boto3


GIS_PREFIX = 'gis_'
S3_STAGING_PREFIX = 'staging'
JSON_SCHEMA_PREFIX = 'schemas'

DATA_TYPE_MAP = {
    'string':           'text',
    'number':           'numeric',
    'float':            'numeric',
    'double precision': 'numeric',
    'integer':          'integer',
    'boolean':          'boolean',
    'object':           'jsonb',
    'array':            'jsonb',
    'date':             'date',
    'time':             'time',
    'datetime':         'date',
    'geom':             'geometry',
    'geometry':         'geometry'
}

GEOM_TYPE_MAP = {
    'point':           'Point',
    'line':            'Linestring',
    'polygon':         'MultiPolygon',
    'multipolygon':    'MultiPolygon',
    'multilinestring': 'MultiLineString',
    'geometry':        'Geometry',
}

class BaseClient(ABC):

    _conn = None
    _logger = None
    _geom_field = None
    _geom_srid = None
    _schema = None
    _num_rows_in_upload_file = None

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, index_fields=None):
        self.connection_string = os.environ.get('CONNECTION_STRING', connection_string)
        self.table_name = table_name
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.index_fields = index_fields

    @property
    def schema_table_name(self):
        schema_table_name = '{}.{}'.format(self.table_schema, self.table_name)
        return schema_table_name

    @property
    def temp_table_name(self):
        if not self.table_name:
            self.logger.error("Can't get table name, exiting...")
            exit(1)
        return 't_' + self.table_name

    @property
    @abstractmethod
    def conn(self):
        pass

    @property
    def csv_path(self):
        # On Windows, save to current directory
        if os.name == 'nt':
            csv_path = '{}.csv'.format(self.table_name)
        # On Linux, save to tmp folder
        else:
            csv_path = '/tmp/{}_{}.csv'.format(self.table_name)
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

    @property
    def csv_s3_key(self):
        if GIS_PREFIX in self.table_schema:
            table_schema = self.table_schema.replace(GIS_PREFIX, '')
        csv_s3_key = '{}/{}/{}.csv'.format(S3_STAGING_PREFIX, table_schema, self.table_name)
        return csv_s3_key

    @property
    def json_schema_file_name(self):
        json_schema_file_name = '{}__{}.json'.format(self.table_schema, self.table_name)
        return json_schema_file_name

    @property
    def json_schema_path(self):
        # On Windows, save to current directory
        if os.name == 'nt':
            if not os.path.exists(JSON_SCHEMA_PREFIX):
                os.makedirs(JSON_SCHEMA_PREFIX)
            json_schema_path = os.path.join(JSON_SCHEMA_PREFIX, self.json_schema_file_name)
        # On Linux, save to tmp folder
        else:
            json_schema_directory = os.path.join('tmp', JSON_SCHEMA_PREFIX)
            if not os.path.exists(json_schema_directory):
                os.makedirs(json_schema_directory)
            json_schema_path = os.path.join(json_schema_directory, self.json_schema_file_name)
        return json_schema_path

    @property
    def json_schema_s3_key(self):
        json_schema_s3_key = '{}/{}'.format(JSON_SCHEMA_PREFIX, self.json_schema_file_name)
        return json_schema_s3_key

    @property
    def schema(self):
        if self._schema is None:
            self.get_json_schema_from_s3()

            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', '')
                if not schema:
                    logger.error('Json schema malformatted...')
                    raise
                num_fields = len(schema)
                schema_fmt = ''
                for i, scheme in enumerate(schema):
                    scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        scheme_srid = scheme.get('srid', '')
                        scheme_geometry_type = GEOM_TYPE_MAP.get(scheme.get('geometry_type', '').lower(), '')
                        if scheme_srid and scheme_geometry_type:
                            scheme_type = '''geometry ({}, {}) '''.format(scheme_geometry_type, scheme_srid)
                        else:
                            logger.error('srid and geometry_type must be provided with geometry field...')
                            raise

                    schema_fmt += ' {} {}'.format(scheme['name'], scheme_type)
                    if i < num_fields - 1:
                        schema_fmt += ','
            self._schema = schema_fmt
        return self._schema

    @property
    def geom_field(self):
        if self._geom_field is None:
            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', None)
                if not schema:
                    self.logger.error('Json schema malformatted...')
                    raise
                for scheme in schema:
                    scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        geom_field = scheme.get('name', None)
                        self._geom_field = geom_field
        return self._geom_field

    @property
    def geom_srid(self):
        if self._geom_srid is None:
            for scheme in self.schema:
                scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    geom_srid = scheme.get('name', None)
                    self._geom_srid = geom_srid
        return self._geom_srid

    def load_csv_to_s3(self):
        self.logger.info('Starting load to s3: {}'.format(self.csv_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_s3_key).put(Body=open(self.csv_path, 'rb'))
        
        self.logger.info('Successfully loaded to s3: {}'.format(self.csv_s3_key))
        
    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.csv_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_s3_key).download_file(self.csv_path)

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.csv_s3_key))

    def get_json_schema_from_s3(self):
        self.logger.info('Fetching json schema: s3://{}/{}'.format(self.s3_bucket, self.json_schema_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(self.json_schema_path)

        self.logger.info('Json schema successfully downloaded.\n'.format(self.s3_bucket, self.json_schema_s3_key))

    def execute_sql(self, stmt, fetch=None):
        self.logger.info('Executing: {}'.format(stmt))

        with self.conn.cursor() as cursor:
            cursor.execute(stmt)

            if fetch == 'one':
                result = cursor.fetchone()
                return result

            elif fetch == 'many':
                result = cursor.fetchmany()
                return result

    def verify_count(self):
        self.logger.info('Verifying row count...')

        data = self.execute_sql('SELECT count(*) FROM "{}";'.format(self.temp_table_name), fetch='many')
        num_rows_in_table = data['rows'][0]['count']
        num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
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

    def generate_select_grants(self):
        grants_sql = ''
        if not db_select_users:
            return grants_sql
        for user in db_select_users:
            self.logger.info('{} - Granting SELECT to {}'.format(db_table_name, user))
            grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(db_table_name, user)
        self.logger.info(grants_sql)
        return grants_sql

    def create_indexes(self):
        self.logger.info('Creating indexes on {}: {}'.format(table_name=self.temp_table_name,
                                                             indexes_fields=self.indexes_fields))
        stmt = ''
        for indexes_field in self.indexes_fields:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=self.temp_table_name,
                                                                                      field=indexes_field)
        self.execute_sql(stmt)
        self.logger.info('Indexes created successfully.\n')

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
        