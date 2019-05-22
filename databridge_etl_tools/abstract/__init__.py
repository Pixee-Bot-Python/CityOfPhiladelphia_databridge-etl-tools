import os
import logging
import sys
from abc import abstractmethod
import json

import boto3


class Client():
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
    _conn = None
    _logger = None
    _schema = None
    _geom_field = None
    _geom_srid = None

    def __init__(self, 
                 table_name, 
                 connection_string, 
                 s3_bucket, 
                 json_schema_s3_key, 
                 csv_s3_key):
        self.table_name = table_name
        self.connection_string = connection_string
        self.s3_bucket = s3_bucket
        self.json_schema_s3_key = json_schema_s3_key
        self.csv_s3_key = csv_s3_key

    @property
    def table_schema_name(self):
        # schema.table
        return '{}.{}'.format(self.table_schema, self.table_name)

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
    def json_schema_file_name(self):
        # This expects the schema to be in a subfolder on S3
        if ('/') in self.json_schema_s3_key:
            json_schema_file_name = self.json_schema_s3_key.split('/')[-1]
        else:
            json_schema_file_name = self.json_schema_s3_key
        return json_schema_file_name

    @property
    def json_schema_path(self):
        # On Windows, save to current directory
        if os.name == 'nt':
            json_schema_path = self.json_schema_file_name
        # On Linux, save to tmp folder
        else:
            json_schema_directory = '/tmp'
            json_schema_path = os.path.join(json_schema_directory, self.json_schema_file_name)
        return json_schema_path

    @property
    def geom_field(self):
        if self._geom_field is None:
            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', None)
                if not schema:
                    self.logger.error('Json schema malformatted...')
                    raise
                for scheme in schema:
                    scheme_type = self.DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        geom_field = scheme.get('name', None)
                        self._geom_field = geom_field
        return self._geom_field

    @property
    def geom_srid(self):
        if self._geom_srid is None:
            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', None)
                if not schema:
                    self.logger.error('Json schema malformatted...')
                    raise
                for scheme in schema:
                    scheme_type = self.DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        geom_srid = scheme.get('srid', None)
                        self._geom_srid = geom_srid
        return self._geom_srid

    @property
    def schema(self):
        if self._schema is None:

            with open(self.json_schema_path) as json_file:
                schema = json.load(json_file).get('fields', '')
                if not schema:
                    logger.error('Json schema malformatted...')
                    raise
                num_fields = len(schema)
                schema_fmt = ''
                for i, scheme in enumerate(schema):
                    scheme_type = self.DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    if scheme_type == 'geometry':
                        scheme_srid = scheme.get('srid', '')
                        scheme_geometry_type = self.GEOM_TYPE_MAP.get(scheme.get('geometry_type', '').lower(), '')
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
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

    @property
    @abstractmethod
    def conn(self):
        pass

    @property
    @abstractmethod
    def execute_sql(self):
        pass

    def get_json_schema_from_s3(self):
        self.logger.info('Fetching json schema: s3://{}/{}'.format(self.s3_bucket, self.json_schema_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(self.json_schema_path)

        self.logger.info('Json schema successfully downloaded.\n'.format(self.s3_bucket, self.json_schema_s3_key))

    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.csv_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_s3_key).download_file(self.csv_path)

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.csv_s3_key))
