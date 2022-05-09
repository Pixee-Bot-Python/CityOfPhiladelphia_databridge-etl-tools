import csv
import logging
import sys
import os
import json

import psycopg2
import boto3
import geopetl
import petl as etl


csv.field_size_limit(sys.maxsize)

DATA_TYPE_MAP = {
    'string':                      'text',
    'number':                      'numeric',
    'float':                       'numeric',
    'double precision':            'numeric',
    'integer':                     'integer',
    'boolean':                     'boolean',
    'object':                      'jsonb',
    'array':                       'jsonb',
    'date':                        'date',
    'time':                        'time',
    'datetime':                    'timestamp without time zone',
    'timestamp without time zone': 'timestamp without time zone',
    'timestamp with time zone':    'timestamp with time zone',
    'geom':                        'geometry',
    'geometry':                    'geometry'
}

GEOM_TYPE_MAP = {
    'point':           'Point',
    'line':            'Linestring',
    'linestring':      'Linestring',
    'polygon':         'MultiPolygon',
    'multipolygon':    'MultiPolygon',
    'multilinestring': 'MultiLineString',
    'geometry':        'Geometry',
}

class Postgres():

    _conn = None
    _logger = None
    _schema = None

    def __init__(self, 
                 table_name, 
                 table_schema, 
                 connection_string, 
                 s3_bucket, 
                 json_schema_s3_key, 
                 s3_key,
                 geom_field=None,
                 geom_type=None):
        self.table_name = table_name
        self.table_schema = table_schema
        self.connection_string = connection_string
        self.s3_bucket = s3_bucket
        self.json_schema_s3_key = json_schema_s3_key
        self.s3_key = s3_key
        self.geom_field = geom_field

    @property
    def table_schema_name(self):
        # schema.tablehealth__child_blood_lead_levels_by_zip
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
        if self.json_schema_s3_key is None:
            json_schema_file_name = None
        elif ('/') in self.json_schema_s3_key:
            json_schema_file_name = self.json_schema_s3_key.split('/')[1]
        else:
            json_schema_file_name = self.json_schema_s3_key
        return json_schema_file_name

    @property
    def json_schema_path(self):
        if self.json_schema_file_name == None:
            json_schema_path = None
        # On Windows, save to current directory
        elif os.name == 'nt':
            json_schema_path = self.json_schema_file_name
        # On Linux, save to tmp folder
        else:
            json_schema_directory = os.path.join('/tmp')
            json_schema_path = os.path.join(json_schema_directory, self.json_schema_file_name)
        return json_schema_path

    @property
    def conn(self):
        if self._conn is None:
            self.logger.info('Trying to connect to postgres...')
            conn = psycopg2.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to postgres.')
        return self._conn

    @property
    def geom_field(self):
        return self._geom_field
    # Seperate out our property's setter method so we're not repeatedly making this db call
    # should only get called once.
    @geom_field.setter
    def geom_field(self, value):
        geom_stmt = f'''
        SELECT f_geometry_column AS column_name
        FROM geometry_columns WHERE f_table_name = '{self.table_name}' and f_table_schema = '{self.table_schema}'
        '''
        #self._geom_field = self.execute_sql(geom_stmt, fetch='one')[0]
        result = self.execute_sql(geom_stmt, fetch='one')
        if result == None:
            self._geom_field = None
        else:
            self._geom_field = result[0]

    @property
    def geom_type(self):
        return self._geom_type
    # Seperate out our property's setter method so we're not repeatedly making this db call
    # should only get called once.
    @geom_type.setter
    def geom_type(self, value):
        geom_stmt = f'''
        SELECT geometry_type({self.table_schema}, {self.table_name}, {self.geom_field})
        '''
        result = self.execute_sql(geom_stmt, fetch='one')
        if result == None:
            self._geom_type = None
        else:
            self._geom_type = result[0]

    # not currently used, getting SRID from the csv
    #@property
    #def geom_srid(self):
    #    if self._geom_srid is None:
    #        with open(self.json_schema_path) as json_file:
    #            schema = json.load(json_file).get('fields', None)
    #            if not schema:
    #                self.logger.error('Json schema malformatted...')
    #                raise
    #            for scheme in schema:
    #                scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
    #                if scheme_type == 'geometry':
    #                    geom_srid = scheme.get('srid', None)
    #                    self._geom_srid = geom_srid
    #    return self._geom_srid

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
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

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

    def get_json_schema_from_s3(self):
        self.logger.info('Fetching json schema: s3://{}/{}'.format(self.s3_bucket, self.json_schema_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(self.json_schema_path)

        self.logger.info('Json schema successfully downloaded.\n'.format(self.s3_bucket, self.json_schema_s3_key))

    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.s3_key))


    def create_indexes(self, table_name):
        raise NotImplementedError


    def write(self):
        self.get_csv_from_s3()
        # self.get_json_schema_from_s3()
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:    
            self.logger.info("Exception encountered trying to load rows with utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        if self.geom_field is not None:
            # Multi-geom fix
            # ESRI seems to only store polygon feature clasess as only multipolygons,
            # so we need to convert all polygon datasets to multipolygon for a successful copy_export.
            # 1) identify if multi in geom_field AND non-multi
            # Grab the geom type in a wierd way for all rows and insert into new column
            self.logger.info(self.geom_field)
            rows = rows.addfield('row_geom_type', lambda a: a[f'{self.geom_field}'].split('(')[0].split(';')[1].replace(' ', ''))
            # 2) Update geom_field "POLYGON" type values to "MULTIPOLYGON":
            #    Also add a third paranthesis around the geom info to make it a MUTLIPOLYGON type
            rows = rows.convert(self.geom_field, lambda u, row: u.replace(row.row_geom_type, 'MULTI' + row.row_geom_type + '(' ) + ')' if row.row_geom_type == 'POLYGON' else u, pass_row=True)
            # Remove our temporary column
            rows = rows.cutout('row_geom_type')


        # Grab our header string for the copy_stmt beflow
        header = rows[0]
        str_header = ''
        num_fields = len(header)
        self._num_rows_in_upload_file = rows.nrows()
        for i, field in enumerate(header):
            if i < num_fields - 1:
                str_header += field + ', '
            else:
                str_header += field

        # Workaround: oracle allows special characters into it's column names
        # We copy the oracle schema to postgres with ArcPy which at least for 
        # the character '#' replaces it with an '_'. So do that here.
        str_header = str_header.replace('#', '_')

        self.logger.info(str_header)
        self.logger.info('Writing to table: {}...'.format(self.table_schema_name))

        # Write our possibly modified lines into the temp_csv file
        write_file = self.temp_csv_path
        rows.tocsv(write_file)
        #self.logger.info("DEBUG Rows: " + str(etl.look(rows)))

        with open(write_file, 'r') as f:
            with self.conn.cursor() as cursor:
                copy_stmt = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
                            table_name=self.table_schema_name, header=str_header)
                self.logger.info('copy_stmt: ' + copy_stmt)
                cursor.copy_expert(copy_stmt, f)

        check_load_stmt = "SELECT COUNT(*) FROM {table_name}".format(table_name=self.table_schema_name)
        response = self.execute_sql(check_load_stmt, fetch='one')

        self.logger.info('Postgres Write Successful: {} rows imported.\n'.format(response[0]))


    def get_geom_field(self):
        """Not currently implemented. Relying on csv to be extracted by geopetl fromoraclesde with geom_with_srid = True"""
        # with open(self.json_schema_path) as json_file:
        #     schema = json.load(json_file).get('fields', '')
        #     if not schema:
        #         self.logger.error('json schema malformatted...')
        #         raise
        #     for scheme in schema:
        #         scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
        #         if scheme_type == 'geometry':
        #             geom_srid = scheme.get('srid', '')
        #             geom_field = scheme.get('name', '')
        #
        raise NotImplementedError

    def verify_count(self):
        self.logger.info('Verifying row count...')

        data = self.execute_sql('SELECT count(*) FROM {};'.format(self.table_schema_name), fetch='many')
        num_rows_in_table = data[0][0]
        num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
        # Postgres doesn't count the header
        num_rows_expected = self._num_rows_in_upload_file
        message = '{} - expected rows: {} inserted rows: {}.'.format(
            self.table_schema_name,
            num_rows_expected,
            num_rows_inserted
        )
        self.logger.info(message)
        if num_rows_in_table != num_rows_expected:
            self.logger.error('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
                    'DROP TABLE if exists {} cascade;'.format(self.table_schema_name) + \
                    'COMMIT;'
            self.execute_sql(stmt)
            exit(1)

    def vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.table_schema_name))

        # An autocommit connection is needed for vacuuming for psycopg2
        # https://stackoverflow.com/questions/1017463/postgresql-how-to-run-vacuum-from-code-outside-transaction-block
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.execute_sql('VACUUM ANALYZE {};'.format(self.table_schema_name))
        self.conn.set_isolation_level(old_isolation_level)
        
        self.logger.info('Vacuum analyze complete.\n')

    def cleanup(self):
        self.logger.info('Attempting to drop temp files...')
        for f in [self.csv_path, self.temp_csv_path, self.json_schema_path]:
            if f is not None:
                if os.path.isfile(f):
                    try:
                        os.remove(f)
                    except Exception as e:
                        self.logger.info(f'Failed removing file {f}.')
                        pass



    def load(self):
        try:
            self.write()
            self.conn.commit()
            self.verify_count()
            self.vacuum_analyze()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed...')
            self.logger.error(f'Error: {str(e)}')
            self.conn.rollback()
            raise e
        finally:
            self.cleanup()


    def load_csv_to_s3(self):
        self.logger.info('Starting load to s3: {}'.format(self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))
        
        self.logger.info('Successfully loaded to s3: {}'.format(self.s3_key))


    def extract_verify_row_count(self):
        with open(self.csv_path, 'r') as file:
            for csv_count, line in enumerate(file):
                pass
        data = self.execute_sql('SELECT count(*) FROM {};'.format(self.table_schema_name), fetch='many')
        postgres_table_count = data[0][0]
        self.logger.info(f'Asserting counts match up: {csv_count} == {postgres_table_count}')
        assert csv_count == postgres_table_count


    def extract(self):
        rows = etl.frompostgis(self.conn, self.table_schema_name, geom_with_srid=True)
        # Dump to our CSV temp file
        print('Extracting csv...')
        try:
            rows.tocsv(self.csv_path, 'utf-8')
        except UnicodeError:
            self.logger.info("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
            rows.tocsv(self.csv_path, 'latin-1')
        self.extract_verify_row_count()
        self.load_csv_to_s3()

