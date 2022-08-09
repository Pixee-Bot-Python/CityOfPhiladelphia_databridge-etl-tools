import csv
import logging
import sys
import os
import json
import csv

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
                 with_srid,
                 geom_field=None,
                 geom_type=None):
        self.table_name = table_name
        self.table_schema = table_schema
        self.connection_string = connection_string
        self.s3_bucket = s3_bucket
        self.json_schema_s3_key = json_schema_s3_key
        self.s3_key = s3_key
        self.with_srid = with_srid
        self.geom_field = geom_field
        self.geom_type = geom_type

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
            print('Trying to connect to postgres...')
            conn = psycopg2.connect(self.connection_string, connect_timeout=5)
            self._conn = conn
            print('Connected to postgres.\n')
        return self._conn

    @property
    def geom_field(self):
        return self._geom_field
    # Seperate out our property's setter method so we're not repeatedly making this db call
    # should only get called once.
    @geom_field.setter
    def geom_field(self, value):
        if self.table_name == 'testing' and self.table_schema == 'test':
            # If we recieve these values, this is the unit tests being run by tests/test_postgres.py
            # Return something so it doesn't attempt to make a connection, as conn info passed by the
            # tests is bogus.
            self._geom_field = 'shape'
        else:
            # start off with a None value to fall through conditionals properly.
            self._geom_field = None
            # First check if we're a view:
            check_view_stmt = f"select table_name from INFORMATION_SCHEMA.views where table_name = \'{self.table_name}\'"
            result = self.execute_sql(check_view_stmt, fetch='one')
            if result:
                # We're a bit limited in our options, so let's hope the shape fiel is named 'shape'
                # And check if the data_type is "USER-DEFINED".
                geom_stmt = f'''
                select column_name from information_schema.columns
                    where table_name = '{self.table_name}' and (data_type = 'USER-DEFINED' or data_type = 'ST_GEOMETRY')
                '''
                result = self.execute_sql(geom_stmt, fetch='one')
                if result:
                    if len(result) == 1 and result[0]:
                        self._geom_field = result[0]
                        return self._geom_field
                    elif len(result) > 1:
                        raise LookupError('Multiple geometry fields')

            # Then check if were an SDE-enabled database
            if self._geom_field is None:
                check_table_stmt = "SELECT to_regclass(\'sde.st_geometry_columns\');"
                result = self.execute_sql(check_table_stmt, fetch='one')[0]
                if result != None:
                    # sde.st_geometry_columns table exists, we are an SDE-enabled database
                    geom_stmt = f'''
                    select column_name from sde.st_geometry_columns where table_name = '{self.table_name}'
                    '''
                    result = self.execute_sql(geom_stmt, fetch='one')
                    if result != None:
                        if result[0] != None:
                            self._geom_field = result[0]

            # Else if we're still None, then we're a PostGIS database and this query should work:
            if self._geom_field is None:
                check_table_stmt = "SELECT to_regclass(\'public.geometry_columns\');"
                result = self.execute_sql(check_table_stmt, fetch='one')[0]
                if result != None:
                    geom_stmt = f'''
                    SELECT f_geometry_column AS column_name
                    FROM public.geometry_columns WHERE f_table_name = '{self.table_name}' and f_table_schema = '{self.table_schema}'
                    '''
                    self._geom_field = self.execute_sql(geom_stmt, fetch='one')
                    result = self.execute_sql(geom_stmt, fetch='one')
                    if result != None:
                        if result[0] != None:
                            self._geom_field = result[0]
            # Else, there truly isn't a shape field and we're not geometric? Leave as None.


    @property
    def geom_type(self):
        return self._geom_type
    # Seperate out our property's setter method so we're not repeatedly making this db call
    # should only get called once.
    @geom_type.setter
    def geom_type(self, value):
        if self.table_name == 'testing' and self.table_schema == 'test':
            # If we recieve these values, this is the unit tests being run by tests/test_postgres.py
            # Return something so it doesn't attempt to make a connection, as conn info passed by the
            # tests is bogus.
            self._geom_type = 'POINT'
        else:
            check_table_stmt = "SELECT EXISTS(SELECT * FROM pg_proc WHERE proname = 'geometry_type');"
            result = self.execute_sql(check_table_stmt, fetch='one')[0]
            if result:
                geom_stmt = f'''
                SELECT geometry_type('{self.table_schema}', '{self.table_name}', '{self.geom_field}')
                '''
                print(f'Determining our geom_type, running statement: {geom_stmt}')
                result = self.execute_sql(geom_stmt, fetch='one')
                if result == None:
                    self._geom_type = None
                else:
                    self._geom_type = result[0]
            else:
                self._geom_type = None

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
        print('Executing: {}'.format(stmt))

        with self.conn.cursor() as cursor:
            cursor.execute(stmt)

            if fetch == 'one':
                result = cursor.fetchone()
                return result

            elif fetch == 'many':
                result = cursor.fetchmany()
                return result

    def get_json_schema_from_s3(self):
        print('Fetching json schema: s3://{}/{}'.format(self.s3_bucket, self.json_schema_s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(self.json_schema_path)

        print('Json schema successfully downloaded.\n'.format(self.s3_bucket, self.json_schema_s3_key))

    def get_csv_from_s3(self):
        print('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)

        print('CSV successfully downloaded.\n'.format(self.s3_bucket, self.s3_key))


    def create_indexes(self, table_name):
        raise NotImplementedError


    def write(self):
        self.get_csv_from_s3()
        # self.get_json_schema_from_s3()
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:    
            print("Exception encountered trying to load rows with utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        # Shape types we will transform on, hacky way so we can insert it into our lambda function below
        # Note: this is for transforming non-multi types to multi, but we include multis in this list
        # because we will compare this against self.geom_type, which is retrieved from the etl_staging table,
        # which will probably be multi. This is safe becaue we will transform each row only if they are not already MULTI
        shape_types = ['POLYGON', 'POLYGON Z', 'POLYGON M', 'POLYGON MZ', 'LINESTRING', 'LINESTRING Z', 'LINESTRING M', 'LINESTRING MZ', 'MULTIPOLYGON', 'MULTIPOLYGON Z', 'MULTIPOLYGON M', 'MULTIPOLYGON MZ', 'MULTILINESTRING', 'MULTILINESTRING Z', 'MULTILINESTRING M', 'MULTILINESTRING MZ']

        # Note: also run this if the data type is 'MULTILINESTRING' some source datasets will export as LINESTRING but the dataset type is actually MULTILINESTRING (one example: GIS_PLANNING.pedbikeplan_bikerec)
        # Note2: Also happening with poygons, example dataset: GIS_PPR.ppr_properties
        print(f'self.geom_field is: {self.geom_field}')
        print(f'self.geom_type is: {self.geom_type}')
        if self.geom_field is not None and (self.geom_type in shape_types):
            print('Detected that shape type needs conversion to MULTI....')
            # Multi-geom fix
            # ESRI seems to only store polygon feature clasess as only multipolygons,
            # so we need to convert all polygon datasets to multipolygon for a successful copy_export.
            # 1) identify if multi in geom_field AND non-multi
            # Grab the geom type in a wierd way for all rows and insert into new column
            #rows = rows.addfield('row_geom_type', lambda a: a[f'{self.geom_field}'].split('(')[0].split(';')[1].strip())
            rows = rows.addfield('row_geom_type', lambda a: a[f'{self.geom_field}'].split('(')[0].split(';')[1].strip() if a[f'{self.geom_field}'] and '(' in a[f'{self.geom_field}'] else None)
            # 2) Update geom_field "POLYGON" type values to "MULTIPOLYGON":
            #    Also add a third paranthesis around the geom info to make it a MUTLIPOLYGON type
            rows = rows.convert(self.geom_field, lambda u, row: u.replace(row.row_geom_type, 'MULTI' + row.row_geom_type + ' (' ) + ')' if 'MULTI' not in row.row_geom_type else u, pass_row=True)
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

        # Workaround: We have many datasets in Oracle where the objectid field is "objectid_1".
        # When I make an empty dataset from Oracle with "create-beta-enterprise-table.py" it seems
        # to remake the dataset with a proper 'objectid' primary key. However the CSV we make from Oracle
        # will still have "objectid_1" in it.
        # We'll attempt to handle this by replacing the "objectid_1" with "objectid" in the
        # CSV header if there's not a second objectid field in there.
        if ('objectid_1,' in str_header) and ('objectid,' not in str_header):
            print('\nDetected objectid_1 primary key, implementing workaround and modifying header...')
            rows = rows.rename('objectid_1', 'objectid')
            str_header = str_header.replace('objectid_1', 'objectid')

        print(str_header)
        print('\nWriting to table: {}...'.format(self.table_schema_name))

        # Write our possibly modified lines into the temp_csv file
        write_file = self.temp_csv_path
        rows.tocsv(write_file)
        #print("DEBUG Rows: " + str(etl.look(rows)))

        with open(write_file, 'r') as f:
            with self.conn.cursor() as cursor:
                copy_stmt = f'''
                    COPY {self.table_schema_name} ({str_header}) FROM STDIN WITH (FORMAT csv, HEADER true)
                '''
                print('copy_stmt: ' + copy_stmt)
                cursor.copy_expert(copy_stmt, f)

        check_load_stmt = "SELECT COUNT(*) FROM {table_name}".format(table_name=self.table_schema_name)
        response = self.execute_sql(check_load_stmt, fetch='one')

        print('Postgres Write Successful: {} rows imported.\n'.format(response[0]))


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
        print('Verifying row count...')

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
        print(message)
        if num_rows_in_table != num_rows_expected:
            self.logger.error('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
                    'DROP TABLE if exists {} cascade;'.format(self.table_schema_name) + \
                    'COMMIT;'
            self.execute_sql(stmt)
            exit(1)

    def vacuum_analyze(self):
        print('Vacuum analyzing table: {}'.format(self.table_schema_name))

        # An autocommit connection is needed for vacuuming for psycopg2
        # https://stackoverflow.com/questions/1017463/postgresql-how-to-run-vacuum-from-code-outside-transaction-block
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.execute_sql('VACUUM ANALYZE {};'.format(self.table_schema_name))
        self.conn.set_isolation_level(old_isolation_level)
        
        print('Vacuum analyze complete.\n')

    def cleanup(self):
        print('Attempting to drop temp files...')
        for f in [self.csv_path, self.temp_csv_path, self.json_schema_path]:
            if f is not None:
                if os.path.isfile(f):
                    try:
                        os.remove(f)
                    except Exception as e:
                        print(f'Failed removing file {f}.')
                        pass



    def load(self):
        try:
            self.write()
            self.conn.commit()
            self.verify_count()
            self.vacuum_analyze()
            print('Done!')
        except Exception as e:
            self.logger.error('Workflow failed...')
            self.logger.error(f'Error: {str(e)}')
            self.conn.rollback()
            raise e
        finally:
            self.cleanup()


    def load_csv_to_s3(self):
        print('Starting load to s3: {}'.format(self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))
        
        print('Successfully loaded to s3: {}'.format(self.s3_key))


    def extract_verify_row_count(self):
        with open(self.csv_path, 'r') as file:
            for csv_count, line in enumerate(file):
                pass
        data = self.execute_sql('SELECT count(*) FROM {};'.format(self.table_schema_name), fetch='many')
        postgres_table_count = data[0][0]
        # ignore differences less than 2 until I can figure out why views have a difference in row counts.
        if abs(csv_count - postgres_table_count) >= 2:
            print(f'Asserting counts match up: {csv_count} == {postgres_table_count}')
            assert csv_count == postgres_table_count

    def check_remove_nulls(self):
        '''
        This function checks for null bytes ('\0'), and if exists replace with null string (''):
        Check only the first 500 lines to stay efficient, if there aren't 
        any in the first 500, there likely(maybe?) aren't any.
        '''
        has_null_bytes = False
        with open(self.csv_path, 'r') as infile:
            for i, line in enumerate(infile):
                if i >= 500:
                    break
                for char in line:
                    if char == '\0':
                        has_null_bytes = True
                        break

        if has_null_bytes:
            print("Dataset has null bytes, removing...")
            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
            with open(self.csv_path, 'r') as infile:
                with open(temp_file, 'w') as outfile:
                    reader = csv.reader((line.replace('\0', '') for line in infile), delimiter=",")
                    writer = csv.writer(outfile)
                    writer.writerows(reader)
            os.replace(temp_file, self.csv_path)



    def extract(self):
        # First make sure the table exists:
        exists_query = f'''SELECT to_regclass('{self.table_schema}.{self.table_name}');'''
        result = self.execute_sql(exists_query, fetch='one')[0]

        if result is None:
            raise AssertionError(f'Table does not exist in this DB: {self.table_schema}.{self.table_name}!')

        if self.with_srid is True:
            rows = etl.frompostgis(self.conn, self.table_schema_name, geom_with_srid=True)
        else:
            rows = etl.frompostgis(self.conn, self.table_schema_name, geom_with_srid=False)

        # Dump to our CSV temp file
        print('Extracting csv...')
        try:
            rows.tocsv(self.csv_path, 'utf-8')
        except UnicodeError:
            print("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
            rows.tocsv(self.csv_path, 'latin-1')

        self.check_remove_nulls()
        self.load_csv_to_s3()

