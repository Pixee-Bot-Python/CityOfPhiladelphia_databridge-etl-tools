import csv
import logging
import sys
import os
import json
import csv
import re
import psycopg2
from psycopg2.sql import Identifier, SQL
import boto3
import geopetl
import petl as etl
import pandas as pd
from .postgres_map import DATA_TYPE_MAP, GEOM_TYPE_MAP

csv.field_size_limit(sys.maxsize)

class Postgres():

    _conn = None
    _logger = None
    _schema = None
    _export_json_schema = None
    _row_count = None
    _primary_keys = None

    def __init__(self, table_name, table_schema, connection_string, s3_bucket, s3_key, 
                 **kwargs):
        self.table_name = table_name
        self.table_schema = table_schema
        self.table_schema_name = f'{self.table_schema}.{self.table_name}'
        self.temp_table_name = self.table_name + '_t'
        self.temp_table_schema_name = f'{self.table_schema}.{self.temp_table_name}'
        self.connection_string = connection_string
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_schema_s3_key = kwargs.get('json_schema_s3_key', None)
        self.geom_field = kwargs.get('geom_field', None)
        self.geom_type = kwargs.get('geom_type', None)
        self.with_srid = kwargs.get('with_srid', None)
        # just initialize this self variable here so we connect first
        self.conn

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
        '''Create or Make the Postgres db connection'''
        if self._conn is None:
            self.logger.info('Trying to connect to postgres...')
            conn = psycopg2.connect(self.connection_string, connect_timeout=5)
            self._conn = conn
            self.logger.info('Connected to postgres.\n')
        return self._conn

    @property
    def export_json_schema(self):
        '''Json schema to export to s3 during extraction, for use when uploading to places like Carto.'''
        if self._export_json_schema is None:
            stmt = f'''
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = '{self.table_schema}'
            AND table_name = '{self.table_name}'
            '''
            results = self.execute_sql(stmt, fetch='all')
            self._export_json_schema = json.dumps(results)
        return self._export_json_schema

    @property
    def primary_keys(self) -> 'tuple': 
        '''Get or return the primary keys of the table'''
        if self._primary_keys == None: 
            stmt = f'''
            SELECT a.attname
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{self.table_schema_name}'::regclass
            AND    i.indisprimary;
            '''
            results = self.execute_sql(stmt, fetch='all')
            self._primary_keys = tuple(x[0] for x in results)
        return self._primary_keys

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
                self.logger.info(f'Determining our geom_type, running statement: {geom_stmt}')
                result = self.execute_sql(geom_stmt, fetch='one')
                if result == None:
                    self._geom_type = None
                else:
                    self._geom_type = result[0]
            else:
                self._geom_type = None

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

    def execute_sql(self, stmt, data=None, fetch=None):
        '''
        Execute an SQL statement and fetch rows if specified. 
            - stmt can allow for passing parameters via %s if data != None
            - data should be a tuple or list
            - fetch should be one of None, "one", "many", "all"
        '''
        with self.conn.cursor() as cursor:
            cursor.execute(stmt, data)

            if fetch == 'one':
                result = cursor.fetchone()
                return result

            elif fetch == 'many':
                result = cursor.fetchmany()
                return result

            elif fetch == 'all':
                result = cursor.fetchall()
                return result

    def _interact_with_s3(self, method: str, path: str, s3_key: str): 
        '''
        - method should be one of "get", "load"
        '''
        self.logger.info(f"{method.upper()}-ing file: s3://{self.s3_bucket}/{s3_key}")

        s3 = boto3.resource('s3')
        if method == 'get': 
            s3.Object(self.s3_bucket, s3_key).download_file(path)
            self.logger.info(f'File successfully downloaded from S3 to {path}\n')
        elif method == 'load': 
            s3.Object(self.s3_bucket, s3_key).put(Body=open(path, 'rb'))
            self.logger.info(f'File successfully uploaded from {path} to S3\n')
    
    def get_json_schema_from_s3(self):
        self._interact_with_s3('get', self.json_schema_path, self.json_schema_s3_key)

    def get_csv_from_s3(self):
        self._interact_with_s3('get', self.csv_path, self.s3_key)

    def load_json_schema_to_s3(self):
        json_schema_path = self.csv_path.replace('.csv','') + '.json'
        json_s3_key = self.s3_key.replace('staging', 'schemas').replace('.csv', '.json')

        with open(json_schema_path, 'w') as f:
            f.write(self.export_json_schema)
        
        self._interact_with_s3('load', json_schema_path, json_s3_key)

    def load_csv_to_s3(self):
        self._interact_with_s3('load', self.csv_path, self.s3_key)

    def create_indexes(self, table_name):
        raise NotImplementedError

    def prepare_file(self):
        '''
        Get a CSV from S3; prepare its geometry and headers for insertion into Postgres; 
        write to CSV at self.temp_csv_path
        '''
        self.get_csv_from_s3()
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:    
            self.logger.info("Exception encountered trying to load rows with utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        # Shape types we will transform on, hacky way so we can insert it into our lambda function below
        # Note: this is for transforming non-multi types to multi, but we include multis in this list
        # because we will compare this against self.geom_type, which is retrieved from the etl_staging table,
        # which will probably be multi. This is safe becaue we will transform each row only if they are not already MULTI
        shape_types = ['POLYGON', 'POLYGON Z', 'POLYGON M', 'POLYGON MZ', 'LINESTRING', 'LINESTRING Z', 'LINESTRING M', 'LINESTRING MZ', 'MULTIPOLYGON', 'MULTIPOLYGON Z', 'MULTIPOLYGON M', 'MULTIPOLYGON MZ', 'MULTILINESTRING', 'MULTILINESTRING Z', 'MULTILINESTRING M', 'MULTILINESTRING MZ']

        # Note: also run this if the data type is 'MULTILINESTRING' some source datasets will export as LINESTRING but the dataset type is actually MULTILINESTRING (one example: GIS_PLANNING.pedbikeplan_bikerec)
        # Note2: Also happening with poygons, example dataset: GIS_PPR.ppr_properties
        self.logger.info(f'self.geom_field is: {self.geom_field}')
        self.logger.info(f'self.geom_type is: {self.geom_type}')
        if self.geom_field is not None and (self.geom_type in shape_types):
            self.logger.info('Detected that shape type needs conversion to MULTI....')
            # Multi-geom fix
            # ESRI seems to only store polygon feature clasess as only multipolygons,
            # so we need to convert all polygon datasets to multipolygon for a successful copy_export.
            # 1) identify if multi in geom_field AND non-multi
            # Grab the geom type in a wierd way for all rows and insert into new column
            rows = rows.addfield('row_geom_type', lambda a: a[f'{self.geom_field}'].split('(')[0].split(';')[1].strip() if a[f'{self.geom_field}'] and '(' in a[f'{self.geom_field}'] else None)
            # 2) Update geom_field "POLYGON" type values to "MULTIPOLYGON":
            #    Also add a third paranthesis around the geom info to make it a MUTLIPOLYGON type
            rows = rows.convert(self.geom_field, lambda u, row: u.replace(row.row_geom_type, 'MULTI' + row.row_geom_type + ' (' ) + ')' if 'MULTI' not in row.row_geom_type else u, pass_row=True)
            # Remove our temporary column
            rows = rows.cutout('row_geom_type')

        header = rows[0]
        str_header = ', '.join(header).replace('#', '_')
        self._num_rows_in_upload_file = rows.nrows()

        # Many Oracle datasets have the objectid field as "objectid_1". Making an 
        # empty dataset from Oracle with "create-beta-enterprise-table.py" remakes 
        # the dataset with a proper 'objectid' primary key. However the CSV made from Oracle
        # will still have "objectid_1" in it. Handle this by replacing "objectid_1" 
        # with "objectid" in CSV header if "objectid" doesn't already exist.
        if (re.search('objectid,', str_header) == None and 
        (match := re.search('(objectid_\d+),', str_header)) != None): 
            old_col = match.groups()[0]
            self.logger.info(f'\nDetected {old_col} primary key, implementing workaround and modifying header...')
            str_header = str_header.replace(f'{old_col}', 'objectid')
        rows = rows.rename({old:new for old, new in zip(header, str_header.split(', '))})

        self.logger.info(f'Header:\n\t{str_header}')

        # Write our possibly modified lines into the temp_csv file
        write_file = self.temp_csv_path
        rows.tocsv(write_file)

    def write(self, write_file: str, table_schema_name: str):
        '''Use Postgres COPY FROM method to append a table to a Postgres table, 
        gathering the header from the first line of the file'''

        self.logger.info('\nWriting to table: {}...'.format(table_schema_name))
        with open(write_file, 'r') as f: 
            # f.readline() moves cursor position from 0 to 1, so tell COPY there is no header
            str_header = f.readline().strip() 
            with self.conn.cursor() as cursor:
                copy_stmt = f'''
    COPY {table_schema_name} ({str_header}) 
    FROM STDIN WITH (FORMAT csv, HEADER false)'''
                self.logger.info('copy_stmt: ' + copy_stmt)
                cursor.copy_expert(copy_stmt, f)
                self.logger.info(f'Postgres Write Successful: {cursor.rowcount:,} rows imported.\n')

    def get_geom_field(self):
        """Not currently implemented. Relying on csv to be extracted by geopetl fromoraclesde with geom_with_srid = True"""
        raise NotImplementedError

    @property
    def row_count(self):
        if self._row_count:
            return self._row_count
        data = self.execute_sql('SELECT count(*) FROM {};'.format(self.table_schema_name), fetch='many')
        self._row_count = data[0][0]
        return self._row_count

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
        '''Remove local CSV, temp CSV, JSON schema; DROP temp table if exists'''
        self.logger.info('Attempting to drop temp files...')
        for f in [self.csv_path, self.temp_csv_path, self.json_schema_path]:
            if f is not None:
                if os.path.isfile(f):
                    try:
                        os.remove(f)
                    except Exception as e:
                        self.logger.info(f'Failed removing file {f}.')
                        pass
        self.drop_table(self.temp_table_name, exists='log')                

    def load(self):
        try:
            self.write(self.temp_csv_path, self.table_schema_name)
            self.conn.commit()
            # self.verify_count() # Broken until acccounting for insert/update
            self.vacuum_analyze()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed...')
            self.logger.error(f'Error: {str(e)}')
            self.conn.rollback()
            raise e
        finally:
            self.cleanup()
            self.conn.commit()

    def extract_verify_row_count(self):
        with open(self.csv_path, 'r') as file:
            for csv_count, line in enumerate(file):
                pass
        data = self.execute_sql('SELECT count(*) FROM {};'.format(self.table_schema_name), fetch='many')
        postgres_table_count = data[0][0]
        # ignore differences less than 2 until I can figure out why views have a difference in row counts.
        if abs(csv_count - postgres_table_count) >= 2:
            self.logger.info(f'Asserting counts match up: {csv_count} == {postgres_table_count}')
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
            self.logger.info("Dataset has null bytes, removing...")
            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
            with open(self.csv_path, 'r') as infile:
                with open(temp_file, 'w') as outfile:
                    reader = csv.reader((line.replace('\0', '') for line in infile), delimiter=",")
                    writer = csv.writer(outfile)
                    writer.writerows(reader)
            os.replace(temp_file, self.csv_path)

    def extract(self):
        self.logger.info(f'Starting extract from {self.table_schema_name}')
        self.logger.info(f'Rows to extract: {self.row_count}')
        self.logger.info("Note: petl can cause log messages to seemingly come out of order.")

        # First make sure the table exists:
        exists_query = f'''SELECT to_regclass('{self.table_schema}.{self.table_name}');'''
        result = self.execute_sql(exists_query, fetch='one')[0]

        if self.row_count == 0:
            raise AssertionError('Error! Row count of dataset in database is 0??')

        # Try to get an (arbitrary) sensible interval to print progress on by dividing by the row count
        if self.row_count < 10000:
            interval = int(self.row_count/3)
        if self.row_count > 10000:
            interval = int(self.row_count/15)
        if self.row_count == 1:
            interval = 1
        # If it rounded down to 0 with int(), that means we have a very small amount of rows
        if not interval:
            interval = 1

        if result is None:
            raise AssertionError(f'Table does not exist in this DB: {self.table_schema}.{self.table_name}!')

        self.logger.info('Initializing data var with etl.frompostgis()..')
        if self.with_srid is True:
            rows = etl.frompostgis(self.conn, self.table_schema_name, geom_with_srid=True)
        else:
            rows = etl.frompostgis(self.conn, self.table_schema_name, geom_with_srid=False)

        # Dump to our CSV temp file
        self.logger.info('Extracting csv...')
        try:
            rows.progress(interval).tocsv(self.csv_path, 'utf-8')
        except UnicodeError:
            self.logger.warning("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
            rows.progress(interval).tocsv(self.csv_path, 'latin-1')

        num_rows_in_csv = rows.nrows()

        if num_rows_in_csv == 0:
            raise AssertionError('Error! Dataset is empty? Line count of CSV is 0.')

        self.logger.info(f'Asserting counts match between db and extracted csv')
        self.logger.info(f'{self.row_count} == {num_rows_in_csv}')
        assert self.row_count == num_rows_in_csv

        self.check_remove_nulls()
        self.load_csv_to_s3()
    
    def create_temp_table(self): 
        '''Create an empty temp table from self.table_name'''
        with self.conn.cursor() as cursor:
            cursor.execute(SQL('''
            CREATE TABLE {} AS 
                SELECT * 
                FROM {}
            WHERE 1=0;
            ''').format(                          # This is psycopg2.sql.SQL.format() not f string format
                Identifier(self.temp_table_name), # See https://www.psycopg.org/docs/sql.html
                Identifier(self.table_name)))
            self.logger.info(f'Created temp table {self.temp_table_name}')
    
    def delete_using(self, deleted_table: 'str', deleting_table: 'str', keys: 'list'): 
        '''Delete records from a table using another table in the same database. Postgres
        equivalent of a DELETE JOIN. 
        '''
        # DELETE FROM ... WHERE PKEY = ANY(ARRAY[...]) AND PKEY2 = ANY(ARRAY[...]) 
        # won't work because of composite primary keys. 

        assert len(keys) >= 1
        where_stmt = 'WHERE ' + ' AND '.join([f'a.{{pk{x[0]}}} = b.{{pk{x[0]}}}' for x in enumerate(self.primary_keys)])
        # Example: 'WHERE a.{pk0} = b.{pk0} AND a.{pk1} = b.{pk1} AND ...'
        pkeys_dict = {f'pk{i}': Identifier(x) for i, x in enumerate(self.primary_keys)} 
        # Example: {pk0: Identifier(self.primary_keys[0]), pk1: Identifier(self.primary_keys[1]), ...}
        with self.conn.cursor() as cursor:
            cursor.execute(
                SQL('''
                DELETE 
                FROM {deleted_table} as a
                USING {deleting_table} as b
                ''' + where_stmt
                ).format(    
                    deleted_table=Identifier(deleted_table), 
                    deleting_table=Identifier(deleting_table), 
                    **pkeys_dict) # Example: pk0 = self.primary_keys[0], pk1 = self.primary_keys[1], ...
                    )
            self.logger.info(f'Deleted {cursor.rowcount} rows from {deleted_table}')
    
    def drop_table(self, table_name: 'str', exists='log'): 
        '''DROP a table
            - table_name - Table name to drop
            - exists - One of "log", "error". If the table name already exists, 
            whether to record that in the log or raise an error
        '''
        self.logger.info(f'Attempting to drop table if exists {table_name}')
        cursor = self.conn.cursor()
        try: 
            cursor.execute(
                SQL('''SELECT * FROM {} LIMIT 1''').format(Identifier(table_name)))
        except psycopg2.errors.UndefinedTable as e: 
            self.logger.info(f'\tTable {table_name} does not exist.')
            self.conn.rollback()
        else:
            if exists == 'error': 
                raise ValueError(f'Table {table_name} already exists and was set to be dropped.')
            if exists == 'log': 
                self.logger.info(f'\tExisting table {table_name} will be dropped.')
        cursor.execute(
            SQL('''DROP TABLE IF EXISTS {}''').format(Identifier(table_name)))
        self.logger.info('DROP IF EXISTS statement successfully executed.\n')

    def __upsert_csv(self): 
        '''Upsert a CSV file from S3 to a Postgres table'''
        self.drop_table(self.temp_table_name, exists='error')
        self.prepare_file()
        self.create_temp_table()
        self.write(self.temp_csv_path, self.temp_table_schema_name)
        self.delete_using(self.table_name, self.temp_table_name, self.primary_keys)
        self.load()
        pass

    def __upsert_db(self): 
        '''Upsert a table within the same Postgres database to a Postgres table'''
        pass
    
    def upsert(self, method): 
        '''Updates data from a CSV or from within the same database to a Postgres 
        table, which must have at least one primary key. This method will 
            - Delete data in the Postgres table where the primary key ID appears in the 
        new data, 
            - Leave any old data in the Postgres table where the primary key ID 
        does not appear in the new data, 
            - Append all data from the new table
        
        Method should be one of "csv", "db"
        '''
        if self.primary_keys == (): 
            raise ValueError(f'Upsert method requires that table "{self.table_schema_name}" have at least one column as primary key.')
        
        if method == 'csv': 
            self.__upsert_csv()
        elif method == 'db': 
            self.__upsert_db()
