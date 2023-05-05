import sys
import os
import json
import psycopg2
import logging
from .postgres_map import DATA_TYPE_MAP, GEOM_TYPE_MAP

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

def get_geom_field(self):
    """Not currently implemented. Relying on csv to be extracted by geopetl fromoraclesde with geom_with_srid = True"""
    raise NotImplementedError
