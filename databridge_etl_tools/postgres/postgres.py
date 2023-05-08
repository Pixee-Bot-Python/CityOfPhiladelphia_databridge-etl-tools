import csv
import sys
import csv
import re
import ast
import psycopg2
from psycopg2.sql import Identifier, SQL
import geopetl
import petl as etl

csv.field_size_limit(sys.maxsize)

class Postgres():
    '''
    A class that encapsulates a connection to a Postgres database and a specific table. 
    The class includes several verification and clean-up methods that generally be invoked 
    when manipulating a table via the form
    
    with Postgres(...) as pg: 
        pg.method(...)
    
    See Postgres.__enter__ and Postgres.__exit__ for details.

    To skip the checks, simply call Postgres(...).method(...)
    '''
    _conn = None
    _logger = None

    from ._properties import (
        conn, csv_path, temp_csv_path, json_schema_file_name, json_schema_path, 
        export_json_schema, primary_keys, geom_field, geom_type, schema, logger, get_geom_field)
    from ._s3 import (
        get_csv_from_s3, get_json_schema_from_s3, load_csv_to_s3, load_json_schema_to_s3)
    from ._cleanup import (
        verify_count, vacuum_analyze, cleanup, check_remove_nulls)

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
        self.start_row_count = 0
        self.inserted_row_count = 0
        self.deleted_row_count = 0
        self.json_schema_s3_key = kwargs.get('json_schema_s3_key', None)
        self.geom_field = kwargs.get('geom_field', None)
        self.geom_type = kwargs.get('geom_type', None)
        self.with_srid = kwargs.get('with_srid', None)
        self._schema = None
        self._export_json_schema = None
        self._primary_keys = None
        # just initialize this self variable here so we connect first
        self.conn

    def __enter__(self):
        '''Context manager functions to be called BEFORE any functions inside
        ```
        with Postgres(...) as pg: 
            ...
        ```
        See https://book.pythontips.com/en/latest/context_managers.html
        '''
        self.start_row_count = self.get_row_count()
        return self
    
    def __exit__(self, type, value, traceback):
        '''Context manager functions to execute AFTER all functions inside 
        ```
        with Postgres(...) as pg: 
            ...
        ```
        '''
        if type == None: # No exception was raised before __exit__()
            try: 
                self.drop_table(self.table_schema, self.temp_table_name, exists='log')   
                self.verify_count(
                    start=self.start_row_count, 
                    inserted=self.inserted_row_count, 
                    deleted=self.deleted_row_count) 
                self.conn.commit()
                self.vacuum_analyze()
                self.logger.info('Done! All transactions committed.\n')
            except Exception as e:
                self.logger.error('Workflow failed... rolling back database transactions.\n')
                self.conn.rollback()
            finally:
                self.cleanup() 
        else: # An exception was raised before __exit__()
            self.logger.error('Workflow failed... rolling back database transactions.\n')
            self.conn.rollback()
            self.cleanup() 

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

    def create_indexes(self, table_name):
        raise NotImplementedError

    def prepare_file(self, file, **kwargs):
        '''
        Prepare a CSV file's geometry and header for insertion into Postgres; 
        write to CSV at self.temp_csv_path. If the keyword arguments "column_mappings" or 
        "mappings_file" are passed with values other than None, no edits are made 
        to the data header.
        '''
        try:
            rows = etl.fromcsv(file, encoding='utf-8')
        except UnicodeError:    
            self.logger.info("Exception encountered trying to load rows with utf-8 encoding, trying latin-1...")
            rows = etl.fromcsv(file, encoding='latin-1')

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
        str_header = ', '.join(header)
        if kwargs.get('column_mappings', None) == None and kwargs.get('mappings_file', None) == None: 
            str_header = str_header.replace('#', '_')

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
        
        # Write our possibly modified lines into the temp_csv file
        write_file = self.temp_csv_path
        rows.tocsv(write_file)

    def _map_header(self, str_header: str, column_mappings:str = None, mappings_file:str = None) -> str: 
        '''Internal function to transform a header according to a mapping'''
        if column_mappings != None: 
            mapping_dict = ast.literal_eval(column_mappings)
        elif mappings_file != None: 
            with open(mappings_file, 'r') as f: 
                mapping_text = f.read()
            mapping_dict = ast.literal_eval(mapping_text)
        assert type(mapping_dict) == dict, 'Column mappings could not be read as a Python dictionary'
        
        mapped_header = []
        header_list = str_header.split(',')
        for h in header_list: 
            if h in mapping_dict: 
                mapped_header.append(mapping_dict[h])
            else: 
                mapped_header.append(h)
        mapped_header = ','.join(mapped_header)
        
        return mapped_header   
    
    def write(self, write_file: 'str', table_schema_name: 'str', column_mappings=None, mappings_file=None):
        '''Use Postgres COPY FROM method to append a table to a Postgres table, 
        gathering the header from the first line of the file. If the write occurs to 
        self.table_name, it remembers the inserted row count. Use column_mappings or 
        mappings_file to specify a mapping of data file columns to database table columns.
        
        - write_file: Path of file for postgresql COPY FROM
        - table_schema_name: Destination table
        - column_mappings: A string that can be read as a dictionary using `ast.literal_eval()`. 
            - It should take the form '{"data_col": "db_table_col", "data_col2": "db_table_col2", ...}'
            - Note the quotes around the curly braces `'{}'` because it is a string
        - mappings_file: A text file (not Python file) that can be opened with open() 
        and that contains one Python dictionary that can be read with `ast.literal_eval()`
            - The file should take the form {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }
            - Note no quotes around the curly braces `{}`. 
    
        Only one of column_mappings or mappings_file should be provided. Note that only the columns whose headers differ between the data file and the database table need to be included. All column names must be quoted. While this method can be called directly, it is preferable to call load() if possible instead.
        '''

        self.logger.info('Writing to table: {}...'.format(table_schema_name))
        with open(write_file, 'r') as f: 
            # f.readline() moves cursor position out of position
            str_header = f.readline().strip() 
            if column_mappings != None or mappings_file != None: 
                str_header = self._map_header(str_header, column_mappings, mappings_file)
            self.logger.info(f'Header:\n\t{str_header}')
            f.seek(0)
            with self.conn.cursor() as cursor:
                copy_stmt = f'''
    COPY {table_schema_name} ({str_header}) 
    FROM STDIN WITH (FORMAT csv, HEADER true)'''
                self.logger.info('copy_stmt: ' + copy_stmt)
                cursor.copy_expert(copy_stmt, f)
                if table_schema_name == self.table_schema_name: 
                    self.inserted_row_count += cursor.rowcount
                    self.logger.info(f'{self.table_schema_name} inserted row count updated')
                self.logger.info(f'Postgres Write Successful: {cursor.rowcount:,} rows imported.\n')

    def get_row_count(self):
        '''Get the current table row count. Don't make this a property because row counts change.'''
        data = self.execute_sql(f'SELECT count(*) FROM {self.table_schema_name};', fetch='many')
        count = data[0][0]
        self.logger.info(f'{self.table_schema_name} current row count: {count:,}\n')
        return count

    def extract(self):
        self.logger.info(f'Starting extract from {self.table_schema_name}')
        self.logger.info(f'Rows to extract: {self.get_row_count()}')
        self.logger.info("Note: petl can cause log messages to seemingly come out of order.")

        # First make sure the table exists:
        exists_query = f'''SELECT to_regclass('{self.table_schema}.{self.table_name}');'''
        result = self.execute_sql(exists_query, fetch='one')[0]

        if self.get_row_count() == 0:
            raise AssertionError('Error! Row count of dataset in database is 0??')

        # Try to get an (arbitrary) sensible interval to print progress on by dividing by the row count
        if self.get_row_count() < 10000:
            interval = int(self.get_row_count()/3)
        if self.get_row_count() > 10000:
            interval = int(self.get_row_count()/15)
        if self.get_row_count() == 1:
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
        self.logger.info(f'{self.get_row_count()} == {num_rows_in_csv}')
        assert self.get_row_count() == num_rows_in_csv

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
            self.logger.info(f'Created temp table {self.temp_table_name}\n')
    
    def delete_using(self, deleted_table: 'str', deleting_table: 'str', keys: 'list'): 
        '''Delete records from a table using another table in the same database. Postgres
        equivalent of a DELETE JOIN. Remembers the number of deleted rows.
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
                delete_statement := 
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
            print(f'delete_statement:{cursor.mogrify(delete_statement).decode()}')
            if deleted_table == self.table_name: 
                self.deleted_row_count += cursor.rowcount
                self.logger.info(f'{self.table_schema_name} deleted row count updated')
            self.logger.info(f'Deleted {cursor.rowcount} rows from {deleted_table}\n')
    
    def drop_table(self, schema: str, table_name: 'str', exists='log'): 
        '''DROP a table
            - schema - Schema of table to drop
            - table_name - Table name to drop
            - exists - One of "log", "error". If the table name already exists, 
            whether to record that in the log or raise an error
        '''
        self.logger.info(f'Attempting to drop table if exists {table_name}')
        cursor = self.conn.cursor()
        cursor.execute('''
    SELECT EXISTS (
        SELECT FROM pg_tables
        WHERE  schemaname = %s
        AND    tablename  = %s
    )''', (schema, table_name))
        rv = cursor.fetchone()[0]
        if rv == False:         
            self.logger.info(f'\tTable {table_name} does not exist.\n')
            return None
        elif rv == True: 
            if exists == 'error': 
                raise ValueError(f'Table {table_name} already exists and was set to be dropped.')
            if exists == 'log': 
                self.logger.info(f'\tExisting table {table_name} will be dropped.')
        else:
            raise ValueError('Query return value not boolean')
        cursor.execute(
            SQL('''DROP TABLE IF EXISTS {}''').format(Identifier(table_name)))
        self.logger.info('DROP IF EXISTS statement successfully executed.\n')

    def load(self, **kwargs):
        '''
        Prepare and COPY a CSV from S3 to a Postgres table. If the keyword arguments "column_mappings" or "mappings_file" are passed with values other than None, those mappings are used to map data file columns to database table colums.

        - column_mappings: A string that can be read as a dictionary using `ast.literal_eval()`. 
            - It should take the form '{"data_col": "db_table_col", "data_col2": "db_table_col2", ...}'
            - Note the quotes around the curly braces `'{}'` because it is a string
        - mappings_file: A text file that can be opened with open() and that contains one Python dictionary that can be read with `ast.literal_eval()`
            - The file should take the form {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }
            - Note no quotes around the curly braces `{}`. 
    
        Only one of column_mappings or mappings_file should be provided. Note that only the columns whose headers differ between the data file and the database table need to be included. All column names must be quoted. While this method can be called directly, it is preferable to call load() if possible instead.
        '''
        self.get_csv_from_s3()
        self.prepare_file(file=self.csv_path, **kwargs)
        self.write(self.temp_csv_path, self.table_schema_name, **kwargs)

    def __upsert_csv(self, **kwargs): 
        '''Upsert a CSV file from S3 to a Postgres table'''
        self.drop_table(self.table_schema, self.temp_table_name, exists='error')
        self.get_csv_from_s3()
        self.prepare_file(file=self.csv_path, **kwargs)
        self.create_temp_table()
        self.write(self.temp_csv_path, self.temp_table_schema_name, **kwargs)
        self.delete_using(self.table_name, self.temp_table_name, self.primary_keys)
        self.write(self.temp_csv_path, self.table_schema_name, **kwargs)

    def __upsert_table(self, **kwargs): 
        '''Upsert a table within the same Postgres database to a Postgres table'''
        pass
    
    def upsert(self, method, **kwargs): 
        '''Upserts data from a CSV or from a table within the same database to a 
        Postgres table, which must have at least one primary key. If the keyword arguments 
        "column_mappings" or "mappings_file" are passed with values other than None, 
        those mappings are used to map data file columns to database table colums.
        
        - method: Indicates the source type. Should be one of "csv", "table".
        - column_mappings: A string that can be read as a dictionary using `ast.literal_eval()`. 
            - It should take the form '{"data_col": "db_table_col", "data_col2": "db_table_col2", ...}'
            - Note the quotes around the curly braces `'{}'` because it is a string
        - mappings_file: A text file (not Python file) that can be opened with open() 
        and that contains one Python dictionary that can be read with `ast.literal_eval()`
            - The file should take the form {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }
            - Note no quotes around the curly braces `{}`. 
    
        Only one of column_mappings or mappings_file should be provided. Note that only the columns whose headers differ between the data file and the database table need to be included. All column names must be quoted. 
        '''
        self.logger.info(f'{"*" * 80}\nUpserting into {self.table_schema_name}\n')
        if self.primary_keys == (): 
            raise ValueError(f'Upsert method requires that table "{self.table_schema_name}" have at least one column as primary key.')
        
        if method == 'csv': 
            self.__upsert_csv(**kwargs)
        elif method == 'table': 
            self.__upsert_table(**kwargs)
