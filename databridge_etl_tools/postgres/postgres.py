import csv, sys, re, ast
import psycopg2.sql as sql
import geopetl
import pytz
import petl as etl
from .postgres_connector import Postgres_Connector

csv.field_size_limit(sys.maxsize)

class Postgres():
    '''
    A class that encapsulates objects and attributes for a specific Postgres table. 
    The class includes several verification and clean-up methods that should be 
    invoked using
    ```
    with Postgres(...) as pg: 
        pg.method(...)
    ```
    See `Postgres.__enter__` and `Postgres.__exit__` for details.

    To skip the checks, simply call `Postgres(...).<method>(...)`
    '''

    from ._properties import (
        csv_path, temp_csv_path, json_schema_path, json_schema_s3_key, 
        export_json_schema, primary_keys, pk_constraint_name, table_self_identifier, 
        fields, fields_and_types, geom_field, geom_type, database_object_type)
    from ._s3 import (get_csv_from_s3, get_json_schema_from_s3, load_csv_to_s3, 
                      load_json_schema_to_s3)
    from ._cleanup import (vacuum_analyze, cleanup, check_remove_nulls)

    def __init__(self, connector: 'Postgres_Connector', table_name:str, table_schema:str=None,
                 **kwargs):
        '''Pass table_schema = None for TEMP tables'''
        self.connector = connector
        self.logger = self.connector.logger
        self.conn = self.connector.conn
        self.table_name = table_name
        self.table_schema = table_schema
        self.fully_qualified_table_name = f'{self.table_schema}.{self.table_name}'
        self.temp_table_name = self.table_name + '_t'
        self.s3_bucket = kwargs.get('s3_bucket', None)
        self.s3_key = kwargs.get('s3_key', None)
        self.geom_field = kwargs.get('geom_field', None)
        self.geom_type = kwargs.get('geom_type', None)
        self.with_srid = kwargs.get('with_srid', None)
        self._json_schema_s3_key = kwargs.get('json_schema_s3_key', None)
        self._schema = None
        self._export_json_schema = None
        self._primary_keys = None
        self._pk_constraint_name = None
        self._fields = None
        self._database_object_type = None

        # First make sure the table exists: 
        if self.table_schema == None: # If no schema provided, assume the table is TEMPORARY
            assert_statement = f'TEMPORARY Table {self.table_name} does not exist in this DB'
            logger_statement = f'TEMPORARY table {self.table_name}'
        else: 
            assert_statement = f'Table {self.table_schema}.{self.table_name} does not exist in this DB'
            logger_statement = f'table {self.fully_qualified_table_name}'
        
        if table_name != None: # If no table name was provided, don't bother with checking
            assert self.check_exists(self.table_name, self.table_schema), assert_statement
            self.logger.info(f'Connected to {logger_statement}\n')

    def __enter__(self):
        '''Context manager functions to be called BEFORE any functions inside
        ```
        with Postgres(...) as pg: 
            ...
        ```
        See https://book.pythontips.com/en/latest/context_managers.html
        '''
        
        self.get_row_count()
        self.logger.info(f'{"*" * 80}\n')
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
                self.get_row_count()   
                self.conn.commit()
                self.vacuum_analyze()
                self.logger.info('Done! All transactions committed.\n')
            except Exception as e:
                self.logger.error('Workflow failed... rolling back database transactions.\n')
                self.conn.rollback()
                raise e
            finally:
                self.cleanup() 
        else: # An exception was raised before __exit__()
            self.logger.error('Workflow failed... rolling back database transactions.\n')
            self.conn.rollback()
            self.cleanup()

    def check_exists(self, table_name: str, schema_name:str) -> bool: 
        '''Check if a table or view exists, returning True or False. 
        - `table_name`: Name of table or view
        - `schema_name`: If not None, look for a BASE TABLE or VIEW. Otherwise 
        look for a LOCAL TEMPORARY table. '''
        stmt = sql.SQL('''
    SELECT *
    FROM information_schema.tables
    WHERE table_name = %s''')
        
        data = [table_name]
        if schema_name != None: 
            stmt += sql.SQL(' AND table_schema = %s')
            data.append(self.table_schema)
        with self.conn.cursor() as cursor: 
            cursor.execute(stmt, data)
            rv = cursor.fetchone()
            return rv != None
    
    def execute_sql(self, stmt, data=None, fetch=None):
        '''
        Execute an sql.SQL statement and fetch rows if specified. 
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

    def prepare_file(self, file:str, mapping_dict:dict=None):
        '''
        Prepare a CSV file's geometry and header for insertion into Postgres; 
        write to CSV at self.temp_csv_path. If mapping_dict is not None, no edits 
        are made to the data header.
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
        self.logger.info(f'self.geom_type is: {self.geom_type}\n')
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
        if mapping_dict != None: 
            str_header = str_header.replace('#', '_')

            # Many Oracle datasets have the objectid field as "objectid_1". Making an 
            # empty dataset from Oracle with "create-beta-enterprise-table.py" remakes 
            # the dataset with a proper 'objectid' primary key. However the CSV made from Oracle
            # will still have "objectid_1" in it. Handle this by replacing "objectid_1" 
            # with "objectid" in CSV header if "objectid" doesn't already exist.
            match = re.search('(objectid_\d+),', str_header)
            if (re.search('objectid,', str_header) == None and match != None): 
                old_col = match.groups()[0]
                self.logger.info(f'\nDetected {old_col} primary key, implementing workaround and modifying header...\n')
                str_header = str_header.replace(f'{old_col}', 'objectid')
            rows = rows.rename({old:new for old, new in zip(header, str_header.split(', '))})
        
        # Write our possibly modified lines into the temp_csv file
        write_file = self.temp_csv_path
        rows.tocsv(write_file)

    def _make_mapping_dict(self, column_mappings:str = None, mappings_file:str = None) -> dict: 
        '''Transform a string dict or a file with a string dict into that dict'''
        if column_mappings != None: 
            mapping_dict = ast.literal_eval(column_mappings)
        elif mappings_file != None: 
            with open(mappings_file, 'r') as f: 
                mapping_text = f.read()
            mapping_dict = ast.literal_eval(mapping_text)
        else:
            return {}

        assert type(mapping_dict) == dict, 'Column mappings could not be read as a Python dictionary'
        return mapping_dict    

    def _map_header(self, str_header: str, mapping_dict: dict) -> str: 
        '''Internal function to transform a header according to a mapping'''
        mapped_header = []
        header_list = str_header.split(',')
        for h in header_list: 
            if h in mapping_dict: 
                mapped_header.append(mapping_dict[h])
            else: 
                mapped_header.append(h)
        mapped_header = ','.join(mapped_header)
        
        return mapped_header   
    
    def write_csv(self, write_file:'str', table_name:'str', schema_name:'str', 
                  mapping_dict:dict={}, temp_table:bool=False):
        '''Use Postgres COPY FROM method to append a CSV to a Postgres table, 
        gathering the header from the first line of the file. Use mapping_dict to
        map data file columns to database table columns.
        
        - write_file: Path of file for postgresql COPY FROM
        - table_name: Destination table
        - schema_name: Schema of destination table, ignored if temp_table == True
        - mapping_dict: A dict of the form 
            - {"data_col": "db_table_col", "data_col2": "db_table_col2", ... }
        - temp_table: True if the table is a TEMP TABLE, False otherwise
        
        Note that only the columns whose names differ between the data file and 
        the database table need to be included. While this method can be called 
        directly, it is preferable to call load() if possible instead.
        '''

        if temp_table: # temp_tables do not exist in a user-defined schema
            self.logger.info(f'Writing to TEMP table {table_name} from {write_file}...')
            table_identifier = sql.Identifier(table_name)
        else: 
            self.logger.info(f'Writing to table {schema_name}.{table_name} from {write_file}...')
            table_identifier = sql.Identifier(schema_name, table_name)
        
        with open(write_file, 'r') as f: 
            # f.readline() moves cursor position out of position
            str_header = f.readline(5_000_000).strip().split(',')            
            f.seek(0)

            with self.conn.cursor() as cursor:
                cols_composables = []
                for col in str_header: 
                    mapped_col = mapping_dict.get(col, col)
                    cols_composables.append(sql.Identifier(mapped_col))
                cols_composed = sql.Composed(cols_composables).join(', ')
                
                copy_stmt = sql.SQL('''
    COPY {table} ({cols_composed}) 
    FROM STDIN WITH (FORMAT csv, HEADER true)''').format(
                    table=table_identifier, 
                    cols_composed=cols_composed)
                self.logger.info(f'copy_statement:{cursor.mogrify(copy_stmt).decode()}') # Does this mean they need to be correctly capitalized as identifiers?
                cursor.copy_expert(copy_stmt, f)

                self.logger.info(f'Postgres Write Successful: {cursor.rowcount:,} rows imported.\n')

    def get_row_count(self):
        '''Get the current table row count. Don't make this a property because 
        row counts change.'''
        data = self.execute_sql(
            sql.SQL('SELECT count(*) FROM {}').format(
                self.table_self_identifier), 
            fetch='many')
        count = data[0][0]
        self.logger.info(f'{self.fully_qualified_table_name} current row count: {count:,}\n')
        return count

    def extract(self, return_data:bool=False):
        """Extract data from a postgres table into a CSV file in S3. 
        
        Has spatial and SRID detection and will output it in a way that the ago 
        append commands will recognize.
        
        ### Params: 
        * return_data (bool): If True return the data in memory as a 'geopetl.postgis.PostgisQuery'
        otherwise perform null bytes checks, write to CSV, and load to S3. 
        """
        row_count = self.get_row_count()
        
        self.logger.info(f'Starting extract from {self.fully_qualified_table_name}')
        self.logger.info(f'Rows to extract: {row_count}')
        self.logger.info("Note: petl can cause log messages to seemingly come out of order.")
        
        assert row_count != 0, 'Error! Row count of dataset in database is 0??'

        # Try to get an (arbitrary) sensible interval to print progress on by dividing by the row count
        if row_count < 10000:
            interval = int(row_count/3)
        if row_count > 10000:
            interval = int(row_count/15)
        if row_count == 1:
            interval = 1
        # If it rounded down to 0 with int(), that means we have a very small amount of rows
        if not interval:
            interval = 1

        self.logger.info('Initializing data var with etl.frompostgis()..')
        if self.with_srid is True:
            rows = etl.frompostgis(self.conn, self.fully_qualified_table_name, geom_with_srid=True)
        else:
            rows = etl.frompostgis(self.conn, self.fully_qualified_table_name, geom_with_srid=False)

        num_rows_in_csv = rows.nrows()
        if num_rows_in_csv == 0:
            raise AssertionError('Error! Dataset is empty? Line count of CSV is 0.')

        # Find datetime fields so we can make everything timezone aware if it's naive.. This is important for Carto.
        datetime_fields = []
        # Do not use etl.typeset to determine data types because otherwise it causes geopetl to
        # read the database multiple times
        for field in self.fields_and_types: 
            # Create list of datetime type fields that aren't timezone aware:
            if ('timestamp' in field[1].lower() or 'date' in field[1].lower()) and \
                ('tz' not in field[1].lower() and 'with time zone' not in field[1].lower()):
                datetime_fields.append(field[0].lower())

        if datetime_fields:
            self.logger.info(f'\nConverting {datetime_fields} fields to Eastern timezone datetime\n')
            rows_new = etl.convert(rows, datetime_fields, pytz.timezone('US/Eastern').localize)
            # Replace rows with our converted object
            rows = rows_new

        self.logger.info(f'Asserting counts match between db and extracted csv')
        self.logger.info(f'{row_count} == {num_rows_in_csv}')
        assert row_count == num_rows_in_csv
        
        if return_data: 
            return rows
        
        # Dump to our CSV temp file
        self.logger.info('Extracting csv...')
        try:
            rows.progress(interval).tocsv(self.csv_path, 'utf-8')
        except UnicodeError:
            self.logger.warning("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
            rows.progress(interval).tocsv(self.csv_path, 'latin-1')

        # New assert as well that will fail if row_count doesn't equal CSV again (because of time difference)
        db_newest_row_count = self.get_row_count()
        self.logger.info(f'Asserting counts match between current db count and extracted csv')
        self.logger.info(f'{db_newest_row_count} == {num_rows_in_csv}')
        assert db_newest_row_count == num_rows_in_csv

        self.check_remove_nulls()
        self.load_csv_to_s3(path=self.csv_path)
    
    def create_temp_table(self): 
        '''Create an empty temp table from self.table_name in the same schema'''
        with self.conn.cursor() as cursor: 
            cursor.execute(sql.SQL('''
            CREATE TEMPORARY TABLE {} AS 
                SELECT * 
                FROM {}
                WHERE 1=0;
            ''').format(    # This is psycopg2.sql.SQL.format() not f string format
                sql.Identifier(self.temp_table_name), # Temp tables cannot be created in a user-defined schema
                self.table_self_identifier)) # See https://www.psycopg.org/docs/sql.html
            self.logger.info(f'Created TEMP table {self.temp_table_name}\n')
    
    def drop_table(self, schema_name: str, table_name: 'str', exists='log'): 
        '''DROP a table
        - schema - Schema of table to drop, use <self>.table_schema for this object's 
        schema
        - table_name - Table name to drop, use <self>.table_name for this object's table
        - exists - One of "log", "error". If the table name already exists, 
        whether to record that in the log or raise an error
        '''
        self.logger.info(f'Attempting to drop table if exists {table_name}')
        if self.check_exists(table_name, schema_name): 
            if exists == 'error': 
                raise ValueError(f'Table {table_name} already exists and was set to be dropped.')
            if exists == 'log': 
                self.logger.info(f'\tExisting table {table_name} will be dropped.')
        else:         
            self.logger.info(f'\tTable {table_name} does not exist.\n')
            return None
        with self.conn.cursor() as cursor:
            cursor.execute(
                sql.SQL('''DROP TABLE IF EXISTS {}''').format(sql.Identifier(table_name)))
        self.logger.info('DROP IF EXISTS statement successfully executed.\n')

    def truncate(self):
        '''Simply Truncates a table'''
        
        truncate_stmt = sql.SQL('TRUNCATE TABLE {table_schema_name}').format(
            table_schema_name=self.table_self_identifier)
        with self.conn.cursor() as cursor: 
            self.logger.info(f'truncate_stmt:{cursor.mogrify(truncate_stmt).decode()}')
            cursor.execute(truncate_stmt)
            self.logger.info(f'Truncate successful: {cursor.rowcount:,} rows updated/inserted.\n')

    def delete_from_truncate(self):
        '''Truncates a table but instead does it via a DELETE FROM which allows it to happen in one transaction'''
        truncate_stmt = sql.SQL('DELETE FROM {table_schema_name}').format(
            table_schema_name=self.table_self_identifier)
        with self.conn.cursor() as cursor: 
            self.logger.info(f'truncate_stmt:{cursor.mogrify(truncate_stmt).decode()}')
            cursor.execute(truncate_stmt)
            self.logger.info(f'Truncate successful: {cursor.rowcount:,} rows updated/inserted.\n')

    def load(self, column_mappings:str=None, mappings_file:str=None, truncate_before_load:bool=False):
        '''
        Prepare and COPY a CSV from S3 to a Postgres table. If the keyword arguments 
        "column_mappings" or "mappings_file" are passed with values other than None, 
        those mappings are used to map data file columns to database table colums.

        - column_mappings: A string that can be read as a dictionary using 
        `ast.literal_eval()`. 
            - It should take the form '{"data_col": "db_table_col", 
                                        "data_col2": "db_table_col2", ...}'
            - Note the quotes around the curly braces `'{}'` because it is a string
        - mappings_file: A text file that can be opened with open() and that 
        contains one Python dictionary that can be read with `ast.literal_eval()`
            - The file should take the form {"data_col": "db_table_col", 
                                             "data_col2": "db_table_col2", ... }
            - Note no quotes around the curly braces `{}`. 
    
        Only one of column_mappings or mappings_file should be provided. Note that 
        only the columns whose headers differ between the data file and the database 
        table need to be included. All column names must be quoted. 
        '''
        mapping_dict = self._make_mapping_dict(column_mappings, mappings_file)
        self.get_csv_from_s3()
        self.prepare_file(file=self.csv_path, mapping_dict=mapping_dict)
        if truncate_before_load:
            self.delete_from_truncate()
        self.write_csv(write_file=self.temp_csv_path, table_name=self.table_name, 
                       schema_name=self.table_schema, mapping_dict=mapping_dict)

    def _delete_using_except(self, staging, mapping_dict:dict): 
        '''Run a query to delete the rows from a table that do not appear in another 
        table using EXCEPT'''
        prod_fields_composables = []
        staging_fields_composables = []
        for staging_field in staging.fields: 
            # If there is a mapping for staging_field, use that mapping, otherwise 
            # just use staging_field
            prod_field = mapping_dict.get(staging_field, staging_field) 
            
            prod_fields_composables.append(sql.Identifier(prod_field))
            staging_fields_composables.append(sql.SQL('STAGING.') + sql.Identifier(staging_field))
        
        where_composables = []
        for pk in self.primary_keys: 
            where_composables.append(
                sql.Composed(
                    sql.SQL('PROD.') + sql.Identifier(pk) + 
                    sql.SQL(' = ') + 
                    sql.SQL('NOT_MATCHED.') + sql.Identifier(pk)))
        
        # Any composed statement can be examined with print(<sql.Composed>.as_string(cursor))
        prod_fields_composed = sql.Composed(prod_fields_composables).join(', ')
        staging_fields_composed = sql.Composed(staging_fields_composables).join(', ')
        where_composed = sql.Composed(where_composables).join(' AND ')

        delete_using_stmt = sql.SQL('''
    DELETE 
    FROM {table_schema_name} AS PROD
    USING
        (SELECT {prod_fields_composed} 
        FROM {table_schema_name} 
        EXCEPT 
        SELECT {staging_fields_composed} 
        FROM {staging_table_schema_name} AS STAGING) AS NOT_MATCHED
    WHERE {where_composed}''').format(
            table_schema_name=self.table_self_identifier, 
            prod_fields_composed=prod_fields_composed, 
            staging_fields_composed=staging_fields_composed,
            staging_table_schema_name=staging.table_self_identifier, 
            where_composed=where_composed)
    
        with self.conn.cursor() as cursor: 
            self.logger.info(f'delete_using_statement:{cursor.mogrify(delete_using_stmt).decode()}')
            cursor.execute(delete_using_stmt)
            self.logger.info(f'Delete Using Except statement successful: {cursor.rowcount:,} rows deleted.\n')
    
    def _upsert_data_from_db(self, staging: 'Postgres', mapping_dict:dict={}, 
                             delete_stale:bool=False): 
        '''
        Create the SQL statements to upsert a table into another, optionally deleting 
        stale data beforehand. In general form, this SQL takes the form of 
        ```
        INSERT INTO {table_schema_name} AS PROD (col1, col2, ...)
        SELECT STAGING.col1, STAGING.col2, ...
        FROM {staging_table_schema_name} AS STAGING
        ON CONFLICT ON CONSTRAINT {pk_constraint}
        DO UPDATE SET 
            col1 = EXCLUDED.col1, col2 = EXCLUDED.col2, ...
        WHERE PROD.pk1 = EXCLUDED.pk1 AND PROD.pk2 = EXCLUDED.pk2 AND ...
        ```
        See https://www.psycopg.org/docs/sql.html for how sql.Composable, sql.SQL, 
        sql.Identifier, and sql.Composed related to each other       
        '''
        # See https://www.postgresql.org/docs/current/sql-insert.html for why the 
        # table is called EXCLUDED

        # Iterate through the Other table's fields and use the mapping_dict to create 
        # three sql.Composed statements: prod_fields, staging_fields, update_set
        prod_fields_composables = []
        staging_fields_composables = []
        update_set_composables = []
        for staging_field in staging.fields: 
            # If there is a mapping for staging_field, use that mapping, otherwise 
            # just use staging_field
            prod_field = mapping_dict.get(staging_field, staging_field) 
            
            prod_fields_composables.append(sql.Identifier(prod_field))
            staging_fields_composables.append(sql.SQL('STAGING.') + sql.Identifier(staging_field))
            update_set_composables.append(
                sql.Composed(
                    sql.Identifier(prod_field) + 
                    sql.SQL(' = EXCLUDED.') + 
                    sql.Identifier(prod_field)))
        
        # Iterate through self.primary_keys and use the mapping_dict to create 
        # one sql.Composed statement: where_composed
        where_composables = []
        for pk in self.primary_keys: 
            where_composables.append(
                sql.Composed(
                    sql.SQL('PROD.') + sql.Identifier(pk) + 
                    sql.SQL(' = ') + 
                    sql.SQL('EXCLUDED.') + sql.Identifier(pk)))
        
        # Any composed statement can be examined with print(<sql.Composed>.as_string(cursor))
        prod_fields_composed = sql.Composed(prod_fields_composables).join(', ')
        staging_fields_composed = sql.Composed(staging_fields_composables).join(', ')
        update_set_composed = sql.Composed(update_set_composables).join(', ')
        where_composed = sql.Composed(where_composables).join(' AND ')

        upsert_stmt = sql.SQL('''
    INSERT INTO {table_schema_name} AS PROD ({prod_fields_composed})
    SELECT {staging_fields_composed}
    FROM {staging_table_schema_name} AS STAGING
    ON CONFLICT ON CONSTRAINT {pk_constraint}
    DO UPDATE SET {update_set_composed}
    WHERE {where_composed}
    ''').format(
            table_schema_name=self.table_self_identifier, 
            prod_fields_composed=prod_fields_composed,
            staging_fields_composed=staging_fields_composed, 
            staging_table_schema_name=staging.table_self_identifier, 
            pk_constraint=sql.Identifier(self.pk_constraint_name), 
            update_set_composed=update_set_composed, 
            where_composed=where_composed)

        with self.conn.cursor() as cursor: 
            self.logger.info(f'upsert_statement:{cursor.mogrify(upsert_stmt).decode()}')
            cursor.execute(upsert_stmt)
            self.logger.info(f'Upsert Successful: {cursor.rowcount:,} rows updated/inserted.\n')
        
        if delete_stale: 
            self._delete_using_except(staging=staging, mapping_dict=mapping_dict)

    def _upsert_csv(self, mapping_dict:dict, delete_stale:bool): 
        '''Upsert a CSV file from S3 to a Postgres table'''
        assert self.check_exists(self.temp_table_name, self.table_schema) == False, f'Temporary Table {self.temp_table_name} already exists in this DB!'

        self.get_csv_from_s3()
        self.prepare_file(self.csv_path, mapping_dict)
        self.create_temp_table()
        self.write_csv(write_file=self.temp_csv_path, table_name=self.temp_table_name, 
                       schema_name=self.table_schema, mapping_dict=mapping_dict, temp_table=True)
        staging = Postgres(connector=self.connector, table_name=self.temp_table_name, 
                         table_schema=None)
        self._upsert_data_from_db(staging=staging, mapping_dict=mapping_dict, delete_stale=delete_stale)

    def _upsert_table(self, mapping_dict:dict, staging_table:str, staging_schema:str, 
                      delete_stale:bool): 
        '''Upsert a table within the same Postgres database to a Postgres table'''
        if not staging_schema: 
            staging_schema = self.table_schema
        staging = Postgres(connector=self.connector, table_name=staging_table, 
                         table_schema=staging_schema)
        self._upsert_data_from_db(staging=staging, mapping_dict=mapping_dict, delete_stale=delete_stale)
    
    def upsert(self, method:str, staging_table:str=None, staging_schema:str=None, 
               column_mappings:str=None, mappings_file:str=None, delete_stale:bool=False): 
        '''Upserts data from a CSV or from a table/view within the same database to a 
        Postgres table, which must have at least one primary key. Whether 
        upserting from a CSV or Postgres table, the keyword arguments 
        "column_mappings" or "mappings_file" may be passed with values other than 
        None to map data file columns to database table columns.
        
        - method: Indicates the source type. Should be one of "csv", "table".
        - staging_table: Name of Postgres table to upsert from 
        - staging_schema: Schema of Postgres table to upsert from. If None, assume the 
        same schema as the table being upserted to
        - column_mappings: A string that can be read as a dict using `ast.literal_eval()`. 
            - It should take the form '{"data_col": "db_table_col", 
                                        "data_col2": "db_table_col2", ...}'
            - Note the quotes around the curly braces `'{}'` because it is a string
        - mappings_file: A text file (not Python file) that can be opened with open() 
        and that contains one Python dict that can be read with `ast.literal_eval()`
            - The file should take the form {"data_col": "db_table_col", 
                                             "data_col2": "db_table_col2", ... }
            - Note no quotes around the curly braces `{}`. 
        - delete_stale: If True, delete rows that do not appear in the staging 
        data table. 
    
        Only one of column_mappings or mappings_file should be provided. Note that 
        only the columns whose headers differ between the data file and the database 
        table need to be included. All column names must be quoted. 
        '''
        self.logger.info(f'Upserting into {self.fully_qualified_table_name}\n')
        if self.primary_keys == set(): 
            raise ValueError(f'Upsert method requires that table "{self.fully_qualified_table_name}" have at least one column as primary key.')
        mapping_dict = self._make_mapping_dict(column_mappings, mappings_file)
        
        if method == 'csv': 
            self._upsert_csv(mapping_dict, delete_stale)
        elif method == 'table': 
            self._upsert_table(mapping_dict, staging_table, staging_schema, delete_stale)
        else: 
            raise KeyError('Method {method} not recognized for upsert')
