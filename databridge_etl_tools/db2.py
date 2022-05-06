import os
import sys
import logging
import click
import json
import psycopg2
import cx_Oracle


class Db2():
    '''One-off functions for databridge v2 stuff'''
    _logger = None
    _staging_dataset_name = None
    _enterprise_dataset_name = None
    _pg_cursor = None
    _oracle_cursor = None

    def __init__(self,
                table_name,
                account_name,
                enterprise_schema = None,
                oracle_conn_string = None,
                libpq_conn_string = None
                ):
        self.table_name = table_name
        self.account_name = account_name
        self.enterprise_schema = enterprise_schema
        self.libpq_conn_string = libpq_conn_string
        self.oracle_conn_string = oracle_conn_string
        self.staging_schema = 'etl_staging'
        # use this to transform specific to more general data types for staging table
        self.data_type_map = {'character varying': 'text'}
        self.ignore_field_name = []
        # placeholders vars for passing between methods
        self.geom_info = None
        self.column_info = None
        self.ddl = None


    @property
    def pg_cursor(self):
        if self._pg_cursor is None: 
            conn = psycopg2.connect(self.libpq_conn_string)
            assert conn.closed == 0
            conn.autocommit = True
            self._pg_cursor = conn.cursor()
        return self._pg_cursor

    @property
    def oracle_cursor(self):
        if self._oracle_cursor is None: 
            conn = cx_Oracle.connect(self.oracle_conn_string)
            conn.autocommit = True
            self._oracle_cursor = conn.cursor()
        return self._oracle_cursor

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
    def staging_dataset_name(self):
        if self._staging_dataset_name is None:
            self._staging_dataset_name = f'{self.staging_schema}__{self.table_name}'
        return self._staging_dataset_name

    @property
    def enterprise_dataset_name(self):
        if self._enterprise_dataset_name is None:
            self._enterprise_dataset_name = f'{self.account_name.replace("GIS_","").lower()}__{self.table_name}'
        return self._enterprise_dataset_name


    def get_table_column_info_from_enterprise(self):
        """Queries the information_schema.columns table to get column names and data types"""

        col_info_stmt = f'''
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_schema = '{self.enterprise_schema}' and table_name = '{self.enterprise_dataset_name}'
        '''
        self.logger.info('Running col_info_stmt: ' + col_info_stmt)
        self.pg_cursor.execute(col_info_stmt)
        # Format and transform data types:
        column_info = {i[0]: self.data_type_map.get(i[1], i[1]) for i in self.pg_cursor.fetchall()}

        self.logger.info(f'DEBUG! column_info: {column_info}')

        # Metadata column added into postgres tables by arc programs, not needed.
        if 'gdb_geomattr_data' in column_info.keys():
            column_info.pop('gdb_geomattr_data')

        # If the table doesn't exist, the above query silently fails.
        assert column_info
        self.column_info = column_info
        #return column_info


    def get_geom_column_info(self):
        """Queries the geometry_columns table to geom field and srid, then queries the sde table to get geom_type"""

        get_column_name_and_srid_stmt = f'''
            select f_geometry_column, srid from geometry_columns
            where f_table_schema = '{self.enterprise_schema}' and f_table_name = '{self.enterprise_dataset_name}'
        '''
        # Identify the geometry column values
        self.logger.info('Running get_column_name_and_srid_stmt' + get_column_name_and_srid_stmt)
        self.pg_cursor.execute(get_column_name_and_srid_stmt)

        #col_name = self.pg_cursor.fetchall()[0]
        col_name1 = self.pg_cursor.fetchall()
    
        # If the result is empty, there is no shape field and this is a table.
        # return empty dict that will evaluate to False.
        if not col_name1: 
            return {}
        self.logger.info(f'Got shape field and SRID back as: {col_name1}')

        col_name = col_name1[0]
        # Grab the column names
        header = [h.name for h in self.pg_cursor.description]
        # zip column names with the values
        geom_column_and_srid = dict(zip(header, list(col_name)))
        geom_column = geom_column_and_srid['f_geometry_column']
        srid = geom_column_and_srid['srid']

        # Get the type of geometry, e.g. point, line, polygon.. etc.
        # docs on this SDE function: https://desktop.arcgis.com/en/arcmap/latest/manage-data/using-sql-with-gdbs/st-geometrytype.htm
        # NOTE!: if phl is empty, e.g. this is a first run, this will fail, as a backup get the value from XCOM
        # Which will be populated by our "get_geomtype" task.
        geom_type_stmt = f'''
            select public.st_geometrytype({geom_column}) as geom_type 
            from {self.enterprise_schema}.{self.enterprise_dataset_name}
            where st_isempty({geom_column}) is False
            limit 1
        '''
        self.logger.info('Running geom_type_stmt: ' + geom_type_stmt)
        self.pg_cursor.execute(geom_type_stmt)
        result = self.pg_cursor.fetchone()
        if result is None:
            geom_type_stmt = f'''
            select geometry_type('{self.enterprise_schema}', '{self.enterprise_dataset_name}', '{geom_column}')
            '''
            self.logger.info('Running geom_type_stmt: ' + geom_type_stmt)
            self.pg_cursor.execute(geom_type_stmt)
            geom_type = self.pg_cursor.fetchone()[0]

            #geom_type = ti.xcom_pull(key=xcom_task_id_key + 'geomtype')
            assert geom_type
            #self.logger.info(f'Got our geom_type from xcom: {geom_type}') 
        else:
            geom_type = result[0]

        self.geom_info = {'geom_field': geom_column,
                          'geom_type': geom_type,
                          'srid': srid}

        #return {'geom_field': geom_column, 'geom_type': geom_type, 'srid': srid}


    def generate_ddl(self):
        """Builds the DDL based on the table's generic and geom column info"""
        # If geom_info is not None
        if self.geom_info:
            column_type_map = [f'{k} {v}' for k,v in self.column_info.items() if k not in self.ignore_field_name and k != self.geom_info['geom_field']]
            srid = self.geom_info['srid']
            geom_column = self.geom_info['geom_field']

            # Think postgres calls for something like 'Point' vs 'POINT' ?
            geom_type = self.geom_info['geom_type'].replace('ST_', '').capitalize()
            geom_column_string = f'{geom_column} geometry({geom_type}, {srid})'
            column_type_map.append(geom_column_string)
            column_type_map_string = ', '.join(column_type_map)
        # If this is a table with no geometry column..
        else:
            column_type_map = [f'{k} {v}' for k,v in self.column_info.items() if k not in self.ignore_field_name]
            column_type_map_string = ', '.join(column_type_map)

        #self.logger.info('DEBUG!!: ' + str(self.column_info))

        assert column_type_map_string

        ddl = f'''CREATE TABLE {self.staging_schema}.{self.enterprise_dataset_name}
            ({column_type_map_string})'''
        self.ddl = ddl
        #return ddl


    def run_ddl(self):
        drop_stmt = f'DROP TABLE IF EXISTS {self.staging_schema}.{self.enterprise_dataset_name}'
        # drop first so we have a total refresh
        self.logger.info('Running drop stmt: ' + drop_stmt)
        self.pg_cursor.execute(drop_stmt)
        # Identify the geometry column values
        self.logger.info('Running ddl stmt: ' + self.ddl)
        self.pg_cursor.execute(self.ddl)
        # Make sure we were successful
        try:
            check_stmt = f'''
                SELECT EXISTS
                    (SELECT FROM pg_tables
                    WHERE schemaname = \'{self.staging_schema}\'
                    AND tablename = \'{self.enterprise_dataset_name}\');
                    '''
            self.logger.info('Running check_stmt: ' + check_stmt)
            self.pg_cursor.execute(check_stmt)
            return_val = str(self.pg_cursor.fetchone()[0])
            assert (return_val == 'True' or return_val == 'False')
            if return_val == 'False':
                raise Exception('Table does not appear to have been created!')
            if return_val != 'True':
                raise Exception('This value from the check_stmt query is unexpected: ' + return_val)
            if return_val == 'True':
                self.logger.info(f'Table "{self.staging_schema}.{self.enterprise_dataset_name}" created successfully.')
        except Exception as e:
            raise Exception("DEBUG: " + str(e) + " RETURN: " + str(return_val) + " DDL: " + self.ddl + " check query" + check_stmt)


    def create_staging_from_enterprise(self):
        self.get_table_column_info_from_enterprise()
        self.get_geom_column_info()
        self.generate_ddl()
        self.run_ddl()


    def copy_staging_to_enterprise(self):
        get_enterprise_columns_stmt = f'''
        SELECT array_agg(COLUMN_NAME::text order by COLUMN_NAME)
        FROM information_schema.columns
        WHERE table_name = '{self.enterprise_dataset_name}' AND table_schema = '{self.enterprise_schema}'
        '''

        self.logger.info("Executing get_enterprise_columns_stmt: " + str(get_enterprise_columns_stmt))
        self.pg_cursor.execute(get_enterprise_columns_stmt)
        enterprise_columns = [column for column in self.pg_cursor.fetchall()[0]][0]

        # Figure out what the official OBJECTID is (since there can be multiple like "OBJECTID_1")

        get_oid_column_stmt = f'''
            SELECT rowid_column FROM sde.sde_table_registry
            WHERE table_name = '{self.enterprise_dataset_name}' AND schema = '{self.enterprise_schema}'
            '''
        self.logger.info("Executing get_oid_column_stmt: " + str(get_oid_column_stmt))
        self.pg_cursor.execute(get_oid_column_stmt)
        oid_column = self.pg_cursor.fetchone()[0]

        # if table has objectid column, put at end of column list:
        print('DEBUG enterprise_columns: ' + str(enterprise_columns))
        print('DEBUG oid_column: ' + str(oid_column))
        if oid_column:
            enterprise_columns.remove(oid_column)
            enterprise_columns.append(oid_column)

        # Metadata column added into postgres tables by arc programs, not needed.
        if 'gdb_geomattr_data' in enterprise_columns:
            enterprise_columns.remove('gdb_geomattr_data')

        enterprise_columns_str = ', '.join(enterprise_columns)
        staging_columns = enterprise_columns
        # Remove objectid (or whatever it is) the value we'll insert will be next_rowid('{table_schema}', '{table_name}')'
        print('DEBUG staging_columns: ' + str(staging_columns))
        if oid_column:
            staging_columns.remove(oid_column)
        staging_columns_str = ', '.join(staging_columns)


        ###############
        # First we need to reset the objectid number that next_rowid() pulls from
        # so we're not making crazy objectids in the trillions.
        # To do this we need to modify the insert delta table of our SDE table.
        reg_stmt=f'''
        SELECT registration_id FROM sde.sde_table_registry
        WHERE owner = '{self.enterprise_schema}' AND table_name = '{self.enterprise_dataset_name}'
        '''
        self.logger.info("Running reg_stmt: " + str(reg_stmt))
        self.pg_cursor.execute(reg_stmt)
        reg_id = self.pg_cursor.fetchone()[0]


        reset_stmt=f'''
        UPDATE phl.i{reg_id} SET base_id=1, last_id=1
        WHERE id_type = 2
        '''
        self.logger.info("Running reset_stmt: " + str(reset_stmt))
        self.pg_cursor.execute(reset_stmt)
        self.pg_cursor.execute('COMMIT')
        #############


        select_fields = f'''
        {staging_columns_str}''' if not oid_column else f'''{staging_columns_str}''' + f''', next_rowid('{self.enterprise_schema}', '{self.enterprise_dataset_name}')
        '''

        insert_stmt = f'''
            INSERT INTO {self.enterprise_schema}.{self.enterprise_dataset_name} ({enterprise_columns_str})
            SELECT {select_fields}
            FROM {self.staging_schema}.{self.enterprise_dataset_name}
            '''

        ###############
        # Truncate is not 'MVCC-safe', which means concurrent select transactions will not be able to
        # view/select the data during the execution of the update_stmt.
        #truncate_stmt = f'''TRUNCATE TABLE phl.{self.enterprise_dataset_name}'''
        # DELTE FROM is 'MVCC-safe'.
        truncate_stmt = f'''DELETE FROM phl.{self.enterprise_dataset_name}'''

        update_stmt = f'''
            BEGIN;
            {truncate_stmt};
            {insert_stmt};
            '''
        self.logger.info("Running update_stmt: " + str(update_stmt))
        try:
            self.pg_cursor.execute(update_stmt)
            self.pg_cursor.execute('COMMIT')
            # Drop the etl_staging table when we're done to save space.
            delete_stmt = f'''
            DROP TABLE {self.staging_schema}.{self.enterprise_dataset_name}
            '''
            self.pg_cursor.execute(delete_stmt)
            self.pg_cursor.execute('COMMIT')

            # Run a quick select statement to test.
            select_test_stmt = f'''
            SELECT * FROM {self.enterprise_schema}.{self.enterprise_dataset_name} LIMIT 1
            '''
            self.logger.info("Running select_test_stmt: " + str(select_test_stmt))

            self.pg_cursor.execute(select_test_stmt)
            result = self.pg_cursor.fetchone()[0]
            self.logger.info('Result of select test:')
            self.logger.info(str(result))
            assert result

        except psycopg2.Error as e:
            self.logger.error(f'Error truncating and inserting into enterprise! Error: {str(e)}')
            self.pg_cursor.execute('ROLLBACK')


    def update_oracle_scn(self):
        
        oracle_account_name = 'GIS_' + self.account_name.upper()

        stmt = f'''SELECT MAX(ora_rowscn) FROM {oracle_account_name}.{self.table_name.upper()}'''
        self.oracle_cursor.execute(stmt)
        current_scn = self.oracle_cursor.fetchone()[0]

        stmt=f'''
            SELECT SCN FROM GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY
            WHERE TABLE_OWNER = '{oracle_account_name}'
            AND TABLE_NAME = '{self.table_name}'
        '''
        self.logger.info('Executing stmt: ' + str(stmt))
        self.oracle_cursor.execute(stmt)
        old_scn = self.oracle_cursor.fetchone()
        print('DEBUG!: ' + str(old_scn))

        # Because Oracle is an outdated database product, we don't have upsert and need to do
        # either an insert or update depending if the row we want already exists.
        if old_scn is None:
            stmt = f'''
            INSERT INTO GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY (TABLE_OWNER, TABLE_NAME, SCN)
                VALUES('{oracle_account_name}', '{self.table_name}', {current_scn})
            '''
        elif old_scn:
            stmt = f'''
            UPDATE GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY SET SCN={current_scn}
                WHERE TABLE_OWNER = '{oracle_account_name}' AND TABLE_NAME = '{self.table_name}'
            '''
        self.logger.info('Executing stmt: ' + str(stmt))
        self.oracle_cursor.execute(stmt)




@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
