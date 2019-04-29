import os

import psycopg2
import petl as etl
import geopetl

from .abstract import BaseClient


TEST = os.environ.get('TEST', False)

class Postgres(BaseClient):

    def __init__(self, connection_string, table_name, table_schema, s3_bucket):

        super(Postgres, self).__init__(
            connection_string=connection_string,
            table_name=table_name,
            table_schema=table_schema,
            s3_bucket=s3_bucket
        )

    @property
    def conn(self):
        if self._conn is None:
            self.logger.info('Trying to connect to postgres...')
            conn = psycopg2.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to postgres.')
        return self._conn

    def create_indexes(self, table_name):
        self.logger.info('Creating indexes on {}: {}'.format(self.temp_table_name, self.index_fields))
        stmt = ''
        for index_field in self.indexes_fields:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=self.temp_table_name,
                                                                                        field=self.index_field)
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
        exists = response[0]

        if not exists:
            message = '{} - Could not create table'.format(self.temp_table_name)
            self.logger.error(message)
            raise Exception(message)

        if self.index_fields:
            self.logger.info("Indexing fields: {}".format(self.index_fields))
            self.create_indexes()

        self.logger.info('Temp table created successfully.\n')

    def write(self):
        self.get_csv_from_s3()
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
        if self.geom_field and geom_srid:
            rows = rows.convert(geom_field,
                                lambda c: 'SRID={srid};{geom}'.format(srid=geom_srid, geom=c) if c else '') \

        write_file = self.temp_csv_path
        rows.tocsv(write_file, write_header=False)

        with open(write_file, 'r') as f:
            with self.conn.cursor() as cursor:
                copy = 'COPY {} TO STDOUT WITH CSV'.format(self.table_name)
                cursor.copy_expert(copy, file)

        check_load_stmt = "SELECT COUNT(*) FROM {table_name}".format(table_name=self.temp_table_name)
        response = self.execute_sql(check_load_stmt, fetch='one')

        self.logger.info('Postgres Write Successful: {} rows imported.\n'.format(response[0]))

    def get_geom_field(self):
        with open(json_schema_path) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                self.logger.error('json schema malformatted...')
                raise
            for scheme in schema:
                scheme_type = mapping.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    geom_srid = scheme.get('srid', '')
                    geom_field = scheme.get('name', '')

    def verify_count(self):
        self.logger.info('Verifying row count...')

        data = self.execute_sql('SELECT count(*) FROM "{}";'.format(self.temp_table_name), fetch='many')
        num_rows_in_table = data[0][0]
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
                    'DROP TABLE if exists "{}" cascade;'.format(self.temp_table_name) + \
                    'COMMIT;'
            self.execute_sql(stmt)
            exit(1)
        self.logger.info('Row count verified.\n')

    def vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.temp_table_name))

        # An autocommit connection is needed for vacuuming
        self._conn.autocommit = True
        self.execute_sql('VACUUM ANALYZE "{}";'.format(self.temp_table_name))
        self._conn.autocommit = False
                
        self.logger.info('Vacuum analyze complete.\n')

    def swap_table(self):
        stmt = 'BEGIN;' + \
                'ALTER TABLE "{}" RENAME TO "{}_old";'.format(self.table_name, self.table_name) + \
                'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.table_name) + \
                'DROP TABLE "{}_old" cascade;'.format(self.table_name) + \
                generate_select_grants() + \
                'COMMIT;'
        self.logger.info('Swapping temporary and production tables...')
        self.logger.info(stmt)
        self.execute_sql(stmt)

    def cleanup(self):
        self.logger.info('Attempting to drop any temporary tables: {}'.format(self.temp_table_name))
        stmt = '''DROP TABLE if exists {} cascade'''.format(self.temp_table_name)
        self.execute_sql(stmt)
        self.logger.info('Temporary tables dropped successfully.\n')

    def run_workflow(self):
        if TEST:
            self.logger.info('THIS IS A TEST RUN, PRODUCTION TABLES WILL NOT BE AFFECTED!\n')
        try:
            self.create_table()
            self.write()
            self.verify_count()
            if TEST:
                self.cleanup()
            else:
                self.swap_table()
            self.vacuum_analyze()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed, reverting...')
            self.cleanup()
            raise e