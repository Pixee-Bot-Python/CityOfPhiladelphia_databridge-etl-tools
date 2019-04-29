import psycopg2

from .abstract import BaseClient


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
            self.logger.info('Trying to connect to db_host: {}, db_port: {}, db_name: {}'.format(db_host, db_port, db_name))
            conn = psycopg2.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to database {}'.format(db_name))
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
        self.logger.info('Creating temp Carto table...')
        stmt = '''DROP TABLE IF EXISTS {table_name}; 
                    CREATE TABLE {table_name} ({schema});'''.format(table_name=self.temp_table_name,
                                                                    schema=self.schema)
        self.execute_sql(stmt)
        check_table_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}');"
        response = self.execute_sql(check_table_sql.format(self.temp_table_name))
        exists = response['rows'][0]['exists']

        if not exists:
            message = '{} - Could not create table'.format(self.temp_table_name)
            self.logger.error(message)
            raise Exception(message)

        if self.index_fields:
            self.logger.info("Indexing fields: {}".format(self.index_fields))
            self.create_indexes()

        self.logger.info('Temp table created successfully.\n')

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

    def vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.temp_table_name))
        self.execute_sql('VACUUM ANALYZE "{}";'.format(self.temp_table_name))
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
