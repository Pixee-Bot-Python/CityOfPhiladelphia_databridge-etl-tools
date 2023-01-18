import logging
import sys
import os
import csv
import pytz
import boto3
import petl as etl
import json


class Oracle():

    _conn = None
    _logger = None
    _json_schema = None

    def __init__(self, connection_string, table_name, table_schema, s3_bucket, s3_key):
        self.connection_string = connection_string
        self.table_name = table_name
        self.table_schema = table_schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    @property
    def schema_table_name(self):
        schema_table_name = '{}.{}'.format(self.table_schema, self.table_name)
        return schema_table_name

    @property
    def conn(self):
        if self._conn is None:
            try:
                import cx_Oracle
            except ImportError:
                self.logger.error("cx_Oracle wasn't found... Did you install it as well as the oracle instant client?")
            self.logger.info('Trying to connect to Oracle database...')
            conn = cx_Oracle.connect(self.connection_string)
            self._conn = conn
            self.logger.info('Connected to database.')
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
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

    @property 
    def json_schema(self):
        if self._json_schema is None:
            stmt=f'''
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                DATA_PRECISION,
                DATA_SCALE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{self.table_schema.upper()}'
            AND TABLE_NAME = '{self.table_name.upper()}'
            '''
            cursor = self.conn.cursor()
            cursor.execute(stmt)
            results = cursor.fetchall()
            self._json_schema = json.dumps(results)
        return self._json_schema


    def load_csv_and_schema_to_s3(self):
        self.logger.info('Starting load to s3: {}'.format(self.s3_key))

        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))
        
        self.logger.info('Successfully loaded to s3: {}'.format(self.s3_key))
    
        # Now load the schema
        json_schema_path = self.csv_path.replace('.csv','') + '_oracle_schema.json'
        json_s3_key = self.s3_key.replace('.csv','') + '_oracle_schema.json'
        with open(json_schema_path, 'w') as f:
            f.write(self.json_schema)

        s3.Object(self.s3_bucket, json_s3_key).put(Body=open(json_schema_path, 'rb'))
        self.logger.info('Successfully loaded to s3: {}'.format(json_s3_key))


    def check_remove_nulls(self):
        '''
        This function checks for null bytes ('\0'), and if exists replace with null string (''):
        Check only the first 500 lines to stay efficient, if there aren't
        any in the first 500, there likely(maybe?) aren't any.
        '''
        has_null_bytes = False
        with open(self.csv_path, 'r', encoding='utf-8') as infile:
            for i, line in enumerate(infile):
                if has_null_bytes:
                # Break the cycle if we already found them 
                    break
                if i >= 500:
                    break
                for char in line:
                    #if char == '\0' or char == u'\xa0' or char == b'\xc2\xa0':
                    if char == '\0' or char == u'\xa0':
                        has_null_bytes = True
                        break


        if has_null_bytes:
            self.logger.info("Dataset has null bytes, removing...")
            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
            with open(self.csv_path, 'r') as infile:
                with open(temp_file, 'w') as outfile:
                    reader = csv.reader((line.replace('\0', '') for line in infile), delimiter=",")
                    reader = csv.reader((line.replace(u'\xa0', '') for line in infile), delimiter=",")
                    #reader = csv.reader((line.replace(b'\xc2\xa0', '') for line in infile), delimiter=",")
                    writer = csv.writer(outfile)
                    writer.writerows(reader)
            os.replace(temp_file, self.csv_path)

    def extract(self):
        '''
        Extract data from database and save as a CSV file. Any fields that contain 
        datetime information will be converted to US/Eastern time zone (with historical 
        accuracy for Daylight Savings Time). Oracle also stories DATE fields with a 
        time component as well, so "DATE" fields that may appear without time information
        will also have timezone niformation added.
        Append CSV file to S3 bucket.
        '''
        self.logger.info('Starting extract from {}'.format(self.schema_table_name))
        import geopetl

        data = etl.fromoraclesde(self.conn, self.schema_table_name, geom_with_srid=True)
        datetime_fields = []
        for field in data.fieldnames(): 
            if 'datetime' in etl.typeset(data, field): 
                datetime_fields.append(field)
        if datetime_fields:
            print(f'Converting {datetime_fields} fields to Eastern timezone datetime')
            data = etl.convert(data, datetime_fields, pytz.timezone('US/Eastern').localize)
        
        try:
            etl.tocsv(data, self.csv_path, encoding='utf-8')
        except UnicodeError:
            self.logger.info("Exception encountered trying to extract to CSV with utf-8 encoding, trying latin-1...")
            etl.tocsv(data, self.csv_path, encoding='latin-1')

        # Confirm CSV isn't empty
        try:
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        except UnicodeError:
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')

        self.check_remove_nulls()

        num_rows_in_csv = rows.nrows()
        if num_rows_in_csv == 0:
            raise AssertionError('Error! Dataset is empty? Line count of CSV is 0.')

        self.load_csv_and_schema_to_s3()
        os.remove(self.csv_path)

        self.logger.info('Successfully extracted from {}'.format(self.schema_table_name))

    def write(self):
        raise NotImplementedError

