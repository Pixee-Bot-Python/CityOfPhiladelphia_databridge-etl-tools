import logging
import sys

from databridge_etl_tools.carto_ import Carto
from databridge_etl_tools.postgres import Postgres


def handler(event, context):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    logger.addHandler(sh)

    logger.info('Received event: ' + str(event))

    command = event
    command_name = event['command_name']

    if command_name == 'cartoupdate':
        carto = Carto(
            table_name=command['table_name'],
            connection_string=command['connection_string'],
            s3_bucket=command['s3_bucket'],
            json_schema_s3_key=command['json_schema_s3_key'],
            csv_s3_key=command['csv_s3_key'],
            select_users=command['select_users'],
            index_fields=None #TODO: UPDATE THIS
        )
        carto.run_workflow()
    elif command_name == 'load':
        postgres = Postgres(
            table_name=command['table_name'],
            table_schema=command['table_schema'],
            connection_string=command['connection_string'],
            s3_bucket=command['s3_bucket'],
            json_schema_s3_key=command['json_schema_s3_key'],
            csv_s3_key=command['csv_s3_key']
        )
        postgres.run_workflow()
    elif command_name == 'extract':
        raise NotImplementedError('Not implemented due to the Oracle client being too large to fit on lambda.')
    else:
        log.error('Command not recognized!')
        raise

    logger.info('Process completed!')