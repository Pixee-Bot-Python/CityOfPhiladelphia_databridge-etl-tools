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
    logger.info(command)

    command_name = command['command_name']
    logger.info(command_name)

    if command_name == 'carto_update':
        carto = Carto(
            table_name=command['table_name'],
            connection_string=command['connection_string'],
            s3_bucket=command['s3_bucket'],
            json_schema_s3_key=command['json_schema_s3_key'],
            csv_s3_key=command['csv_s3_key'],
            select_users=command['select_users'],
            index_fields=command['index_fields']
        )
        carto.run_workflow()
    elif command_name == 'load':
        postgres = Postgres(
            table_name=table_name,
            table_schema=table_schema,
            connection_string=connection_string,
            s3_bucket=s3_bucket,
            json_schema_s3_key=json_schema_s3_key,
            csv_s3_key=csv_s3_key)
        postgres.run_workflow()

    logger.info('Process completed!')