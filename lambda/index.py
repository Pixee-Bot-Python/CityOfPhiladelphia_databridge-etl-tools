import logging

from databridge_etl_tools.cli import cartoupdate, load


def handler(event, context):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    logger.addHandler(sh)

    logger.info('Received event: ' + event)

    command = event
    logger.info(command)

    command_name = command['command_name']
    logger.info(command_name)

    if command_name == 'carto_update':
        carto_update(
            table_name=command['table_name'],
            connection_string=command['connection_string'],
            s3_bucket=command['s3_bucket'],
            json_schema_s3_key=command['json_schema_s3_key'],
            csv_s3_key=command['csv_s3_key'],
            select_users=command['select_users'],
            index_fields=command['index_fields']
        )
    elif command_name == 'load':
        load(
            table_name=command['table_name'],
            table_schema=command['table_schema'],
            connection_string=command['connection_string'],
            s3_bucket=command['s3_bucket'],
            json_schema_s3_key=command['json_schema_s3_key'],
            csv_s3_key=command['csv_s3_key']
        )

    logger.info('Process completed!')