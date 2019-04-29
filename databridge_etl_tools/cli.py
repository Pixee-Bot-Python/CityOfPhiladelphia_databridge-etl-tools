import click

from client.oracle_client import Oracle
from client.carto_client import Carto


@click.group()
def main():
    pass

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
def extract(table_name, table_schema, connection_string, s3_bucket):
    oracle = Oracle(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket)
    oracle.extract()

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
def cartoupdate(table_name, table_schema, connection_string, s3_bucket):
    carto = Carto(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket)
    carto.run_workflow()

if __name__ == '__main__':
    main()