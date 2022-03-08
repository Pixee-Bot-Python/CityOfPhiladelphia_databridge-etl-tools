import click

from .oracle import Oracle
from .carto_ import Carto
from .postgres import Postgres
from .ago import AGO


@click.group()
def main():
    pass

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--s3_key')
def extract(table_name, table_schema, connection_string, s3_bucket, s3_key):
    oracle = Oracle(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        s3_key=s3_key)
    oracle.extract()

@main.command()
@click.option('--table_name')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--json_schema_s3_key')
@click.option('--csv_s3_key')
@click.option('--select_users')
@click.option('--index_fields')
def cartoupdate(table_name, 
                connection_string, 
                s3_bucket, 
                json_schema_s3_key, 
                csv_s3_key, 
                select_users,
                index_fields):
    carto = Carto(
        table_name=table_name,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        csv_s3_key=csv_s3_key,
        select_users=select_users,
        index_fields=index_fields)
    carto.run_workflow()

@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--json_schema_s3_key')
@click.option('--csv_s3_key')
def load(table_name, 
         table_schema, 
         connection_string, 
         s3_bucket, 
         json_schema_s3_key, 
         csv_s3_key):
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        csv_s3_key=csv_s3_key)
    postgres.run_workflow()

@main.command()
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_pw')
@click.option('--ago_item_name')
@click.option('--s3_bucket')
@click.option('--csv_s3_key')
def ago_truncate_append(
        ago_org_url,
        ago_user,
        ago_pw,
        ago_item_name,
        s3_bucket, 
        csv_s3_key):
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        csv_s3_key=csv_s3_key)
    ago.get_csv_from_s3()
    ago.truncate()
    ago.append()

@main.command()
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_pw')
@click.option('--ago_item_name')
@click.option('--s3_bucket')
@click.option('--csv_s3_key')
def ago_export(
        ago_org_url,
        ago_user,
        ago_pw,
        ago_item_name,
        s3_bucket, 
        csv_s3_key):
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        csv_s3_key=csv_s3_key)
    ago.export()

if __name__ == '__main__':
    main()
