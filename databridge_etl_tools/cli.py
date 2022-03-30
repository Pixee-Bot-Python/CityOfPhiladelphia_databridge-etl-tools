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
def oracle_extract(table_name, table_schema, connection_string, s3_bucket, s3_key):
    """Extracts a dataset in Oracle into a CSV file in S3"""
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
@click.option('--s3_key')
@click.option('--select_users')
@click.option('--index_fields')
def cartoupdate(table_name, connection_string, s3_bucket, json_schema_s3_key, s3_key, select_users, index_fields):
    """Loads a datasets from S3 into carto"""
    carto = Carto(
        table_name=table_name,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        s3_key=s3_key,
        select_users=select_users,
        index_fields=index_fields)
    carto.run_workflow()


@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--json_schema_s3_key')
@click.option('--s3_key')
def postgres_load(table_name, table_schema, connection_string, s3_bucket, json_schema_s3_key, s3_key):
    """Loads a dataset from postgresql into a CSV file in S3"""
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        s3_key=s3_key)
    postgres.load()


@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--json_schema_s3_key', default=None, required=False)
@click.option('--s3_key')
def postgres_extract(table_name, table_schema, connection_string, s3_bucket, json_schema_s3_key, s3_key):
    """Extracts data from a postgres table into a CSV file in S3. Has spatial and SRID detection
    and will output it in a way that the ago append commands will recognize."""
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        json_schema_s3_key=json_schema_s3_key,
        s3_key=s3_key)
    postgres.extract()


#@click.option('--in_srid', type=click.INT, default=0, required=False)

@main.command()
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_pw')
@click.option('--ago_item_name')
@click.option('--s3_bucket')
@click.option('--s3_key')
def ago_truncate_append(ago_org_url, ago_user, ago_pw, ago_item_name, s3_bucket, s3_key):
    """Truncates a dataset in AGO and appends to it from a CSV. CSV needs to be made
    from the postgres-extract command."""
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key)
    ago.get_csv_from_s3()
    ago.truncate()
    ago.append()
    ago.verify_count()


# Truncate without append
@main.command()
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_pw')
@click.option('--ago_item_name')
@click.option('--s3_bucket')
@click.option('--s3_key')
def ago_append(ago_org_url, ago_user, ago_pw, ago_item_name, s3_bucket, s3_key):
    """Appends records to AGO without truncating. NOTE that this is NOT an upsert 
    and will absolutely duplicate rows if you run this multiple times."""
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key)
    ago.get_csv_from_s3()
    ago.append()


@main.command()
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_pw')
@click.option('--ago_item_name')
@click.option('--s3_bucket')
@click.option('--s3_key')
def ago_export(ago_org_url, ago_user, ago_pw, ago_item_name, s3_bucket, s3_key):
    """Export from an AGO dataset into an csv file in S3"""
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key)
    ago.export()


if __name__ == '__main__':
    main()
