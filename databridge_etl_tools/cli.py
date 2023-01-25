import click

from .oracle import Oracle
from .carto_ import Carto
from .postgres import Postgres
from .ago import AGO
# One-off scripts for use in betabridge
from .db2 import Db2


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
@click.option('--s3_key')
@click.option('--select_users', required=False, default=None)
@click.option('--index_fields', required=False, default=None)
def carto_update(table_name, connection_string, s3_bucket, s3_key, select_users, index_fields):
    """Loads a datasets from S3 into carto"""
    carto = Carto(
        table_name=table_name,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        select_users=select_users,
        index_fields=index_fields)
    carto.run_workflow()


@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--s3_key')
@click.option('--json_schema_s3_key', default=None, required=False)
def postgres_load(table_name, table_schema, connection_string, s3_bucket, s3_key, json_schema_s3_key=None):
    """Loads a dataset from postgresql into a CSV file in S3"""
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        json_schema_s3_key=json_schema_s3_key)
    postgres.load()


@main.command()
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--s3_key')
@click.option('--json_schema_s3_key', default=None, required=False)
@click.option('--with_srid', default=True, required=False,
        help='''Likely only needed for certain views. This
        controls whether the geopetl frompostgis() function exports with geom_with_srid. That wont work
        for some views so just export without.''')
def postgres_extract(table_name, table_schema, connection_string, s3_bucket, s3_key, json_schema_s3_key=None, with_srid=None):
    """Extracts data from a postgres table into a CSV file in S3. Has spatial and SRID detection
    and will output it in a way that the ago append commands will recognize."""
    postgres = Postgres(
        table_name=table_name,
        table_schema=table_schema,
        connection_string=connection_string,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        json_schema_s3_key=json_schema_s3_key,
        with_srid=with_srid)
    postgres.extract()



# AGO truncate and append
@main.command()
@click.option('--ago_org_url', required=True)
@click.option('--ago_user', required=True)
@click.option('--ago_pw', required=True)
@click.option('--ago_item_name', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--in_srid', type=click.INT, default=False, required=False,
            help='The SRID of the source datasets geometry features.')
@click.option('--clean_columns', type=click.STRING, default=False, required=False,
            help='Column, or comma separated list of column names to clean of AGO invalid characters.')
@click.option('--batch_size', type=click.INT, default=500, required=False,
            help='Size of batch updates to send to AGO')
def ago_truncate_append(ago_org_url, ago_user, ago_pw, ago_item_name, s3_bucket, s3_key, in_srid=None, clean_columns=None, batch_size=500):
    """Truncates a dataset in AGO and appends to it from a CSV. CSV needs to be made
    from the postgres-extract command."""
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        in_srid=in_srid,
        clean_columns=clean_columns,
        batch_size=batch_size)
    ago.get_csv_from_s3()
    ago.append(truncate=True)
    ago.verify_count()


# AGO append, no truncate
@main.command()
@click.option('--ago_org_url', required=True)
@click.option('--ago_user', required=True)
@click.option('--ago_pw', required=True)
@click.option('--ago_item_name', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--in_srid', type=click.INT, default=False, required=False,
            help='The SRID of the source datasets geometry features.')
@click.option('--clean_columns', type=click.STRING, default=False, required=False,
            help='Column, or comma separated list of column names to clean of AGO invalid characters.')
@click.option('--batch_size', type=click.INT, default=500, required=False,
            help='Size of batch updates to send to AGO')
def ago_append(ago_org_url, ago_user, ago_pw, ago_item_name, s3_bucket, s3_key, in_srid=None, clean_columns=None, batch_size=500):
    """Appends records to AGO without truncating. NOTE that this is NOT an upsert 
    and will absolutely duplicate rows if you run this multiple times."""
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        in_srid=in_srid,
        clean_columns=clean_columns,
        batch_size=batch_size)
    ago.get_csv_from_s3()
    ago.append(truncate=False)

@main.command()
@click.option('--ago_org_url', required=True)
@click.option('--ago_user', required=True)
@click.option('--ago_pw', required=True)
@click.option('--ago_org_id', required=True)
@click.option('--ago_item_name', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--index_fields', required=True)
def ago_post_index_fields(ago_org_url, ago_user, ago_pw, ago_org_id, ago_item_name, s3_bucket, s3_key, index_fields):
    '''Post index fields to AGO'''
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_org_id=ago_org_id,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        index_fields=index_fields)
    ago.post_index_fields()

# AGO Upsert
@main.command()
@click.option('--ago_org_url', required=True)
@click.option('--ago_user', required=True)
@click.option('--ago_pw', required=True)
@click.option('--ago_item_name', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--primary_key', type=click.STRING, required=True)
@click.option('--in_srid', type=click.INT, default=False, required=False,
            help='The SRID of the source datasets geometry features.')
@click.option('--clean_columns', type=click.STRING, default=False, required=False,
            help='Column, or comma separated list of column names to clean of AGO invalid characters.')
@click.option('--batch_size', type=click.INT, default=500, required=False,
            help='Size of batch updates to send to AGO')
def ago_upsert(ago_org_url, ago_user, ago_pw, ago_item_name, s3_bucket, s3_key, primary_key, in_srid=None, clean_columns=None, batch_size=500):
    """Upserts records to AGO, requires a primary key. Upserts the entire CSV
    into AGO, it does not look for changes or differences."""
    ago = AGO(
        ago_org_url=ago_org_url,
        ago_user=ago_user,
        ago_pw=ago_pw,
        ago_item_name=ago_item_name,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        primary_key=primary_key,
        in_srid=in_srid,
        clean_columns=clean_columns,
        batch_size=batch_size)
    ago.get_csv_from_s3()
    ago.upsert()

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


@main.command()
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
@click.option('--enterprise_schema', required=True)
@click.option('--libpq_conn_string', required=True)
def create_staging_from_enterprise(table_name, account_name, enterprise_schema, libpq_conn_string):
    """Creates a staging table in etl_staging from the specified enterprise authoritative dataset."""
    db2 = Db2(
        table_name=table_name,
        account_name=account_name,
        enterprise_schema=enterprise_schema,
        libpq_conn_string=libpq_conn_string)
    db2.create_staging_from_enterprise()


@main.command()
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
@click.option('--copy_from_source_schema', required=True)
@click.option('--enterprise_schema', required=True)
@click.option('--libpq_conn_string', required=True)
def copy_dept_to_enterprise(table_name, account_name, copy_from_source_schema, enterprise_schema, libpq_conn_string):
    """Copy from the dept table to the staging table"""
    db2 = Db2(
        table_name=table_name,
        account_name=account_name,
        copy_from_source_schema=account_name,
        enterprise_schema=enterprise_schema,
        libpq_conn_string=libpq_conn_string)
    db2.copy_to_enterprise()


@main.command()
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
@click.option('--copy_from_source_schema', required=True)
@click.option('--enterprise_schema', required=True)
@click.option('--libpq_conn_string', required=True)
def copy_staging_to_enterprise(table_name, account_name, copy_from_source_schema, enterprise_schema, libpq_conn_string):
    """Copies from etl_staging to the specified enterprise authoritative dataset."""
    db2 = Db2(
        table_name=table_name,
        account_name=account_name,
        copy_from_source_schema='etl_staging',
        enterprise_schema=enterprise_schema,
        libpq_conn_string=libpq_conn_string)
    db2.copy_to_enterprise()


@main.command()
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
@click.option('--oracle_conn_string', default=None, required=False)
def update_oracle_scn(table_name, account_name, oracle_conn_string):
    """Creates a staging table in etl_staging from the specified enterprise authoritative dataset."""
    db2 = Db2(
        table_name=table_name,
        account_name=account_name,
        oracle_conn_string=oracle_conn_string)
    db2.update_oracle_scn()


if __name__ == '__main__':
    main()
