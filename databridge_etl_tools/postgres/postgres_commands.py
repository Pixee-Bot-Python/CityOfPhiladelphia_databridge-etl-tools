from .postgres import Postgres
from .. import utils
import click

@click.group()
@click.pass_context
@click.option('--table_name')
@click.option('--table_schema')
@click.option('--connection_string')
@click.option('--s3_bucket')
@click.option('--s3_key')
def postgres(ctx, **kwargs):
    '''Run ETL commands for Postgres'''
    ctx.obj = {}
    ctx = utils.pass_params_to_ctx(ctx, **kwargs)

@postgres.command()
@click.pass_context
@click.option('--json_schema_s3_key', default=None, show_default=True, required=False)
@click.option('--with_srid', default=True, required=False, show_default=True, 
        help='''Likely only needed for certain views. This
        controls whether the geopetl frompostgis() function exports with geom_with_srid. That wont work
        for some views so just export without.''')
def extract(ctx, **kwargs):
    """Extracts data from a postgres table into a CSV file in S3. Has spatial and SRID detection
    and will output it in a way that the ago append commands will recognize."""
    postgres = Postgres(**ctx.obj, **kwargs)
    postgres.extract()

@postgres.command()
def extract_json_schema(ctx):
    """Extracts a dataset's schema in Oracle into a JSON file in S3"""
    postgres = Postgres(**ctx.obj)
    postgres.load_json_schema_to_s3()
    
@postgres.command()
@click.pass_context
@click.option('--json_schema_s3_key', default=None, required=False)
def load(ctx, **kwargs):
    """Loads from S3 to a postgres table, usually etl_staging."""
    postgres = Postgres(**ctx.obj, **kwargs)
    postgres.load()
