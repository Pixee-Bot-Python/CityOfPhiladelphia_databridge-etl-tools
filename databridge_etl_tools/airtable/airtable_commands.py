from .. import utils
from .airtable import Airtable
import click


@click.group()
@click.pass_context
@click.option('--app_id', required=True)
@click.option('--api_key', required=True)
@click.option('--table_name', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--add_objectid', required=False, is_flag=True, help='Adds an objectid to the CSV')
@click.option('--get_fields', required=False, help='Fields you want to extract, comma separated string.')
def airtable(ctx, **kwargs):
    ctx.obj = Airtable(**kwargs)

@airtable.command()
@click.pass_context
def extract(ctx): 
    ctx.obj.extract()