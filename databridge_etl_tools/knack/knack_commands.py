from .knack import Knack
from .. import utils
import click


@click.group()
@click.pass_context
@click.option('--app_id', required=True, help='specific app id for a dataset in Knack, found under settings')
@click.option('--api_key', required=True, help='specific key for a dataset in Knack, found under settings')
@click.option('--knack_objectid', required=True, help='Not an objectid in the ESRI sense, refers to a table under an "app" in Knack')
@click.option('--s3_bucket', required=True, help='Bucket to place the extracted csv in.')
@click.option('--s3_key', required=True, help='key under the bucket, example: "staging/dept/table_name.csv')
@click.option('--indent', type=int, default=None, help='???')
def knack(ctx, **kwargs):
    ctx.obj = Knack(**kwargs)

@knack.command()
@click.pass_context
def extract(ctx): 
    ctx.obj.extract()