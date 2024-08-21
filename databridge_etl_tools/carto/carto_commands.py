from .carto_ import Carto
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--connection_string', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--json_schema_s3_key', required=False, default=None)
@click.option('--select_users', required=False, default=None)
@click.option('--index_fields', required=False, default=None)
def carto(ctx, **kwargs):
    '''Run ETL commands for Carto'''
    ctx.obj = Carto(**kwargs)

@carto.command()
@click.pass_context
def update(ctx):
    """Loads a datasets from S3 into carto"""
    ctx.obj.run_workflow()
