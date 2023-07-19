from .opendata import OpenData
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--table_schema', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
@click.option('--libpq_conn_string', required=True)
@click.option('--opendata_bucket', required=True)
def opendata(ctx, **kwargs):
    '''Run ETL commands for OpenData'''
    ctx.obj = OpenData(**kwargs)

@opendata.command()
@click.pass_context
def upload(ctx):
    """Takes a CSV from S3, runs some transformations, and then uploads to the specified opendata bucket"""
    ctx.obj.run()
