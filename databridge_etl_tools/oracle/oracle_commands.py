from .oracle import Oracle
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--table_schema', required=True)
@click.option('--connection_string', required=True)
@click.option('--s3_bucket', required=True)
@click.option('--s3_key', required=True)
def oracle(ctx, **kwargs):
    '''Run ETL commands for Oracle'''
    ctx.obj = Oracle(**kwargs)

@oracle.command()
@click.pass_context
def extract(ctx): 
    """Extracts a dataset in Oracle into a CSV file in S3"""
    ctx.obj.extract()

@oracle.command()
@click.pass_context
def extract_json_schema(ctx): 
    """Extracts a dataset's schema in Oracle into a JSON file in S3"""
    ctx.obj.load_json_schema_to_s3()
