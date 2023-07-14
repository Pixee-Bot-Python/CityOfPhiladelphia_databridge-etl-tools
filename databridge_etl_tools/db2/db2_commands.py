from .db2 import Db2
from .. import utils
import click

@click.group()
@click.pass_context
@click.option('--table_name', required=True)
@click.option('--account_name', required=True)
def db2(ctx, **kwargs):
    '''Run ETL commands for DB2'''
    ctx.obj = {}
    ctx = utils.pass_params_to_ctx(ctx, **kwargs)

@db2.group()
@click.pass_context
@click.option('--enterprise_schema', required=True)
@click.option('--libpq_conn_string', required=True)
def libpq(ctx, **kwargs): 
    '''Use this group for any commands that utilize libpq'''
    ctx = utils.pass_params_to_ctx(ctx, **kwargs)

@libpq.command()
@click.pass_context
def copy_dept_to_enterprise(ctx, **kwargs):
    """Copy from the dept table directly to an enterpise able in a single transaction that can roll back if it fails."""
    db2 = Db2(**ctx.obj, **kwargs, copy_from_source_schema=ctx.obj['account_name'])
    db2.copy_to_enterprise()

@libpq.command()
@click.pass_context
def create_staging_from_enterprise(ctx, **kwargs):
    """Creates a staging table in etl_staging from the specified enterprise authoritative dataset."""
    db2 = Db2(**ctx.obj, **kwargs)
    db2.create_staging_from_enterprise()

@db2.command()
@click.pass_context
def copy_staging_to_enterprise(ctx, **kwargs):
    """Copies from etl_staging to the specified enterprise authoritative dataset."""
    db2 = Db2(**ctx.obj, **kwargs, copy_from_source_schema='etl_staging')
    db2.copy_to_enterprise()

@db2.command()
@click.pass_context
@click.option('--oracle_conn_string', default=None, required=False)
def update_oracle_scn(ctx, **kwargs):
    """Creates a staging table in etl_staging from the specified enterprise authoritative dataset."""
    db2 = Db2(**ctx.obj, **kwargs)
    db2.update_oracle_scn()
