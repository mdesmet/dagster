from dagster import job, op
from dagster_trino import trino_resource

@op(required_resource_keys={'trino'})
def get_one(context):
    return context.resources.trino.execute_query('SELECT 1', fetch_results=True)

@op(required_resource_keys={'trino'})
def create_schema(context):
    context.resources.trino.execute_query('CREATE SCHEMA IF NOT EXISTS memory.default', fetch_results=False)


@op(required_resource_keys={'trino'})
def create_table(context, get_one, create_schema):
    context.resources.trino.execute_query('CREATE TABLE memory.default.test AS SELECT ? a', parameters=[get_one[0][0]], fetch_results=True)


@job(resource_defs={'trino': trino_resource})
def my_trino_job():
    create_table(get_one(), create_schema())
