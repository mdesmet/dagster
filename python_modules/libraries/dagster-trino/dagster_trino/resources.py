import sys
import warnings
from contextlib import closing, contextmanager
from typing import Mapping

import dagster._check as check
from dagster import resource

from .configs import define_trino_config

try:
    import trino
except ImportError:
    msg = (
        "Could not import trino."
    )
    warnings.warn(msg)
    raise


class TrinoConnection:
    def __init__(self, config: Mapping[str, str], log):  # pylint: disable=too-many-locals
        # Extract parameters from resource config. Note that we can't pass None values to
        # trino.dbapi.connect() because they will override the default values set within the
        # connector; remove them from the conn_args dict.
        self.connector = config.get("connector", None)

        if self.connector == "sqlalchemy":
            self.conn_args = {
                k: config.get(k)
                for k in (
                    "host",
                    "port"
                    "user",
                    "password",
                    "catalog",
                    "schema",
                )
                if config.get(k) is not None
            }

        else:
            self.conn_args = {
                k: config.get(k)
                for k in (
                    "host",
                    "port",
                    "user",
                    "password",
                    "catalog",
                    "schema",
                )
                if config.get(k) is not None
            }

        self.autocommit = True
        self.log = log

    @contextmanager
    def get_connection(self, raw_conn=True):
        if self.connector == "sqlalchemy":
            from trino.sqlalchemy import URL  # pylint: disable=no-name-in-module,import-error
            from sqlalchemy import create_engine

            engine = create_engine(URL(**self.conn_args))
            conn = engine.raw_connection() if raw_conn else engine.connect()

            yield conn
            conn.close()
            engine.dispose()
        else:
            conn = trino.dbapi.connect(**self.conn_args)

            yield conn
            if not self.autocommit:
                conn.commit()
            conn.close()

    def execute_query(self, sql, parameters=None, fetch_results=False):
        check.str_param(sql, "sql")
        check.opt_list_param(parameters, "parameters")
        check.bool_param(fetch_results, "fetch_results")

        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                if sys.version_info[0] < 3:
                    sql = sql.encode("utf-8")
                self.log.info("Executing query: " + sql)
                cursor.execute(sql, parameters)  # pylint: disable=E1101
                return cursor.fetchall() if fetch_results else None

    def execute_queries(
        self, sql_queries, parameters=None, fetch_results=False
    ):
        check.list_param(sql_queries, "sql_queries", of_type=str)
        check.opt_list_param(parameters, "parameters")
        check.bool_param(fetch_results, "fetch_results")

        results = []
        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                for sql in sql_queries:
                    if sys.version_info[0] < 3:
                        sql = sql.encode("utf-8")
                    self.log.info("Executing query: " + sql)
                    cursor.execute(sql, parameters)  # pylint: disable=E1101
                    results.append(cursor.fetchall())  # pylint: disable=E1101

        return results if fetch_results else None


@resource(
    config_schema=define_trino_config(),
    description="This resource is for connecting to Trino",
)
def trino_resource(context):
    """A resource for connecting to Trino.

    A simple example of loading data into Trino and subsequently querying that data is shown below:

    Examples:

    .. code-block:: python

        from dagster import job, op
        from dagster_trino import trino_resource

        @op(required_resource_keys={'trino'})
        def get_one(context):
            context.resources.trino.execute_query('SELECT 1')

        @job(resource_defs={'trino': trino_resource})
        def my_trino_job():
            get_one()

        my_trino_job.execute_in_process(
            run_config={
                'resources': {
                    'trino': {
                        'config': {
                            'host': {'env': 'TRINO_HOST'},
                            'port': {'env': 'TRINO_PORT'},
                            'user': {'env': 'TRINO_USER'},
                            'catalog': {'env': 'TRINO_CATALOG'},
                            'schema': {'env': 'TRINO_SCHEMA'},
                        }
                    }
                }
            }
        )

    """
    return TrinoConnection(context.resource_config, context.log)


def _filter_password(args):
    """Remove password from connection args for logging"""
    return {k: v for k, v in args.items() if k != "password"}
