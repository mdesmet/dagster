from typing import Sequence

from trino.exceptions import ProgrammingError

from dagster import Field, IOManagerDefinition, OutputContext, StringSource, io_manager, IntSource

from .db_io_manager import DbClient, DbIOManager, DbTypeHandler, TablePartition, TableSlice
from .resources import TrinoConnection

TRINO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def build_trino_io_manager(type_handlers: Sequence[DbTypeHandler]) -> IOManagerDefinition:
    """
    Builds an IO manager definition that reads inputs from and writes outputs to Trino.

    Args:
        type_handlers (Sequence[DbTypeHandler]): Each handler defines how to translate between
            slices of Trino tables and an in-memory type - e.g. a Pandas DataFrame.

    Returns:
        IOManagerDefinition

    Examples:

        .. code-block:: python

            from dagster_trino import build_trino_io_manager
            from dagster_trino_pandas import TrinoPandasTypeHandler

            trino_io_manager = build_trino_io_manager([TrinoPandasTypeHandler()])

            @job(resource_defs={'io_manager': trino_io_manager})
            def my_job():
                ...
    """

    @io_manager(
        config_schema={
            "host": StringSource,
            "port": IntSource,
            "catalog": StringSource,
            "user": StringSource,
            "password": Field(StringSource, is_required=False),
            "schema": Field(StringSource, is_required=False),
        }
    )
    def trino_io_manager():
        return DbIOManager(type_handlers=type_handlers, db_client=TrinoDbClient())

    return trino_io_manager


class TrinoDbClient(DbClient):
    @staticmethod
    def delete_table_slice(context: OutputContext, table_slice: TableSlice) -> None:
        no_schema_config = (
            {k: v for k, v in context.resource_config.items() if k != "schema"}
            if context.resource_config
            else {}
        )
        with TrinoConnection(
            dict(schema=table_slice.schema, **no_schema_config), context.log  # type: ignore
        ).get_connection() as con:
            try:
                con.execute_string(_get_cleanup_statement(table_slice))
            except ProgrammingError:
                # table doesn't exist yet, so ignore the error
                pass

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"
        if table_slice.partition:
            return (
                f"SELECT {col_str} FROM {table_slice.catalog}.{table_slice.schema}.{table_slice.table}\n"
                + _time_window_where_clause(table_slice.partition)
            )
        else:
            return f"""SELECT {col_str} FROM {table_slice.catalog}.{table_slice.schema}.{table_slice.table}"""


def _get_cleanup_statement(table_slice: TableSlice) -> str:
    """
    Returns a SQL statement that deletes data in the given table to make way for the output data
    being written.
    """
    if table_slice.partition:
        return (
            f"DELETE FROM {table_slice.catalog}.{table_slice.schema}.{table_slice.table}\n"
            + _time_window_where_clause(table_slice.partition)
        )
    else:
        return f"DELETE FROM {table_slice.catalog}.{table_slice.schema}.{table_slice.table}"


def _time_window_where_clause(table_partition: TablePartition) -> str:
    start_dt, end_dt = table_partition.time_window
    start_dt_str = start_dt.strftime(TRINO_DATETIME_FORMAT)
    end_dt_str = end_dt.strftime(TRINO_DATETIME_FORMAT)
    return f"""WHERE {table_partition.partition_expr} BETWEEN TIMESTAMP '{start_dt_str}' AND TIMESTAMP '{end_dt_str}'"""
