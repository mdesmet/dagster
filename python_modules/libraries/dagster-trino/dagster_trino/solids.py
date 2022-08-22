from dagster import Nothing
from dagster import _check as check
from dagster import op
from dagster._legacy import InputDefinition, solid


def _core_create_trino_command(dagster_decorator, decorator_name, sql, parameters=None):
    check.str_param(sql, "sql")
    check.opt_dict_param(parameters, "parameters")

    @dagster_decorator(
        name=f"trino_{decorator_name}",
        input_defs=[InputDefinition("start", Nothing)],
        required_resource_keys={"trino"},
        tags={"kind": "sql", "sql": sql},
    )
    def trino_fn(context):
        context.resources.trino.execute_query(sql=sql, parameters=parameters)

    return trino_fn


def trino_solid_for_query(sql, parameters=None):
    """This function is a solid factory that constructs solids to execute a trino query.

    Note that you can only use `trino_solid_for_query` if you know the query you'd like to
    execute at pipeline construction time. If you'd like to execute queries dynamically during
    pipeline execution, you should manually execute those queries in your custom solid using the
    trino resource.

    Args:
        sql (str): The sql query that will execute against the provided trino resource.
        parameters (dict): The parameters for the sql query.

    Returns:
        SolidDefinition: Returns the constructed solid definition.
    """
    return _core_create_trino_command(solid, "solid", sql, parameters)


def trino_op_for_query(sql, parameters=None):
    """This function is an op factory that constructs an op to execute a trino query.

    Note that you can only use `trino_op_for_query` if you know the query you'd like to
    execute at graph construction time. If you'd like to execute queries dynamically during
    job execution, you should manually execute those queries in your custom op using the
    trino resource.

    Args:
        sql (str): The sql query that will execute against the provided trino resource.
        parameters (dict): The parameters for the sql query.

    Returns:
        OpDefinition: Returns the constructed op definition.
    """
    return _core_create_trino_command(op, "op", sql, parameters)
