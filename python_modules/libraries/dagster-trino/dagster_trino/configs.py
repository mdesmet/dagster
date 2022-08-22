from dagster import Bool, Field, IntSource, StringSource


def define_trino_config():
    """Trino configuration.

    See the Trino documentation for reference:
        https://trino.io/docs/current/
    """

    host = Field(
        StringSource,
        description="Your Trino server's hostname",
        is_required=True,
    )

    port = Field(IntSource, description="Your Trino's server port", is_required=True)

    user = Field(StringSource, description="User login name.", is_required=True)

    password = Field(StringSource, description="User password.", is_required=False)

    catalog = Field(
        StringSource,
        description="Name of the default catalog to use. After login, you can use USE catalog_name "
        " to change the database.",
        is_required=False,
    )

    schema = Field(
        StringSource,
        description="Name of the default schema to use. After login, you can use USE schema_name to "
        "change the schema.",
        is_required=False,
    )

    connector = Field(
        StringSource,
        description="Indicate alternative database connection engine. Permissible option is "
        "'sqlalchemy' otherwise defaults to use the Snowflake Connector for Python.",
        is_required=False,
    )

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "catalog": catalog,
        "schema": schema,
        "connector": connector,
    }
