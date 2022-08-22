from unittest import mock

from dagster_trino import trino_resource

from dagster._core.test_utils import environ
from dagster._legacy import ModeDefinition, execute_solid, solid

from .utils import create_mock_connector


@mock.patch("trino.dbapi.connect", new_callable=create_mock_connector)
def test_trino_resource(trino_connect):
    @solid(required_resource_keys={"trino"})
    def trino_solid(context):
        assert context.resources.trino
        with context.resources.trino.get_connection() as _:
            pass

    result = execute_solid(
        trino_solid,
        run_config={
            "resources": {
                "trino": {
                    "config": {
                        "catalog": "database_abc",
                        "host": "account_abc",
                        "port": 443,
                        "user": "user_abc",
                        "password": "password_abc",
                        "schema": "my_schema",
                    }
                }
            }
        },
        mode_def=ModeDefinition(resource_defs={"trino": trino_resource}),
    )
    assert result.success
    trino_connect.assert_called_once_with(
        catalog="database_abc",
        host="account_abc",
        port=443,
        user="user_abc",
        password="password_abc",
        schema="my_schema",
    )


@mock.patch("trino.dbapi.connect", new_callable=create_mock_connector)
def test_trino_resource_from_envvars(trino_connect):
    @solid(required_resource_keys={"trino"})
    def trino_solid(context):
        assert context.resources.trino
        with context.resources.trino.get_connection() as _:
            pass

    env_vars = {
        "trino_HOST": "foo",
        "trino_PORT": "443",
        "trino_USER": "bar",
        "trino_PASSWORD": "baz",
        "trino_CATALOG": "TESTDB",
        "trino_SCHEMA": "TESTSCHEMA",
    }
    with environ(env_vars):
        result = execute_solid(
            trino_solid,
            run_config={
                "resources": {
                    "trino": {
                        "config": {
                            "host": {"env": "trino_HOST"},
                            "port": {"env": "trino_PORT"},
                            "user": {"env": "trino_USER"},
                            "password": {"env": "trino_PASSWORD"},
                            "catalog": {"env": "trino_CATALOG"},
                            "schema": {"env": "trino_SCHEMA"},
                        }
                    }
                }
            },
            mode_def=ModeDefinition(resource_defs={"trino": trino_resource}),
        )
        assert result.success
        trino_connect.assert_called_once_with(
            host="foo",
            port=443,
            user="bar",
            password="baz",
            catalog="TESTDB",
            schema="TESTSCHEMA",
        )
