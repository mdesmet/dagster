from dagster._core.utils import check_dagster_package_version

from .resources import TrinoConnection, trino_resource
from .trino_io_manager import DbTypeHandler, build_trino_io_manager
from .solids import trino_op_for_query
from .version import __version__

check_dagster_package_version("dagster-trino", __version__)

__all__ = [
    "trino_op_for_query",
    "trino_resource",
    "build_trino_io_manager",
    "TrinoConnection",
    "DbTypeHandler",
]
