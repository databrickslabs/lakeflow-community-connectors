"""
Registry module for registering LakeflowSource with Spark's DataSource API.

This module provides functions to register a LakeflowConnect implementation
or a custom DataSource with Spark, making it available as a Python Data Source.
"""

import importlib
import inspect
from typing import Type, Union
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource
from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
    IngestionAgentSource,
    _connector_options,
)


_BASE_PKG = "databricks.labs.community_connector.sources"


def _get_class_fqn(cls: Type) -> str:
    """Get the fully qualified name of a class (module.ClassName)."""
    return f"{cls.__module__}.{cls.__name__}"


def _import_class(fqn: str) -> Type:
    """Import a class from its fully qualified name (e.g., 'module.ClassName')."""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _find_lakeflow_connect_class(source_name: str) -> Type[LakeflowConnect]:
    """Find the LakeflowConnect subclass exposed by a source package.

    Looks in databricks.labs.community_connector.sources.{source_name} for a
    class that inherits from LakeflowConnect. Source packages are expected to
    expose their implementation at the package level via __init__.py.
    """
    package_fqn = f"{_BASE_PKG}.{source_name}"
    try:
        package = importlib.import_module(package_fqn)
    except ModuleNotFoundError:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the package '{package_fqn}' is installed."
        )

    for _, obj in inspect.getmembers(package, inspect.isclass):
        if issubclass(obj, LakeflowConnect) and obj is not LakeflowConnect:
            return obj

    raise ValueError(
        f"Could not find a LakeflowConnect implementation for source '{source_name}'. "
        f"Expected a class inheriting from LakeflowConnect exposed in {package_fqn}.__init__."
    )


def _bound_agent_source_cls(class_fqn: str) -> type:
    """Return an IngestionAgentSource subclass bound to ``class_fqn``.

    The agent dispatcher needs to instantiate the concrete LakeflowConnect
    class — same FQN-binding trick the LakeflowSource wrapper uses below.
    """

    class _BoundAgentSource(IngestionAgentSource):
        def _build_connector(self):
            lakeflow_connect_cls = _import_class(class_fqn)
            return lakeflow_connect_cls(_connector_options(self.options))

    _BoundAgentSource.__name__ = f"BoundIngestionAgentSource_{class_fqn.rsplit('.', 1)[-1]}"
    return _BoundAgentSource


def _register_lakeflow_connect(spark: SparkSession, cls: Type[LakeflowConnect]) -> None:
    """Wrap a LakeflowConnect class in a LakeflowSource and register it with Spark.

    The registered LakeflowSource serves both the regular ``tableName``
    dispatch path and the ingestion-agent operation dispatch path
    (option ``operation``) under the same ``lakeflow_connect`` format.
    """
    class_fqn = _get_class_fqn(cls)
    bound_agent_cls = _bound_agent_source_cls(class_fqn)

    class RegisterableLakeflowSource(LakeflowSource):
        """Wrapper that dynamically imports the LakeflowConnect class by FQN."""

        def _build_table_connector(self, options):
            return _import_class(class_fqn)(options)

        def _build_agent_source(self, options):
            return bound_agent_cls(options)

    RegisterableLakeflowSource.__name__ = f"RegisterableLakeflowSource_{cls.__name__}"

    spark.dataSource.register(RegisterableLakeflowSource)


def register(
    spark: SparkSession,
    source: Union[str, Type[DataSource], Type[LakeflowConnect]],
) -> None:
    """
    Register a source with Spark's DataSource API.

    This unified registration function handles:
    - String source names: discovers the LakeflowConnect subclass exposed
      by the source package and wraps it in a LakeflowSource.
    - DataSource subclasses: registered directly with Spark.
    - LakeflowConnect subclasses: wrapped in a LakeflowSource and registered.

    Args:
        spark: The SparkSession instance.
        source: A source name string (e.g., "zendesk", "github"), a DataSource subclass,
                or a LakeflowConnect subclass.

    Raises:
        TypeError: If source is not a string, DataSource subclass, or LakeflowConnect subclass.
        ValueError: If a string source name is provided but the source module doesn't exist.

    Examples:
        >>> # Register a source by name:
        >>> register(spark, "zendesk")
        >>> df = spark.read.format("lakeflow_connect").options(...).load()

        >>> # Register a LakeflowConnect implementation:
        >>> from my_connector import MyLakeflowConnect
        >>> register(spark, MyLakeflowConnect)
        >>> df = spark.read.format("lakeflow_connect").options(...).load()

        >>> # Register a custom DataSource directly:
        >>> from my_pds import MyCustomPDS
        >>> register(spark, MyCustomPDS)
        >>> df = spark.read.format(MyCustomPDS.name()).options(...).load()
    """
    if isinstance(source, str):
        lakeflow_cls = _find_lakeflow_connect_class(source)
        _register_lakeflow_connect(spark, lakeflow_cls)
        return

    if isinstance(source, type) and issubclass(source, DataSource):
        spark.dataSource.register(source)
        return

    if isinstance(source, type) and issubclass(source, LakeflowConnect):
        _register_lakeflow_connect(spark, source)
        return

    raise TypeError(
        f"source must be a string, DataSource subclass, or LakeflowConnect subclass, got {type(source)}"
    )
