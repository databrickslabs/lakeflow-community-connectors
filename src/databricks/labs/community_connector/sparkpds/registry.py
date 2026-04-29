"""
Registry module for registering LakeflowSource with Spark's DataSource API.

This module provides functions to register a LakeflowConnect implementation
or a custom DataSource with Spark, making it available as a Python Data Source.
"""

import importlib
import inspect
from types import ModuleType
from typing import Type, Union
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource


_BASE_PKG = "databricks.labs.community_connector.sources"

def _get_class_fqn(cls: Type) -> str:
    """Get the fully qualified name of a class (module.ClassName)."""
    return f"{cls.__module__}.{cls.__name__}"


def _import_class(fqn: str) -> Type:
    """Import a class from its fully qualified name (e.g., 'module.ClassName')."""
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _get_source_module(source_name: str, module_name: str) -> ModuleType:
    """Import a module from a source package."""
    try:
        importlib.import_module(f"{_BASE_PKG}.{source_name}")
    except ModuleNotFoundError:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the directory 'src/databricks/labs/community_connector/sources/{source_name}/' exists."
        )

    module_path = f"{_BASE_PKG}.{source_name}.{module_name}"
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ImportError(
            f"Could not import '{module_name}.py' from source '{source_name}'. "
            f"Please ensure 'src/databricks/labs/community_connector/sources/{source_name}/{module_name}.py' exists."
        )


def _get_register_function(source_name: str):
    """Get the register_lakeflow_source function from a generated source module."""
    module_name = f"_generated_{source_name}_python_source"
    module = _get_source_module(source_name, module_name)

    if not hasattr(module, "register_lakeflow_source"):
        raise ImportError(
            f"Module '{module_name}' does not have a 'register_lakeflow_source' function. "
            f"Please ensure the module defines this function."
        )

    return module.register_lakeflow_source


def _find_lakeflow_connect_class(source_name: str) -> Type[LakeflowConnect]:
    """
    Find a LakeflowConnect subclass from the source package.

    Looks in databricks.labs.community_connector.sources.{source_name} for a class
    that inherits from LakeflowConnect. All source packages are expected to expose
    their implementation at the package level via __init__.py. This handles the case
    where the source is provided via a wheel package (uploaded or installed from PyPI)
    rather than via the generated source module.
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


def _register_lakeflow_connect(spark: SparkSession, cls: Type[LakeflowConnect]) -> None:
    """Wrap a LakeflowConnect class in a LakeflowSource and register it with Spark."""
    class_fqn = _get_class_fqn(cls)

    class RegisterableLakeflowSource(LakeflowSource):
        """Wrapper that dynamically imports the LakeflowConnect class by FQN."""

        def __init__(self, options):
            self.options = options
            lakeflow_connect_cls = _import_class(class_fqn)
            self.lakeflow_connect = lakeflow_connect_cls(options)

    RegisterableLakeflowSource.__name__ = f"RegisterableLakeflowSource_{cls.__name__}"

    spark.dataSource.register(RegisterableLakeflowSource)


def register(
    spark: SparkSession,
    source: Union[str, Type[DataSource], Type[LakeflowConnect]],
) -> None:
    """
    Register a source with Spark's DataSource API.

    This unified registration function handles:
    - String source names: Dynamically loads and registers the generated source module.
      Falls back to discovering a LakeflowConnect subclass from the source package
      when the generated module is not available (e.g. package-based deployment).
    - DataSource subclasses: Registered directly with Spark.
    - LakeflowConnect subclasses: Wrapped in a LakeflowSource and registered.

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
        try:
            register_fn = _get_register_function(source)
            register_fn(spark)
            return
        except ImportError:
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
