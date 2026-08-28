"""
Registry helpers for Lakeflow source DataSources.

Three ways to register a source with Spark, all valid:

1. Direct import (preferred for wheel-installed connectors)::

       from databricks.labs.community_connector.sources.gmail import GmailDataSource
       spark.dataSource.register(GmailDataSource)
       df = spark.read.format("lakeflow_connect").option(...).load()

2. Lookup by source name (handy when the source is selected at runtime)::

       from databricks.labs.community_connector.sparkpds import find_data_source
       spark.dataSource.register(find_data_source("gmail"))

3. Legacy ``register()`` (used by pipeline templates)::

       from databricks.labs.community_connector import register
       register(spark, "gmail")

   This resolves the pre-merged single-file deployment
   (``_generated_<source>_python_source.py``) and registers the
   ``lakeflow_connect`` format. It is intentionally limited to that path and
   will be deprecated — new code should call ``spark.dataSource.register``
   with the source's ``DataSource`` class (path 1) or :func:`find_data_source`
   (path 2) instead.
"""

import importlib
import inspect
from types import ModuleType
from typing import Type

from pyspark.sql import SparkSession

from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource


_BASE_PKG = "databricks.labs.community_connector.sources"


def find_data_source(source_name: str) -> Type[LakeflowSource]:
    """Return the ``<Source>DataSource`` class exposed by a source package.

    Looks in ``databricks.labs.community_connector.sources.<source_name>`` for
    a :class:`LakeflowSource` subclass and returns it. The match restricts to
    classes defined inside the source package so unrelated re-exports (e.g.
    the base ``LakeflowSource`` itself) don't satisfy the search.

    Raises:
        ValueError: when the source package is missing or doesn't expose a
            :class:`LakeflowSource` subclass.
    """
    package_fqn = f"{_BASE_PKG}.{source_name}"
    try:
        package = importlib.import_module(package_fqn)
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the package '{package_fqn}' is installed."
        ) from exc

    for _, obj in inspect.getmembers(package, inspect.isclass):
        module = getattr(obj, "__module__", "")
        # Match on the package boundary, not a bare prefix, so a source whose
        # name is a prefix of another (e.g. "github" vs "github_enterprise")
        # can't resolve to the wrong package's class.
        in_package = module == package_fqn or module.startswith(package_fqn + ".")
        if issubclass(obj, LakeflowSource) and obj is not LakeflowSource and in_package:
            return obj

    raise ValueError(
        f"Could not find a LakeflowSource subclass for source '{source_name}'. "
        f"Expose a ``<Source>DataSource`` class from '{package_fqn}.__init__' "
        f"that sets '_lakeflow_connect_cls'."
    )


def _get_source_module(source_name: str, module_name: str) -> ModuleType:
    """Import a module from a source package."""
    try:
        importlib.import_module(f"{_BASE_PKG}.{source_name}")
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"Source '{source_name}' not found. "
            f"Make sure the directory 'src/databricks/labs/community_connector/sources/{source_name}/' exists."
        ) from exc

    module_path = f"{_BASE_PKG}.{source_name}.{module_name}"
    return importlib.import_module(module_path)  # raises ModuleNotFoundError if absent


def _get_register_function(source_name: str):
    """Return ``register_lakeflow_source`` from the pre-merged source module.

    Raises ``ModuleNotFoundError`` when the source has no merged file, or
    ``AttributeError`` when the module lacks the function, so the caller can
    fall back to the per-source DataSource class lookup.
    """
    module_name = f"_generated_{source_name}_python_source"
    module = _get_source_module(source_name, module_name)

    if not hasattr(module, "register_lakeflow_source"):
        raise AttributeError(
            f"Module '{module_name}' does not have a 'register_lakeflow_source' function."
        )

    return module.register_lakeflow_source


def register(spark: SparkSession, source: str) -> None:
    """Register a source by name via its merged ``_generated`` module.

    This is a legacy shim that exists only so existing code (pipeline
    templates, SDP deployments) can register a source by name. It resolves
    the pre-merged single-file module and registers the ``lakeflow_connect``
    format.

    Deprecated — to register a source directly, call
    ``spark.dataSource.register(<Source>DataSource)`` (or
    ``spark.dataSource.register(find_data_source("<source>"))``). There is
    intentionally no class-based or :func:`find_data_source` path here.

    Args:
        spark: The active SparkSession.
        source: A source name, e.g. ``"gmail"``.

    Raises:
        TypeError: If source is not a string.
        ValueError: If the source name has no source package.
        ModuleNotFoundError: If the source package has no merged module.
        AttributeError: If the merged module lacks ``register_lakeflow_source``.

    Examples:
        >>> register(spark, "gmail")
    """
    if not isinstance(source, str):
        raise TypeError(
            f"register() takes a source name string, got {type(source)}. "
            f"To register a DataSource class, call spark.dataSource.register(...) "
            f"directly (e.g. spark.dataSource.register(GmailDataSource))."
        )
    register_fn = _get_register_function(source)
    register_fn(spark)
