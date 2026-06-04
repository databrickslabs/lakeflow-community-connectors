"""
Registry helpers for Lakeflow source DataSources.

Three ways to register a source with Spark, all valid:

1. Direct import (preferred for wheel-installed connectors)::

       from databricks.labs.community_connector.sources.gmail import GmailDataSource
       spark.dataSource.register(GmailDataSource)
       df = spark.read.format("gmail").option(...).load()

2. Lookup by source name (handy when the source is selected at runtime)::

       from databricks.labs.community_connector.sparkpds import find_data_source
       spark.dataSource.register(find_data_source("gmail"))

3. Legacy ``register()`` (used by pipeline templates)::

       from databricks.labs.community_connector import register
       register(spark, "gmail")

   This first tries the pre-merged single-file deployment
   (``_generated_<source>_python_source.py``) which registers as
   ``format("lakeflow_connect")``; if not present, it falls back to
   :func:`find_data_source` which registers as ``format("<source>")``.
"""

import importlib
import inspect
from types import ModuleType
from typing import Type, Union

from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource

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
        if (
            issubclass(obj, LakeflowSource)
            and obj is not LakeflowSource
            and getattr(obj, "__module__", "").startswith(package_fqn)
        ):
            return obj

    raise ValueError(
        f"Could not find a LakeflowSource subclass for source '{source_name}'. "
        f"Expose a ``<Source>DataSource`` class from '{package_fqn}.__init__' "
        f"that sets '_lakeflow_connect_cls' and '_format_name'."
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

    Raises ``ModuleNotFoundError`` when the source has no merged file, so the
    caller can fall back to the per-source DataSource class lookup.
    """
    module_name = f"_generated_{source_name}_python_source"
    module = _get_source_module(source_name, module_name)

    if not hasattr(module, "register_lakeflow_source"):
        raise AttributeError(
            f"Module '{module_name}' does not have a 'register_lakeflow_source' function."
        )

    return module.register_lakeflow_source


def register(
    spark: SparkSession,
    source: Union[str, Type[DataSource]],
) -> None:
    """Register a source with Spark's DataSource API.

    Args:
        spark: The active SparkSession.
        source: Either a source name (e.g. ``"gmail"``) or a ``DataSource``
            subclass. Strings first attempt the legacy pre-merged module
            (registers as ``format("lakeflow_connect")``); on miss they fall
            back to :func:`find_data_source` (registers as
            ``format("<source>")``).

    Examples:
        >>> register(spark, "gmail")
        >>> # Or directly:
        >>> from .gmail import GmailDataSource
        >>> spark.dataSource.register(GmailDataSource)
    """
    if isinstance(source, str):
        try:
            register_fn = _get_register_function(source)
        except (ModuleNotFoundError, AttributeError):
            spark.dataSource.register(find_data_source(source))
            return
        register_fn(spark)
        return

    if isinstance(source, type) and issubclass(source, DataSource):
        spark.dataSource.register(source)
        return

    raise TypeError(
        f"source must be a string or DataSource subclass, got {type(source)}"
    )
