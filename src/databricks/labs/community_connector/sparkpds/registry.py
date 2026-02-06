"""
Registry module for registering LakeflowSource with Spark's DataSource API.

This module provides functions to register a LakeflowConnect implementation
with Spark, making it available as a Python Data Source.
"""

import importlib
from typing import Type
from pyspark.sql.datasource import DataSource

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource


def _get_class_fqn(cls: Type) -> str:
    """Get the fully qualified name of a class (module.ClassName)."""
    return f"{cls.__module__}.{cls.__name__}"


def _import_class(fqn: str) -> Type:
    """
    Dynamically import a class from its fully qualified name.

    Args:
        fqn: Fully qualified class name (e.g., 'module.submodule.ClassName')

    Returns:
        The imported class.

    Raises:
        ImportError: If the module cannot be imported.
        AttributeError: If the class doesn't exist in the module.
    """
    module_name, class_name = fqn.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def register_pds(
    spark,
    pds_class: Type[DataSource],
) -> Type[DataSource]:
    """
    Register a Python Data Source (PDS) class directly with Spark's DataSource API.

    This is the low-level registration function that takes a pre-built PDS class
    and registers it with Spark.

    Args:
        spark: The SparkSession instance.
        pds_class: The DataSource class to register (must implement name() classmethod).

    Returns:
        The registered DataSource class.

    Example:
        >>> from my_pds import MyCustomPDS
        >>> register_pds(spark, MyCustomPDS)
        >>> # Now you can use it in Spark:
        >>> df = spark.read.format(MyCustomPDS.name()).options(...).load()
    """
    spark.dataSource.register(pds_class)
    return pds_class


def register(
    spark,
    lakeflow_connect_cls: Type[LakeflowConnect],
) -> Type[DataSource]:
    """
    Register a LakeflowConnect implementation with Spark's DataSource API.

    This function creates a DataSource wrapper that automatically injects
    the LakeflowConnect class reference into options, allowing the class
    to be dynamically imported on Spark executors.

    Args:
        spark: The SparkSession instance.
        lakeflow_connect_cls: The LakeflowConnect class implementation to use.

    Returns:
        The registered DataSource class.

    Example:
        >>> from my_connector import MyLakeflowConnect
        >>> register(spark, MyLakeflowConnect)
        >>> # Now you can use it in Spark:
        >>> df = spark.read.format("lakeflow_connect").options(...).load()
    """
    # Get the fully qualified class name.
    class_fqn = _get_class_fqn(lakeflow_connect_cls)

    # Create a wrapper class that dynamically imports the LakeflowConnect class
    class RegisterableLakeflowSource(LakeflowSource):
        """
        A LakeflowSource that dynamically imports the LakeflowConnect class.

        This class is used when registering a connector via the registry.register()
        function. It dynamically imports the LakeflowConnect class from the fully
        qualified name, allowing it to work across Spark driver and executor processes.
        """

        def __init__(self, options):
            self.options = options
            # Dynamically import the LakeflowConnect class from FQN
            lakeflow_connect_cls = _import_class(class_fqn)
            self.lakeflow_connect = lakeflow_connect_cls(options)

    # Register the wrapper with Spark
    return register_pds(spark, RegisterableLakeflowSource)
