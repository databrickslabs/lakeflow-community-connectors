"""
Registry module for registering LakeflowSource with Spark's DataSource API.

This module provides functions to register a LakeflowConnect implementation
with Spark, making it available as a Python Data Source.
"""

from typing import Type, Optional
from pyspark.sql.types import StructType
from pyspark.sql.datasource import DataSource

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    LakeflowSource,
    LakeflowStreamReader,
    LakeflowBatchReader,
)


# Module-level variable to hold the active LakeflowConnect implementation class.
_lakeflow_connect_cls: Optional[Type[LakeflowConnect]] = None


def set_lakeflow_connect_class(cls: Type[LakeflowConnect]) -> None:
    """
    Set the active LakeflowConnect implementation class.

    This should be called before registering RegisterableLakeflowSource with Spark.

    Args:
        cls: The LakeflowConnect implementation class to use.
    """
    global _lakeflow_connect_cls
    _lakeflow_connect_cls = cls


def get_lakeflow_connect_class() -> Type[LakeflowConnect]:
    """
    Get the active LakeflowConnect implementation class.

    Returns:
        The currently registered LakeflowConnect class.

    Raises:
        RuntimeError: If no LakeflowConnect class has been registered.
    """
    if _lakeflow_connect_cls is None:
        raise RuntimeError(
            "No LakeflowConnect implementation has been registered. "
            "Call register() before using RegisterableLakeflowSource."
        )
    return _lakeflow_connect_cls


class RegisterableLakeflowSource(LakeflowSource):
    """
    A subclass of LakeflowSource that uses a dynamically registered LakeflowConnect class.

    This class is used when registering a connector via the registry.register() function.
    It overrides __init__ to use the LakeflowConnect class that was set via
    set_lakeflow_connect_class() instead of importing it directly.
    """

    def __init__(self, options):
        self.options = options
        lakeflow_connect_cls = get_lakeflow_connect_class()
        self.lakeflow_connect = lakeflow_connect_cls(options)

    def reader(self, schema: StructType):
        return LakeflowBatchReader(self.options, schema, self.lakeflow_connect)

    def simpleStreamReader(self, schema: StructType):
        return LakeflowStreamReader(self.options, schema, self.lakeflow_connect)


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

    This function sets the active LakeflowConnect class and registers
    RegisterableLakeflowSource with Spark.

    Args:
        spark: The SparkSession instance.
        lakeflow_connect_cls: The LakeflowConnect class implementation to use.

    Returns:
        The registered RegisterableLakeflowSource class.

    Example:
        >>> from my_connector import MyLakeflowConnect
        >>> register(spark, MyLakeflowConnect)
        >>> # Now you can use it in Spark:
        >>> df = spark.read.format("lakeflow_connect").options(...).load()
    """
    # Set the active LakeflowConnect class
    set_lakeflow_connect_class(lakeflow_connect_cls)

    # Register RegisterableLakeflowSource with Spark
    return register_pds(spark, RegisterableLakeflowSource)
