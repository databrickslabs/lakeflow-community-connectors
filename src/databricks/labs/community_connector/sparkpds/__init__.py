"""
Spark Python Data Source (PDS) module for Lakeflow Community Connectors.

This module provides the infrastructure for registering LakeflowSource
data sources with Spark.
"""

from databricks.labs.community_connector.sparkpds.registry import (
    register,
)
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    LakeflowSource,
    LakeflowStreamReader,
    LakeflowBatchReader,
    METADATA_TABLE,
    TABLE_NAME,
    TABLE_NAME_LIST,
    TABLE_CONFIGS,
    IS_DELETE_FLOW,
)

__all__ = [
    # Registry
    "register",
    # Core classes
    "LakeflowSource",
    "LakeflowStreamReader",
    "LakeflowBatchReader",
    # Constants
    "METADATA_TABLE",
    "TABLE_NAME",
    "TABLE_NAME_LIST",
    "TABLE_CONFIGS",
    "IS_DELETE_FLOW",
]
