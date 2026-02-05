"""
Spark Python Data Source (PDS) module for Lakeflow Community Connectors.

This module provides the infrastructure for registering LakeflowSource
data sources with Spark.
"""

from databricks.labs.community_connector.sparkpds.registry import (
    register,
    register_pds,
    set_lakeflow_connect_class,
    get_lakeflow_connect_class,
    RegisterableLakeflowSource,
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
    "register_pds",
    "set_lakeflow_connect_class",
    "get_lakeflow_connect_class",
    "RegisterableLakeflowSource",
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
