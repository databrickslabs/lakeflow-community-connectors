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
)

__all__ = [
    # Registry
    "register",
    # Core classes
    "LakeflowSource",
    "LakeflowStreamReader",
    "LakeflowBatchReader",
]
