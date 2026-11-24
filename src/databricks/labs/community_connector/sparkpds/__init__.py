"""
Spark Python Data Source (PDS) module for Lakeflow Community Connectors.

This module provides the infrastructure for registering LakeflowSource
data sources with Spark. The ingestion-agent operation surface lives
inside the same ``lakeflow_connect`` format — set the ``operation``
option to dispatch to the agent surface instead of the regular table
read path.
"""

from databricks.labs.community_connector.sparkpds.registry import (
    register,
    find_data_source,
)
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    LakeflowSource,
    LakeflowStreamReader,
    LakeflowBatchReader,
)
from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
    GetObjectMetadataOp,
    IngestionAgentDispatcher,
    IngestionAgentReader,
    ListObjectsOp,
    ListOperationsOp,
    ReadTableOp,
    ValidateConnectionOp,
)

__all__ = [
    # Registry
    "register",
    "find_data_source",
    # Core classes
    "LakeflowSource",
    "LakeflowStreamReader",
    "LakeflowBatchReader",
    # Built-in ingestion-agent operations — subclass these to customise
    # behaviour while keeping the framework's schema / dispatch contract.
    "ListObjectsOp",
    "ReadTableOp",
    "GetObjectMetadataOp",
    "ValidateConnectionOp",
    "ListOperationsOp",
    # Internal dispatcher for the agent-operation path on lakeflow_connect.
    # Exposed for unit testing; not a registered Spark format on its own.
    "IngestionAgentDispatcher",
    "IngestionAgentReader",
]
