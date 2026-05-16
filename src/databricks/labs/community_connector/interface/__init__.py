"""LakeflowConnect base interface for source connectors."""

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_ingestion_agent import (
    AgentOperation,
    SupportsIngestionAgent,
    agent_operation,
)
from databricks.labs.community_connector.interface.supports_namespaces import (
    SupportsNamespaces,
)
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartition,
    SupportsPartitionedStream,
)

__all__ = [
    "AgentOperation",
    "LakeflowConnect",
    "SupportsIngestionAgent",
    "SupportsNamespaces",
    "SupportsPartition",
    "SupportsPartitionedStream",
    "agent_operation",
]
