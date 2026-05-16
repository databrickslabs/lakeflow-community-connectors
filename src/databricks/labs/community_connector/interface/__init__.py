"""LakeflowConnect base interface for source connectors."""

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_ingestion_agent import (
    SupportsIngestionAgent,
)
from databricks.labs.community_connector.interface.supports_namespaces import (
    SupportsNamespaces,
)
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartition,
    SupportsPartitionedStream,
)

__all__ = [
    "LakeflowConnect",
    "SupportsIngestionAgent",
    "SupportsNamespaces",
    "SupportsPartition",
    "SupportsPartitionedStream",
]
