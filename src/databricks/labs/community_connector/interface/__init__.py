"""LakeflowConnect base interface for source connectors."""

from databricks.labs.community_connector.interface.agent_protocol import (
    FRAMEWORK_PROTOCOL_VERSION,
    AgentError,
    ErrorCode,
    Parameter,
)
from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_ingestion_agent import (
    AgentOperation,
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
    "FRAMEWORK_PROTOCOL_VERSION",
    "AgentError",
    "AgentOperation",
    "ErrorCode",
    "LakeflowConnect",
    "Parameter",
    "SupportsIngestionAgent",
    "SupportsNamespaces",
    "SupportsPartition",
    "SupportsPartitionedStream",
]
