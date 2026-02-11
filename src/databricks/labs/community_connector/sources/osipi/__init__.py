"""OSI PI Lakeflow Community Connector.

This package provides a connector for reading data from OSI PI Web API.
"""

from databricks.labs.community_connector.sources.osipi.osipi import OsipiLakeflowConnect
from databricks.labs.community_connector.sources.osipi.osipi_constants import (
    SUPPORTED_TABLES,
    TABLES_ASSET_FRAMEWORK,
    TABLES_DISCOVERY_INVENTORY,
    TABLES_EVENT_FRAMES,
    TABLES_GOVERNANCE_DIAGNOSTICS,
    TABLES_TIME_SERIES,
)
from databricks.labs.community_connector.sources.osipi.osipi_schemas import (
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

__all__ = [
    "OsipiLakeflowConnect",
    "SUPPORTED_TABLES",
    "TABLES_DISCOVERY_INVENTORY",
    "TABLES_TIME_SERIES",
    "TABLES_ASSET_FRAMEWORK",
    "TABLES_EVENT_FRAMES",
    "TABLES_GOVERNANCE_DIAGNOSTICS",
    "TABLE_SCHEMAS",
    "TABLE_METADATA",
]
