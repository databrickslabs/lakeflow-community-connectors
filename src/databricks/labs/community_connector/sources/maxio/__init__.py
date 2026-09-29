"""Maxio (SaaSOptics) source connector."""

from databricks.labs.community_connector.sources.maxio.maxio import (
    MaxioLakeflowConnect,
    TABLE_SCHEMAS,
)

__all__ = ["MaxioLakeflowConnect", "TABLE_SCHEMAS"]
