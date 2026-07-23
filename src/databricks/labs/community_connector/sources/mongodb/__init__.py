"""MongoDB community connector for Lakeflow Community Connectors."""

from databricks.labs.community_connector.sources.mongodb.mongodb import (
    MongodbLakeflowConnect,
)

__all__ = ["MongodbLakeflowConnect"]
