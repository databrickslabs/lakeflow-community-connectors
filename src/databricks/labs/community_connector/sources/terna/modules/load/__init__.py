"""Load table readers (total_load, market_load)."""

from databricks.labs.community_connector.sources.terna.modules.load.market_load_reader import (
    MarketLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.total_load_reader import (
    TotalLoadReader,
)

__all__ = ["MarketLoadReader", "TotalLoadReader"]
