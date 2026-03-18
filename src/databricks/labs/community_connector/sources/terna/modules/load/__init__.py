"""Load table readers (total_load, market_load, peak_valley_load)."""

from databricks.labs.community_connector.sources.terna.modules.load.market_load_reader import (
    MarketLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.peak_valley_load_reader import (
    PeakValleyLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.peak_valley_load_details_reader import (
    PeakValleyLoadDetailsReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.total_load_reader import (
    TotalLoadReader,
)
from databricks.labs.community_connector.sources.terna.modules.load.monthly_index_industrial_electrical_consumption_reader import (
    MonthlyIndexIndustrialElectricalConsumptionReader,
)

__all__ = ["MarketLoadReader", "TotalLoadReader", "PeakValleyLoadReader", "PeakValleyLoadDetailsReader", "MonthlyIndexIndustrialElectricalConsumptionReader"]