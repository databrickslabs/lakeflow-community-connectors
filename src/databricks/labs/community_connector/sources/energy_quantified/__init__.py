"""Energy Quantified source connector."""

from databricks.labs.community_connector.sources.energy_quantified.energy_quantified import (
    EnergyQuantifiedLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class EnergyQuantifiedDataSource(LakeflowSource):
    _lakeflow_connect_cls = EnergyQuantifiedLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "energy_quantified"


__all__ = [
    "EnergyQuantifiedLakeflowConnect",
    "EnergyQuantifiedDataSource",
]
