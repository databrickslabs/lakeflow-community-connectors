"""Palantir Foundry connector for Lakeflow Community Connectors."""

from databricks.labs.community_connector.sources.palantir.palantir import PalantirLakeflowConnect
from databricks.labs.community_connector.sparkpds import LakeflowSource


class PalantirDataSource(LakeflowSource):
    _lakeflow_connect_cls = PalantirLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "palantir"


__all__ = ["PalantirLakeflowConnect", "PalantirDataSource"]
