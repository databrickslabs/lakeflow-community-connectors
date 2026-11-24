"""Microsoft Teams source connector."""

from databricks.labs.community_connector.sources.microsoft_teams.microsoft_teams import (
    MicrosoftTeamsLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class MicrosoftTeamsDataSource(LakeflowSource):
    _lakeflow_connect_cls = MicrosoftTeamsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "microsoft_teams"


__all__ = ["MicrosoftTeamsLakeflowConnect",
    "MicrosoftTeamsDataSource",
]
