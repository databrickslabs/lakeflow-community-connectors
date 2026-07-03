"""SurveyMonkey source connector."""

from databricks.labs.community_connector.sources.surveymonkey.surveymonkey import (
    SurveymonkeyLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class SurveymonkeyDataSource(LakeflowSource):
    _lakeflow_connect_cls = SurveymonkeyLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "surveymonkey"


__all__ = ["SurveymonkeyLakeflowConnect",
    "SurveymonkeyDataSource",
]
