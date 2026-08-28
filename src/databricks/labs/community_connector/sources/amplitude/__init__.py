"""Amplitude source connector."""

from databricks.labs.community_connector.sources.amplitude.amplitude import (
    AmplitudeLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class AmplitudeDataSource(LakeflowSource):
    _lakeflow_connect_cls = AmplitudeLakeflowConnect


__all__ = [
    "AmplitudeLakeflowConnect",
    "AmplitudeDataSource",
]
