"""Bluesky source discovery for Lakeflow's wheel-based registry."""

from collections.abc import Mapping

from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sources.bluesky.options import Options
from databricks.labs.community_connector.sparkpds import LakeflowSource


class _LakeflowConfigAdapter(BlueskyLakeflowConnect):
    """Adapt Lakeflow's framework mapping to the connector's typed config.

    Parameters
    ----------
    options : Mapping[str, str]
        Connection and framework options passed by :class:`LakeflowSource`.

    Notes
    -----
    This adapter is the framework boundary. Connector code receives an
    :class:`Options` dataclass instead of an unvalidated mapping.
    """

    def __init__(self, options: Mapping[str, str]) -> None:
        super().__init__(Options.parse(options, {}))


class BlueskyDataSource(LakeflowSource):
    """Register the Bluesky connector with Lakeflow's Spark data source API.

    Attributes
    ----------
    _lakeflow_connect_cls : type
        Adapter that converts framework options into :class:`Options`.
    _format_name : str
        Format identifier used for Unity Catalog connection option injection.
    """

    _lakeflow_connect_cls = _LakeflowConfigAdapter
    # UC connection injection currently requires this framework format name.
    _format_name = "lakeflow_connect"


__all__ = ["BlueskyDataSource", "BlueskyLakeflowConnect"]
