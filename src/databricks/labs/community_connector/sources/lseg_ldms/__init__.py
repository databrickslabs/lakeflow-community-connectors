"""LSEG LDMS (RDMS) source connector."""

from databricks.labs.community_connector.sources.lseg_ldms.lseg_ldms import (
    LSEGLDMSLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class LSEGLDMSDataSource(LakeflowSource):
    _lakeflow_connect_cls = LSEGLDMSLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "lseg_ldms"


__all__ = [
    "LSEGLDMSLakeflowConnect",
    "LSEGLDMSDataSource",
]
