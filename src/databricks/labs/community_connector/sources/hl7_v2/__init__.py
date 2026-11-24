"""HL7 v2 community connector."""

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import (
    HL7V2LakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class HL7V2DataSource(LakeflowSource):
    _lakeflow_connect_cls = HL7V2LakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "hl7_v2"


__all__ = ["HL7V2LakeflowConnect", "HL7V2DataSource"]
