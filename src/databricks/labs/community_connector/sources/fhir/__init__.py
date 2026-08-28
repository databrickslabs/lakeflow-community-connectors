from databricks.labs.community_connector.sources.fhir.fhir import (
    FhirLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class FhirDataSource(LakeflowSource):
    _lakeflow_connect_cls = FhirLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "fhir"


__all__ = ["FhirLakeflowConnect", "FhirDataSource"]
