from databricks.labs.community_connector.sources.dicomweb.dicomweb import DICOMwebLakeflowConnect


from databricks.labs.community_connector.sparkpds import LakeflowSource


class DICOMwebDataSource(LakeflowSource):
    _lakeflow_connect_cls = DICOMwebLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "dicomweb"


__all__ = ["DICOMwebLakeflowConnect",
    "DICOMwebDataSource",
]
