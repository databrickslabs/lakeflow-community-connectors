"""Google Sheets and Google Docs source connector.

This package provides the LakeflowConnect implementation for ingesting
Google Sheets and Google Docs data via the Drive, Sheets, and Docs APIs.
Use GoogleSheetsDocsLakeflowConnect with OAuth 2.0 credentials
(client_id, client_secret, refresh_token).
"""

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class GoogleSheetsDocsDataSource(LakeflowSource):
    _lakeflow_connect_cls = GoogleSheetsDocsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "google_sheets_docs"


__all__ = ["GoogleSheetsDocsLakeflowConnect",
    "GoogleSheetsDocsDataSource",
]
