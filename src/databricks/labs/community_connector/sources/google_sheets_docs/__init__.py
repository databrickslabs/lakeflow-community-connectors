"""Google Sheets and Google Docs source connector.

This package provides the LakeflowConnect implementation for ingesting
Google Sheets and Google Docs data via the Drive, Sheets, and Docs APIs.
Use GoogleSheetsDocsLakeflowConnect with OAuth 2.0 credentials
(client_id, client_secret, refresh_token).
"""

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)

__all__ = ["GoogleSheetsDocsLakeflowConnect"]
