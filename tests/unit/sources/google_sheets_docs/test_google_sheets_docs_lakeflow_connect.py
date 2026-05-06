"""Lakeflow Connect test suite for the google_sheets_docs connector."""

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleSheetsDocsConnector(LakeflowConnectTests):
    connector_class = GoogleSheetsDocsLakeflowConnect
    simulator_source = "google_sheets_docs"
    replay_config = {
        "client_id": "simulator-client-id",
        "client_secret": "simulator-client-secret",
        "refresh_token": "simulator-refresh-token",
    }
