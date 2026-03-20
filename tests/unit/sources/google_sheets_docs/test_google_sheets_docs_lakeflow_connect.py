"""Lakeflow Connect test suite for the google_sheets_docs connector."""

import pytest

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleSheetsDocsConnector(LakeflowConnectTests):
    connector_class = GoogleSheetsDocsLakeflowConnect

    @classmethod
    def setup_class(cls):
        if not (cls._config_dir() / "dev_config.json").exists():
            pytest.skip(
                "dev_config.json not found; add it locally with OAuth2 credentials to run this test"
            )
        super().setup_class()
