"""Lakeflow Connect test suite for the google_sheets_docs connector."""

import pytest

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestGoogleSheetsDocsConnector(LakeflowConnectTests):
    connector_class = GoogleSheetsDocsLakeflowConnect
    simulator_source = "google_sheets_docs"

    @classmethod
    def setup_class(cls):
        config_dir = cls._config_dir()
        if not (
            (config_dir / "dev_config.json").exists()
            or (config_dir / "replay_config.json").exists()
        ):
            pytest.skip(
                "Neither dev_config.json nor replay_config.json found; "
                "add one with OAuth2 credentials (real or simulator placeholder)."
            )
        super().setup_class()
