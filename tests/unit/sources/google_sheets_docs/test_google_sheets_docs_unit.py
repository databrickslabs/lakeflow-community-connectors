"""Unit tests for google_sheets_docs that do not require credentials or live API."""

import json

import pytest

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)


def test_resolve_table_options_extracts_from_table_configs():
    """When options contain tableConfigs (pipeline style), per-table config is extracted."""
    table_configs = {
        "sheet_values": {
            "spreadsheet_id": "abc123",
            "use_first_row_as_header": "true",
            "sheet_name": "Sheet1",
        },
    }
    options = {
        "tableName": "sheet_values",
        "tableConfigs": json.dumps(table_configs),
    }
    resolved = GoogleSheetsDocsLakeflowConnect._resolve_table_options(
        "sheet_values", options
    )
    assert resolved.get("spreadsheet_id") == "abc123"
    assert resolved.get("use_first_row_as_header") == "true"
    assert resolved.get("sheet_name") == "Sheet1"
    assert "tableConfigs" not in resolved


def test_resolve_table_options_passthrough_when_no_table_configs():
    """When options have no tableConfigs, they are returned as-is (e.g. test suite path)."""
    options = {
        "spreadsheet_id": "xyz789",
        "use_first_row_as_header": "true",
    }
    resolved = GoogleSheetsDocsLakeflowConnect._resolve_table_options(
        "sheet_values", options
    )
    assert resolved is options
    assert resolved.get("spreadsheet_id") == "xyz789"
