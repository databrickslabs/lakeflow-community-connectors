"""Lakeflow Connect test suite for the google_sheets_docs connector."""

import sys
from pathlib import Path

import pytest

# Ensure project root is on path so "tests" is importable when run from CI.
# Test was failing because the project root was not on the path; this fix allows the test to pass.
_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.google_sheets_docs.google_sheets_docs import (
    GoogleSheetsDocsLakeflowConnect,
)
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config


def _load_table_config(config_dir: Path) -> dict:
    """Load table config from dev_table_config.json if it exists, else return {}."""
    path = config_dir / "dev_table_config.json"
    if not path.exists():
        return {}
    return load_config(path)


def _is_placeholder(value: str) -> bool:
    """Return True if the value looks like a placeholder or is empty."""
    if not value or not isinstance(value, str):
        return True
    placeholders = ["YOUR_", "REPLACE_", "dummy", "xxx", "***"]
    return any(p in value for p in placeholders)


def _has_sheet_values_config(table_config: dict) -> bool:
    """Return True if sheet_values has a valid (non-placeholder) spreadsheet_id."""
    opts = table_config.get("sheet_values") or {}
    sid = opts.get("spreadsheet_id") or opts.get("spreadsheetId") or ""
    return bool(sid) and not _is_placeholder(sid)


def _probe_drive_access(connector) -> None:
    """Probe Drive API; skip the test if we get 403 (credentials lack Drive scope)."""
    try:
        it, _ = connector.read_table("spreadsheets", {}, {})
        next(iter(it), None)
    except Exception as e:
        if "403" in str(e) or "Forbidden" in str(e):
            pytest.skip(
                "Drive API returned 403 Forbidden. Ensure dev_config.json credentials "
                "have Drive scope (e.g. https://www.googleapis.com/auth/drive.readonly) "
                "and that the Google Cloud project has the Drive API enabled."
            )
        raise


def test_google_sheets_docs_connector():
    """Run the Lakeflow Connect test suite for google_sheets_docs.

    Uses dev_config.json for OAuth2 (client_id, client_secret, refresh_token).
    Optionally uses dev_table_config.json for table_options (e.g. sheet_values
    spreadsheet_id). If sheet_values has no valid spreadsheet_id, that table
    is excluded from the suite so tests can pass without a sample spreadsheet.
    Set dev_table_config.json with a real spreadsheet_id to test sheet_values.

    If the Drive API returns 403, the test is skipped; ensure credentials have
    Drive scope (e.g. drive.readonly) and the project has the Drive API enabled.
    """
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = _load_table_config(config_dir)

    if not _has_sheet_values_config(table_config):

        class _ConnectorWithoutSheetValues(GoogleSheetsDocsLakeflowConnect):
            """Excludes sheet_values from list_tables when no spreadsheet_id is configured."""

            def list_tables(self):
                return ["spreadsheets", "documents"]

        test_suite.LakeflowConnect = _ConnectorWithoutSheetValues
    else:
        test_suite.LakeflowConnect = GoogleSheetsDocsLakeflowConnect

    connector = test_suite.LakeflowConnect(config)
    _probe_drive_access(connector)

    tester = LakeflowConnectTester(config, table_config, sample_records=10)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
