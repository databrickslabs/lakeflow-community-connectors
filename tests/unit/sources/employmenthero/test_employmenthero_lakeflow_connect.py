# Fixtures (employees_data, _patch_time_sleep) and constants (CONFIGS_DIR, DATA_PATH)
# are defined in conftest.py and are available to all tests in this directory.

from unittest.mock import MagicMock

import pytest

from databricks.labs.community_connector.sources.employmenthero.employmenthero import EmploymentHeroLakeflowConnect
from databricks.labs.community_connector.sources.employmenthero.employmenthero_client import EmploymentHeroAPIClient
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config

from tests.unit.sources.employmenthero.conftest import CONFIGS_DIR


# =============================================================================
# Offline Unit Tests
# =============================================================================

def test_employmenthero_connector_reads_snapshot_table(employees_data: dict):
    """Test that read_table uses the client's paginate and returns the expected records.

    Mocks EmploymentHeroAPIClient so paginate returns the 3 employee records from the fixture.
    Asserts paginate was called and that the result matches the expected records.
    read_table is not yet implemented, so this test is expected to fail initially (TDD).
    """
    # Build expected 3 records from fixture (page 1 items + page 2 items)
    expected_records = (
        list(employees_data["pages"][0]["items"])
        + list(employees_data["pages"][1]["items"])
    )
    organisation_id = "test-org-id"
    expected_endpoint = f"/api/v1/organisations/{organisation_id}/employees"

    mock_client = MagicMock(spec=EmploymentHeroAPIClient)
    mock_client.paginate.return_value = iter(expected_records)

    options = {
        "client_id": "test_id",
        "client_secret": "test_secret",
        "redirect_uri": "https://example.com/callback",
        "authorization_code": "test_code",
    }
    connector = EmploymentHeroLakeflowConnect(options=options, client=mock_client)

    # ---- Run test ----
    records_iter, next_offset = connector.read_table(
        table_name="employees",
        start_offset={},
        table_options={"organisation_id": organisation_id},
    )

    # Connector should use the client to fetch the data
    mock_client.paginate.assert_called_once_with(
        endpoint=expected_endpoint,
        params={},
        data_key="data",
        per_page=200,
    )
    records = list(records_iter)
    assert records == expected_records
    assert next_offset == {}


def test_employmenthero_connector_reads_snapshot_table_with_start_date(timesheet_entries_data):
    """Read snapshot table passing in the start_date parameter.

    Mocks EmploymentHeroAPIClient so paginate returns the 2 timesheet entry records from the fixture.
    Asserts paginate was called with the start_date param and that the special timesheet_entries
    endpoint (with employee "-" wildcard) is constructed correctly.
    """
    expected_records = list(timesheet_entries_data["pages"][0]["items"])
    organisation_id = "test-org-id"
    start_date = "01/02/2025"  # dd/mm/yyyy per Employment Hero API
    expected_endpoint = f"/api/v1/organisations/{organisation_id}/employees/-/timesheet_entries"

    mock_client = MagicMock(spec=EmploymentHeroAPIClient)
    mock_client.paginate.return_value = iter(expected_records)

    options = {
        "client_id": "test_id",
        "client_secret": "test_secret",
        "redirect_uri": "https://example.com/callback",
        "authorization_code": "test_code",
    }
    connector = EmploymentHeroLakeflowConnect(options=options, client=mock_client)

    records_iter, next_offset = connector.read_table(
        table_name="timesheet_entries",
        start_offset={},
        table_options={
            "organisation_id": organisation_id,
            "start_date": start_date,
        },
    )

    mock_client.paginate.assert_called_once_with(
        endpoint=expected_endpoint,
        params={"start_date": start_date},
        data_key="data",
        per_page=200,
    )
    records = list(records_iter)
    assert records == expected_records
    assert next_offset == {}


def test_employmenthero_connector_reads_organisations(organisations_data: dict):
    """Read data from the top-level organisations endpoint; no organisation_id required.

    Mocks EmploymentHeroAPIClient so paginate returns the 2 organisation records from the fixture.
    Asserts paginate was called with /api/v1/organisations (no organisation_id in path) and
    that the result matches the expected records.
    """
    expected_records = list(organisations_data["pages"][0]["items"])
    expected_endpoint = "/api/v1/organisations"

    mock_client = MagicMock(spec=EmploymentHeroAPIClient)
    mock_client.paginate.return_value = iter(expected_records)

    options = {
        "client_id": "test_id",
        "client_secret": "test_secret",
        "redirect_uri": "https://example.com/callback",
        "authorization_code": "test_code",
    }
    connector = EmploymentHeroLakeflowConnect(options=options, client=mock_client)

    records_iter, next_offset = connector.read_table(
        table_name="organisations",
        start_offset={},
        table_options={},
    )

    mock_client.paginate.assert_called_once_with(
        endpoint=expected_endpoint,
        params={},
        data_key="data",
        per_page=200,
    )
    records = list(records_iter)
    assert records == expected_records
    assert next_offset == {}


def test_employmenthero_connector_fails_on_unsupported_table():
    """Unsupported table name raises ValueError; client is not called."""
    mock_client = MagicMock(spec=EmploymentHeroAPIClient)
    connector = EmploymentHeroLakeflowConnect(options={}, client=mock_client)

    with pytest.raises(ValueError, match=r"Unsupported table: 'unsupported_table'"):
        connector.read_table(
            table_name="unsupported_table",
            start_offset={},
            table_options={},
        )

    mock_client.paginate.assert_not_called()


def test_employmenthero_connector_fails_on_missing_organisation_id():
    """Reading employees table without organisation_id in table_options raises ValueError; client is not called."""
    mock_client = MagicMock(spec=EmploymentHeroAPIClient)
    connector = EmploymentHeroLakeflowConnect(options={}, client=mock_client)

    with pytest.raises(ValueError, match=r"table_options must contain 'organisation_id'"):
        connector.read_table(
            table_name="employees",
            start_offset={},
            table_options={},
        )

    mock_client.paginate.assert_not_called()


# =============================================================================
# Live Generic Integration Test Suite
# =============================================================================

def test_employmenthero_connector_live():
    """Test the Employment Hero connector using the live test suite"""
    # Inject the Employment Hero LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = EmploymentHeroLakeflowConnect

    # Load configuration (CONFIGS_DIR from conftest.py)
    config_path = CONFIGS_DIR / "dev_config.json"
    table_config_path = CONFIGS_DIR / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create tester with the config
    tester = LakeflowConnectTester(config, table_config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
