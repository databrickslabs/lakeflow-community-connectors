from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.zoho_crm.zoho_crm import LakeflowConnect


def test_zoho_crm_connector():
    """Test the Zoho CRM connector using the shared LakeflowConnect test suite."""
    # Inject the Zoho CRM LakeflowConnect class into the shared test_suite namespace
    # so that LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration (client_id, client_secret, refresh_token, base_url, start_date)
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)

    # Load table config if it exists, otherwise use empty dict
    try:
        table_config = load_config(table_config_path)
    except FileNotFoundError:
        # Zoho CRM modules don't require per-table options for basic testing
        table_config = {}

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(config, table_config)

    # Run all standard LakeflowConnect tests for this connector
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, f"Test suite had failures: {report.failed_tests} failed, " f"{report.error_tests} errors"
