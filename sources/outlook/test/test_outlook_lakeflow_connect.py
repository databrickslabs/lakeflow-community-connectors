from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.outlook.outlook import LakeflowConnect


def test_outlook_connector():
    """Test the Outlook connector using the shared LakeflowConnect test suite."""
    # Inject the Outlook LakeflowConnect class into the shared test_suite namespace
    # so that LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration (OAuth credentials)
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(config, table_config)

    # Run all standard LakeflowConnect tests for this connector
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, "
        f"{report.error_tests} errors"
    )

