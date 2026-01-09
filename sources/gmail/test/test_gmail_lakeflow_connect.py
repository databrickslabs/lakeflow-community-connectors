from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.gmail.gmail import LakeflowConnect


def test_gmail_connector():
    """Test the Gmail connector using the shared LakeflowConnect test suite.

    This test validates:
    - Connector initialization with OAuth credentials
    - list_tables() returns all 10 supported tables
    - get_table_schema() returns valid StructType for each table
    - read_table_metadata() returns required metadata (primary_keys, ingestion_type)
    - read_table() successfully fetches data from Gmail API
    - Custom table options (maxResults, q, labelIds, format) work correctly

    To run these tests:
    1. Copy dev_config.json.template to dev_config.json
    2. Add your OAuth credentials (see README.md for setup instructions)
    3. Run: PYTHONPATH=. pytest sources/gmail/test/test_gmail_lakeflow_connect.py -v
    """
    # Inject the Gmail LakeflowConnect class into the shared test_suite namespace
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
