from pathlib import Path

import tests.unit.sources.test_suite as test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config
from databricks.labs.community_connector.sources.qualtrics.qualtrics import QualtricsLakeflowConnect
from databricks.labs.community_connector.sources.qualtrics.qualtrics_test_utils import (
    LakeflowConnectTestUtils,
)


def test_qualtrics_connector():
    """Test the Qualtrics connector using the shared LakeflowConnect test suite."""
    # Inject the Qualtrics LakeflowConnect class into the shared test_suite namespace
    # so that LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = QualtricsLakeflowConnect

    # Inject the Qualtrics test utils for write-back testing
    test_suite.LakeflowConnectTestUtils = LakeflowConnectTestUtils

    # Load connection-level configuration (e.g. api_token, datacenter_id)
    config_dir = Path(__file__).parent / "configs"
    config_path = config_dir / "dev_config.json"
    table_config_path = config_dir / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(config, table_config)

    # Run all standard LakeflowConnect tests for this connector
    # This now includes write-back tests since we injected LakeflowConnectTestUtils
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, "
        f"{report.error_tests} errors"
    )
