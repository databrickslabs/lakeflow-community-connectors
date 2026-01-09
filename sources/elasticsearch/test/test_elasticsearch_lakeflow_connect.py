from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.elasticsearch.elasticsearch import LakeflowConnect


def test_elasticsearch_connector():
    """Test the Elasticsearch connector using the generic test suite and dev configs."""
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration (e.g. endpoint, index, authentication, api_key, verify_ssl)
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / "configs" / "dev_config.json"
    table_config_path = base_dir / "configs" / "dev_table_config.json"

    init_options = load_config(config_path)
    table_configs = load_config(table_config_path)

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(init_options, table_configs)

    # Run all standard LakeflowConnect tests for this connector
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
