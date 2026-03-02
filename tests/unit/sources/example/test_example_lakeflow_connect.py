from pathlib import Path

from databricks.labs.community_connector.libs.simulated_source.api import reset_api
from databricks.labs.community_connector.sources.example.example import ExampleLakeflowConnect
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config


def test_example_connector():
    """Test the example connector using the test suite.

    The example source uses a simulated in-memory API that is highly scalable
    and has no rate limits, so we can afford a large ``sample_records`` value
    to thoroughly validate record iteration.  For real third-party APIs, keep
    the default (10) or lower to avoid rate-limiting and slow test runs.
    """
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    # Reset the simulated API singleton so each test run starts fresh.
    reset_api(config["username"], config["password"])

    test_suite.LakeflowConnect = ExampleLakeflowConnect

    tester = LakeflowConnectTester(config, table_config, sample_records=100)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
