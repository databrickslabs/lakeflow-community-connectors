"""
Tests for the AppsFlyer LakeflowConnect connector.

To run these tests:
1. Copy the example configs and add your credentials:
   cp sources/appsflyer/configs/dev_config.json \
      sources/appsflyer/configs/local_config.json
   cp sources/appsflyer/configs/dev_table_config.json \
      sources/appsflyer/configs/local_table_config.json

2. Edit local_config.json with your API token:
   {
     "api_token": "YOUR_ACTUAL_API_TOKEN",
     "base_url": "https://hq1.appsflyer.com"
   }

3. Edit local_table_config.json with your app configuration and table-specific options.
   See dev_table_config.json for the expected format.

4. Run: pytest sources/appsflyer/test/test_appsflyer_lakeflow_connect.py -v

Note: local_*.json files are git-ignored for security.
"""

from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.appsflyer.appsflyer import LakeflowConnect


def test_appsflyer_connector():
    """Test the AppsFlyer connector using the shared LakeflowConnect test suite."""
    # Inject the AppsFlyer LakeflowConnect class into the shared test_suite namespace
    # so that LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration (e.g. api_token, base_url)
    # Try local_config.json first (with actual credentials), fall back to dev_config.json
    parent_dir = Path(__file__).parent.parent
    local_config_path = parent_dir / "configs" / "local_config.json"
    dev_config_path = parent_dir / "configs" / "dev_config.json"
    config_path = local_config_path if local_config_path.exists() else dev_config_path

    # Load table-specific configuration
    local_table_config_path = parent_dir / "configs" / "local_table_config.json"
    dev_table_config_path = parent_dir / "configs" / "dev_table_config.json"
    table_config_path = (
        local_table_config_path if local_table_config_path.exists()
        else dev_table_config_path
    )

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
