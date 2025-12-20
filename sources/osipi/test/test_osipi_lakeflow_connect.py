import sys
from pathlib import Path

# Ensure repo root is importable under different pytest import modes.
# (Connector tests in this repo import from the top-level `tests` package.)
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

import pytest

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.osipi.osipi import LakeflowConnect


def _is_blank(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def test_osipi_connector():
    """
    Test the OSI PI connector using the shared LakeflowConnect test suite.

    Notes:
    - This connector typically requires a real PI Web API endpoint + a valid Bearer token.
    - For local development you can point `pi_base_url` at a mock server.
    - If `dev_config.json` is not configured, this test is skipped.
    """

    # Inject into the shared test_suite namespace so LakeflowConnectTester can instantiate it.
    test_suite.LakeflowConnect = LakeflowConnect

    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    if _is_blank(config.get("pi_base_url")):
        pytest.skip("Configure sources/osipi/configs/dev_config.json (pi_base_url) to run this test.")

    # Token can be blank when testing against an unsecured local mock server.
    # For real PI Web API with Bearer auth enabled, set access_token.

    tester = LakeflowConnectTester(config, table_config)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )


