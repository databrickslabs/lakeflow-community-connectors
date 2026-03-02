import json
import pytest
from pathlib import Path

# Import test suite and connector
import tests.unit.sources.test_suite as test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from databricks.labs.community_connector.sources.dicomweb.dicomweb import DICOMwebLakeflowConnect


def load_config():
    """Load connection configuration from dev_config.json"""
    config_path = Path(__file__).parent / "configs" / "dev_config.json"
    with open(config_path, "r") as f:
        return json.load(f)


def load_table_config():
    """Load per-table options from dev_table_config.json"""
    config_path = Path(__file__).parent / "dev_table_config.json"
    if config_path.exists():
        with open(config_path, "r") as f:
            return json.load(f)
    return {}


def test_dicomweb_connector():
    """Test the DICOMweb connector using the standard test suite."""
    test_suite.LakeflowConnect = DICOMwebLakeflowConnect

    config = load_config()
    table_config = load_table_config()

    tester = LakeflowConnectTester(config, table_configs=table_config)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
