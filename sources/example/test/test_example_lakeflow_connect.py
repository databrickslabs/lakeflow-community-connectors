"""Tests for the example LakeFlow connector."""
import pytest
from pathlib import Path

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.example.example import LakeflowConnect


def test_example_connector():
    """Test the example connector using the test suite"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

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
