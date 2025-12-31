"""Tests for the HubSpot LakeFlow connector."""
import pytest
import json
from pathlib import Path

# Import test suite and connector
import tests.test_suite as test_suite
from tests.test_suite import LakeflowConnectTester
from sources.hubspot.hubspot import LakeflowConnect
from sources.hubspot.hubspot_test_utils import LakeflowConnectTestUtils


def load_config():
    """Load configuration from dev_config.json"""
    config_path = Path(__file__).parent.parent / "configs" / "dev_config.json"
    with open(config_path, "r") as f:
        return json.load(f)


def test_hubspot_connector():
    """Test the hubspot connector using the test suite"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect
    test_suite.LakeflowConnectTestUtils = LakeflowConnectTestUtils

    # Load configuration
    config = load_config()

    # Create tester with the config
    tester = LakeflowConnectTester(config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
