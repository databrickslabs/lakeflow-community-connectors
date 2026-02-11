"""
Test suite for SAP SuccessFactors Lakeflow Connector.

This test file uses the standard LakeflowConnectTester to validate
the SAP SuccessFactors connector against a real SuccessFactors instance.

To run tests:
    pytest tests/unit/sources/sap_successfactors/test_sap_successfactors_lakeflow_connect.py -v
"""

import json
from pathlib import Path

import tests.unit.sources.test_suite as test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config
from databricks.labs.community_connector.sources.sap_successfactors.sap_successfactors import (
    SapSuccessFactorsLakeflowConnect,
)


def test_sap_successfactors_connector():
    """Test the SAP SuccessFactors connector using the test suite."""
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = SapSuccessFactorsLakeflowConnect

    # Load configuration
    config_dir = Path(__file__).parent / "configs"
    config_path = config_dir / "dev_config.json"

    config = load_config(config_path)

    # Create tester with the config
    tester = LakeflowConnectTester(init_options=config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
