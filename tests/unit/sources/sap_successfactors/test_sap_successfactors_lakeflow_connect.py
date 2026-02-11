"""
Test suite for SAP SuccessFactors Lakeflow Connector.

This test file uses the standard LakeflowConnectTester to validate
the SAP SuccessFactors connector against a real SuccessFactors instance.

To run tests:
    pytest sources/sap_successfactors/test/test_sap_successfactors_lakeflow_connect.py -v
"""

import json
import os
import sys

# Add project root to path for imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
sys.path.insert(0, project_root)

# Import the connector class and inject into test_suite namespace
from sources.sap_successfactors.sap_successfactors import LakeflowConnect
import tests.test_suite as test_suite
test_suite.LakeflowConnect = LakeflowConnect

from tests.test_suite import LakeflowConnectTester


def load_config():
    """Load the dev configuration."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "configs",
        "dev_config.json",
    )

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        return json.load(f)


def run_tests():
    """Run the connector test suite."""
    config = load_config()

    # Create tester with init_options (the connection credentials)
    tester = LakeflowConnectTester(init_options=config)

    # Run all tests
    report = tester.run_all_tests()

    # Print report
    tester.print_report(report)

    return report


if __name__ == "__main__":
    run_tests()
