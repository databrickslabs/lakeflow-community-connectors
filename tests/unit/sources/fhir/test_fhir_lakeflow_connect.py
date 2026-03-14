"""Integration test for FhirLakeflowConnect using LakeflowConnectTester.

Runs against whatever FHIR server is configured in:
    tests/unit/sources/fhir/configs/dev_config.json

The default config points at the public HAPI FHIR R4 server (auth_type=none).
To test against a SMART-enabled server, replace dev_config.json with your
SMART credentials (see README.md Authentication Setup, or run authenticate.py).
"""
from pathlib import Path

from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect
from tests.unit.sources import test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config


def test_fhir_connector():
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    test_suite.LakeflowConnect = FhirLakeflowConnect

    # Small sample_records -- HAPI FHIR is a public server, be considerate.
    tester = LakeflowConnectTester(config, table_config, sample_records=5)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
