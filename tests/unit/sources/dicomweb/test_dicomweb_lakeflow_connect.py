import pytest
from pathlib import Path

# Import test suite and connector
import tests.unit.sources.test_suite as test_suite
from tests.unit.sources.test_suite import LakeflowConnectTester
from tests.unit.sources.test_utils import load_config
from databricks.labs.community_connector.sources.dicomweb.dicomweb import DICOMwebLakeflowConnect

_CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"


@pytest.mark.skipif(not _CONFIG_PATH.exists(), reason="dev_config.json not found")
def test_dicomweb_connector():
    """Test the DICOMweb connector using the standard test suite."""
    config_dir = Path(__file__).parent / "configs"
    config = load_config(config_dir / "dev_config.json")
    table_config = load_config(config_dir / "dev_table_config.json")

    test_suite.LakeflowConnect = DICOMwebLakeflowConnect

    tester = LakeflowConnectTester(config, table_configs=table_config)
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )
