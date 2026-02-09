from pathlib import Path

from sources.redshift.redshift import LakeflowConnect
from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config


def test_redshift_connector():
    """Test the Redshift connector using the test suite with multiple table configurations"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    config = load_config(config_path)
    table_config = load_config(table_config_path)

    # Test each table configuration
    for table_name, table_options in table_config.items():
        print(f"\n{'='*60}")
        print(f"Testing table: {table_name}")
        print(f"Options: {table_options}")
        print(f"{'='*60}")
        
        # Create tester with the specific table config
        tester = LakeflowConnectTester(config, {table_name: table_options})

        # Run all tests for this table
        report = tester.run_all_tests()

        # Print the report
        tester.print_report(report, show_details=True)

        # Assert that all tests passed for this table
        assert report.passed_tests == report.total_tests, (
            f"Test suite failed for table {table_name}: "
            f"{report.failed_tests} failed, {report.error_tests} errors"
        )

    print(f"\n{'='*60}")
    print(f"✅ All tables tested successfully!")
    print(f"{'='*60}")
