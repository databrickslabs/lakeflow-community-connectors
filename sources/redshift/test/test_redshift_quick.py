"""Quick validation test for Redshift connector - tests single table with basic options"""
from pathlib import Path

from sources.redshift.redshift import LakeflowConnect
from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config


def test_redshift_quick_validation():
    """Quick test of Redshift connector with single table configuration"""
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    
    config = load_config(config_path)
    
    # Simple single-table config for quick validation
    quick_table_config = {
        "public.users": {
            "limit": "100"
        }
    }

    print(f"\n{'='*60}")
    print(f"Quick Validation Test - Redshift Connector")
    print(f"Testing table: public.users with limit=100")
    print(f"{'='*60}")
    
    # Create tester with the simple config
    tester = LakeflowConnectTester(config, quick_table_config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )

    print(f"\n{'='*60}")
    print(f"✅ Quick validation test passed!")
    print(f"{'='*60}")
