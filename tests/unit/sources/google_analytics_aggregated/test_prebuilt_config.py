"""
Test suite for validating prebuilt_reports.json configuration.

This test ensures the prebuilt reports configuration is valid and won't break
the connector at runtime. It validates:
- JSON structure and syntax
- Required fields are present
- dimensions and metrics are valid JSON arrays
- lookback_days can be cast to integer
- No duplicate report names
- Reasonable API limits (max 9 dimensions, max 10 metrics)

Run with: pytest sources/google_analytics_aggregated/test/test_prebuilt_config.py -v
"""

import json
import pytest
from pathlib import Path


# Path to the prebuilt reports JSON file (in the source connector directory)
PREBUILT_REPORTS_PATH = Path(__file__).resolve().parents[4] / "src" / "databricks" / "labs" / "community_connector" / "sources" / "google_analytics_aggregated" / "prebuilt_reports.json"

# GA4 API limits
MAX_DIMENSIONS = 9
MAX_METRICS = 10


@pytest.fixture
def prebuilt_reports():
    """Load and return the prebuilt reports configuration."""
    assert PREBUILT_REPORTS_PATH.exists(), (
        f"prebuilt_reports.json not found at {PREBUILT_REPORTS_PATH}"
    )
    
    with open(PREBUILT_REPORTS_PATH, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            pytest.fail(f"prebuilt_reports.json contains invalid JSON: {e}")
    
    return data


class TestPrebuiltReportsStructure:
    """Tests for the overall structure of prebuilt_reports.json"""
    
    def test_file_exists(self):
        """Ensure the prebuilt_reports.json file exists."""
        assert PREBUILT_REPORTS_PATH.exists(), (
            f"prebuilt_reports.json not found at {PREBUILT_REPORTS_PATH}"
        )
    
    def test_valid_json(self, prebuilt_reports):
        """Ensure the file contains valid JSON."""
        assert isinstance(prebuilt_reports, dict), (
            "prebuilt_reports.json should contain a JSON object (dict)"
        )
    
    def test_not_empty(self, prebuilt_reports):
        """Ensure at least one prebuilt report is defined."""
        assert len(prebuilt_reports) > 0, (
            "prebuilt_reports.json should contain at least one report"
        )
    
    def test_report_names_are_strings(self, prebuilt_reports):
        """Ensure all report names (keys) are strings."""
        for report_name in prebuilt_reports.keys():
            assert isinstance(report_name, str), (
                f"Report name should be a string, got {type(report_name)}"
            )
            assert len(report_name) > 0, "Report name should not be empty"


class TestPrebuiltReportFields:
    """Tests for individual report configurations."""
    
    def test_dimensions_field_present(self, prebuilt_reports):
        """Ensure every report has a 'dimensions' field."""
        for report_name, config in prebuilt_reports.items():
            assert "dimensions" in config, (
                f"Report '{report_name}' is missing required 'dimensions' field"
            )
    
    def test_metrics_field_present(self, prebuilt_reports):
        """Ensure every report has a 'metrics' field."""
        for report_name, config in prebuilt_reports.items():
            assert "metrics" in config, (
                f"Report '{report_name}' is missing required 'metrics' field"
            )
    
    def test_dimensions_is_valid_json_array(self, prebuilt_reports):
        """Ensure 'dimensions' is a valid JSON string that parses to a list."""
        for report_name, config in prebuilt_reports.items():
            dimensions_str = config.get("dimensions", "")
            
            try:
                dimensions = json.loads(dimensions_str)
            except json.JSONDecodeError as e:
                pytest.fail(
                    f"Report '{report_name}': 'dimensions' is not valid JSON: {e}\n"
                    f"  Value: {dimensions_str}"
                )
            
            assert isinstance(dimensions, list), (
                f"Report '{report_name}': 'dimensions' should parse to a list, "
                f"got {type(dimensions).__name__}"
            )
            
            # Ensure all dimension names are strings
            for i, dim in enumerate(dimensions):
                assert isinstance(dim, str), (
                    f"Report '{report_name}': dimension at index {i} should be "
                    f"a string, got {type(dim).__name__}"
                )
    
    def test_metrics_is_valid_json_array(self, prebuilt_reports):
        """Ensure 'metrics' is a valid JSON string that parses to a list."""
        for report_name, config in prebuilt_reports.items():
            metrics_str = config.get("metrics", "")
            
            try:
                metrics = json.loads(metrics_str)
            except json.JSONDecodeError as e:
                pytest.fail(
                    f"Report '{report_name}': 'metrics' is not valid JSON: {e}\n"
                    f"  Value: {metrics_str}"
                )
            
            assert isinstance(metrics, list), (
                f"Report '{report_name}': 'metrics' should parse to a list, "
                f"got {type(metrics).__name__}"
            )
            
            # Ensure all metric names are strings
            for i, metric in enumerate(metrics):
                assert isinstance(metric, str), (
                    f"Report '{report_name}': metric at index {i} should be "
                    f"a string, got {type(metric).__name__}"
                )
    
    def test_metrics_not_empty(self, prebuilt_reports):
        """Ensure every report has at least one metric (GA4 API requirement)."""
        for report_name, config in prebuilt_reports.items():
            metrics = json.loads(config.get("metrics", "[]"))
            assert len(metrics) >= 1, (
                f"Report '{report_name}': must have at least 1 metric "
                f"(GA4 API requirement)"
            )
    
    def test_lookback_days_is_integer_castable(self, prebuilt_reports):
        """Ensure 'lookback_days' can be cast to an integer."""
        for report_name, config in prebuilt_reports.items():
            if "lookback_days" in config:
                lookback_days = config["lookback_days"]
                
                try:
                    int_value = int(lookback_days)
                except (ValueError, TypeError):
                    pytest.fail(
                        f"Report '{report_name}': 'lookback_days' cannot be "
                        f"cast to int: {lookback_days}"
                    )
                
                assert int_value >= 0, (
                    f"Report '{report_name}': 'lookback_days' should be "
                    f"non-negative, got {int_value}"
                )


class TestAPILimits:
    """Tests to ensure reports don't exceed GA4 API limits."""
    
    def test_dimensions_within_limit(self, prebuilt_reports):
        """Ensure no report exceeds the maximum of 9 dimensions."""
        for report_name, config in prebuilt_reports.items():
            dimensions = json.loads(config.get("dimensions", "[]"))
            assert len(dimensions) <= MAX_DIMENSIONS, (
                f"Report '{report_name}': has {len(dimensions)} dimensions, "
                f"but GA4 API allows maximum {MAX_DIMENSIONS}\n"
                f"  Dimensions: {dimensions}"
            )
    
    def test_metrics_within_limit(self, prebuilt_reports):
        """Ensure no report exceeds the maximum of 10 metrics."""
        for report_name, config in prebuilt_reports.items():
            metrics = json.loads(config.get("metrics", "[]"))
            assert len(metrics) <= MAX_METRICS, (
                f"Report '{report_name}': has {len(metrics)} metrics, "
                f"but GA4 API allows maximum {MAX_METRICS}\n"
                f"  Metrics: {metrics}"
            )


class TestReportNaming:
    """Tests for report naming conventions."""
    
    def test_report_names_are_snake_case(self, prebuilt_reports):
        """Ensure report names follow snake_case convention."""
        import re
        snake_case_pattern = re.compile(r"^[a-z][a-z0-9_]*$")
        
        for report_name in prebuilt_reports.keys():
            assert snake_case_pattern.match(report_name), (
                f"Report name '{report_name}' should be snake_case "
                f"(lowercase letters, numbers, and underscores only)"
            )
    
    def test_no_duplicate_dimensions(self, prebuilt_reports):
        """Ensure no report has duplicate dimensions."""
        for report_name, config in prebuilt_reports.items():
            dimensions = json.loads(config.get("dimensions", "[]"))
            unique_dims = set(dimensions)
            
            if len(dimensions) != len(unique_dims):
                duplicates = [d for d in dimensions if dimensions.count(d) > 1]
                pytest.fail(
                    f"Report '{report_name}': has duplicate dimensions: "
                    f"{set(duplicates)}"
                )
    
    def test_no_duplicate_metrics(self, prebuilt_reports):
        """Ensure no report has duplicate metrics."""
        for report_name, config in prebuilt_reports.items():
            metrics = json.loads(config.get("metrics", "[]"))
            unique_metrics = set(metrics)
            
            if len(metrics) != len(unique_metrics):
                duplicates = [m for m in metrics if metrics.count(m) > 1]
                pytest.fail(
                    f"Report '{report_name}': has duplicate metrics: "
                    f"{set(duplicates)}"
                )


class TestExpectedReports:
    """Tests for expected prebuilt reports."""
    
    def test_traffic_by_country_exists(self, prebuilt_reports):
        """Ensure the core 'traffic_by_country' report exists."""
        assert "traffic_by_country" in prebuilt_reports, (
            "Expected 'traffic_by_country' report to exist"
        )
    
    def test_minimum_report_count(self, prebuilt_reports):
        """Ensure we have a reasonable number of prebuilt reports."""
        min_expected = 3  # At least traffic_by_country + a few others
        assert len(prebuilt_reports) >= min_expected, (
            f"Expected at least {min_expected} prebuilt reports, "
            f"got {len(prebuilt_reports)}"
        )


def test_prebuilt_reports_summary(prebuilt_reports):
    """Print a summary of all prebuilt reports (informational test)."""
    print("\n" + "=" * 60)
    print("PREBUILT REPORTS SUMMARY")
    print("=" * 60)
    
    for report_name, config in prebuilt_reports.items():
        dimensions = json.loads(config.get("dimensions", "[]"))
        metrics = json.loads(config.get("metrics", "[]"))
        description = config.get("description", "No description")
        
        print(f"\n📊 {report_name}")
        print(f"   Description: {description}")
        print(f"   Dimensions ({len(dimensions)}): {', '.join(dimensions)}")
        print(f"   Metrics ({len(metrics)}): {', '.join(metrics)}")
        print(f"   Start Date: {config.get('start_date', 'N/A')}")
        print(f"   Lookback Days: {config.get('lookback_days', 'N/A')}")
    
    print("\n" + "=" * 60)
    print(f"Total: {len(prebuilt_reports)} prebuilt reports")
    print("=" * 60)

