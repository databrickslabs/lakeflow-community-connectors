"""
Test suite for the ArcGIS Lakeflow Connect connector.

This module provides tests for the ArcGIS Feature Service connector
using the shared Lakeflow Connect test framework.

Usage:
    pytest sources/arcgis/test/test_arcgis_lakeflow_connect.py -v

Before running tests:
    1. Update sources/arcgis/configs/dev_config.json with your ArcGIS service URL
    2. Add a token if the service requires authentication
    3. Update dev_table_config.json with appropriate table configurations
"""

import pytest
from pathlib import Path
import sys

# Add the project root to the path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.arcgis.arcgis import LakeflowConnect


def test_arcgis_connector():
    """Test the ArcGIS connector using the test suite."""
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


class TestArcGISConnectorUnit:
    """Unit tests for specific ArcGIS connector functionality."""

    def test_sanitize_name(self):
        """Test the name sanitization function."""
        # Access the static method
        assert LakeflowConnect._sanitize_name("My Layer") == "my_layer"
        assert LakeflowConnect._sanitize_name("Layer-Name") == "layer_name"
        assert LakeflowConnect._sanitize_name("Layer.Name.123") == "layer_name_123"
        assert LakeflowConnect._sanitize_name("  Spaces  ") == "spaces"
        assert LakeflowConnect._sanitize_name("Multiple___Underscores") == "multiple_underscores"
        assert LakeflowConnect._sanitize_name("") == "unnamed"

    def test_extract_layer_id(self):
        """Test extraction of layer ID from table names."""
        # Create a minimal connector for testing
        # We need to mock the service URL validation
        import unittest.mock as mock
        
        with mock.patch.object(LakeflowConnect, '__init__', lambda self, options: None):
            connector = LakeflowConnect.__new__(LakeflowConnect)
            
            assert connector._extract_layer_id("layer_0_features") == 0
            assert connector._extract_layer_id("layer_1_points") == 1
            assert connector._extract_layer_id("table_5_records") == 5
            assert connector._extract_layer_id("layer_123_my_layer_name") == 123

    def test_esri_type_mapping(self):
        """Test that all common Esri types have mappings."""
        from sources.arcgis.arcgis import ESRI_TYPE_MAPPING
        from pyspark.sql.types import LongType, StringType, DoubleType
        
        # Check critical type mappings
        assert isinstance(ESRI_TYPE_MAPPING["esriFieldTypeOID"], LongType)
        assert isinstance(ESRI_TYPE_MAPPING["esriFieldTypeString"], StringType)
        assert isinstance(ESRI_TYPE_MAPPING["esriFieldTypeDouble"], DoubleType)
        assert isinstance(ESRI_TYPE_MAPPING["esriFieldTypeDate"], LongType)
        assert isinstance(ESRI_TYPE_MAPPING["esriFieldTypeGlobalID"], StringType)


class TestArcGISConnectorMocked:
    """Tests with mocked HTTP responses."""

    @pytest.fixture
    def mock_connector(self):
        """Create a connector with mocked HTTP requests."""
        import unittest.mock as mock
        
        # Mock service info response
        service_info = {
            "layers": [
                {"id": 0, "name": "Points"},
                {"id": 1, "name": "Lines"}
            ],
            "tables": [
                {"id": 2, "name": "Attributes"}
            ]
        }
        
        # Mock layer info response
        layer_info = {
            "id": 0,
            "name": "Points",
            "objectIdField": "OBJECTID",
            "globalIdField": "GlobalID",
            "geometryType": "esriGeometryPoint",
            "fields": [
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "nullable": False},
                {"name": "Name", "type": "esriFieldTypeString", "nullable": True, "length": 255},
                {"name": "Value", "type": "esriFieldTypeDouble", "nullable": True}
            ]
        }
        
        with mock.patch('requests.Session') as MockSession:
            mock_session = mock.MagicMock()
            MockSession.return_value = mock_session
            
            # Mock responses
            def mock_get(url, params=None, timeout=None):
                mock_response = mock.MagicMock()
                mock_response.status_code = 200
                
                if "/query" in url:
                    mock_response.json.return_value = {
                        "features": [
                            {
                                "attributes": {"OBJECTID": 1, "Name": "Test", "Value": 42.0},
                                "geometry": {"x": -122.4, "y": 37.8}
                            }
                        ],
                        "exceededTransferLimit": False
                    }
                elif url.endswith("/0"):
                    mock_response.json.return_value = layer_info
                else:
                    mock_response.json.return_value = service_info
                
                return mock_response
            
            mock_session.get.side_effect = mock_get
            
            connector = LakeflowConnect({
                "service_url": "https://example.com/arcgis/rest/services/Test/FeatureServer"
            })
            
            yield connector

    def test_list_tables_mocked(self, mock_connector):
        """Test listing tables with mocked responses."""
        tables = mock_connector.list_tables()
        
        assert len(tables) == 3
        assert "layer_0_points" in tables
        assert "layer_1_lines" in tables
        assert "table_2_attributes" in tables

    def test_get_table_schema_mocked(self, mock_connector):
        """Test getting table schema with mocked responses."""
        schema = mock_connector.get_table_schema("layer_0_points", {})
        
        field_names = [f.name for f in schema.fields]
        assert "OBJECTID" in field_names
        assert "GlobalID" in field_names
        assert "Name" in field_names
        assert "Value" in field_names
        assert "geometry" in field_names  # Added by connector

    def test_read_table_metadata_mocked(self, mock_connector):
        """Test reading table metadata with mocked responses."""
        metadata = mock_connector.read_table_metadata("layer_0_points", {})
        
        assert "primary_keys" in metadata
        assert "OBJECTID" in metadata["primary_keys"]
        assert "ingestion_type" in metadata


# Optionally, add integration test that can be skipped if no real service is available
@pytest.mark.skipif(
    True,  # Change to False when you have a real service configured
    reason="Integration test requires real ArcGIS service"
)
class TestArcGISConnectorIntegration:
    """Integration tests with real ArcGIS service."""

    def test_real_service_connection(self):
        """Test connection to a real ArcGIS Feature Service."""
        # Use a public ArcGIS sample service
        connector = LakeflowConnect({
            "service_url": "https://sampleserver6.arcgisonline.com/arcgis/rest/services/USA/MapServer"
        })
        
        tables = connector.list_tables()
        assert len(tables) > 0

