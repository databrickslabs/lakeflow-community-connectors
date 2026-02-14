"""
Tests for the OData $metadata dynamic metadata module.

Covers EDMX XML parsing, PySpark schema building, CDC auto-detection,
dynamic-mode connector integration, and HTTP fetch / caching behaviour.

To run:
    pytest tests/unit/sources/sap_successfactors/test_odata_metadata.py -v
"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructType,
)

from databricks.labs.community_connector.sources.sap_successfactors.odata_metadata import (
    build_schema_from_metadata,
    build_table_config_from_metadata,
    fetch_odata_metadata,
    parse_edmx,
)
from databricks.labs.community_connector.sources.sap_successfactors.sap_successfactors import (
    SapSuccessFactorsLakeflowConnect,
)

# ----------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------

# Sample EDMX XML representing a minimal SAP SuccessFactors $metadata response
SAMPLE_EDMX = """\
<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="1.0"
    xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx">
  <edmx:DataServices
      m:DataServiceVersion="2.0"
      xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
    <Schema Namespace="SFOData"
        xmlns="http://schemas.microsoft.com/ado/2008/09/edm">

      <EntityType Name="Employee">
        <Key>
          <PropertyRef Name="userId"/>
        </Key>
        <Property Name="userId" Type="Edm.String" Nullable="false"/>
        <Property Name="firstName" Type="Edm.String" Nullable="true"/>
        <Property Name="lastName" Type="Edm.String" Nullable="true"/>
        <Property Name="hireDate" Type="Edm.DateTime" Nullable="true"/>
        <Property Name="active" Type="Edm.Boolean" Nullable="true"/>
        <Property Name="salary" Type="Edm.Double" Nullable="true"/>
        <Property Name="age" Type="Edm.Int64" Nullable="true"/>
        <Property Name="lastModifiedDateTime" Type="Edm.DateTimeOffset" Nullable="true"/>
        <NavigationProperty Name="manager" Relationship="SFOData.ManagerRel"
            FromRole="Employee" ToRole="Manager"/>
      </EntityType>

      <EntityType Name="Department">
        <Key>
          <PropertyRef Name="departmentId"/>
          <PropertyRef Name="effectiveStartDate"/>
        </Key>
        <Property Name="departmentId" Type="Edm.String" Nullable="false"/>
        <Property Name="effectiveStartDate" Type="Edm.DateTime" Nullable="false"/>
        <Property Name="name" Type="Edm.String" Nullable="true"/>
        <Property Name="headCount" Type="Edm.Int32" Nullable="true"/>
        <Property Name="costCenter" Type="Edm.String" Nullable="true"/>
        <Property Name="photo" Type="Edm.Binary" Nullable="true"/>
      </EntityType>

      <EntityContainer Name="SFODataServiceContainer"
          m:IsDefaultEntityContainer="true">
        <EntitySet Name="Employee" EntityType="SFOData.Employee"/>
        <EntitySet Name="Department" EntityType="SFOData.Department"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _make_connector(metadata_mode="static"):
    """Helper to create a connector instance for tests."""
    return SapSuccessFactorsLakeflowConnect(
        {
            "endpoint_url": "https://example.successfactors.com",
            "username": "testuser@COMPANY",
            "password": "testpass",
            "metadata_mode": metadata_mode,
        }
    )


# ----------------------------------------------------------------
# EDMX parsing (module-level functions)
# ----------------------------------------------------------------


class TestParseEdmx:
    """Tests for the parse_edmx function."""

    def test_entity_sets_extracted(self):
        """Verify that entity sets are correctly extracted from EDMX XML."""
        metadata = parse_edmx(SAMPLE_EDMX)

        entity_sets = metadata["entity_sets"]
        assert "Employee" in entity_sets
        assert "Department" in entity_sets
        assert len(entity_sets) == 2

    def test_primary_keys(self):
        """Verify that primary keys are extracted from EntityType Key elements."""
        metadata = parse_edmx(SAMPLE_EDMX)

        emp = metadata["entity_sets"]["Employee"]
        assert emp["primary_keys"] == ["userId"]

        dept = metadata["entity_sets"]["Department"]
        assert dept["primary_keys"] == ["departmentId", "effectiveStartDate"]

    def test_properties_types_and_nullability(self):
        """Verify that properties are extracted with correct types and nullability."""
        metadata = parse_edmx(SAMPLE_EDMX)

        emp_props = {
            p["name"]: p for p in metadata["entity_sets"]["Employee"]["properties"]
        }
        assert emp_props["userId"]["type"] == "Edm.String"
        assert emp_props["userId"]["nullable"] is False
        assert emp_props["hireDate"]["type"] == "Edm.DateTime"
        assert emp_props["active"]["type"] == "Edm.Boolean"
        assert emp_props["salary"]["type"] == "Edm.Double"
        assert emp_props["age"]["type"] == "Edm.Int64"
        assert emp_props["lastModifiedDateTime"]["type"] == "Edm.DateTimeOffset"

    def test_navigation_properties_skipped(self):
        """Verify that NavigationProperty elements are not included as properties."""
        metadata = parse_edmx(SAMPLE_EDMX)

        emp_prop_names = [
            p["name"] for p in metadata["entity_sets"]["Employee"]["properties"]
        ]
        assert "manager" not in emp_prop_names


# ----------------------------------------------------------------
# Schema building
# ----------------------------------------------------------------


class TestBuildSchemaFromMetadata:
    """Tests for building PySpark schemas from parsed metadata."""

    def test_edm_type_mapping(self):
        """Verify EDM types are correctly mapped to PySpark types."""
        metadata = parse_edmx(SAMPLE_EDMX)
        schema = build_schema_from_metadata(metadata["entity_sets"]["Employee"])
        field_map = {f.name: f for f in schema.fields}

        assert isinstance(field_map["userId"].dataType, StringType)
        assert isinstance(field_map["active"].dataType, BooleanType)
        assert isinstance(field_map["salary"].dataType, DoubleType)
        assert isinstance(field_map["age"].dataType, LongType)
        # DateTime / DateTimeOffset map to StringType
        assert isinstance(field_map["hireDate"].dataType, StringType)
        assert isinstance(field_map["lastModifiedDateTime"].dataType, StringType)

    def test_nullable_flag(self):
        """Verify that Nullable=false is respected for primary key fields."""
        metadata = parse_edmx(SAMPLE_EDMX)
        schema = build_schema_from_metadata(metadata["entity_sets"]["Employee"])
        field_map = {f.name: f for f in schema.fields}

        assert field_map["userId"].nullable is False
        assert field_map["firstName"].nullable is True

    def test_binary_type(self):
        """Verify Edm.Binary maps to BinaryType."""
        metadata = parse_edmx(SAMPLE_EDMX)
        schema = build_schema_from_metadata(metadata["entity_sets"]["Department"])
        field_map = {f.name: f for f in schema.fields}

        assert isinstance(field_map["photo"].dataType, BinaryType)


# ----------------------------------------------------------------
# Table config building / CDC detection
# ----------------------------------------------------------------


class TestBuildTableConfigFromMetadata:
    """Tests for building table config (primary keys, CDC detection) from metadata."""

    def test_cdc_detection_with_lastModifiedDateTime(self):
        """Verify CDC is detected when lastModifiedDateTime exists."""
        metadata = parse_edmx(SAMPLE_EDMX)
        config = build_table_config_from_metadata(metadata["entity_sets"]["Employee"])

        assert config["cursor_field"] == "lastModifiedDateTime"
        assert config["ingestion_type"] == "cdc"
        assert config["primary_keys"] == ["userId"]

    def test_snapshot_when_no_cursor_field(self):
        """Verify snapshot mode when no common cursor fields exist."""
        metadata = parse_edmx(SAMPLE_EDMX)
        config = build_table_config_from_metadata(
            metadata["entity_sets"]["Department"]
        )

        assert config["cursor_field"] is None
        assert config["ingestion_type"] == "snapshot"
        assert config["primary_keys"] == ["departmentId", "effectiveStartDate"]


# ----------------------------------------------------------------
# HTTP fetch
# ----------------------------------------------------------------


class TestFetchOdataMetadata:
    """Tests for the HTTP fetch + parse flow with mocked responses."""

    def test_success(self):
        """Verify fetch_odata_metadata fetches XML and parses it."""
        session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_EDMX
        session.get.return_value = mock_response

        metadata = fetch_odata_metadata(
            session, "https://example.com/odata/v2", {"Authorization": "Basic x"}
        )

        assert "Employee" in metadata["entity_sets"]
        assert "Department" in metadata["entity_sets"]

    def test_http_error(self):
        """Verify exception is raised on HTTP error from $metadata endpoint."""
        session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        session.get.return_value = mock_response

        with pytest.raises(Exception, match="Failed to fetch OData metadata"):
            fetch_odata_metadata(
                session, "https://example.com/odata/v2", {"Authorization": "Basic x"}
            )


# ----------------------------------------------------------------
# Connector-level dynamic / static mode integration
# ----------------------------------------------------------------


class TestDynamicModeIntegration:
    """Tests for dynamic mode using mocked $metadata responses."""

    def test_list_tables_dynamic(self):
        """Verify list_tables returns entity sets from $metadata in dynamic mode."""
        connector = _make_connector("dynamic")
        connector._metadata_cache = parse_edmx(SAMPLE_EDMX)

        tables = connector.list_tables()
        assert sorted(tables) == ["Department", "Employee"]

    def test_get_table_schema_dynamic(self):
        """Verify get_table_schema returns a StructType in dynamic mode."""
        connector = _make_connector("dynamic")
        connector._metadata_cache = parse_edmx(SAMPLE_EDMX)

        schema = connector.get_table_schema("Employee", {})
        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "userId" in field_names
        assert "lastModifiedDateTime" in field_names

    def test_get_table_schema_dynamic_unsupported(self):
        """Verify ValueError is raised for unknown tables in dynamic mode."""
        connector = _make_connector("dynamic")
        connector._metadata_cache = parse_edmx(SAMPLE_EDMX)

        with pytest.raises(ValueError, match="Unsupported table"):
            connector.get_table_schema("NonExistent", {})

    def test_read_table_metadata_dynamic(self):
        """Verify read_table_metadata returns correct metadata in dynamic mode."""
        connector = _make_connector("dynamic")
        connector._metadata_cache = parse_edmx(SAMPLE_EDMX)

        meta = connector.read_table_metadata("Employee", {})
        assert meta["primary_keys"] == ["userId"]
        assert meta["cursor_field"] == "lastModifiedDateTime"
        assert meta["ingestion_type"] == "cdc"

    def test_read_table_metadata_dynamic_unsupported(self):
        """Verify ValueError is raised for unknown tables in dynamic mode."""
        connector = _make_connector("dynamic")
        connector._metadata_cache = parse_edmx(SAMPLE_EDMX)

        with pytest.raises(ValueError, match="Unsupported table"):
            connector.read_table_metadata("NonExistent", {})

    def test_resolve_table_config_dynamic(self):
        """Verify _resolve_table_config builds config with entity_set in dynamic mode."""
        connector = _make_connector("dynamic")
        connector._metadata_cache = parse_edmx(SAMPLE_EDMX)

        config = connector._resolve_table_config("Employee")
        assert config["entity_set"] == "Employee"
        assert config["primary_keys"] == ["userId"]
        assert config["cursor_field"] == "lastModifiedDateTime"
        assert config["ingestion_type"] == "cdc"


class TestStaticModeUnchanged:
    """Tests to verify that static mode behaviour is preserved."""

    def test_list_tables_static(self):
        """Verify list_tables returns hardcoded TABLE_CONFIG keys in static mode."""
        from databricks.labs.community_connector.sources.sap_successfactors.table_metadata import (
            TABLE_CONFIG,
        )

        connector = _make_connector("static")
        tables = connector.list_tables()
        assert tables == list(TABLE_CONFIG.keys())

    def test_get_table_schema_static(self):
        """Verify get_table_schema returns hardcoded schema in static mode."""
        from databricks.labs.community_connector.sources.sap_successfactors.table_schemas import (
            TABLE_SCHEMAS,
        )

        connector = _make_connector("static")
        first_table = list(TABLE_SCHEMAS.keys())[0]
        schema = connector.get_table_schema(first_table, {})
        assert schema == TABLE_SCHEMAS[first_table]

    def test_metadata_mode_validation(self):
        """Verify that invalid metadata_mode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid metadata_mode"):
            _make_connector("invalid")

    def test_metadata_mode_default_is_static(self):
        """Verify that omitting metadata_mode defaults to static."""
        connector = SapSuccessFactorsLakeflowConnect(
            {
                "endpoint_url": "https://example.successfactors.com",
                "username": "testuser@COMPANY",
                "password": "testpass",
            }
        )
        assert connector.metadata_mode == "static"


class TestGetMetadataCaching:
    """Tests for the connector-level _get_metadata caching behaviour."""

    def test_caches_after_first_call(self):
        """Verify that _get_metadata caches the result after first call."""
        connector = _make_connector("dynamic")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = SAMPLE_EDMX

        with patch.object(
            connector.session, "get", return_value=mock_response
        ) as mock_get:
            metadata1 = connector._get_metadata()
            metadata2 = connector._get_metadata()

        assert mock_get.call_count == 1
        assert metadata1 is metadata2
