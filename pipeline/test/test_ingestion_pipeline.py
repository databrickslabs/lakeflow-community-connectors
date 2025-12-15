"""
Tests for ingestion_pipeline module.

Tests the ingest function with different spec configurations and verifies
that the correct pipeline creation functions are called with the correct parameters.
"""

import sys
from unittest.mock import Mock, MagicMock, patch

import pytest

# Mock pyspark modules before importing ingestion_pipeline
mock_pyspark = MagicMock()
mock_pyspark.pipelines = MagicMock()
mock_pyspark.sql = MagicMock()
mock_pyspark.sql.functions = MagicMock()
mock_pyspark.sql.functions.col = MagicMock(side_effect=lambda x: x)
mock_pyspark.sql.functions.expr = MagicMock(side_effect=lambda x: x)

sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.pipelines"] = mock_pyspark.pipelines
sys.modules["pyspark.sql"] = mock_pyspark.sql
sys.modules["pyspark.sql.functions"] = mock_pyspark.sql.functions

# Now import the module under test
from pipeline.ingestion_pipeline import ingest


@pytest.fixture
def mock_spark():
    """Create a mock Spark session."""
    return Mock()


@pytest.fixture
def base_metadata():
    """Base metadata returned by _get_table_metadata."""
    return {
        "users": {
            "primary_keys": ["user_id"],
            "cursor_field": "updated_at",
            "ingestion_type": "cdc",
        },
        "orders": {
            "primary_keys": ["order_id"],
            "cursor_field": "modified_at",
            "ingestion_type": "snapshot",
        },
        "events": {
            "primary_keys": ["event_id"],
            "cursor_field": "timestamp",
            "ingestion_type": "append",
        },
    }


class TestIngestCDC:
    """Test CDC ingestion scenarios."""

    def test_cdc_ingestion_with_default_scd_type(self, mock_spark, base_metadata):
        """Test CDC ingestion with default SCD type (1)."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ) as mock_metadata, patch(
            "pipeline.ingestion_pipeline._create_cdc_table"
        ) as mock_cdc:
            ingest(mock_spark, spec)

            # Verify metadata was fetched
            mock_metadata.assert_called_once_with(
                mock_spark, "test_connection", ["users"]
            )

            # Verify CDC table was created with correct parameters
            mock_cdc.assert_called_once_with(
                mock_spark,
                "test_connection",
                "users",  # source_table
                "users",  # destination_table (defaults to source_table)
                ["user_id"],  # primary_keys from metadata
                "updated_at",  # sequence_by from metadata cursor_field
                "1",  # default scd_type
                "users_staging",  # view_name
                {},  # table_config
            )

    def test_cdc_ingestion_with_scd_type_2(self, mock_spark, base_metadata):
        """Test CDC ingestion with SCD Type 2."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_2",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify SCD Type 2 is passed (index 6 after adding destination_table)
            assert mock_cdc.call_args[0][6] == "2"

    def test_cdc_ingestion_with_custom_sequence_by(self, mock_spark, base_metadata):
        """Test CDC ingestion with custom sequence_by from spec."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "sequence_by": "custom_timestamp",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify custom sequence_by is used instead of cursor_field (index 5 after adding destination_table)
            assert mock_cdc.call_args[0][5] == "custom_timestamp"

    def test_cdc_ingestion_with_table_config(self, mock_spark, base_metadata):
        """Test CDC ingestion with additional table configuration."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "some_option": "value1",
                            "another_option": "value2",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify table config is passed (excluding special keys) (index 8 after adding destination_table)
            table_config = mock_cdc.call_args[0][8]
            assert table_config == {
                "some_option": "value1",
                "another_option": "value2",
            }


class TestIngestSnapshot:
    """Test snapshot ingestion scenarios."""

    def test_snapshot_ingestion_with_default_scd_type(self, mock_spark, base_metadata):
        """Test snapshot ingestion with default SCD type."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "orders",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_snapshot_table") as mock_snapshot:
            ingest(mock_spark, spec)

            # Verify snapshot table was created with correct parameters
            mock_snapshot.assert_called_once_with(
                mock_spark,
                "test_connection",
                "orders",  # source_table
                "orders",  # destination_table (defaults to source_table)
                ["order_id"],  # primary_keys from metadata
                "1",  # default scd_type
                "orders_staging",  # view_name
                {},  # table_config
            )

    def test_snapshot_ingestion_with_scd_type_2(self, mock_spark, base_metadata):
        """Test snapshot ingestion with SCD Type 2."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "orders",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_2",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_snapshot_table") as mock_snapshot:
            ingest(mock_spark, spec)

            # Verify SCD Type 2 is passed (index 5 after adding destination_table)
            assert mock_snapshot.call_args[0][5] == "2"


class TestIngestAppend:
    """Test append-only ingestion scenarios."""

    def test_append_ingestion_from_metadata(self, mock_spark, base_metadata):
        """Test append ingestion when ingestion_type is 'append' in metadata."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "events",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_append_table") as mock_append:
            ingest(mock_spark, spec)

            # Verify append table was created
            mock_append.assert_called_once_with(
                mock_spark,
                "test_connection",
                "events",  # source_table
                "events",  # destination_table (defaults to source_table)
                "events_staging",  # view_name
                {},  # table_config
            )

    def test_append_ingestion_from_scd_type_append_only(
        self, mock_spark, base_metadata
    ):
        """Test that APPEND_ONLY scd_type overrides ingestion_type to append."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",  # Originally CDC in metadata
                        "table_configuration": {
                            "scd_type": "APPEND_ONLY",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_append_table") as mock_append:
            ingest(mock_spark, spec)

            # Verify append table was created (not CDC)
            mock_append.assert_called_once()
            assert mock_append.call_args[0][2] == "users"  # source_table
            assert mock_append.call_args[0][3] == "users"  # destination_table


class TestIngestMultipleTables:
    """Test ingestion with multiple tables."""

    def test_multiple_tables_with_different_ingestion_types(
        self, mock_spark, base_metadata
    ):
        """Test ingesting multiple tables with different ingestion types."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {},
                    }
                },
                {
                    "table": {
                        "source_table": "orders",
                        "table_configuration": {},
                    }
                },
                {
                    "table": {
                        "source_table": "events",
                        "table_configuration": {},
                    }
                },
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ) as mock_metadata, patch(
            "pipeline.ingestion_pipeline._create_cdc_table"
        ) as mock_cdc, patch(
            "pipeline.ingestion_pipeline._create_snapshot_table"
        ) as mock_snapshot, patch(
            "pipeline.ingestion_pipeline._create_append_table"
        ) as mock_append:
            ingest(mock_spark, spec)

            # Verify metadata was fetched for all tables
            mock_metadata.assert_called_once_with(
                mock_spark, "test_connection", ["users", "orders", "events"]
            )

            # Verify CDC was called for users
            mock_cdc.assert_called_once()
            assert mock_cdc.call_args[0][2] == "users"  # source_table
            assert mock_cdc.call_args[0][3] == "users"  # destination_table

            # Verify snapshot was called for orders
            mock_snapshot.assert_called_once()
            assert mock_snapshot.call_args[0][2] == "orders"  # source_table
            assert mock_snapshot.call_args[0][3] == "orders"  # destination_table

            # Verify append was called for events
            mock_append.assert_called_once()
            assert mock_append.call_args[0][2] == "events"  # source_table
            assert mock_append.call_args[0][3] == "events"  # destination_table

    def test_multiple_tables_with_mixed_configurations(self, mock_spark, base_metadata):
        """Test multiple tables with different SCD types and configurations."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_2",
                            "sequence_by": "created_at",
                        },
                    }
                },
                {
                    "table": {
                        "source_table": "orders",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_1",
                        },
                    }
                },
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc, patch(
            "pipeline.ingestion_pipeline._create_snapshot_table"
        ) as mock_snapshot:
            ingest(mock_spark, spec)

            # Verify users table called with SCD_TYPE_2 and custom sequence_by
            users_call = mock_cdc.call_args
            assert users_call[0][2] == "users"  # source_table
            assert users_call[0][3] == "users"  # destination_table
            assert (
                users_call[0][5] == "created_at"
            )  # custom sequence_by (index shifted)
            assert users_call[0][6] == "2"  # scd_type (index shifted)

            # Verify orders table called with SCD_TYPE_1
            orders_call = mock_snapshot.call_args
            assert orders_call[0][2] == "orders"  # source_table
            assert orders_call[0][3] == "orders"  # destination_table
            assert orders_call[0][5] == "1"  # scd_type (index shifted)


class TestIngestEdgeCases:
    """Test edge cases and error scenarios."""

    def test_table_config_excludes_special_keys(self, mock_spark, base_metadata):
        """Test that special keys are excluded from table_config passed to creation functions."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_2",
                            "sequence_by": "custom_field",
                            "primary_keys": ["id", "tenant_id"],
                            "regular_option": "value",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify table_config only contains regular options, not special keys (index 8 after adding destination_table)
            table_config = mock_cdc.call_args[0][8]
            assert table_config == {"regular_option": "value"}
            assert "scd_type" not in table_config
            assert "sequence_by" not in table_config
            assert "primary_keys" not in table_config

    def test_sequence_by_fallback_to_cursor_field(self, mock_spark, base_metadata):
        """Test that sequence_by falls back to cursor_field when not specified."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {},  # No sequence_by specified
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify sequence_by uses cursor_field from metadata (index 5 after adding destination_table)
            assert (
                mock_cdc.call_args[0][5] == "updated_at"
            )  # cursor_field from metadata

    def test_scd_type_fallback_to_default(self, mock_spark, base_metadata):
        """Test that scd_type falls back to '1' when not specified."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {},  # No scd_type specified
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify scd_type defaults to "1" (index 6 after adding destination_table)
            assert mock_cdc.call_args[0][6] == "1"


class TestMetadataOverride:
    """Test scenarios where metadata is missing but spec provides override values."""

    def test_spec_provides_primary_keys_when_metadata_missing(self, mock_spark):
        """Test that spec can provide primary_keys when metadata doesn't have it."""
        # Metadata missing primary_keys
        partial_metadata = {
            "users": {
                "cursor_field": "updated_at",
                "ingestion_type": "cdc",
            },
        }

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "primary_keys": ["user_id", "tenant_id"],
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=partial_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify primary_keys from spec is used (index 4 after adding destination_table)
            mock_cdc.assert_called_once()
            assert mock_cdc.call_args[0][4] == ["user_id", "tenant_id"]

    def test_spec_overrides_metadata_primary_keys(self, mock_spark, base_metadata):
        """Test that spec primary_keys overrides metadata primary_keys."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "primary_keys": ["composite_key_1", "composite_key_2"],
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify spec primary_keys overrides metadata (index 4 after adding destination_table)
            mock_cdc.assert_called_once()
            assert mock_cdc.call_args[0][4] == ["composite_key_1", "composite_key_2"]

    def test_metadata_missing_all_optional_fields(self, mock_spark):
        """Test ingestion when metadata has no optional fields, spec provides all."""
        # Minimal metadata with only required fields
        minimal_metadata = {
            "users": {},
        }

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "primary_keys": ["id"],
                            "sequence_by": "created_at",
                            "scd_type": "SCD_TYPE_2",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=minimal_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify all values from spec are used (indices shifted after adding destination_table)
            mock_cdc.assert_called_once()
            call_args = mock_cdc.call_args[0]
            assert call_args[4] == ["id"]  # primary_keys
            assert call_args[5] == "created_at"  # sequence_by
            assert call_args[6] == "2"  # scd_type

    def test_metadata_missing_ingestion_type_defaults_to_cdc(self, mock_spark):
        """Test that missing ingestion_type in metadata defaults to 'cdc'."""
        # Metadata without ingestion_type
        metadata_no_ingestion_type = {
            "users": {
                "primary_keys": ["user_id"],
                "cursor_field": "updated_at",
            },
        }

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=metadata_no_ingestion_type,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify CDC table was created (default ingestion_type)
            mock_cdc.assert_called_once()


class TestDestinationTable:
    """Test destination table name scenarios."""

    def test_destination_table_with_full_path(self, mock_spark, base_metadata):
        """Test that destination table uses fully qualified name when all fields are specified."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "destination_catalog": "my_catalog",
                        "destination_schema": "my_schema",
                        "destination_table": "my_users",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify destination_table has the full qualified name
            mock_cdc.assert_called_once()
            assert mock_cdc.call_args[0][2] == "users"  # source_table
            assert (
                mock_cdc.call_args[0][3] == "`my_catalog`.`my_schema`.`my_users`"
            )  # destination_table

    def test_destination_table_defaults_to_source_table(
        self, mock_spark, base_metadata
    ):
        """Test that destination table defaults to source table name when not specified."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify destination_table defaults to source_table name
            mock_cdc.assert_called_once()
            assert mock_cdc.call_args[0][2] == "users"  # source_table
            assert mock_cdc.call_args[0][3] == "users"  # destination_table

    def test_destination_table_uses_source_name_when_only_catalog_schema(
        self, mock_spark, base_metadata
    ):
        """Test that destination uses source table name when destination_table is not specified."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "destination_catalog": "my_catalog",
                        "destination_schema": "my_schema",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ), patch("pipeline.ingestion_pipeline._create_cdc_table") as mock_cdc:
            ingest(mock_spark, spec)

            # Verify destination_table uses source table name with catalog.schema prefix
            mock_cdc.assert_called_once()
            assert mock_cdc.call_args[0][2] == "users"  # source_table
            assert (
                mock_cdc.call_args[0][3] == "`my_catalog`.`my_schema`.`users`"
            )  # destination_table
