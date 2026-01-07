# pylint: disable=too-many-lines
"""
Tests for ingestion_pipeline module.

Tests the ingest function with different spec configurations and verifies
that the correct SDP calls are made with the correct parameters.
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock pyspark modules before importing ingestion_pipeline
mock_sdp = MagicMock()
mock_pyspark = MagicMock()
mock_pyspark.pipelines = mock_sdp
mock_pyspark.sql = MagicMock()
mock_pyspark.sql.functions = MagicMock()
mock_pyspark.sql.functions.col = MagicMock(side_effect=lambda x: f"col({x})")
mock_pyspark.sql.functions.expr = MagicMock(side_effect=lambda x: f"expr({x})")

sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.pipelines"] = mock_sdp
sys.modules["pyspark.sql"] = mock_pyspark.sql
sys.modules["pyspark.sql.functions"] = mock_pyspark.sql.functions

# Now import the module under test
# pylint: disable=wrong-import-position
from pipeline.ingestion_pipeline import ingest  # noqa: E402


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all mocks before each test."""
    mock_sdp.reset_mock()
    mock_sdp.view = MagicMock(side_effect=lambda name: lambda f: f)
    mock_sdp.append_flow = MagicMock(side_effect=lambda name, target: lambda f: f)
    mock_sdp.create_streaming_table = MagicMock()
    mock_sdp.apply_changes = MagicMock()
    mock_sdp.apply_changes_from_snapshot = MagicMock()
    yield


@pytest.fixture
def mock_spark():
    """Create a mock Spark session with proper chaining."""
    spark = MagicMock()
    # Setup read chain
    read_chain = spark.read.format.return_value.option.return_value.option.return_value
    read_chain.option.return_value.options.return_value.load.return_value = MagicMock()
    read_chain.options.return_value.load.return_value = MagicMock()
    # Setup readStream chain
    stream_chain = spark.readStream.format.return_value.option.return_value.option.return_value
    stream_chain.options.return_value.load.return_value = MagicMock()
    return spark


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
        "contacts": {
            "primary_keys": ["contact_id"],
            "cursor_field": "updated_at",
            "ingestion_type": "cdc_with_deletes",
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
        ):
            ingest(mock_spark, spec)

            # Verify streaming table was created
            mock_sdp.create_streaming_table.assert_called_once_with(name="users")

            # Verify apply_changes was called with correct parameters
            mock_sdp.apply_changes.assert_called_once_with(
                target="users",
                source="users_staging",
                keys=["user_id"],
                sequence_by="col(updated_at)",
                stored_as_scd_type="1",
            )

            # Verify view was created
            mock_sdp.view.assert_called_once_with(name="users_staging")

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
        ):
            ingest(mock_spark, spec)

            # Verify apply_changes was called with SCD type 2
            mock_sdp.apply_changes.assert_called_once()
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["stored_as_scd_type"] == "2"

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
        ):
            ingest(mock_spark, spec)

            # Verify custom sequence_by is used
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["sequence_by"] == "col(custom_timestamp)"

    def test_cdc_ingestion_with_custom_primary_keys(self, mock_spark, base_metadata):
        """Test CDC ingestion with custom primary_keys from spec."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "primary_keys": ["id", "tenant_id"],
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            # Verify custom primary_keys is used
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["keys"] == ["id", "tenant_id"]


class TestIngestCDCWithDeletes:
    """Test CDC with deletes ingestion scenarios."""

    def test_cdc_with_deletes_creates_both_flows(self, mock_spark, base_metadata):
        """Test that cdc_with_deletes creates both normal and delete flows."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "contacts",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            # Verify streaming table was created once
            mock_sdp.create_streaming_table.assert_called_once_with(name="contacts")

            # Verify two views were created: normal and delete staging
            assert mock_sdp.view.call_count == 2
            view_calls = [call[1]["name"] for call in mock_sdp.view.call_args_list]
            assert "contacts_staging" in view_calls
            assert "contacts_delete_staging" in view_calls

            # Verify apply_changes was called twice (normal + delete flow)
            assert mock_sdp.apply_changes.call_count == 2

    def test_cdc_with_deletes_delete_flow_parameters(self, mock_spark, base_metadata):
        """Test delete flow has correct apply_changes parameters."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "contacts",
                        "table_configuration": {},
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            # Find the delete flow apply_changes call
            apply_changes_calls = mock_sdp.apply_changes.call_args_list
            delete_flow_call = None
            normal_flow_call = None

            for call in apply_changes_calls:
                kwargs = call[1]
                if "apply_as_deletes" in kwargs:
                    delete_flow_call = kwargs
                else:
                    normal_flow_call = kwargs

            # Verify normal flow parameters
            assert normal_flow_call is not None
            assert normal_flow_call["target"] == "contacts"
            assert normal_flow_call["source"] == "contacts_staging"
            assert normal_flow_call["keys"] == ["contact_id"]
            assert normal_flow_call["sequence_by"] == "col(updated_at)"
            assert normal_flow_call["stored_as_scd_type"] == "1"

            # Verify delete flow parameters
            assert delete_flow_call is not None
            assert delete_flow_call["target"] == "contacts"
            assert delete_flow_call["source"] == "contacts_delete_staging"
            assert delete_flow_call["keys"] == ["contact_id"]
            assert delete_flow_call["apply_as_deletes"] == "expr(true)"
            assert delete_flow_call["name"] == "contacts_delete_staging_delete_flow"

    def test_cdc_with_deletes_with_scd_type_2(self, mock_spark, base_metadata):
        """Test cdc_with_deletes with SCD Type 2."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "contacts",
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
        ):
            ingest(mock_spark, spec)

            # Verify both apply_changes calls have SCD type 2
            for call in mock_sdp.apply_changes.call_args_list:
                kwargs = call[1]
                assert kwargs["stored_as_scd_type"] == "2"

    def test_cdc_with_deletes_custom_primary_keys_and_cursor(self, mock_spark, base_metadata):
        """Test cdc_with_deletes with custom primary_keys and sequence_by from spec."""
        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "contacts",
                        "table_configuration": {
                            "primary_keys": ["id", "tenant_id"],
                            "sequence_by": "modified_at",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            # Verify both flows use custom primary keys and sequence_by
            for call in mock_sdp.apply_changes.call_args_list:
                kwargs = call[1]
                assert kwargs["keys"] == ["id", "tenant_id"]
                assert kwargs["sequence_by"] == "col(modified_at)"


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
        ):
            ingest(mock_spark, spec)

            # Verify streaming table was created
            mock_sdp.create_streaming_table.assert_called_once_with(name="orders")

            # Verify apply_changes_from_snapshot was called
            mock_sdp.apply_changes_from_snapshot.assert_called_once_with(
                target="orders",
                source="orders_staging",
                keys=["order_id"],
                stored_as_scd_type="1",
            )

            # Verify view was created
            mock_sdp.view.assert_called_once_with(name="orders_staging")

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
        ):
            ingest(mock_spark, spec)

            # Verify SCD Type 2 is passed
            call_kwargs = mock_sdp.apply_changes_from_snapshot.call_args[1]
            assert call_kwargs["stored_as_scd_type"] == "2"


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
        ):
            ingest(mock_spark, spec)

            # Verify streaming table was created
            mock_sdp.create_streaming_table.assert_called_once_with(name="events")

            # Verify append_flow was created (not apply_changes)
            mock_sdp.append_flow.assert_called_once_with(name="events_staging", target="events")

            # Verify apply_changes was NOT called
            mock_sdp.apply_changes.assert_not_called()
            mock_sdp.apply_changes_from_snapshot.assert_not_called()

    def test_append_ingestion_from_scd_type_append_only(self, mock_spark, base_metadata):
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
        ):
            ingest(mock_spark, spec)

            # Verify append_flow was used instead of apply_changes
            mock_sdp.append_flow.assert_called_once_with(name="users_staging", target="users")
            mock_sdp.apply_changes.assert_not_called()


class TestIngestMultipleTables:
    """Test ingestion with multiple tables."""

    def test_multiple_tables_with_different_ingestion_types(self, mock_spark, base_metadata):
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
                {
                    "table": {
                        "source_table": "contacts",
                        "table_configuration": {},
                    }
                },
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            # Verify streaming tables were created for all 4 tables
            assert mock_sdp.create_streaming_table.call_count == 4
            create_calls = mock_sdp.create_streaming_table.call_args_list
            table_names = [c[1]["name"] for c in create_calls]
            assert set(table_names) == {"users", "orders", "events", "contacts"}

            # Verify apply_changes called 3 times:
            # - 1 for users (cdc)
            # - 2 for contacts (cdc_with_deletes: normal + delete flow)
            assert mock_sdp.apply_changes.call_count == 3

            # Verify snapshot was called for orders
            mock_sdp.apply_changes_from_snapshot.assert_called_once()
            assert mock_sdp.apply_changes_from_snapshot.call_args[1]["target"] == "orders"

            # Verify append_flow was called for events
            mock_sdp.append_flow.assert_called_once()
            assert mock_sdp.append_flow.call_args[1]["target"] == "events"

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
        ):
            ingest(mock_spark, spec)

            # Verify users table called with SCD_TYPE_2 and custom sequence_by
            apply_changes_kwargs = mock_sdp.apply_changes.call_args[1]
            assert apply_changes_kwargs["target"] == "users"
            assert apply_changes_kwargs["sequence_by"] == "col(created_at)"
            assert apply_changes_kwargs["stored_as_scd_type"] == "2"

            # Verify orders table called with SCD_TYPE_1
            snapshot_kwargs = mock_sdp.apply_changes_from_snapshot.call_args[1]
            assert snapshot_kwargs["target"] == "orders"
            assert snapshot_kwargs["stored_as_scd_type"] == "1"


class TestIngestEdgeCases:
    """Test edge cases and error scenarios."""

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
        ):
            ingest(mock_spark, spec)

            # Verify sequence_by uses cursor_field from metadata
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["sequence_by"] == "col(updated_at)"

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
        ):
            ingest(mock_spark, spec)

            # Verify scd_type defaults to "1"
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["stored_as_scd_type"] == "1"


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
        ):
            ingest(mock_spark, spec)

            # Verify primary_keys from spec is used
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["keys"] == ["user_id", "tenant_id"]

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
        ):
            ingest(mock_spark, spec)

            # Verify spec primary_keys overrides metadata
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["keys"] == ["composite_key_1", "composite_key_2"]

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
        ):
            ingest(mock_spark, spec)

            # Verify all values from spec are used
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["keys"] == ["id"]
            assert call_kwargs["sequence_by"] == "col(created_at)"
            assert call_kwargs["stored_as_scd_type"] == "2"

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
        ):
            ingest(mock_spark, spec)

            # Verify CDC was used (apply_changes called)
            mock_sdp.apply_changes.assert_called_once()
            mock_sdp.apply_changes_from_snapshot.assert_not_called()
            mock_sdp.append_flow.assert_not_called()


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
        ):
            ingest(mock_spark, spec)

            # Verify destination_table has the full qualified name
            mock_sdp.create_streaming_table.assert_called_once_with(
                name="`my_catalog`.`my_schema`.`my_users`"
            )
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["target"] == "`my_catalog`.`my_schema`.`my_users`"

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
        ):
            ingest(mock_spark, spec)

            # Verify destination_table defaults to source_table name
            mock_sdp.create_streaming_table.assert_called_once_with(name="users")
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["target"] == "users"

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
        ):
            ingest(mock_spark, spec)

            # Verify destination_table uses source table name with catalog.schema prefix
            mock_sdp.create_streaming_table.assert_called_once_with(
                name="`my_catalog`.`my_schema`.`users`"
            )
            call_kwargs = mock_sdp.apply_changes.call_args[1]
            assert call_kwargs["target"] == "`my_catalog`.`my_schema`.`users`"


class TestGetTableMetadataOptions:
    """Test that _get_table_metadata receives table_configs and passes them to Spark."""

    def test_get_table_metadata_receives_all_table_configs_as_json(self):
        """Test that _get_table_metadata passes combined table_configs as JSON to Spark option."""

        mock_spark = MagicMock()

        # Setup mock to return metadata DataFrame for both tables
        mock_row_users = MagicMock()
        mock_row_users.__getitem__ = lambda self, key: {
            "tableName": "users",
            "primary_keys": ["id"],
            "cursor_field": "updated_at",
            "ingestion_type": "cdc",
        }.get(key)
        mock_row_orders = MagicMock()
        mock_row_orders.__getitem__ = lambda self, key: {
            "tableName": "orders",
            "primary_keys": ["order_id"],
            "cursor_field": "modified_at",
            "ingestion_type": "cdc",
        }.get(key)
        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_row_users, mock_row_orders]
        # Chain: format().option().option().option().option().load()
        read_chain = mock_spark.read.format.return_value.option.return_value.option.return_value
        read_chain.option.return_value.option.return_value.load.return_value = mock_df

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        "table_configuration": {
                            "custom_option_1": "value1",
                            "custom_option_2": "value2",
                            # special key, excluded by get_table_configurations
                            "scd_type": "SCD_TYPE_1",
                            "primary_keys": ["id"],  # special key, excluded
                        },
                    }
                },
                {
                    "table": {
                        "source_table": "orders",
                        "table_configuration": {
                            "order_option": "order_value",
                        },
                    }
                },
            ],
        }

        ingest(mock_spark, spec)

        # Verify the metadata read chain received the combined table_configs as JSON
        # The chain is: spark.read.format().option().option().option().option().load()
        # The 4th option call should be ("tableConfigs", json.dumps(table_configs))
        opt_chain = mock_spark.read.format.return_value.option.return_value.option.return_value
        fourth_option_call = opt_chain.option.return_value.option
        fourth_option_call.assert_called_once()
        call_args = fourth_option_call.call_args[0]

        assert call_args[0] == "tableConfigs"
        # Parse the JSON to verify content (excluding special keys via get_table_configurations)
        passed_configs = json.loads(call_args[1])
        assert passed_configs == {
            "users": {"custom_option_1": "value1", "custom_option_2": "value2"},
            "orders": {"order_option": "order_value"},
        }

    def test_get_table_metadata_with_empty_table_configs_as_json(self):
        """Test that _get_table_metadata passes empty configs as JSON when tables have no configurations."""

        mock_spark = MagicMock()

        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: {
            "tableName": "users",
            "primary_keys": ["id"],
            "cursor_field": "updated_at",
            "ingestion_type": "cdc",
        }.get(key)
        mock_df = MagicMock()
        mock_df.collect.return_value = [mock_row]
        # Chain: format().option().option().option().option().load()
        read_chain = mock_spark.read.format.return_value.option.return_value.option.return_value
        read_chain.option.return_value.option.return_value.load.return_value = mock_df

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "users",
                        # No table_configuration
                    }
                },
            ],
        }

        ingest(mock_spark, spec)

        # Verify tableConfigs option was called with empty config as JSON
        opt_chain = mock_spark.read.format.return_value.option.return_value.option.return_value
        fourth_option_call = opt_chain.option.return_value.option
        fourth_option_call.assert_called_once()
        call_args = fourth_option_call.call_args[0]

        assert call_args[0] == "tableConfigs"
        passed_configs = json.loads(call_args[1])
        assert passed_configs == {"users": {}}


class TestTableConfigFiltering:
    """Test that table_config excludes reserved keys when passed to Spark read options."""

    def test_cdc_table_config_excludes_reserved_keys(self, base_metadata):
        """Test that CDC view function receives filtered table_config."""
        # Track decorated functions
        captured_view_funcs = []

        def capture_view(name):
            def decorator(f):
                captured_view_funcs.append((name, f))
                return f

            return decorator

        mock_sdp.view = MagicMock(side_effect=capture_view)

        # Create mock spark with trackable options call
        mock_spark = MagicMock()

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
                            "another_option": "value2",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            # Execute the captured view function
            assert len(captured_view_funcs) == 1
            view_name, view_func = captured_view_funcs[0]
            assert view_name == "users_staging"

            # Call the view function to trigger spark.readStream calls
            view_func()

            # Verify options() was called with filtered config (no reserved keys)
            stream_chain = mock_spark.readStream.format.return_value.option.return_value
            options_call = stream_chain.option.return_value.options
            options_call.assert_called_once()
            passed_options = options_call.call_args[1]
            assert passed_options == {
                "regular_option": "value",
                "another_option": "value2",
            }
            assert "scd_type" not in passed_options
            assert "sequence_by" not in passed_options
            assert "primary_keys" not in passed_options

    def test_snapshot_table_config_excludes_reserved_keys(self, base_metadata):
        """Test that snapshot view function receives filtered table_config."""
        captured_view_funcs = []

        def capture_view(name):
            def decorator(f):
                captured_view_funcs.append((name, f))
                return f

            return decorator

        mock_sdp.view = MagicMock(side_effect=capture_view)

        mock_spark = MagicMock()

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "orders",
                        "table_configuration": {
                            "scd_type": "SCD_TYPE_1",
                            "primary_keys": ["order_id"],
                            "batch_size": "1000",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            assert len(captured_view_funcs) == 1
            view_name, view_func = captured_view_funcs[0]
            assert view_name == "orders_staging"

            view_func()

            # Verify options() was called with filtered config
            options_call = (
                mock_spark.read.format.return_value.option.return_value.option.return_value.options
            )
            options_call.assert_called_once()
            passed_options = options_call.call_args[1]
            assert passed_options == {"batch_size": "1000"}
            assert "scd_type" not in passed_options
            assert "primary_keys" not in passed_options

    def test_append_table_config_excludes_reserved_keys(self, base_metadata):
        """Test that append flow function receives filtered table_config."""
        captured_flow_funcs = []

        def capture_append_flow(name, target):
            def decorator(f):
                captured_flow_funcs.append((name, target, f))
                return f

            return decorator

        mock_sdp.append_flow = MagicMock(side_effect=capture_append_flow)

        mock_spark = MagicMock()

        spec = {
            "connection_name": "test_connection",
            "objects": [
                {
                    "table": {
                        "source_table": "events",
                        "table_configuration": {
                            "scd_type": "APPEND_ONLY",
                            "sequence_by": "timestamp",
                            "custom_setting": "enabled",
                        },
                    }
                }
            ],
        }

        with patch(
            "pipeline.ingestion_pipeline._get_table_metadata",
            return_value=base_metadata,
        ):
            ingest(mock_spark, spec)

            assert len(captured_flow_funcs) == 1
            flow_name, flow_target, flow_func = captured_flow_funcs[0]
            assert flow_name == "events_staging"
            assert flow_target == "events"

            flow_func()

            # Verify options() was called with filtered config
            stream_chain = mock_spark.readStream.format.return_value.option.return_value
            options_call = stream_chain.option.return_value.options
            options_call.assert_called_once()
            passed_options = options_call.call_args[1]
            assert passed_options == {"custom_setting": "enabled"}
            assert "scd_type" not in passed_options
            assert "sequence_by" not in passed_options
