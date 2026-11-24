"""Generic connector contract tests for QuickBooks Online."""

import json
import os

import pytest

from databricks.labs.community_connector.sources.quickbooks.quickbooks import (
    QuickBooksLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestQuickBooksConnector(LakeflowConnectTests):
    connector_class = QuickBooksLakeflowConnect
    simulator_source = "quickbooks"
    replay_config = {
        "access_token": "simulator-token",
        "realm_id": "simulator-realm",
        "environment": "sandbox",
        "minor_version": "75",
        "timeout_seconds": "30",
        "max_retries": "5",
    }
    table_configs = {
        table: {
            "page_size": "100",
            "max_records_per_batch": "100",
            "max_incremental_window_seconds": "300",
        }
        for table in (
            "customers",
            "vendors",
            "accounts",
            "items",
            "invoices",
            "bills",
        )
    }

    def test_snapshot_identity_and_raw_payload_integrity(self) -> None:
        """Every simulated source ID is unique and preserved in raw_json."""
        expected_realm_id = self.connector._realm_id  # noqa: SLF001
        for table in self.connector.list_tables():
            records = list(self.connector.read_table(table, {}, self._opts(table))[0])
            ids = [record["id"] for record in records]
            assert len(ids) == len(set(ids)), f"{table} contains duplicate IDs"
            assert all(
                str(json.loads(record["raw_json"])["Id"]) == record["id"] for record in records
            ), f"{table} raw payload does not preserve the source ID"
            assert all(record["realm_id"] == expected_realm_id for record in records)

    def test_inactive_list_entities_are_not_filtered(self) -> None:
        if os.environ.get("CONNECTOR_TEST_MODE", "").strip().lower() == "live":
            pytest.skip("Inactive-entity fixture assertion is simulate-mode only")
        for table in ("customers", "vendors", "accounts", "items"):
            records = list(self.connector.read_table(table, {}, self._opts(table))[0])
            assert any(record["active"] is False for record in records), (
                f"{table} did not retain an inactive entity"
            )

    def test_every_table_incremental_window_includes_cursor_timestamp(self) -> None:
        if os.environ.get("CONNECTOR_TEST_MODE", "").strip().lower() == "live":
            pytest.skip("Fixed corpus boundary assertion is simulate-mode only")
        boundaries = {
            "customers": ("2026-07-21T09:30:00Z", ["2"]),
            "vendors": ("2026-07-20T11:00:00Z", ["11"]),
            "accounts": ("2026-07-20T13:00:00Z", ["21"]),
            "items": ("2026-07-20T15:00:00Z", ["31"]),
            "invoices": ("2026-07-20T16:00:00Z", ["40"]),
            "bills": ("2026-07-20T17:00:00Z", ["50"]),
        }

        for table, (cursor, expected_ids) in boundaries.items():
            records, _ = self.connector.read_table(
                table,
                {
                    "version": 2,
                    "realm_id": "simulator-realm",
                    "table_name": table,
                    "flow": "updates",
                    "updated_through": cursor,
                },
                {
                    **self._opts(table),
                    "incremental_overlap_seconds": "0",
                    "max_incremental_window_seconds": "604800",
                },
            )

            assert [record["id"] for record in records] == expected_ids
