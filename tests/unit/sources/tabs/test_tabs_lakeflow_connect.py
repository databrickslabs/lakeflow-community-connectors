"""Tests for the Tabs Platform LakeflowConnect connector.

Runs against the in-process source simulator described by
``source_simulator/specs/tabs/``. The connector is a
``SupportsPartitionedStream`` implementation, so the test class mixes in
``SupportsPartitionedStreamTests`` alongside ``LakeflowConnectTests``.

Tables exercised:

  * Partitioned incremental (read_partition):
        invoices, invoice_line_items, payments, customers, contracts
  * Snapshot, single-driver (read_table):
        obligations, items, categories

``contracts`` is ``cdc_with_deletes`` and is partitioned, so its
``read_table_deletes`` path is exercised directly below the suite tests.

Stand-in credentials below are values of the right shape; the simulator
does not validate them (Tabs uses a raw API key in the Authorization
header).
"""

from __future__ import annotations

from databricks.labs.community_connector.sources.tabs.tabs import (
    TabsLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestTabsConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = TabsLakeflowConnect
    simulator_source = "tabs"
    sample_records = 50

    # Stand-in credential. Tabs passes the raw API key in the
    # Authorization header; the simulator never validates it.
    replay_config = {"api_key": "test-key-123"}

    # ------------------------------------------------------------------
    # contracts.read_table_deletes
    #
    # ``contracts`` is cdc_with_deletes AND partitioned, so the base
    # suite's ``test_read_table_deletes`` (which only covers
    # non-partitioned tables) skips it. Exercise the deletes path here
    # directly so the soft-delete (deletedAt) scan is covered in
    # simulate mode.
    # ------------------------------------------------------------------

    def test_contracts_read_table_deletes(self):
        """read_table_deletes(contracts) returns (iterator, offset) and the
        offset is JSON-serializable and stable across a feed-back call."""
        result = self.connector.read_table_deletes(
            "contracts", {}, self._opts("contracts")
        )
        assert isinstance(result, tuple) and len(result) == 2, (
            "read_table_deletes must return (iterator, offset_dict)"
        )
        iterator, offset = result
        assert hasattr(iterator, "__iter__")
        records = list(iterator)
        for rec in records:
            assert isinstance(rec, dict)
        assert isinstance(offset, dict)

        # Feeding the returned offset back must terminate (empty, same offset).
        iterator2, offset2 = self.connector.read_table_deletes(
            "contracts", offset, self._opts("contracts")
        )
        assert list(iterator2) == []
        assert offset2 == offset

    def test_read_table_deletes_rejects_non_delete_table(self):
        """Only contracts carries a soft-delete field; others must raise."""
        import pytest

        with pytest.raises(ValueError):
            self.connector.read_table_deletes(
                "invoices", {}, self._opts("invoices")
            )
