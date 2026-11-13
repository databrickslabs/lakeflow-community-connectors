import json

import pytest

from databricks.labs.community_connector.sources.lancedb.lancedb import (
    LancedbLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestLancedbConnector(LakeflowConnectTests):
    connector_class = LancedbLakeflowConnect
    simulator_source = "lancedb"
    sample_records = 5
    # LanceDB Cloud auth is an ``x-api-key`` header; the project/database name
    # and region are interpolated into the host. The simulator never validates
    # these, so any strings of the right shape work. ``project_name`` and
    # ``region`` must pass ``_sanitize_identifier`` (alphanumeric / hyphen /
    # underscore only). __init__ reads: api_key, project_name (or database),
    # region.
    replay_config = {
        "api_key": "simulator-fake-api-key",
        "project_name": "simulator-project",
        "region": "us-east-1",
    }

    # ------------------------------------------------------------------
    # Ingestion-mode coverage (offline, simulate mode).
    #
    # The base suite exercises the default ``snapshot`` mode for every
    # discovered table (empty / snapshot table_options). The tests below drive
    # the two opt-in modes (``cdc`` and ``append``) directly against the
    # simulator, plus the snapshot metadata contract and the option-validation
    # paths, using the ``documents`` example table (its ``updated_at`` column is
    # a natural cdc cursor).
    # ------------------------------------------------------------------

    _CDC_OPTS = {
        "ingestion_type": "cdc",
        "cursor_field": "updated_at",
        "max_records_per_batch": "100",
    }

    def _read_until_converged(self, table, opts, max_iter=20):
        """Loop read_table feeding the offset back until two consecutive calls
        return the same offset. Returns the records from the first call."""
        offset = {}
        prev = None
        first_records = None
        for _ in range(max_iter):
            iterator, offset = self.connector.read_table(table, offset, opts)
            records = list(iterator)
            if first_records is None:
                first_records = records
            assert offset is None or isinstance(offset, dict)
            cur = json.dumps(offset, sort_keys=True)
            if prev is not None and cur == prev:
                return first_records
            prev = cur
        raise AssertionError(f"[{table}] did not converge in {max_iter} iterations")

    def test_snapshot_mode_metadata_and_read(self):
        """Default mode: snapshot metadata, full read, empty offset, terminates."""
        meta = self.connector.read_table_metadata("documents", {})
        assert meta["ingestion_type"] == "snapshot"
        assert meta["cursor_field"] is None
        assert meta["primary_keys"] == ["_rowid"]

        iterator, offset = self.connector.read_table("documents", {}, {})
        records = list(iterator)
        assert records, "snapshot read should return the corpus rows"
        assert offset == {}
        # Feeding the empty offset back terminates immediately.
        self._read_until_converged("documents", {})

    def test_append_mode_metadata_and_read(self):
        """append: no primary key/cursor; offset advances on _rowid and converges."""
        opts = {"ingestion_type": "append"}
        meta = self.connector.read_table_metadata("documents", opts)
        assert meta["ingestion_type"] == "append"
        assert meta["primary_keys"] == []
        assert meta["cursor_field"] is None

        iterator, offset = self.connector.read_table("documents", {}, opts)
        records = list(iterator)
        assert records, "append read should return the corpus rows"
        assert "rowid" in offset, "append must advance the offset (streaming contract)"
        # Feeding the advanced offset back yields no new rows and terminates.
        iterator2, offset2 = self.connector.read_table("documents", offset, opts)
        assert list(iterator2) == []
        assert offset2 == offset

    def test_cdc_mode_metadata(self):
        """cdc metadata carries the cursor_field and the _rowid primary key."""
        meta = self.connector.read_table_metadata("documents", self._CDC_OPTS)
        assert meta["ingestion_type"] == "cdc"
        assert meta["cursor_field"] == "updated_at"
        assert meta["primary_keys"] == ["_rowid"]

    def test_cdc_mode_advances_then_converges(self):
        """cdc advances the cursor on the first call, then converges to empty."""
        iterator, first_offset = self.connector.read_table(
            "documents", {}, self._CDC_OPTS
        )
        first_records = list(iterator)
        assert first_records, "first cdc read should return rows"
        assert first_offset.get("cursor") is not None

        # Every returned row carries the _rowid primary key (needed for upsert).
        assert all("_rowid" in r for r in first_records)

        # Second call from the advanced offset finds nothing newer → empty
        # iterator + unchanged offset, so Trigger.AvailableNow terminates.
        iterator2, second_offset = self.connector.read_table(
            "documents", first_offset, self._CDC_OPTS
        )
        assert list(iterator2) == []
        assert second_offset == first_offset

        # And the full termination loop converges.
        self._read_until_converged("documents", self._CDC_OPTS)

    def test_cdc_missing_cursor_field_raises(self):
        """ingestion_type=cdc without a cursor_field is a clear error."""
        with pytest.raises(ValueError):
            self.connector.read_table_metadata("documents", {"ingestion_type": "cdc"})
        with pytest.raises(ValueError):
            self.connector.read_table("documents", {}, {"ingestion_type": "cdc"})

    def test_invalid_ingestion_type_raises(self):
        """An unknown ingestion_type value is rejected."""
        with pytest.raises(ValueError):
            self.connector.read_table_metadata(
                "documents", {"ingestion_type": "bogus"}
            )

    def test_primary_keys_option_override(self):
        """A user-supplied primary_keys option overrides the default _rowid."""
        meta = self.connector.read_table_metadata(
            "documents", {"primary_keys": '["id"]'}
        )
        assert meta["primary_keys"] == ["id"]
