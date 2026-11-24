"""Write-back test suite for LakeflowConnect implementations.

These tests mutate a real source (POST/PUT/DELETE), so they require
``CONNECTOR_TEST_MODE=live`` and a working set of credentials. They
auto-skip in simulate mode and are therefore safe to leave on the test
class — CI runs in simulate mode by default and skips them.

Use alongside :class:`LakeflowConnectTests`. The write-back mixin must
appear **before** :class:`LakeflowConnectTests` in the inheritance list
so its ``setup_class`` runs first and chains via ``super()``::

    from tests.unit.sources.test_suite import LakeflowConnectTests
    from tests.unit.sources.test_write_back_suite import (
        LakeflowConnectWriteBackTests,
    )

    class TestMyConnector(LakeflowConnectWriteBackTests, LakeflowConnectTests):
        connector_class = MyLakeflowConnect
        test_utils_class = MyWriteTestUtils

To run only write-back tests against a live source::

    CONNECTOR_TEST_MODE=live \
      CONNECTOR_TEST_CONFIG_PATH=~/secrets/my_source.json \
      pytest tests/unit/sources/my_source/ -v -k "write or delete or insertable or deletable"
"""

from typing import Dict, List, Optional

import pytest

from databricks.labs.community_connector.source_simulator import MODE_LIVE

from tests.unit.sources.test_suite import _resolve_env_mode_for_simulator


class LakeflowConnectWriteBackTests:
    """Mixin holding write-back tests. Inherit BEFORE LakeflowConnectTests.

    Class attributes:
        test_utils_class: A LakeflowConnectWriteTestUtils subclass that
            knows how to write to / delete from the source. Set this on
            the connector test subclass.
    """

    test_utils_class = None

    @classmethod
    def setup_class(cls):
        # Chain to LakeflowConnectTests.setup_class first so that
        # cls.connector and cls.config are populated.
        super().setup_class()
        cls.test_utils = None
        if cls.test_utils_class:
            try:
                cls.test_utils = cls.test_utils_class(cls.config)
            except Exception:
                pass

    @classmethod
    def _skip_if_simulate_mode(cls) -> None:
        """Write-back tests POST/PUT/DELETE against the source. In simulate
        mode there's no real source to mutate (and stubbed write endpoints
        aren't authored in the spec), so skip. ``CONNECTOR_TEST_MODE=live``
        opts back in by routing requests to the real source."""
        if cls.simulator_source and _resolve_env_mode_for_simulator(
            cls.simulator_source
        ) != MODE_LIVE:
            pytest.skip(
                "Write-back test requires a real source; "
                "set CONNECTOR_TEST_MODE=live to run."
            )

    # ------------------------------------------------------------------
    # test_list_insertable_tables
    # ------------------------------------------------------------------

    def test_list_insertable_tables(self):
        """list_insertable_tables returns a subset of list_tables."""
        if not self.test_utils:
            pytest.skip("No test_utils_class configured")

        insertable = self.test_utils.list_insertable_tables()
        assert isinstance(insertable, list), (
            f"Expected list, got {type(insertable).__name__}.\n"
            "  Fix: list_insertable_tables() must return list[str]."
        )
        all_tables = set(self._tables())
        invalid = set(insertable) - all_tables
        assert not invalid, (
            f"Insertable tables not in list_tables(): {invalid}.\n"
            "  Fix: Every insertable table must also appear in list_tables()."
        )

    # ------------------------------------------------------------------
    # test_list_deletable_tables
    # ------------------------------------------------------------------

    def test_list_deletable_tables(self):
        """list_deletable_tables returns valid cdc_with_deletes tables."""
        if not self.test_utils:
            pytest.skip("No test_utils_class configured")
        if not hasattr(self.test_utils, "list_deletable_tables"):
            pytest.skip("test_utils does not implement list_deletable_tables")

        deletable = self.test_utils.list_deletable_tables()
        assert isinstance(deletable, list)
        if not deletable:
            pytest.skip("No deletable tables configured")

        all_tables = set(self._tables())
        invalid = set(deletable) - all_tables
        assert not invalid, (
            f"Deletable tables not in list_tables(): {invalid}.\n"
            "  Fix: Every deletable table must also appear in list_tables()."
        )

        errors = []
        for table in deletable:
            it = self._ingestion_type(table)
            if it != "cdc_with_deletes":
                errors.append(
                    f"[{table}] ingestion_type is '{it}', expected 'cdc_with_deletes'.\n"
                    "  Fix: Change ingestion_type or remove from list_deletable_tables()."
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_write_to_source
    # ------------------------------------------------------------------

    def test_write_to_source(self):  # pylint: disable=too-many-branches
        """generate_rows_and_write works on each insertable table."""
        self._skip_if_simulate_mode()
        if not self.test_utils:
            pytest.skip("No test_utils_class configured")

        insertable = self.test_utils.list_insertable_tables()
        if not insertable:
            pytest.skip("No insertable tables")

        errors = []
        for table in insertable:
            try:
                result = self.test_utils.generate_rows_and_write(table, 1)
                if not isinstance(result, tuple) or len(result) != 3:
                    errors.append(f"[{table}] Expected 3-tuple, got {type(result).__name__}.")
                    continue

                success, rows, col_map = result
                if not success:
                    errors.append(f"[{table}] Write returned success=False.")
                    continue
                if len(rows) != 1:
                    errors.append(f"[{table}] Expected 1 row, got {len(rows)}.")
                    continue
                if not col_map:
                    errors.append(f"[{table}] Empty column_mapping on success.")
                    continue
                for i, row in enumerate(rows):
                    if not isinstance(row, dict):
                        errors.append(f"[{table}] Row {i} is {type(row).__name__}, expected dict.")
                        break
            except Exception as e:
                errors.append(f"[{table}] generate_rows_and_write raised: {e}")
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_incremental_after_write
    # ------------------------------------------------------------------

    def test_incremental_after_write(self):  # pylint: disable=too-many-branches
        """Incremental read after write returns the written row."""
        self._skip_if_simulate_mode()
        if not self.test_utils:
            pytest.skip("No test_utils_class configured")

        insertable = [
            t for t in self.test_utils.list_insertable_tables()
            if not self._is_partitioned(t)
        ]
        if not insertable:
            pytest.skip("No insertable non-partitioned tables")

        errors = []
        for table in insertable:
            try:
                err = self._validate_incremental_after_write(table)
                if err:
                    errors.append(err)
            except Exception as e:
                errors.append(f"[{table}] Incremental test error: {e}")
        if errors:
            pytest.fail("\n\n".join(errors))

    def _validate_incremental_after_write(self, table: str) -> Optional[str]:
        """Write 1 row, then read incrementally. Returns error or None."""
        metadata = self.connector.read_table_metadata(table, self._opts(table))
        ingestion_type = metadata.get("ingestion_type", "cdc")

        # Initial read
        initial_result = self.connector.read_table(table, {}, self._opts(table))
        if not isinstance(initial_result, tuple) or len(initial_result) != 2:
            return f"[{table}] Initial read_table returned invalid format."
        initial_iter, initial_offset = initial_result
        initial_count = 0
        if ingestion_type == "snapshot":
            for _ in initial_iter:
                initial_count += 1

        # Write
        write_result = self.test_utils.generate_rows_and_write(table, 1)
        if not isinstance(write_result, tuple) or len(write_result) != 3:
            return f"[{table}] generate_rows_and_write returned invalid format."
        success, written_rows, col_map = write_result
        if not success:
            return f"[{table}] Write returned success=False."

        # Read after write — create a fresh connector instance, just like a
        # real pipeline trigger would, so connectors that cap cursors at init
        # time can observe the newly-written data.
        fresh_connector = self.connector_class(self.config)
        after_result = fresh_connector.read_table(table, initial_offset, self._opts(table))
        if not isinstance(after_result, tuple) or len(after_result) != 2:
            return f"[{table}] Read after write returned invalid format."
        after_iter, _ = after_result
        after_records = list(after_iter)
        count = len(after_records)

        if ingestion_type in ("cdc", "cdc_with_deletes", "append"):
            if count < 1:
                return (
                    f"[{table}] Expected >= 1 record for {ingestion_type}, got {count}.\n"
                    "  Fix: Offset logic not tracking new data correctly."
                )
        else:
            expected = initial_count + 1
            if count != expected:
                return (
                    f"[{table}] Expected {expected} records for snapshot, got {count}.\n"
                    "  Fix: Snapshot re-read should include newly written row."
                )

        if not self._rows_present(written_rows, after_records, col_map):
            return (
                f"[{table}] Written row not found in read results.\n"
                "  Fix: Check column_mapping and offset logic."
            )

        return None

    # ------------------------------------------------------------------
    # test_delete_and_read_deletes
    # ------------------------------------------------------------------

    def test_delete_and_read_deletes(self):
        """Delete a row and verify it appears in read_table_deletes."""
        self._skip_if_simulate_mode()
        if not self.test_utils:
            pytest.skip("No test_utils_class configured")
        if not hasattr(self.test_utils, "delete_rows"):
            pytest.skip("test_utils does not implement delete_rows")
        if not hasattr(self.connector, "read_table_deletes"):
            pytest.skip("Connector does not implement read_table_deletes")

        try:
            deletable = self.test_utils.list_deletable_tables()
        except Exception:
            deletable = []
        if not deletable:
            pytest.skip("No deletable tables configured")

        table = deletable[0]
        delete_result = self.test_utils.delete_rows(table, 1)
        assert isinstance(delete_result, tuple) and len(delete_result) == 3, (
            f"delete_rows returned {type(delete_result)}, expected 3-tuple."
        )
        success, deleted_rows, col_map = delete_result
        assert success and deleted_rows, "delete_rows failed or returned empty."

        read_result = self.connector.read_table_deletes(table, {}, self._opts(table))
        assert isinstance(read_result, tuple) and len(read_result) == 2, (
            f"read_table_deletes returned {type(read_result)}, expected 2-tuple."
        )
        iterator, _ = read_result
        deleted_records = list(iterator)

        assert self._rows_present(deleted_rows, deleted_records, col_map), (
            f"Deleted row not found in read_table_deletes results.\n"
            "  Fix: Check column_mapping and that the source API surfaces deletes."
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _rows_present(
        self,
        written_rows: List[Dict],
        returned_records: List[Dict],
        col_map: Dict[str, str],
    ) -> bool:
        import json

        if not written_rows or not col_map:
            return True

        written_sigs = []
        for row in written_rows:
            sig = {c: row.get(c) for c in col_map if c in row}
            written_sigs.append(sig)

        returned_sigs = []
        for rec in returned_records:
            if isinstance(rec, str):
                try:
                    rec = json.loads(rec)
                except Exception:
                    continue
            if isinstance(rec, dict):
                sig = {}
                for wc, rc in col_map.items():
                    v = self._nested_get(rec, rc)
                    if v is not None:
                        sig[wc] = v
                if sig:
                    returned_sigs.append(sig)

        return all(ws in returned_sigs for ws in written_sigs)
