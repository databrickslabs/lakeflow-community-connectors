# pylint: disable=too-many-lines
"""Test suite for LakeflowConnect implementations.

Usage — each connector test file subclasses ``LakeflowConnectTests``::

    class TestMyConnector(LakeflowConnectTests):
        connector_class = MyLakeflowConnect

Config and table_configs are auto-loaded from a ``configs/`` directory next to
the test file (``dev_config.json`` and ``dev_table_config.json``).  Override
the class attributes to supply them explicitly.

Then run::

    pytest tests/unit/sources/my_source/ -v                       # all tests
    pytest tests/unit/sources/my_source/ -k "test_read_table"     # one test
"""

import inspect
import json
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type

import pytest
from pyspark.sql.types import *  # pylint: disable=wildcard-import,unused-wildcard-import

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartition,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.libs.utils import parse_value
from tests.unit.sources.mock_framework import (
    MODE_REPLAY,
    RecordReplayPatch,
    get_mode,
)

VALID_INGESTION_TYPES = {"snapshot", "cdc", "cdc_with_deletes", "append"}
_INVALID_TABLE_NAME = "__nonexistent_table_$$_9z9z9z__"


class LakeflowConnectTests:
    """Base test class for LakeflowConnect connectors.

    Subclass this and set the class attributes below.  Pytest discovers the
    subclass and runs each ``test_*`` method as a separate test item.

    Class attributes:
        connector_class: The LakeflowConnect subclass to test (required).
        config: Init options dict passed to connector_class.__init__.
        table_configs: Per-table options keyed by table name.
        sample_records: Max records to consume per table during read tests.
        test_utils_class: Optional LakeflowConnectWriteTestUtils subclass for
            write-back tests.
    """

    connector_class: Type[LakeflowConnect] = None  # type: ignore[assignment]
    config: dict = None  # type: ignore[assignment]
    table_configs: Dict[str, Dict[str, Any]] = None  # type: ignore[assignment]
    sample_records: int = 50
    test_utils_class = None

    # Query-param names whose values are non-deterministic (e.g. now()-based
    # timestamps, request IDs, nonces). The mock framework ignores these when
    # matching recorded interactions against incoming requests. Setting this
    # affects both record and replay modes.
    record_replay_ignore_query_params: frozenset = frozenset()

    # Records-per-response kept in the cassette at record time. Each response
    # body's records array is truncated to this many, and pagination pointers
    # are stripped so connectors stop after one page. Keep small (≤10) to
    # avoid bloating git history with real API data.
    record_replay_sample_size: int = 5

    # At replay time, expand each response's records up to this many via
    # type-aware variation (ints, ISO timestamps, UUIDs, strings). 0 = return
    # responses exactly as recorded. Use this when tests need more records
    # than the sample size provides.
    record_replay_synthesize_count: int = 0

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    @classmethod
    def _config_dir(cls) -> Path:
        """Return the ``configs/`` directory next to the subclass test file."""
        return Path(inspect.getfile(cls)).parent / "configs"

    @classmethod
    def _cassette_dir(cls) -> Path:
        """Return the ``cassettes/`` directory next to the subclass test file."""
        return Path(inspect.getfile(cls)).parent / "cassettes"

    @classmethod
    def _cassette_path(cls) -> Path:
        return cls._cassette_dir() / f"{cls.__name__}.json"

    @classmethod
    def _load_config(cls) -> dict:
        """Load ``dev_config.json`` — or ``replay_config.json`` in replay mode."""
        mode = get_mode()
        if mode == MODE_REPLAY:
            replay_path = cls._config_dir() / "replay_config.json"
            if replay_path.exists():
                with open(replay_path, "r") as f:
                    return json.load(f)
        path = cls._config_dir() / "dev_config.json"
        assert path.exists(), (
            f"Config file not found: {path}\n"
            "  Fix: Create dev_config.json with connector credentials "
            "(or replay_config.json for replay-mode fake credentials)."
        )
        with open(path, "r") as f:
            return json.load(f)

    @classmethod
    def _load_table_configs(cls) -> Dict[str, Dict[str, Any]]:
        """Load ``dev_table_config.json`` if it exists, else return {}."""
        path = cls._config_dir() / "dev_table_config.json"
        if not path.exists():
            return {}
        with open(path, "r") as f:
            return json.load(f)

    @classmethod
    def setup_class(cls):
        assert cls.connector_class is not None, (
            "Set connector_class in your test subclass"
        )

        # Install the HTTP record/replay patch BEFORE creating the connector
        # so any HTTP in __init__ is captured / replayed too. In live mode
        # (the default) this is a no-op context.
        cls._record_replay_patch = RecordReplayPatch(
            mode=get_mode(),
            cassette_path=cls._cassette_path(),
            source=cls.__module__.split(".")[-2] if "." in cls.__module__ else "",
            ignore_query_params=frozenset(cls.record_replay_ignore_query_params),
            sample_size=cls.record_replay_sample_size,
            synthesize_count=cls.record_replay_synthesize_count,
        )
        cls._record_replay_patch.__enter__()

        try:
            if cls.config is None:
                cls.config = cls._load_config()
            if cls.table_configs is None:
                cls.table_configs = cls._load_table_configs()
            cls.connector = cls.connector_class(cls.config)
            cls.test_utils = None
            if cls.test_utils_class:
                try:
                    cls.test_utils = cls.test_utils_class(cls.config)
                except Exception:
                    pass
        except Exception:
            # Don't leave the patch installed if setup fails.
            cls._record_replay_patch.__exit__(None, None, None)
            raise

    @classmethod
    def teardown_class(cls):
        patch = getattr(cls, "_record_replay_patch", None)
        if patch is not None:
            patch.__exit__(None, None, None)
            cls._record_replay_patch = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _opts(self, table: str) -> dict:
        return self.table_configs.get(table, {})

    def _tables(self) -> List[str]:
        return self.connector.list_tables()

    def _is_partitioned(self, table: str) -> bool:
        """Check if a table uses partitioned reads instead of read_table."""
        if isinstance(self.connector, SupportsPartitionedStream):
            return self.connector.is_partitioned(table)
        if isinstance(self.connector, SupportsPartition):
            return True
        return False

    def _non_partitioned_tables(self) -> List[str]:
        """Return tables that use read_table (not partitioned reads)."""
        return [t for t in self._tables() if not self._is_partitioned(t)]

    def _partitioned_tables(self) -> List[str]:
        """Return tables that use partitioned reads."""
        return [t for t in self._tables() if self._is_partitioned(t)]

    # ------------------------------------------------------------------
    # test_initialization
    # ------------------------------------------------------------------

    def test_initialization(self):
        """Connector was created successfully in setup_class."""
        assert self.connector is not None, (
            "Connector is None after __init__.\n"
            "  Fix: __init__ must not return None."
        )

    # ------------------------------------------------------------------
    # test_partition_suite
    # ------------------------------------------------------------------

    def test_partition_requires_lakeflow_connect(self):
        """SupportsPartition / SupportsPartitionedStream must also subclass LakeflowConnect."""
        cls = type(self.connector)
        if not issubclass(cls, (SupportsPartition, SupportsPartitionedStream)):
            pytest.skip("Connector does not use partition mixins")
        assert issubclass(cls, LakeflowConnect), (
            f"{cls.__name__} extends {SupportsPartition.__name__} or "
            f"{SupportsPartitionedStream.__name__} but not {LakeflowConnect.__name__}.\n"
            "  Fix: Use multiple inheritance, e.g. "
            f"class {cls.__name__}(LakeflowConnect, SupportsPartition): ..."
        )

    # ------------------------------------------------------------------
    # test_list_tables
    # ------------------------------------------------------------------

    def test_list_tables(self):
        """list_tables returns a non-empty list of unique strings."""
        tables = self.connector.list_tables()
        assert isinstance(tables, list), (
            f"Expected list, got {type(tables).__name__}.\n"
            "  Fix: list_tables() must return list[str]."
        )
        assert tables, (
            "list_tables() returned an empty list.\n"
            "  Fix: Must return at least one table name."
        )
        for i, t in enumerate(tables):
            assert isinstance(t, str), (
                f"Table at index {i} is {type(t).__name__}, expected str.\n"
                "  Fix: Every element of list_tables() must be a str."
            )
        dupes = [t for t in tables if tables.count(t) > 1]
        assert not dupes, (
            f"Duplicate table names: {sorted(set(dupes))}.\n"
            "  Fix: list_tables() must return unique names."
        )

    # ------------------------------------------------------------------
    # test_invalid_table_name
    # ------------------------------------------------------------------

    def test_invalid_table_name(self):
        """get_table_schema, read_table_metadata, and read_table raise on an invalid table name."""
        methods = {
            "get_table_schema": lambda: self.connector.get_table_schema(_INVALID_TABLE_NAME, {}),
            "read_table_metadata": lambda: self.connector.read_table_metadata(_INVALID_TABLE_NAME, {}),
            "read_table": lambda: self.connector.read_table(_INVALID_TABLE_NAME, {}, {}),
        }
        errors = []
        for name, fn in methods.items():
            try:
                fn()
                errors.append(
                    f"{name}() did not raise for invalid table '{_INVALID_TABLE_NAME}'.\n"
                    f"  Fix: {name}() should raise an exception for unknown table names."
                )
            except Exception:
                pass
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_get_table_schema  (per-table, collected)
    # ------------------------------------------------------------------

    def test_get_table_schema(self):
        """get_table_schema returns a valid StructType for every table."""
        errors = []
        for table in self._tables():
            try:
                schema = self.connector.get_table_schema(table, self._opts(table))
                if not isinstance(schema, StructType):
                    errors.append(
                        f"[{table}] Expected StructType, got {type(schema).__name__}.\n"
                        "  Fix: get_table_schema() must return pyspark.sql.types.StructType."
                    )
                    continue
                if not schema.fields:
                    errors.append(
                        f"[{table}] Schema has no fields.\n"
                        "  Fix: Add StructField entries to the returned StructType."
                    )
                    continue
                names = [f.name for f in schema.fields]
                dupes = [n for n in names if names.count(n) > 1]
                if dupes:
                    errors.append(
                        f"[{table}] Duplicate field names: {sorted(set(dupes))}.\n"
                        "  Fix: Schema field names must be unique."
                    )
            except Exception as e:
                errors.append(f"[{table}] get_table_schema raised: {e}")
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_read_table_metadata  (per-table, collected)
    # ------------------------------------------------------------------

    def test_read_table_metadata(self):  # pylint: disable=too-many-branches
        """read_table_metadata returns valid metadata for every table."""
        errors = []
        for table in self._tables():
            try:
                err = self._validate_metadata(table)
                if err:
                    errors.append(err)
            except Exception as e:
                errors.append(f"[{table}] read_table_metadata raised: {e}")
        if errors:
            pytest.fail("\n\n".join(errors))

    def _validate_metadata(self, table: str) -> Optional[str]:  # pylint: disable=too-many-return-statements,too-many-branches
        """Validate metadata for one table. Returns error string or None."""
        metadata = self.connector.read_table_metadata(table, self._opts(table))
        if not isinstance(metadata, dict):
            return (
                f"[{table}] Expected dict, got {type(metadata).__name__}.\n"
                "  Fix: read_table_metadata() must return a dict."
            )

        # ingestion_type
        it = metadata.get("ingestion_type")
        if it is None:
            return (
                f"[{table}] Missing 'ingestion_type'.\n"
                f"  Fix: Must be one of {sorted(VALID_INGESTION_TYPES)}."
            )
        if it not in VALID_INGESTION_TYPES:
            return (
                f"[{table}] Invalid ingestion_type '{it}'.\n"
                f"  Fix: Must be one of {sorted(VALID_INGESTION_TYPES)}."
            )

        # primary_keys (required for non-append)
        if it != "append":
            pks = metadata.get("primary_keys")
            if pks is None:
                return (
                    f"[{table}] Missing 'primary_keys'.\n"
                    f"  Fix: Required for ingestion_type='{it}'."
                )
            if not isinstance(pks, list) or not pks:
                return (
                    f"[{table}] primary_keys must be a non-empty list, got {pks!r}.\n"
                    "  Fix: Provide at least one column name string."
                )
            try:
                schema = self.connector.get_table_schema(table, self._opts(table))
                missing = [pk for pk in pks if not self._field_in_schema(pk, schema)]
                if missing:
                    return (
                        f"[{table}] primary_keys {missing} not in schema {schema.fieldNames()}.\n"
                        "  Fix: Add them to get_table_schema() or fix the primary_keys list."
                    )
            except Exception:
                pass

        # cursor_field (required for cdc / cdc_with_deletes)
        if it not in ("snapshot", "append"):
            cf = metadata.get("cursor_field")
            if cf is None:
                return (
                    f"[{table}] Missing 'cursor_field'.\n"
                    f"  Fix: Required for ingestion_type='{it}'."
                )
            if not isinstance(cf, str):
                return (
                    f"[{table}] cursor_field must be str, got {type(cf).__name__}.\n"
                    "  Fix: Provide a single column name string."
                )
            try:
                schema = self.connector.get_table_schema(table, self._opts(table))
                if not self._field_in_schema(cf, schema):
                    return (
                        f"[{table}] cursor_field '{cf}' not in schema {schema.fieldNames()}.\n"
                        "  Fix: Add it to get_table_schema() or choose a different cursor_field."
                    )
            except Exception:
                pass

        # cdc_with_deletes requires read_table_deletes
        if it == "cdc_with_deletes" and not hasattr(self.connector, "read_table_deletes"):
            return (
                f"[{table}] ingestion_type='cdc_with_deletes' but read_table_deletes() not implemented.\n"
                "  Fix: Implement read_table_deletes() or change ingestion_type to 'cdc'."
            )

        return None

    # ------------------------------------------------------------------
    # test_read_table  (per-table, collected)
    # ------------------------------------------------------------------

    def test_read_table(self):
        """read_table returns valid (iterator, offset) for non-partitioned tables."""
        tables = self._non_partitioned_tables()
        if not tables:
            pytest.skip("All tables use partitioned reads")
        errors = []
        for table in tables:
            err = self._validate_read(
                table, self.connector.read_table, "read_table", is_read_table=True
            )
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_read_table_deletes  (per-table, collected)
    # ------------------------------------------------------------------

    def test_read_table_deletes(self):
        """read_table_deletes works for all non-partitioned cdc_with_deletes tables."""
        if not hasattr(self.connector, "read_table_deletes"):
            pytest.skip("Connector does not implement read_table_deletes")

        tables = [
            t for t in self._non_partitioned_tables()
            if self._ingestion_type(t) == "cdc_with_deletes"
        ]
        if not tables:
            pytest.skip("No tables with ingestion_type 'cdc_with_deletes'")

        errors = []
        for table in tables:
            err = self._validate_read(
                table, self.connector.read_table_deletes,
                "read_table_deletes", is_read_table=False,
            )
            if err:
                errors.append(err)
        if errors:
            pytest.fail("\n\n".join(errors))

    # ------------------------------------------------------------------
    # test_micro_batch_offset_contract  (per-table, collected)
    # ------------------------------------------------------------------

    def test_micro_batch_offset_contract(self):
        """read_table handles the micro-batch offset round-trip.

        The framework calls read_table repeatedly, passing the previous offset
        back. This test makes two calls per table to verify the contract.
        """
        tables = self._non_partitioned_tables()
        if not tables:
            pytest.skip("All tables use partitioned reads")
        errors = []
        for table in tables:
            try:
                err = self._validate_offset_contract(table)
                if err:
                    errors.append(err)
            except Exception as e:
                errors.append(
                    f"[{table}] Offset contract error: {e}\n"
                    "  Fix: read_table() must handle receiving its own previously-returned offset."
                )
        if errors:
            pytest.fail("\n\n".join(errors))

    def _validate_offset_contract(self, table: str) -> Optional[str]:
        """Two-call offset round-trip check. Returns error string or None."""
        # Call 1
        result1 = self.connector.read_table(table, {}, self._opts(table))
        if not isinstance(result1, tuple) or len(result1) != 2:
            return (
                f"[{table}] read_table returned {type(result1).__name__}, expected 2-tuple.\n"
                "  Fix: read_table() must return (iterator, offset_dict)."
            )

        iter1, offset1 = result1
        self._consume(iter1)

        if offset1 is not None and not isinstance(offset1, dict):
            return (
                f"[{table}] Offset must be dict or None, got {type(offset1).__name__}.\n"
                "  Fix: Return a dict as the offset."
            )

        ingestion_type = self._ingestion_type(table)
        if offset1 is None:
            if ingestion_type != "snapshot":
                return (
                    f"[{table}] Offset is None but ingestion_type is '{ingestion_type}'.\n"
                    "  Fix: read_table() must return a non-None offset dict for "
                    "non-snapshot tables so the framework can track micro-batch progress."
                )
            return None  # Snapshot table, nothing more to test.

        try:
            json.dumps(offset1)
        except (TypeError, ValueError) as e:
            return (
                f"[{table}] Offset not JSON-serializable: {e}\n"
                "  Fix: Use only strings/numbers/booleans/None in the offset dict."
            )

        # Call 2 — pass offset1 back
        result2 = self.connector.read_table(table, offset1, self._opts(table))
        if not isinstance(result2, tuple) or len(result2) != 2:
            return (
                f"[{table}] Second read_table call returned invalid format.\n"
                "  Fix: read_table() must handle its own offset as start_offset."
            )

        _, offset2 = result2
        if offset2 is not None and not isinstance(offset2, dict):
            return (
                f"[{table}] Second offset must be dict or None, got {type(offset2).__name__}."
            )

        return None

    # ------------------------------------------------------------------
    # Shared read-validation helper
    # ------------------------------------------------------------------

    def _validate_read(  # pylint: disable=too-many-return-statements,too-many-branches
        self,
        table: str,
        read_fn: Callable,
        method_name: str,
        is_read_table: bool = True,
    ) -> Optional[str]:
        """Validate a read method for one table. Returns error string or None."""
        try:
            result = read_fn(table, {}, self._opts(table))

            if not isinstance(result, tuple) or len(result) != 2:
                return (
                    f"[{table}] {method_name}() returned {type(result).__name__}, expected 2-tuple.\n"
                    f"  Fix: {method_name}() must return (records_iterator, offset_dict)."
                )

            iterator, offset = result

            if not hasattr(iterator, "__iter__"):
                return (
                    f"[{table}] First element is not iterable: {type(iterator).__name__}.\n"
                    f"  Fix: {method_name}() must return an iterator/generator as the first element."
                )

            if offset is not None and not isinstance(offset, dict):
                return (
                    f"[{table}] Offset must be dict or None, got {type(offset).__name__}.\n"
                    f"  Fix: {method_name}() must return a dict (or None) as the second element."
                )

            if isinstance(offset, dict):
                try:
                    json.dumps(offset)
                except (TypeError, ValueError) as e:
                    return (
                        f"[{table}] Offset not JSON-serializable: {e}\n"
                        "  Fix: Use only strings/numbers/booleans/None in the offset dict."
                    )

            # Consume iterator
            records: List[dict] = []
            try:
                for rec in iterator:
                    if not isinstance(rec, dict):
                        return (
                            f"[{table}] Record is {type(rec).__name__}, expected dict.\n"
                            f"  Fix: {method_name}() must yield dicts."
                        )
                    records.append(rec)
                    if len(records) >= self.sample_records:
                        break
            except Exception as e:
                return (
                    f"[{table}] Iterator raised: {e}\n"
                    f"  Fix: The iterator from {method_name}() must not raise during iteration."
                )

            # Parse with schema
            try:
                schema = self.connector.get_table_schema(table, self._opts(table))
            except Exception as e:
                return f"[{table}] get_table_schema raised during record validation: {e}"

            for i, rec in enumerate(records):
                try:
                    parse_value(rec, schema)
                except Exception as e:
                    return (
                        f"[{table}] Record {i} failed schema parsing: {e}\n"
                        f"  Fix: Check that {method_name}() returns raw values compatible "
                        f"with get_table_schema('{table}'). The framework handles type conversion."
                    )

            # Field-level checks
            for rec in records:
                if is_read_table:
                    violations = self._check_non_nullable(rec, schema)
                    if violations:
                        return (
                            f"[{table}] Non-nullable field(s) are None: {violations}\n"
                            f"  Fix: Make these fields nullable=True in schema, or populate them in {method_name}()."
                        )

                if self._all_null(rec, schema):
                    return (
                        f"[{table}] All columns are null in a record.\n"
                        f"  Fix: API response is likely not being parsed correctly in {method_name}()."
                    )

                if not is_read_table:
                    try:
                        meta = self.connector.read_table_metadata(table, self._opts(table))
                        pks = meta.get("primary_keys", [])
                        missing = [pk for pk in pks if self._nested_get(rec, pk) is None]
                        if missing:
                            return (
                                f"[{table}] Deleted record missing primary key(s): {missing}\n"
                                "  Fix: read_table_deletes() must include primary keys in every record."
                            )
                    except Exception:
                        pass

            return None

        except Exception as e:
            return f"[{table}] {method_name}() raised: {e}\n{traceback.format_exc()}"

    # ------------------------------------------------------------------
    # Write-back tests
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

    def test_write_to_source(self):  # pylint: disable=too-many-branches
        """generate_rows_and_write works on each insertable table."""
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

    def test_incremental_after_write(self):  # pylint: disable=too-many-branches
        """Incremental read after write returns the written row."""
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

    def test_delete_and_read_deletes(self):
        """Delete a row and verify it appears in read_table_deletes."""
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
    # Internal utilities
    # ------------------------------------------------------------------

    def _ingestion_type(self, table: str) -> Optional[str]:
        try:
            meta = self.connector.read_table_metadata(table, self._opts(table))
            return meta.get("ingestion_type")
        except Exception:
            return None

    def _consume(self, iterator, max_records: int = None) -> List[dict]:
        if max_records is None:
            max_records = self.sample_records
        out: List[dict] = []
        for rec in iterator:
            out.append(rec)
            if len(out) >= max_records:
                break
        return out

    def _field_in_schema(self, path: str, schema) -> bool:
        if "." not in path:
            return path in schema.fieldNames()
        head, tail = path.split(".", 1)
        if head not in schema.fieldNames():
            return False
        dt = schema[head].dataType
        return isinstance(dt, StructType) and self._field_in_schema(tail, dt)

    def _check_non_nullable(
        self, record: dict, schema: StructType, prefix: str = ""
    ) -> List[str]:
        violations = []
        for f in schema.fields:
            path = f"{prefix}.{f.name}" if prefix else f.name
            val = record.get(f.name) if isinstance(record, dict) else None
            if not f.nullable and val is None:
                violations.append(path)
            if isinstance(f.dataType, StructType) and isinstance(val, dict):
                violations.extend(self._check_non_nullable(val, f.dataType, path))
        return violations

    def _all_null(self, record: dict, schema: StructType) -> bool:
        for f in schema.fields:
            val = record.get(f.name) if isinstance(record, dict) else None
            if val is not None and not isinstance(val, dict):
                return False
            if isinstance(f.dataType, StructType) and isinstance(val, dict):
                if not self._all_null(val, f.dataType):
                    return False
        return True

    def _nested_get(self, record: dict, path: str) -> Any:
        cur = record
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def _rows_present(
        self,
        written_rows: List[Dict],
        returned_records: List[Dict],
        col_map: Dict[str, str],
    ) -> bool:
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
