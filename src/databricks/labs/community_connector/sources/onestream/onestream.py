"""OneStream (CPM) community connector.

Reads OneStream application data through the REST API
``DataProvider/GetAdoDataSetForSqlCommand`` using connector-generated,
read-only SQL (see ``onestream_sql.py``).

Tables
------
* ``cube_data`` -- stored cube cells from ``DataRecord{YYYY}``, unpivoted to one
  row per month. Incremental (``cdc``) by data unit: OneStream stamps every
  data unit (Cube x Entity x Parent x Cons x Scenario x Year) in
  ``DataUnitCacheTimestamp`` when its data changes; changed data units are
  re-read in full and upserted. Checkpoints are kept per (year, scenario)
  slice -- see ``onestream_offsets.py`` for scope widening, confirmation
  re-reads (lookback) and in-flight hold-back. Reads are partitioned across
  executors and always seek on the clustered key (``PartitionId`` first).
* metadata tables (``members``, ``member_relationships``, ...) -- snapshot.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterator, Sequence

from pyspark.sql.types import StructField, StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.onestream.onestream_client import (
    DEFAULT_API_VERSION,
    DEFAULT_TIMEOUT_SECONDS,
    OneStreamClient,
    OneStreamError,
)
from databricks.labs.community_connector.sources.onestream.onestream_offsets import (
    ALL_ENTITIES,
    cube_apply_scope,
    cube_batch_ranges,
    cube_entity_csv,
    cube_entity_set,
    cube_new_entities,
    cube_offset_advance,
    cube_offset_decode,
    cube_offset_encode,
)
from databricks.labs.community_connector.sources.onestream.onestream_schemas import (
    CUBE_DATA,
    CUBE_DATA_CURSOR,
    CUBE_DATA_PRIMARY_KEYS,
    CUBE_DATA_SCHEMA,
    DATA_RECORD_PK,
    DATA_RECORD_SELECT,
    DATA_UNIT_COLUMNS,
    DIM_TYPE_ENTITY,
    DIM_TYPE_SCENARIO,
    DU_STAMP_COLUMN,
    INTERSECTION_COLUMNS,
    SNAPSHOT_TABLES,
    SUPPORTED_TABLES,
    SourceTable,
    datetime_to_ticks,
    spark_type,
    ticks_to_iso,
    time_id,
)
from databricks.labs.community_connector.sources.onestream.onestream_sql import (
    SQL_MAX_YEAR,
    SQL_MIN_YEAR,
    sql_conj,
    sql_data_record_table,
    sql_eq,
    sql_eq_name,
    sql_group_max,
    sql_gt,
    sql_in,
    sql_le,
    sql_pair_filter,
    sql_row_probe,
    sql_select,
    sql_table_ref,
)

DEFAULT_PAGE_SIZE = 50_000
MAX_PAGE_SIZE = 200_000
DEFAULT_MAX_PARTITIONS = 4
DEFAULT_MAX_DATA_UNITS_PER_BATCH = 5_000
DEFAULT_LOOKBACK_SECONDS = 900
DEFAULT_IN_FLIGHT_TIMEOUT_SECONDS = 3_600
TICKS_PER_SECOND = 10_000_000
# Above this many changed (Parent, Cons) pairs in one slice, re-read the whole
# slice instead of building a long OR filter.
MAX_DU_PAIRS_PER_SLICE = 100

DU_TIMESTAMP_TABLE = "DataUnitCacheTimestamp"
DU_TIMESTAMP_ORDER = [
    "PartitionId",
    "ScenarioId",
    "YearId",
    "CubeId",
    "EntityId",
    "ParentId",
    "ConsId",
]
DU_IN_FLIGHT_COLUMN = "IsInCellUpdateLoop"
CALC_STATUS_TABLE = "CalcStatus"
SLICE_COLUMNS = ["YearId", "ScenarioId", "CubeId"]
_TRUE = ("1", "true", "yes", "y")
_FALSE = ("0", "false", "no", "n", "")
REFRESH_MODES = ("incremental", "snapshot")


def _truthy(value) -> bool:
    return str(value).strip().lower() in _TRUE


def _bool_option(table_options: dict, key: str, default: bool) -> bool:
    """Parse a boolean option strictly, so a typo fails instead of meaning ``false``."""
    value = table_options.get(key)
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"Option '{key}' must be true or false, got {value!r}")


def _csv(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [v.strip() for v in str(value).split(",") if v.strip()]


def _parse_years(value) -> list[int]:
    years: set[int] = set()
    for token in _csv(value):
        if "-" in token:
            lo, hi = (int(x) for x in token.split("-", 1))
            years.update(range(min(lo, hi), max(lo, hi) + 1))
        else:
            years.add(int(token))
    for y in years:
        if not SQL_MIN_YEAR <= y <= SQL_MAX_YEAR:
            raise ValueError(f"cube_data option 'years' contains an out-of-range year: {y}")
    return sorted(years)


_LOG = logging.getLogger(__name__)

# Numeric table options and their allowed ranges (validated before any request).
_INT_OPTIONS = {
    "page_size": (1, MAX_PAGE_SIZE),
    "max_partitions": (1, 64),
    "max_data_units_per_batch": (1, 1_000_000),
}
_NUMBER_OPTIONS = {
    "lookback_seconds": (0, 7 * 86_400),
    "in_flight_timeout_seconds": (0, 7 * 86_400),
}


def _number_option(options: dict, key: str, default: float, lo: float, hi: float) -> float:
    value = options.get(key)
    if value is None or str(value).strip() == "":
        return float(default)
    try:
        number = float(str(value).strip())
    except ValueError:
        raise ValueError(f"Option '{key}' must be a number, got {value!r}") from None
    if not lo <= number <= hi:
        raise ValueError(f"Option '{key}' must be between {lo} and {hi}, got {value!r}")
    return number


def _int_option(options: dict, key: str, default: int, lo: int, hi: int) -> int:
    value = options.get(key)
    if value is None or str(value).strip() == "":
        return int(default)
    text = str(value).strip()
    if not text.isdigit():
        raise ValueError(f"Option '{key}' must be a whole number, got {value!r}")
    number = int(text)
    if not lo <= number <= hi:
        raise ValueError(f"Option '{key}' must be between {lo} and {hi}, got {value!r}")
    return number


def _validate_table_options(table_name: str, table_options: dict) -> None:
    """Fail fast on malformed numeric options, before any request reaches OneStream."""
    for key, (lo, hi) in _INT_OPTIONS.items():
        _int_option(table_options, key, lo, lo, hi)
    if table_name == CUBE_DATA:
        for key, (lo, hi) in _NUMBER_OPTIONS.items():
            _number_option(table_options, key, lo, lo, hi)


def _read_sequence(end_state: dict, key: tuple, catch_up: bool) -> int:
    """Merge sequence for rows of slice ``key`` read in the batch ending at ``end_state``.

    A batch reads a slice only when its cursor ``c`` or verified watermark ``w``
    grows (see ``cube_batch_ranges``), and neither ever decreases, so ``c + w``
    is strictly larger on every later read of the slice -- including a
    confirmation re-read that finds a cell changed under an unchanged
    data-unit stamp. Catch-up reads add 1 so they win over the slice's previous
    read, and any later read still exceeds them. Derived from the offset alone,
    so planning stays deterministic. 0 for batch/snapshot reads, where the
    sequence is unused.
    """
    if not end_state:
        return 0
    seq = end_state["c"].get(key, 0) + end_state["w"].get(key, 0)
    return seq + 1 if catch_up else seq


def _slice_key(row: dict) -> tuple:
    return (int(row["YearId"]), int(row["ScenarioId"]), int(row["CubeId"]))


class OneStreamLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for OneStream."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._client = OneStreamClient(
            base_url=options.get("base_url", ""),
            application_name=options.get("application_name", ""),
            token=options.get("personal_access_token", ""),
            api_version=options.get("api_version") or DEFAULT_API_VERSION,
            timeout_seconds=_number_option(
                options, "request_timeout_seconds", DEFAULT_TIMEOUT_SECONDS, 1, 3600
            ),
        )
        # Cap every incremental read at init time so Trigger.AvailableNow terminates.
        self._init_ticks = datetime_to_ticks(datetime.now(timezone.utc))
        # Per-instance caches. A new instance is created per trigger, so these
        # never outlive one run.
        self._partition_map: dict[int, dict[int, list[int]]] = {}
        self._scope_cache: dict[str, dict] = {}
        self._live_cols: dict[str, set[str]] = {}
        self._du_cache: dict[tuple, tuple[int, list[dict]]] = {}
        self._checked_years: set[tuple] = set()
        self._cap_checked = False
        self._row_cap: int | None = None

    # ------------------------------------------------------------------
    # LakeflowConnect
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        if table_name == CUBE_DATA:
            return CUBE_DATA_SCHEMA
        columns = self._snapshot_columns(SNAPSHOT_TABLES[table_name])
        return StructType([StructField(c[0], spark_type(c[1], c[2], c[3]), c[4]) for c in columns])

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        _validate_table_options(table_name, table_options)
        if table_name == CUBE_DATA:
            if self._cube_data_snapshot_mode(table_options):
                return {"primary_keys": list(CUBE_DATA_PRIMARY_KEYS), "ingestion_type": "snapshot"}
            return {
                "primary_keys": list(CUBE_DATA_PRIMARY_KEYS),
                "cursor_field": CUBE_DATA_CURSOR,
                "ingestion_type": "cdc",
            }
        table = SNAPSHOT_TABLES[table_name]
        return {"primary_keys": list(table.primary_keys), "ingestion_type": "snapshot"}

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        self._validate_table(table_name)
        if table_name != CUBE_DATA:
            return iter(self._read_snapshot_table(table_name, table_options)), None

        if self._cube_data_snapshot_mode(table_options):
            units = self._all_units(table_options)
            return self._read_units(units, table_options), None

        start = start_offset or {}
        end = self.latest_offset(table_name, table_options, start)
        if end == start:
            return iter([]), end
        units = self._units_between(table_options, start, end)
        return self._read_units(units, table_options), end

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        return table_name == CUBE_DATA

    def latest_offset(
        self, table_name: str, table_options: dict[str, str], start_offset: dict | None = None
    ) -> dict:
        """Next checkpoint: per-slice cursors advanced by at most ~max_data_units_per_batch."""
        self._validate_table(table_name)
        if table_name != CUBE_DATA:
            return {}
        state = cube_offset_decode(start_offset)
        scope = self._resolve_scope(table_options)
        cfg = self._cube_settings(table_options)
        now = self._init_ticks
        certify = now - cfg["lookback"]

        # Cheap per-slice high-water marks decide which slices are in scope,
        # which have work, and how far back the data-unit fetch must reach.
        stats = self._slice_max_ticks(scope)
        in_scope = set(stats)
        new_entities = cube_new_entities(
            state["e"], scope["entity_csv"], lambda: self._all_entity_ids(scope["page_size"])
        )
        target = cube_apply_scope(state, scope["entity_csv"], new_entities, in_scope)

        rows = self._slice_rows(scope, state, stats, certify)
        catch_rows = self._catch_up_rows(scope, target)
        nxt = cube_offset_advance(
            target,
            rows,
            catch_rows,
            now,
            cfg["lookback"],
            cfg["max_units"],
            cfg["hold"],
            cfg["hold_timeout"],
            in_scope,
        )
        return cube_offset_encode(nxt)

    def _slice_rows(self, scope: dict, state: dict, stats: dict, certify: int) -> dict:
        """``{slice: sorted [(ticks, in_flight)]}`` for the slices that have work.

        A slice has work if it is new, has stamps above its cursor, is due a
        confirmation re-read, or is held back; the fetch reaches back to the
        lowest ``w`` among those slices.
        """
        bounds = []
        for key, max_ticks in stats.items():
            c0, w0 = state["c"].get(key), state["w"].get(key, 0)
            if c0 is None:
                bounds.append(0)
            elif max_ticks > c0 or w0 < min(c0, certify) or key in state["h"]:
                bounds.append(w0)
        rows: dict = defaultdict(list)
        if bounds:
            for r in self._fetch_du_rows(scope, min(bounds), self._init_ticks, scope["entity_ids"]):
                rows[_slice_key(r)].append(
                    (int(r["TimestampTicks"]), _truthy(r.get(DU_IN_FLIGHT_COLUMN)))
                )
        return {k: sorted(v) for k, v in rows.items()}

    def _catch_up_rows(self, scope: dict, target: dict) -> dict:
        """``{slice: sorted [ticks]}`` for entities being caught up after joining the scope."""
        if not target.get("x"):
            return {}
        lo = min(target["xp"].values(), default=0)
        hi = max(target["xm"].values(), default=0)
        rows: dict = defaultdict(list)
        for r in self._fetch_du_rows(scope, lo, hi, sorted(cube_entity_set(target["x"]))):
            rows[_slice_key(r)].append(int(r["TimestampTicks"]))
        return {k: sorted(v) for k, v in rows.items()}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        self._validate_table(table_name)
        if table_name != CUBE_DATA:
            return [{"table": table_name}]
        if start_offset is None and end_offset is None:
            units = self._all_units(table_options)
        else:
            units = self._units_between(table_options, start_offset or {}, end_offset or {})
        if not units:
            return []
        n = _int_option(table_options, "max_partitions", DEFAULT_MAX_PARTITIONS, 1, 64)
        buckets: list[list] = [[] for _ in range(min(n, len(units)))]
        for i, unit in enumerate(units):
            buckets[i % len(buckets)].append(unit)
        return [{"units": json.dumps(b, separators=(",", ":"))} for b in buckets]

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        self._validate_table(table_name)
        if table_name != CUBE_DATA:
            return iter(self._read_snapshot_table(table_name, table_options))
        return self._read_units(json.loads(partition["units"]), table_options)

    # ------------------------------------------------------------------
    # Snapshot tables
    # ------------------------------------------------------------------

    def _live_columns(self, table: str) -> set[str]:
        """Columns that exist in this application (OneStream versions differ)."""
        if table not in self._live_cols:
            where = sql_conj(sql_eq_name("TABLE_SCHEMA", "dbo"), sql_eq_name("TABLE_NAME", table))
            rows = self._unpaged(
                sql_select(["COLUMN_NAME"], "[INFORMATION_SCHEMA].[COLUMNS]", where)
            )
            self._live_cols[table] = {str(r["COLUMN_NAME"]) for r in rows}
        return self._live_cols[table]

    def _snapshot_columns(self, table: SourceTable) -> list[tuple]:
        live = self._live_columns(table.source_table)
        if not live:
            raise ValueError(
                f"Table dbo.{table.source_table} was not found in the OneStream application"
            )
        missing_keys = [k for k in table.primary_keys if k not in live]
        if missing_keys:
            raise ValueError(
                f"dbo.{table.source_table} is missing key column(s) {missing_keys}; "
                "this OneStream version is not supported for this table"
            )
        return [c for c in table.columns if c[0] in live]

    def _read_snapshot_table(self, table_name: str, table_options: dict[str, str]) -> list[dict]:
        table = SNAPSHOT_TABLES[table_name]
        columns = [c[0] for c in self._snapshot_columns(table)]
        return self._paged(
            columns,
            sql_table_ref(table.source_table),
            "1 = 1",
            table.primary_keys,
            self._page_size_for(table_options),
        )

    def _server_row_cap(self) -> int | None:
        """Rows the server returns per request at most (``None`` = no cap up to the max page).

        Measured once per instance with a one-column probe. Paging stops at the
        first short page and the small planning queries aren't paged, so a silent
        server-side cap would otherwise truncate reads without an error.
        """
        if not self._cap_checked:
            probe = MAX_PAGE_SIZE + 1
            returned = len(self._client.run_sql(sql_row_probe(probe)))
            self._row_cap = returned if returned < probe else None
            self._cap_checked = True
        return self._row_cap

    def _ensure_page_size(self, page_size: int) -> None:
        cap = self._server_row_cap()
        if cap is not None and page_size > cap:
            raise OneStreamError(
                f"OneStream returned only {cap} rows for a request; "
                f"set the page_size option to {cap} or less"
            )

    def _unpaged(self, sql: str) -> list[dict]:
        """Run a planning query that isn't paged; fail if a server cap could have cut it short."""
        rows = self._client.run_sql(sql)
        cap = self._server_row_cap()
        if cap is not None and len(rows) >= cap:
            raise OneStreamError(
                f"A planning query returned {len(rows)} rows, which reaches the server's "
                f"limit of {cap} rows per request, so it may be incomplete. Narrow the scope "
                "(years, scenarios, entities) or raise the server limit."
            )
        return rows

    def _paged(self, columns, source, where, order_by, page_size) -> list[dict]:
        self._ensure_page_size(page_size)
        rows: list[dict] = []
        offset = 0
        while True:
            page = self._client.run_sql(
                sql_select(
                    columns, source, where, order_by=order_by, offset=offset, limit=page_size
                )
            )
            rows.extend(page)
            if len(page) < page_size:
                return rows
            offset += page_size

    # ------------------------------------------------------------------
    # cube_data planning (driver)
    # ------------------------------------------------------------------

    def _cube_data_snapshot_mode(self, table_options: dict[str, str]) -> bool:
        mode = str(table_options.get("refresh_mode", "incremental")).strip().lower()
        if mode not in REFRESH_MODES:
            raise ValueError(f"Option 'refresh_mode' must be one of {REFRESH_MODES}, got {mode!r}")
        return mode == "snapshot"

    def _resolve_scope(self, table_options: dict[str, str]) -> dict:
        """Resolve user-facing names to integer ids (cached per option set)."""
        key = json.dumps(
            {
                k: table_options.get(k)
                for k in (
                    "years",
                    "scenarios",
                    "entities",
                    "include_descendants",
                    "cube",
                    "page_size",
                )
            },
            sort_keys=True,
        )
        if key in self._scope_cache:
            return self._scope_cache[key]

        years = _parse_years(table_options.get("years"))
        if not years:
            raise ValueError(
                "cube_data requires the table option 'years' (e.g. '2025,2026' or '2024-2026'). "
                "Unscoped backfills of a OneStream application can take many hours."
            )
        # One page size for every query this table makes, so lowering page_size
        # (e.g. for a server that caps rows per request) applies everywhere.
        page_size = self._page_size_for(table_options)
        scenario_ids = self._member_ids(
            DIM_TYPE_SCENARIO, _csv(table_options.get("scenarios")), "scenarios", page_size
        )
        entity_ids = self._member_ids(
            DIM_TYPE_ENTITY, _csv(table_options.get("entities")), "entities", page_size
        )
        if entity_ids is not None and _bool_option(table_options, "include_descendants", False):
            entity_ids = self._with_descendants(DIM_TYPE_ENTITY, entity_ids, page_size)
        cube_id = None
        cube_name = (table_options.get("cube") or "").strip()
        if cube_name:
            cubes = {
                r["Name"]: int(r["CubeId"])
                for r in self._unpaged(sql_select(["CubeId", "Name"], sql_table_ref("Cube")))
            }
            if cube_name not in cubes:
                raise ValueError(f"Unknown cube '{cube_name}'. Available: {sorted(cubes)}")
            cube_id = cubes[cube_name]
        scope = {
            "key": key,
            "years": years,
            "scenario_ids": scenario_ids,
            "entity_ids": entity_ids,
            "entity_csv": ALL_ENTITIES if entity_ids is None else cube_entity_csv(entity_ids),
            "cube_id": cube_id,
            "page_size": page_size,
        }
        self._verify_time_profile(scope)
        self._scope_cache[key] = scope
        return scope

    def _verify_time_profile(self, scope: dict) -> None:
        """Fail loudly unless the application uses the standard monthly time profile.

        ``TimeId``/``Period`` are derived from the standard OneStream encoding
        (Year * 1e6 + period index * 1e3, months at indices 3-5, 7-9, 12-14, 16-18).
        ``CalcStatus`` records the period TimeIds that actually hold calculated
        data, so any other period key means a different time profile.
        """
        for year in scope["years"]:
            check = (year, scope["cube_id"])
            if check in self._checked_years:
                continue
            base = year * 1_000_000
            where = sql_conj(
                sql_gt("TimeId", base - 1),
                sql_le("TimeId", base + 999_999),
                sql_eq("CubeId", scope["cube_id"]) if scope["cube_id"] is not None else None,
            )
            seen = {
                int(r["TimeId"])
                for r in self._unpaged(
                    sql_select(["TimeId"], sql_table_ref(CALC_STATUS_TABLE), where, distinct=True)
                )
            }
            if not seen:
                _LOG.warning(
                    "Cannot verify the time profile for %s: CalcStatus has no period keys "
                    "for that year (never calculated). Assuming the standard monthly profile.",
                    year,
                )
            unexpected = sorted(seen - {time_id(year, m) for m in range(1, 13)})
            if unexpected:
                raise ValueError(
                    f"OneStream application uses period keys {unexpected[:6]} in {year} "
                    "that are not "
                    "standard monthly periods. cube_data currently supports the standard monthly "
                    "time profile only."
                )
            self._checked_years.add(check)

    def _member_ids(
        self, dim_type: int, names: list[str], option: str, page_size: int
    ) -> list[int] | None:
        if not names:
            return None
        rows = self._paged(
            ["MemberId", "Name"],
            sql_table_ref("Member"),
            sql_eq("DimTypeId", dim_type),
            ["MemberId"],
            page_size,
        )
        by_name: dict[str, set[int]] = defaultdict(set)
        for r in rows:
            by_name[str(r["Name"]).lower()].add(int(r["MemberId"]))
        missing = [n for n in names if n.lower() not in by_name]
        if missing:
            raise ValueError(f"cube_data option '{option}': unknown member name(s) {missing}")
        ambiguous = [n for n in names if len(by_name[n.lower()]) > 1]
        if ambiguous:
            raise ValueError(
                f"cube_data option '{option}': member name(s) {ambiguous} exist in more than one "
                "dimension of this type; rename them or narrow the scope"
            )
        return sorted({next(iter(by_name[n.lower()])) for n in names})

    def _with_descendants(self, dim_type: int, roots: list[int], page_size: int) -> list[int]:
        rows = self._paged(
            ["ParentId", "ChildId"],
            sql_table_ref("Relationship"),
            sql_eq("DimTypeId", dim_type),
            ["ParentId", "ChildId"],
            page_size,
        )
        children: dict[int, list[int]] = defaultdict(list)
        for r in rows:
            children[int(r["ParentId"])].append(int(r["ChildId"]))
        seen, stack = set(roots), list(roots)
        while stack:
            for child in children.get(stack.pop(), []):
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return sorted(seen)

    def _cube_settings(self, table_options: dict[str, str]) -> dict:
        lookback = _number_option(
            table_options, "lookback_seconds", DEFAULT_LOOKBACK_SECONDS, 0, 7 * 86_400
        )
        timeout = _number_option(
            table_options,
            "in_flight_timeout_seconds",
            DEFAULT_IN_FLIGHT_TIMEOUT_SECONDS,
            0,
            7 * 86_400,
        )
        hold = _bool_option(table_options, "hold_in_flight_data_units", False)
        if hold and DU_IN_FLIGHT_COLUMN not in self._live_columns(DU_TIMESTAMP_TABLE):
            raise ValueError(
                f"hold_in_flight_data_units requires {DU_TIMESTAMP_TABLE}.{DU_IN_FLIGHT_COLUMN}, "
                "which this OneStream version does not have"
            )
        return {
            "lookback": int(lookback * TICKS_PER_SECOND),
            "max_units": _int_option(
                table_options,
                "max_data_units_per_batch",
                DEFAULT_MAX_DATA_UNITS_PER_BATCH,
                1,
                1_000_000,
            ),
            "hold": hold,
            "hold_timeout": int(timeout * TICKS_PER_SECOND),
        }

    def _all_entity_ids(self, page_size: int) -> list[int]:
        rows = self._paged(
            ["MemberId"],
            sql_table_ref("Member"),
            sql_eq("DimTypeId", DIM_TYPE_ENTITY),
            ["MemberId"],
            page_size,
        )
        return [int(r["MemberId"]) for r in rows]

    @staticmethod
    def _scope_where(scope: dict, entity_ids) -> str:
        return sql_conj(
            sql_in("YearId", scope["years"]),
            sql_in("ScenarioId", scope["scenario_ids"]),
            sql_in("EntityId", entity_ids),
            sql_eq("CubeId", scope["cube_id"]) if scope["cube_id"] is not None else None,
        )

    def _slice_max_ticks(self, scope: dict) -> dict:
        where = sql_conj(
            self._scope_where(scope, scope["entity_ids"]),
            sql_le("TimestampTicks", self._init_ticks),
        )
        rows = self._unpaged(
            sql_group_max(SLICE_COLUMNS, "TimestampTicks", sql_table_ref(DU_TIMESTAMP_TABLE), where)
        )
        return {_slice_key(r): int(r["max_value"]) for r in rows}

    def _fetch_du_rows(self, scope: dict, lo: int, hi: int, entity_ids) -> list[dict]:
        """Data-unit stamps in scope with ``lo < TimestampTicks <= hi``.

        Cached for the life of the instance (one run): micro-batches of the same
        run reuse the widest fetch instead of re-reading the stamp table. Stamps
        written after the cache was filled are picked up by the next run.
        """
        cache_key = (scope["key"], tuple(entity_ids) if entity_ids is not None else None, int(hi))
        cached = self._du_cache.get(cache_key)
        if cached is not None and cached[0] <= lo:
            return [r for r in cached[1] if int(r["TimestampTicks"]) > lo]
        columns = [*DATA_UNIT_COLUMNS, "TimestampTicks"]
        if DU_IN_FLIGHT_COLUMN in self._live_columns(DU_TIMESTAMP_TABLE):
            columns.append(DU_IN_FLIGHT_COLUMN)
        where = sql_conj(
            self._scope_where(scope, entity_ids),
            sql_gt("TimestampTicks", int(lo)),
            sql_le("TimestampTicks", int(hi)),
        )
        rows = self._paged(
            columns,
            sql_table_ref(DU_TIMESTAMP_TABLE),
            where,
            DU_TIMESTAMP_ORDER,
            scope["page_size"],
        )
        self._du_cache[cache_key] = (int(lo), rows)
        return rows

    def _all_units(self, table_options: dict[str, str]) -> list:
        """Every data unit in scope (batch reads and refresh_mode=snapshot)."""
        scope = self._resolve_scope(table_options)
        rows = self._fetch_du_rows(scope, 0, self._init_ticks, scope["entity_ids"])
        return self._units_from_rows(rows, {})

    def _units_between(self, table_options: dict[str, str], start: dict, end: dict) -> list:
        """Read units for the micro-batch ``start -> end`` (derived from the offsets alone)."""
        if start == end:
            return []
        s_state, e_state = cube_offset_decode(start), cube_offset_decode(end)
        main, catch, catch_entities = cube_batch_ranges(s_state, e_state)
        scope = self._resolve_scope(table_options)
        cfg = self._cube_settings(table_options)
        selected: list[dict] = []
        if main:
            entity_ids = (
                None
                if e_state["e"] in (None, ALL_ENTITIES)
                else sorted(cube_entity_set(e_state["e"]))
            )
            selected += self._rows_in_ranges(scope, main, entity_ids, e_state, cfg)
        if catch:
            catch_rows = self._rows_in_ranges(
                scope, catch, sorted(cube_entity_set(catch_entities)), e_state, cfg
            )
            selected += [dict(r, _catch_up=True) for r in catch_rows]
        return self._units_from_rows(selected, e_state)

    def _rows_in_ranges(
        self, scope: dict, ranges: list, entity_ids, e_state: dict, cfg: dict
    ) -> list[dict]:
        by_slice = {key: (lo, hi) for key, lo, hi in ranges}
        lo = min(r[1] for r in ranges)
        hi = max(r[2] for r in ranges)
        out = []
        for r in self._fetch_du_rows(scope, lo, hi, entity_ids):
            key = _slice_key(r)
            window = by_slice.get(key)
            ticks = int(r["TimestampTicks"])
            if window is None or not window[0] < ticks <= window[1]:
                continue
            held_since = e_state["h"].get(key)
            if (
                cfg["hold"]
                and held_since is not None
                and _truthy(r.get(DU_IN_FLIGHT_COLUMN))
                and self._init_ticks - held_since <= cfg["hold_timeout"]
            ):
                continue  # in flight: the slice cursor is held below it
            out.append(r)
        return out

    def _units_from_rows(self, rows: list[dict], end_state: dict) -> list:
        """Group data-unit rows into read units.

        A read unit is ``[year, partition_id, cube_id, entity_id, scenario_id, pairs, seq]``
        where ``pairs`` is ``[[parent_id, cons_id, ticks], ...]`` (or ``None`` to read
        every data unit of that slice when there are very many) and ``seq`` is the
        merge sequence for the rows read (see ``_read_sequence``). An entity stored
        in several partitions yields one unit per partition.
        """
        groups: dict[tuple, dict] = defaultdict(dict)
        seqs: dict[tuple, int] = {}
        for r in rows:
            key = (int(r["YearId"]), int(r["CubeId"]), int(r["EntityId"]), int(r["ScenarioId"]))
            pair = (int(r["ParentId"]), int(r["ConsId"]))
            groups[key][pair] = max(groups[key].get(pair, 0), int(r["TimestampTicks"]))
            seq = _read_sequence(end_state, _slice_key(r), bool(r.get("_catch_up")))
            seqs[key] = max(seqs.get(key, 0), seq)
        units = []
        for (year, cube_id, entity_id, scenario_id), pairs in sorted(groups.items()):
            pair_list = (
                None
                if len(pairs) > MAX_DU_PAIRS_PER_SLICE
                else [[p, c, t] for (p, c), t in sorted(pairs.items())]
            )
            # No partition means the data unit is stamped but holds no stored rows.
            for partition_id in self._partitions_for(year, entity_id):
                units.append(
                    [
                        year,
                        partition_id,
                        cube_id,
                        entity_id,
                        scenario_id,
                        pair_list,
                        seqs[(year, cube_id, entity_id, scenario_id)],
                    ]
                )
        return units

    def _partitions_for(self, year: int, entity_id: int) -> list[int]:
        if year not in self._partition_map:
            # One scan per year table (~60 s on ~50M rows); the result is small
            # (about one row per entity), so no paging is needed.
            rows = self._unpaged(
                sql_select(["PartitionId", "EntityId"], sql_data_record_table(year), distinct=True)
            )
            mapping: dict[int, list[int]] = defaultdict(list)
            for r in rows:
                mapping[int(r["EntityId"])].append(int(r["PartitionId"]))
            self._partition_map[year] = {e: sorted(p) for e, p in mapping.items()}
        return self._partition_map[year].get(entity_id, [])

    # ------------------------------------------------------------------
    # cube_data reading (executors)
    # ------------------------------------------------------------------

    def _read_units(self, units: list, table_options: dict[str, str]) -> Iterator[dict]:
        page_size = self._page_size_for(table_options)
        if units:
            self._ensure_page_size(page_size)
        for year, partition_id, cube_id, entity_id, scenario_id, pairs, seq in units:
            yield from self._read_unit(
                year, partition_id, cube_id, entity_id, scenario_id, pairs, page_size, seq
            )

    def _read_unit(
        self, year, partition_id, cube_id, entity_id, scenario_id, pairs, page_size, seq
    ):
        if pairs:
            stamps = {(int(p), int(c)): int(t) for p, c, t in pairs}
        else:
            stamp_where = sql_conj(
                sql_eq("YearId", year),
                sql_eq("CubeId", cube_id),
                sql_eq("EntityId", entity_id),
                sql_eq("ScenarioId", scenario_id),
            )
            stamps = {
                (int(r["ParentId"]), int(r["ConsId"])): int(r["TimestampTicks"])
                for r in self._unpaged(
                    sql_select(
                        ["ParentId", "ConsId", "TimestampTicks"],
                        sql_table_ref(DU_TIMESTAMP_TABLE),
                        stamp_where,
                    )
                )
            }
        where = sql_conj(
            sql_eq("PartitionId", partition_id),
            sql_eq("CubeId", cube_id),
            sql_eq("EntityId", entity_id),
            sql_eq("ScenarioId", scenario_id),
            sql_eq("YearId", year),
            sql_pair_filter("ParentId", "ConsId", [(p, c) for p, c, _ in pairs]) if pairs else None,
        )
        source = sql_data_record_table(year)
        offset = 0
        while True:
            page = self._client.run_sql(
                sql_select(
                    DATA_RECORD_SELECT,
                    source,
                    where,
                    order_by=DATA_RECORD_PK,
                    offset=offset,
                    limit=page_size,
                )
            )
            for row in page:
                ticks = stamps.get((int(row["ParentId"]), int(row["ConsId"])), 0)
                yield from _unpivot(row, year, ticks, seq)
            if len(page) < page_size:
                return
            offset += page_size

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _page_size_for(table_options: dict[str, str]) -> int:
        return _int_option(table_options, "page_size", DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)

    @staticmethod
    def _validate_table(table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table '{table_name}'. Supported tables: {SUPPORTED_TABLES}"
            )


def _unpivot(row: dict, year: int, ticks: int, seq: int) -> Iterator[dict]:
    """One DataRecord row -> 12 monthly records (zeros included, so cleared cells overwrite)."""
    base = {c: row[c] for c in DATA_UNIT_COLUMNS}
    base.update({c: row[c] for c in INTERSECTION_COLUMNS})
    base[DU_STAMP_COLUMN] = ticks
    base[CUBE_DATA_CURSOR] = seq
    base["_du_modified_at"] = ticks_to_iso(ticks) if ticks else None
    for month in range(1, 13):
        rec = dict(base)
        rec["TimeId"] = time_id(year, month)
        rec["Period"] = month
        rec["Amount"] = row[f"M{month}Value"]
        rec["CellStatus"] = row[f"M{month}Status"]
        yield rec
