"""Lakeflow Community Connector for the Apis REST Service (Hive historian).

The source is a Hive-instance-scoped REST API in front of an industrial data
historian: instances host pluggable modules, modules expose items
(points/tags), items have a current value and — when configured for logging —
recorded history.

Three tables are ingested:

===============  ===============  =========================================
Table            Ingestion type   Endpoint
===============  ===============  =========================================
``items``        ``snapshot``     ``GET /hive/{instance}/items``
``values``       ``cdc``          ``GET /hive/{instance}/values``
``timeseries``   ``append``       ``GET /hive/{instance}/timeseries``
===============  ===============  =========================================

``GET /hive`` is **not** a table. It is an upstream lookup that resolves the
``{instance}`` path segment every other call needs; it runs once on the
driver (and only when the caller has not pinned ``instance`` in the
connection or table options).

Why this connector is partitioned
---------------------------------
``timeseries`` takes required ``starttime``/``endtime`` parameters, i.e. the
source supports genuine range queries. That is the signal for
``SupportsPartitionedStream``: instead of walking one sliding window at a
time on the driver, ``get_partitions`` splits the requested range into
non-overlapping windows and Spark reads them in parallel on executors. The
same mechanism fans ``items`` and ``values`` out across
``(instance x item pattern)``, which is also the only truncation mitigation
the source offers (see below).

``read_table`` is still implemented in full: the framework falls back to it
for batch reads when ``get_partitions`` raises, and it is the path used by
``simpleStreamReader`` should ``is_partitioned`` ever be narrowed.

Source limitations that shape this implementation
-------------------------------------------------
* **No pagination.** The spec defines no ``limit``/``offset``/``cursor``/
  ``nextPageToken`` parameter and no ``Link`` header on any operation, yet
  ``items``, ``values`` and ``timeseries`` all declare ``206 Partial
  Content`` — so the server *can* truncate and there is no documented way to
  resume. This connector does not invent a cursor parameter. It issues one
  request per (instance, item pattern, window) tuple, narrows scope via the
  ``item`` wildcard and the window size, and logs a warning whenever a
  ``206`` comes back so the truncation is at least visible.
* **Most response bodies have no declared schema.** Only ``values`` does.
  See ``apis_schemas.py`` for exactly which fields are inferred, and
  ``_normalize_*`` below for parsers written to tolerate more than one
  plausible wire shape.
* **No documented rate limits.** Retries are exponential and conservative.
* **No delete feed**, so no table can be ``cdc_with_deletes``.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.apis.apis_schemas import (
    AGGREGATE_VALUES,
    DEFAULT_AUTH_SCHEME,
    DEFAULT_BACKFILL_DAYS,
    DEFAULT_ITEM_PATTERN,
    DEFAULT_ITEMS_PER_REQUEST,
    DEFAULT_MAX_PARTITIONS,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MAX_WINDOWS_PER_READ,
    DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_WINDOW_SECONDS,
    EPOCH_ISO,
    INITIAL_BACKOFF_SECONDS,
    MAX_INTERVAL_SECONDS,
    MIN_INTERVAL_SECONDS,
    QUALITY_VALUES,
    RESPONSE_FORMAT,
    RETRIABLE_STATUS_CODES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
    TIMESTAMP_FORMAT,
    WIRE_TIMESTAMP_FORMAT,
)

logger = logging.getLogger(__name__)

_INSTANCE_NAME_KEYS = ("name", "instance", "instance_name", "hive", "hive_instance", "id")
_ITEM_NAME_KEYS = ("item_name", "name", "item", "itemname", "tag")
_POINT_KEYS = ("v", "q", "t")


class ApisLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for the Apis REST Service.

    Connection options
    ------------------
    base_url
        Root URL of the Apis REST Service, e.g. ``http://hive-host:8080``.
        Required. The spec's ``http://localhost:8080`` server entry is a
        dev placeholder and is never assumed here.
    token
        Pre-issued bearer token sent in the ``Authorization`` header.
        Required. Aliases: ``api_token``, ``access_token``, ``bearer_token``.
        The spec defines no OAuth2 flow, no token endpoint and no scopes, so
        the credential is treated as static.
    auth_scheme
        Prefix for the ``Authorization`` header value. Defaults to
        ``Bearer``. Set to an empty string (or ``none``/``raw``) to send the
        bare token — the spec models the header as a raw ``apiKey`` and does
        not state which form the server wants.
    instance / instances
        Optional. Comma-separated Hive instance name(s). When omitted the
        connector resolves them once via ``GET /hive``.
    request_timeout
        Per-request timeout in seconds (default 30).
    max_retries
        Attempts per request before giving up (default 4).
    verify_ssl
        ``false`` to skip TLS verification — on-prem Hive deployments often
        use a self-signed certificate.

    Table options (per table; never put these in the connection options)
    --------------------------------------------------------------------
    instance / instances
        Overrides the connection-level instance selection for one table.
    item / items / item_patterns
        Comma-separated ``item`` wildcard patterns, e.g.
        ``Work*.Sig*,Logger.*``. Defaults to ``*``. Each pattern becomes its
        own partition, which is also how response truncation (``206``) is
        kept at bay, since the source offers no continuation mechanism.
    attrib
        ``items`` only. Comma-separated numeric attribute IDs to request.
    quality
        ``values``/``timeseries`` only. One of ``good``/``uncertain``/``bad``
        — a *minimum* quality filter.
    aggregate, interval
        ``timeseries`` only. Server-side aggregation function and resample
        interval in seconds (1..86400).
    start_timestamp
        ``timeseries`` only. ISO-8601 lower bound for the first run. Strongly
        recommended: without it the first run reaches back
        ``backfill_days``.
    backfill_days
        ``timeseries`` only. First-run lookback when ``start_timestamp`` is
        absent (default 1095).
    window_seconds
        ``timeseries`` only. Partition width in seconds (default 86400).
        Start small when testing.
    max_partitions
        Upper bound on partitions per micro-batch (default 64). When the
        range needs more windows than this, windows are widened rather than
        emitting thousands of tasks.
    max_records_per_batch
        Admission control for the single-driver ``read_table`` path
        (default 1000).
    max_windows_per_read
        ``timeseries`` only. Windows one ``read_table`` call may walk before
        returning a micro-batch (default 64).
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)

        base_url = options.get("base_url") or options.get("host") or options.get("url")
        if not base_url:
            raise ValueError(
                "Apis connector requires connection option 'base_url', e.g. "
                "'http://hive-host:8080'"
            )
        self._base_url = str(base_url).rstrip("/")

        token = (
            options.get("token")
            or options.get("api_token")
            or options.get("access_token")
            or options.get("bearer_token")
        )
        if not token:
            raise ValueError(
                "Apis connector requires connection option 'token' (aliases: "
                "'api_token', 'access_token', 'bearer_token') — a pre-issued "
                "bearer token for the Authorization header"
            )
        self._token = str(token)

        scheme = options.get("auth_scheme", DEFAULT_AUTH_SCHEME)
        if scheme is None or str(scheme).strip().lower() in ("", "none", "raw"):
            self._auth_scheme = ""
        else:
            self._auth_scheme = str(scheme).strip()

        self._timeout = _parse_int(
            options.get("request_timeout"), DEFAULT_REQUEST_TIMEOUT, minimum=1
        )
        self._max_retries = _parse_int(
            options.get("max_retries"), DEFAULT_MAX_RETRIES, minimum=1
        )
        self._verify_ssl = str(options.get("verify_ssl", "true")).strip().lower() not in (
            "false",
            "0",
            "no",
        )

        #: Instances pinned at the connection level, if any.
        self._configured_instances = _split_csv(
            options.get("instance") or options.get("instances")
        )
        #: Cache for ``GET /hive`` discovery. Driver-side only; partition
        #: descriptors carry the resolved instance so executors never call
        #: ``/hive``.
        self._discovered_instances: list[str] | None = None

        # Cap every offset this connector returns at construction time.
        # Trigger.AvailableNow stops only when latest_offset repeats itself,
        # so a cursor that chases a live historian's incoming data would
        # never let a trigger finish. The next trigger builds a fresh
        # instance with a newer _init_time and picks up the remainder.
        self._init_time = _format_iso(datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # Session — created lazily and never pickled to executors
    # ------------------------------------------------------------------

    @property
    def _session(self) -> requests.Session:
        session = getattr(self, "_session_obj", None)
        if session is None:
            session = requests.Session()
            self._session_obj = session
        return session

    def __getstate__(self) -> dict:
        # Spark ships this object to executors; a live Session (sockets,
        # connection pool) must not travel with it. read_partition rebuilds
        # one lazily on the worker from self.options-derived state.
        state = dict(self.__dict__)
        state.pop("_session_obj", None)
        return state

    # ------------------------------------------------------------------
    # LakeflowConnect — discovery
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Static list — the source has no schema/discovery meta-API.

        ``/hive`` only enumerates instances (a path parameter), not tables,
        and ``runstate``/``modules`` are instance-level lookups rather than
        row-producing objects.
        """
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # SupportsPartitionedStream
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """All three tables fan out cleanly, so all three are partitioned.

        ``items``/``values`` partition on ``(instance x item pattern)``;
        ``timeseries`` adds the time window as a third dimension.
        """
        return table_name in SUPPORTED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return the high-water mark, capped at construction time.

        Metadata-only: no records are fetched here. The source exposes no
        "max timestamp" endpoint at all — there is nothing cheap to query —
        so the cap *is* the high-water mark. Returning the constant
        ``_init_time`` makes ``Trigger.AvailableNow`` converge on the second
        call for every table, which is the required termination condition.
        """
        self._validate_table(table_name)
        return {"cursor": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Split the read into self-contained descriptors.

        Batch (both offsets ``None``) covers the whole table. Streaming
        covers ``(start_offset, end_offset]`` and returns ``[]`` once the two
        are equal, which is how the micro-batch loop stops.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        # Streaming steady state: nothing new since the last commit.
        if (
            start_offset is not None
            and end_offset is not None
            and start_offset == end_offset
        ):
            return []

        instances = self._resolve_instances(table_options)
        patterns = _resolve_item_patterns(table_options)

        if table_name == "items":
            # Snapshot: every partition is a full re-list of one
            # (instance, pattern) slice. No cursor is involved.
            return [
                {"instance": inst, "item": pattern}
                for inst in instances
                for pattern in patterns
            ]

        if table_name == "values":
            # CDC: updatedSince is the only incremental knob the endpoint
            # has — there is no matching "updatedUntil", so a partition is
            # simply "current values for this slice, changed since X".
            updated_since = (start_offset or {}).get("cursor")
            partitions: list[dict] = []
            for inst in instances:
                for pattern in patterns:
                    descriptor = {"instance": inst, "item": pattern}
                    if updated_since:
                        descriptor["updated_since"] = updated_since
                    partitions.append(descriptor)
            return partitions

        # timeseries — the range-query table, and the reason this connector
        # is partitioned at all.
        start_iso, end_iso = self._resolve_timeseries_range(
            table_options, start_offset, end_offset
        )
        if start_iso >= end_iso:
            return []

        slices = max(1, len(instances) * len(patterns))
        windows = self._build_windows(start_iso, end_iso, table_options, slices)
        return [
            {
                "instance": inst,
                "item": pattern,
                "starttime": window_start,
                "endtime": window_end,
            }
            for inst in instances
            for pattern in patterns
            for window_start, window_end in windows
        ]

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        """Read one partition. Runs on an executor; must be self-contained.

        Everything needed is in ``partition`` plus ``self.options`` — in
        particular the ``{instance}`` path segment is baked into the
        descriptor so no worker ever calls ``GET /hive``.
        """
        self._validate_table(table_name)
        table_options = table_options or {}
        instance = partition["instance"]
        pattern = partition.get("item", DEFAULT_ITEM_PATTERN)

        if table_name == "items":
            yield from self._fetch_items(instance, [pattern], table_options)
        elif table_name == "values":
            yield from self._fetch_values(
                instance, [pattern], table_options, partition.get("updated_since")
            )
        else:
            yield from self._fetch_timeseries(
                instance,
                [pattern],
                table_options,
                partition["starttime"],
                partition["endtime"],
            )

    # ------------------------------------------------------------------
    # LakeflowConnect.read_table — single-driver fallback
    # ------------------------------------------------------------------

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Sequential driver-side read.

        Used when the framework falls back from partitioned batch reads, and
        by ``simpleStreamReader`` for any table ``is_partitioned`` excludes.
        """
        self._validate_table(table_name)
        table_options = table_options or {}

        if table_name == "items":
            return self._read_items_snapshot(table_options)
        if table_name == "values":
            return self._read_values_incremental(start_offset, table_options)
        return self._read_timeseries_windowed(start_offset, table_options)

    def _read_items_snapshot(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full re-list of the item catalog. Snapshot tables carry no offset."""
        records: list[dict] = []
        patterns = _resolve_item_patterns(table_options)
        for instance in self._resolve_instances(table_options):
            records.extend(self._fetch_items(instance, patterns, table_options))
        return iter(records), {}

    def _read_values_incremental(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Current values changed since the checkpoint.

        The offset is the init-time cap rather than the maximum observed
        ``timestamp``: the endpoint returns one row per item with no ordering
        guarantee and no upper time bound, so there is no reliable
        record-derived cursor to advance to. Re-reading a boundary row on the
        next trigger is harmless because ``values`` upserts on
        ``(instance, item_name)``.
        """
        since = (start_offset or {}).get("cursor")
        if since and since >= self._init_time:
            # Already caught up to this trigger's cap — converge without
            # touching the API.
            return iter([]), start_offset

        max_records = _parse_int(
            table_options.get("max_records_per_batch"),
            DEFAULT_MAX_RECORDS_PER_BATCH,
            minimum=1,
        )
        patterns = _resolve_item_patterns(table_options)

        records: list[dict] = []
        for instance in self._resolve_instances(table_options):
            if len(records) >= max_records:
                break
            for record in self._fetch_values(instance, patterns, table_options, since):
                records.append(record)
                # Client-side truncation is safe here: the table is CDC with
                # a primary key, so anything cut mid-flight is re-delivered
                # next trigger and merged.
                if len(records) >= max_records:
                    break

        return iter(records), {"cursor": self._init_time}

    def _read_timeseries_windowed(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding ``[starttime, endtime)`` walk, one micro-batch per call.

        The cursor advances to the *window boundary*, never to the last
        record's timestamp: an empty window must still move the cursor
        forward or the read would re-scan it forever and never satisfy
        ``end_offset == start_offset``.

        ``max_records_per_batch`` is best-effort by design — a window is
        always drained completely. Truncating inside a window would either
        duplicate or drop rows on an append-only table, since the next call
        would re-request the same instant.
        """
        start_iso, hard_end = self._resolve_timeseries_range(
            table_options, start_offset, None
        )
        if start_iso >= hard_end:
            return iter([]), start_offset if start_offset else {"cursor": hard_end}

        window_seconds = _parse_int(
            table_options.get("window_seconds"), DEFAULT_WINDOW_SECONDS, minimum=1
        )
        max_records = _parse_int(
            table_options.get("max_records_per_batch"),
            DEFAULT_MAX_RECORDS_PER_BATCH,
            minimum=1,
        )
        max_windows = _parse_int(
            table_options.get("max_windows_per_read"),
            DEFAULT_MAX_WINDOWS_PER_READ,
            minimum=1,
        )

        instances = self._resolve_instances(table_options)
        patterns = _resolve_item_patterns(table_options)

        records: list[dict] = []
        cursor = start_iso
        for _ in range(max_windows):
            if cursor >= hard_end:
                break
            window_end = min(_add_seconds(cursor, window_seconds), hard_end)
            for instance in instances:
                records.extend(
                    self._fetch_timeseries(
                        instance, patterns, table_options, cursor, window_end
                    )
                )
            cursor = window_end
            if len(records) >= max_records:
                break

        end_offset = {"cursor": cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Endpoint readers
    # ------------------------------------------------------------------

    def _resolve_exact_items(
        self,
        instance: str,
        patterns: Sequence[str],
        table_options: dict[str, str],
    ) -> list[str]:
        """Resolve ``item`` patterns to exact item names for ``values``/
        ``timeseries``.

        CONFIRMED live (2026-09-23): unlike ``items``, the ``values`` and
        ``timeseries`` endpoints reject wildcard patterns outright — a bare
        ``*`` fails with ``"module * not found"``, and even a real module
        prefix like ``ApisOT.*`` fails with ``"item ApisOT.* not found"`` —
        despite the spec's own documented wildcard support. Only ``items``
        genuinely supports wildcards.

        So: any pattern containing ``*`` is resolved to its real, exact
        item names via one ``/items`` lookup (patterns without ``*`` are
        already exact and pass through untouched, at no extra request
        cost — the common case of pinning a specific item stays cheap).
        All wildcard patterns are resolved in a single ``/items`` call,
        mirroring ``_fetch_items``'s own multi-pattern style.
        """
        exact: list[str] = []
        wildcard: list[str] = []
        for pattern in patterns:
            (wildcard if "*" in pattern else exact).append(pattern)

        if wildcard:
            payload = self._get_json(
                f"/hive/{instance}/items",
                {"item": wildcard, "format": RESPONSE_FORMAT},
            )
            exact.extend(
                record["item_name"]
                for record in _normalize_items(instance, payload)
                if record.get("item_name")
            )

        seen: set[str] = set()
        out: list[str] = []
        for name in exact:
            if name not in seen:
                seen.add(name)
                out.append(name)
        return out

    def _fetch_items(
        self,
        instance: str,
        patterns: Sequence[str],
        table_options: dict[str, str],
    ) -> list[dict]:
        """``GET /hive/{instance}/items`` for one or more wildcard patterns."""
        params: dict[str, Any] = {
            "item": list(patterns),
            "format": RESPONSE_FORMAT,
        }
        attribs = _split_csv(table_options.get("attrib") or table_options.get("attribs"))
        if attribs:
            params["attrib"] = attribs

        payload = self._get_json(f"/hive/{instance}/items", params)
        return _normalize_items(instance, payload)

    def _fetch_values(
        self,
        instance: str,
        patterns: Sequence[str],
        table_options: dict[str, str],
        updated_since: str | None,
    ) -> list[dict]:
        """``GET /hive/{instance}/values`` — current value per item.

        Patterns are resolved to exact item names first (see
        ``_resolve_exact_items`` — this endpoint rejects wildcards), then
        batched into groups of ``items_per_request`` exact names per
        request (CONFIRMED live: multiple exact names batch into one
        request via repeated ``item=`` params; the real upper limit on
        batch size is unconfirmed — see ``DEFAULT_ITEMS_PER_REQUEST``).

        Local validation (``quality``) runs before ``_resolve_exact_items``,
        which can itself make a network call for wildcard patterns — bad
        options must fail before any request goes out, never after.
        """
        quality = _resolve_quality(table_options)
        batch_size = _parse_int(
            table_options.get("items_per_request"),
            DEFAULT_ITEMS_PER_REQUEST,
            minimum=1,
        )

        exact_items = self._resolve_exact_items(instance, patterns, table_options)
        if not exact_items:
            return []

        records: list[dict] = []
        for batch in _chunk(exact_items, batch_size):
            params: dict[str, Any] = {
                "item": batch,
                "format": RESPONSE_FORMAT,
            }
            if updated_since:
                params["updatedSince"] = _to_wire_timestamp(updated_since)
            if quality:
                params["quality"] = quality

            payload = self._get_json(f"/hive/{instance}/values", params)
            records.extend(_normalize_values(instance, payload))
        return records

    def _fetch_timeseries(
        self,
        instance: str,
        patterns: Sequence[str],
        table_options: dict[str, str],
        start_iso: str,
        end_iso: str,
    ) -> list[dict]:
        """``GET /hive/{instance}/timeseries`` over one window.

        ``starttime``/``endtime`` are both required by the source. The
        connector treats the window as half-open ``[starttime, endtime)``
        (the reading taken in ``apis_api_doc.md``) and makes consecutive
        windows contiguous, so an append-only row cannot land in two
        windows. If live testing shows the server treats ``endtime`` as
        inclusive, boundary rows would duplicate and the windows should be
        shortened by one interval instead.

        Patterns are resolved to exact item names first (see
        ``_resolve_exact_items`` — this endpoint rejects wildcards, same as
        ``values``), then batched into groups of ``items_per_request``.

        Local validation (``quality``/``aggregate``/``interval``) runs
        before ``_resolve_exact_items``, which can itself make a network
        call for wildcard patterns — bad options must fail before any
        request goes out, never after.
        """
        quality = _resolve_quality(table_options)

        aggregate = (table_options.get("aggregate") or "").strip()
        if aggregate and aggregate not in AGGREGATE_VALUES:
            raise ValueError(
                f"Unsupported 'aggregate' value {aggregate!r}. Must be one of "
                f"{sorted(AGGREGATE_VALUES)}"
            )

        interval: int | None = None
        interval_raw = table_options.get("interval")
        if interval_raw not in (None, ""):
            interval = _parse_int(interval_raw, MIN_INTERVAL_SECONDS, minimum=1)
            if not MIN_INTERVAL_SECONDS <= interval <= MAX_INTERVAL_SECONDS:
                raise ValueError(
                    f"'interval' must be between {MIN_INTERVAL_SECONDS} and "
                    f"{MAX_INTERVAL_SECONDS} seconds, got {interval}"
                )

        batch_size = _parse_int(
            table_options.get("items_per_request"),
            DEFAULT_ITEMS_PER_REQUEST,
            minimum=1,
        )

        exact_items = self._resolve_exact_items(instance, patterns, table_options)
        if not exact_items:
            return []

        records: list[dict] = []
        for batch in _chunk(exact_items, batch_size):
            params: dict[str, Any] = {
                "item": batch,
                "starttime": _to_wire_timestamp(start_iso),
                "endtime": _to_wire_timestamp(end_iso),
                "format": RESPONSE_FORMAT,
            }
            if quality:
                params["quality"] = quality
            if aggregate:
                params["aggregate"] = aggregate
            if interval is not None:
                params["interval"] = interval

            payload = self._get_json(f"/hive/{instance}/timeseries", params)
            records.extend(_normalize_timeseries(instance, payload))
        return records

    # ------------------------------------------------------------------
    # Instance resolution
    # ------------------------------------------------------------------

    def _resolve_instances(self, table_options: dict[str, str]) -> list[str]:
        """Resolve the ``{instance}`` path parameter(s).

        Precedence: table options, then connection options, then a one-time
        ``GET /hive`` lookup. ``/hive`` is a config-time lookup, not a
        table — it produces no rows, only the names every other call needs.
        """
        from_table = _split_csv(
            (table_options or {}).get("instance") or (table_options or {}).get("instances")
        )
        if from_table:
            return from_table
        if self._configured_instances:
            return list(self._configured_instances)
        if self._discovered_instances is None:
            self._discovered_instances = self._discover_instances()
        return list(self._discovered_instances)

    def _discover_instances(self) -> list[str]:
        """``GET /hive`` — the only way to learn instance names.

        The response has no declared schema. Both plausible renderings are
        accepted: a bare array of strings (``["ApisHive"]``) and an array of
        objects carrying a name-ish field.
        """
        payload = self._get_json("/hive", {"format": RESPONSE_FORMAT})
        names: list[str] = []
        for entry in _as_list(payload):
            name = None
            if isinstance(entry, str):
                name = entry
            elif isinstance(entry, dict):
                name = _first_str(entry, _INSTANCE_NAME_KEYS)
            if name and name not in names:
                names.append(name)

        if not names:
            raise RuntimeError(
                "GET /hive returned no Hive instances. Set the 'instance' "
                "connection or table option to target one explicitly."
            )
        return names

    # ------------------------------------------------------------------
    # Range / window arithmetic
    # ------------------------------------------------------------------

    def _resolve_timeseries_range(
        self,
        table_options: dict[str, str],
        start_offset: dict | None,
        end_offset: dict | None,
    ) -> tuple[str, str]:
        """Resolve the ``[start, end)`` range for a ``timeseries`` read.

        ``starttime`` is mandatory upstream, and the source offers no way to
        discover the oldest recorded point (no "first value" endpoint), so
        auto-discovery is impossible. The lower bound comes from, in order:
        the committed cursor, an explicit ``start_timestamp``, or
        ``backfill_days`` before the init-time cap.
        """
        end_iso = (end_offset or {}).get("cursor") or self._init_time
        # Never read past this trigger's cap, whatever the caller passed.
        if end_iso > self._init_time:
            end_iso = self._init_time

        start_iso = (start_offset or {}).get("cursor")
        if not start_iso:
            start_iso = (table_options.get("start_timestamp") or "").strip() or None
        if start_iso:
            return _normalize_timestamp(start_iso), end_iso

        backfill_days = _parse_int(
            table_options.get("backfill_days"), DEFAULT_BACKFILL_DAYS, minimum=1
        )
        return _add_seconds(end_iso, -backfill_days * 86_400), end_iso

    def _build_windows(
        self,
        start_iso: str,
        end_iso: str,
        table_options: dict[str, str],
        slices: int,
    ) -> list[tuple[str, str]]:
        """Chop ``[start, end)`` into contiguous, non-overlapping windows.

        ``max_partitions`` bounds the task count: when the requested range
        needs more windows than the budget allows, the window is widened
        rather than emitting thousands of tiny tasks. ``slices`` is the
        number of (instance x pattern) combinations each window is
        multiplied by, so the budget accounts for the full fan-out.
        """
        window_seconds = _parse_int(
            table_options.get("window_seconds"), DEFAULT_WINDOW_SECONDS, minimum=1
        )
        max_partitions = _parse_int(
            table_options.get("max_partitions"), DEFAULT_MAX_PARTITIONS, minimum=1
        )

        total_seconds = max(1, int((_parse_iso(end_iso) - _parse_iso(start_iso)).total_seconds()))
        window_budget = max(1, max_partitions // max(1, slices))
        needed = math.ceil(total_seconds / window_seconds)
        if needed > window_budget:
            window_seconds = math.ceil(total_seconds / window_budget)

        windows: list[tuple[str, str]] = []
        cursor = start_iso
        while cursor < end_iso:
            window_end = min(_add_seconds(cursor, window_seconds), end_iso)
            windows.append((cursor, window_end))
            cursor = window_end
        return windows

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        value = f"{self._auth_scheme} {self._token}".strip() if self._auth_scheme else self._token
        return {"Authorization": value, "Accept": "application/json"}

    def _get_json(self, path: str, params: dict[str, Any]) -> Any:
        """Issue one GET and return the parsed JSON body.

        Deliberately single-shot: the source documents no pagination, so
        there is no next-page parameter to follow. A ``206 Partial Content``
        means the server truncated the result and there is *no* documented
        way to resume — the only mitigation the spec allows is a narrower
        ``item`` pattern or a shorter window, so we surface a warning rather
        than silently returning a partial table.
        """
        url = f"{self._base_url}{path}"
        response = self._request_with_retry(url, params)

        if response.status_code == 206:
            logger.warning(
                "Apis %s returned 206 Partial Content: the server truncated the "
                "result and the API defines no continuation token, Link header or "
                "offset parameter to resume with. Narrow the 'item' pattern "
                "(e.g. per-module wildcards) or shorten 'window_seconds' to keep "
                "responses whole.",
                path,
            )
        elif response.status_code != 200:
            raise RuntimeError(
                f"Apis request failed: GET {path} -> HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

        if not response.content:
            return []
        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Apis request GET {path} returned a non-JSON body: "
                f"{response.text[:500]}"
            ) from exc

    def _request_with_retry(self, url: str, params: dict[str, Any]) -> requests.Response:
        """GET with exponential backoff on transient failures.

        The spec documents no rate limits and declares no ``429`` anywhere,
        so the retry set is the conservative union of throttling and gateway
        codes rather than anything the source promised.
        """
        backoff = INITIAL_BACKOFF_SECONDS
        last_error: Exception | None = None
        response: requests.Response | None = None

        for attempt in range(self._max_retries):
            try:
                response = self._session.get(
                    url,
                    params=params,
                    headers=self._headers(),
                    timeout=self._timeout,
                    verify=self._verify_ssl,
                )
                if response.status_code not in RETRIABLE_STATUS_CODES:
                    return response
                last_error = None
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                response = None

            if attempt < self._max_retries - 1:
                time.sleep(backoff)
                backoff *= 2

        if response is not None:
            return response
        raise RuntimeError(
            f"Apis request failed after {self._max_retries} attempts: {url}"
        ) from last_error

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table {table_name!r} is not supported. Supported tables: "
                f"{list(SUPPORTED_TABLES)}"
            )


# ---------------------------------------------------------------------------
# Response normalizers
#
# Only ``values`` has a spec-declared body. The other two parsers accept
# several plausible renderings on purpose: guessing one shape and hard-failing
# on the rest would make the connector brittle against the very uncertainty
# the spec leaves open. Each branch below is labelled with why it is plausible.
# ---------------------------------------------------------------------------


def _normalize_items(instance: str, payload: Any) -> list[dict]:
    """Project an ``items`` response onto ``ITEMS_SCHEMA``.

    Accepted renderings for the identifying field (CONFIRMED live: the real
    server sends ``name``, not the spec's ``item_name``):
      * array of objects with an item-name field (``item_name`` per
        ``single_value_t``, or ``name``/``item``);
      * array of bare item-name strings;
      * object keyed by item name, value = attribute bag.
    """
    records: list[dict] = []
    for name, entry in _iter_named_entries(payload, _ITEM_NAME_KEYS):
        if not name:
            continue
        records.append(
            {
                "instance": instance,
                "item_name": name,
                # CONFIRMED live: matches the real `modules` endpoint's
                # module names exactly, including for deeply nested paths.
                "module": name.split(".", 1)[0] if "." in name else None,
                # CONFIRMED live: the source's own field is named "type"
                # (observed: "Signal", "Function item", "Status").
                "item_type": entry.get("type") if isinstance(entry, dict) else None,
                "attributes": _normalize_attributes(entry),
            }
        )
    return records


def _normalize_attributes(entry: Any) -> list[dict] | None:
    """Best-effort projection of the ``attrib`` payload.

    The spec says only that ``attrib`` takes numeric "Attribute ID" values;
    it never says how they come back, and there is no ID-to-meaning registry
    anywhere in the document. Both a list form and a mapping form are
    accepted, and anything unrecognised yields ``None`` rather than a guess.
    """
    if not isinstance(entry, dict):
        return None

    raw = entry.get("attributes")
    if raw is None:
        raw = entry.get("attribs")

    if isinstance(raw, list):
        out: list[dict] = []
        for item in raw:
            if isinstance(item, dict):
                attrib_id = _first_str(item, ("attrib_id", "attrib", "id", "attribute_id"))
                out.append(
                    {
                        "attrib_id": attrib_id,
                        "value": _as_str(
                            item.get("value") if "value" in item else item.get("v")
                        ),
                    }
                )
            else:
                out.append({"attrib_id": None, "value": _as_str(item)})
        return out

    if isinstance(raw, dict):
        return [
            {"attrib_id": str(key), "value": _as_str(value)}
            for key, value in raw.items()
        ]

    # Fall back to numerically-named top-level keys — the shape a server
    # would produce if it inlined requested attribute IDs onto the item.
    inline = [
        {"attrib_id": str(key), "value": _as_str(value)}
        for key, value in entry.items()
        if str(key).isdigit()
    ]
    return inline or None


def _normalize_values(instance: str, payload: Any) -> list[dict]:
    """Project a ``values`` response onto ``VALUES_SCHEMA``.

    SPEC-DECLARED shape: ``[{"item_name": ..., "value": {"v","q","t"}}]``.
    A flattened ``{"item_name","v","q","t"}`` rendering is also tolerated,
    since the declared schema is the *only* one in the document and a thin
    service could easily inline the triple. Either way, the wire triple is
    flattened onto the row's ``value``/``quality``/``timestamp`` columns.
    """
    records: list[dict] = []
    for name, entry in _iter_named_entries(payload, _ITEM_NAME_KEYS):
        records.append(
            {
                "instance": instance,
                "item_name": name,
                **_flatten_point(_extract_point(entry)),
            }
        )
    return records


def _normalize_timeseries(instance: str, payload: Any) -> list[dict]:
    """Project a ``timeseries`` response onto ``TIMESERIES_SCHEMA``.

    INFERRED shape — the single largest gap in the source spec (neither
    ``200`` nor ``206`` declares any ``content``). One row is emitted per
    (item, point in time). Accepted renderings, all structurally derived
    from ``single_value_t``:

      * per-item bundle: ``[{"item_name": X, "values": [{v,q,t}, ...]}]``
        — the shape implied by ``aggregate``/``interval`` being per-item,
        per-bucket concepts;
      * per-item single point: ``[{"item_name": X, "value": {v,q,t}}]``
        — i.e. ``values``' declared shape reused verbatim;
      * flat point stream: ``[{"item_name": X, "v":..., "q":..., "t":...}]``.
    """
    records: list[dict] = []
    for name, entry in _iter_named_entries(payload, _ITEM_NAME_KEYS):
        for point in _extract_points(entry):
            records.append(
                {
                    "instance": instance,
                    "item_name": name,
                    **_flatten_point(point),
                }
            )
    return records


def _extract_points(entry: Any) -> list[dict | None]:
    """Pull every ``(v, q, t)`` point out of one per-item entry."""
    if not isinstance(entry, dict):
        return [_extract_point(entry)]

    for key in ("values", "value", "points", "data"):
        raw = entry.get(key)
        if isinstance(raw, list):
            return [_extract_point(point) for point in raw]
        if isinstance(raw, dict):
            return [_extract_point(raw)]

    if any(key in entry for key in _POINT_KEYS):
        return [_extract_point(entry)]
    return []


def _extract_point(raw: Any) -> dict | None:
    """Coerce one point into a ``{"v", "q", "t"}`` dict.

    Returns ``None`` rather than ``{}`` when nothing usable is present —
    ``_flatten_point`` maps that into all-``None`` output columns.
    """
    if isinstance(raw, dict):
        nested = raw.get("value")
        if isinstance(nested, dict):
            raw = nested
        if not any(key in raw for key in _POINT_KEYS):
            return None
        return {"v": raw.get("v"), "q": raw.get("q"), "t": raw.get("t")}
    if raw is None:
        return None
    # A bare scalar: the value itself, with no quality or timestamp.
    return {"v": raw, "q": None, "t": None}


def _flatten_point(point: dict | None) -> dict:
    """Project a ``{"v", "q", "t"}`` point onto flat ``value``/``quality``/
    ``timestamp`` row columns (``VALUES_SCHEMA``/``TIMESERIES_SCHEMA``),
    rather than keeping the wire shape nested, so downstream SQL can
    reference each field directly without a struct accessor.
    """
    point = point or {}
    return {
        "value": point.get("v"),
        "quality": point.get("q"),
        "timestamp": point.get("t"),
    }


def _iter_named_entries(
    payload: Any, name_keys: Sequence[str]
) -> Iterator[tuple[str | None, Any]]:
    """Yield ``(item_name, entry)`` pairs from any of the plausible shapes."""
    if isinstance(payload, dict):
        # Either a wrapper around the array, or a mapping keyed by item name.
        for key in ("items", "values", "results", "data", "value"):
            inner = payload.get(key)
            if isinstance(inner, list):
                yield from _iter_named_entries(inner, name_keys)
                return
        for key, value in payload.items():
            if isinstance(value, (dict, list)) or not str(key).isdigit():
                yield str(key), value
        return

    for entry in _as_list(payload):
        if isinstance(entry, str):
            yield entry, None
        elif isinstance(entry, dict):
            yield _first_str(entry, name_keys), entry


def _as_list(payload: Any) -> list:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return [payload]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _resolve_quality(table_options: dict[str, str]) -> str | None:
    quality = (table_options.get("quality") or "").strip().lower()
    if not quality:
        return None
    if quality not in QUALITY_VALUES:
        raise ValueError(
            f"Unsupported 'quality' value {quality!r}. Must be one of "
            f"{sorted(QUALITY_VALUES)}"
        )
    return quality


def _resolve_item_patterns(table_options: dict[str, str]) -> list[str]:
    """``item`` is a required query parameter, so there is always a pattern."""
    raw = (
        (table_options or {}).get("item")
        or (table_options or {}).get("items")
        or (table_options or {}).get("item_patterns")
    )
    patterns = _split_csv(raw)
    return patterns or [DEFAULT_ITEM_PATTERN]


def _split_csv(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        values = [str(v).strip() for v in raw]
    else:
        values = [part.strip() for part in str(raw).split(",")]
    out: list[str] = []
    for value in values:
        if value and value not in out:
            out.append(value)
    return out


def _chunk(items: Sequence[str], size: int) -> Iterator[list[str]]:
    """Split ``items`` into consecutive batches of at most ``size``."""
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def _first_str(entry: dict, keys: Sequence[str]) -> str | None:
    for key in keys:
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, (int, float)):
            return str(value)
    return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _parse_int(value: Any, default: int, *, minimum: int = 0) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError, AttributeError):
        return default
    return parsed if parsed >= minimum else default


def _parse_iso(value: str) -> datetime:
    normalised = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO-8601 timestamp {value!r}. The source also accepts "
            f"relative 'OPC time' expressions (e.g. 'DAY-1D'), but this "
            f"connector always sends absolute instants so incremental reads "
            f"stay deterministic — supply an absolute timestamp."
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


def _normalize_timestamp(value: str) -> str:
    return _format_iso(_parse_iso(value))


def _to_wire_timestamp(iso_value: str) -> str:
    """Convert an internal ISO-8601 cursor/offset value into the format the
    source actually accepts on the wire (CONFIRMED live, 2026-09-23) for
    ``updatedSince``/``starttime``/``endtime`` request parameters — see
    ``WIRE_TIMESTAMP_FORMAT`` in ``apis_schemas.py`` for why this differs
    from ISO-8601. Apply this only when placing a timestamp into an actual
    HTTP request parameter, never to values used for internal cursor
    arithmetic or comparisons.
    """
    return _parse_iso(iso_value).strftime(WIRE_TIMESTAMP_FORMAT)


def _add_seconds(iso_value: str, seconds: int) -> str:
    shifted = _parse_iso(iso_value) + timedelta(seconds=seconds)
    epoch = _parse_iso(EPOCH_ISO)
    if shifted < epoch:
        shifted = epoch
    return _format_iso(shifted)
