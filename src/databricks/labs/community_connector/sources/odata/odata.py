"""OData v4 community connector for Lakeflow Connect.

Implements the LakeflowConnect interface for any OData v4 service. The
connector discovers tables and schemas from the service's ``$metadata``
endpoint, supports four auth methods (bearer / basic / api_key /
oauth2), and ingests each entity set either as a snapshot, an
incremental CDC stream keyed off a user-supplied cursor field, or
(when the service supports it) a server-driven delta query stream.

Connection options (set on the UC connection):
    service_url   required   OData service root, e.g.
                             https://services.odata.org/V4/Northwind/Northwind.svc/
    auth_type     optional   bearer | basic | api_key | oauth2
    token, username, password, api_key, api_key_header,
    oauth2_token_url, oauth2_client_id, oauth2_client_secret, oauth2_scope

Per-table options (allowlisted via externalOptionsAllowList):
    cursor_field          column to drive incremental reads; absent → snapshot
    select                comma-separated $select projection
    filter                additional $filter expression
    page_size             $top per request (default 1000)
    max_records_per_batch cap rows returned per read_table call (default 10000)
    delta_tracking        disabled (default) | auto | enabled. Opt-in.
                          When the source honours ``Prefer: odata.track-changes``
                          (MS Graph, Dataverse, SAP S/4HANA Cloud …), the
                          connector reads via the OData delta link instead of
                          cursor filtering, and emits removals as in-band
                          ``_deleted=True`` rows. ``auto`` probes once per
                          table and falls back to cursor/snapshot if the
                          server doesn't acknowledge; ``enabled`` requires
                          support and errors if the server doesn't acknowledge;
                          ``disabled`` skips the probe entirely.
"""

import base64
import hashlib
import itertools
import json
import os
import pickle
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterator
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.interface.supports_namespaces import (
    SupportsNamespaces,
)

# Contained navigation-property support lives in ``_contained.py`` to keep
# this module under its line-count budget. Re-exported under the original
# private names so the rest of this file can keep using them as before.
from databricks.labs.community_connector.sources.odata._helpers import (
    trim_to_distinct_cursor_boundary as _trim_to_distinct_cursor_boundary,
)
from databricks.labs.community_connector.sources.odata._contained import (
    CONTAINED_PATH_SEP as _CONTAINED_PATH_SEP,
    MAX_CONTAINED_DEPTH as _MAX_CONTAINED_DEPTH,
    ContainedNavMixin,
    combine_filters as _combine_filters,
    contained_nav_props as _contained_nav_props,
    fk_column_name as _fk_column_name,
    join_url as _join_url,
    looks_like_iso8601 as _looks_like_iso8601,
    odata_literal as _odata_literal,
    parse_contained_path as _parse_contained_path,
    resolve_segment_filters as _resolve_segment_filters,
)
from databricks.labs.community_connector.sources.odata._partition import (
    PartitionMixin,
)


# ---------------------------------------------------------------------------
# EDM (CSDL) → Spark type mapping
# ---------------------------------------------------------------------------

_EDM_TO_SPARK = {
    "Edm.String": StringType(),
    "Edm.Boolean": BooleanType(),
    # Widen integers up to Int32 to IntegerType (the framework's
    # parse_value doesn't support ShortType or ByteType, so the narrow
    # EDM widths can't map to their natural Spark types). Int64 needs
    # the full 64-bit range, so it stays as LongType.
    "Edm.Byte": IntegerType(),
    "Edm.SByte": IntegerType(),
    "Edm.Int16": IntegerType(),
    "Edm.Int32": IntegerType(),
    "Edm.Int64": LongType(),
    "Edm.Single": FloatType(),
    "Edm.Double": DoubleType(),
    "Edm.Decimal": DecimalType(38, 18),
    "Edm.Date": DateType(),
    "Edm.DateTime": TimestampType(),
    "Edm.DateTimeOffset": TimestampType(),
    "Edm.TimeOfDay": StringType(),
    "Edm.Duration": StringType(),
    "Edm.Guid": StringType(),
    "Edm.Binary": BinaryType(),
}

_NS_EDMX = "{http://docs.oasis-open.org/odata/ns/edmx}"
_NS_EDM = "{http://docs.oasis-open.org/odata/ns/edm}"

# Delta tracking constants.
#
# Synthetic columns appended to the schema when delta is active so the
# destination MERGE (apply_changes) has a sequence column and a tombstone
# flag. Their names are namespaced so they can't collide with any real
# OData property — OData property names start with a letter, never an
# underscore.
_DELTA_PREFER = "odata.track-changes"
_DELETED_COL = "_deleted"
_SEQUENCE_COL = "_lc_sequence"
# Effectively-unlimited value for ``max_records_per_batch`` when the
# framework's batch reader is detected (``start_offset is None``).
# A bare ``sys.maxsize`` is unnecessary — the per-fetch cap arithmetic
# only ever compares ``<= len(emitted)``; any value larger than what a
# single ingestion could plausibly buffer works.
_BATCH_UNCAPPED = 10**12
# Monotonic across the whole process — guarantees each emitted record
# has a strictly increasing sequence value, so apply_changes can pick a
# deterministic winner when the same primary key appears multiple times
# in one batch (e.g. update then delete arriving back-to-back).
_SEQUENCE_COUNTER = itertools.count()

# Process-wide CSDL cache, keyed by service_url. SDP creates a fresh
# ``LakeflowSource`` (and ``ODataLakeflowConnect``) for every
# ``spark.readStream.format("lakeflow_connect").load()`` call; within
# a single Python process this cache makes all instances share one
# parse.
_METADATA_CACHE: dict[str, tuple[str, ET.Element, "_CsdlIndex"]] = {}

# On-disk CSDL cache. PySpark's Python Data Source forks a fresh
# ``pyspark.daemon`` worker for schema inference on every ``.load()``
# call, so the process-wide cache above doesn't survive — each fork
# starts with an empty dict. On a pipeline with N tables that means
# N HTTP fetches and N multi-MB XML parses during INITIALIZING. The
# file cache lets each forked worker read a pickled parsed tree from
# tempdir instead, saving the HTTP RT + parse on every fork after the
# first. The TTL is short so subsequent pipeline triggers pick up
# upstream schema changes; per-trigger we still pay one fresh fetch.
_METADATA_FILE_CACHE_TTL_SECONDS = 60

# Network-level exceptions treated as transient by ``_http_get``'s retry
# loop. ``ConnectionError`` covers TCP resets, DNS failures, and remote
# disconnects (e.g. the server killed the keep-alive connection mid-
# request). ``Timeout`` covers both connect and read timeouts.
# ``ChunkedEncodingError`` covers servers that close mid-body during a
# chunked transfer (seen in practice with Hexagon SCApi under load).
_TRANSIENT_NETWORK_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def _metadata_cache_path(service_url: str) -> str:
    """Tempdir path for the pickled CSDL of ``service_url``."""
    digest = hashlib.sha256(service_url.encode("utf-8")).hexdigest()[:16]
    return os.path.join(tempfile.gettempdir(), f"odata_csdl_{digest}.pickle")


def _clear_metadata_cache() -> None:
    """Clear the in-process CSDL cache and remove any on-disk pickle
    files. Tests use this between cases that reuse a ``service_url``
    with different mocked ``$metadata`` bodies."""
    _METADATA_CACHE.clear()
    # Best-effort cleanup of all on-disk cache files. Tests don't know
    # the service_url hash in advance, so wipe the whole pattern.
    tmpdir = tempfile.gettempdir()
    try:
        for entry in os.listdir(tmpdir):
            if entry.startswith("odata_csdl_") and entry.endswith(".pickle"):
                try:
                    os.remove(os.path.join(tmpdir, entry))
                except OSError:
                    pass
    except OSError:
        pass


@dataclass
class _CsdlIndex:
    """One-time index of a parsed CSDL document.

    Before this index existed every metadata lookup (resolve an entity
    set to its type, follow a base-type chain, find an entity type by
    qualified name) re-walked the whole ET tree. On a multi-MB CSDL
    that's tens of milliseconds per call, and the connector makes
    dozens of calls per table — measurable both during INITIALIZING
    (table discovery, schema inference) and during steady-state
    incremental reads (FK column resolution per batch).

    The index is built once per parsed root and bundled with that root
    in the in-memory cache; whenever the root is refreshed (file-cache
    TTL expiry, in-process eviction) the index is rebuilt with it.
    Per-instance memo dicts hang off ``ODataLakeflowConnect`` rather
    than this dataclass — they're populated lazily by callers and
    invalidated when the index they were built against is replaced.
    """

    # Every ``(schema_namespace, entity_set_name)`` pair declared in
    # ``$metadata``. Order matches CSDL declaration order so error
    # hints and listings stay deterministic.
    entity_set_pairs: list[tuple[str, str]]
    # ``(namespace, entity_set_name) → entity_type_ref_string`` — the
    # raw ``EntityType=`` attribute the entity set points at.
    entity_set_to_type_ref: dict[tuple[str, str], str]
    # ``entity_set_name → list[(namespace, type_ref)]``. Multiple
    # entries means the same name lives in two schemas; callers must
    # disambiguate via the ``namespace`` table option.
    entity_set_by_name: dict[str, list[tuple[str, str]]]
    # ``namespace → list[entity_set_name]``. Used for error hints when
    # a namespace was supplied but the set wasn't found.
    entity_set_names_by_ns: dict[str, list[str]]
    # ``namespace_or_alias → canonical_namespace``. CSDL ``Alias``
    # attributes route through here so ``BaseType="graph.user"``
    # resolves to the schema declaring ``Namespace="microsoft.graph"``.
    alias_to_namespace: dict[str, str]
    # Fully-qualified type name (using canonical namespace) →
    # ``EntityType`` element. The qname uses the canonical namespace,
    # not any alias; callers must alias-resolve first.
    entity_type_by_qname: dict[str, ET.Element]
    # All namespaces that declare at least one entity set. Used for
    # error hints when the requested namespace declares only types.
    namespaces_with_sets: list[str]


def _build_csdl_index(root: ET.Element) -> _CsdlIndex:
    """Single tree walk that populates every lookup in ``_CsdlIndex``.

    The CSDL can declare multiple ``<Schema>`` blocks; each schema can
    declare entity types and (optionally) an ``<EntityContainer>`` with
    entity sets. The walk threads schemas → containers → entity sets in
    one pass while also indexing every ``<EntityType>`` by qualified
    name. Subsequent dict lookups replace what used to be O(N) tree
    scans.
    """
    entity_set_pairs: list[tuple[str, str]] = []
    entity_set_to_type_ref: dict[tuple[str, str], str] = {}
    entity_set_by_name: dict[str, list[tuple[str, str]]] = {}
    entity_set_names_by_ns: dict[str, list[str]] = {}
    alias_to_namespace: dict[str, str] = {}
    entity_type_by_qname: dict[str, ET.Element] = {}
    namespaces_with_sets: list[str] = []

    for schema in root.iter(f"{_NS_EDM}Schema"):
        ns = schema.get("Namespace") or ""
        if ns:
            alias_to_namespace[ns] = ns
        alias = schema.get("Alias")
        if alias:
            alias_to_namespace[alias] = ns

        for entity_type in schema.findall(f"{_NS_EDM}EntityType"):
            type_name = entity_type.get("Name")
            if type_name:
                entity_type_by_qname[f"{ns}.{type_name}"] = entity_type

        had_set = False
        for container in schema.iter(f"{_NS_EDM}EntityContainer"):
            for es in container.iter(f"{_NS_EDM}EntitySet"):
                set_name = es.get("Name")
                type_ref = es.get("EntityType") or ""
                entity_set_pairs.append((ns, set_name))
                entity_set_to_type_ref[(ns, set_name)] = type_ref
                entity_set_by_name.setdefault(set_name, []).append((ns, type_ref))
                entity_set_names_by_ns.setdefault(ns, []).append(set_name)
                had_set = True
        if had_set and ns and ns not in namespaces_with_sets:
            namespaces_with_sets.append(ns)

    return _CsdlIndex(
        entity_set_pairs=entity_set_pairs,
        entity_set_to_type_ref=entity_set_to_type_ref,
        entity_set_by_name=entity_set_by_name,
        entity_set_names_by_ns=entity_set_names_by_ns,
        alias_to_namespace=alias_to_namespace,
        entity_type_by_qname=entity_type_by_qname,
        namespaces_with_sets=namespaces_with_sets,
    )


@dataclass
class _MetadataState:
    """Per-instance bundle for parsed-CSDL state: the root, the index
    built from it, and the memo dicts populated as callers walk it.

    Bundling lets ``ODataLakeflowConnect`` stay within pylint's
    instance-attribute budget (one attribute vs. seven) and ensures
    the memos invalidate atomically when the root is refreshed —
    callers reach for ``self._metadata`` and either get the full
    bundle or rebuild it from scratch."""

    root: ET.Element
    index: _CsdlIndex
    # All memos are keyed off either ``id(et)`` (for methods taking
    # an ``ET.Element``) or ``(table_name, namespace)``. They're
    # safe across the lifetime of ``root`` because element identity
    # is stable within one parsed tree.
    fields: dict = field(default_factory=dict)
    primary_keys: dict = field(default_factory=dict)
    base_chain: dict = field(default_factory=dict)
    own_fields: dict = field(default_factory=dict)
    own_pks: dict = field(default_factory=dict)
    entity_type: dict = field(default_factory=dict)
    fk_columns: dict = field(default_factory=dict)


def _next_sequence() -> str:
    """Strictly-increasing per-record sequence value for apply_changes.

    Format: ``<ns_since_epoch:020d>_<counter:012d>``. Both parts
    are zero-padded so the lexicographic string ordering used by
    ``apply_changes`` matches the underlying numeric ordering. The
    nanosecond timestamp tracks wall-clock so values stay ordered
    across process restarts (latest data wins per key); the counter
    breaks ties for records emitted in the same nanosecond.

    ``time.time_ns()`` skips the ``datetime`` + ``strftime`` round-
    trip the previous ISO-8601 format paid per row — meaningful for
    delta-tracked tables that synthesise a sequence on every record.
    """
    return f"{time.time_ns():020d}_{next(_SEQUENCE_COUNTER):012d}"


class ODataLakeflowConnect(
    LakeflowConnect,
    SupportsNamespaces,
    PartitionMixin,
    ContainedNavMixin,
):
    """LakeflowConnect implementation for OData v4 services.

    OData ``$metadata`` documents can declare multiple ``<Schema>`` blocks,
    each with its own namespace and its own entity sets. Two schemas in the
    same service can re-use entity set names (e.g. ``Sales.Customers`` and
    ``HR.Customers``), so this connector exposes the schema namespace as a
    single-segment Lakeflow namespace path.

    Pipelines disambiguate by passing ``namespace`` in *table_options*. When
    only one schema declares a given table name, ``namespace`` may be omitted.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.service_url = _require(options, "service_url")
        # Default 180s (3 min). Deep ``expand_contained=true`` chains
        # (3+ segments) materialise a large cross-product server-side
        # before responding; 60s isn't long enough for most real
        # deployments and the previous default surfaced as
        # ``ReadTimeout`` retried-to-exhaustion failures. Connection
        # option ``timeout_seconds`` overrides per deployment.
        self.timeout = int(options.get("timeout_seconds", "180"))
        # On-disk pickle TTL. The default suits typical 1-minute SDP
        # trigger intervals — each trigger spawns a fresh forked
        # worker, the file cache survives, but stale state is bounded.
        # Users with stable schemas can raise this to skip even the
        # first-fork fetch within a longer window; users iterating on
        # the source model can drop it to 0 to disable file caching.
        self.metadata_cache_ttl_seconds = int(
            options.get("metadata_cache_ttl_seconds", _METADATA_FILE_CACHE_TTL_SECONDS)
        )
        # Retry budget for transient server-side failures (HTTP 429 / 503).
        # 5 attempts at exponential backoff (1, 2, 4, 8, 16 s) covers the
        # vast majority of momentary throttling spikes from Graph /
        # Dataverse / S/4HANA Cloud without keeping a Spark task pinned
        # indefinitely. `retry_max_delay_seconds` caps any single sleep —
        # honour the server's Retry-After header but never sleep longer
        # than this (some misbehaving servers emit hour-long values).
        self.max_retries = int(options.get("max_retries", "5"))
        self.retry_max_delay_seconds = int(options.get("retry_max_delay_seconds", "60"))
        self._session: requests.Session | None = None
        # Parsed CSDL bundle: root + lookup index + per-instance memos.
        # ``None`` until the first ``_metadata_root()`` call.
        self._metadata: _MetadataState | None = None
        # Monotonic deadline (seconds) for the current OAuth access token.
        # Set when the token endpoint returns ``expires_in``; `None` means we
        # don't know the expiry (user-supplied access token without metadata)
        # so we fall through to the 401-retry path only.
        self._access_token_expires_at: float | None = None
        # Delta-tracking capability cache, keyed by (namespace_or_empty,
        # table_name). Populated lazily by ``_probe_delta_support`` on the
        # first metadata-resolution call for each table in ``auto`` mode.
        # ``enabled`` mode trusts the user and skips the cache; ``disabled``
        # mode never touches it.
        self._delta_capable: dict[tuple[str, str], bool] = {}

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        """Flat fallback used by the framework when SupportsNamespaces is absent.

        Includes both top-level entity sets and contained collections
        reachable via ``ContainsTarget="true"`` navigation properties
        (double-underscore-pathed, e.g. ``Instances__Assets__AssetDocuments``).
        """
        names: set[str] = set()
        for ns, es_name in self._entity_set_index():
            names.add(es_name)
            names.update(self._enumerate_contained_paths(es_name, ns))
        return sorted(names)

    def list_namespaces(self, prefix: list[str] | None = None) -> list[list[str]]:
        # OData has a single, flat level of schema namespaces. Anything
        # below the root has no further children.
        if prefix:
            return []
        index = self._entity_set_index()
        seen = sorted({ns for ns, _ in index if ns})
        return [[ns] for ns in seen]

    def list_tables_in_namespace(self, namespace: list[str]) -> list[str]:
        index = self._entity_set_index()
        if not namespace:
            # Entity sets always live inside a Schema with a Namespace
            # attribute; root-level tables don't exist in OData v4.
            return []
        target = namespace[0]
        flat = sorted({es for ns, es in index if ns == target})
        contained: set[str] = set()
        for es_name in flat:
            contained.update(self._enumerate_contained_paths(es_name, target))
        return flat + sorted(contained)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        namespace = (table_options or {}).get("namespace")
        fields = self._fields_for(table_name, namespace)
        select = (table_options or {}).get("select")
        if select:
            wanted = {c.strip() for c in select.split(",")}
            # ``select`` filters leaf columns only; FK columns survive.
            segments = _parse_contained_path(table_name) or [table_name]
            fk_names = set(self._resolve_fk_columns(segments, namespace).values())
            fields = [f for f in fields if f.name in fk_names or f.name in wanted]
        # Contained path + cursor_field lives on an ancestor → propagate
        # the ancestor's cursor column type onto the leaf schema. The
        # incremental read path stamps the value onto each emitted row.
        cursor_field = (table_options or {}).get("cursor_field")
        if cursor_field:
            ancestor_cursor = self._ancestor_cursor_field(table_name, namespace, cursor_field)
            if ancestor_cursor is not None and ancestor_cursor.name not in {f.name for f in fields}:
                fields = list(fields) + [ancestor_cursor]
        if not fields:
            raise ValueError(
                f"Could not derive a non-empty schema for entity set {table_name!r}. "
                f"Check the 'select' option."
            )
        # When delta tracking is active for this table the connector emits
        # two synthetic columns alongside the entity's own properties:
        # ``_deleted`` (in-band tombstone flag) and ``_lc_sequence`` (the
        # cursor column apply_changes uses to order updates). Both must be
        # in the declared schema so Spark accepts the records.
        if self._delta_active_for(table_name, table_options):
            fields = list(fields) + [
                StructField(_DELETED_COL, BooleanType(), False),
                StructField(_SEQUENCE_COL, StringType(), False),
            ]
        return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        namespace = (table_options or {}).get("namespace")
        primary_keys = self._primary_keys_for(table_name, namespace)
        user_cursor = (table_options or {}).get("cursor_field")
        # Contained paths skip the delta probe (server delta is for
        # top-level sets only; mutex enforced in dispatch below).
        if _parse_contained_path(table_name) is None and self._delta_active_for(
            table_name, table_options
        ):
            return {
                "primary_keys": primary_keys,
                "cursor_field": _SEQUENCE_COL,
                "ingestion_type": "cdc",
            }
        return {
            "primary_keys": primary_keys,
            "cursor_field": user_cursor,
            "ingestion_type": "cdc" if user_cursor else "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # The Spark Python Data Source batch reader
        # (``LakeflowBatchReader``) passes ``start_offset=None`` and
        # discards the returned end-offset — so any continuation state
        # the connector would normally park in the offset (e.g.
        # ``pending_fetches`` on the ``expand_contained=true`` path,
        # ``chain_next_link`` on the leaf-cursor N+1 path) would be
        # silently dropped, truncating the read at the default
        # ``max_records_per_batch``. Both streaming readers always
        # pass a dict (``{}`` or the parked offset). Treat ``None``
        # plus an *unset* cap as the batch-mode signal and force the
        # cap effectively-infinite so the chain drains fully in one
        # call. When the user passes ``max_records_per_batch``
        # themselves we still honour it — same-cursor-cohort overflow
        # detection and any other cap-driven behaviour stays intact.
        opts = dict(table_options or {})
        if start_offset is None and "max_records_per_batch" not in opts:
            opts["max_records_per_batch"] = str(_BATCH_UNCAPPED)
        # ``offset`` is a local view used for shape checks below.
        # The original ``start_offset`` (``None`` for batch reader,
        # ``{}`` or populated dict for streaming) is passed through to
        # the read methods so ``_finalize_cursor_read`` can distinguish
        # batch from streaming and skip the no-progress raise when the
        # framework will discard the returned offset anyway.
        offset = start_offset or {}
        if _parse_contained_path(table_name) is not None:
            if self._delta_setting(opts) == "enabled":
                raise ValueError(
                    "delta_tracking=enabled is not supported on contained-"
                    "collection paths (server change tracking only applies "
                    "to top-level entity sets). Set delta_tracking=disabled "
                    "or ingest the parent set directly."
                )
            if self._expand_contained_active(opts):
                return self._read_contained_expand(table_name, start_offset, opts)
            if opts.get("cursor_field"):
                return self._read_contained_incremental(
                    table_name, start_offset, opts, opts["cursor_field"]
                )
            return self._read_contained_snapshot(table_name, opts)
        # Offset-shape check ahead of the delta predicate so a resumed
        # delta stream (offset carries delta_link / next_link) takes the
        # delta path even if delta_tracking is no longer set in options.
        if (
            "delta_link" in offset
            or "next_link" in offset
            or self._delta_active_for(table_name, opts)
        ):
            return self._read_incremental_delta(table_name, offset, opts)
        if opts.get("cursor_field"):
            return self._read_incremental(table_name, start_offset, opts, opts["cursor_field"])
        return self._read_snapshot(table_name, opts)

    # ------------------------------------------------------------------
    # Snapshot + incremental read paths
    # ------------------------------------------------------------------

    def _read_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Return the page generator directly. Spark's
        # LakeflowBatchReader.read consumes it lazily through a
        # map(parse_value, ...), so each page is fetched, parsed, and
        # streamed out before the next page is requested. Materialising
        # the whole result into a list (the prior shape) pinned every
        # row in memory at once on large tables.
        segment_filters = _resolve_segment_filters(table_options, [table_name])
        url = self._build_url(table_name, table_options, extra_filter=segment_filters.get(0))
        return self._fetch_pages(url), {}

    def _read_incremental(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        # No wall-clock upper bound on the cursor — `max_records_per_batch`
        # is the only per-call cap. Each call fetches `cursor gt since`
        # (no `le` clause), advances the offset, and Spark drives the
        # call loop. Two consequences worth knowing:
        #   * Continuous SDP pipelines pick up new rows as they arrive,
        #     because we never freeze a "snapshot at startup" timestamp.
        #     The connector instance can live for the whole stream and
        #     each batch still sees fresh source state.
        #   * Cursor type doesn't matter for the filter. Timestamps,
        #     monotonic integer IDs, GUIDs — anything the server can
        #     order in `$orderby` and compare in `$filter` works the
        #     same way. There is no type mismatch between the cursor
        #     literal and the server's column type because we don't
        #     manufacture a timestamp ceiling out of wall-clock time.
        since = start_offset.get("cursor") if start_offset else None
        segment_filters = _resolve_segment_filters(table_options, [table_name])
        extra_filter = _combine_filters(
            self._cursor_filter(cursor_field, since),
            segment_filters.get(0),
        )
        # Append primary-key columns as $orderby tie-breakers. Without a
        # fully unique sort, OData servers that paginate internally (via
        # `@odata.nextLink` with a value-based skiptoken) can split a
        # same-cursor cohort across pages: the skiptoken's strict-`>` on
        # the cursor value drops the unread tail. A unique total ordering
        # forces the skiptoken to use the key as well, so no rows are lost.
        namespace = (table_options or {}).get("namespace")
        order_terms = [f"{cursor_field} asc"]
        for pk in self._primary_keys_for(table_name, namespace):
            if pk != cursor_field:
                order_terms.append(f"{pk} asc")
        url = self._build_url(
            table_name,
            table_options,
            extra_filter=extra_filter,
            order_by=",".join(order_terms),
        )
        max_records = int(table_options.get("max_records_per_batch", "10000"))

        records: list[dict] = []
        truncated = False
        for row in self._fetch_pages(url):
            rec_cursor = row.get(cursor_field)
            if since is not None and rec_cursor is not None and rec_cursor <= since:
                continue
            records.append(row)
            if len(records) >= max_records:
                truncated = True
                break

        if not records:
            return iter([]), start_offset or {}

        # Cursor boundary safety: the next call resumes with
        # `cursor gt <last_cursor>`, so if the trailing records share that
        # cursor with unseen records on the next page — OR with concurrently
        # inserted siblings that arrive before the next call — the `gt`
        # filter would silently drop them. Trim back to the last distinct
        # cursor on every batch (not just truncated ones), so a stop/restart
        # or natural completion can't lose same-cursor inserts at the boundary.
        # Re-fetched rows on the next call are deduped at the destination
        # via apply_changes' MERGE on the primary key.
        trimmed = _trim_to_distinct_cursor_boundary(records, cursor_field)
        if not trimmed:
            # Every record in this batch shares one cursor value.
            if truncated:
                raise RuntimeError(
                    f"max_records_per_batch ({max_records}) is too small for "
                    f"{table_name!r}: every record in the batch shares cursor "
                    f"value {records[-1].get(cursor_field)!r}. Increase "
                    f"max_records_per_batch above the largest same-cursor "
                    f"cohort, or choose a higher-cardinality cursor field."
                )
            # Natural exhaustion of a single-cursor cohort. Emit as-is —
            # trimming would lose data with no way to re-fetch. There's a
            # residual race for same-cursor rows added between now and any
            # future call, which is unavoidable without finer cursor resolution.
        else:
            records = trimmed

        # OData responses ordered by the cursor — the trailing distinct
        # cursor carries the watermark in the common case. But a nullable
        # cursor with server-dependent null-ordering can produce records
        # with null cursor values, and the cohort fall-through above
        # keeps records as-is when every value is null. Compute ``max``
        # over the non-null cursors and fall back to ``since`` / ``{}``
        # — mirrors ``_read_contained_incremental_leaf_cursor``'s
        # normalization. The shared no-progress guard then fires on
        # null-only batches (committing ``{"cursor": None}`` would loop
        # because every subsequent trigger re-emits the same nulls).
        cursors = [r.get(cursor_field) for r in records if r.get(cursor_field) is not None]
        if cursors:
            end_offset = {"cursor": max(cursors)}
        elif since is not None:
            end_offset = {"cursor": since}
        else:
            end_offset = {}
        return self._finalize_cursor_read(
            start_offset, end_offset, records, table_name, cursor_field
        )

    def _read_incremental_delta(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Delta-tracked read via OData ``Prefer: odata.track-changes``.

        Three offset shapes, three entry behaviours:

        * No ``delta_link`` and no ``next_link`` — bootstrap. Send the
          initial entity-set GET with ``Prefer: odata.track-changes``,
          verify the server acknowledges via ``Preference-Applied``, and
          stream the full snapshot. The terminal page carries
          ``@odata.deltaLink``.
        * ``next_link`` set — we hit ``max_records_per_batch`` mid-
          pagination on a previous call. Resume by walking from that
          link.
        * ``delta_link`` set — server's "changes since" cursor. Walk
          ``@odata.nextLink`` chain (if any) until the terminal page
          delivers a fresh ``@odata.deltaLink``.

        Records emitted carry two synthetic columns: ``_deleted`` (bool,
        in-band tombstone flag — set ``True`` for ``@removed`` entries)
        and ``_lc_sequence`` (monotonic per-record string used as
        apply_changes' sequence column). The microsoft_teams connector
        established this convention in this repo; we follow it so the
        framework's standard ``cdc`` path handles tombstones without
        needing ``cdc_with_deletes`` + ``read_table_deletes`` split.
        """
        prev_delta_link = (start_offset or {}).get("delta_link")
        prev_next_link = (start_offset or {}).get("next_link")
        is_bootstrap = prev_delta_link is None and prev_next_link is None
        url, initial_headers = self._delta_initial_request(
            table_name, table_options, prev_delta_link, prev_next_link
        )

        namespace = (table_options or {}).get("namespace")
        primary_keys = self._primary_keys_for(table_name, namespace)
        max_records = int((table_options or {}).get("max_records_per_batch", "10000"))

        records, new_delta_link, carry_next_link, rebootstrap = self._delta_walk_pages(
            url=url,
            initial_headers=initial_headers,
            is_bootstrap=is_bootstrap,
            prev_delta_link=prev_delta_link,
            prev_next_link=prev_next_link,
            table_name=table_name,
            table_options=table_options,
            primary_keys=primary_keys,
            max_records=max_records,
        )
        if rebootstrap:
            # 410 Gone surfaced during pagination → re-bootstrap from
            # scratch. ``MERGE``-on-PK + ``_lc_sequence`` ordering
            # reconciles re-fetched rows at the destination; no data
            # loss, only HTTP cost.
            return self._read_incremental_delta(table_name, {}, table_options)

        # Graph-rotation guard. Some servers (notably Microsoft Graph)
        # mint a fresh ``@odata.deltaLink`` on every response, even when
        # the change set is empty. If we already had a delta link and
        # produced no records this call, hand back the prior link so the
        # framework sees ``end_offset == start_offset`` and a trigger
        # like AvailableNow can terminate. Following microsoft_teams.py.
        if prev_delta_link is not None and not records and not carry_next_link:
            return iter([]), {"delta_link": prev_delta_link}

        if carry_next_link:
            offset: dict = {"next_link": carry_next_link}
            # Preserve the prior delta_link as a fallback if the
            # next_link expires before the cap-resume call lands.
            if prev_delta_link is not None:
                offset["delta_link"] = prev_delta_link
            return iter(records), offset

        if new_delta_link is None:
            # Reached end of stream without a deltaLink. Server is
            # misbehaving (spec requires the terminal page to carry one).
            # Resume from the prior delta_link if we have one — better
            # than losing the offset entirely.
            if prev_delta_link is not None:
                return iter(records), {"delta_link": prev_delta_link}
            raise RuntimeError(
                f"OData delta bootstrap for {table_name!r} ended without an "
                f"@odata.deltaLink. The server may have aborted change "
                f"tracking. Set delta_tracking=disabled to fall back to "
                f"snapshot or cursor-based reads."
            )

        return iter(records), {"delta_link": new_delta_link}

    def _build_delta_record(self, item: dict, primary_keys: list[str]) -> dict:
        """Translate one delta payload entry into the emitted record shape.

        - ``@removed`` entries become tombstones: a record carrying the
          primary-key fields plus ``_deleted=True``.
        - Regular adds/changes pass through with all ``@odata.*`` control
          properties stripped and ``_deleted=False``.

        Every emitted record gets ``_lc_sequence`` — a strictly monotonic
        string — so apply_changes has a deterministic sequence_by column.
        """
        if "@removed" in item:
            record: dict = {pk: item.get(pk) for pk in primary_keys}
            record[_DELETED_COL] = True
        else:
            record = {k: v for k, v in item.items() if not k.startswith("@odata.")}
            record[_DELETED_COL] = False
        record[_SEQUENCE_COL] = _next_sequence()
        return record

    def _delta_initial_request(
        self,
        table_name: str,
        table_options: dict[str, str] | None,
        prev_delta_link: str | None,
        prev_next_link: str | None,
    ) -> tuple[str, dict[str, str] | None]:
        """Pick the first URL + headers for a delta read.

        ``next_link`` wins over ``delta_link`` when both are present —
        we were mid-pagination on a cap hit, finish that before
        consulting the prior change cursor.
        """
        if prev_next_link is not None:
            return prev_next_link, None
        if prev_delta_link is not None:
            return prev_delta_link, None
        segment_filters = _resolve_segment_filters(table_options, [table_name])
        return (
            self._build_url(table_name, table_options or {}, extra_filter=segment_filters.get(0)),
            {"Prefer": _DELTA_PREFER},
        )

    def _delta_walk_pages(
        self,
        *,
        url: str,
        initial_headers: dict[str, str] | None,
        is_bootstrap: bool,
        prev_delta_link: str | None,
        prev_next_link: str | None,
        table_name: str,
        table_options: dict[str, str] | None,
        primary_keys: list[str],
        max_records: int,
    ) -> tuple[list[dict], str | None, str | None, bool]:
        """Walk the ``@odata.nextLink`` chain until a deltaLink, cap, or 410.

        Returns ``(records, new_delta_link, carry_next_link, rebootstrap)``:

        * ``records`` — emitted records (already passed through
          :py:meth:`_build_delta_record`).
        * ``new_delta_link`` — the freshest ``@odata.deltaLink`` seen.
        * ``carry_next_link`` — set when ``max_records`` capped the read
          mid-pagination; the caller persists this in the offset.
        * ``rebootstrap`` — True iff a 410 fired on a stored link; the
          caller re-issues from a fresh empty offset.
        """
        session = self._get_session()
        records: list[dict] = []
        new_delta_link: str | None = None
        carry_next_link: str | None = None
        page_index = 0
        bootstrap_verified = not is_bootstrap
        sparse_checked = False
        current_url: str | None = url

        while current_url:
            headers = initial_headers if (page_index == 0 and initial_headers) else None
            kwargs: dict[str, Any] = {"headers": headers} if headers else {}
            resp = self._http_get(session, current_url, **kwargs)
            if resp.status_code == 410 and (prev_delta_link or prev_next_link):
                return [], None, None, True
            _raise_for_status_with_body(resp, current_url)

            if not bootstrap_verified:
                self._verify_delta_bootstrap(resp, table_name)
                bootstrap_verified = True

            payload = _decode_json_with_body(resp, current_url)
            sparse_checked = self._delta_collect_page_records(
                payload=payload,
                records=records,
                primary_keys=primary_keys,
                table_name=table_name,
                table_options=table_options,
                sparse_checked=sparse_checked,
                max_records=max_records,
            )
            current_url, new_delta_link, carry_next_link = self._delta_advance_links(
                payload=payload,
                resp_url=resp.url,
                records=records,
                max_records=max_records,
                new_delta_link=new_delta_link,
                carry_next_link=carry_next_link,
            )
            page_index += 1

        return records, new_delta_link, carry_next_link, False

    def _verify_delta_bootstrap(self, resp: requests.Response, table_name: str) -> None:
        """Confirm the server actually honored ``Prefer: odata.track-changes``."""
        applied = resp.headers.get("Preference-Applied", "")
        if _DELTA_PREFER not in applied.lower():
            raise RuntimeError(
                f"OData server did not honor 'Prefer: odata.track-changes' "
                f"for {table_name!r} (response missing 'Preference-Applied' "
                f"header). The probe in delta_tracking=auto should have "
                f"caught this — your service may have inconsistent support. "
                f"Set delta_tracking=disabled and use cursor-based "
                f"incremental instead."
            )

    def _delta_collect_page_records(
        self,
        *,
        payload: dict,
        records: list[dict],
        primary_keys: list[str],
        table_name: str,
        table_options: dict[str, str] | None,
        sparse_checked: bool,
        max_records: int,
    ) -> bool:
        """Append delta records from ``payload`` until cap or page exhaustion.

        Returns the updated ``sparse_checked`` flag so the caller can
        continue to skip the check on later pages once it's already
        passed for this call.
        """
        for item in payload.get("value", []):
            if not sparse_checked and "@removed" not in item:
                self._check_no_sparse_entity(item, table_name, table_options)
                sparse_checked = True
            records.append(self._build_delta_record(item, primary_keys))
            if len(records) >= max_records:
                break
        return sparse_checked

    def _delta_advance_links(
        self,
        *,
        payload: dict,
        resp_url: str,
        records: list[dict],
        max_records: int,
        new_delta_link: str | None,
        carry_next_link: str | None,
    ) -> tuple[str | None, str | None, str | None]:
        """Resolve the next URL + offset bookkeeping after one page.

        Returns ``(next_url, new_delta_link, carry_next_link)``.
        ``next_url`` is ``None`` when pagination should stop (either we
        hit the cap, saw a terminal deltaLink, or the server omitted
        both pagination links).
        """
        raw_delta = payload.get("@odata.deltaLink")
        raw_next = payload.get("@odata.nextLink")
        cap_hit = len(records) >= max_records

        if cap_hit:
            if raw_next:
                carry_next_link = urljoin(resp_url, raw_next)
            # Capture the deltaLink even on a cap-hit page — when the
            # cap lines up exactly with the terminal page we still want
            # the next-batch resume to follow ``delta_link`` rather
            # than re-walk the whole bootstrap.
            if raw_delta and new_delta_link is None:
                new_delta_link = urljoin(resp_url, raw_delta)
            return None, new_delta_link, carry_next_link

        if raw_delta:
            new_delta_link = urljoin(resp_url, raw_delta)
            return None, new_delta_link, carry_next_link
        if raw_next:
            return urljoin(resp_url, raw_next), new_delta_link, carry_next_link
        return None, new_delta_link, carry_next_link

    def _check_no_sparse_entity(
        self,
        item: dict,
        table_name: str,
        table_options: dict[str, str] | None,
    ) -> None:
        """Refuse silently-corrupting sparse delta responses.

        OData v4 §11.4 lets a delta payload return only the *changed*
        properties on an updated entity. That sounds harmless until you
        realize the connector emits the dict as-is to Spark, which
        treats absent fields as NULL — overwriting good destination
        values with nulls on every partial update. The damage is silent
        and not recoverable from the destination table alone.

        We can't safely apply partial updates in v1, so refuse them up
        front with an actionable error. Run only on the first
        non-tombstone entry per call.

        The expected key set is the declared schema for the table, less
        any selection imposed by ``$select`` and less the synthetic
        ``_deleted`` / ``_lc_sequence`` columns we add ourselves.
        """
        select = (table_options or {}).get("select")
        if select:
            expected = {c.strip() for c in select.split(",") if c.strip()}
        else:
            namespace = (table_options or {}).get("namespace")
            expected = {f.name for f in self._fields_for(table_name, namespace)}
        expected -= {_DELETED_COL, _SEQUENCE_COL}
        actual = {k for k in item.keys() if not k.startswith("@odata.")}
        missing = expected - actual
        if missing:
            raise RuntimeError(
                f"OData delta response for {table_name!r} returned a sparse "
                f"entity: missing properties {sorted(missing)}. The connector "
                f"cannot safely apply partial updates — every missing field "
                f"would write NULL at the destination, silently corrupting "
                f"data. Set delta_tracking=disabled to use cursor-based "
                f"incremental, or restrict the schema with $select to only "
                f"the fields the server always returns in delta payloads."
            )

    # ------------------------------------------------------------------
    # Delta tracking capability
    # ------------------------------------------------------------------

    def _delta_setting(self, table_options: dict[str, str] | None) -> str:
        """Resolve the delta_tracking option, normalised to lower case.

        Defaults to ``disabled``. Delta tracking is opt-in because most
        OData services don't honor ``Prefer: odata.track-changes``, and
        a default-``auto`` would burn one wasted HTTP probe per table
        per pipeline trigger on the common case where the user doesn't
        want this feature anyway.
        """
        raw = ((table_options or {}).get("delta_tracking") or "disabled").strip().lower()
        if raw not in {"auto", "enabled", "disabled"}:
            raise ValueError(
                f"Invalid delta_tracking={raw!r}. Expected one of: auto, enabled, disabled."
            )
        return raw

    def _delta_cache_key(
        self, table_name: str, table_options: dict[str, str] | None
    ) -> tuple[str, str]:
        """Cache key for :py:attr:`_delta_capable` keyed on (namespace, table).

        Namespace defaults to the empty string when omitted so multi-schema
        services with a single un-namespaced declaration also key cleanly.
        """
        namespace = (table_options or {}).get("namespace") or ""
        return (namespace, table_name)

    def _delta_active_for(self, table_name: str, table_options: dict[str, str] | None) -> bool:
        """Whether delta tracking is the read mode for this table.

        Resolution order:
          1. ``delta_tracking=disabled`` → never.
          2. ``cursor_field`` set + ``delta_tracking=enabled`` → ValueError
             (the two are mutually exclusive — delta tracking provides
             its own sequencing).
          3. ``cursor_field`` set + ``delta_tracking=auto`` → cursor wins;
             delta is left dormant, no probe.
          4. ``delta_tracking=enabled`` → assume support; a probe failure
             surfaces at read time rather than here.
          5. ``delta_tracking=auto`` → probe once, cache, decide.
        """
        setting = self._delta_setting(table_options)
        if setting == "disabled":
            return False
        if (table_options or {}).get("cursor_field"):
            if setting == "enabled":
                raise ValueError(
                    "delta_tracking=enabled is mutually exclusive with "
                    "cursor_field; the server-driven delta stream provides "
                    "its own sequencing. Remove cursor_field, or switch to "
                    "delta_tracking=disabled to use cursor-based incremental."
                )
            return False
        if setting == "enabled":
            return True
        key = self._delta_cache_key(table_name, table_options)
        if key not in self._delta_capable:
            self._delta_capable[key] = self._probe_delta_support(table_name, table_options)
        return self._delta_capable[key]

    def _probe_delta_support(self, table_name: str, table_options: dict[str, str] | None) -> bool:
        """Light-touch capability probe.

        Sends a small GET against the entity set with the
        ``Prefer: odata.track-changes`` header and inspects the response
        headers for ``Preference-Applied: odata.track-changes``. That
        header is the spec's positive acknowledgement that the server is
        honoring change tracking on this request.

        Returns ``False`` for every failure mode (non-200, missing
        header, malformed body, network error). The cache is populated
        with that ``False`` so we don't retry the probe per call — the
        connector falls back to whatever cursor/snapshot path the
        user's options imply.
        """
        # Force ``$top=1`` for the probe so the response stays small even
        # against entity sets with millions of rows. We only care about
        # headers.
        probe_options = {**(table_options or {}), "page_size": "1"}
        url = self._build_url(table_name, probe_options)
        try:
            session = self._get_session()
            resp = self._http_get(
                session,
                url,
                headers={"Prefer": _DELTA_PREFER},
            )
        except (requests.RequestException, ValueError, RuntimeError, PermissionError):
            return False
        if resp.status_code != 200:
            return False
        applied = resp.headers.get("Preference-Applied", "")
        return _DELTA_PREFER in applied.lower()

    # ------------------------------------------------------------------
    # URL + HTTP plumbing
    # ------------------------------------------------------------------

    def _build_url(
        self,
        table_name: str,
        table_options: dict[str, str],
        extra_filter: str | None = None,
        order_by: str | None = None,
    ) -> str:
        base = _join_url(self.service_url, table_name)
        return f"{base}?{self._format_query_params(table_options, extra_filter, order_by)}"

    def _format_query_params(
        self,
        table_options: dict[str, str],
        extra_filter: str | None = None,
        order_by: str | None = None,
    ) -> str:
        """Compose $top/$select/$filter/$orderby; shared across all URL builders."""
        opts = table_options or {}
        params = [f"$top={opts.get('page_size', '1000')}"]
        if opts.get("select"):
            params.append(f"$select={opts['select']}")
        filters = [f for f in (opts.get("filter"), extra_filter) if f]
        if filters:
            if len(filters) == 1:
                # A single clause goes on the wire as-is. Wrapping it
                # in parens would compound with any pre-wrapped clause
                # passed via ``extra_filter`` (e.g. a multi-source
                # ``combine_filters`` result), producing triple-paren
                # ``$filter=((A) and (B))`` shapes that are harder to
                # eyeball.
                params.append(f"$filter={filters[0]}")
            else:
                params.append(f"$filter={' and '.join(f'({f})' for f in filters)}")
        if order_by:
            params.append(f"$orderby={order_by}")
        return "&".join(params)

    def _fetch_pages(self, url: str) -> Iterator[dict]:
        """Walk @odata.nextLink, yielding raw JSON dicts (no coercion).

        The OData v4 spec allows @odata.nextLink to be either an absolute
        URL or a relative one (resolved against the request URL). Some
        services (e.g. SAP NetWeaver Gateway, certain self-hosted Olingo
        deployments) return just ``Customers?$skiptoken=...`` and rely on
        the client to prepend the service root. urljoin handles both
        cases — absolutes pass through unchanged.
        """
        for page_rows, _ in self._fetch_pages_with_links(url):
            yield from page_rows

    def _fetch_pages_with_links(self, url: str) -> Iterator[tuple[list[dict], str | None]]:
        """Page-aware variant of ``_fetch_pages``: yields
        ``(page_rows, next_url)`` for each HTTP response.

        Lets callers checkpoint at page boundaries — the yielded
        ``next_url`` is the resolved @odata.nextLink (or ``None`` when
        the chain is exhausted). Resuming from that link on a later
        call hands the server back its own opaque skiptoken; the server
        picks up exactly where it left off without the caller needing
        to reconstruct ``$filter``/``$orderby``/``$select`` state.
        """
        session = self._get_session()
        next_url: str | None = url
        while next_url:
            resp, payload = self._fetch_page_payload(session, next_url)
            page_rows = [
                {k: v for k, v in item.items() if not k.startswith("@odata.")}
                for item in payload.get("value", [])
            ]
            raw_next = payload.get("@odata.nextLink")
            new_next = urljoin(resp.url, raw_next) if raw_next else None
            yield page_rows, new_next
            next_url = new_next

    def _fetch_page_payload(
        self, session: requests.Session, url: str
    ) -> tuple[requests.Response, dict]:
        """GET one page + decode JSON, retrying on truncated/malformed
        response bodies.

        ``_http_get`` already retries on transport-layer transients
        (TCP reset, timeout, 429/503). Some upstream sources additionally
        emit **200 responses with corrupt JSON bodies** under load —
        observed with Hexagon SCApi, which sometimes truncates response
        bodies mid-serialization for large contained-collection
        responses. Each outer attempt issues a fresh ``_http_get``, so
        the retry composes cleanly with the transport-layer retries
        already inside ``_http_get``. After ``max_retries`` exhausted
        JSON decode attempts, raises the enriched JSONDecodeError with
        the URL + truncated body in the message so the operator can
        escalate to the upstream owner.
        """
        attempts = self.max_retries + 1
        for attempt in range(attempts):
            resp = self._http_get(session, url)
            _raise_for_status_with_body(resp, url)
            try:
                return resp, _decode_json_with_body(resp, url)
            except json.JSONDecodeError:
                if attempt >= self.max_retries:
                    raise
                time.sleep(self._backoff_delay(attempt))
        # Defensive: the loop above always returns or raises.
        raise RuntimeError(  # pragma: no cover
            f"Exhausted retries decoding JSON for {url!r}."
        )

    def _http_get(self, session: requests.Session, url: str, **kwargs: Any) -> requests.Response:
        """GET with auth-aware 401/403 handling + transient-failure retry.

        Outer loop retries on two classes of transient failure, both
        capped by ``retry_max_delay_seconds`` per attempt:

        * **HTTP 429 / 503** — throttling or service unavailable.
          Honours the ``Retry-After`` header when present (integer
          seconds or HTTP-date), otherwise exponential backoff
          (1, 2, 4, 8, 16 s …). After ``max_retries`` attempts, raises
          :class:`RuntimeError` with the last response truncated into
          the message.
        * **Connection-level exceptions** —
          :class:`requests.ConnectionError`,
          :class:`requests.Timeout`,
          :class:`requests.ChunkedEncodingError`. The server didn't
          finish sending an HTTP response (TCP reset, remote disconnect,
          read/connect timeout, half-closed mid-body), so there's no
          ``Retry-After`` to honour — pure exponential backoff. After
          ``max_retries`` attempts, re-raises the original exception
          type with the attempt count appended; ``__cause__`` preserves
          the original traceback for triage.

        Inner per-attempt logic (see ``_http_get_once``):

        1. **Pre-emptive token refresh** — when the OAuth ``expires_in``
           clock is past the recorded deadline (60 s safety buffer),
           swap the bearer header *before* sending. Avoids a wasted
           round-trip on long paginated reads straddling an expiry
           boundary.
        2. **Reactive token refresh** — 401 from the source + OAuth
           refresh path is available → mint a fresh token and retry
           once. A second 401 means the access token reached the server
           but the principal lacks access (raise
           :class:`PermissionError` immediately, no further retry).
        3. **Actionable no-refresh-path failure** — 401 or 403 with no
           automatic refresh configured (bearer, basic, api_key, or
           OAuth without a refresh-issuing token endpoint). Raise
           :class:`PermissionError` whose message names the specific
           connection options the operator should check.

        Retries happen between attempts of the inner logic, so a token
        refresh and a throttle backoff compose cleanly: refresh →
        request → 429 → sleep → next attempt's pre-emptive refresh
        check picks up where we left off.
        """
        attempts = self.max_retries + 1
        for attempt in range(attempts):
            try:
                resp = self._http_get_once(session, url, **kwargs)
            except _TRANSIENT_NETWORK_ERRORS as exc:
                # Server closed the TCP connection / DNS failed / read
                # timed out — no HTTP response, so no Retry-After to
                # consult. Pure exponential backoff. Preserve the
                # original exception type so callers that catch
                # ConnectionError specifically still match.
                if attempt >= self.max_retries:
                    raise type(exc)(
                        f"{exc} (after {attempt + 1} attempts on {url!r}; "
                        f"raise 'max_retries' on the connection if the "
                        f"source needs more retries)"
                    ) from exc
                time.sleep(self._backoff_delay(attempt))
                continue
            if resp.status_code in (429, 503):
                if attempt >= self.max_retries:
                    raise RuntimeError(self._throttle_exhausted_error(resp, url, attempt + 1))
                time.sleep(self._retry_after_delay(resp, attempt))
                continue
            return resp
        # Defensive: the loop always returns or raises before exiting.
        raise RuntimeError(  # pragma: no cover
            f"Exhausted retries for {url!r} without producing a response."
        )

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff capped at ``retry_max_delay_seconds``.

        Used for transient network failures where the server never
        sent a response, so there's no ``Retry-After`` to honour (the
        429/503 path prefers the server hint via
        ``_retry_after_delay``).
        """
        return min(float(2**attempt), float(self.retry_max_delay_seconds))

    def _http_get_once(
        self, session: requests.Session, url: str, **kwargs: Any
    ) -> requests.Response:
        """One auth-aware GET attempt; throttle handling lives in `_http_get`."""
        if self._should_preemptively_refresh():
            session.headers["Authorization"] = f"Bearer {self._oauth2_token()}"
        resp = session.get(url, timeout=self.timeout, **kwargs)
        if resp.status_code == 401 and self._has_oauth_refresh_path():
            session.headers["Authorization"] = f"Bearer {self._oauth2_token()}"
            resp = session.get(url, timeout=self.timeout, **kwargs)
            if resp.status_code == 401:
                # We just minted a token straight from the OAuth provider
                # and the source still rejected it — the access token isn't
                # the problem. Most likely the principal lacks read access
                # to this entity set, the scope is insufficient, or the
                # tenant is mis-mapped. Surface that explicitly so the user
                # doesn't chase a non-existent token issue.
                raise PermissionError(
                    f"OData service returned 401 for {url!r} even after "
                    f"refreshing the OAuth2 access token. The new token "
                    f"reached the server, so the access token itself is "
                    f"not the problem. Check that the OAuth principal has "
                    f"read access to this entity set, that 'oauth2_scope' "
                    f"grants the right permissions, and that any "
                    f"tenant/instance identifier in 'service_url' or "
                    f"'extra_headers' matches the credentials. Server "
                    f"response: {_truncate(resp.text, 300)}"
                )
            return resp
        if resp.status_code in (401, 403):
            raise PermissionError(self._no_refresh_auth_error(resp, url))
        return resp

    def _retry_after_delay(self, resp: requests.Response, attempt: int) -> float:
        """Pick the sleep duration before the next retry.

        Priority:
          1. ``Retry-After`` header — integer seconds or HTTP-date.
          2. Exponential backoff: ``2**attempt`` seconds (1, 2, 4, 8,
             16 …).

        Either way the value is capped at ``retry_max_delay_seconds``.
        """
        cap = float(self.retry_max_delay_seconds)
        header = resp.headers.get("Retry-After")
        if header is not None:
            parsed = _parse_retry_after(header)
            if parsed is not None:
                return min(parsed, cap)
        return min(float(2**attempt), cap)

    def _throttle_exhausted_error(self, resp: requests.Response, url: str, attempts: int) -> str:
        """Message for the post-retry-budget RuntimeError."""
        retry_after = resp.headers.get("Retry-After", "<none>")
        return (
            f"OData service returned {resp.status_code} for {url!r} after "
            f"{attempts} attempts (server is throttling or temporarily "
            f"unavailable). Last Retry-After header: {retry_after}. "
            f"Raise 'max_retries' (current: {self.max_retries}) or "
            f"'retry_max_delay_seconds' (current: {self.retry_max_delay_seconds}) "
            f"on the connection if the source needs longer cooldowns; "
            f"reduce read concurrency via the per-table 'num_partitions' "
            f"option if the throttle is concurrency-driven. Server "
            f"response: {_truncate(resp.text, 300)}"
        )

    def _no_refresh_auth_error(self, resp: requests.Response, url: str) -> str:
        """Construct an actionable auth-failure message for the no-refresh path.

        Different auth modes have very different failure modes — bearer
        tokens expire, OAuth scopes can be too narrow, basic creds rot —
        and bundling them all into one generic "401 Unauthorized" makes
        triage from a pipeline log nearly impossible. This method picks
        the relevant remediation hints based on which auth mode is
        active on the connection.
        """
        status = resp.status_code
        body = _truncate(resp.text, 300) or "(empty body)"
        auth = (self.options.get("auth_type") or "").lower().strip()
        if not auth and self.options.get("token"):
            auth = "bearer"
        prefix = (
            f"OData service returned {status} for {url!r} and no "
            f"automatic token-refresh path is configured. "
        )
        if auth == "bearer":
            return (
                f"{prefix}"
                f"With auth_type=bearer the pre-acquired access token cannot "
                f"be refreshed by the connector — either it has expired "
                f"(typical lifetime ~1 h), or the principal that issued it "
                f"lacks read access to this entity set. Fixes: replace "
                f"'token' on the connection with a fresh one; or switch to "
                f"auth_type=oauth2 with 'oauth2_client_id' + "
                f"'oauth2_client_secret' so the connector mints and "
                f"refreshes tokens automatically. For Microsoft Graph "
                f"high-privilege endpoints (identityProviders, auditLogs, "
                f"etc.), ensure the token carries the required scope and "
                f"admin consent. Server response: {body}"
            )
        if auth == "basic":
            return (
                f"{prefix}"
                f"With auth_type=basic the credentials are sent on every "
                f"request unchanged. Check 'username' and 'password' on "
                f"the connection — the password may have expired or been "
                f"rotated — and confirm the user has read access to this "
                f"entity set at the source. Server response: {body}"
            )
        if auth == "api_key":
            return (
                f"{prefix}"
                f"With auth_type=api_key the key is sent on every request "
                f"unchanged. Check 'api_key' (may have been rotated or "
                f"revoked) and 'api_key_header' (some services expect a "
                f"non-default header name). Confirm the key's scope "
                f"includes this entity set. Server response: {body}"
            )
        if auth == "oauth2":
            return (
                f"{prefix}"
                f"With auth_type=oauth2 but no refresh path available "
                f"(missing 'oauth2_client_id' / 'oauth2_client_secret' "
                f"for client-credentials, and no 'oauth2_refresh_token' "
                f"for user-flow refresh), the connector can't mint a "
                f"fresh access token. Provide one of those pairs, or "
                f"replace 'oauth2_access_token' with a fresh value. "
                f"Also confirm 'oauth2_scope' grants read on this entity "
                f"set. Server response: {body}"
            )
        return (
            f"{prefix}"
            f"No authentication is configured on this connection but the "
            f"OData service requires it. Set 'auth_type' to one of: "
            f"bearer, basic, api_key, oauth2 (with the matching parameter "
            f"set). Server response: {body}"
        )

    def _should_preemptively_refresh(self) -> bool:
        """True iff a known-expiry token has hit its 60 s safety window."""
        if self._access_token_expires_at is None:
            return False
        return time.monotonic() >= self._access_token_expires_at

    def _has_oauth_refresh_path(self) -> bool:
        """True iff `_oauth2_token()` can mint a fresh access token.

        Either a refresh token is on hand (user flow) or
        ``oauth2_client_id`` + ``oauth2_client_secret`` are present
        for the client-credentials grant.
        """
        if self.options.get("oauth2_refresh_token"):
            return True
        return bool(
            self.options.get("oauth2_client_id") and self.options.get("oauth2_client_secret")
        )

    # ------------------------------------------------------------------
    # Auth session
    # ------------------------------------------------------------------

    def _get_session(self) -> requests.Session:
        if self._session is not None:
            return self._session

        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/json",
                "OData-Version": "4.0",
                "OData-MaxVersion": "4.0",
            }
        )
        extra_headers = self.options.get("extra_headers")
        if extra_headers:
            for pair in extra_headers.split(","):
                if ":" in pair:
                    k, v = pair.split(":", 1)
                    session.headers[k.strip()] = v.strip()

        auth_type = (self.options.get("auth_type") or "").lower().strip()
        if not auth_type and self.options.get("token"):
            auth_type = "bearer"

        if auth_type == "bearer":
            session.headers["Authorization"] = f"Bearer {_require(self.options, 'token')}"
        elif auth_type == "basic":
            session.auth = HTTPBasicAuth(
                _require(self.options, "username"),
                _require(self.options, "password"),
            )
        elif auth_type == "api_key":
            header = self.options.get("api_key_header", "x-api-key")
            session.headers[header] = _require(self.options, "api_key")
        elif auth_type == "oauth2":
            # Two sub-modes share this branch:
            #  * **User flow** — `oauth2_refresh_token` is set. A
            #    pre-supplied `oauth2_access_token` is used as-is if
            #    present (avoids an unnecessary round-trip); otherwise
            #    `_oauth2_token()` runs the refresh-token grant to
            #    mint one. Expired tokens mid-run are caught in
            #    `_http_get` and refreshed once.
            #  * **Client-credentials flow** — no refresh token; the
            #    connector mints a fresh access token via
            #    `client_credentials` at session start.
            initial_token = self.options.get("oauth2_access_token") or self._oauth2_token()
            session.headers["Authorization"] = f"Bearer {initial_token}"
        elif auth_type:
            raise ValueError(
                f"Unknown auth_type {auth_type!r}. "
                f"Expected one of: bearer, basic, api_key, oauth2."
            )

        self._session = session
        return session

    def _oauth2_token(self) -> str:
        """Mint an OAuth2 access token.

        Picks the grant type from what's available in `self.options`:
          * `oauth2_refresh_token` present -> `refresh_token` grant
            (user-flow refresh). Client id/secret are required so the
            token endpoint can authenticate the client.
          * Otherwise -> `client_credentials` grant (server-to-server).

        Some providers issue a rotated refresh token in the response;
        when that happens, the new value is written back into
        `self.options` so the next refresh uses it.
        """
        refresh_token = self.options.get("oauth2_refresh_token")
        if refresh_token:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": _require(self.options, "oauth2_client_id"),
                "client_secret": _require(self.options, "oauth2_client_secret"),
            }
        else:
            data = {
                "grant_type": "client_credentials",
                "client_id": _require(self.options, "oauth2_client_id"),
                "client_secret": _require(self.options, "oauth2_client_secret"),
            }
        scope = self.options.get("oauth2_scope")
        if scope:
            data["scope"] = scope
        token_url = _require(self.options, "oauth2_token_url")
        resp = requests.post(
            token_url,
            data=data,
            timeout=self.timeout,
        )
        # Surface a precise, actionable error when the token endpoint
        # itself rejects the request. raise_for_status() would otherwise
        # produce a terse "401 Client Error: Unauthorized for url ..."
        # that doesn't tell the user *which* credential is the problem.
        if resp.status_code in (400, 401):
            grant = data["grant_type"]
            hint = _extract_oauth_error_hint(resp)
            if grant == "refresh_token":
                raise ValueError(
                    f"OAuth2 token endpoint returned {resp.status_code} when "
                    f"refreshing the access token. The refresh token may be "
                    f"expired, revoked, or paired with a different OAuth "
                    f"client. Check that 'oauth2_refresh_token' was issued by "
                    f"the same 'oauth2_client_id' configured on this "
                    f"connection, and re-run the authorization-code flow if "
                    f"needed. Server response: {hint}"
                ) from None
            raise ValueError(
                f"OAuth2 token endpoint returned {resp.status_code} for the "
                f"client_credentials grant. Check 'oauth2_client_id', "
                f"'oauth2_client_secret', 'oauth2_token_url', and "
                f"'oauth2_scope' on this connection. Server response: {hint}"
            ) from None
        resp.raise_for_status()
        payload = _decode_json_with_body(resp, token_url)
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("OAuth2 token endpoint did not return access_token.")
        rotated_refresh = payload.get("refresh_token")
        if rotated_refresh:
            self.options["oauth2_refresh_token"] = rotated_refresh
        # Track wall-clock deadline so `_http_get` can refresh the token
        # *before* the source returns 401. Subtract a 60 s safety margin
        # to cover clock skew + in-flight request latency. Absent
        # `expires_in` means the provider didn't tell us — fall back to
        # the lazy 401-retry path.
        expires_in = payload.get("expires_in")
        if expires_in is not None:
            try:
                self._access_token_expires_at = time.monotonic() + int(expires_in) - 60
            except (TypeError, ValueError):
                self._access_token_expires_at = None
        else:
            self._access_token_expires_at = None
        return token

    # ------------------------------------------------------------------
    # $metadata caching + parsing
    # ------------------------------------------------------------------

    def _metadata_root(self) -> ET.Element:
        """Convenience accessor — returns the parsed root from the
        cached bundle. Most callers want lookups against the index;
        reach for ``self._metadata_state()`` directly when so."""
        return self._metadata_state().root

    def _metadata_state(self) -> _MetadataState:
        """Return the bundled parsed-CSDL state for this instance,
        fetching + parsing + indexing on first call only.

        Four cache layers, checked in order:

        1. Instance ``self._metadata`` — re-used across every downstream
           lookup; the per-instance memos hang off this bundle so all
           the lookup methods see the same cached root + index.
        2. Module ``_METADATA_CACHE`` keyed by ``service_url`` — shared
           across all connector instances in the same Python process.
           Stores ``(xml_text, root, index)`` so the index isn't
           rebuilt per instance either.
        3. On-disk pickle at ``_metadata_cache_path(service_url)`` —
           shared across forked ``pyspark.daemon`` workers (PySpark
           forks one per ``.load()`` schema inference). The pickle
           stores ``(xml_text, root)``; each fork rebuilds the index
           from the unpickled root (one tree walk, ~50 ms).
        4. Network — the actual ``GET $metadata``, taken only when no
           cache has it.
        """
        if self._metadata is not None:
            return self._metadata
        cached = _METADATA_CACHE.get(self.service_url)
        if cached is not None:
            xml_text, root, index = cached
            self._metadata = _MetadataState(root=root, index=index)
            # ``xml_text`` is only needed for the write path; once
            # cached, we don't carry it on the bundle.
            del xml_text
            return self._metadata
        file_cached = self._read_metadata_file_cache()
        if file_cached is not None:
            xml_text, root = file_cached
            index = _build_csdl_index(root)
            self._metadata = _MetadataState(root=root, index=index)
            _METADATA_CACHE[self.service_url] = (xml_text, root, index)
            return self._metadata
        session = self._get_session()
        url = _join_url(self.service_url, "$metadata")
        resp = self._http_get(session, url, headers={"Accept": "application/xml"})
        _raise_for_status_with_body(resp, url)
        xml_text = resp.text
        root = ET.fromstring(xml_text)
        index = _build_csdl_index(root)
        self._metadata = _MetadataState(root=root, index=index)
        _METADATA_CACHE[self.service_url] = (xml_text, root, index)
        self._write_metadata_file_cache(xml_text, root)
        return self._metadata

    def _read_metadata_file_cache(self) -> tuple[str, ET.Element] | None:
        """Return the cached ``(xml_text, parsed_root)`` from the
        on-disk pickle if it exists and is within the TTL. Returns
        ``None`` for any miss (missing, expired, unreadable,
        unpicklable). All failures are silent — the caller falls
        through to the network."""
        if self.metadata_cache_ttl_seconds <= 0:
            return None
        path = _metadata_cache_path(self.service_url)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return None
        if time.time() - mtime > self.metadata_cache_ttl_seconds:
            return None
        try:
            with open(path, "rb") as fh:
                payload = pickle.load(fh)
        except (OSError, pickle.UnpicklingError, EOFError, ValueError):
            return None
        # Defensive shape check — a corrupt or wrong-shape pickle
        # shouldn't crash the connector.
        if (
            not isinstance(payload, tuple)
            or len(payload) != 2
            or not isinstance(payload[0], str)
            or not isinstance(payload[1], ET.Element)
        ):
            return None
        return payload

    def _write_metadata_file_cache(self, xml_text: str, root: ET.Element) -> None:
        """Best-effort write of ``(xml_text, parsed_root)`` to the
        on-disk pickle. Uses atomic rename so a concurrent reader
        either sees the old file or the fully-written new one, never
        a torn write."""
        if self.metadata_cache_ttl_seconds <= 0:
            return
        path = _metadata_cache_path(self.service_url)
        tmp_path = f"{path}.{os.getpid()}.tmp"
        try:
            with open(tmp_path, "wb") as fh:
                pickle.dump((xml_text, root), fh, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp_path, path)
        except (OSError, pickle.PicklingError):
            # File cache is purely an optimization. If anything goes
            # wrong (read-only tempdir, disk full, pickling failure)
            # the connector still works — just slower.
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    def _entity_set_index(self) -> list[tuple[str, str]]:
        """All (schema_namespace, entity_set_name) pairs declared in $metadata."""
        return self._metadata_state().index.entity_set_pairs

    def _entity_type_for(self, table_name: str, namespace: str | None = None) -> ET.Element:
        """Resolve flat names or contained paths (segment-by-segment via
        contained nav props on the base-type chain)."""
        state = self._metadata_state()
        cache_key = (table_name, namespace)
        cached = state.entity_type.get(cache_key)
        if cached is not None:
            return cached
        segments = _parse_contained_path(table_name) or [table_name]
        et = self._flat_entity_type_for(segments[0], namespace)
        for idx, child_segment in enumerate(segments[1:], start=1):
            nav_props = self._all_contained_nav_props(et)
            target_ref = next((r for n, r in nav_props if n == child_segment), None)
            if target_ref is None:
                walked = _CONTAINED_PATH_SEP.join(segments[:idx])
                raise ValueError(
                    f"{child_segment!r} is not a contained-collection navigation "
                    f"property on {walked!r}. Available contained collections: "
                    f"{sorted(n for n, _ in nav_props)}"
                )
            target_et = self._resolve_type_ref(target_ref)
            if target_et is None:
                raise ValueError(
                    f"Contained navigation target {target_ref!r} (referenced by "
                    f"{child_segment!r} on {segments[idx - 1]!r}) not found in $metadata."
                )
            et = target_et
        state.entity_type[cache_key] = et
        return et

    def _flat_entity_type_for(self, table_name: str, namespace: str | None = None) -> ET.Element:
        """Resolve a top-level entity-set name to its EntityType element."""
        index = self._metadata_state().index
        candidates = index.entity_set_by_name.get(table_name) or []
        if namespace is not None:
            matches = [(ns, ref) for ns, ref in candidates if ns == namespace]
        else:
            matches = list(candidates)
        if not matches:
            if namespace is not None:
                hint = sorted(index.entity_set_names_by_ns.get(namespace, []))
                if not hint:
                    # The requested namespace has zero entity sets — common
                    # confusion when the user picks a type-only schema
                    # (e.g. one declaring BaseType references) instead of
                    # the schema whose <EntityContainer> declares the sets.
                    raise ValueError(
                        f"Entity set {table_name!r} not found in namespace "
                        f"{namespace!r}. Namespace {namespace!r} declares "
                        f"no entity sets (probably a type-only schema). "
                        f"Namespaces with entity sets: {sorted(index.namespaces_with_sets)}."
                    )
                raise ValueError(
                    f"Entity set {table_name!r} not found in namespace "
                    f"{namespace!r}. Available in this namespace: {hint}"
                )
            raise ValueError(
                f"Entity set {table_name!r} not found in $metadata. "
                f"Available: {sorted({n for _, n in index.entity_set_pairs})}"
            )
        if len(matches) > 1:
            namespaces = sorted({m[0] for m in matches})
            raise ValueError(
                f"Entity set {table_name!r} is declared in multiple namespaces: "
                f"{namespaces}. Set 'namespace' in table_options to disambiguate."
            )
        schema_ns, type_ref = matches[0]
        et = self._resolve_type_ref(type_ref)
        if et is None:
            raise ValueError(
                f"EntityType {type_ref!r} (referenced by entity set "
                f"{table_name!r} in schema {schema_ns!r}) not found in $metadata."
            )
        return et

    def _schema_alias_map(self) -> dict[str, str]:
        """``{namespace_or_alias → canonical_namespace}``.

        Kept as a public-shape method so callers reading the source
        for OData spec context still find it; internally it's a
        direct index lookup. CSDL allows each ``<Schema>`` to declare
        both a ``Namespace`` and a shorter ``Alias``; downstream
        ``BaseType`` / ``EntityType`` references can use either.
        Microsoft Graph for instance declares
        ``Namespace="microsoft.graph" Alias="graph"`` and then writes
        ``BaseType="graph.directoryObject"``.
        """
        return self._metadata_state().index.alias_to_namespace

    def _resolve_type_ref(self, type_ref: str) -> ET.Element | None:
        """Find the ``<EntityType>`` element for a qualified type reference.

        Accepts both fully-qualified namespace references
        (``microsoft.graph.user``) and alias-based references
        (``graph.user``). Returns ``None`` if no matching declaration
        exists — callers decide whether that's a hard error or worth
        falling back to a shallower lookup.
        """
        if "." not in type_ref:
            return None
        prefix, type_name = type_ref.rsplit(".", 1)
        index = self._metadata_state().index
        target_ns = index.alias_to_namespace.get(prefix)
        if target_ns is None:
            return None
        return index.entity_type_by_qname.get(f"{target_ns}.{type_name}")

    def _resolve_base_chain(self, et: ET.Element) -> list[ET.Element]:
        """Walk the ``BaseType`` chain starting at ``et``.

        Returns ``[et, parent, grandparent, …]`` until ``BaseType`` is
        absent or unresolvable. OData v4 §8.4: derived entity types
        inherit Keys and Properties from their base. Real-world
        services (Microsoft Graph, Microsoft Dataverse, most SAP
        deployments) lean on this heavily — Graph's ``user`` extends
        ``directoryObject`` extends ``entity``, and ``entity`` is the
        type that actually declares ``<Key>id</Key>``.

        Cycles are guarded against (cyclic CSDL is malformed but won't
        crash the connector).
        """
        state = self._metadata_state()
        cache_key = id(et)
        cached = state.base_chain.get(cache_key)
        if cached is not None:
            return cached
        chain = [et]
        current = et
        seen: set[str] = set()
        while True:
            base_ref = current.get("BaseType")
            if not base_ref or base_ref in seen:
                break
            seen.add(base_ref)
            parent = self._resolve_type_ref(base_ref)
            if parent is None:
                break
            chain.append(parent)
            current = parent
        state.base_chain[cache_key] = chain
        return chain

    def _fields_for(self, table_name: str, namespace: str | None = None) -> list[StructField]:
        state = self._metadata_state()
        cache_key = (table_name, namespace)
        cached = state.fields.get(cache_key)
        if cached is not None:
            return cached
        segments = _parse_contained_path(table_name) or [table_name]
        own_fields = self._own_fields_for_et(self._entity_type_for(table_name, namespace))
        if len(segments) == 1:
            state.fields[cache_key] = own_fields
            return own_fields
        # Every non-leaf ancestor contributes FK columns (OData v4
        # §13.4.3 — contained-entity keys are unique within parent only).
        fk_columns = self._resolve_fk_columns(segments, namespace)
        fk_fields: list[StructField] = []
        for idx in range(len(segments) - 1):
            seg = segments[idx]
            if not any(k[0] == seg for k in fk_columns):
                continue
            ancestor_et = self._entity_type_for(
                _CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
            )
            own = {f.name: f.dataType for f in self._own_fields_for_et(ancestor_et)}
            for pk in self._own_primary_keys_for_et(ancestor_et):
                fk_fields.append(
                    StructField(
                        fk_columns[(seg, pk)],
                        own.get(pk, StringType()),
                        False,
                    )
                )
        result = fk_fields + own_fields
        state.fields[cache_key] = result
        return result

    def _own_fields_for_et(self, et: ET.Element) -> list[StructField]:
        """Property fields on ``et`` and its base chain. Walks root → leaf
        so inherited fields appear before leaf's own additions; de-dupes
        by name with closest-to-root winning (spec forbids redeclaration)."""
        state = self._metadata_state()
        cache_key = id(et)
        cached = state.own_fields.get(cache_key)
        if cached is not None:
            return cached
        fields: list[StructField] = []
        seen: set[str] = set()
        for type_el in reversed(self._resolve_base_chain(et)):
            for prop in type_el.findall(f"{_NS_EDM}Property"):
                name = prop.get("Name")
                if name in seen:
                    continue
                seen.add(name)
                fields.append(
                    StructField(
                        name,
                        _EDM_TO_SPARK.get(prop.get("Type", "Edm.String"), StringType()),
                        prop.get("Nullable", "true").lower() != "false",
                    )
                )
        state.own_fields[cache_key] = fields
        return fields

    def _primary_keys_for(self, table_name: str, namespace: str | None = None) -> list[str]:
        state = self._metadata_state()
        cache_key = (table_name, namespace)
        cached = state.primary_keys.get(cache_key)
        if cached is not None:
            return cached
        segments = _parse_contained_path(table_name) or [table_name]
        leaf_pks = self._own_primary_keys_for_et(self._entity_type_for(table_name, namespace))
        if len(segments) == 1:
            state.primary_keys[cache_key] = leaf_pks
            return leaf_pks
        # Composite: every ancestor's FK columns + leaf's own PKs.
        fk_columns = self._resolve_fk_columns(segments, namespace)
        composite: list[str] = []
        for idx in range(len(segments) - 1):
            seg = segments[idx]
            if not any(k[0] == seg for k in fk_columns):
                continue
            ancestor_et = self._entity_type_for(
                _CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
            )
            for pk in self._own_primary_keys_for_et(ancestor_et):
                composite.append(fk_columns[(seg, pk)])
        composite.extend(leaf_pks)
        state.primary_keys[cache_key] = composite
        return composite

    def _own_primary_keys_for_et(self, et: ET.Element) -> list[str]:
        """Primary-key property names (OData v4 §8.4: derived types inherit
        Keys; closest-to-leaf Key wins where multiple levels redeclare)."""
        state = self._metadata_state()
        cache_key = id(et)
        cached = state.own_pks.get(cache_key)
        if cached is not None:
            return cached
        result: list[str] = []
        for type_el in self._resolve_base_chain(et):
            key = type_el.find(f"{_NS_EDM}Key")
            if key is not None:
                result = [ref.get("Name") for ref in key.findall(f"{_NS_EDM}PropertyRef")]
                break
        state.own_pks[cache_key] = result
        return result

    # ------------------------------------------------------------------
    # Cursor filter formatting
    # ------------------------------------------------------------------

    def _cursor_filter(self, cursor_field: str, since: Any) -> str | None:
        """Build the `$filter` clause for an incremental fetch.

        Strict `cursor gt since` once the offset has advanced; `None` on
        the very first call so the server returns the natural start of
        the table. `max_records_per_batch` is the per-call cap — there
        is no wall-clock ceiling, which is what makes continuous polling
        work and what keeps the connector type-agnostic over the cursor
        column.
        """
        if since is None:
            return None
        return f"{cursor_field} gt {_odata_literal(since)}"


# ---------------------------------------------------------------------------
# Helpers (module-level, no class state — easier to unit-test)
# ---------------------------------------------------------------------------


def _extract_oauth_error_hint(resp: requests.Response) -> str:
    """Pull the most informative error description out of an OAuth2 response.

    Token endpoints conventionally return JSON with ``error`` (machine code,
    e.g. ``invalid_grant``) and often ``error_description`` (human-readable).
    Fall back to the raw body when the response isn't JSON, and truncate so
    we never dump a 50 KB error page into a user-facing message.
    """
    try:
        payload = resp.json()
    except ValueError:
        return _truncate(resp.text, 300) or "Unauthorized"
    if isinstance(payload, dict):
        description = payload.get("error_description")
        code = payload.get("error")
        if description and code:
            return f"{code}: {description}"
        if description:
            return str(description)
        if code:
            return str(code)
    return _truncate(resp.text, 300) or "Unauthorized"


def _parse_retry_after(header: str) -> float | None:
    """Parse an HTTP ``Retry-After`` header into seconds.

    Accepts the two formats RFC 7231 §7.1.3 allows:

      * Integer seconds (``"30"``) — most APIs (Microsoft Graph,
        Dataverse, S/4HANA Cloud) emit this.
      * HTTP-date (``"Wed, 21 Oct 2026 07:28:00 GMT"``) — older
        spec-conformant servers.

    Returns ``None`` for anything unparseable; the caller falls back
    to exponential backoff. Past-dated HTTP-dates clamp to 0 (retry
    immediately) rather than returning a negative sleep.
    """
    header = header.strip()
    try:
        seconds = float(header)
    except (TypeError, ValueError):
        seconds = None
    if seconds is not None:
        return max(0.0, seconds)
    try:
        dt = parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = (dt - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delta)


def _decode_json_with_body(resp: requests.Response, url: str):
    """Like ``resp.json()`` but enrich the error with URL + truncated
    body when the decoder fails.

    The bare ``requests.exceptions.JSONDecodeError`` exposes only the
    Python parser's "Expecting … at line X column Y" message —
    useless for diagnosing a source that returned a truncated or
    non-JSON body (an HTML error page, a partial response under load,
    an upstream proxy intercept, etc.). This wrapper catches the
    decoder error and re-raises with the offending URL and the first
    1000 chars of the body baked into the message, mirroring the
    pattern ``_raise_for_status_with_body`` uses for 4xx/5xx
    responses.

    Preserves the original exception type so any callers catching
    ``JSONDecodeError`` (or its ``requests`` subclass) specifically
    still match.
    """
    try:
        return resp.json()
    except json.JSONDecodeError as exc:
        body = _truncate((resp.text or "").strip(), 1000) or "(empty body)"
        msg = f"{exc.msg} (parsing response for url: {url}). Server response body: {body}"
        raise type(exc)(msg, exc.doc, exc.pos) from exc


def _raise_for_status_with_body(resp: requests.Response, url: str) -> None:
    """Like ``resp.raise_for_status()`` but include the response body
    in the exception message.

    The bare ``requests.HTTPError`` message is just "400 Client Error:
    Bad Request for url ..." — useless for diagnosing OData services
    that put the actual error reason in the response body (e.g.
    ``{"error": {"message": "Page size 1000 exceeds maximum 500"}}``).
    Without the body, every 4xx from the source is opaque.

    Preserves the original :class:`requests.HTTPError` type so any
    callers catching that class specifically still match. The
    enriched message is what gets shown to operators in pipeline
    logs.
    """
    if resp.status_code < 400:
        return
    # Mirror requests' own format for the leading line so log filters
    # keyed off "<status> Client Error" / "Server Error" keep working.
    reason = resp.reason or ""
    family = "Client Error" if resp.status_code < 500 else "Server Error"
    body = _truncate((resp.text or "").strip(), 1000) or "(empty body)"
    msg = f"{resp.status_code} {family}: {reason} for url: {url}. " f"Server response body: {body}"
    raise requests.HTTPError(msg, response=resp)


def _truncate(text: str, limit: int) -> str:
    """Cap a string at ``limit`` chars with a trailing ellipsis when clipped."""
    if text is None:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _require(options: dict[str, str], key: str) -> str:
    val = options.get(key)
    if not val:
        raise ValueError(f"Required option {key!r} is missing.")
    return val


# Re-export base64/binary helper for any downstream caller that wants
# to materialize Edm.Binary fields into Python bytes prior to Spark.
def _decode_binary(value: str) -> bytes:
    return base64.b64decode(value)
