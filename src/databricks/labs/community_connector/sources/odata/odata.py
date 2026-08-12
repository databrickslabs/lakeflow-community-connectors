"""OData v4 community connector for Lakeflow Connect.

Implements the LakeflowConnect interface for any OData v4 service. The
connector discovers tables and schemas from the service's ``$metadata``
endpoint, supports static auth (bearer / basic / api_key) plus
UC-managed OAuth (the COMMUNITY connection runs the flow, refreshes
server-side, and injects ``access_token`` at query time), and ingests
each entity set either as a snapshot, an incremental CDC stream keyed
off a user-supplied cursor field, or (when the service supports it) a
server-driven delta query stream.

Connection options (set on the UC connection):
    service_url   required   OData service root, e.g.
                             https://services.odata.org/V4/Northwind/Northwind.svc/
    auth_type     optional   bearer | basic | api_key (omit for the
                             UC-managed OAuth connection mode)
    token, username, password, api_key, api_key_header
    access_token  runtime-injected by a UC COMMUNITY OAuth connection
                  (community_oauth_flow=m2m/u2m) — never set by hand;
                  the connection is created with client_id,
                  client_secret, token_endpoint (and oauth_scope /
                  authorization_endpoint as the flow requires)

Per-table options (allowlisted via externalOptionsAllowList):
    cursor_field          column to drive incremental reads; absent → snapshot
    select                comma-separated $select projection
    filter                additional $filter expression
    filters_at            JSON object mapping contained segment names (or
                          zero-based indices) to per-segment $filter expressions
    page_size             $top per request; unset → 1000 under the default
                          client-driven pagination (auto/skip/keyset need a
                          $top to size pages). Only pagination=nextlink leaves
                          snapshot ingest without a $top (server default);
                          cursor/delta ingest defaults to 1000 either way
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

# Primary connector module: the cohesive read / auth / pagination logic keeps it
# over pylint's 1500-line advisory cap. Splitting it would scatter tightly
# coupled helpers across files for no readability gain (the contained-nav and
# partition logic already live in _contained.py / _partition.py).
# pylint: disable=too-many-lines

import base64
import collections
import hashlib
import itertools
import json
import logging
import math
import os
import pickle
import random
import re
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterator
from urllib.parse import unquote, urljoin, urlparse
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
    DELETED_COL as _DELETED_COL,
    SEQUENCE_COL as _SEQUENCE_COL,
    cursor_at_or_before_for_refilter as _cursor_at_or_before_for_refilter,
    cursor_max as _cursor_max,
    jsonify_complex_values as _jsonify_complex_values,
    max_or as _max_or,
    offset_progress_view as _offset_progress_view,
    pad_row_to_fields as _pad_row_to_fields,
    parse_iso8601 as _parse_iso8601,
    parse_lookback_dedup as _parse_lookback_dedup,
    parse_max_records as _parse_max_records,
    trim_to_distinct_cursor_boundary as _trim_to_distinct_cursor_boundary,
    url_origin as _url_origin,
)

# Note: the ``_pg_*`` client-side pagination URL helpers live in ``_contained``
# (so the inner-``$expand`` continuation builder can share them without an
# import cycle); they're re-exported here for ``_client_paginate_pages`` and
# existing ``from ...odata.odata import _pg_*`` callers.
from databricks.labs.community_connector.sources.odata._contained import (
    CONTAINED_PATH_SEP as _CONTAINED_PATH_SEP,
    DEFAULT_PAGE_SIZE as _DEFAULT_PAGE_SIZE,
    MAX_CONTAINED_DEPTH as _MAX_CONTAINED_DEPTH,
    ContainedNavMixin,
    _pg_get_query,
    _pg_base_filter,
    _pg_keyset_filter,
    _pg_keyset_seek_url,
    _pg_orderby_keys,
    _pg_page_fingerprint,
    _pg_parse_top,
    _pg_set_query,
    _pg_strip_positional,
    _pg_strip_query,
    _pg_with_extra_filter,
    combine_filters as _combine_filters,
    contained_nav_props as _contained_nav_props,
    fk_column_name as _fk_column_name,
    join_url as _join_url,
    looks_like_iso8601 as _looks_like_iso8601,
    odata_literal as _odata_literal,
    odata_literal_typed as _odata_literal_typed,
    parse_batch_size_cap as _parse_batch_size_cap,
    parse_contained_path as _parse_contained_path,
    resolve_segment_filters as _resolve_segment_filters,
    validate_page_size as _validate_page_size,
    _TRANSIENT_HTTP_STATUSES,
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


def _spark_type_for_property(prop, edm_type: str | None = None):
    """Spark type for one CSDL ``<Property>`` element: the static EDM map,
    except ``Edm.Decimal``, which honours the declared ``Precision`` /
    ``Scale`` facets (a hardcoded ``DecimalType(38, 18)`` leaves only 20
    digits left of the point — it can't hold a ``Decimal(38, 0)`` ID
    column's large values). Facet handling:

    * both facets absent — the historical wide ``DecimalType(38, 18)``,
      so existing destinations don't shift types;
    * ``Scale="variable"``/``"floating"`` — also ``(38, 18)`` (Spark's
      fixed-scale decimal can't express a varying scale);
    * ``Scale`` absent with ``Precision`` declared — scale 0 (the CSDL
      default);
    * values clamped to Spark's 38-digit maximum with
      ``scale <= precision``.

    ``edm_type`` (when given) overrides the element's raw ``Type`` — the
    caller passes the ``TypeDefinition``-resolved underlying primitive so a
    property typed ``ta.Qty`` (UnderlyingType ``Edm.Int64``) maps to
    ``LongType`` instead of falling to the ``StringType`` default while the
    literal-rendering map correctly knows it's numeric. A Decimal-backed
    definition still gets the wide ``(38, 18)`` default (its facets live on
    the TypeDefinition element, which isn't threaded here)."""
    edm_type = edm_type or prop.get("Type", "Edm.String")
    if edm_type != "Edm.Decimal":
        return _EDM_TO_SPARK.get(edm_type, StringType())
    raw_precision = prop.get("Precision")
    raw_scale = prop.get("Scale")
    if raw_precision is None and raw_scale is None:
        return DecimalType(38, 18)
    if raw_scale is None:
        scale = 0
    elif raw_scale.isdigit():
        scale = int(raw_scale)
    else:  # "variable" / "floating"
        return DecimalType(38, 18)
    precision = int(raw_precision) if raw_precision and raw_precision.isdigit() else 38
    precision = min(max(precision, 1), 38)
    return DecimalType(precision, min(scale, precision))


# Delta tracking constants.
#
# The synthetic MERGE columns appended to the schema when delta is active
# (``_DELETED_COL`` tombstone flag / ``_SEQUENCE_COL`` sequence) are imported
# from ``_helpers`` above — they live there so the partition mixin can share
# them. The OData v4 ABNF permits a leading underscore in a property name, so a
# real source column CAN collide with these; ``get_table_schema`` detects the
# collision and raises a curated reserved-name error rather than emitting a
# schema with duplicate columns (Spark rejects those) and silently overwriting
# the source value at stamp time.
_DELTA_PREFER = "odata.track-changes"

# ``Name=value`` (named-key) form inside a key predicate — the name is a
# simple identifier, so a ``=`` inside a quoted VALUE can't false-match.
_KEY_EQ_RE = re.compile(r"^\w+\s*=")


def _split_key_predicate(pred: str) -> list[str]:
    """Split a key-predicate body on top-level commas, honoring OData
    string quoting (``''`` escapes a quote inside a quoted value)."""
    parts: list[str] = []
    buf: list[str] = []
    in_quote = False
    i = 0
    while i < len(pred):
        ch = pred[i]
        if ch == "'":
            if in_quote and i + 1 < len(pred) and pred[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_quote = not in_quote
            buf.append(ch)
        elif ch == "," and not in_quote:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    if buf:
        parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _coerce_key_literal(text: str, edm_type: str | None):
    """One key-predicate literal → the Python value the matching UPSERT
    rows carry (JSON-decoded), so a tombstone built from an entity
    reference MERGE-matches them. Quoted strings un-escape ``''``;
    numeric/boolean Edm types parse; everything else (guids, dates,
    unknown types) stays as its raw text — exactly how the JSON payload
    delivers those."""
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        return text[1:-1].replace("''", "'")
    if edm_type in ("Edm.Int16", "Edm.Int32", "Edm.Int64", "Edm.Byte", "Edm.SByte"):
        try:
            return int(text)
        except ValueError:
            return text
    if edm_type in ("Edm.Single", "Edm.Double"):
        try:
            return float(text)
        except ValueError:
            return text
    if edm_type == "Edm.Boolean" and text.lower() in ("true", "false"):
        return text.lower() == "true"
    if edm_type is None:
        # Untyped fallback: an integer-looking bare literal parses (JSON
        # would have carried it as a number); anything else stays text.
        try:
            return int(text)
        except ValueError:
            return text
    return text


# Effectively-unlimited value for ``max_records_per_batch`` when the
# framework's batch reader is detected (``start_offset is None``).
# A bare ``sys.maxsize`` is unnecessary — the per-fetch cap arithmetic
# only ever compares ``<= len(emitted)``; any value larger than what a
# single ingestion could plausibly buffer works.
_BATCH_UNCAPPED = 10**12

# Pagination strategy for walking a collection's pages:
#   * nextlink — follow the server's ``@odata.nextLink`` only; strictly
#     spec-compliant, and the choice for a ``$top``-free snapshot scan.
#   * keyset — ignore ``@odata.nextLink``; seek the next page via a
#     ``(k gt last)`` predicate on the ``$orderby`` key set. For servers
#     that page-limit a response but omit the continuation link.
#   * skip — ignore ``@odata.nextLink``; page via ``$top`` + ``$skip``.
#     The keyless fallback (entities with no unique sort key); O(n)
#     offsets and fragile under concurrent writes.
#   * auto (default) — follow ``@odata.nextLink`` while emitted; when the
#     server stops linking, drain via a keyset (when the ``$orderby`` has
#     keys) or skip seek until an *empty* page. This covers a server that
#     page-limits below ``$top`` while omitting the link, and one that
#     treats ``$top`` as a total-result limit and propagates the budget
#     through its skiptoken links (the chain self-terminates at ``$top``;
#     auto seeks past it when ``fetched >= top``).
# keyset/skip/auto require a ``$top`` to size pages and detect
# truncation, so they force a default ``page_size`` when none is set.
_PAGINATION_MODES = frozenset({"nextlink", "keyset", "skip", "auto"})

# ``cursor_lookback_seconds=auto`` self-sizes the overlap window from the
# **max** measured walk duration over the last ``_LOOKBACK_AUTO_WINDOW``
# completed walks (the worst recent walk is the robust ceiling on how long a
# walk runs — far less hand-wavy than last-value × a big fudge factor, and
# resistant to a single slow spike skewing the estimate). That max is then
# multiplied by ``cursor_lookback_factor`` (small margin for a walk worse
# than any seen recently) and clamped to ``cursor_lookback_max_seconds`` — a
# runaway backstop. Both the factor and ceiling are per-table options; these
# are their defaults.
_LOOKBACK_AUTO_WINDOW = 5
_LOOKBACK_AUTO_DEFAULT_FACTOR = 1.5
_LOOKBACK_AUTO_DEFAULT_CEILING_SECONDS = 3600
# Monotonic across the whole process — guarantees each emitted record
# has a strictly increasing sequence value, so apply_changes can pick a
# deterministic winner when the same primary key appears multiple times
# in one batch (e.g. update then delete arriving back-to-back).


class _SequenceCounter:
    """Pickle-safe wrapper around ``itertools.count`` for the
    ``_lc_sequence`` tie-breaker.

    A bare module-level ``itertools.count`` breaks the DEPLOYED artifact on
    Python >= 3.14 (which removed itertools pickling): in the merged
    single-file bundle every class is function-local, so cloudpickle — what
    PySpark uses to ship readers to executors — serializes the connector
    class BY VALUE, walking the closure cells that hold this counter, and
    raises ``TypeError: cannot pickle 'itertools.count' object``. (The
    package layout pickles the class by reference and never touches the
    counter, which is why the module-level unit suite alone can't catch
    it — see the bundle round-trip test.) The iterator is excluded from
    the pickled state; an executor copy restarts at zero, which is benign:
    the nanosecond timestamp dominates ``_next_sequence`` ordering and the
    counter only breaks same-nanosecond ties within one process."""

    def __init__(self):
        self._it = itertools.count()

    def __next__(self):
        return next(self._it)  # GIL-atomic increment, like the bare count

    def __getstate__(self):
        return {}

    def __setstate__(self, _state):
        self._it = itertools.count()


_SEQUENCE_COUNTER = _SequenceCounter()

# Process-wide CSDL cache, keyed by service_url. SDP creates a fresh
# ``LakeflowSource`` (and ``ODataLakeflowConnect``) for every
# ``spark.readStream.format("lakeflow_connect").load()`` call; within
# a single Python process this cache makes all instances share one
# parse. Entries carry the wall-clock time the document was FETCHED
# (for file-cache hits, the file's mtime — the fetch time of the
# process that wrote it) so ``metadata_cache_ttl_seconds`` governs this
# layer exactly like the on-disk one: entries expire after the TTL and
# a TTL of 0 disables the layer entirely. Without the stamp a
# long-running driver would serve the same parsed ``$metadata`` forever
# regardless of the configured TTL. Deliberately lock-free (unlike the
# capability cache, whose entries are MERGED read-modify-write under
# ``_CAPABILITY_LOCK``): entries here are immutable tuples swapped
# whole, and the worst race — two threads observing an expired entry —
# costs a duplicate fetch, never a torn value.
_METADATA_CACHE: dict[str, tuple[str, ET.Element, "_CsdlIndex", float]] = {}

# Expired entries for a service are only popped when THAT service is next
# read, so a long-lived driver serving many distinct services would retain
# one multi-MB parsed tree per service forever. Cap the cache and evict
# oldest-first on insert (per-entry TTLs belong to the writing instance, so
# age is the only cross-service eviction signal); an evicted service just
# re-fetches on its next read.
_METADATA_CACHE_MAX_SERVICES = 16


def _metadata_cache_put(
    service_url: str, entry: tuple[str, ET.Element, "_CsdlIndex", float]
) -> None:
    """Insert into :data:`_METADATA_CACHE`, evicting the oldest entries
    (by their ``fetched_at`` stamp) beyond :data:`_METADATA_CACHE_MAX_SERVICES`.
    The just-inserted key is exempt from eviction: the file-cache-hit path
    stamps entries with the FILE's mtime, which can be older than every
    cached entry — evicting the newcomer itself would make that service
    re-parse its pickle on every fresh instance while 16 idle services
    stay cached, the exact thrash this cache exists to avoid. Same
    lock-free discipline as the cache itself — a racing double-evict just
    costs the loser a re-fetch.

    The eviction candidates are SNAPSHOTTED before choosing: iterating the
    live dict while sibling threads insert/pop (a driver streaming from
    more than the cap's worth of distinct services) raises "dictionary
    changed size during iteration" — iteration+mutation is not one of the
    GIL-atomic dict operations the lock-free discipline relies on, unlike
    the single-shot ``list(...)`` copy and the ``pop(..., None)``."""
    _METADATA_CACHE[service_url] = entry
    while len(_METADATA_CACHE) > _METADATA_CACHE_MAX_SERVICES:
        candidates = [
            (stamp, key)
            for key, (_, _, _, stamp) in list(_METADATA_CACHE.items())
            if key != service_url
        ]
        if not candidates:
            break
        _METADATA_CACHE.pop(min(candidates)[1], None)


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

# HTTP status codes treated as transient by ``_http_get``'s retry loop:
# * 408 — Request Timeout. The server (or a proxy) gave up waiting for
#   the request; same transient shape as a read timeout, which IS
#   retried as a network error. Keeps this set aligned with
#   ``_TRANSIENT_HTTP_STATUSES`` in ``_contained.py`` so a flaky
#   408-emitting proxy doesn't kill a read a 503-emitting one survives.
# * 429 — Too Many Requests (throttling).
# * 500 — Internal Server Error. Frequently transient (the "contact
#   support" templated body that Hexagon SCApi returns under load is
#   the prototype case); deterministic 500s eat the retry budget and
#   surface the original body, same as deterministic 503s.
# * 502 — Bad Gateway. Upstream proxy failure; virtually always
#   transient.
# * 503 — Service Unavailable. Server overload / restart.
# * 504 — Gateway Timeout. Upstream took too long; same shape as 502.
# 429 and 503 honour the server's ``Retry-After`` header when present;
# 500/502/504 fall back to pure exponential backoff (Retry-After is
# rarely emitted on those, and we shouldn't trust it if it is).
_RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})

# Cap on manually-followed same-origin redirects per request (redirect-loop
# guard). Off-origin redirects never count — they raise immediately.
_MAX_SAME_ORIGIN_REDIRECTS = 5

# Module logger. Always-on:
#   * WARNING — every retry (network/429/503/JSON decode), so an
#     operator inspecting pipeline logs sees how often the source
#     flakes without enabling anything extra.
#   * ERROR   — JSON decode failure (after retries exhausted) and
#     other terminal problems re-raised as exceptions.
# Opt-in via the ``verbose_http_logging`` connection option:
#   * INFO    — one log line per request URL + response status / body
#     snippet. Useful for triaging "why am I missing rows" but
#     **emits source data into the log stream**, so it's off by
#     default. Body snippet length is bounded by
#     ``verbose_http_log_body_chars`` (default 500).
_LOG = logging.getLogger(__name__)


class _PageCycleGuard:
    """Bounded-window repeat detector for pagination continuation URLs.

    The per-page guards (identical consecutive fingerprint, self-referential
    link) catch only a PERIOD-1 cycle. A server or caching proxy that
    ALTERNATES skiptokens (``tokA → tokB → tokA → …``) yields pages whose
    consecutive fingerprints AND links both differ every step, so only
    revisiting a URL reveals the loop — otherwise the walk runs forever
    (OOM under the uncapped batch reader; a never-advancing stream under the
    microbatch reader). Continuation URLs are strictly forward (each
    skiptoken/seek advances), so ANY repeat is a spurious cycle. Tracks the
    last ``_WINDOW`` fetched URLs — bounded so a legitimate long walk (all
    distinct URLs) never grows unboundedly or false-positives; a real cyclic
    server alternates only a handful of tokens, caught well within the
    window."""

    _WINDOW = 512

    def __init__(self):
        self._seen: set[str] = set()
        self._order: "collections.deque[str]" = collections.deque()

    def seen_before(self, url: str) -> bool:
        """Record ``url`` and return whether it was already in the window."""
        if url in self._seen:
            return True
        self._seen.add(url)
        self._order.append(url)
        if len(self._order) > self._WINDOW:
            self._seen.discard(self._order.popleft())
        return False


def _cache_owner_tag() -> str:
    """Per-user tag baked into every tempdir cache filename. The system
    tempdir is world-writable on multi-user hosts and both cache paths are
    otherwise predictable (digest of ``service_url`` only) — another local
    user could pre-create the file. For the CSDL cache that file feeds
    ``pickle.load`` (arbitrary code execution); for the capability JSON it
    could force an unverified ``$expand`` read. A per-user filename plus the
    ownership check in the readers closes both."""
    try:
        return str(os.getuid())
    except AttributeError:  # Windows — no uid; fall back to the login name
        import getpass

        try:
            # Windows account names can't contain path separators, but the
            # value can come from the USERNAME env var — sanitize so the
            # tag can never smuggle path syntax into the cache filename.
            return re.sub(r"[^A-Za-z0-9._-]", "_", getpass.getuser())
        except Exception:  # noqa: BLE001 — cache tag must never fail
            return "user"


def _cache_file_owned_by_us(path: str) -> bool:
    """Whether ``path`` itself is owned by the current uid (POSIX). Uses
    ``lstat`` so a foreign-owned symlink planted at the (predictable) cache
    path fails the check outright — with following ``stat`` a symlink
    pointing at some victim-owned file would pass, diverging from what the
    subsequent ``open`` actually reads. On platforms without ``os.getuid``
    the per-user filename is the only guard."""
    try:
        return os.lstat(path).st_uid == os.getuid()
    except AttributeError:
        return True
    except OSError:
        return False


def _replace_with_private_tmp(path: str, data: bytes) -> bool:
    """Atomically publish ``data`` at ``path`` via a private temp file in
    the same directory. The temp name embeds ``os.urandom`` so it can't be
    predicted, and it is opened ``O_CREAT | O_EXCL | O_NOFOLLOW`` with mode
    ``0o600`` — a pre-planted file or symlink at the name makes the open
    fail instead of following the link and clobbering whatever it points
    at (the tempdir is world-writable on multi-user hosts). Best-effort:
    returns ``False`` on any OSError, ``True`` once ``os.replace`` lands."""
    tmp = f"{path}.{os.getpid()}.{os.urandom(4).hex()}.tmp"
    # O_BINARY: without it the Windows CRT applies text-mode LF→CRLF
    # translation to the fd, corrupting the pickle/JSON bytes (readers
    # then fail closed — a silent permanent cache miss, not corruption).
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
    )
    try:
        fd = os.open(tmp, flags, 0o600)
    except OSError:
        return False
    try:
        fh = os.fdopen(fd, "wb")
    except OSError:
        os.close(fd)  # fdopen failed to take ownership — don't leak the fd
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False
    try:
        with fh:
            fh.write(data)
        os.replace(tmp, path)
        return True
    except OSError:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False


def _metadata_cache_path(service_url: str) -> str:
    """Tempdir path for the pickled CSDL of ``service_url`` (per-user —
    see :func:`_cache_owner_tag`)."""
    digest = hashlib.sha256(service_url.encode("utf-8")).hexdigest()[:16]
    return os.path.join(tempfile.gettempdir(), f"odata_csdl_{_cache_owner_tag()}_{digest}.pickle")


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


# Process-wide capability-verdict cache, keyed by service_url — same
# lifecycle problem as ``_METADATA_CACHE``: SDP recreates the connector
# instance per microbatch / ``.load()``, so instance caches don't survive,
# and the paths that keep their offsets bare (contained snapshot streams)
# or have no offset at all (the batch reader behind pipeline snapshot
# refreshes) would otherwise re-run their preflight probes on every read.
# Entry shape: ``{"batch_ok": bool, "batch_size_ok": int,
# "or_filter_ok": bool, "expand_ok": {table_name: bool},
# "cursor_probe_ok": {shared_key: bool}}`` — the server-wide verdicts flat,
# the per-table verdicts (nested-$expand and cursor-probe, listed in
# ``_PER_TABLE_CAPABILITY_KEYS``) keyed by contained path (different
# nesting depths can verify differently).
_CAPABILITY_CACHE: dict[str, dict] = {}

# The verdict keys stored as ``{table_key: bool}`` maps rather than flat
# server-wide values — the disk merge must union these per table instead of
# ``setdefault``-shadowing a sibling worker's whole map.
_PER_TABLE_CAPABILITY_KEYS = ("expand_ok", "cursor_probe_ok")

# On-disk mirror of the capability cache (JSON, not pickle — plain data
# only). Covers the forked-worker gap the process dict can't (PySpark may
# fork a fresh daemon worker per ``.load()``), so a pipeline refresh with N
# contained snapshot tables pays each probe once, not N times. The TTL is
# much longer than the CSDL cache's (a capability verdict is a couple of
# booleans that only change when the SERVER is upgraded, and the
# offset-persisted copies of these same verdicts never expire at all);
# an explicit non-``auto`` mode switch purges the entry immediately (see
# ``_scrub_nonauto_verdicts``), so re-selecting ``auto`` still re-probes.
_CAPABILITY_FILE_CACHE_TTL_SECONDS = 900

# Cap on distinct service_urls held in the process capability cache (and its
# on-disk mirror). The metadata cache got _METADATA_CACHE_MAX_SERVICES; this
# sibling — same per-service_url keying, same "SDP forks/recreates instances"
# rationale — lacked one, so a long-lived multi-tenant driver issuing
# ``.load()`` against many services accumulated one dict entry + one
# mtime-memo entry + one tempdir file PER service for the driver's whole
# lifetime (the TTL only makes a stale file IGNORED on read, never deletes
# it). Generous relative to the metadata cap of 16 because these entries are
# tiny (a few booleans + a small JSON file, vs multi-MB CSDL trees), so the
# cap sits well above any realistic concurrent working set — a service in
# active rotation never evicts — while still bounding a driver that touches
# thousands of distinct services over its lifetime. Eviction is by first-
# touch (creation) order.
_CAPABILITY_CACHE_MAX_SERVICES = 256

# Per-service mtime of the on-disk mirror the last time this process merged it
# into ``_CAPABILITY_CACHE``. Lets ``_capability_cache_load`` skip the re-read +
# re-parse when the file hasn't changed since — so the hot lookup on the
# offset-less paths (snapshot / batch reader, once per table per microbatch) is
# a single ``stat`` rather than a full JSON parse. A sibling worker's write
# bumps the mtime and is picked up on the next load.
_CAPABILITY_DISK_MTIME: dict[str, float] = {}

# Serializes every read-modify-write-serialize of the shared cache. On the
# standard GIL interpreter the individual dict ops are already atomic (and all
# callers share one dict object, so there are no lost updates), but under a
# free-threaded build (PEP 703, available in 3.14) concurrent streaming queries
# on one driver — same ``service_url`` — would race the mutations against the
# ``json.dump`` / merge iterations. Cheap and uncontended in the common case;
# re-entrant because store/drop nest load and write under a single hold.


class _PicklableRLock:
    """Re-entrant lock that survives pickling by re-creating itself.

    Same deployment constraint as ``_SequenceCounter``: in the merged
    single-file bundle this lock lives in a closure cell that cloudpickle
    walks when shipping the connector class BY VALUE to executors, and a
    bare ``threading.RLock`` raises ``TypeError: cannot pickle
    '_thread.RLock' object``. A fresh lock per unpickled copy is the
    CORRECT semantics anyway — a lock guards state within one process,
    and each executor gets its own process-wide caches to guard."""

    def __init__(self):
        self._lock = threading.RLock()

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)

    def __getstate__(self):
        return {}

    def __setstate__(self, _state):
        self._lock = threading.RLock()


_CAPABILITY_LOCK = _PicklableRLock()


def _capability_cache_path(service_url: str) -> str:
    """Tempdir path for the capability-verdict JSON of ``service_url``
    (per-user — see :func:`_cache_owner_tag`)."""
    digest = hashlib.sha256(service_url.encode("utf-8")).hexdigest()[:16]
    return os.path.join(tempfile.gettempdir(), f"odata_caps_{_cache_owner_tag()}_{digest}.json")


def _capability_cache_flush(service_url: str, payload: str) -> None:
    """Write an already-serialized ``payload`` string to the on-disk mirror
    via :func:`_replace_with_private_tmp` (unpredictable ``O_EXCL`` temp name
    + ``os.replace``), so a concurrent worker never observes a half-written
    file and a pre-planted symlink can't redirect the write. Takes a
    **string**, not the live dict, so the caller serializes under
    :data:`_CAPABILITY_LOCK` and the blocking disk I/O here runs lock-free —
    concurrent cache ops don't serialize on each other's I/O. Best-effort:
    like the cross-process case, a concurrent writer's swap can win
    (last-writer-wins on the mirror); the in-memory cache stays authoritative
    and a re-probe/TTL recovers any lag. The mtime memo is refreshed so the
    writing process doesn't immediately re-read its own write."""
    path = _capability_cache_path(service_url)
    if not _replace_with_private_tmp(path, payload.encode("utf-8")):
        return
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return
    # Plant the "already merged" memo ONLY while the service is still live in
    # the cache — under the lock so a concurrent eviction can't interleave.
    # If the service was evicted in the load->flush window, skip the memo: an
    # orphaned memo would leak (eviction only reclaims memos it pops) AND make
    # the next load short-circuit past this on-disk verdict (a redundant
    # re-probe). Skipping it lets that next load re-read + merge the file
    # instead. The in-memory copy — when present — is authoritative, so not
    # re-reading our own write stays the fast path.
    with _CAPABILITY_LOCK:
        if service_url in _CAPABILITY_CACHE:
            _CAPABILITY_DISK_MTIME[path] = mtime


def _capability_cache_evict_locked(keep_url: str) -> None:
    """Drop oldest-created cache entries beyond
    :data:`_CAPABILITY_CACHE_MAX_SERVICES`, deleting each evicted service's
    on-disk mirror + mtime-memo entry so both memory and tempdir inodes stay
    bounded. The caller MUST hold :data:`_CAPABILITY_LOCK` (this iterates the
    shared dict). Never evicts ``keep_url`` (the entry the caller just
    created). Eviction only fires once the working set exceeds the cap, i.e.
    the evicted service is cold by the cap's own logic; a concurrent process
    still using it keeps its own in-memory copy and rewrites the file on its
    next store — worst case one re-probe, the same best-effort posture as TTL
    expiry."""
    while len(_CAPABILITY_CACHE) > _CAPABILITY_CACHE_MAX_SERVICES:
        victim = next((k for k in _CAPABILITY_CACHE if k != keep_url), None)
        if victim is None:
            return
        _CAPABILITY_CACHE.pop(victim, None)
        path = _capability_cache_path(victim)
        _CAPABILITY_DISK_MTIME.pop(path, None)
        try:
            os.remove(path)
        except OSError:
            pass


def _capability_cache_load(service_url: str) -> dict:
    """The cached capability verdicts for ``service_url``: the process-wide
    entry, hydrated from the on-disk JSON (when fresh) for keys the process
    hasn't determined itself. Returns the live process entry (mutable). The
    disk file is re-parsed only when its mtime changed since the last merge
    (see :data:`_CAPABILITY_DISK_MTIME`); otherwise this is just a ``stat``.
    The blocking file read runs lock-free; only the merge (which iterates the
    shared dict) is under :data:`_CAPABILITY_LOCK`. The returned dict is a live
    reference read afterwards via atomic ``.get`` only."""
    entry = _CAPABILITY_CACHE.get(service_url)  # atomic; no iteration
    if entry is None:
        # First touch of this service — create under the lock so the cap
        # eviction (which iterates the shared dict) can't race a sibling
        # thread's insert. The steady-state path (entry present) stays
        # lock-free: a single atomic ``.get``.
        with _CAPABILITY_LOCK:
            entry = _CAPABILITY_CACHE.get(service_url)
            if entry is None:
                entry = _CAPABILITY_CACHE[service_url] = {}
                _capability_cache_evict_locked(service_url)
    path = _capability_cache_path(service_url)
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return entry  # no file yet — process entry stands alone
    if mtime < time.time() - _CAPABILITY_FILE_CACHE_TTL_SECONDS:
        return entry  # stale on disk — ignore it (process entry stands)
    if _CAPABILITY_DISK_MTIME.get(path) == mtime:
        return entry  # already merged this exact disk state
    if not _cache_file_owned_by_us(path):
        # A foreign-owned file at our (predictable) cache path is not ours to
        # trust: a poisoned ``expand_ok: true`` would force an UNVERIFIED
        # $expand read — the deep-row-loss case the preflight exists to
        # prevent. The per-user filename already makes this unreachable in
        # practice; defense-in-depth backstop.
        return entry
    try:
        with open(path, "r", encoding="utf-8") as fh:  # blocking read, lock-free
            disk = json.load(fh)
    except (OSError, ValueError):
        return entry  # missing/corrupt file — the process entry stands alone
    with _CAPABILITY_LOCK:  # only the shared-dict merge needs the lock
        if isinstance(disk, dict):
            for key, value in disk.items():
                if key in _PER_TABLE_CAPABILITY_KEYS and isinstance(value, dict):
                    # Per-table maps union table-by-table (process verdicts
                    # win) — ``setdefault`` would shadow a sibling worker's
                    # whole map as soon as this process holds ANY table.
                    current = entry.get(key)
                    entry[key] = {**value, **current} if isinstance(current, dict) else dict(value)
                else:
                    entry.setdefault(key, value)
        # Only memo the merged mtime while this service is still the live
        # cache entry. A sibling ``load(other)`` can trip cap-eviction and pop
        # THIS service during the lock-free file read above; planting the memo
        # unconditionally would then orphan it against an evicted entry, and
        # the next load would short-circuit past the on-disk verdicts (return
        # ``{}``). Symmetric to the guard in ``_capability_cache_flush``.
        if service_url in _CAPABILITY_CACHE:
            _CAPABILITY_DISK_MTIME[path] = mtime
    return entry


def _capability_cache_store(service_url: str, key: str, value, table_name: str | None = None):
    """Record one capability verdict in the process cache and rewrite the on-disk
    mirror. ``expand_ok`` verdicts are per-table (``table_name`` required);
    everything else is server-wide. The mutation + serialization run under
    :data:`_CAPABILITY_LOCK` (so a concurrent reader/writer never sees the dict
    mid-mutation); the load's file read and the flush's disk write run lock-free
    (see :func:`_capability_cache_flush`)."""
    entry = _capability_cache_load(service_url)
    with _CAPABILITY_LOCK:
        if table_name is not None:
            entry.setdefault(key, {})[table_name] = value
        else:
            entry[key] = value
        payload = json.dumps(entry)
    _capability_cache_flush(service_url, payload)


def _cap_dict_has(container: dict, key: str, table_name: str | None) -> bool:
    """Whether ``container`` holds the verdict identified by ``key`` (+ optional
    ``table_name`` for a per-table map) — the guard that keeps a purge from
    rewriting the file when there is nothing to remove."""
    if key not in container:
        return False
    if table_name is None:
        return True
    value = container[key]
    return isinstance(value, dict) and table_name in value


def _cap_dict_drop(container: dict, key: str, table_name: str | None) -> None:
    """Remove one verdict from a cache dict in place. With ``table_name`` and a
    per-table map, drop only that table's entry (leaving sibling tables intact);
    otherwise drop the whole key."""
    if table_name is not None and isinstance(container.get(key), dict):
        container[key].pop(table_name, None)
    else:
        container.pop(key, None)


def _capability_cache_drop(service_url: str, keys: set[str], table_name: str | None = None) -> None:
    """Purge ``keys`` from the cached verdicts of ``service_url`` (process AND
    disk) — called when an explicit non-``auto`` mode leaves a recorded verdict
    the user asked to forget, so the shared cache can't resurrect it. With
    ``table_name`` the per-table verdicts (``expand_ok`` / ``cursor_probe_ok``)
    drop only that table's entry, leaving sibling tables' verdicts intact;
    without it the whole key is dropped (server-wide verdicts, or a
    table-agnostic reset).

    Goes through :func:`_capability_cache_load` for the authoritative merged
    view, so when nothing matches it returns without touching the file — and
    that check is a bare ``stat`` while the mtime is unchanged (the common
    steady-state pinned read every microbatch), not a full JSON parse. The
    check-mutate-serialize runs under :data:`_CAPABILITY_LOCK`; the load's file
    read and the flush's disk write run lock-free."""
    entry = _capability_cache_load(service_url)
    with _CAPABILITY_LOCK:
        if not any(_cap_dict_has(entry, k, table_name) for k in keys):
            return  # nothing recorded anywhere — no rewrite
        for key in keys:
            _cap_dict_drop(entry, key, table_name)
        payload = json.dumps(entry)
    _capability_cache_flush(service_url, payload)


def _clear_capability_cache() -> None:
    """Clear the in-process capability cache and remove any on-disk JSON
    files. Tests use this between cases that reuse a ``service_url`` with
    different mocked server behaviours."""
    with _CAPABILITY_LOCK:
        _CAPABILITY_CACHE.clear()
        _CAPABILITY_DISK_MTIME.clear()
    tmpdir = tempfile.gettempdir()
    try:
        for entry in os.listdir(tmpdir):
            # Also sweep orphaned atomic-write temps (a process killed
            # between creating its private ``…json.<pid>.<rand>.tmp`` file
            # and the rename leaves it behind forever otherwise).
            if entry.startswith("odata_caps_") and (".json" in entry):
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
    # Fully-qualified ``<TypeDefinition>`` name (canonical namespace) →
    # its ``UnderlyingType`` (an ``Edm.*`` primitive). Properties typed
    # via a TypeDefinition quote their literals per the underlying
    # primitive — without this map an ``Edm.String``-backed definition
    # would fall out of typed rendering and an ISO-looking value would
    # render bare (invalid predicate).
    typedef_underlying: dict[str, str]


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
    typedef_underlying: dict[str, str] = {}

    for schema in root.iter(f"{_NS_EDM}Schema"):
        ns = schema.get("Namespace") or ""
        if ns:
            alias_to_namespace[ns] = ns
        alias = schema.get("Alias")
        if alias:
            # Duplicate aliases across schemas (spec-malformed) resolve
            # last-writer-wins — same policy as duplicate namespaces.
            alias_to_namespace[alias] = ns

        for entity_type in schema.findall(f"{_NS_EDM}EntityType"):
            type_name = entity_type.get("Name")
            if type_name:
                entity_type_by_qname[f"{ns}.{type_name}"] = entity_type

        for typedef in schema.findall(f"{_NS_EDM}TypeDefinition"):
            td_name = typedef.get("Name")
            underlying = typedef.get("UnderlyingType")
            if td_name and underlying:
                typedef_underlying[f"{ns}.{td_name}"] = underlying

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
        typedef_underlying=typedef_underlying,
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
    # is stable within one parsed tree — and within one PROCESS: see
    # ``__getstate__`` for the pickle boundary.
    fields: dict = field(default_factory=dict)
    primary_keys: dict = field(default_factory=dict)
    base_chain: dict = field(default_factory=dict)
    own_fields: dict = field(default_factory=dict)
    own_pks: dict = field(default_factory=dict)
    entity_type: dict = field(default_factory=dict)
    fk_columns: dict = field(default_factory=dict)
    edm_types: dict = field(default_factory=dict)

    def __getstate__(self):
        """Drop the ``id(et)``-keyed memos at the pickle boundary.

        Spark pickles the reader (and this bundle with it) to executor
        tasks. There the unpickled tree's elements have NEW addresses, so
        driver-address keys are guaranteed dead weight (serialized per task,
        never hit again) — and an address coincidence between a worker
        element and a stale driver key would silently return the WRONG
        entity type's fields. The executor re-derives per element on first
        use (one tree walk); the name-keyed memos stay, they're
        process-portable. Fork-based workers (no pickle) preserve identity
        and never pass through here."""
        state = self.__dict__.copy()
        for memo in ("base_chain", "own_fields", "own_pks", "edm_types"):
            state[memo] = {}
        return state


def _parse_conn_int(options: dict, key: str, default, minimum: int) -> int:
    """Curated parse for a connection-level integer option — same
    discipline as the per-table numeric parsers (``validate_page_size``,
    ``parse_max_records``, ``_parse_num_partitions``). A bare ``int()``
    turns garbage into an uncurated traceback at construction, and a
    negative ``max_retries`` makes every ``range(max_retries + 1)`` retry
    loop run ZERO iterations → ``UnboundLocalError`` on ``resp`` instead
    of any HTTP call."""
    raw = options.get(key, default)
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {key}={raw!r}: expected an integer.") from None
    if value < minimum:
        raise ValueError(f"Invalid {key}={raw!r}: must be >= {minimum}.")
    return value


def _next_sequence() -> str:
    """Strictly-increasing per-record sequence value for apply_changes.

    Format: ``<ns_since_epoch:020d>_<counter:012d>``. Both parts
    are zero-padded so the lexicographic string ordering used by
    ``apply_changes`` matches the underlying numeric ordering. The
    nanosecond timestamp tracks wall-clock so values stay ordered
    across process restarts (latest data wins per key); the counter
    breaks ties for records emitted in the same nanosecond. The counter
    is per-process, so two PARTITION tasks emitting the same primary
    key in the same nanosecond could mint equal sequences — an
    astronomically unlikely tie apply_changes resolves arbitrarily,
    accepted rather than paying a per-partition discriminator on every
    row. Cross-batch ordering ASSUMES a non-regressing wall clock: after
    a backwards step (VM snapshot restore, hard NTP correction) a later
    batch's rows sequence BELOW already-applied ones and lose the MERGE
    until the clock passes its old high-water mark — accepted; a
    monotonic source would instead regress on every process restart,
    which is the common case.

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
        parsed_root = urlparse(self.service_url)
        # (scheme, host, port) the credential-bearing session may talk to.
        # Every request URL — including server-supplied ``@odata.nextLink``s
        # — is checked against this before the auth-carrying session sends
        # it, so a malicious/compromised source (or a MITM injecting one
        # nextLink field) can't redirect the Authorization header off-origin.
        self._service_origin = _url_origin(self.service_url)
        if parsed_root.username or parsed_root.password:
            # The URL is echoed verbatim in logs (verbose_http_logging,
            # every retry/no-progress warning) and in error messages —
            # embedded credentials would leak on every request.
            raise ValueError(
                "service_url must not embed credentials (the "
                "'user:password@host' userinfo form) — the URL is echoed "
                "in logs and error messages on every request. Use "
                "auth_type=basic with the 'username' / 'password' "
                "connection options instead."
            )
        if parsed_root.query or parsed_root.fragment or set(self.service_url) & {"?", "#"}:
            # The raw-char check catches a bare trailing "?"/"#"
            # (urlparse reports an EMPTY query/fragment for those, but
            # join_url would still produce "svc?/Customers").
            # Every URL builder appends '/<path>' to the root, so a query-
            # carrying root (the SAP Gateway '?sap-client=100' form) would
            # put the entity path INSIDE the query string on every request
            # — the first symptom is the $metadata fetch dying in the XML
            # parser with no hint the URL was malformed. Fail at
            # construction with the working alternative instead.
            cut = min(i for i in (self.service_url.find("?"), self.service_url.find("#")) if i >= 0)
            trailing = self.service_url[cut:]
            raise ValueError(
                f"service_url must not carry a query string or fragment "
                f"(got {trailing!r}); the connector appends entity paths "
                f"to it, which would land inside the query. For SAP "
                f"Gateway client selection ('?sap-client=NNN'), pass the "
                f"client as a header instead: "
                f"extra_headers='sap-client: NNN' — SAP accepts the "
                f"header form. Other root query parameters have no "
                f"equivalent; such services are not supported."
            )
        # Default 180s (3 min). Deep ``expand_contained=true`` chains
        # (3+ segments) materialise a large cross-product server-side
        # before responding; 60s isn't long enough for most real
        # deployments and the previous default surfaced as
        # ``ReadTimeout`` retried-to-exhaustion failures. Connection
        # option ``timeout_seconds`` overrides per deployment.
        self.timeout = _parse_conn_int(options, "timeout_seconds", "180", 1)
        # On-disk pickle TTL. The default suits typical 1-minute SDP
        # trigger intervals — each trigger spawns a fresh forked
        # worker, the file cache survives, but stale state is bounded.
        # Users with stable schemas can raise this to skip even the
        # first-fork fetch within a longer window; users iterating on
        # the source model can drop it to 0 to disable file caching.
        self.metadata_cache_ttl_seconds = _parse_conn_int(
            options, "metadata_cache_ttl_seconds", _METADATA_FILE_CACHE_TTL_SECONDS, 0
        )
        # Retry budget for transient server-side failures (HTTP 429 / 503).
        # 5 attempts at exponential backoff (1, 2, 4, 8, 16 s) covers the
        # vast majority of momentary throttling spikes from Graph /
        # Dataverse / S/4HANA Cloud without keeping a Spark task pinned
        # indefinitely. `retry_max_delay_seconds` caps any single sleep —
        # honour the server's Retry-After header but never sleep longer
        # than this (some misbehaving servers emit hour-long values).
        self.max_retries = _parse_conn_int(options, "max_retries", "5", 0)
        self.retry_max_delay_seconds = _parse_conn_int(options, "retry_max_delay_seconds", "60", 0)
        # Per-request diagnostic logging. Off by default — when on,
        # writes one INFO line per HTTP request (URL + status + body
        # snippet) to the module logger. The body snippet is the
        # source's response data, so enabling this emits source rows
        # into pipeline logs; turn it on only for triage.
        verbose_raw = (options.get("verbose_http_logging") or "false").strip().lower()
        if verbose_raw not in ("true", "false"):
            # Strict like every other enum option — a typo'd "1"/"yes" would
            # otherwise silently mean OFF, the opposite of what the user
            # asked for while triaging.
            raise ValueError(
                f"Invalid verbose_http_logging={verbose_raw!r}: expected 'true' or 'false'."
            )
        self.verbose_http_logging = verbose_raw == "true"
        # How many chars of the response body to include in each INFO
        # log line when ``verbose_http_logging`` is on. Default 500.
        self.verbose_http_log_body_chars = _parse_conn_int(
            options, "verbose_http_log_body_chars", "500", 0
        )
        self._session: requests.Session | None = None
        # Parsed CSDL bundle: root + lookup index + per-instance memos.
        # ``None`` until the first ``_metadata_root()`` call.
        self._metadata: _MetadataState | None = None
        # Monotonic deadline (seconds) for the current OAuth access token.
        # Set when the token endpoint returns ``expires_in``; `None` means we
        # don't know the expiry (user-supplied access token without metadata)
        # so we fall through to the 401-retry path only.
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
        if len(namespace) != 1:
            # Entity sets always live inside a Schema with a Namespace
            # attribute; root-level tables don't exist in OData v4, and
            # namespaces are single-level — a multi-segment path names
            # nothing (returning segment[0]'s tables would fabricate
            # rows under a nonexistent namespace path).
            return []
        # Accept the schema's ``Alias`` as well as its canonical
        # ``Namespace`` — the same contract as the ``namespace`` table
        # option (CSDL lets references use either, and the read path
        # already resolves both; an alias-spelled listing used to return
        # a silent ``[]``).
        target = namespace[0]
        target = self._metadata_state().index.alias_to_namespace.get(target, target)
        flat = sorted({es for ns, es in index if ns == target})
        contained: set[str] = set()
        for es_name in flat:
            contained.update(self._enumerate_contained_paths(es_name, target))
        # Dedup against flat: a containment-path spelling that collides
        # with a declared flat set (e.g. flat ``My__Set`` next to ``My``
        # with a contained ``Set``) is shadowed by the flat set anyway —
        # listing it twice would fabricate a duplicate table.
        return flat + sorted(contained - set(flat))

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        namespace = (table_options or {}).get("namespace")
        self._set_excluded_ancestor_columns(table_options)
        excluded = self._excluded_ancestor_columns
        fields = self._fields_for(table_name, namespace)
        # ``exclude_ancestor_columns`` can ONLY drop synthetic ancestor-FK
        # columns (the filter in ``_resolve_fk_columns`` iterates the FK
        # mapping alone) — a leaf/own table column named here is left in
        # place. Validate against the path's real FK columns, but only for
        # contained paths: a flat table has none, so a connection-wide
        # default applied to it is a silent no-op rather than warning noise.
        if excluded and "*" not in excluded and self._table_segments(table_name) is not None:
            segments = self._table_segments(table_name)
            all_fk = self._all_fk_column_names(segments, namespace)
            non_fk = excluded - all_fk
            if non_fk:
                # Split into "names a real table column (kept on purpose)"
                # vs "matches nothing (likely a typo)" so the message is
                # actionable either way.
                own_cols = {f.name for f in fields} - all_fk
                kept = sorted(non_fk & own_cols)
                typos = sorted(non_fk - own_cols)
                if kept:
                    _LOG.warning(
                        "exclude_ancestor_columns for %r names %s, which are "
                        "table columns, not synthetic ancestor-FK columns; they "
                        "are kept (only ancestor-FK columns can be excluded).",
                        table_name,
                        kept,
                    )
                if typos:
                    _LOG.warning(
                        "exclude_ancestor_columns for %r names %s, which match "
                        "no column of this path (FK columns: %s); they have no "
                        "effect. Check for typos.",
                        table_name,
                        typos,
                        sorted(all_fk),
                    )
        select = (table_options or {}).get("select")
        if select:
            wanted = {c.strip() for c in select.split(",")}
            # ``$select=*`` means every structural property (OData v4
            # §11.2.4.2.1) — the wire is unprojected, so the schema must be
            # too. Without this, no field is literally named ``*`` and the
            # filter below would silently drop every non-FK column (flat
            # tables then fail the non-empty-schema check outright).
            if "*" not in wanted:
                # ``select`` filters leaf columns only; FK columns survive.
                segments = self._table_segments(table_name) or [table_name]
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
        # in the declared schema so Spark accepts the records. Contained
        # paths never take the delta read path (``read_table`` rejects
        # ``enabled`` there and ``read_table_metadata`` skips the probe with
        # the same guard) — without it, a contained table under
        # ``delta_tracking=auto`` whose server 200-acknowledges the Prefer
        # header on the contained URL would declare two NON-NULLABLE columns
        # no emitted row carries.
        if self._table_segments(table_name) is None and self._delta_active_for(
            table_name, table_options
        ):
            self._assert_no_reserved_delta_columns(table_name, (f.name for f in fields))
            fields = list(fields) + [
                StructField(_DELETED_COL, BooleanType(), False),
                StructField(_SEQUENCE_COL, StringType(), False),
            ]
        return StructType(fields)

    @staticmethod
    def _assert_no_reserved_delta_columns(table_name: str, field_names) -> None:
        """Delta stamps ``_deleted`` / ``_lc_sequence``; the OData v4 ABNF
        allows a leading underscore in a property name, so a source column can
        legally carry one of those names. Emitting the synthetic anyway would
        produce a schema with duplicate columns (Spark rejects those) and
        silently overwrite the source value at stamp time — raise a curated
        error instead."""
        clash = set(field_names) & {_DELETED_COL, _SEQUENCE_COL}
        if clash:
            raise ValueError(
                f"Table {table_name!r} has source column(s) {sorted(clash)} "
                f"that collide with the reserved delta synthetic column(s) "
                f"{sorted({_DELETED_COL, _SEQUENCE_COL})}. Delta tracking stamps "
                f"those names, which would produce a schema with duplicate "
                f"columns and overwrite the source value. Rename the source "
                f"column(s) or disable delta_tracking for this table."
            )

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        namespace = (table_options or {}).get("namespace")
        self._set_excluded_ancestor_columns(table_options)
        # Mirror the read's own select validation (dispatch runs it for every
        # read shape) so metadata fails in the same place rather than
        # reporting a valid source the read then rejects (a select that drops
        # the cursor_field or a PK).
        self._validate_select_columns(table_name, table_options or {})
        primary_keys = self._primary_keys_for(table_name, namespace)
        user_cursor = (table_options or {}).get("cursor_field")
        # Contained paths skip the delta probe (server delta is for
        # top-level sets only; mutex enforced in dispatch below).
        if self._table_segments(table_name) is None and self._delta_active_for(
            table_name, table_options
        ):
            # Mirror the read's own validation EXACTLY: get_table_schema runs
            # the reserved-column collision check on the select/exclusion-
            # filtered fields (a select that drops a colliding source column
            # is legal — the synthetic takes its place), so calling it here
            # fails iff read_table would, in the same place. Checking the raw
            # _fields_for instead over-fires on a select that drops the
            # collision — a config that actually reads fine.
            self.get_table_schema(table_name, table_options)
            return {
                "primary_keys": primary_keys,
                "cursor_field": _SEQUENCE_COL,
                "ingestion_type": "cdc",
            }
        if user_cursor and not primary_keys:
            # cursor_field reports ingestion_type=cdc, which apply_changes
            # MERGEs on the primary key — a keyless entity type declares none,
            # so the MERGE has no key to match on (silent loss by
            # construction). Fail loudly, mirroring the keyless delta gate in
            # _delta_active_for, rather than emitting a broken CDC contract.
            raise ValueError(
                f"cursor_field={user_cursor!r} makes {table_name!r} an incremental "
                f"(CDC) source, which MERGEs on the primary key, but the entity "
                f"type declares no key in $metadata — matched rows would be lost. "
                f"Remove cursor_field for a full-snapshot read, or point at a "
                f"keyed entity set."
            )
        return {
            "primary_keys": primary_keys,
            "cursor_field": user_cursor,
            "ingestion_type": "cdc" if user_cursor else "snapshot",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Dispatch the read, then JSON-render structured values at the emit
        boundary (see :func:`_jsonify_complex_values` — complex-typed /
        collection values map to string columns, and ``str()`` of a dict is
        an unparseable Python repr). List results stay lists (tests and any
        len()-callers rely on that); iterators stay lazy."""
        records, offset = self._read_table_dispatch(table_name, start_offset, table_options)
        # Declared column names for this read (respects ``select`` / streams /
        # ancestor-FK exclusions and the delta synthetics). Emitted rows are
        # padded to it with explicit ``None`` for any absent column: the
        # framework parser rejects an ABSENT non-nullable column but accepts
        # an explicit null, and a server may legally omit null-valued
        # properties from the JSON — without the pad the first such row would
        # hard-fail the whole batch with a cryptic framework error (the delta
        # path already pads tombstones for the same reason; this extends it to
        # every read shape). ``get_table_schema`` itself isn't memoized, but
        # everything it reads (metadata bundle, field cache, capability
        # verdicts) is — this call rebuilds the schema from cached parts with
        # no I/O after the first call. Primary keys and the delta synthetics
        # are exempt from padding: a server never legally omits a KEY (so a
        # missing one is a broken response that must fail loudly, not MERGE a
        # null-key row), and the synthetics are stamped by the connector
        # itself (absence = stamping bug).
        schema = self.get_table_schema(table_name, table_options)
        field_names = tuple(f.name for f in schema.fields)
        # ``Edm.Binary`` arrives as base64url TEXT on the wire (OData v4.01
        # JSON); the framework's row parser only understands standard base64,
        # so it corrupts the base64url alphabet (``-``/``_``). Decode
        # Binary-typed columns to raw ``bytes`` here — the parser passes bytes
        # through untouched. Empty set (the common case) short-circuits.
        binary_fields = frozenset(
            f.name for f in schema.fields if isinstance(f.dataType, BinaryType)
        )
        never_pad = frozenset(
            self._primary_keys_for(table_name, (table_options or {}).get("namespace")) or ()
        ) | {_DELETED_COL, _SEQUENCE_COL}

        def _emit(row: dict) -> dict:
            return _jsonify_complex_values(
                _decode_binary_fields(
                    _pad_row_to_fields(row, field_names, never_pad), binary_fields
                )
            )

        if isinstance(records, list):
            return [_emit(r) for r in records], offset
        return map(_emit, records), offset

    def _validate_select_columns(self, table_name: str, opts: dict[str, str]) -> None:
        """A user ``select`` must keep the columns the machinery depends on.
        Omitting a PK desyncs the schema (drops the column) from
        read_table_metadata (still lists it) — apply_changes then MERGEs on
        an undeclared column. Omitting the cursor_field is worse and
        SILENT: every row's cursor reads None, so under the default
        cursor_nulls=coalesce each batch re-reads the whole table forever
        behind a synthetic-floor watermark, and under ``ignore`` the read
        emits nothing. Both misconfigurations raise here instead."""
        select_raw = (opts.get("select") or "").strip()
        if not select_raw:
            return
        select_cols = {c.strip() for c in select_raw.split(",") if c.strip()}
        if "*" in select_cols:
            return
        leaf_et = self._entity_type_for(table_name, opts.get("namespace"))
        missing_pks = [pk for pk in self._own_primary_keys_for_et(leaf_et) if pk not in select_cols]
        if missing_pks:
            raise ValueError(
                f"select={select_raw!r} omits primary-key column(s) "
                f"{missing_pks} of {table_name!r}. The destination MERGE "
                f"keys on them; add them to select (or drop select)."
            )
        cf = opts.get("cursor_field")
        if (
            cf
            and cf not in select_cols
            and any(f.name == cf for f in self._own_fields_for_et(leaf_et))
        ):
            raise ValueError(
                f"select={select_raw!r} omits cursor_field={cf!r}. The "
                f"incremental read filters and watermarks on that column; "
                f"without it every row's cursor reads null (silent "
                f"full-table re-reads under cursor_nulls=coalesce, zero "
                f"rows under ignore). Add {cf!r} to select."
            )

    def _init_read_scoped_state(self, table_name: str, opts: dict[str, str]) -> None:
        # Pagination strategy for this read. keyset/skip/auto drive
        # pagination client-side (for servers that omit @odata.nextLink)
        # and need a $top to size pages, so force a default page_size when
        # the user left it unset. Held on ``self`` for the duration of the
        # read so the shared fetch primitives pick it up without threading
        # it through every call site; defaults to nextlink (today's
        # behaviour) for any path that doesn't set it. This (like every
        # read-scoped field below) leans on the framework's SERIAL-calls
        # contract: one instance serves one table's calls sequentially, and
        # the lazy batch-mode generators are drained before any other entry
        # point runs on this instance. An interleaving framework would
        # clobber these mid-drain.
        self._pagination = self._parse_pagination(opts)
        self._set_excluded_ancestor_columns(opts)
        # Overlap re-read window for non-atomic walks. Held on ``self`` for
        # the read's duration (like ``_pagination``); the floor is applied
        # only to the read filter (``_apply_cursor_lookback``), never to the
        # committed offset. Applies to every contained cursor-read path —
        # ``expand_contained=true``, the N+1 leaf-cursor walk, the
        # ``cursor_probe`` hydrate, and the ancestor-cursor walk (floored at
        # the ancestor enumeration filter) — each of which self-sizes the
        # ``auto`` window from its measured walk/cycle duration. No-op for
        # non-timestamp cursors.
        self._cursor_lookback = self._parse_cursor_lookback(opts)
        # ``auto`` tuning knobs (ignored for static/off modes).
        self._cursor_lookback_factor = self._parse_cursor_lookback_factor(opts)
        self._cursor_lookback_max_seconds = self._parse_cursor_lookback_ceiling(opts)
        # Overlap-dedup seen-set cap (on by default; 0 = off). Consumed by
        # ``_apply_lookback_dedup`` inside ``_finalize_cursor_read``; the
        # namespace rides along for composite-PK resolution there. Validated
        # eagerly so a bad value fails the read up front even when the
        # lookback window is inactive.
        self._lookback_dedup_cap = _parse_lookback_dedup(opts)
        self._lookback_dedup_ns = (opts or {}).get("namespace")
        # Resolved per-read by each contained cursor-read path; stays 0 for
        # every other read.
        self._active_lookback_seconds = 0
        # Streaming dedup filter — armed per-read by ``_arm_lookback_dedup``
        # on the contained cursor paths; None everywhere else.
        self._lb_dedup_filter = None
        # Only an EXPLICIT positive window is validated against the read
        # config — the default ``auto`` is a no-op outside the expand-cursor
        # path, so leaving it on for flat / N+1 / snapshot tables must not
        # raise.
        if (
            isinstance(self._cursor_lookback, int)
            and self._cursor_lookback > 0
            and not (self._table_segments(table_name) is not None and opts.get("cursor_field"))
        ):
            raise ValueError(
                "cursor_lookback_seconds (an explicit value) is supported "
                "only with a cursor_field on a contained path — it floors "
                "the read filter for the non-atomic expand_contained=true "
                "walk, the leaf-cursor N+1 / cursor_probe walk, and the "
                "ancestor-cursor walk (where it floors the ancestor "
                "enumeration filter, re-reading recently-dirty subtrees). "
                "Use 'auto' (default) or 'off' for other read "
                "configurations."
            )
        self._validate_select_columns(table_name, opts)
        # ``auto`` (the default) is a best-effort hint that no-ops where it can't
        # engage, so these conflict checks fire only on an EXPLICIT strategy
        # opt-in (``cursor_probe=nested-expand`` or ``cursor_probe=batch``) — otherwise
        # every flat / snapshot / expand_contained read would trip them.
        cursor_probe_raw = (opts.get("cursor_probe") or "").strip().lower()
        if "cursor_probe" in opts and self._cursor_probe_mode(opts) in ("probe", "batch"):
            if self._table_segments(table_name) is None:
                raise ValueError(
                    f"cursor_probe={cursor_probe_raw} is supported only on "
                    f"contained-collection paths; {table_name!r} is a flat entity "
                    "set, which is already a single filtered request per batch."
                )
            if self._expand_contained_active(opts):
                raise ValueError(
                    f"cursor_probe={cursor_probe_raw} conflicts with "
                    "expand_contained=true: both fetch only changed leaves, by "
                    "different strategies. Pick one (expand_contained for shallow "
                    "trees; cursor_probe=nested-expand/batch for deep trees with sparse "
                    "changes)."
                )
            if not opts.get("cursor_field"):
                raise ValueError(
                    f"cursor_probe={cursor_probe_raw} requires a cursor_field (it "
                    "only changes how a leaf-owned cursor read is executed). Set "
                    "cursor_field or drop cursor_probe."
                )
        _validate_page_size(opts)
        # Validate the contained-read shape options for EVERY table, flat
        # included: their parsers otherwise run only on contained paths (and
        # ``contained_fetch``'s only after enumeration HTTP), so a typo'd
        # value was silent exactly where every other enum option is loud.
        self._expand_contained_mode(opts)
        self._contained_fetch_batch_size(opts)

    def _read_table_dispatch(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # The Spark Python Data Source batch reader
        # (``LakeflowBatchReader``) passes ``start_offset=None`` and
        # discards the returned end-offset — so any continuation state
        # the connector would normally park in the offset (e.g.
        # ``pending_fetches`` on the ``expand_contained=true`` path,
        # ``chain_next_link`` on the leaf-cursor N+1 path) is dropped.
        # Honouring ``max_records_per_batch`` here would therefore
        # truncate the read at the cap and silently lose the remainder:
        # a cap can only do something *correct* when the offset survives
        # to resume from, which it never does under the batch reader.
        # So treat ``start_offset is None`` as the batch-mode signal and
        # force the cap effectively-infinite regardless of whether the
        # user set one — warning when we override a user value so the
        # ignored option isn't silent. Streaming readers always pass a
        # dict (``{}`` or the parked offset) and keep their cap intact.
        # The cursor read paths additionally stream lazily in this mode
        # (see ``_read_incremental`` / ``_read_contained_incremental`` /
        # ``_read_contained_expand``) so an uncapped batch doesn't
        # materialise the whole result set in memory.
        opts = dict(table_options or {})
        # The user's OWN page_size, captured before any default is injected
        # below — the delta branch must distinguish "user asked for response
        # sizing" (honored via Prefer: odata.maxpagesize) from "client-paging
        # default" (dropped outright: $top is fatal to a delta bootstrap).
        user_page_size = opts.get("page_size")
        # Parse + validate the read-scoped options and pin them on ``self``
        # for the duration of this read (pagination, lookback window,
        # dedup, select/page-size/contained-shape validation). Split out to
        # keep this dispatcher below pylint's statement cap; see the helper
        # for why each field is read-scoped state.
        self._init_read_scoped_state(table_name, opts)
        if self._pagination != "nextlink":
            opts.setdefault("page_size", _DEFAULT_PAGE_SIZE)
        if start_offset is None:
            if "max_records_per_batch" in opts:
                _LOG.warning(
                    "max_records_per_batch=%s ignored for %r: the batch reader "
                    "(LakeflowBatchReader) discards the returned offset, so a "
                    "cap can only truncate the read and silently drop the "
                    "remainder. Reading uncapped. Use a streaming table for a "
                    "resumable per-batch cap.",
                    opts["max_records_per_batch"],
                    table_name,
                )
            opts["max_records_per_batch"] = str(_BATCH_UNCAPPED)
        # ``offset`` is a local view used for shape checks below.
        # The original ``start_offset`` (``None`` for batch reader,
        # ``{}`` or populated dict for streaming) is passed through to
        # the read methods so ``_finalize_cursor_read`` can distinguish
        # batch from streaming and skip the no-progress raise when the
        # framework will discard the returned offset anyway.
        offset = start_offset or {}
        # Seed per-instance capability verdicts from the resume offset so a
        # reader the framework recreates each microbatch skips re-probing.
        self._seed_capability_caches(table_name, opts, start_offset)
        if self._table_segments(table_name) is not None:
            if self._delta_setting(opts) == "enabled":
                raise ValueError(
                    "delta_tracking=enabled is not supported on contained-"
                    "collection paths (server change tracking only applies "
                    "to top-level entity sets). Set delta_tracking=disabled "
                    "or ingest the parent set directly."
                )
            # Reset any per-table shared-cache verdict whose option is pinned
            # non-``auto`` before resolving the read, so a switch back to
            # ``auto`` re-probes even on the bare-offset snapshot / batch-reader
            # paths (the offset scrub only sees offset-carrying reads).
            self._purge_nonauto_table_verdicts(table_name, opts)
            # ``expand_contained``: ``true`` forces the nested-$expand read;
            # ``auto`` attempts it behind a one-shot behavioural preflight
            # (real expand URL + inline-containment cross-check; verdict
            # persisted as ``expand_ok``) and falls back to the N+1 branches
            # below when the server can't be trusted with $expand. The same
            # resolver drives partition activation, so an ``auto`` table that
            # falls back to N+1 stays partitionable.
            if self._expand_read_active(table_name, opts, start_offset):
                # Cursor-based expand keeps a default $top (page_size);
                # snapshot expand omits $top when page_size is unset — which
                # can only happen under pagination=nextlink (every other mode
                # already defaulted page_size above).
                if opts.get("cursor_field"):
                    opts.setdefault("page_size", _DEFAULT_PAGE_SIZE)
                    return self._with_capabilities(
                        self._read_contained_expand(table_name, start_offset, opts),
                        opts,
                        table_name,
                    )
                # Snapshot expand: same streaming-snapshot marker rule as the
                # N+1 snapshot below (see ``_snapshot_stream_result``); the
                # preflight verdict rides the process/file capability cache
                # instead of the offset.
                return self._snapshot_stream_result(
                    start_offset,
                    lambda: self._read_contained_expand(table_name, start_offset, opts),
                )
            if opts.get("cursor_field"):
                # Cursor-based read: default page_size so a $top is sent.
                # Snapshot (the branch below) leaves it unset — no $top only
                # under pagination=nextlink; other modes defaulted it above.
                opts.setdefault("page_size", _DEFAULT_PAGE_SIZE)
                return self._with_capabilities(
                    self._read_contained_incremental(
                        table_name, start_offset, opts, opts["cursor_field"]
                    ),
                    opts,
                    table_name,
                )
            # Snapshot: the streaming offset carries only the
            # ``snapshot_done`` marker (see ``_snapshot_stream_result``) —
            # still deliberately NOT threaded with capability verdicts, so
            # the marker offset stays byte-stable across triggers (the
            # quiesce shape the pyspark wrapper accepts is an EMPTY batch
            # with an unchanged offset). The batch reader discards the
            # offset anyway. Preflight dedup across framework-recreated
            # instances comes from the process/file capability cache instead
            # (see ``_CAPABILITY_CACHE``), which needs no offset channel.
            self._warn_streaming_snapshot_cap(table_name, opts, start_offset)
            return self._snapshot_stream_result(
                start_offset, lambda: self._read_contained_snapshot(table_name, opts)
            )
        if self._delta_offset_shape_gate(table_name, opts, offset) or self._delta_active_for(
            table_name, opts
        ):
            # NEVER send $top on the delta path. OData §11.2.5.3 makes $top a
            # TOTAL-RESULT limit — the exact trap the pagination docs call out
            # for the flat walks — and the delta walker follows only raw
            # @odata.nextLinks (no seek-past-budget fallback, by design). A
            # spec-compliant server would end the bootstrap at $top rows and
            # mint the terminal deltaLink there, silently and permanently
            # dropping every never-again-changed row beyond it. So the
            # client-paging default injected above is stripped, and an
            # EXPLICIT user page_size is honored through the spec's
            # response-sizing mechanism instead (Prefer: odata.maxpagesize —
            # see _delta_initial_request).
            delta_opts = {k: v for k, v in opts.items() if k != "page_size"}
            if user_page_size:
                delta_opts["page_size"] = user_page_size
            return self._read_incremental_delta(table_name, offset, delta_opts)
        if opts.get("cursor_field"):
            # Cursor-based read: default page_size so a $top is sent.
            # Snapshot (the branch below) leaves it unset → no $top.
            opts.setdefault("page_size", _DEFAULT_PAGE_SIZE)
            return self._stamp_delta_verdict(
                self._with_capabilities(
                    self._read_incremental(table_name, start_offset, opts, opts["cursor_field"]),
                    opts,
                    table_name,
                ),
                table_name,
                opts,
            )
        self._warn_streaming_snapshot_cap(table_name, opts, start_offset)
        return self._stamp_delta_verdict(
            self._snapshot_stream_result(
                start_offset, lambda: self._read_snapshot(table_name, opts)
            ),
            table_name,
            opts,
        )

    def _warn_streaming_snapshot_cap(
        self, table_name: str, opts: dict, start_offset: dict | None
    ) -> None:
        """Warn when a STREAMING snapshot read (flat or contained N+1)
        carries a user ``max_records_per_batch``: these shapes ignore the
        cap by design — a snapshot offset holds only the quiesce marker (no
        park state), so truncating would silently drop the remainder. The
        expand snapshot honors the cap (it parks a resumable
        ``pending_fetches`` queue), and batch-mode reads are warned by the
        dispatcher's own override (which also rewrites the option, so this
        never double-fires)."""
        if start_offset is None or "max_records_per_batch" not in opts:
            return
        if opts["max_records_per_batch"] == str(_BATCH_UNCAPPED):
            return
        _LOG.warning(
            "max_records_per_batch=%s ignored for %r: a snapshot stream's "
            "offset carries no park state (only the quiesce marker), so a "
            "cap could only truncate the snapshot and silently drop the "
            "remainder. Reading the full snapshot this trigger. Use a "
            "cursor_field (resumable capped batches) or expand_contained="
            "true (parks a resumable queue) to bound per-trigger work.",
            opts["max_records_per_batch"],
            table_name,
        )

    def _snapshot_stream_result(self, start_offset: dict | None, read) -> tuple:
        """Streaming-snapshot semantics under pyspark's simple-reader wrapper.

        The wrapper (``_SimpleStreamReaderWrapper.add_result_to_cache``)
        raises ``SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE`` for a
        NON-EMPTY batch whose end offset equals its start — so a bare-``{}``
        "quiesce on end == start" contract only ever worked for empty
        batches, and a snapshot read is never empty. Streaming snapshots
        therefore mark the first pass done (``{"snapshot_done": True}`` —
        the offset advances, satisfying the wrapper) and every later trigger
        returns an EMPTY batch with the unchanged marker offset — the one
        quiesce shape the wrapper accepts. The marker check runs BEFORE the
        read, so idle triggers cost zero HTTP. Batch mode (``start_offset is
        None`` — offset discarded) passes through untouched. Restart
        semantics: a FRESH checkpoint re-reads once, then quiesces; a
        checkpoint-preserving restart stays quiesced (the marker persists
        in the checkpoint — a full refresh re-snapshots). Compat-safe: this shape
        previously CRASHED on its first or second trigger, so no working
        checkpoint carries a bare ``{}``.

        The marker stamps only a TERMINAL (``{}``) offset: a capped expand
        snapshot parks ``pending_fetches`` mid-drain, and stamping that
        pass would strand the queue forever behind the marker's early
        return — a parked offset already differs from its start, so the
        wrapper needs no marker until the drain completes."""
        if start_offset is None:
            return read()
        if start_offset.get("snapshot_done"):
            return iter([]), start_offset
        records, offset = read()
        if offset:
            return records, offset
        return records, {**offset, "snapshot_done": True}

    def _delta_offset_shape_gate(
        self, table_name: str, table_options: dict | None, offset: dict
    ) -> bool:
        """The offset-shape check ahead of the delta predicate, both ways.

        Returns True when the offset is delta-shaped (carries a
        ``delta_link``/``next_link``), so a resumed delta stream takes the
        delta path even if ``delta_tracking`` is no longer set in options.

        The REVERSE shape check runs as a side effect: under ``auto``, a
        non-empty fallback-shaped offset (``snapshot_done`` / ``cursor``, no
        delta link) proves earlier batches of THIS stream ran the
        cursor/snapshot shape against the setup-frozen schema. If the first
        batch's probe was transient, nothing was stamped (``delta_ok`` rides
        definitive verdicts only) and a later re-probe coming back TRUE would
        flip the stream ONTO the delta path mid-stream — emitting
        ``_deleted``/``_lc_sequence`` columns the frozen schema never
        declared; the framework parser drops undeclared columns silently, so
        a delta tombstone MERGEs as a live row. So the fallback is pinned for
        the checkpoint's lifetime: the instance verdict makes
        ``_delta_active_for`` skip the re-probe, and ``_stamp_delta_verdict``
        persists ``delta_ok: false`` into the outgoing offset. Deliberately
        NOT written to the shared process/file cache — the pin is evidence
        about this checkpoint's frozen schema, not about the server.
        Cursor-configured tables are exempt (cursor wins deterministically —
        they never probe, so there is no flip to pin against)."""
        if "delta_link" in offset or "next_link" in offset:
            return True
        if (
            offset
            and not (table_options or {}).get("cursor_field")
            and self._delta_setting(table_options) == "auto"
        ):
            self._delta_capable[self._delta_cache_key(table_name, table_options)] = False
        return False

    def _stamp_delta_verdict(
        self, result: tuple, table_name: str, table_options: dict | None
    ) -> tuple:
        """Thread the definitive ``delta_tracking=auto`` probe verdict into the
        outgoing offset (``delta_ok``) so a stream that has decided its read
        shape once never re-decides differently.

        Only the FALLBACK direction needs the stamp: the delta path's own
        offsets carry ``delta_link``/``next_link`` and the offset-shape check
        in ``_read_table_dispatch`` routes them back to the delta path
        regardless of later verdicts. A bare cursor/snapshot offset, though,
        would let a later batch's re-probe (15-minute shared cache expired +
        a ``Preference-Applied``-flapping server) flip the stream ONTO the
        delta path mid-stream — emitting ``_deleted``/``_lc_sequence`` columns
        the setup-frozen schema never declared; the framework parser drops
        undeclared columns silently, so a delta tombstone MERGEs as a live
        all-null-column upsert over good destination values.

        Definitive verdicts only: a transient probe failure leaves
        ``_delta_capable`` unset and stamps nothing, so a blip can't pin
        delta off durably (the checkpoint is immortal). A cursor-configured
        table never probes (cursor wins deterministically) so nothing is
        stamped there. Explicit ``enabled``/``disabled`` stamps nothing and
        scrubs an existing flag — the same non-``auto`` reset discipline as
        the other persisted verdicts. Cost: a snapshot stream under an
        explicit ``delta_tracking=auto`` pays one extra re-read on the
        ``{}`` → ``{"delta_ok": False}`` transition, then quiesces as before
        (the stamp is deterministic per batch)."""
        records, offset = result
        if not isinstance(offset, dict):
            return result
        if self._delta_setting(table_options) != "auto":
            if "delta_ok" in offset:
                return records, {k: v for k, v in offset.items() if k != "delta_ok"}
            return result
        key = self._delta_cache_key(table_name, table_options)
        if key in self._delta_capable and "delta_ok" not in offset:
            return records, {**offset, "delta_ok": self._delta_capable[key]}
        return result

    def _seed_capability_caches(
        self,
        table_name: str,
        table_options: dict | None,
        start_offset: dict | None,
    ) -> None:
        """Seed per-instance capability verdicts from the resume offset so a
        reader the framework recreates each microbatch skips re-probing.

        Mirrors ``cursor_probe_ok`` (which the leaf-cursor path threads itself),
        but for the **OR-across-columns** verdict (``or_filter_ok``) and the
        **$batch** verdict (``batch_ok``, shared with the leaf-cursor path and
        the ``contained_fetch`` snapshot/stream walks), and the discovered
        **$batch chunk cap** (``batch_size_ok``, the working ops-per-request the
        adaptive shrink settled on after a "too many parts" rejection). Those
        are server-wide, so a single cached value serves every table this
        instance reads; ``expand_ok`` is PER TABLE and namespace-qualified
        (nesting depths and namespaces verify differently), so it seeds only
        under the offset's own :meth:`_expand_shared_key` — a scalar here
        would hand table A's verdict to table B and then bake it into B's
        offset. Persisted back by :meth:`_merge_capability_caches`."""
        off = start_offset or {}
        if "or_filter_ok" in off:
            self.__dict__["_or_filter_ok"] = bool(off["or_filter_ok"])
        if "batch_ok" in off:
            self.__dict__["_batch_supported"] = bool(off["batch_ok"])
        if "batch_size_ok" in off:
            self.__dict__["_batch_size_cap"] = _parse_batch_size_cap(
                off["batch_size_ok"], "resume checkpoint"
            )
        if off.get("expand_ok"):
            # PASS verdicts only — by design the offset never carries a fail
            # (see _merge_capability_caches), and a checkpoint poisoned with
            # ``expand_ok: false`` by a pre-fix build must not seed a memo
            # that skips the preflight forever; fails live in the 15-minute
            # shared cache so a fixed server gets re-probed.
            memo = self.__dict__.setdefault("_expand_supported", {})
            memo[self._expand_shared_key(table_name, table_options)] = True
        if isinstance(off.get("delta_ok"), bool) and self._delta_setting(table_options) == "auto":
            # The stream's OWN persisted delta verdict wins over the shared
            # cache and the probe: schema/read_table_metadata were frozen at
            # setup from one verdict, and a re-probe that flips it (15-min
            # shared cache expired + a Preference-Applied-flapping server)
            # would emit synthetic columns the declared schema lacks — the
            # framework parser drops them silently and a delta tombstone
            # then MERGEs as a live all-null row. See _stamp_delta_verdict.
            self._delta_capable[self._delta_cache_key(table_name, table_options)] = off["delta_ok"]

    def _merge_capability_caches(self, offset: dict, table_name: str | None = None) -> dict:
        """Thread the per-instance OR / $batch / batch-size verdicts into the
        returned offset so they survive the framework recreating the reader each
        microbatch.
        Only adds a flag once actually **determined** this instance (the probe
        ran), and never overwrites one a read path already wrote. ``expand_ok``
        is per-table (``table_name`` here is the namespace-qualified
        :meth:`_expand_shared_key`): only THIS table's own memoized verdict may
        ride its offset (another table's verdict baked in here would persist in
        the checkpoint and skip this table's preflight forever). Excluded from
        the no-progress comparison (see ``_finalize_cursor_read``), so baking in
        a verdict never reads as forward progress."""
        if not isinstance(offset, dict):
            return offset
        add: dict = {}
        if "_or_filter_ok" in self.__dict__ and "or_filter_ok" not in offset:
            add["or_filter_ok"] = self.__dict__["_or_filter_ok"]
        if "_batch_supported" in self.__dict__ and "batch_ok" not in offset:
            add["batch_ok"] = self.__dict__["_batch_supported"]
        if "_batch_size_cap" in self.__dict__ and "batch_size_ok" not in offset:
            add["batch_size_ok"] = self.__dict__["_batch_size_cap"]
        expand_memo = self.__dict__.get("_expand_supported") or {}
        if expand_memo.get(table_name) is True and "expand_ok" not in offset:
            # The PASS only. A fail baked into the checkpoint would be
            # immortal (offsets never expire) and skip the preflight even
            # after the server is fixed — fails belong in the 15-minute
            # shared cache, exactly like ``cursor_probe_ok`` (the README's
            # "the offset only ever carries the pass" contract).
            add["expand_ok"] = True
        return {**offset, **add} if add else offset

    def _cached_capability(self, key: str, table_name: str | None = None):
        """The process/file-cached verdict for ``key`` (``None`` when
        undetermined). Consulted by the ``_verify_*`` preflights AFTER the
        offset flag and the instance cache, BEFORE probing — so a connector
        instance the framework recreates each microbatch (or a forked batch
        worker within the file-cache TTL) skips the probe even on paths
        that carry no offset (contained snapshot streams, the batch
        reader)."""
        entry = _capability_cache_load(self.service_url)
        value = entry.get(key)
        if table_name is not None:
            return value.get(table_name) if isinstance(value, dict) else None
        return value

    def _store_capability(self, key: str, value, table_name: str | None = None) -> None:
        """Mirror a freshly determined DEFINITIVE verdict into the
        process/file capability cache (see :data:`_CAPABILITY_CACHE`).
        Callers keep the transient-vs-definitive discipline — a transient
        outcome must record nothing anywhere, so it never reaches here."""
        _capability_cache_store(self.service_url, key, value, table_name)

    def _scrub_nonauto_verdicts(self, offset: dict, table_options: dict | None) -> dict:
        """Drop persisted preflight verdicts whose governing option is **not**
        ``auto``, so re-selecting ``auto`` later re-runs the preflight instead of
        reusing a stale verdict. ``cursor_probe`` owns its nested-``$expand``
        probe verdict (``cursor_probe_ok``); ``expand_contained`` owns the
        expand-read capability verdict (``expand_ok``). The ``$batch`` verdicts
        (``batch_ok`` / ``batch_size_ok``) are **shared**: ``contained_fetch``'s
        full walks AND the ``cursor_probe`` ``auto`` cascade's hydrate both read
        and refresh them — so they're kept while ANY auto-mode consumer is live
        (``contained_fetch`` auto, or ``cursor_probe`` auto with a ``$batch``
        hydrate not suppressed by an explicit ``contained_fetch=single``/``1``)
        and scrubbed only when every consumer is pinned non-auto. Without the
        live-consumer carve-out, ``contained_fetch=batch:<N>`` + default
        ``cursor_probe`` would re-pay the preflight AND the adaptive
        size-discovery 400s every microbatch. (Trade-off: an all-pinned config
        re-runs its preflights each microbatch, since no verdict rides the
        offset.)"""
        if not isinstance(offset, dict):
            return offset
        drop: set[str] = set()
        cp_mode = self._cursor_probe_mode(table_options)
        if cp_mode != "auto":
            drop.add("cursor_probe_ok")
        # ``expand_contained`` owns the nested-$expand verdict (``expand_ok``):
        # an explicit ``true``/``false`` scrubs it so a later switch back to
        # ``auto`` (the unset DEFAULT, which keeps the verdict) re-runs the
        # preflight.
        if self._expand_contained_mode(table_options) != "auto":
            drop.add("expand_ok")
        # ``pagination`` owns the OR-across-columns keyset verdict
        # (``or_filter_ok``): an explicit mode that never CONSUMES it
        # (``skip`` pages positionally, ``nextlink`` follows server links)
        # scrubs it — this is also the user's only escape hatch for a
        # wrongly-false verdict persisted by an old checkpoint (pin
        # ``pagination=skip`` for one batch, then unpin), since the offset
        # copy otherwise never expires.
        if (table_options or {}).get("pagination", "").strip().lower() in ("skip", "nextlink"):
            drop.add("or_filter_ok")
        batch_live = self._contained_fetch_is_auto(table_options) or (
            cp_mode == "auto" and not self._contained_fetch_forces_single(table_options)
        )
        if not batch_live:
            drop |= {"batch_ok", "batch_size_ok"}
        if not drop:
            return offset
        # Shared-cache reset for the SERVER-WIDE ``$batch`` verdicts only:
        # a value actually present in the offset means the user just switched
        # this option away from ``auto`` (the transition batch). These keys
        # aren't table-scoped, so purge conservatively — transition-driven —
        # to avoid churning a sibling table's live ``auto`` consumer. The
        # per-table verdicts (``expand_ok`` / ``cursor_probe_ok``) are reset
        # separately and unconditionally by ``_purge_nonauto_table_verdicts``
        # (table-scoped, so it also safely covers the bare-offset snapshot /
        # batch-reader paths this offset scrub can't see).
        recorded_server_wide = drop & {"batch_ok", "batch_size_ok", "or_filter_ok"} & offset.keys()
        if recorded_server_wide:
            _capability_cache_drop(self.service_url, recorded_server_wide)
        if "or_filter_ok" in drop:
            # Also clear the instance memo so THIS read doesn't keep consuming
            # the verdict it just scrubbed from the outgoing offset.
            self.__dict__.pop("_or_filter_ok", None)
        return {k: v for k, v in offset.items() if k not in drop}

    def _purge_nonauto_table_verdicts(self, table_name: str, table_options: dict | None) -> None:
        """Reset the per-table shared-cache verdicts (``expand_ok`` /
        ``cursor_probe_ok``) whose governing option is pinned non-``auto``, so a
        later switch back to ``auto`` re-runs the preflight rather than reusing
        the cached verdict. Unlike the offset scrub, this runs on **every**
        contained read — not just an offset-carrying transition — so it also
        covers the contained snapshot stream and the batch reader, whose bare /
        absent offsets can't trigger the scrub. Table-scoped (pinning one table
        never disturbs a sibling's verdict) and idempotent (a no-op, no file
        rewrite, once the entry is gone)."""
        if self._expand_contained_mode(table_options) != "auto":
            _capability_cache_drop(
                self.service_url,
                {"expand_ok"},
                table_name=self._expand_shared_key(table_name, table_options),
            )
        if self._cursor_probe_mode(table_options) != "auto":
            segments = self._table_segments(table_name)
            if segments is not None:
                key = self._cursor_probe_shared_key(
                    segments, (table_options or {}).get("namespace")
                )
                _capability_cache_drop(self.service_url, {"cursor_probe_ok"}, table_name=key)

    def _with_capabilities(
        self,
        result: tuple,
        table_options: dict | None = None,
        table_name: str | None = None,
    ) -> tuple:
        """Wrap a ``(records, offset)`` read result, threading capability verdicts
        into the offset (see :meth:`_merge_capability_caches`; ``table_name``
        scopes the per-table ``expand_ok`` merge via its namespace-qualified
        key) and then scrubbing any whose governing option is non-``auto``
        (see :meth:`_scrub_nonauto_verdicts`)."""
        records, offset = result
        key = self._expand_shared_key(table_name, table_options) if table_name is not None else None
        offset = self._merge_capability_caches(offset, key)
        return records, self._scrub_nonauto_verdicts(offset, table_options)

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
        # PK-only ``$orderby`` so server skiptoken paging is stable across
        # pages — OData v4 §11.2.5.7 doesn't promise a stable default
        # order, and a value-based skiptoken over an unstable sort can
        # drop or duplicate rows mid-scan. Empty (keyless entity) → no
        # ``$orderby`` appended.
        namespace = (table_options or {}).get("namespace")
        pk_order = ",".join(f"{pk} asc" for pk in self._primary_keys_for(table_name, namespace))
        url = self._build_url(
            table_name,
            table_options,
            extra_filter=segment_filters.get(0),
            order_by=pk_order or None,
        )
        return self._fetch_pages(url, self._edm_types_for_table(table_name, namespace)), {}

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
        if start_offset is None:
            # Batch reader: offset discarded, ``since`` is None (no
            # ``cursor gt`` filter), no cap and no no-progress guard — so
            # the watermark, same-cursor trim and ``records`` buffer the
            # streaming path builds all serve nothing. Stream pages
            # straight through so peak memory is one page, not the whole
            # table. See ``read_table`` for why the cap is force-disabled.
            return self._stream_incremental_flat(table_name, table_options, cursor_field), {}
        since = start_offset.get("cursor") if start_offset else None
        segment_filters = _resolve_segment_filters(table_options, [table_name])
        extra_filter = _combine_filters(
            self._cursor_filter(
                cursor_field,
                since,
                edm_type=self._edm_types_for_table(
                    table_name, (table_options or {}).get("namespace")
                ).get(cursor_field),
            ),
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
        max_records = _parse_max_records(table_options)
        # ``cursor_nulls`` policy: ``effective`` yields the value used for
        # filtering/trim/watermark (a synthetic floor for nulls under
        # ``coalesce``); ``skip_null`` drops null-cursor rows under
        # ``ignore``. The emitted row is never mutated — its cursor column
        # keeps the real null.
        skip_null, effective = self._make_cursor_resolver(
            table_name, namespace, cursor_field, table_options
        )

        records: list[dict] = []
        truncated = False
        for row in self._fetch_pages(url, self._edm_types_for_table(table_name, namespace)):
            if skip_null and row.get(cursor_field) is None:
                continue
            rec_cursor = effective(row)
            # Chronological, not lexical: a server that
            # renders fractional seconds value-dependently puts ``…00.5Z``
            # lexically BEFORE ``…00Z`` — a raw ``<=`` would drop the newer
            # row the server correctly returned, permanently.
            if (
                since is not None
                and rec_cursor is not None
                and _cursor_at_or_before_for_refilter(rec_cursor, since)
            ):
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
            # Every record in this batch shares one cursor value (including
            # an all-null-cursor batch, which trims to empty here and is
            # kept as-is; under cursor_nulls=coalesce the watermark still
            # advances via the synthetic effective value below).
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
        cursors = [effective(r) for r in records if effective(r) is not None]
        end_offset = self._cursor_max_end_offset(cursors, since)
        return self._finalize_cursor_read(
            start_offset, end_offset, records, table_name, cursor_field
        )

    def _stream_incremental_flat(
        self, table_name: str, table_options: dict[str, str], cursor_field: str
    ) -> Iterator[dict]:
        """Lazy batch-mode flat cursor read.

        Mirrors ``_read_incremental``'s per-row work minus everything the
        batch reader makes moot: no ``cursor gt`` filter (``since`` is
        None), no cap, no same-cursor trim, no watermark, no no-progress
        guard. The only per-row behaviour kept is ``cursor_nulls=ignore``
        null-skipping (``coalesce`` emits the real null as-is here, since
        nothing consumes the synthetic ``effective`` value). Pages stream
        through ``_fetch_pages`` so peak memory is one page."""
        namespace = (table_options or {}).get("namespace")
        segment_filters = _resolve_segment_filters(table_options, [table_name])
        order_terms = [f"{cursor_field} asc"]
        for pk in self._primary_keys_for(table_name, namespace):
            if pk != cursor_field:
                order_terms.append(f"{pk} asc")
        url = self._build_url(
            table_name,
            table_options,
            extra_filter=segment_filters.get(0),
            order_by=",".join(order_terms),
        )
        skip_null, _effective = self._make_cursor_resolver(
            table_name, namespace, cursor_field, table_options
        )
        for row in self._fetch_pages(url, self._edm_types_for_table(table_name, namespace)):
            if skip_null and row.get(cursor_field) is None:
                continue
            yield row

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
        max_records = _parse_max_records(table_options)

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
            if prev_next_link is not None and prev_delta_link is not None:
                # The parked mid-pagination link expired, but the offset
                # retained the prior delta_link exactly for this: replay the
                # changes-since window instead of re-reading the whole entity
                # set. Rows between the two links are re-fetched — dup-safe.
                # If THAT link 410s too, the recursion's next level has no
                # next_link and falls to the full re-bootstrap below.
                return self._read_incremental_delta(
                    table_name, {"delta_link": prev_delta_link}, table_options
                )
            # 410 Gone on the stored delta link → re-bootstrap from
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

        if new_delta_link is None and prev_delta_link is None:
            # Bootstrap reached end of stream without a deltaLink. Server is
            # misbehaving (spec requires the terminal page to carry one).
            raise RuntimeError(
                f"OData delta bootstrap for {table_name!r} ended without an "
                f"@odata.deltaLink. The server may have aborted change "
                f"tracking. Set delta_tracking=disabled to fall back to "
                f"snapshot or cursor-based reads."
            )
        if prev_delta_link is not None and (
            new_delta_link is None or new_delta_link == prev_delta_link
        ):
            # ``records`` is non-empty here: the empty-record cases returned
            # above (Graph-rotation guard / cap park). Change records with a
            # change cursor that OMITTED the terminal link or did NOT advance
            # both mean every future trigger re-fetches the SAME set forever
            # (MERGE dedupes, but the stream churns without progressing).
            # Mirror the cursor paths' no-progress raise instead of looping.
            shape = (
                "no terminal @odata.deltaLink"
                if new_delta_link is None
                else "the SAME @odata.deltaLink as the prior batch"
            )
            raise RuntimeError(
                f"OData delta read for {table_name!r} returned {len(records)} "
                f"change records but {shape} — the server is not advancing "
                f"its change cursor, so the stream would re-read this change "
                f"set forever. Set delta_tracking=disabled and use "
                f"cursor-based incremental instead."
            )
        return iter(records), {"delta_link": new_delta_link}

    def _build_delta_record(
        self,
        item: dict,
        primary_keys: list[str],
        *,
        tombstone: bool | None = None,
        key_types: dict[str, str] | None = None,
        pad_fields: frozenset | None = None,
        entity_set: str | None = None,
    ) -> dict:
        """Translate one delta payload entry into the emitted record shape.

        - Tombstones (``@removed`` entries, or v4.0-format ``$deletedEntity``
          entries flagged by the caller) become a record carrying the
          primary-key fields plus ``_deleted=True``. Keys are taken from the
          INLINE properties when present (Graph style); otherwise parsed out
          of the entry's ``@odata.id`` / ``id`` entity reference (the
          v4.01/v4.0 spec shapes carry the key ONLY there). A tombstone whose
          keys resolve to None raises — a keyless tombstone MERGEs against
          nothing, silently losing the deletion. Every remaining schema
          column (``pad_fields``) is padded with an EXPLICIT ``None``: the
          framework parser rejects a non-nullable column that is *absent*
          but accepts an explicit null, and a tombstone legitimately carries
          nothing but its keys — without the padding the first delete on any
          schema with a ``Nullable="false"`` non-key property kills the
          batch.
        - Regular adds/changes pass through with all ``@odata.*`` control
          properties stripped and ``_deleted=False``.

        Every emitted record gets ``_lc_sequence`` — a strictly monotonic
        string — so apply_changes has a deterministic sequence_by column.
        """
        is_tombstone = tombstone if tombstone is not None else "@removed" in item
        if is_tombstone:
            record: dict = {pk: item.get(pk) for pk in primary_keys}
            if any(v is None for v in record.values()):
                id_text = item.get("@odata.id") or item.get("id")
                from_id = (
                    self._tombstone_keys_from_id(
                        id_text, primary_keys, key_types, entity_set=entity_set
                    )
                    if isinstance(id_text, str)
                    else None
                )
                if from_id:
                    for pk in primary_keys:
                        if record.get(pk) is None:
                            record[pk] = from_id.get(pk)
            if primary_keys and any(record.get(pk) is None for pk in primary_keys):
                raise RuntimeError(
                    f"OData delta tombstone carries no resolvable primary key "
                    f"(need {primary_keys}, got inline keys "
                    f"{ {pk: item.get(pk) for pk in primary_keys} }, entity "
                    f"reference {item.get('@odata.id') or item.get('id')!r}). "
                    f"A keyless tombstone would MERGE against nothing and the "
                    f"deletion would be silently lost. If the server's "
                    f"tombstone format differs, set delta_tracking=disabled "
                    f"and use cursor/snapshot reads."
                )
            for name in pad_fields or ():
                record.setdefault(name, None)
            record[_DELETED_COL] = True
        else:
            # Strip control information: bare annotations (``@odata.type``)
            # AND property-scoped ones (``Prop@odata.type``) — the OData JSON
            # format spells all control info with ``@``, and property names
            # (CSDL SimpleIdentifiers) can never contain it.
            record = {k: v for k, v in item.items() if "@" not in k}
            record[_DELETED_COL] = False
        record[_SEQUENCE_COL] = _next_sequence()
        return record

    def _tombstone_keys_from_id(
        self,
        id_text: str,
        primary_keys: list[str],
        key_types: dict[str, str] | None,
        entity_set: str | None = None,
    ) -> dict | None:
        """Parse PK values out of a tombstone's entity reference — the
        inverse of ``_format_key_predicate``, for ids like
        ``Customers('ALFKI')``, ``…/Orders(OrderID=1,Lang='en')`` or a full
        absolute URL. Returns ``None`` when no key predicate is found or the
        shape doesn't match the PK list; values are coerced by declared Edm
        type (quoted strings un-escaped, numerics parsed) so the emitted
        tombstone MERGE-matches the upserts' JSON-decoded values.

        When ``entity_set`` is given, the reference's collection segment must
        NAME that set (exact, or as a dotted suffix for container-qualified
        forms): a cross-set reference (``…/Suppliers(77)`` in a ``Customers``
        feed — spec-unreachable, but the failure mode is a DELETE applied to
        the wrong key, the worst destination-corruption class) is treated as
        unresolvable, so the caller's loud keyless-tombstone raise fires
        instead of a silent misapplied delete."""
        path = id_text.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        seg = path.rsplit("/", 1)[-1]
        if "%" in seg:
            seg = unquote(seg)
        open_idx = seg.find("(")
        if open_idx < 0 or not seg.endswith(")"):
            return None
        if entity_set:
            coll = seg[:open_idx]
            if coll != entity_set and not coll.endswith(f".{entity_set}"):
                return None
        parts = _split_key_predicate(seg[open_idx + 1 : -1])
        if not parts:
            return None
        types = key_types or {}
        out: dict = {}
        named = [p for p in parts if _KEY_EQ_RE.match(p)]
        if len(named) == len(parts):
            for part in parts:
                name, _, raw = part.partition("=")
                name = name.strip()
                out[name] = _coerce_key_literal(raw.strip(), types.get(name))
            return out if set(out) >= set(primary_keys) else None
        if len(parts) == 1 and len(primary_keys) == 1:
            return {primary_keys[0]: _coerce_key_literal(parts[0], types.get(primary_keys[0]))}
        return None

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
        # No $top on the bootstrap URL — a total-result limit ends change
        # tracking at page_size rows (see the delta dispatch branch). An
        # explicit page_size rides the Prefer header as odata.maxpagesize,
        # the spec's per-RESPONSE sizing hint, which servers may honor or
        # ignore without affecting the result set's completeness.
        opts_no_top = {k: v for k, v in (table_options or {}).items() if k != "page_size"}
        prefer = _DELTA_PREFER
        page_size = (table_options or {}).get("page_size")
        if page_size:
            prefer = f"{_DELTA_PREFER}, odata.maxpagesize={int(page_size)}"
        return (
            self._build_url(table_name, opts_no_top, extra_filter=segment_filters.get(0)),
            {"Prefer": prefer},
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
        expected_fields = self._delta_expected_fields(table_name, table_options)
        # PK Edm types for entity-reference tombstones (typed literal
        # coercion — see _tombstone_keys_from_id). Best-effort.
        key_types = self._edm_types_for_table(table_name, (table_options or {}).get("namespace"))
        current_url: str | None = url

        while current_url:
            headers = initial_headers if (page_index == 0 and initial_headers) else None
            kwargs: dict[str, Any] = {"headers": headers} if headers else {}
            resp, payload = self._delta_fetch_page(
                session,
                current_url,
                kwargs,
                allow_410=bool(prev_delta_link or prev_next_link),
                # Only the walk's FIRST fetch hits the checkpoint-stored URL;
                # deeper pages follow links minted by THIS walk's responses,
                # so the expired-stored-token curation must not claim them.
                stored_link=bool(prev_delta_link or prev_next_link) and page_index == 0,
            )
            if resp is None:
                return [], None, None, True

            if not bootstrap_verified:
                self._verify_delta_bootstrap(resp, table_name)
                bootstrap_verified = True

            self._delta_collect_page_records(
                payload=payload,
                records=records,
                primary_keys=primary_keys,
                table_name=table_name,
                expected_fields=expected_fields,
                key_types=key_types,
            )
            fetched_url = resp.url
            current_url, new_delta_link, carry_next_link = self._delta_advance_links(
                payload=payload,
                resp_url=resp.url,
                records=records,
                max_records=max_records,
                new_delta_link=new_delta_link,
                carry_next_link=carry_next_link,
            )
            if current_url is not None and current_url == fetched_url:
                # Self-referential ``@odata.nextLink`` — the server handed back
                # the URL we just fetched. Stop rather than loop forever; the
                # caller persists ``new_delta_link`` (or falls back to the prior
                # one) so the next run still resumes.
                _LOG.warning(
                    "delta walk: server returned a self-referential "
                    "@odata.nextLink for %r; stopping to avoid an infinite loop.",
                    fetched_url,
                )
                break
            page_index += 1

        return records, new_delta_link, carry_next_link, False

    def _delta_fetch_page(
        self,
        session: requests.Session,
        url: str,
        kwargs: dict,
        allow_410: bool,
        stored_link: bool = False,
    ) -> tuple[requests.Response | None, dict | None]:
        """GET + decode one delta page, retrying corrupt-200 JSON bodies.

        Mirrors :meth:`_fetch_page_payload`'s decode retry (some sources
        emit 200s with truncated bodies under load — see there); the delta
        walk can't reuse it directly because it needs per-page headers and
        the 410 rebootstrap escape. Returns ``(None, None)`` when a 410
        fired with a stored link to rebootstrap from.
        """
        for attempt in range(self.max_retries + 1):
            resp = self._http_get(session, url, **kwargs)
            if resp.status_code == 410 and allow_410:
                return None, None
            if stored_link and 400 <= resp.status_code < 500:
                # A non-410 4xx on the STORED delta/next link itself (the
                # spec prescribes 410 for an expired token, but real
                # gateways answer 404/400 too). The checkpoint pins this
                # link, so a bare HTTPError would re-raise unexplained
                # every trigger forever — curate it instead. Deliberately
                # NOT an auto-rebootstrap: silently full-re-reading on any
                # 4xx would mask genuine config/auth errors; 410 stays the
                # only auto-recovery trigger. Fresh links minted mid-walk
                # keep the bare raise below — they aren't persisted
                # anywhere, so "run a full refresh" would be the wrong
                # remedy for what is a transient server anomaly.
                try:
                    _raise_for_status_with_body(resp, url)
                except requests.HTTPError as exc:
                    raise RuntimeError(
                        f"The stored OData change-tracking link for this stream was "
                        f"rejected with HTTP {resp.status_code} ({exc}). The delta "
                        f"token has likely expired or been invalidated (the spec "
                        f"prescribes 410 for this; some gateways answer "
                        f"{resp.status_code}). The checkpoint pins this link, so the "
                        f"stream cannot recover on its own: run a full refresh to "
                        f"restart change tracking from a fresh bootstrap, or set "
                        f"delta_tracking=disabled and use cursor/snapshot reads. "
                        f"Note a re-bootstrap cannot recover deletions that happened "
                        f"while the token was expired — see the delta docs."
                    ) from exc
            _raise_for_status_with_body(resp, url)
            try:
                return resp, _decode_json_with_body(resp, url)
            except json.JSONDecodeError as exc:
                if attempt >= self.max_retries:
                    _LOG.error(
                        "OData delta JSON decode failed after %d attempts on GET %s — %s",
                        attempt + 1,
                        url,
                        exc.msg,
                    )
                    raise
                _LOG.warning(
                    "OData delta JSON decode failed on GET %s (%s) — retrying (%d/%d)",
                    url,
                    exc.msg,
                    attempt + 1,
                    self.max_retries,
                )
        raise AssertionError("unreachable: retry loop returns or raises")

    def _verify_delta_bootstrap(self, resp: requests.Response, table_name: str) -> None:
        """Confirm the server actually honored ``Prefer: odata.track-changes``."""
        applied = resp.headers.get("Preference-Applied", "")
        if _DELTA_PREFER not in applied.lower():
            raise RuntimeError(
                f"OData server did not honor 'Prefer: odata.track-changes' "
                f"for {table_name!r} (response missing 'Preference-Applied' "
                f"header). Under delta_tracking=auto this means the probe's "
                f"acknowledgement was inconsistent (a flapping load "
                f"balancer?); under delta_tracking=enabled the server simply "
                f"doesn't support change tracking for this entity set. Set "
                f"delta_tracking=disabled (or auto) and use cursor-based "
                f"incremental instead."
            )

    def _delta_collect_page_records(
        self,
        *,
        payload: dict,
        records: list[dict],
        primary_keys: list[str],
        table_name: str,
        expected_fields: frozenset,
        key_types: dict[str, str] | None = None,
    ) -> None:
        """Append every delta record from ``payload`` (one whole page).

        Tombstones come in two wire shapes: the v4.01 JSON format's
        ``@removed`` control property, and the v4.0 format's
        ``$deletedEntity``-context entry (``@odata.context`` ending in
        ``/$deletedEntity``, key carried in ``id``, no ``@removed`` at
        all). Both are routed to the tombstone branch — a v4.0 deleted
        entry misread as a regular entity would trip the sparse-entity
        guard with a misleading "partial updates" diagnosis.

        Deliberately NOT capped mid-page: ``max_records_per_batch`` is
        enforced at page boundaries by :meth:`_delta_advance_links`
        (stop following ``@odata.nextLink`` once the cap is reached).
        Breaking mid-page would silently drop the tail of the current
        page — the persisted ``carry_next_link`` points at the NEXT
        page, so the skipped rows would never be re-fetched (permanent
        loss during bootstrap). The cap may therefore overshoot by at
        most one server page; MERGE dedupes any overlap.

        The sparse-entity guard runs on EVERY non-tombstone entry, not
        just the first: mixed payloads are the norm for real delta
        services (full entities for creates, changed-properties-only for
        updates), so one full-bodied create at the head of the batch must
        not wave the sparse updates behind it through to a NULL-writing
        MERGE. ``expected_fields`` is precomputed once per walk, so the
        per-item cost is one set difference.
        """
        for item in payload.get("value") or []:
            if not isinstance(item, dict):
                # Spec-invalid entry (null / scalar in the value array):
                # raise a precise error instead of an AttributeError deep
                # in the tombstone sniff.
                raise RuntimeError(
                    f"OData delta response for {table_name!r} carried a "
                    f"malformed entry in 'value': expected an object, got "
                    f"{item!r}."
                )
            context = str(item.get("@odata.context", "")).lower()
            is_tombstone = "@removed" in item or "$deletedentity" in context
            if not is_tombstone and (
                context.endswith("/$link") or context.endswith("/$deletedlink")
            ):
                # v4.01 relationship-change entries (added/deleted LINKS,
                # carrying source/relationship/target — not entity rows).
                # This connector never ingests navigation properties on the
                # delta path, so relationship changes can't affect emitted
                # rows — skip them. Without this they fell into the regular-
                # entity branch and died in the sparse-entity guard with a
                # misleading "missing properties" diagnosis, blocking delta
                # entirely against servers that report link changes. A
                # contradictory entry carrying BOTH @removed and a link
                # context takes the tombstone branch instead (its keys
                # resolve-or-raise — loud, never a silently dropped delete).
                _LOG.debug(
                    "OData delta for %r: skipping relationship-change entry "
                    "(context %r) — navigation properties are not ingested.",
                    table_name,
                    item.get("@odata.context"),
                )
                continue
            if not is_tombstone:
                self._check_no_sparse_entity(item, table_name, expected_fields)
            records.append(
                self._build_delta_record(
                    item,
                    primary_keys,
                    tombstone=is_tombstone,
                    key_types=key_types,
                    pad_fields=expected_fields,
                    entity_set=table_name,
                )
            )

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
        both pagination links). The cap check runs AFTER the page was
        appended in full (see :meth:`_delta_collect_page_records`), so
        ``carry_next_link`` — the link to the next page — never skips
        rows: the cap overshoots by at most one server page instead.
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
            if raw_next:
                # Spec-violating page carrying BOTH links (they're mutually
                # exclusive per page). Prefer the continuation — stopping at
                # the deltaLink would silently drop every trailing page's
                # rows (absent until they next change, which the committed
                # delta cursor may consider already delivered) — and retain
                # the deltaLink like the cap-hit branch above does.
                return urljoin(resp_url, raw_next), new_delta_link, carry_next_link
            return None, new_delta_link, carry_next_link
        if raw_next:
            return urljoin(resp_url, raw_next), new_delta_link, carry_next_link
        return None, new_delta_link, carry_next_link

    def _delta_expected_fields(
        self, table_name: str, table_options: dict[str, str] | None
    ) -> frozenset:
        """The key set every non-tombstone delta entity must carry: the
        declared schema for the table, less any selection imposed by
        ``$select``, less the synthetic ``_deleted`` / ``_lc_sequence``
        columns we add ourselves, and less any ``Edm.Stream`` properties —
        stream values are media references the JSON payload NEVER carries
        (§11.2.4), so demanding them would fail every healthy entity with
        the sparse-entity error's wrong "partial updates" diagnosis.
        Computed once per delta walk and passed into the per-item
        :meth:`_check_no_sparse_entity`."""
        namespace = (table_options or {}).get("namespace")
        select = (table_options or {}).get("select")
        if select:
            expected = {c.strip() for c in select.split(",") if c.strip()}
        else:
            expected = {f.name for f in self._fields_for(table_name, namespace)}
        edm_types = self._edm_types_for_table(table_name, namespace)
        streams = {name for name, t in edm_types.items() if t == "Edm.Stream"}
        return frozenset(expected - {_DELETED_COL, _SEQUENCE_COL} - streams)

    def _check_no_sparse_entity(
        self,
        item: dict,
        table_name: str,
        expected: frozenset,
    ) -> None:
        """Refuse silently-corrupting sparse delta responses.

        OData v4 §11.4 lets a delta payload return only the *changed*
        properties on an updated entity. That sounds harmless until you
        realize the connector emits the dict as-is to Spark, which
        treats absent fields as NULL — overwriting good destination
        values with nulls on every partial update. The damage is silent
        and not recoverable from the destination table alone.

        We can't safely apply partial updates in v1, so refuse them up
        front with an actionable error. Runs on EVERY non-tombstone
        entry (see :meth:`_delta_collect_page_records` for why first-
        entry-only sampling is unsafe on mixed create/update payloads).
        """
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
          4. no declared primary key → ``enabled`` raises, ``auto`` falls
             back without probing (delta rows MERGE on the key: a keyless
             tombstone would MERGE against nothing and the deletion would
             be silently lost — the per-tombstone raise in
             ``_build_delta_record`` can't fire with an empty key list, so
             the gate lives here, eagerly and curated).
          5. ``delta_tracking=enabled`` → assume support; a probe failure
             surfaces at read time rather than here.
          6. ``delta_tracking=auto`` → probe once, cache, decide.
        """
        setting = self._delta_setting(table_options)
        if setting != "auto":
            # Explicit pin (enabled/disabled): purge the shared ``auto``
            # verdict so a later switch back to ``auto`` re-probes — the
            # same reset discipline as ``expand_ok``/``cursor_probe_ok``.
            # Idempotent and cheap (no file rewrite once the entry is
            # gone), so running on every non-auto call is fine.
            key = self._delta_cache_key(table_name, table_options)
            self._delta_capable.pop(key, None)
            shared_key = f"{key[0]}:{key[1]}" if key[0] else key[1]
            _capability_cache_drop(self.service_url, {"delta_ok"}, table_name=shared_key)
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
        if not self._primary_keys_for(table_name, (table_options or {}).get("namespace")):
            # Keyless entity type: every delta row (upsert or tombstone)
            # MERGEs on the primary key, so keyless delta is silent loss by
            # construction — and the per-tombstone raise is gated on a
            # non-empty key list, so it can never fire here. Enabled →
            # curated config error; auto → snapshot/cursor fallback with
            # no probe (the read shape must be decided identically at
            # schema-freeze and read time, and this decision is static).
            if setting == "enabled":
                raise ValueError(
                    f"delta_tracking=enabled requires a declared primary key on "
                    f"{table_name!r}: delta upserts and tombstones MERGE on the "
                    f"key, and the entity type declares none in $metadata — "
                    f"deletions would be silently lost. Use "
                    f"delta_tracking=disabled with cursor/snapshot reads."
                )
            return False
        if setting == "enabled":
            return True
        key = self._delta_cache_key(table_name, table_options)
        if key not in self._delta_capable:
            # Shared (process + file, 15-min TTL) cache first: schema
            # inference and the streaming read run in different forked
            # workers, and an instance-only verdict lets a server that
            # flaps its Preference-Applied ack between the two probes
            # desync the declared schema from the emitted rows (synthetic
            # columns declared-but-absent, or emitted-but-undeclared).
            # One persisted verdict keeps every process on one answer.
            shared_key = f"{key[0]}:{key[1]}" if key[0] else key[1]
            shared = self._cached_capability("delta_ok", table_name=shared_key)
            if isinstance(shared, bool):
                self._delta_capable[key] = shared
                return shared
            verdict = self._probe_delta_support(table_name, table_options)
            if verdict is None:
                # Transient failure — no verdict. Degrade THIS call to the
                # cursor/snapshot fallback and cache nothing, so the next
                # call re-probes instead of pinning delta off for the
                # instance's whole lifetime on a momentary blip (the same
                # definitive-only discipline as the other capability probes).
                return False
            self._delta_capable[key] = verdict
            self._store_capability("delta_ok", verdict, table_name=shared_key)
        return self._delta_capable[key]

    def _probe_delta_support(
        self, table_name: str, table_options: dict[str, str] | None
    ) -> bool | None:
        """Light-touch capability probe.

        Sends a small GET against the entity set with the
        ``Prefer: odata.track-changes`` header and inspects the response
        headers for ``Preference-Applied: odata.track-changes``. That
        header is the spec's positive acknowledgement that the server is
        honoring change tracking on this request.

        Returns a DEFINITIVE verdict only when the probe actually reached
        the server: ``True`` on a 200 acknowledging the preference,
        ``False`` when the server answered but didn't acknowledge (missing
        header, or a non-transient non-200). Returns ``None`` — no verdict,
        the caller caches nothing and re-probes next call — on a
        transport/auth failure or an exhausted transient, matching the
        definitive-only discipline of the other capability probes.
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
            return None  # transient/auth — no verdict, re-probe next call
        if resp.status_code in _TRANSIENT_HTTP_STATUSES:
            # Defensive: ``_http_get`` retries every transient status and
            # raises after the budget (caught above), so nothing should
            # reach here in practice — the membership test keeps the
            # definitive/transient split in one place regardless.
            return None  # transient status — no verdict, re-probe next call
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
        """Compose $top/$select/$filter/$orderby; shared across all URL builders.

        ``$top`` is emitted only when ``page_size`` is set. With no
        ``page_size`` the connector sends no ``$top`` at all and lets the
        server pick its own page size — some services reject or mishandle
        an explicit ``$top`` (e.g. a value above their per-page cap), and
        omitting it is the safe default. Server-driven paging via
        ``@odata.nextLink`` still walks the full collection either way.
        """
        opts = table_options or {}
        params = []
        if opts.get("page_size"):
            params.append(f"$top={opts['page_size']}")
        if opts.get("select"):
            # Strip per-column whitespace ("Id, Label" → "Id,Label") — the
            # validation set and the expand-leaf merge both strip, and a
            # strict server may 400 the padded wire form ($select=Id,%20Label).
            # A select that strips to NOTHING (","," ") emits no $select at
            # all rather than an empty param.
            cols = ",".join(c.strip() for c in opts["select"].split(",") if c.strip())
            if cols:
                params.append(f"$select={cols}")
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

    def _fetch_pages(self, url: str, edm_types: dict[str, str] | None = None) -> Iterator[dict]:
        """Walk a collection's pages, yielding raw JSON dicts (no coercion).

        Thin row-flattening wrapper over :meth:`_fetch_pages_with_links`,
        which handles the pagination strategy (nextlink / keyset / skip /
        auto). The whole collection is drained within this call: under the
        default ``auto`` a server that page-limits below ``$top`` without a
        continuation link is still fully drained (keep seeking until empty).
        ``edm_types`` (the collection's declared property types, when the
        caller has them) types the keyset-seek boundary literals.
        """
        for page_rows, _ in self._fetch_pages_with_links(url, edm_types):
            yield from page_rows

    def _parse_pagination(self, table_options: dict[str, str] | None) -> str:
        """Parse + validate the ``pagination`` table option.

        Defaults to ``auto``: follow ``@odata.nextLink`` while the server
        emits it (identical to ``nextlink`` for spec-compliant servers), but
        fall back to a keyset/skip continuation on any link-less page and keep
        seeking until empty — so a server that silently page-limits a response
        below the requested ``$top`` *without* a continuation link doesn't drop
        the remainder. ``auto`` forces a default ``page_size`` to size its
        requests (including on snapshot scans); a spec-compliant server that
        keeps emitting ``@odata.nextLink`` is followed directly with no extra
        request, while a link-omitting server costs one trailing empty request
        per collection.
        """
        raw = ((table_options or {}).get("pagination") or "auto").strip().lower()
        if raw not in _PAGINATION_MODES:
            raise ValueError(
                f"Invalid pagination={raw!r}. Expected one of: " f"{sorted(_PAGINATION_MODES)}."
            )
        return raw

    @staticmethod
    def _page_fp_repeated(prev_fp, fp, page_rows: list, url: str, mode: str) -> bool:
        """Period-1 no-progress check shared by both pagination loops: a
        non-empty page whose raw-item fingerprint equals the previous page's
        (server ignoring the seek/$skip, or a cyclic link handing back the
        same rows). Logs the actionable warning and returns True to stop.
        Complements :meth:`_page_url_cycled` — this catches SAME-page /
        advancing-token, that catches REPEATED-url / alternating-token."""
        if page_rows and prev_fp is not None and fp == prev_fp:
            # The "switch to nextlink" advice only makes sense from the
            # connector-driven modes; the nextlink loop is already on nextlink.
            advice = (
                ""
                if mode == "nextlink"
                else " Use pagination=nextlink if the server pages correctly."
            )
            _LOG.warning(
                "pagination=%s made no progress on %r: an identical page came back "
                "(server ignoring the seek/$skip, or a cyclic @odata.nextLink). "
                "Stopping to avoid an infinite loop; some rows may be unread.%s",
                mode,
                url,
                advice,
            )
            return True
        return False

    @staticmethod
    def _page_url_cycled(guard: "_PageCycleGuard", url: str, mode: str) -> bool:
        """Bounded-window continuation-URL repeat check shared by both
        pagination loops: logs the actionable warning and returns True when
        ``url`` has already been fetched this drain (a period-N token cycle
        the per-page fingerprint / self-reference guards miss). See
        :class:`_PageCycleGuard`."""
        if guard.seen_before(url):
            _LOG.warning(
                "pagination=%s made no progress on %r: a continuation URL repeated "
                "(the server is cycling @odata.nextLink/seek tokens). Stopping to "
                "avoid an infinite loop; some rows may be unread.",
                mode,
                url,
            )
            return True
        return False

    def _fetch_pages_with_links(
        self, url: str, edm_types: dict[str, str] | None = None
    ) -> Iterator[tuple[list[dict], str | None]]:
        """Page-aware fetch: yields ``(page_rows, next_url)`` per response,
        where ``next_url`` resumes the next page (``None`` at the end).

        The yielded ``next_url`` is an opaque resume checkpoint — callers
        park it to continue across batches without reconstructing query
        state. What it *is* depends on ``pagination`` (``self._pagination``,
        default ``nextlink``):

        * ``nextlink`` — the server's resolved ``@odata.nextLink`` (its own
          opaque skiptoken). Spec-compliant default.
        * ``keyset`` / ``skip`` / ``auto`` — a connector-built URL (a
          ``(k gt last)`` seek on the ``$orderby`` key set, or ``$top`` +
          ``$skip``), for servers that page-limit a response but omit the
          continuation link. See :meth:`_client_paginate_pages`.

        Under ``auto``, when the server has emitted no ``@odata.nextLink`` the
        walk keeps seeking past a short link-less page until empty, so a server
        that page-limits below ``$top`` while suppressing the continuation link
        is still fully drained.
        """
        mode = getattr(self, "_pagination", "nextlink")
        if mode != "nextlink":
            yield from self._client_paginate_pages(url, mode, edm_types)
            return
        session = self._get_session()
        next_url: str | None = url
        # No-progress guards: a server that hands back a self-referential or
        # cyclic ``@odata.nextLink`` would loop forever. Stop if the resolved
        # link points back at the URL we just fetched, or if a non-empty page
        # repeats the one before it (period-1) — or if ANY continuation URL
        # repeats within a bounded window (period-N alternation, which the
        # per-page checks miss). See :class:`_PageCycleGuard`.
        prev_fp: int | None = None
        cycle = _PageCycleGuard()
        while next_url:
            if self._page_url_cycled(cycle, next_url, "nextlink"):
                return
            resp, payload = self._fetch_page_payload(session, next_url)
            raw_items = payload.get("value") or []  # `or`: tolerate a spec-invalid null
            page_rows = [
                {k: v for k, v in item.items() if not k.startswith("@odata.")} for item in raw_items
            ]
            # Raw pre-strip fingerprint — see _client_paginate_pages for why
            # (identical projected pages must not false-positive the guard).
            fp = _pg_page_fingerprint(raw_items)
            if self._page_fp_repeated(prev_fp, fp, page_rows, next_url, "nextlink"):
                return
            prev_fp = fp
            raw_next = payload.get("@odata.nextLink")
            new_next = self._resolve_next_link(resp.url, raw_next) if raw_next else None
            if new_next is not None and new_next == next_url:
                _LOG.warning(
                    "pagination=nextlink: server returned a self-referential "
                    "@odata.nextLink for %r; stopping to avoid an infinite loop.",
                    next_url,
                )
                yield page_rows, None
                return
            yield page_rows, new_next
            next_url = new_next

    def _verify_or_filter_support(
        self,
        base_url: str,
        order_keys: list[str],
        sample_row: dict,
        edm_types: dict[str, str] | None = None,
    ) -> bool:
        """One-shot, per-instance probe: does the server accept an
        OR-across-**different-columns** ``$filter`` — the composite keyset-seek
        shape ``(k1 eq v1 and k2 gt v2) or (k1 gt v1)``?

        A single-key ``$orderby`` never builds an OR, so this short-circuits to
        ``True`` for ``len(order_keys) < 2``. For a composite seek it issues ONE
        auth-aware ``$top=1`` probe carrying the OR filter, built from
        ``sample_row`` so the literals are correctly typed. Mirrors the
        batch/expand preflight discipline — only a **definitive** outcome is
        cached/persisted (instance + shared process/file cache):

        * definitive pass — a **2xx**: the server accepts OR across columns;
        * definitive fail — a **non-transient 4xx** (e.g. Hexagon Smart API's
          400 "on different columns, only AND operators are supported"): the
          caller falls back to ``$skip`` (pagination mode B).

        A transient status (429/5xx) or a transport/auth failure is **not**
        evidence about OR support, so it fails **open** for this seek (assume
        supported — the real seek then surfaces any genuine error) and records
        **nothing**, so the next seek re-probes instead of durably pinning the
        slower ``$skip`` walk on a momentary blip. Going through
        :meth:`_http_get_once` (not a raw ``session.get``) means a 401/403 is
        raised as an auth failure and caught by this probe rather than misread
        as "OR unsupported"."""
        if len(order_keys) < 2:
            return True
        cached = self.__dict__.get("_or_filter_ok")
        if cached is not None:
            return cached
        cached = self._cached_capability("or_filter_ok")
        if cached is not None:
            self.__dict__["_or_filter_ok"] = cached
            return cached
        seek = _pg_keyset_filter(order_keys, sample_row, edm_types)
        if seek is None or " or " not in seek:
            return True  # no OR-across-columns built for this key set — nothing to probe
        probe = _pg_strip_query(
            _pg_set_query(
                _pg_keyset_seek_url(base_url, _pg_base_filter(base_url), seek),
                "$top",
                "1",
            ),
            "__pgbase",
        )
        try:
            resp = self._http_get_once(self._get_session(), probe)
        except Exception:  # transport error, or auth failure (PermissionError)
            return True  # not OR evidence — fail open this seek, record nothing
        # 408/429/5xx — transient, so a request timeout isn't misread as
        # "OR rejected" and durably persisted. The verdict outlives the
        # instance and its only reset is the explicit ``pagination=
        # skip/nextlink`` scrub (``_scrub_nonauto_verdicts``) — the same
        # discipline the _contained preflights follow.
        if resp.status_code in _TRANSIENT_HTTP_STATUSES:
            return True  # not OR evidence — fail open this seek, record nothing
        ok = not 400 <= resp.status_code < 500  # a non-transient 4xx = OR rejected
        self.__dict__["_or_filter_ok"] = ok
        self._store_capability("or_filter_ok", ok)
        return ok

    def _client_paginate_pages(
        self, url: str, mode: str, edm_types: dict[str, str] | None = None
    ) -> Iterator[tuple[list[dict], str | None]]:
        """Client-driven pagination for servers that don't (always) emit
        ``@odata.nextLink``. Yields ``(page_rows, next_url)`` like
        :meth:`_fetch_pages_with_links`, where ``next_url`` is a
        connector-built resume URL (``None`` at the end):

        * ``keyset`` — seek the next page with a ``(k gt last)`` predicate
          on the ``$orderby`` key set, rebuilt from the original URL each
          page (so a same-collection walk never accumulates seeks). Commits
          to ``$skip`` for the rest of the collection if a boundary value is
          null (no comparable seek).
        * ``skip`` — ``$top`` + ``$skip`` offset paging, continuing from any
          ``$skip`` already on the URL (so a parked checkpoint resumes at
          the right offset).
        * ``auto`` — trust ``@odata.nextLink`` whenever the server emits it;
          when a page arrives with no link, fall back to a keyset (when the
          ``$orderby`` has keys) or skip continuation and keep seeking until an
          empty page — so a server that page-limits below the requested
          ``$top`` while omitting the link is still drained fully.

        Termination (all client-driven modes, ``auto`` included): a continuation
        that returns an **empty** page means done — NOT a short non-empty one.
        A server may page-limit a response below the ``$top`` we request while
        omitting ``@odata.nextLink`` (e.g. ``SUPPRESS_NEXTLINK_WITH_TOP``), so a
        short page is not proof of exhaustion; the walk keeps seeking until
        empty. Cost: one trailing empty request per collection that genuinely
        ends on a short page. ``auto`` still short-circuits to the server's
        ``@odata.nextLink`` whenever one is present, so a spec-compliant server
        never incurs the extra request.

        ``$top`` must be present (callers force a default ``page_size``). A
        no-progress guard stops the walk if a continuation returns a page
        identical to the one it continued from (server ignoring the
        seek/``$skip``) or a self-referential ``@odata.nextLink`` — either
        would otherwise loop forever.
        """
        session = self._get_session()
        top = _pg_parse_top(url)
        order_keys = _pg_orderby_keys(url)
        can_keyset = mode in ("keyset", "auto") and bool(order_keys)
        base_skip = int(_pg_get_query(url, "$skip") or _pg_get_query(url, "%24skip") or 0)
        # Stable base $filter for keyset seeks — recovered from the private
        # __pgbase marker on a resume URL, so each new seek REPLACES the prior
        # one rather than AND-ing onto it across cap-resume batches (which
        # would grow the URL unboundedly). See _pg_keyset_seek_url.
        base_filter = _pg_base_filter(url)
        fetched = 0
        cur_url: str | None = url
        # No-progress guard, shared across all client-driven steps: a server
        # that ignores our seek/$skip (keyset/skip) or hands back a cyclic
        # @odata.nextLink (auto) would loop forever. Stop when a non-empty page
        # repeats the previous one — those rows were already emitted, so the
        # duplicate is dropped rather than re-yielded.
        prev_fp: int | None = None
        # Period-N backstop (see _PageCycleGuard): the fingerprint guard below
        # catches a server ignoring the seek (same page, advancing token); this
        # catches a server CYCLING continuation URLs (alternating tokens), which
        # the fingerprint misses because the alternating pages differ.
        cycle = _PageCycleGuard()
        # ``auto`` only: did the server emit an @odata.nextLink at any point in
        # this walk? A server either drives pagination via the link or it
        # doesn't — mixing isn't a real pattern. So once we've seen a link, a
        # later page with no link means the collection genuinely ended (stop, no
        # probe). If we've NEVER seen one, a link-less page is ambiguous (could
        # be a server that page-limits below $top while suppressing the link),
        # so auto falls back to the keyset/skip drain like the explicit modes.
        # This keeps spec-compliant nextLink flows free of any extra trailing
        # request while still draining link-omitting servers fully.
        saw_next_link = False
        while cur_url is not None:
            if self._page_url_cycled(cycle, cur_url, mode):
                return
            resp, payload = self._fetch_page_payload(session, cur_url)
            raw_items = payload.get("value") or []  # `or`: tolerate a spec-invalid null
            page_rows = [
                {k: v for k, v in item.items() if not k.startswith("@odata.")} for item in raw_items
            ]
            # Fingerprint the RAW items (annotations included): with a
            # low-cardinality $select two DISTINCT consecutive pages can be
            # identical after the @odata.* strip, and stripping first would
            # false-positive the guard and stop the walk with rows unread.
            # Per-entity annotations (@odata.id / etag) disambiguate for free
            # where the server emits them.
            fp = _pg_page_fingerprint(raw_items)
            if self._page_fp_repeated(prev_fp, fp, page_rows, cur_url, mode):
                return
            prev_fp = fp
            fetched += len(page_rows)
            raw_next = payload.get("@odata.nextLink")
            if mode == "auto" and raw_next:
                # Server is paginating — defer to its link for this step.
                saw_next_link = True
                nxt = self._resolve_next_link(resp.url, raw_next)
                if nxt == cur_url:
                    # Self-referential link (catches the empty-page case the
                    # fingerprint guard skips). Emit this page, then stop.
                    _LOG.warning(
                        "pagination=auto: server returned a self-referential "
                        "@odata.nextLink for %r; stopping to avoid an infinite "
                        "loop.",
                        cur_url,
                    )
                    yield page_rows, None
                    return
                yield page_rows, nxt
                cur_url = nxt
                continue
            if not page_rows:
                # Empty page ⇒ collection exhausted.
                yield page_rows, None
                return
            if top is None:
                # No $top to drive client paging, so take the one page. (This
                # branch is only reachable for a caller that didn't size the
                # request; the modes that need client paging force a $top.)
                yield page_rows, None
                return
            if mode == "auto" and saw_next_link and len(page_rows) < top and fetched < top:
                # The server drove this walk with @odata.nextLink and this SHORT
                # page carried none, *before* our $top budget was reached — a
                # spec-compliant pager signalling the genuine end of the
                # collection. Don't probe further (no trailing request); trust
                # the absent link.
                #
                # The ``fetched < top`` guard is essential: if the chain instead
                # terminated at exactly the budget (``fetched >= top``), the
                # absent link may signal $top exhaustion, NOT end-of-collection.
                # OData ``$top`` is a TOTAL-result limit (§11.2.5.3), and a
                # server may propagate the remaining budget through its skiptoken
                # nextLinks — e.g. Northwind: ``$top=1000`` → page 1's link
                # carries ``$top=500`` → after 1000 rows no further link, though
                # the collection has more. Trusting the short final page there
                # silently caps the table at ``$top`` rows. So we fall through to
                # the keyset/$skip seek below, which resumes past the budget and
                # drains the rest (each re-budgeted seek ends on a trailing empty
                # page). A *full* no-link page after we've seen links is likewise
                # ambiguous and falls through.
                yield page_rows, None
                return
            # auto (never saw a link) / keyset / skip: do NOT stop on a short
            # non-empty page — the server's page size may be below the $top we
            # requested while it omits @odata.nextLink (SUPPRESS_NEXTLINK_WITH_TOP
            # servers),
            # so a short page is not proof of exhaustion. Keep seeking until an
            # EMPTY page. ``auto`` already deferred to @odata.nextLink above
            # whenever the server emitted one; reaching here means it didn't, so
            # auto issues the same confirming seek/$skip as keyset/skip (one
            # trailing empty request per genuinely-ended collection). The
            # no-progress guard above bounds a server that ignores the
            # seek/$skip — auto then stops with this page's rows, exactly as the
            # old short-page default did, minus the dropped duplicate.
            if can_keyset and not self._verify_or_filter_support(
                url, order_keys, page_rows[-1], edm_types
            ):
                # Composite keyset seek would build an OR-across-columns filter
                # the server rejects (Hexagon Smart API). Drop to $skip (mode B)
                # for the rest of this collection — and, via the cached verdict,
                # every later walk this instance.
                _LOG.warning(
                    "pagination=%s: server rejected an OR-across-columns keyset "
                    "seek (composite $orderby %s); using $skip for %r. Set "
                    "pagination=skip to skip this probe.",
                    mode,
                    order_keys,
                    cur_url,
                )
                can_keyset = False
            if can_keyset:
                seek = _pg_keyset_filter(order_keys, page_rows[-1], edm_types)
                if seek is not None:
                    nxt = _pg_keyset_seek_url(url, base_filter, seek)
                else:
                    # Null boundary value — no comparable keyset seek;
                    # commit to offset paging for the rest of this walk so
                    # keyset and skip positions can't interleave.
                    can_keyset = False
                    # Strip any residual opaque token (a resumed foreign
                    # continuation) like the keyset path does — a URL
                    # carrying BOTH $skiptoken and $skip double-positions
                    # on servers that apply both.
                    nxt = _pg_set_query(
                        _pg_strip_positional(url), "$skip", str(base_skip + fetched)
                    )
            else:
                nxt = _pg_set_query(_pg_strip_positional(url), "$skip", str(base_skip + fetched))
            yield page_rows, nxt
            cur_url = nxt

    def _resolve_next_link(self, request_url: str, raw_next: str) -> str:
        """Resolve an ``@odata.nextLink`` against the request URL.

        Absolute links, root-absolute links (leading ``/``) and bare
        query references (``?...``) all resolve correctly with a plain
        ``urljoin`` against the request URL. The trap is a **relative
        path** nextLink on a contained-collection request: some servers
        (Hexagon SCApi, SAP Gateway, …) return it relative to the
        **service root** — ``Top(key)/Child(key)/Leaf?$skiptoken=...`` —
        rather than the current resource. Naively ``urljoin``-ing that
        against the deep request URL **duplicates the ancestor path**
        (``.../Leaf/Top(key)/Child(key)/Leaf?...``), so the next page
        404s and the read silently stops after page one — only visible
        on multi-page collections such as a full contained snapshot.

        Detect that form — the relative link restates the entity set
        that immediately follows the service root in the request URL —
        and resolve it against the service root instead. Genuinely
        resource-relative links (e.g. flat ``Customers?$skiptoken=...``
        or leaf-only ``Leaf?$skiptoken=...``) fall through to the
        standard request-URL resolution.
        """
        parsed = urlparse(raw_next)
        if parsed.scheme or raw_next.startswith("/") or raw_next.startswith("?"):
            return urljoin(request_url, raw_next)

        def _first_seg(path: str) -> str:
            seg = path.lstrip("/").split("/", 1)[0]
            # Strip any key predicate and query so ``Top(key)?x`` → ``Top``.
            return seg.split("(", 1)[0].split("?", 1)[0]

        root = self.service_url if self.service_url.endswith("/") else self.service_url + "/"
        root_path = urlparse(root).path
        req_path = urlparse(request_url).path
        after_root = req_path[len(root_path) :] if req_path.startswith(root_path) else req_path
        if after_root and _first_seg(raw_next) == _first_seg(after_root):
            # Service-root-relative: resolve against the root so the
            # ancestor path isn't doubled.
            return urljoin(root, raw_next)
        return urljoin(request_url, raw_next)

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
        # Drop the connector-private keyset base marker before it can reach
        # the server (it only exists to carry the stable base $filter across
        # cap-resume batches; see _pg_keyset_seek_url).
        url = _pg_strip_query(url, "__pgbase")
        attempts = self.max_retries + 1
        for attempt in range(attempts):
            resp = self._http_get(session, url)
            _raise_for_status_with_body(resp, url)
            try:
                return resp, _decode_json_with_body(resp, url)
            except json.JSONDecodeError as exc:
                if attempt >= self.max_retries:
                    _LOG.error(
                        "OData JSON decode failed after %d attempts on GET %s — %s",
                        attempt + 1,
                        url,
                        exc.msg,
                    )
                    raise
                _LOG.warning(
                    "OData JSON decode failed on GET %s (%s) — retrying (%d/%d)",
                    url,
                    exc.msg,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(self._backoff_delay(attempt))
        # Defensive: the loop above always returns or raises.
        raise RuntimeError(  # pragma: no cover
            f"Exhausted retries decoding JSON for {url!r}."
        )

    def _http_get(
        self, session: requests.Session, url: str, method: str = "GET", **kwargs: Any
    ) -> requests.Response:
        """GET (or other ``method``) with terminal auth handling and transient retries.

        ``method`` defaults to ``GET``; a ``POST`` (with ``json=``) routes the
        ``$batch`` endpoint through the same throttle/transient retry path as
        every read.

        Outer loop retries on two classes of transient failure, both
        capped by ``retry_max_delay_seconds`` per attempt:

        * **HTTP 408 / 429 / 500 / 502 / 503 / 504** — timeout, throttling,
          or transient server/proxy failure.
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

        ``_http_get_once`` enforces same-origin redirects and raises an
        actionable :class:`PermissionError` immediately on 401/403. The
        connector never mints or refreshes tokens: Unity Catalog refreshes
        OAuth tokens server-side and injects one at query start. Consequently,
        a token that expires mid-read terminates that read; the next query
        receives a fresh token. Auth failures are not retried here.
        """
        attempts = self.max_retries + 1
        for attempt in range(attempts):
            try:
                resp = self._http_get_once(session, url, method=method, **kwargs)
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
                _LOG.warning(
                    "OData transient %s on %s %s — retrying (%d/%d)",
                    type(exc).__name__,
                    method,
                    url,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(self._backoff_delay(attempt))
                continue
            if resp.status_code in _RETRYABLE_HTTP_STATUSES:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        self._transient_status_exhausted_error(resp, url, attempt + 1)
                    )
                _LOG.warning(
                    "OData %d on %s %s — retrying (%d/%d)",
                    resp.status_code,
                    method,
                    url,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(self._retry_after_delay(resp, attempt))
                continue
            return resp
        # Defensive: the loop always returns or raises before exiting.
        raise RuntimeError(  # pragma: no cover
            f"Exhausted retries for {url!r} without producing a response."
        )

    def _log_http_request(self, method: str, url: str) -> None:
        """Emit a one-line INFO log for the outgoing request when
        ``verbose_http_logging`` is enabled. No-op otherwise so the
        hot path stays free of formatting work."""
        if self.verbose_http_logging:
            _LOG.info("OData %s %s", method, url)

    def _log_http_response(self, method: str, url: str, resp: requests.Response) -> None:
        """Emit a one-line INFO log for the response when
        ``verbose_http_logging`` is enabled. Includes the status code,
        ``Content-Length``, and the first ``verbose_http_log_body_chars``
        chars of the body. **Source data ends up in the log stream**
        when this is on — that's the point — so don't enable it for
        steady-state pipelines."""
        if not self.verbose_http_logging:
            return
        body_snippet = _truncate(resp.text or "", self.verbose_http_log_body_chars)
        _LOG.info(
            "OData %s %s → %d (%s bytes); body: %s",
            method,
            url,
            resp.status_code,
            resp.headers.get("Content-Length", "?"),
            body_snippet or "(empty)",
        )

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff capped at ``retry_max_delay_seconds``,
        jittered to 50–100 % of the capped value.

        Used for transient network failures where the server never
        sent a response, so there's no ``Retry-After`` to honour (the
        429/503 path prefers the server hint via
        ``_retry_after_delay``). The jitter de-synchronizes the
        ``num_partitions`` executor tasks a throttling source knocked
        back in the same instant — without it they all retry in
        lockstep at 1, 2, 4 … s and re-trigger the throttle together.
        """
        capped = min(float(2**attempt), float(self.retry_max_delay_seconds))
        return capped * random.uniform(0.5, 1.0)

    def _require_same_origin(self, url: str) -> None:
        """Refuse to send the credential-bearing session off the
        ``service_url`` origin. Every request funnels through here, so a
        server-supplied ``@odata.nextLink`` (or any other URL the connector
        follows) pointing at a different scheme/host/port raises instead of
        leaking the ``Authorization`` header (or ``session.auth`` /
        api-key header) to that host — the protection ``requests`` applies
        to cross-host *redirects* (``rebuild_auth``) doesn't engage when
        the connector builds the next request directly."""
        origin = _url_origin(url)
        if origin != self._service_origin:
            raise PermissionError(
                f"OData connector refused to follow a URL to a different "
                f"origin than 'service_url'. The credentialed session may "
                f"only talk to {self._service_origin[0]}://"
                f"{self._service_origin[1]}"
                f"{'' if self._service_origin[2] is None else ':' + str(self._service_origin[2])}"
                f", but was asked to reach {origin[0]}://{origin[1]}"
                f"{'' if origin[2] is None else ':' + str(origin[2])} "
                f"(a server-supplied @odata.nextLink pointing off-host, or an "
                f"HTTP redirect Location). "
                f"Following it would send the Authorization header to that "
                f"host. If the service legitimately paginates across hosts, "
                f"this connector does not support it."
            )

    def _request_same_origin(
        self, session: requests.Session, method: str, url: str, **kwargs: Any
    ) -> requests.Response:
        """Issue one request with ``allow_redirects=False`` and manually
        follow only SAME-ORIGIN 3xx redirects (bounded), raising on the first
        off-origin ``Location``.

        ``requests``' auto-redirect would follow a cross-host ``Location``
        with the session's credentials attached — and its ``rebuild_auth``
        strips only ``Authorization``, leaving ``api_key`` / ``extra_headers``
        exposed to the redirect target. The connector never needs
        auto-redirect (every next URL is built explicitly from
        ``@odata.nextLink``); same-origin redirects (server-side URL
        normalization) are still followed here so a trailing-slash / case 301
        keeps working."""
        for _ in range(_MAX_SAME_ORIGIN_REDIRECTS + 1):
            resp = session.request(
                method, url, timeout=self.timeout, allow_redirects=False, **kwargs
            )
            self._log_http_response(method, url, resp)
            if not resp.is_redirect:
                if 300 <= resp.status_code < 400:
                    # A 3xx the follow loop can't act on (no ``Location``
                    # header, or a non-redirect 3xx like 300/304 — the
                    # connector never sends conditional headers, so a 304 is
                    # as anomalous as any other). Left to flow onward it dies
                    # much later as a bare JSON/XML parse error on the empty
                    # body with zero HTTP context; name the status here
                    # instead.
                    raise RuntimeError(
                        f"OData request to {url!r} returned HTTP "
                        f"{resp.status_code} without a followable same-origin "
                        f"redirect Location — the connector cannot act on it. "
                        f"Check the service URL and any proxy in front of "
                        f"the service."
                    )
                return resp
            target = urljoin(url, resp.headers.get("Location", ""))
            self._require_same_origin(target)  # off-origin → PermissionError
            url = target
            self._log_http_request(method, url)
        raise RuntimeError(
            f"OData request exceeded {_MAX_SAME_ORIGIN_REDIRECTS} same-origin "
            f"redirects starting from {url!r} — likely a redirect loop."
        )

    def _http_get_once(
        self, session: requests.Session, url: str, method: str = "GET", **kwargs: Any
    ) -> requests.Response:
        """One auth-aware request attempt; throttle handling lives in `_http_get`.

        No token-refresh path lives here (or anywhere in the connector):
        OAuth tokens are minted and refreshed by the Unity Catalog
        COMMUNITY connection layer, which injects a fresh ``access_token``
        at query time. A 401/403 is therefore always terminal for this
        read and surfaces the per-auth-mode remediation."""
        self._require_same_origin(url)
        self._log_http_request(method, url)
        resp = self._request_same_origin(session, method, url, **kwargs)
        if resp.status_code in (401, 403):
            raise PermissionError(self._no_refresh_auth_error(resp, url))
        return resp

    def _retry_after_delay(self, resp: requests.Response, attempt: int) -> float:
        """Pick the sleep duration before the next retry.

        Priority:
          1. ``Retry-After`` header — integer seconds or HTTP-date.
             Honoured as-is (capped): the server picked the moment, so
             jittering it would retry too early.
          2. Jittered exponential backoff via ``_backoff_delay``.

        Either way the value is capped at ``retry_max_delay_seconds``.
        """
        cap = float(self.retry_max_delay_seconds)
        header = resp.headers.get("Retry-After")
        if header is not None:
            parsed = _parse_retry_after(header)
            if parsed is not None:
                return min(parsed, cap)
        return self._backoff_delay(attempt)

    def _transient_status_exhausted_error(
        self, resp: requests.Response, url: str, attempts: int
    ) -> str:
        """Message for the post-retry-budget RuntimeError when the
        server kept returning a retryable 4xx/5xx
        (``_RETRYABLE_HTTP_STATUSES``)."""
        status = resp.status_code
        retry_after = resp.headers.get("Retry-After", "<none>")
        if status in (429, 503):
            symptom = "server is throttling or temporarily unavailable"
        elif status == 408:
            symptom = (
                "server (or a proxy) timed out waiting on every attempt — "
                "consider raising 'timeout_seconds' or lowering 'page_size'"
            )
        elif status == 500:
            symptom = (
                "server returned an internal error on every attempt — likely a "
                "request shape the source can't handle (e.g. ``$top`` above its "
                "per-page cap) or a deterministic upstream bug; check the "
                "response body and lower ``page_size`` / narrow the request "
                "before retrying"
            )
        else:  # 502, 504
            symptom = "upstream gateway returned a transient error on every attempt"
        return (
            f"OData service returned {status} for {url!r} after "
            f"{attempts} attempts ({symptom}). Last Retry-After header: "
            f"{retry_after}. Raise 'max_retries' (current: {self.max_retries}) "
            f"or 'retry_max_delay_seconds' (current: "
            f"{self.retry_max_delay_seconds}) on the connection if the source "
            f"needs longer cooldowns; reduce read concurrency via the "
            f"per-table 'num_partitions' option if the failure is "
            f"concurrency-driven. Server response: {_truncate(resp.text, 300)}"
        )

    def _no_refresh_auth_error(self, resp: requests.Response, url: str) -> str:
        """Construct an actionable auth-failure message for the no-refresh path.

        Different auth modes have very different failure modes — bearer
        tokens expire, OAuth scopes can be too narrow, basic creds rot —
        and bundling them all into one generic "401 Unauthorized" makes
        triage from a pipeline log nearly impossible. This method picks
        the relevant remediation hints based on which auth mode is
        active on the connection.

        403 gets its own message ahead of the per-mode 401 branches:
        it is an *authorization* failure (the token/credentials were
        accepted), so the token-expiry/refresh hints — and the "no
        refresh path is configured" framing — don't apply.
        """
        status = resp.status_code
        body = _truncate(resp.text, 300) or "(empty body)"
        auth = (self.options.get("auth_type") or "").lower().strip()
        injected = not auth and bool(self.options.get("access_token"))
        if not auth and not injected and self.options.get("token"):
            auth = "bearer"
        if status == 403:
            # 403 means the request WAS authenticated but the principal is
            # not authorized for this resource — a fresh token can't fix it,
            # and the "no refresh path" prefix below would be misleading.
            # Say what actually needs fixing: permissions.
            return (
                f"OData service returned 403 (Forbidden) for {url!r}. The "
                f"request was authenticated but the principal is not "
                f"authorized for this resource, so a fresh token cannot "
                f"fix it. Grant the principal read access to this entity "
                f"set at the source (role/permission assignment), or use "
                f"credentials whose scope covers it (for an OAuth "
                f"connection, check the connection's 'oauth_scope' and any "
                f"required admin consent), and confirm tenant/instance "
                f"identifiers in 'service_url' or 'extra_headers' match "
                f"the credentials. Server response: {body}"
            )
        if injected:
            return (
                f"OData service returned {status} for {url!r} using the "
                f"access token injected by the Unity Catalog OAuth "
                f"connection. The connection layer refreshes the token at "
                f"query start, so a 401 here usually means the token "
                f"expired mid-read (retry — the next query gets a fresh "
                f"token), the OAuth principal lacks read access to this "
                f"entity set, or the connection's 'oauth_scope' is too "
                f"narrow. Server response: {body}"
            )
        prefix = (
            f"OData service returned {status} for {url!r} and no "
            f"automatic token-refresh path is configured. "
        )
        if auth == "bearer":
            return (
                f"{prefix}"
                f"With auth_type=bearer the pre-acquired static token cannot "
                f"be refreshed — either it has expired (typical lifetime "
                f"~1 h), or the principal that issued it lacks read access "
                f"to this entity set. Fixes: replace 'token' on the "
                f"connection with a fresh one; or switch to a Unity Catalog "
                f"COMMUNITY OAuth connection (community_oauth_flow=m2m/u2m), "
                f"which refreshes tokens server-side and injects a fresh "
                f"'access_token' every query. For Microsoft Graph "
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
        return (
            f"{prefix}"
            f"No authentication is configured on this connection but the "
            f"OData service requires it. Set 'auth_type' to one of: "
            f"bearer, basic, api_key (with the matching parameter set), "
            f"or use a Unity Catalog COMMUNITY OAuth connection "
            f"(community_oauth_flow=m2m/u2m), which injects "
            f"'access_token' at query time. Server response: {body}"
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
                    k = k.strip()
                    # Same RFC 7230 token check as ``api_key_header`` below:
                    # http.client's send-time regex tolerates interior spaces,
                    # so a malformed name would otherwise go out on the wire
                    # as-is and fail at a strict server/proxy with nothing
                    # pointing at this option.
                    if not re.fullmatch(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+", k):
                        raise ValueError(
                            f"Invalid header name {k!r} in extra_headers: not "
                            f"a valid HTTP header name (letters, digits, and "
                            f"!#$%&'*+-.^_`|~ only, no spaces)."
                        )
                    session.headers[k] = v.strip()

        auth_type = (self.options.get("auth_type") or "").lower().strip()
        if not auth_type and self.options.get("access_token"):
            # UC COMMUNITY OAuth connection (``community_oauth_flow`` =
            # m2m / u2m / u2m_per_user): the connection layer ran the OAuth
            # flow, refreshes the token SERVER-SIDE, and injects a fresh
            # ``access_token`` into the options at query time. Opaque here —
            # no client credentials, no minting, no refresh code. A mid-read
            # expiry surfaces as 401 → ``_no_refresh_auth_error`` (the next
            # query start gets a freshly injected token).
            session.headers["Authorization"] = f"Bearer {self.options['access_token']}"
            self._session = session
            return session
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
            # Strip and default-on-empty: a padded or explicitly-empty
            # value would otherwise raise requests' uncurated
            # ``InvalidHeader`` deep inside the first request. Validate the
            # rest as an RFC 7230 header token so garbage fails here, with
            # the option named, not at send time.
            header = (self.options.get("api_key_header") or "").strip() or "x-api-key"
            if not re.fullmatch(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+", header):
                raise ValueError(
                    f"Invalid api_key_header={header!r}: not a valid HTTP "
                    f"header name (letters, digits, and !#$%&'*+-.^_`|~ "
                    f"only, no spaces)."
                )
            session.headers[header] = _require(self.options, "api_key")
        elif auth_type == "oauth2":
            # Retired: connector-side OAuth (token minting + refresh) moved
            # to the Unity Catalog COMMUNITY connection, which owns the flow
            # and injects a fresh ``access_token`` at query time.
            raise ValueError(
                "auth_type=oauth2 has been retired. OAuth is handled by the "
                "Unity Catalog COMMUNITY connection: create the connection "
                "with community_oauth_flow=m2m (client credentials) or u2m "
                "(authorization code), supplying 'client_id', "
                "'client_secret', 'token_endpoint' (and, for u2m, "
                "'authorization_endpoint'; optionally 'oauth_scope'). The "
                "connection layer runs the flow, refreshes the token "
                "server-side, and injects 'access_token' into the connector "
                "at query time — remove auth_type and the oauth2_* options."
            )
        elif auth_type:
            raise ValueError(
                f"Unknown auth_type {auth_type!r}. Expected one of: bearer, "
                f"basic, api_key — or omit auth_type entirely and use a "
                f"Unity Catalog COMMUNITY OAuth connection, which injects "
                f"'access_token' at query time."
            )

        self._session = session
        return session

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
           Stores ``(xml_text, root, index, fetched_at)`` so the index
           isn't rebuilt per instance either. Honours
           ``metadata_cache_ttl_seconds`` exactly like layer 3: entries
           past the TTL are refreshed, and a TTL of 0 skips the layer
           entirely (read AND write) so ``$metadata`` is re-fetched per
           instance as documented.
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
        ttl = self.metadata_cache_ttl_seconds
        cached = _METADATA_CACHE.get(self.service_url) if ttl > 0 else None
        if cached is not None:
            xml_text, root, index, fetched_at = cached
            if time.time() - fetched_at <= ttl:
                self._metadata = _MetadataState(root=root, index=index)
                # ``xml_text`` is only needed for the write path; once
                # cached, we don't carry it on the bundle.
                del xml_text
                return self._metadata
            # Expired — drop it so a concurrent reader doesn't race the
            # refresh below against the stale entry.
            _METADATA_CACHE.pop(self.service_url, None)
        file_cached = self._read_metadata_file_cache()
        if file_cached is not None:
            xml_text, root, fetched_at = file_cached
            index = _build_csdl_index(root)
            self._metadata = _MetadataState(root=root, index=index)
            # Stamp with the FILE's fetch time (its mtime), not now() —
            # the process entry must expire when the on-disk one would,
            # or an old document gains a second TTL lease per process.
            _metadata_cache_put(self.service_url, (xml_text, root, index, fetched_at))
            return self._metadata
        session = self._get_session()
        url = _join_url(self.service_url, "$metadata")
        resp = self._http_get(session, url, headers={"Accept": "application/xml"})
        _raise_for_status_with_body(resp, url)
        xml_text = resp.text
        root = ET.fromstring(xml_text)
        index = _build_csdl_index(root)
        self._metadata = _MetadataState(root=root, index=index)
        if ttl > 0:
            _metadata_cache_put(self.service_url, (xml_text, root, index, time.time()))
        self._write_metadata_file_cache(xml_text, root)
        return self._metadata

    def _read_metadata_file_cache(self) -> tuple[str, ET.Element, float] | None:
        """Return the cached ``(xml_text, parsed_root, fetched_at)`` from
        the on-disk pickle if it exists and is within the TTL —
        ``fetched_at`` is the file's mtime, i.e. when the writing process
        fetched the document. Returns ``None`` for any miss (missing,
        expired, unreadable, unpicklable). All failures are silent — the
        caller falls through to the network."""
        if self.metadata_cache_ttl_seconds <= 0:
            return None
        path = _metadata_cache_path(self.service_url)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return None
        if time.time() - mtime > self.metadata_cache_ttl_seconds:
            return None
        if not _cache_file_owned_by_us(path):
            # NEVER unpickle a file another uid put at our (predictable)
            # cache path — unpickling is arbitrary code execution, and the
            # shape check below runs after the damage. The per-user filename
            # already makes this unreachable in practice; this is the
            # defense-in-depth backstop.
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
        return payload[0], payload[1], mtime

    def _write_metadata_file_cache(self, xml_text: str, root: ET.Element) -> None:
        """Best-effort write of ``(xml_text, parsed_root)`` to the
        on-disk pickle via :func:`_replace_with_private_tmp` — atomic
        rename (a concurrent reader sees the old file or the new one,
        never a torn write) through an unpredictable ``O_EXCL`` temp
        name, so a pre-planted symlink can't redirect the write. File
        cache is purely an optimization: if anything goes wrong
        (read-only tempdir, disk full, pickling failure) the connector
        still works — just slower."""
        if self.metadata_cache_ttl_seconds <= 0:
            return
        try:
            data = pickle.dumps((xml_text, root), protocol=pickle.HIGHEST_PROTOCOL)
        except (pickle.PicklingError, RecursionError):
            # RecursionError: a pathologically deep CSDL tree can blow the
            # pickler's stack — the cache is an optimization, never worth
            # failing the read over.
            return
        _replace_with_private_tmp(_metadata_cache_path(self.service_url), data)

    def _entity_set_index(self) -> list[tuple[str, str]]:
        """All (schema_namespace, entity_set_name) pairs declared in $metadata."""
        return self._metadata_state().index.entity_set_pairs

    def _table_segments(self, table_name: str) -> list[str] | None:
        """:func:`parse_contained_path` with the declared-flat-set override:
        the LONGEST prefix declared as a top-level entity set becomes
        ``segments[0]``, even when it contains ``__``. CSDL
        SimpleIdentifiers legally allow consecutive underscores, so
        ``My__Set`` can be a real entity set — without the override
        ``list_tables`` emits names (``My__Set``, and its contained
        children like ``My__Set__Kids``) the read path then splits into a
        nonexistent containment path and can never resolve. Longest-prefix
        also pins the collision rule the README documents: a declared flat
        set always shadows a containment-path spelling of the same name.
        Every table-name split in the connector goes through here; the raw
        parser is only for contexts with no metadata access."""
        if _CONTAINED_PATH_SEP in table_name:
            try:
                declared = self._metadata_state().index.entity_set_by_name
            except Exception:  # noqa: BLE001 — let the parse/resolve error surface instead
                declared = None
            if declared is not None:
                if table_name in declared:
                    return None
                parts = table_name.split(_CONTAINED_PATH_SEP)
                # Longest declared ``__``-bearing prefix wins; the remainder
                # parses as the containment path under it. k=1 (a plain
                # single-segment head) is the raw parser's own result, and
                # empty remainder segments fall through so the parser raises
                # its precise empty-segment error.
                for k in range(len(parts) - 1, 1, -1):
                    head = _CONTAINED_PATH_SEP.join(parts[:k])
                    if head in declared and all(parts[k:]):
                        return [head] + parts[k:]
        return _parse_contained_path(table_name)

    def _entity_type_for(self, table_name: str, namespace: str | None = None) -> ET.Element:
        """Resolve flat names or contained paths (segment-by-segment via
        contained nav props on the base-type chain)."""
        state = self._metadata_state()
        cache_key = (table_name, namespace)
        cached = state.entity_type.get(cache_key)
        if cached is not None:
            return cached
        segments = self._table_segments(table_name) or [table_name]
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
        requested_ns = namespace
        if namespace is not None:
            # Accept the schema's ``Alias`` as well as its canonical
            # ``Namespace`` — CSDL lets type references use either, so
            # the table option should too. Error messages echo what the
            # user actually passed (``requested_ns``), naming the
            # canonical resolution when it differs.
            namespace = index.alias_to_namespace.get(namespace, namespace)
            matches = [(ns, ref) for ns, ref in candidates if ns == namespace]
        else:
            matches = list(candidates)
        if not matches:
            if namespace is not None:
                shown = (
                    f"{requested_ns!r}"
                    if requested_ns == namespace
                    else f"{requested_ns!r} (alias of {namespace!r})"
                )
                hint = sorted(index.entity_set_names_by_ns.get(namespace, []))
                if not hint:
                    # The requested namespace has zero entity sets — common
                    # confusion when the user picks a type-only schema
                    # (e.g. one declaring BaseType references) instead of
                    # the schema whose <EntityContainer> declares the sets.
                    raise ValueError(
                        f"Entity set {table_name!r} not found in namespace "
                        f"{shown}. Namespace {shown} declares "
                        f"no entity sets (probably a type-only schema). "
                        f"Namespaces with entity sets: {sorted(index.namespaces_with_sets)}."
                    )
                raise ValueError(
                    f"Entity set {table_name!r} not found in namespace "
                    f"{shown}. Available in this namespace: {hint}"
                )
            raise ValueError(
                f"Entity set {table_name!r} not found in $metadata. "
                f"Available: {sorted({n for _, n in index.entity_set_pairs})}"
            )
        if len(matches) > 1:
            namespaces = sorted({m[0] for m in matches})
            if len(namespaces) == 1:
                # Same name twice in ONE namespace (multiple containers in
                # one schema — malformed CSDL): 'namespace' can't
                # disambiguate, so don't suggest it.
                raise ValueError(
                    f"Entity set {table_name!r} is declared more than once in "
                    f"namespace {namespaces[0]!r} (malformed $metadata — "
                    f"duplicate EntitySet declarations). The connector cannot "
                    f"tell the declarations apart; fix the service metadata."
                )
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
        # The result embeds the exclusion-FILTERED FK columns, so the
        # current ``exclude_ancestor_columns`` set must be part of the key:
        # a (table, namespace)-only key would freeze schema AND composite
        # PK at the first call's exclusions while row stamping follows the
        # current ones — hard parse failures one way, silent MERGE
        # collisions the other (``_resolve_fk_columns`` itself caches
        # unfiltered for exactly this reason).
        excluded = getattr(self, "_excluded_ancestor_columns", frozenset())
        cache_key = (table_name, namespace, excluded)
        cached = state.fields.get(cache_key)
        if cached is not None:
            return cached
        segments = self._table_segments(table_name) or [table_name]
        own_fields = self._own_fields_for_et(self._entity_type_for(table_name, namespace))
        if len(segments) == 1:
            state.fields[cache_key] = own_fields
            return own_fields
        # Every non-leaf ancestor contributes FK columns (OData v4
        # §13.4.3 — contained-entity keys are unique within parent only).
        fk_columns = self._resolve_fk_columns(segments, namespace)
        fk_fields: list[StructField] = []
        for idx in range(len(segments) - 1):
            if not any(k[0] == idx for k in fk_columns):
                continue
            ancestor_et = self._entity_type_for(
                _CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
            )
            own = {f.name: f.dataType for f in self._own_fields_for_et(ancestor_et)}
            for pk in self._own_primary_keys_for_et(ancestor_et):
                fk_fields.append(
                    StructField(
                        fk_columns[(idx, pk)],
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
                nullable = prop.get("Nullable", "true").lower() != "false"
                if prop.get("Type") == "Edm.Stream":
                    # §11.2.4: stream values are media references the JSON
                    # payload NEVER carries — honoring Nullable="false" here
                    # would fail EVERY row of the table on the framework's
                    # absent-non-nullable check. The column is always null.
                    nullable = True
                fields.append(
                    StructField(
                        name,
                        # TypeDefinition-resolved: a property typed ``ta.Qty``
                        # (UnderlyingType Edm.Int64) must map to LongType, not
                        # the StringType fallback — the literal-rendering map
                        # already resolves the same way, and a split verdict
                        # silently degrades every TypeDefinition-backed column
                        # (SAP uses them in production) to a string at the
                        # destination.
                        _spark_type_for_property(
                            prop,
                            self._resolve_underlying_type(prop.get("Type", "Edm.String")),
                        ),
                        nullable,
                    )
                )
        state.own_fields[cache_key] = fields
        return fields

    def _primary_keys_for(self, table_name: str, namespace: str | None = None) -> list[str]:
        state = self._metadata_state()
        # Exclusion-aware key — same poisoning door as ``_fields_for``:
        # the composite PK embeds the filtered FK columns.
        excluded = getattr(self, "_excluded_ancestor_columns", frozenset())
        cache_key = (table_name, namespace, excluded)
        cached = state.primary_keys.get(cache_key)
        if cached is not None:
            return cached
        segments = self._table_segments(table_name) or [table_name]
        leaf_pks = self._own_primary_keys_for_et(self._entity_type_for(table_name, namespace))
        if len(segments) == 1:
            state.primary_keys[cache_key] = leaf_pks
            return leaf_pks
        # Composite: every ancestor's FK columns + leaf's own PKs.
        fk_columns = self._resolve_fk_columns(segments, namespace)
        composite: list[str] = []
        for idx in range(len(segments) - 1):
            if not any(k[0] == idx for k in fk_columns):
                continue
            ancestor_et = self._entity_type_for(
                _CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
            )
            for pk in self._own_primary_keys_for_et(ancestor_et):
                composite.append(fk_columns[(idx, pk)])
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
                if any("/" in (name or "") for name in result):
                    # Complex-path key (<PropertyRef Name="Info/Code"
                    # Alias="IC"/>). Neither the raw path NOR the alias is a
                    # column the emitted rows carry (the value sits nested
                    # inside the complex property's JSON), so reporting
                    # either hands the destination a MERGE key no column
                    # matches — a silent contract desync. No mainstream
                    # service uses this key form; fail loudly and honestly.
                    raise ValueError(
                        f"Entity type {et.get('Name')!r} keys on a property "
                        f"inside a complex type ({[n for n in result if '/' in (n or '')]}); "
                        f"this connector maps complex properties to JSON "
                        f"strings and cannot address or MERGE on nested "
                        f"keys. Ingest a different entity set, or expose a "
                        f"flattened key on the server."
                    )
                break
        state.own_pks[cache_key] = result
        return result

    def _edm_types_for_et(self, et: ET.Element) -> dict[str, str]:
        """Declared property → Edm-type map over the base-type chain
        (closest-to-ROOT declaration wins, matching ``_own_fields_for_et``
        — the SCHEMA resolver: the seek/predicate literal must be quoted
        for the type the schema declares and the framework parser expects,
        so on (spec-forbidden) redeclaring metadata the two must not
        diverge).

        Feeds ``odata_literal_typed`` at the key-predicate / keyset-seek
        render sites: the OData JSON payload delivers ``Edm.Guid`` (and, on
        IEEE754Compatible servers, ``Edm.Int64``/``Edm.Decimal``) values as
        JSON strings, so only the declared type can decide whether the wire
        literal is quoted. Missing/undeclared properties simply aren't in the
        map — the renderer falls back to the value sniff for those."""
        state = self._metadata_state()
        cache_key = id(et)
        cached = state.edm_types.get(cache_key)
        if cached is not None:
            return cached
        result: dict[str, str] = {}
        # ``reversed``: root-first, first declaration wins — the same
        # direction ``_own_fields_for_et`` resolves the schema with.
        for type_el in reversed(self._resolve_base_chain(et)):
            for prop in type_el.findall(f"{_NS_EDM}Property"):
                name = prop.get("Name")
                if name and name not in result:
                    result[name] = self._resolve_underlying_type(prop.get("Type", "Edm.String"))
        state.edm_types[cache_key] = result
        return result

    def _resolve_underlying_type(self, type_ref: str) -> str:
        """Resolve a ``<TypeDefinition>``-typed property reference to its
        underlying ``Edm.*`` primitive; anything else passes through
        verbatim. A definition backed by ``Edm.String`` must quote its
        literals like any string — recording the definition name instead
        would drop the property out of typed rendering and an ISO-looking
        value would render bare (the exact misfire typed rendering
        exists to prevent). Accepts alias- or namespace-qualified refs."""
        if type_ref.startswith("Edm.") or "." not in type_ref:
            return type_ref
        index = self._metadata_state().index
        prefix, type_name = type_ref.rsplit(".", 1)
        target_ns = index.alias_to_namespace.get(prefix)
        if target_ns is None:
            return type_ref
        return index.typedef_underlying.get(f"{target_ns}.{type_name}", type_ref)

    def _edm_types_for_table(self, table_name: str, namespace: str | None) -> dict[str, str]:
        """Best-effort :meth:`_edm_types_for_et` by table name / contained
        path. Resolution failure returns ``{}`` (sniff-based literal
        rendering) — typing seek literals must never break a read that
        worked untyped."""
        try:
            return self._edm_types_for_et(self._entity_type_for(table_name, namespace))
        except Exception:  # noqa: BLE001 — metadata gaps must not break reads
            return {}

    # ------------------------------------------------------------------
    # Cursor filter formatting
    # ------------------------------------------------------------------

    def _cursor_filter(
        self, cursor_field: str, since: Any, edm_type: str | None = None
    ) -> str | None:
        """Build the `$filter` clause for an incremental fetch.

        Strict `cursor gt since` once the offset has advanced; `None` on
        the very first call so the server returns the natural start of
        the table. `max_records_per_batch` is the per-call cap — there
        is no wall-clock ceiling, which is what makes continuous polling
        work and what keeps the connector type-agnostic over the cursor
        column.

        ``edm_type`` (the cursor column's declared Edm type, when the
        caller has it) steers literal quoting via ``odata_literal_typed``
        — an IEEE754Compatible server renders an Edm.Int64 watermark as a
        STRING, and the untyped sniff would quote it (``Seq gt '7000'``:
        strict servers 400). Callers without CSDL in reach fall back to
        the sniff, exactly as before.
        """
        if since is None:
            return None
        literal = _odata_literal_typed(since, edm_type) if edm_type else _odata_literal(since)
        return f"{cursor_field} gt {literal}"

    def _cursor_max_end_offset(self, cursors: list, since: Any) -> dict:
        """End offset for a natural-completion cursor batch: ``{"cursor": max}``
        over the batch's effective (non-null) cursor values, falling back to the
        carried ``since`` and then ``{}``.

        Shared by the flat (``_read_incremental``) and contained leaf-cursor
        (``_read_contained_incremental_leaf_cursor``) reads. ``{"cursor": None}``
        must never be committed — it would advance ``{}`` → ``{"cursor": None}``
        on an all-null-cursor batch and then loop the no-progress guard — so a
        null-only batch yields ``since`` (if carried) or ``{}``. The max is
        CURSOR-ordered (chronological for ISO renderings — a lexical ``max``
        prefers ``…00Z`` over the later ``…00.5Z``, regressing the watermark
        behind emitted rows) and FLOORED at ``since``: with an active lookback
        window the read filter sits below the committed watermark, and if the
        watermark-defining row was deleted between batches the overlap's own
        max lands below ``since`` — committing it would regress the watermark
        (duplicate-safe, but the window re-reads grow and can repeat every
        batch)."""
        if cursors:
            return {"cursor": _max_or(_cursor_max(cursors), since)}
        if since is not None:
            return {"cursor": since}
        return {}

    def _parse_cursor_lookback(self, table_options: dict[str, str] | None):
        """Parse the ``cursor_lookback_seconds`` table option.

        Returns the mode: the string ``"auto"`` (default), or a non-negative
        ``int`` of explicit seconds (``0`` disables the overlap).

        The overlap re-reads a window behind the committed watermark on each
        incremental ``expand_contained=true`` batch, so rows inserted *during*
        a long (non-atomic) walk — which land with a cursor below the walk's
        final max and would otherwise be skipped by ``cursor gt <max>`` — are
        re-scanned and captured on the next progressing batch (deduped at the
        destination by ``apply_changes`` MERGE).

        * ``auto`` (default): size the window from the measured duration of
          the previous completed walk (persisted in the offset), times a
          safety factor, clamped to a ceiling. Self-tuning; no manual guess.
          A no-op outside the expand-cursor path and for non-timestamp
          cursors, so defaulting it on is safe.
        * an explicit integer: a fixed window in cursor units (seconds for a
          timestamp cursor). Set it at or above the worst-case walk duration.
        * ``0`` / ``off``: disabled (exact prior behaviour).
        """
        raw = (table_options or {}).get("cursor_lookback_seconds")
        if raw is None or str(raw).strip() == "":
            return "auto"
        norm = str(raw).strip().lower()
        if norm == "auto":
            return "auto"
        if norm == "off":
            return 0
        try:
            val = int(norm)
        except ValueError as exc:
            raise ValueError(
                f"Invalid cursor_lookback_seconds={raw!r}; expected 'auto', "
                f"'off', or a non-negative integer (seconds)."
            ) from exc
        if val < 0:
            raise ValueError(f"cursor_lookback_seconds must be >= 0; got {val}.")
        return val

    def _parse_cursor_lookback_factor(self, table_options: dict[str, str] | None) -> float:
        """Parse ``cursor_lookback_factor`` — the ``auto`` safety multiplier
        applied to the max recent walk duration (default 1.5). Must be > 0;
        values < 1 risk under-covering the overlap (dropped rows)."""
        raw = (table_options or {}).get("cursor_lookback_factor")
        if raw is None or str(raw).strip() == "":
            return _LOOKBACK_AUTO_DEFAULT_FACTOR
        try:
            val = float(str(raw).strip())
        except ValueError as exc:
            raise ValueError(
                f"Invalid cursor_lookback_factor={raw!r}; expected a positive number."
            ) from exc
        # NaN slips past every ``<=`` comparison (all NaN comparisons are
        # False) and then poisons the resolved window — min/max keep it and
        # the read dies at ``timedelta(seconds=nan)`` with an uncurated
        # ValueError. The only ``float()``-based option parser, so the only
        # place this check is needed (the int parsers reject "nan" upfront).
        if val <= 0 or math.isnan(val):
            raise ValueError(f"cursor_lookback_factor must be > 0; got {val}.")
        return val

    def _parse_cursor_lookback_ceiling(self, table_options: dict[str, str] | None) -> int:
        """Parse ``cursor_lookback_max_seconds`` — the ``auto`` ceiling clamp
        (runaway backstop) on the overlap window (default 3600). Must be > 0."""
        raw = (table_options or {}).get("cursor_lookback_max_seconds")
        if raw is None or str(raw).strip() == "":
            return _LOOKBACK_AUTO_DEFAULT_CEILING_SECONDS
        try:
            val = int(str(raw).strip())
        except ValueError as exc:
            raise ValueError(
                f"Invalid cursor_lookback_max_seconds={raw!r}; expected a positive integer."
            ) from exc
        if val <= 0:
            raise ValueError(f"cursor_lookback_max_seconds must be > 0; got {val}.")
        return val

    def _resolve_active_lookback(self, start_offset: dict | None) -> float:
        """Seconds to subtract from the committed watermark for THIS read.

        Static mode → the configured integer. ``auto`` → the **max** walk
        duration over the last ``_LOOKBACK_AUTO_WINDOW`` completed walks
        (``lb_history`` in the offset) × ``cursor_lookback_factor``, clamped
        to ``cursor_lookback_max_seconds``; ``0`` until the first walk has
        been measured. Held on ``self`` for the read so
        ``_apply_cursor_lookback`` can read it without threading the offset
        through ``_cursor_expand_clause``.

        The ``auto`` value keeps **sub-second (down to nanosecond) precision**
        — a fast walk only needs a fast overlap, and the mid-walk-arrival
        window is itself sub-second on a small/fast source, so flooring to
        whole seconds would collapse a 0.3s walk's window to a useless 0 and
        strand rows that landed a few ms below the watermark. ``timedelta`` (in
        ``_apply_cursor_lookback``) accepts the float directly."""
        mode = getattr(self, "_cursor_lookback", "auto")
        if mode != "auto":
            return int(mode)
        raw_history = (start_offset or {}).get("lb_history") or []
        # Connector-written entries are always positive finite floats (the
        # append guards ``measured > 0`` and the clock-skew max), but the
        # checkpoint is user-visible state: a hand-edited/corrupt entry
        # ("abc", NaN, a negative) would otherwise crash the multiply /
        # timedelta uncurated — or, worse for a negative, float the read
        # filter ABOVE the watermark (a silent exclusion band). Same
        # non-finite discipline as the cursor_lookback_factor parser.
        history = [
            v
            for v in raw_history
            if isinstance(v, (int, float))
            and not isinstance(v, bool)
            and math.isfinite(v)
            and v > 0
        ]
        if not history:
            return 0
        factor = getattr(self, "_cursor_lookback_factor", _LOOKBACK_AUTO_DEFAULT_FACTOR)
        ceiling = getattr(
            self, "_cursor_lookback_max_seconds", _LOOKBACK_AUTO_DEFAULT_CEILING_SECONDS
        )
        return min(round(max(history) * factor, 9), ceiling)

    def _attach_lookback_state(
        self, out_offset: dict, start_offset: dict | None, in_flight: bool, elapsed: float
    ) -> dict:
        """Maintain the ``auto`` walk-duration history on the returned offset.
        No-op for static/off modes (nothing to measure).

        ``lb_history`` is a rolling list of the last ``_LOOKBACK_AUTO_WINDOW``
        completed-walk durations (seconds, down to nanosecond precision);
        ``_resolve_active_lookback`` sizes the window from its max.

        * In-flight (the walk spans more cap-resume batches): carry the prior
          history unchanged so the read floor stays stable until completion,
          and stamp ``lb_cycle_started`` (wall-clock epoch, set once at the
          cycle's first batch) so completion can measure the WHOLE cycle.
        * Idled (``out_offset is start_offset`` — quiescent overlap re-read)
          or advanced only ``lb_*`` bookkeeping (a ``cursor_lookback_dedup``
          delta delivery — see ``_finalize_cursor_read``): keep the prior
          history; a quiescent walk only re-reads the small overlap and
          would under-represent a real walk.
        * Completed a progressing walk: append the cycle's wall-clock span —
          ``now - lb_cycle_started`` when the walk spanned multiple capped
          batches (the churn-exposure window of a capped cycle is the full
          span INCLUDING the trigger intervals between batches, so sizing
          from one batch's drain time alone under-covers it), else this
          batch's ``elapsed`` (rounded to nanoseconds, capped to the last N).
          Sub-second walks ARE recorded — on a small/fast source the
          mid-walk-arrival window is itself sub-second, so a sub-second
          overlap is exactly what recovers rows that landed just below the
          committed watermark; the old whole-second rounding floored those to
          a zero window and stranded them. Idle/empty batches never reach
          here (``out_offset is start_offset``), so only real walks are
          captured — never a no-op zero. A pathological multi-hour cycle
          can't blow the window up: ``_resolve_active_lookback`` clamps to
          ``cursor_lookback_max_seconds``.
        """
        if getattr(self, "_cursor_lookback", "auto") != "auto":
            return out_offset
        if out_offset is start_offset:
            return out_offset
        history = list((start_offset or {}).get("lb_history") or [])
        cycle_started = (start_offset or {}).get("lb_cycle_started")

        if _offset_progress_view(out_offset) == _offset_progress_view(start_offset):
            # Progress-equal under the SAME view ``_finalize_cursor_read``
            # uses (shared helper — the stripped-key sets can never drift):
            # the batch advanced only ``lb_*`` bookkeeping, a
            # ``cursor_lookback_dedup`` delta delivery that returns a NEW
            # offset dict the identity check above can't catch. Still a
            # quiescent overlap re-read, not a real walk: appending its
            # overlap-only duration would, ``_LOOKBACK_AUTO_WINDOW`` such
            # triggers later, flush every real walk out of the rolling
            # history and shrink the ``auto`` window below the walks it must
            # cover. Carry the prior measurement state unchanged instead.
            out = dict(out_offset)
            if history:
                out.setdefault("lb_history", history)
            if cycle_started is not None:
                out.setdefault("lb_cycle_started", cycle_started)
            return out
        if in_flight:
            out = {k: v for k, v in out_offset.items() if k != "lb_cycle_started"}
            if cycle_started is None:
                # First capped batch of a cycle: anchor at this batch's start
                # (wall clock; ``elapsed`` is a duration, safe to subtract).
                cycle_started = round(time.time() - elapsed, 3)
            out["lb_cycle_started"] = cycle_started
            if history:
                out["lb_history"] = history
            return out
        measured = round(elapsed, 9)
        if cycle_started is not None:
            try:
                # Multi-batch cycle: the exposure span is first-batch start to
                # now. ``max`` guards a skewed/stepped wall clock — never
                # record less than the final batch's own walk.
                measured = max(measured, round(time.time() - float(cycle_started), 9))
            except (TypeError, ValueError):
                pass
        if measured > 0:
            history.append(measured)
            history = history[-_LOOKBACK_AUTO_WINDOW:]
        out_offset = {k: v for k, v in out_offset.items() if k != "lb_cycle_started"}
        if not history:
            return out_offset
        return {**out_offset, "lb_history": history}

    def _apply_cursor_lookback(self, since: Any) -> Any:
        """Return the read floor: ``since`` minus the active lookback window.

        Unchanged when the active window is 0 or ``since`` is ``None`` (the
        first read stays unfiltered). For a positive window the cursor must be
        a timestamp — ISO-8601 string or ``datetime``; the result is a BARE
        ISO-8601 string (same value space as the rows' own cursor text). The
        committed watermark is never floored — only the read filter is — so
        the offset still advances to the true max seen.

        A non-timestamp cursor under ``auto`` is a no-op (auto is the default
        and must not break such tables); under an explicit window it raises."""
        seconds = getattr(self, "_active_lookback_seconds", 0)
        if not seconds or since is None:
            return since
        dt = since
        if isinstance(since, str):
            try:
                dt = _parse_iso8601(since)
            except ValueError as exc:
                if getattr(self, "_cursor_lookback", "auto") == "auto":
                    return since
                raise ValueError(
                    f"cursor_lookback_seconds={seconds} requires a timestamp "
                    f"cursor_field, but the cursor value {since!r} is not "
                    f"ISO-8601. Unset cursor_lookback_seconds or pick a "
                    f"timestamp cursor."
                ) from exc
        if not isinstance(dt, datetime):
            if getattr(self, "_cursor_lookback", "auto") == "auto":
                return since
            raise ValueError(
                f"cursor_lookback_seconds={seconds} requires a datetime/"
                f"timestamp cursor; got {type(since).__name__} {since!r}."
            )
        # Return the BARE ISO string (``...Z`` / ``...+10:00``), not a
        # datetime and NOT ``_odata_literal(...)``: the leaf-cursor walk
        # compares it client-side against the rows' own cursor strings
        # (``rec_cursor <= chain_since``) — a NAIVE datetime would still
        # raise through ``_cursor_le``'s raw fallback, and a pre-escaped
        # literal would compare escaped-vs-raw text AND get re-fed through
        # ``_odata_literal`` at the ``_cursor_filter`` URL build, where a
        # non-UTC ``%2B`` offset fails the ISO sniff and double-escapes
        # into a quoted garbage string on the wire. Raw value space here;
        # the single escape happens at literal generation.
        floored = (dt - timedelta(seconds=seconds)).isoformat()
        return floored.replace("+00:00", "Z")

    # ------------------------------------------------------------------
    # Null-cursor policy (``cursor_nulls``)
    # ------------------------------------------------------------------

    _DEFAULT_COALESCE_FLOOR_YEAR = 2000

    def _parse_cursor_nulls(self, table_options: dict[str, str] | None) -> tuple[str, int]:
        """Parse the ``cursor_nulls`` option into ``(mode, floor_year)``.

        Forms: ``coalesce`` (default), ``error``, ``ignore``, and
        ``coalesce:<YYYY>`` to override the temporal synthetic floor year
        (default ``2000``). The year suffix is only valid with
        ``coalesce``. Raises on an unrecognised mode or a malformed year.
        """
        # ``or``-defaulting (not ``.get`` default) so an explicitly-empty
        # value means "unset" — consistent with delta_tracking / pagination /
        # expand_contained, which all treat "" as their default.
        raw = ((table_options or {}).get("cursor_nulls") or "coalesce").strip().lower()
        mode, _, floor = raw.partition(":")
        mode = mode.strip()
        if mode not in ("coalesce", "error", "ignore"):
            raise ValueError(
                f"cursor_nulls mode must be one of 'coalesce', 'error', 'ignore'; got {mode!r}."
            )
        floor_year = self._DEFAULT_COALESCE_FLOOR_YEAR
        if floor:
            floor = floor.strip()
            if mode != "coalesce":
                raise ValueError(
                    f"cursor_nulls floor year is only valid with 'coalesce'; got {raw!r}."
                )
            if not (floor.isdigit() and len(floor) == 4):
                raise ValueError(
                    f"cursor_nulls floor must be a 4-digit year (e.g. 'coalesce:1990'); "
                    f"got {floor!r}."
                )
            floor_year = int(floor)
        return mode, floor_year

    def _cursor_floor(
        self,
        table_name: str,
        namespace: str | None,
        cursor_field: str,
        floor_year: int = _DEFAULT_COALESCE_FLOOR_YEAR,
    ) -> tuple[Any, str]:
        """Deterministic floor used to substitute a null cursor under
        ``cursor_nulls=coalesce``. Returns ``(floor, kind)`` where the
        floor sorts below every real value of the cursor's type, so a
        later ``cursor gt <floor>`` never skips a real row. ``kind`` is
        ``datetime`` (sub-second room for a per-row PK offset), ``date``,
        ``int``, ``num`` or ``str`` (constant floor — distinct synthetic
        values aren't representable, so same-floor nulls fall back to the
        complete-cohort handling).

        Temporal floors use ``<floor_year>-01-01`` (default ``2000``,
        configurable via ``cursor_nulls=coalesce:<YYYY>``) rather than the
        EDM minimum: modification/created timestamps are comfortably after
        it, every OData server parses it cleanly, and it keeps the
        synthetic watermark readable. (A real cursor value *before* the
        floor only matters if a synthetic floor is ever committed as the
        watermark — i.e. a batch with no real-cursor rows — which doesn't
        arise for the modification-timestamp cursors this targets; lower
        the year if your data predates it.)

        Raises ``ValueError`` for cursor types with no well-defined floor
        (boolean/binary/etc.); pick ``cursor_nulls=ignore``/``error`` or a
        different cursor for those.
        """
        et = self._entity_type_for(table_name, namespace)
        dtype = next(
            (f.dataType for f in self._own_fields_for_et(et) if f.name == cursor_field), None
        )
        if isinstance(dtype, TimestampType):
            return f"{floor_year:04d}-01-01T00:00:00", "datetime"
        if isinstance(dtype, DateType):
            return f"{floor_year:04d}-01-01", "date"
        if isinstance(dtype, (IntegerType, LongType)):
            return -(2**63), "int"
        if isinstance(dtype, (DecimalType, DoubleType, FloatType)):
            return -1.0e308, "num"
        if isinstance(dtype, StringType):
            return "", "str"
        raise ValueError(
            f"cursor_nulls=coalesce cannot synthesise a floor for cursor_field "
            f"{cursor_field!r} of type {type(dtype).__name__ if dtype else 'unknown'} "
            f"on {table_name!r}. Use cursor_nulls=ignore (skip null-cursor rows), "
            f"cursor_nulls=error, or pick a temporal/numeric/string cursor."
        )

    def _make_cursor_resolver(
        self,
        table_name: str,
        namespace: str | None,
        cursor_field: str,
        table_options: dict[str, str] | None,
    ):
        """Return ``(skip_null, effective)`` for a cursor read.

        ``effective(row)`` is the value used for filtering, the same-cursor
        boundary trim and the watermark — **never** written back into the
        row, so the emitted column keeps its real ``null``:

        * ``coalesce`` (default) — a null cursor resolves to a synthetic
          floor (``datetime`` floors carry a per-PK sub-second offset so
          distinct nulls don't collapse into one cohort). The watermark
          always advances, so null-cursor rows are ingested once on the
          seed pass and the stream converges.
        * ``error`` — ``effective`` is the raw value (``None`` for nulls);
          a null-only batch can't advance the watermark and surfaces the
          shared no-progress ``RuntimeError``.
        * ``ignore`` — ``skip_null`` is True; null-cursor rows are dropped
          (never emitted), so they don't block watermark progress.
        """
        mode, floor_year = self._parse_cursor_nulls(table_options)
        if mode == "ignore":
            return True, (lambda row: row.get(cursor_field))
        if mode == "error":
            return False, (lambda row: row.get(cursor_field))
        try:
            floor, kind = self._cursor_floor(table_name, namespace, cursor_field, floor_year)
        except ValueError:
            # No synthesisable floor for this cursor type. ``coalesce`` is
            # the default, so silently fall back to ``error`` behaviour
            # rather than break a previously-working pipeline — unless the
            # user asked for ``coalesce`` explicitly, in which case the
            # type error is theirs to see.
            if "cursor_nulls" in (table_options or {}):
                raise
            return False, (lambda row: row.get(cursor_field))
        pk_names = self._own_primary_keys_for_et(self._entity_type_for(table_name, namespace))

        def effective(row: dict) -> Any:
            value = row.get(cursor_field)
            if value is not None:
                return value
            if kind == "datetime":
                return f"{floor}.{_synthetic_pk_ordinal(row, pk_names):06d}Z"
            return floor

        return False, effective


# ---------------------------------------------------------------------------
# Helpers (module-level, no class state — easier to unit-test)
# ---------------------------------------------------------------------------


def _synthetic_pk_ordinal(row: dict, pk_names: list[str]) -> int:
    """Deterministic 0..999999 ordinal from a row's primary key, used to
    spread synthetic datetime floors for null cursors across the
    sub-second range so they don't form one same-cursor cohort. Hash, so
    it's stable across runs and works for any PK type; collisions only
    cost cohort granularity, never correctness (the destination MERGE
    keys on the real PK)."""
    key = "\x1f".join(str(row.get(p)) for p in pk_names) if pk_names else repr(sorted(row.items()))
    return int(hashlib.sha1(key.encode("utf-8")).hexdigest()[:8], 16) % 1_000_000


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


# Materialize an OData JSON ``Edm.Binary`` value into Python ``bytes``. The
# wire form is base64url (OData v4.01 JSON, ``-``/``_`` alphabet, padding
# optional); ``urlsafe_b64decode`` also accepts a standard-base64 string (its
# ``-_``→``+/`` translate leaves ``+/`` untouched), so both encodings decode.
# The framework's own parser only understands standard base64, so without this
# a base64url payload is silently corrupted.
def _decode_binary(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _decode_binary_fields(row: dict, binary_fields: frozenset) -> dict:
    """Decode base64url ``Edm.Binary`` string values in ``row`` to raw bytes.

    Returns ``row`` unchanged when there are no binary columns or none carry a
    string value (the common case — no copy). A clean base64url or standard
    base64 payload always decodes correctly; a value that *raises* (e.g. an
    invalid length) keeps its original form (the framework's lossy fallback
    then runs, no worse than before). Note ``urlsafe_b64decode`` silently
    drops stray non-alphabet characters rather than raising, so a garbage
    payload that survives to a valid length decodes to whatever its valid
    characters spell — but such inputs are already non-conformant and were
    corrupted before this decode existed. Never mutates the caller's row —
    lookback re-emits the same object, so an in-place edit would double-decode
    on the second pass."""
    if not binary_fields:
        return row
    out = None
    for name in binary_fields:
        value = row.get(name)
        if isinstance(value, str):
            try:
                decoded = _decode_binary(value)
            except Exception:  # pylint: disable=broad-except
                continue
            if out is None:
                out = dict(row)
            out[name] = decoded
    return out if out is not None else row
