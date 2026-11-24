"""Schemas, metadata and constants for the Apis REST Service connector.

Source of truth: ``apis_openapi.json`` (OpenAPI 3.1.0, ``info.title =
"Apis REST Service"``, ``info.version = "0.4.0"``) and the research
write-up in ``apis_api_doc.md``.

SCHEMA PROVENANCE — read this before changing anything here
-----------------------------------------------------------
The upstream OpenAPI document declares a response schema for **exactly one**
operation: ``GET /hive/{instance}/values``, whose body is an array of
``single_value_t``::

    single_value_t = {
        "item_name": string,
        "value": {"v": <untyped>, "q": string, "t": string}
    }

Every other operation declares only ``"200": {"description": "Success"}``
with no ``content``/``schema`` block at all. That means:

* ``values``     -> SPEC-DECLARED shape, flattened onto three top-level
                    columns (``value``/``quality``/``timestamp``) rather than
                    a nested struct — see ``VALUES_SCHEMA`` below.
* ``items``      -> INFERRED shape. Nothing in the spec names a single field
                    of the response. We model it on the only identity the
                    spec does expose for an item — the ``item_name`` string
                    used by ``single_value_t`` and by the ``item`` query
                    parameter — plus an ``attributes`` collection implied by
                    the optional numeric ``attrib`` query parameter (whose
                    ID-to-meaning registry is *not* in the spec either).
* ``timeseries`` -> INFERRED shape. The largest gap in the spec. We model a
                    historian's canonical shape: one row per (item, point in
                    time), where each point carries the same ``(v, q, t)``
                    triple as ``single_value_t``. The connector's parser is
                    deliberately tolerant of several plausible wire shapes
                    (see ``_normalize_timeseries`` in ``apis.py``) precisely
                    because the real one is unknown.

Anything marked INFERRED must be re-validated against a live Hive instance
during Phase 2 (``/validate-connector``) before it is treated as settled.

LIVE VALIDATION FINDINGS (manual spot-check, 2026-09-22)
---------------------------------------------------------
A developer ran a handful of authenticated calls against a real Apis Hive
instance (not yet the full ``/validate-connector`` record-mode pass) and
confirmed/corrected several of the assumptions above:

* **``item_name`` is not the real field name.** The spec's own declared
  ``single_value_t.item_name`` does not match reality: the live server
  returns ``"item"`` for both ``values`` and ``timeseries``, and ``"name"``
  for ``items``. ``_ITEM_NAME_KEYS`` in ``apis.py`` already includes both
  ``"name"`` and ``"item"`` ahead of this discovery, so no code change was
  needed — but this means the spec's declared schema was itself wrong, not
  just incomplete.
* **``items`` has a real ``"type"`` field** (observed values: ``"Signal"``,
  ``"Function item"``, ``"Status"``) that was not modelled at all. Added
  below as ``item_type``.
* **Module derivation confirmed correct**: splitting ``item_name`` on the
  first ``.`` reproduces exactly the module names the live ``modules``
  endpoint lists (``PumpHouse``, ``DataSampler``, ``ApisOT``,
  ``UaPublisherBee``, etc.), even for deeply nested paths.
* **``timestamp`` is NOT ISO-8601.** A live ``value.t`` looked like
  ``"2026-09-22 20:38:02.545"`` — space-separated (no ``T``), millisecond
  precision, no timezone suffix. The "presumed ISO-8601" language below has
  been corrected. This does not affect connector logic (offsets are always
  self-generated ISO-8601 strings, never parsed from response ``t``
  values), only downstream casting guidance.
* **Quality is genuinely open-ended and mixed-case**: a live ``value.q``
  came back as ``"Good"`` (capitalized), confirming the decision not to
  constrain/lowercase this column.
* **The 206 truncation limit is real, not theoretical**: an unfiltered
  ``item=*`` call against a live instance with roughly 100 items already
  came back ``206 Partial Content``.
* **Auth accepts both forms**: a live call with ``Authorization: Bearer
  <token>`` and one with the bare token both returned identical ``200``
  responses, so the ``auth_scheme`` default of ``Bearer`` needs no change.
* **Still unconfirmed**: the exact per-point shape inside a populated
  ``timeseries`` bundle (the one live call made returned zero points for
  its window), and whether ``endtime`` is inclusive or exclusive.

OTHER REAL LIMITATIONS OF THE SOURCE (not oversights here)
----------------------------------------------------------
* **No pagination exists anywhere in the spec** — no ``limit``, ``offset``,
  ``cursor``, ``nextPageToken`` or ``Link`` header on any operation — even
  though ``items``, ``values`` and ``timeseries`` all declare a ``206
  Partial Content`` response, which implies the server *can* truncate. The
  connector therefore issues exactly one request per (instance, item
  pattern, time window) tuple and never invents a continuation parameter.
  Truncation is mitigated the only way the spec allows: by narrowing the
  ``item`` wildcard patterns and the time windows. A ``206`` is surfaced as
  a warning.
* **No rate limits are documented** (no ``429``, no headers, no extensions).
  Retries are conservative and back off exponentially anyway.
* **No delete feed** for any object, so no table can be ``cdc_with_deletes``.
"""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

#: The three ingestible tables. ``/hive`` (instance list), ``runstate`` and
#: ``modules`` are deliberately *not* here: ``/hive`` is an upstream lookup
#: used to resolve the ``{instance}`` path parameter, and the other two are
#: instance-level health/catalog calls this connector does not ingest.
SUPPORTED_TABLES: tuple[str, ...] = ("items", "values", "timeseries")

# ---------------------------------------------------------------------------
# Shared struct shapes
# ---------------------------------------------------------------------------

#: INFERRED. One entry per numeric ``attrib`` ID the caller asked for. The
#: spec documents neither the field names nor the ID-to-meaning registry, so
#: both sides are kept as open strings.
ITEM_ATTRIBUTE_STRUCT = StructType(
    [
        StructField("attrib_id", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------

#: INFERRED, except where noted (see LIVE VALIDATION FINDINGS above).
#:
#: ``instance`` is not part of any response body — it is the ``{instance}``
#: path segment, stamped onto every row because item names are only unique
#: within a Hive instance.
#:
#: ``module`` is derived client-side by splitting ``item_name`` on the first
#: ``.``. CONFIRMED by a live spot-check: the derived value matches the
#: real ``modules`` endpoint's module names exactly, including for deeply
#: nested item paths.
#:
#: ``item_type`` is CONFIRMED live (observed values: ``"Signal"``,
#: ``"Function item"``, ``"Status"``) — the source's own field is literally
#: named ``"type"``, renamed here to avoid colliding with Python/SQL
#: connotations of a bare ``type`` column.
ITEMS_SCHEMA = StructType(
    [
        StructField("instance", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("module", StringType(), True),
        StructField("item_type", StringType(), True),
        StructField("attributes", ArrayType(ITEM_ATTRIBUTE_STRUCT, True), True),
    ]
)

#: SPEC-DECLARED (``single_value_t``) plus the ``instance`` scoping column.
#: The declared ``value: {v, q, t}`` triple is flattened onto three
#: top-level columns rather than kept as a nested struct, so downstream SQL
#: can reference ``value``/``quality``/``timestamp`` directly.
#:
#: ``value`` is typed as ``StringType`` because the spec gives it the
#: *empty* schema ``{}``: no type constraint whatsoever. The underlying
#: historian point may be numeric, boolean or textual and only the source
#: system knows which. String is the lossless-enough common denominator;
#: the framework's ``parse_value`` stringifies whatever JSON scalar arrives,
#: so a numeric point lands as e.g. ``"42.7"`` rather than failing the
#: batch. Downstream casts should be applied in SQL where the point's real
#: type is known.
#:
#: ``quality`` is an open string on purpose: the ``quality`` *query*
#: parameter is constrained to ``good``/``uncertain``/``bad``, but the
#: response field carries no such enum, so richer raw quality codes (e.g.
#: OPC codes) must not be rejected.
#:
#: ``timestamp`` is NOT ISO-8601, confirmed live: an observed value was
#: ``"2026-09-22 20:38:02.545"`` — space-separated (no ``T``), millisecond
#: precision, no timezone suffix. Kept as a string regardless, since the
#: response field declares no format and a different Hive deployment could
#: render it differently. It also doubles as the table's cursor field (see
#: ``TABLE_METADATA`` below); this is safe because the connector's own
#: cursor/offset values are always self-generated ISO-8601 strings, never
#: parsed out of a response ``t`` value. Downstream consumers casting this
#: column should use its real format, e.g.
#: ``TO_TIMESTAMP(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS')``, not
#: ``TO_TIMESTAMP(timestamp)``.
VALUES_SCHEMA = StructType(
    [
        StructField("instance", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

#: INFERRED. One row per (item, point in time). Structurally identical to
#: ``values`` (same flattened ``value``/``quality``/``timestamp`` columns)
#: because the spec's only per-item value shape is ``single_value_t``, and
#: ``aggregate``/``interval`` produce one such triple per resample bucket.
TIMESERIES_SCHEMA = StructType(
    [
        StructField("instance", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

TABLE_SCHEMAS: dict[str, StructType] = {
    "items": ITEMS_SCHEMA,
    "values": VALUES_SCHEMA,
    "timeseries": TIMESERIES_SCHEMA,
}

# ---------------------------------------------------------------------------
# Table metadata
# ---------------------------------------------------------------------------

#: ``items``      — snapshot. No incremental parameter exists on
#:                  ``GET /hive/{instance}/items`` (the ``item`` wildcard
#:                  narrows scope, it is not a change cursor) and no
#:                  created/modified field is declared, so the catalog is
#:                  fully re-listed each run.
#: ``values``     — cdc (upsert-only). ``updatedSince`` is a genuine
#:                  incremental signal and the endpoint returns one row per
#:                  item, so ``(instance, item_name)`` upserts cleanly.
#:                  ``cdc_with_deletes`` is impossible: the spec has no
#:                  tombstone field and no delete endpoint.
#: ``timeseries`` — append. Recorded history is immutable and the endpoint is
#:                  addressed by a required ``[starttime, endtime)`` window.
#:
#: Note on ``timeseries.primary_keys``: the *logical* uniqueness key is
#: ``(instance, item_name, timestamp)``, but append-only tables are inserted
#: rather than merged, so no primary key is declared. The connector keeps
#: its time windows half-open and non-overlapping so inserts cannot
#: duplicate across window boundaries.
TABLE_METADATA: dict[str, dict] = {
    "items": {
        "primary_keys": ["instance", "item_name"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "values": {
        "primary_keys": ["instance", "item_name"],
        "cursor_field": "timestamp",
        "ingestion_type": "cdc",
    },
    "timeseries": {
        "primary_keys": [],
        "cursor_field": "timestamp",
        "ingestion_type": "append",
    },
}

# ---------------------------------------------------------------------------
# HTTP / retry constants
# ---------------------------------------------------------------------------

DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 4
INITIAL_BACKOFF_SECONDS = 1.0
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

#: ``Authorization`` is declared as a raw ``apiKey`` header, not an
#: ``http``/``bearer`` scheme, so the spec never says whether the value is
#: ``Bearer <token>`` or the bare token. We default to the conventional
#: ``Bearer`` prefix and let operators blank it out via the ``auth_scheme``
#: option if live testing shows otherwise.
DEFAULT_AUTH_SCHEME = "Bearer"

# ---------------------------------------------------------------------------
# Read-shaping defaults
# ---------------------------------------------------------------------------

#: ``item`` is a *required* query parameter on items/values/timeseries, so
#: there is no "no filter" call. ``*`` is the spec's own wildcard syntax and
#: matches everything — for ``items``. CONFIRMED live (2026-09-23): despite
#: the spec's own documented wildcard support (``Work*.Sig*``), ``values``
#: and ``timeseries`` reject wildcard patterns outright — a bare ``*``
#: fails with ``"module * not found"``, and even a real module prefix like
#: ``ApisOT.*`` fails with ``"item ApisOT.* not found"``. Only ``items``
#: genuinely supports wildcards. See ``_resolve_exact_items`` in
#: ``apis.py``, which resolves any wildcard pattern to real item names via
#: ``items`` before it ever reaches ``values``/``timeseries``.
DEFAULT_ITEM_PATTERN = "*"

#: Format is always JSON — the spec defines no CSV schema.
RESPONSE_FORMAT = "json"

#: How many exact item names ``values``/``timeseries`` requests batch into
#: one call via repeated ``item=`` params (the spec's own ``array``/
#: ``explode: true`` style). CONFIRMED live (2026-09-23) that batching
#: multiple exact names into one request works at all (tested with 2).
#: The real upper limit (URL length, server-side cap) is UNCONFIRMED — 50
#: is a conservative placeholder pending further live testing at scale.
#: Overridable per table via the ``items_per_request`` option.
DEFAULT_ITEMS_PER_REQUEST = 50

#: Internal cursor/offset representation ONLY (checkpoints, ``_init_time``,
#: ``_add_seconds`` arithmetic). Kept as clean ISO-8601 for readability and
#: because it is never sent to the source directly — see
#: ``WIRE_TIMESTAMP_FORMAT`` below for what actually goes on the wire.
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
EPOCH_ISO = "1970-01-01T00:00:00Z"

#: The actual wire format for ``updatedSince`` / ``starttime`` / ``endtime``
#: request parameters — CONFIRMED via a live spot-check (2026-09-23) against
#: a real Prediktor Apis Hive instance. This directly contradicts what was
#: previously assumed here: the spec's own example (``2023-06-01T12:00:00Z``)
#: suggested ISO-8601, and an initial live probe using that exact format
#: even *appeared* to succeed (200, empty body) for a fully-past 2024 date —
#: but every subsequent ISO-8601-formatted request for a 2026 date failed
#: with a raw ``500 Internal Server Error`` / ``"Invalid time string: ..."``
#: regardless of whether the requested time was hours in the past or
#: seconds in the future, which ruled out a past/future validation rule.
#: The fix was found empirically: a space-separated, no-timezone-suffix
#: value — matching the exact wire shape the source uses for its own
#: response ``value.t`` field (e.g. ``"2026-09-23 00:05:00.344"``) —
#: succeeded immediately. The connector's internal cursor arithmetic stays
#: ISO-8601 (``TIMESTAMP_FORMAT`` above); this format is applied only at
#: the point a timestamp is placed into an actual HTTP request parameter
#: (see ``_to_wire_timestamp`` in ``apis.py``). No milliseconds: the
#: confirmed-working live request omitted them, even though responses
#: include millisecond precision.
WIRE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

#: Size of one ``timeseries`` partition, in seconds.
DEFAULT_WINDOW_SECONDS = 86_400

#: Upper bound on partitions handed to Spark for a single micro-batch. If the
#: requested range needs more windows than this, the window is grown instead
#: of emitting thousands of tiny tasks.
DEFAULT_MAX_PARTITIONS = 64

#: How far back a first ``timeseries`` run reaches when the caller supplies
#: neither a checkpoint nor ``start_timestamp``. Three years is deliberately
#: generous — a historian backfill is usually the point of the first run —
#: but it is bounded, and operators who want a short first run set
#: ``start_timestamp`` or lower ``backfill_days``.
DEFAULT_BACKFILL_DAYS = 1095

#: Admission control for the single-driver ``read_table`` path.
DEFAULT_MAX_RECORDS_PER_BATCH = 1000

#: Cap on how many windows one ``read_table`` call will walk before handing
#: a micro-batch back to the framework, so the driver path advances in
#: comparable strides to the partitioned path.
DEFAULT_MAX_WINDOWS_PER_READ = 64

#: ``quality`` filter enum, per the spec, for ``values`` and ``timeseries``.
QUALITY_VALUES = frozenset({"good", "uncertain", "bad"})

#: ``interval`` bounds for ``timeseries``, per the spec.
MIN_INTERVAL_SECONDS = 1
MAX_INTERVAL_SECONDS = 86_400

#: The full ``aggregate`` enum, verbatim from the spec. Validated locally so
#: a typo fails fast with a useful message instead of a bare HTTP 400.
AGGREGATE_VALUES = frozenset(
    {
        "interpolative",
        "total",
        "average",
        "timeaverage",
        "count",
        "regstddev",
        "minimumactualtime",
        "minimum",
        "maximumactualtime",
        "maximum",
        "start",
        "end",
        "delta",
        "regslope",
        "regconst",
        "regdev",
        "variance",
        "range",
        "duration_good",
        "duration_bad",
        "percent_good",
        "percent_bad",
        "worst_quality",
        "annotation_count",
        "sum",
        "interpolative_0",
        "median",
        "ua_minimumactualtime2",
        "ua_maximumactualtime2",
        "ua_range2",
        "vec_sum",
        "vec_aver",
        "vec_min",
        "vec_max",
        "displayvalues",
        "lowpassfilter",
        "movingaveragebycount",
        "movingaveragebytime",
        "ua_percentgood",
        "ua_percentbad",
    }
)
