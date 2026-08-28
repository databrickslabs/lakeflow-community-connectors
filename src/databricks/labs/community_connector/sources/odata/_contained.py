"""Contained-navigation-property support for the OData v4 connector.

OData v4 ``<NavigationProperty ContainsTarget="true">`` declares a
parent-owned collection accessed as ``GET Parent(<key>)/ContainedNavProp``.
The connector exposes these as double-underscore-pathed tables
(``Parent__Child__...__Leaf``) — slash isn't valid in Spark SQL
identifiers — with ``<seg>_<pk>`` ancestor-FK
columns prepended onto each row. The split keeps the main connector
file under its line cap; the methods here are mixed into
``ODataLakeflowConnect`` via ``ContainedNavMixin``.

All ``ContainedNavMixin`` methods call back into the main connector
class through ``self`` (URL building, HTTP fetch, metadata resolution),
so the mixin requires no abstract-method declarations — it duck-types
against the concrete class.
"""

# Cohesive contained-navigation logic (snapshot N+1, nested $expand drainer,
# leaf/ancestor cursor walks) keeps this mixin over pylint's 1500-line advisory
# cap; splitting it further would fragment one tightly-coupled feature.
# pylint: disable=too-many-lines

import hashlib
import json
import logging
import math
import re
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests
from pyspark.sql.types import StructField
from requests.utils import requote_uri

from databricks.labs.community_connector.sources.odata._helpers import (
    _as_exact_number,
    offset_progress_view,
    parse_iso8601,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_at_or_before_for_refilter as _cursor_at_or_before_for_refilter,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_max as _cursor_max,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_newer as _cursor_newer,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_same_instant as _cursor_same_instant,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    cursor_same_rendering as _cursor_same_rendering,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    max_or as _max_or,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    parse_max_records as _parse_max_records,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    trim_to_distinct_cursor_boundary as _trim_to_distinct_cursor_boundary,
)
from databricks.labs.community_connector.sources.odata._helpers import (
    url_origin as _url_origin,
)

_LOG = logging.getLogger(__name__)


# Path-segment separator. ``__`` (double underscore), not ``/``, so
# the framework can interpolate slash-free table names directly into
# Spark SQL identifiers (view names, temp views). The OData URL path
# still uses ``/`` — that's hardcoded in ``_build_contained_path``.
CONTAINED_PATH_SEP = "__"


# Inside generated OData request URLs the segment separator is always
# a forward slash (the wire format the spec mandates).
_URL_SEGMENT_SEP = "/"
# Cap on path depth. Prevents pathological discovery walks on services
# with cyclic containment graphs; cycles within the cap are also
# detected via target-type tracking.
MAX_CONTAINED_DEPTH = 10

# Floor for any per-level ``$top`` computed by the dynamic
# distribution. Below this the page is so small that per-request
# overhead dominates; smaller chunks also amplify the
# ``@odata.nextLink`` chase at every level.
MIN_DYNAMIC_TOP = 5


def _lb_row_key(row: dict, pks: list[str]) -> str:
    """Composite-PK identity key for the ``lb_seen`` dedup map."""
    return json.dumps([row.get(k) for k in pks], default=str)


def _lb_row_digest(row: dict) -> str:
    """Whole-row content hash for the ``lb_seen`` dedup map. Hashing the
    full row (not PK+cursor) is what lets a source update columns WITHOUT
    advancing the cursor and still get its in-window change delivered."""
    return hashlib.sha1(json.dumps(row, sort_keys=True, default=str).encode("utf-8")).hexdigest()[
        :16
    ]


class _LookbackDedupFilter:
    """Streaming half of ``cursor_lookback_dedup``: drop already-delivered
    unchanged overlap rows AT EMIT TIME, so they never materialize in the
    batch buffer — peak memory scales with rows actually delivered (plus
    ~100 bytes/entry of bookkeeping), not with the lookback window's
    churn. Armed per cursor read by ``_arm_lookback_dedup``; consumed by
    ``_apply_lookback_dedup`` in the finalize step.

    A suppressed row contributes only its carried ``lb_seen`` entry (kept
    alive for as long as the row stays in-window). Entries for DELIVERED
    rows are deliberately NOT recorded here — the finalize step computes
    them from the FINAL batch buffer, after the boundary trim, so a row
    that was admitted but later trimmed (not delivered) can never leave
    behind an entry that would suppress its future re-fetch (silent
    loss). Corrupt checkpoint entries (non-list / wrong arity) never
    match, so the row re-emits and its entry is rebuilt well-formed —
    same discipline as the ``lb_history`` validation."""

    __slots__ = ("prior", "pks", "carried")

    def __init__(self, prior: dict, pks: list[str]):
        self.prior = prior
        self.pks = pks
        self.carried: dict[str, list] = {}

    def admit(self, row: dict) -> bool:
        """True → deliver the row; False → proven already-delivered
        unchanged, drop it (its entry is carried forward)."""
        key = _lb_row_key(row, self.pks)
        entry = self.prior.get(key)
        if isinstance(entry, (list, tuple)) and len(entry) == 2 and entry[1] == _lb_row_digest(row):
            self.carried[key] = entry
            return False
        return True


def _lb_seen_order_key(raw_cursor):
    """Cursor-order sort key for ``lb_seen`` cap eviction: ISO timestamps
    sort chronologically, numeric cursors numerically, anything else by
    string length (a coarse largest-last proxy — non-negative integers
    without leading zeros still order by magnitude). ``None``/unparseable
    values sort below every parsed one (evicted first). Never raises: the
    connector supports non-timestamp cursors (integer IDs, GUIDs — see
    ``_read_incremental``), and a checkpoint entry's cursor slot is
    user-visible state, so both must degrade, not crash. A table's
    cursors are homogeneous, so the class ranks never actually interleave;
    eviction is best-effort regardless — a dropped entry only costs one
    redundant MERGE re-emit, never loss."""
    if raw_cursor is None:
        return (0, 0.0)
    raw = str(raw_cursor)
    # ``looks_like_iso8601`` (not a bare parse) so the verdict is uniform
    # across Python versions and non-ISO input can't raise.
    if looks_like_iso8601(raw):
        return (2, parse_iso8601(raw).timestamp())
    try:
        num = float(raw)
    except ValueError:
        return (0, float(len(raw)))
    # Same non-finite discipline as the ``lb_history`` validation: a
    # nan/inf cursor drops to the length class instead of poisoning the
    # sort.
    if math.isfinite(num):
        return (1, num)
    return (0, float(len(raw)))


# Default ``page_size`` applied to **cursor-based** reads (cursor_field
# or delta) when the user didn't set one, so a ``$top`` is still sent.
# Snapshot reads deliberately omit ``$top`` entirely when ``page_size``
# is unset (see ``_format_query_params``) — letting the server choose
# its page size avoids servers that reject an explicit ``$top``. Cursor
# reads keep a bounded page for predictable incremental batches.
DEFAULT_PAGE_SIZE = "1000"


_TOP_PARAM_RE = re.compile(r"(?<=[?&])(\$top=|%24top=)\d+", re.IGNORECASE)

# How many leaf-parents the cursor_probe capability preflight inspects looking
# for a discriminating sample (>= 2 distinct leaf cursors) before giving up and
# allowing the read (inconclusive). Bounds the preflight's request cost.
_CURSOR_PROBE_PREFLIGHT_SCAN = 50

# Instance-cache ``problem`` message stamped when a per-instance cursor_probe
# verdict is rehydrated from the shared process/file cache as a definitive fail
# (the original preflight message isn't cached across instances). Only surfaced
# if a same-instance strict call reuses it — non-strict callers just fall back.
_CURSOR_PROBE_SHARED_FAIL = (
    "cursor_probe nested-$expand support was previously found unreliable on this "
    "server (cached verdict); using the $batch / plain N+1 fallback."
)


class _CursorProbePreflightUnavailable(Exception):
    """The cursor-probe preflight's enumeration or trusted-reference fetch
    failed before reaching a verdict — indistinguishable from a transient,
    so it must degrade a ``cursor_probe=auto`` read to the ``$batch``/plain
    cascade (recording nothing) rather than escape as a raw HTTP error.

    Raised ONLY around the preflight's HTTP fetches, so
    ``_verify_cursor_probe_support`` can catch exactly this — a programming
    error in the preflight's own logic still propagates instead of being
    silently converted into permanent degradation."""


# Max GET sub-requests packed into one OData ``$batch`` request by the
# ``cursor_probe=batch`` / ``auto``-fallback hydrate. A hard cap — some Smart
# Default ``$batch`` chunk size: the batched walk packs leaf-parent reads (and
# their @odata.nextLink continuations) into groups of this size. A server that
# caps batch parts lower (e.g. "OData batch message contains too many parts")
# triggers an adaptive shrink (see ``_post_batch_adaptive``), and the discovered
# working size is recorded in the offset as ``batch_size_ok``.
_BATCH_MAX_OPS = 1000


def parse_batch_size_cap(raw: Any, origin: str) -> int:
    """Validate a persisted adaptive ``$batch`` chunk cap.

    Connector-written values are positive integers. Checkpoints and the
    process/file capability cache are external persistence boundaries, so a
    corrupt or hand-edited value gets an actionable error instead of a bare
    ``int()`` failure or a lossy coercion.
    """
    if isinstance(raw, bool):
        raise ValueError(
            f"Invalid batch_size_ok in {origin}: {raw!r}. Expected a positive integer (>= 1)."
        )
    try:
        size = int(raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"Invalid batch_size_ok in {origin}: {raw!r}. Expected a positive integer (>= 1)."
        ) from None
    if size < 1 or isinstance(raw, float) and not raw.is_integer():
        raise ValueError(
            f"Invalid batch_size_ok in {origin}: {raw!r}. Expected a positive integer (>= 1)."
        )
    return size


# On a "too many parts" rejection, shrink the working chunk size by this factor
# and retry, up to ``_BATCH_OVERFLOW_RETRIES`` times before falling back to a
# plain per-leaf-parent GET. The budget is sized so the geometric shrink from
# the 1000-op default converges to a small cap before giving up: 1000 × 0.75ⁿ
# crosses 100 at n=8 (1000→750→562→421→315→236→177→132→99), so 10 retries leave
# headroom for servers (e.g. Hexagon Smart API) that cap a batch around 100 parts.
_BATCH_SHRINK_FACTOR = 0.75
_BATCH_OVERFLOW_RETRIES = 10

# Page budget for the ``expand_contained=auto`` preflight GET. Small on
# purpose: ``compute_dynamic_tops`` floors every inner ``$expand`` level at
# ``MIN_DYNAMIC_TOP`` (5) and the top-level ``$top`` is rewritten to 1, so the
# probe response is one shallow subtree, not a real page.
_EXPAND_PREFLIGHT_PAGE = "25"

# HTTP statuses that say nothing definitive about a server's capabilities —
# throttling and transient server-side failures. A capability preflight that
# hits one records NO verdict (the read degrades for this batch only and the
# next batch re-probes); only a definitive outcome (a working envelope, or a
# hard rejection like 404/405) is cached and persisted as ``batch_ok``.
_TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def _is_vanished_error(exc: requests.HTTPError) -> bool:
    """Whether ``exc`` is a 404/410 — the entity addressed by the URL is
    gone. On the partitioned walk that means a parent frozen into the task
    descriptor at planning was deleted before this task walked it."""
    status = getattr(exc.response, "status_code", None)
    return status in (404, 410)


def _log_vanished_parent(chain: list, exc: requests.HTTPError) -> None:
    """One warning per skipped parent subtree. The next batch's re-plan
    re-discovers the live parent set, so the skip is self-healing; rows of
    the deleted parent already at the destination linger exactly as they
    would on any non-delta path (no tombstones)."""
    status = getattr(exc.response, "status_code", None)
    _LOG.warning(
        "partitioned read: parent chain %r vanished mid-batch (HTTP %s) — "
        "skipping its subtree; the next batch re-discovers the live parent set.",
        chain,
        status,
    )


class _BatchTooManyParts(RuntimeError):
    """Raised by :meth:`_post_batch` when the server rejects a ``$batch`` for
    carrying too many sub-requests (e.g. Hexagon Smart API: "OData batch message
    contains too many parts"). Signals the adaptive shrink path to reduce the
    chunk size and retry; a plain ``RuntimeError`` subclass so existing
    fall-back-to-plain-walk ``except`` clauses still catch it."""


def _is_batch_too_large(body: str) -> bool:
    """Whether a 4xx ``$batch`` error body indicates the request carried too many
    sub-requests — the adaptive-shrink trigger. Matches both known phrasings:
    "...contains too many parts" and "$batch exceeds the maximum of N operations".
    Heuristic: a size/limit word together with a batch-unit word, so phrasing
    variants across servers still trip the shrink rather than hard-failing."""
    low = (body or "").lower()
    size_words = ("too many", "maximum", "exceed", "limit")
    unit_words = ("part", "operation")
    return any(s in low for s in size_words) and any(u in low for u in unit_words)


def rewrite_top_in_url(url: str, new_top: int) -> str:
    """Rewrite the ``$top=<N>`` (or url-encoded ``%24top=<N>``) parameter
    in a URL's query string. Returns the URL unchanged if no ``$top``
    parameter is present.

    Used when following an inner-collection ``<NavProp>@odata.nextLink``
    continuation: the server's link inherits the small per-level
    ``$top`` from the original ``$expand($top=N;...)`` clause, but the
    continuation is one level shallower than the original
    cross-product, so a larger ``$top`` is safe and saves round trips
    when paging through a wide inner collection. OData v4 §11.2.5.7
    says clients SHOULD use the nextLink as-is — we're consciously
    rewriting only the literal ``$top`` request hint, leaving any
    skiptoken/skip parameters untouched."""
    return _TOP_PARAM_RE.sub(lambda m: m.group(1) + str(new_top), url)


def compute_dynamic_tops(page_size: int, num_levels: int) -> list[int]:
    """Distribute ``page_size`` budget across ``num_levels`` levels of
    nested ``$expand`` ``$top`` values so the cross-product
    ``$top_0 × $top_1 × … × $top_{N-1}`` stays within ``page_size`` —
    the maximum number of leaf rows a single HTTP response can carry.

    Triangular-weighted distribution: level ``i`` (0-indexed from
    the top) gets weight ``N - i`` out of ``N(N+1)/2`` total weight.
    The top URL gets the largest share since it's the outermost
    multiplier; deeper levels get progressively less. Each level is
    raised to ``MIN_DYNAMIC_TOP`` (5) so the page is never smaller
    than a useful chunk.

    When the geometric distribution would put a deep level below the
    minimum, that level is clamped to ``MIN_DYNAMIC_TOP`` and the
    *remaining* budget for upper levels is divided down accordingly,
    so the cross-product stays at-or-under ``page_size`` whenever
    that's mathematically possible.

    Examples with ``page_size = 1000``:

    * ``N=1`` (flat read): ``[1000]``
    * ``N=2`` (e.g. ``Parents__Children``): ``[100, 10]``
      → ``100 × 10 = 1000``
    * ``N=3``: ``[34, 5, 5]`` → ``850`` (under budget; bottom clamped)
    * ``N=4``: ``[8, 5, 5, 5]`` → ``1000`` exactly

    If the chain is so deep that ``MIN_DYNAMIC_TOP ** num_levels``
    already exceeds ``page_size`` (e.g. ``5**5 = 3125`` for
    ``page_size=1000``, ``N=5``), every level falls to the minimum and
    the cross-product unavoidably exceeds the budget — raise
    ``page_size`` to restore the cap, or use ``expand_contained=false``
    so the chain becomes N+1 fetches instead of a single big request.
    """
    if num_levels <= 0:
        return []
    tops = [MIN_DYNAMIC_TOP] * num_levels
    # ``remaining`` counts the *upper* levels still being distributed.
    # Anything at index ``>= remaining`` is already pinned to the minimum.
    remaining = num_levels
    budget = page_size
    while remaining > 0:
        if remaining == 1:
            tops[0] = max(MIN_DYNAMIC_TOP, int(budget))
            break
        total_weight = remaining * (remaining + 1) // 2
        candidate: list[int] = []
        any_below_min = False
        for i in range(remaining):
            weight = remaining - i
            exact = budget ** (weight / total_weight)
            # Floating-point quirk: ``1000 ** (2/3)`` is ``99.999…`` in
            # IEEE-754. Snap to the rounded integer when it's effectively
            # exact, otherwise floor so we never overshoot the budget.
            rounded = round(exact)
            value = rounded if math.isclose(exact, rounded, rel_tol=1e-9) else math.floor(exact)
            if value < MIN_DYNAMIC_TOP:
                any_below_min = True
            candidate.append(int(value))
        if not any_below_min:
            for i, v in enumerate(candidate):
                tops[i] = max(MIN_DYNAMIC_TOP, v)
            break
        # Bottom of the active range can't honour the geometric share
        # without dropping below ``MIN_DYNAMIC_TOP``. Pin it to the minimum
        # and redistribute what's left of the budget across the upper levels.
        tops[remaining - 1] = MIN_DYNAMIC_TOP
        budget = max(1, budget // MIN_DYNAMIC_TOP)
        remaining -= 1
    return tops


def compute_expand_tops_for_root(page_size: int, num_segments: int, root_level: int) -> list[int]:
    """Per-level ``$top`` for an ``$expand`` request rooted at ``root_level``.

    Only the levels from ``root_level`` to the leaf are collections that
    multiply into the response cross-product; the ancestors ``0..root_level-1``
    are addressed by key in the request path (e.g. ``Instances(6)/Projects(7)/
    WorkPackageDetails?...``), so they carry no ``$top`` and must NOT eat into
    the ``page_size`` budget. Distributing across only the collection levels is
    what lets a continuation rooted deep in the chain use a sensible ``$top``
    (e.g. ``[100, 10]`` for the last two levels) instead of the whole-chain
    floor (``[…, 5, 5]``).

    Returns a full-length list so callers keep indexing by absolute segment
    level; entries below ``root_level`` are placeholders that are never read
    (those levels carry a key, not a ``$top``)."""
    return [0] * root_level + compute_dynamic_tops(page_size, num_segments - root_level)


def join_url(base: str, suffix: str) -> str:
    """Append ``suffix`` to ``base`` with at most one slash."""
    return f"{base}{suffix}" if base.endswith("/") else f"{base}/{suffix}"


def looks_like_iso8601(s: str) -> bool:
    """Cheap ISO-8601 sniff used by ``odata_literal`` to render bare timestamps.

    Routed through :func:`parse_iso8601` so the verdict is identical on
    every supported Python — a bare ``fromisoformat`` on 3.10 rejects
    ``…00.5Z`` (1/2/4/5/7+ fractional digits), which would QUOTE a
    fractional watermark in ``$filter`` and 400 every incremental batch."""
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return False
    try:
        parse_iso8601(s)
        return True
    except ValueError:
        return False


# URL-reserved characters percent-encoded inside GENERATED literal text
# (row-data-derived values only — user-authored ``filter``/``select`` syntax
# is never touched). ``requests`` does NOT encode these when sending an
# already-assembled URL string (it only encodes spaces/non-ASCII, and it
# preserves existing escapes, so pre-encoding here is safe and decodes
# correctly server-side):
#   * ``%`` first (so the escapes below aren't double-escaped);
#   * ``+`` — form-decoding servers (ASP.NET, servlet stacks) read a raw
#     query-string ``+`` as a SPACE: a non-UTC ISO watermark
#     (``…T12:00:00+10:00``) becomes a malformed timestamp → 400 on every
#     incremental batch; ``+`` inside a quoted seek boundary silently
#     compares against the wrong value;
#   * ``&`` — splits the query at the value;
#   * ``#`` — truncates the whole request at the fragment;
#   * ``?`` — starts the query string when the literal sits in a PATH
#     segment (a key predicate ``Parent('A?B')``);
#   * ``/`` — splits the PATH segment when the literal sits in a key
#     predicate (``Parent('A/B')`` parses as two segments). ``%2F`` is the
#     spec form inside quoted path literals; in a query string it decodes
#     back to ``/`` server-side, so encoding it in both contexts is safe.
_LITERAL_ESCAPES = (
    ("%", "%25"),
    ("+", "%2B"),
    ("&", "%26"),
    ("#", "%23"),
    ("?", "%3F"),
    ("/", "%2F"),
)


def _escape_literal_text(s: str) -> str:
    """Percent-encode URL-reserved characters in generated literal text
    (see :data:`_LITERAL_ESCAPES`).

    This is deliberately not a general OData-syntax sanitizer. Spaces are
    encoded by the HTTP client's URL preparation; apostrophes in string
    values are doubled before this helper is called; and parentheses or
    apostrophes cannot occur in a conforming bare Guid/numeric/Boolean/date/
    time literal. The typed bare path is selected from CSDL, not from arbitrary
    user-authored filter text.
    """
    for raw, enc in _LITERAL_ESCAPES:
        s = s.replace(raw, enc)
    return s


def odata_literal(value: Any) -> str:
    """Render a Python value as an OData v4 literal for a generated
    ``$filter`` / key predicate. The literal ends up interpolated into a
    URL string, so URL-reserved characters inside the VALUE text are
    percent-encoded (see :data:`_LITERAL_ESCAPES`); structural characters
    (the surrounding single quotes) stay raw."""
    if isinstance(value, datetime):
        # Non-UTC offsets keep a ``+`` (only +00:00 normalizes to Z) —
        # escape it so the wire form survives form-decoding servers.
        return _escape_literal_text(value.isoformat().replace("+00:00", "Z"))
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        finite = (
            value.is_finite()
            if isinstance(value, Decimal)
            else (isinstance(value, int) or math.isfinite(value))
        )
        if not finite:
            # OData ABNF spells these ``NaN`` / ``INF`` / ``-INF``;
            # Python's ``nan`` / ``inf`` / ``Decimal("Infinity")`` are
            # not valid wire literals. (NaN check first — comparing a
            # Decimal NaN with ``>`` raises InvalidOperation.)
            nan = value.is_nan() if isinstance(value, Decimal) else math.isnan(value)
            return "NaN" if nan else ("INF" if value > 0 else "-INF")
        if isinstance(value, Decimal):
            # OData's ``decimalValue`` ABNF has NO exponent form —
            # ``str(Decimal("1.5E+7"))`` keeps the exponent and a strict
            # server 400s. ``format(…, 'f')`` renders the exact value in
            # plain positional notation.
            return _escape_literal_text(format(value, "f"))
        # ``str(1e20)`` renders ``1e+20`` — valid for Edm.Double, whose
        # ABNF allows exponents, but the exponent's ``+`` must be escaped
        # like any literal ``+`` (form-decoding servers read a raw
        # query-string ``+`` as a space → malformed number → 400).
        return _escape_literal_text(str(value))
    s = str(value)
    if looks_like_iso8601(s):
        return _escape_literal_text(s)
    return "'" + _escape_literal_text(s.replace("'", "''")) + "'"


# Edm types whose literals are BARE (unquoted) on the wire even though the
# OData JSON payload delivers the value as a JSON string. ``Edm.Guid`` is the
# big one — guid key predicates / seek boundaries are unquoted per the v4
# ABNF, and strict stacks (Olingo, SAP) 400 on ``Set('<guid>')``. The numeric
# types cover IEEE754Compatible servers that render Int64/Decimal as strings.
_EDM_BARE_TYPES = frozenset(
    {
        "Edm.Guid",
        "Edm.Boolean",
        "Edm.Byte",
        "Edm.SByte",
        "Edm.Int16",
        "Edm.Int32",
        "Edm.Int64",
        "Edm.Single",
        "Edm.Double",
        "Edm.Decimal",
        "Edm.Date",
        "Edm.DateTimeOffset",
        "Edm.TimeOfDay",
    }
)


def odata_literal_typed(value: Any, edm_type: str | None) -> str:
    """:func:`odata_literal` with the property's declared Edm type steering
    the quote decision for STRING values, instead of value sniffing.

    The sniff in :func:`odata_literal` misfires in both directions on typed
    columns: an ``Edm.Guid`` key arrives as a JSON string and gets QUOTED
    (strict servers 400 the key predicate / keyset seek), while an
    ``Edm.String`` key whose text happens to look ISO-8601 (``"2024-01-01"``)
    gets rendered BARE (invalid predicate). When the caller knows the declared
    type — key predicates and ``$orderby`` seek boundaries, where the CSDL is
    already indexed — that type wins. Falls back to :func:`odata_literal` for
    non-string values, unknown/missing types, and types with wrapped literal
    forms this connector never generates (``Edm.Duration``, ``Edm.Binary``,
    enums). Cursor-watermark filters (``_cursor_filter``) pass the declared
    type too — watermarks round-trip through offsets as strings, so the
    declared type is the only reliable quote signal; the sniff remains the
    fallback when the CSDL lookup misses."""
    if isinstance(value, str) and edm_type:
        if edm_type in _EDM_BARE_TYPES:
            return _escape_literal_text(value)
        if edm_type == "Edm.String":
            return "'" + _escape_literal_text(value.replace("'", "''")) + "'"
    return odata_literal(value)


# --- client-side pagination URL helpers -----------------------------------
# These manipulate the connector's own generated URLs, where query options
# (``$top``/``$filter``/``$orderby``/``$skip``) are stored one per
# ``&``-separated segment. Generated values contain no literal ``&`` — any
# ``&`` in row-derived literal text is percent-encoded at generation time by
# ``odata_literal`` (see ``_LITERAL_ESCAPES``; requests only encodes
# spaces/non-ASCII in an assembled URL, NOT reserved characters, so the
# encoding must happen here) — which is what makes splitting on ``&`` and
# matching on a ``$name=`` prefix safe, the same convention
# ``rewrite_top_in_url`` relies on. They live here (rather than in
# ``odata.py``) so both the flat pager (``_client_paginate_pages``) and the
# inner-``$expand`` continuation builder can use them without an import cycle;
# ``odata.py`` re-exports them for callers that still import from there.


def _pg_get_query(url: str, name: str) -> str | None:
    """Return the raw value of query option ``name`` (e.g. ``$filter``), or
    ``None`` when absent."""
    _, _, query = url.partition("?")
    pref = name + "="
    for part in query.split("&") if query else []:
        if part.startswith(pref):
            return part[len(pref) :]
    return None


def _pg_set_query(url: str, name: str, value: str) -> str:
    """Set/replace/append query option ``name`` to ``value``; preserves the
    order of existing options."""
    head, sep, query = url.partition("?")
    pref = name + "="
    parts = query.split("&") if query else []
    out, found = [], False
    for part in parts:
        if part.startswith(pref):
            out.append(f"{name}={value}")
            found = True
        else:
            out.append(part)
    if not found:
        out.append(f"{name}={value}")
    return f"{head}?{'&'.join(out)}" if (sep or out) else head


def _pg_parse_top(url: str) -> int | None:
    """Parse ``$top`` (or ``%24top``) as an int; ``None`` when absent/bad."""
    raw = _pg_get_query(url, "$top") or _pg_get_query(url, "%24top")
    return int(raw) if raw and raw.isdigit() else None


def _pg_param_name(part: str) -> str:
    """Normalized query-option name of one raw ``k=v`` query pair:
    ``%24`` decoded to ``$`` and lowercased. Server-issued continuation
    params use arbitrary casing (Microsoft stacks emit ``$skipToken``), so
    name comparisons on foreign URLs must not be case-sensitive."""
    name = part.split("=", 1)[0]
    return name.replace("%24", "$").lower()


_PG_POSITIONAL = ("$skiptoken", "$skip")


def _pg_is_continuation(url: str) -> bool:
    """Whether ``url`` carries a positional param (``$skiptoken`` or
    ``$skip``, any casing/encoding) — i.e. it points mid-collection.
    NOTE this matches the connector's OWN synthesized ``$skip``
    continuations too, not just server-issued token links: the sole
    consumer today (the ``$batch`` re-issue's ``$top`` injection guard)
    wants exactly that — never append ``$top`` onto anything positioned
    mid-walk (the §11.2.5.7 hazard) — but a future consumer needing
    "server-issued token specifically" must test for ``$skiptoken``
    alone. Case-insensitive: a camelCase ``$skipToken=`` continuation
    must not be mistaken for a plain collection URL."""
    _, _, query = url.partition("?")
    return any(
        _pg_param_name(part) in _PG_POSITIONAL for part in (query.split("&") if query else [])
    )


def _pg_strip_positional(url: str) -> str:
    """Remove every offset/token positional param (``$skip`` /
    ``$skiptoken``, any casing or ``%24`` encoding) from ``url``.

    A keyset seek positions **absolutely** via its ``$filter``, so a
    residual positional param would ALSO be applied by the server —
    ``$skip=N`` riding a seek URL skips N rows INSIDE the seek window on
    every seek page (silent, repeating loss). Reachable whenever the drain
    re-enables keyset on an entry URL that came from offset paging: a
    cap-resumed parked ``$skip`` checkpoint, or an inner-expand ``$skip``
    continuation whose later boundary rows have non-null keys."""
    head, _sep, query = url.partition("?")
    if not query:
        return url
    kept = [p for p in query.split("&") if _pg_param_name(p) not in _PG_POSITIONAL]
    return f"{head}?{'&'.join(kept)}" if kept else head


def _pg_orderby_keys(url: str) -> list[str]:
    """Column names from the URL's ``$orderby``, in order. Returns ``[]``
    when there's no ``$orderby`` or any term is ``desc`` (a ``gt`` seek
    only walks ascending order; the connector only ever emits ``asc``)."""
    raw = _pg_get_query(url, "$orderby") or _pg_get_query(url, "%24orderby")
    if not raw:
        return []
    keys = []
    # ``+`` is a legal space encoding in query strings and server-issued
    # continuation links may use it ("Id+asc") — without decoding it the
    # desc guard misses and the "key" keeps its suffix, seeking on a
    # nonexistent column (null boundary → $skip, or a server 400).
    for term in raw.replace("%20", " ").replace("+", " ").split(","):
        term = term.strip()
        if term.endswith(" desc"):
            return []
        keys.append(term[:-4].strip() if term.endswith(" asc") else term)
    return [k for k in keys if k]


def _pg_keyset_filter(
    order_keys: list[str], row: dict, edm_types: dict[str, str] | None = None
) -> str | None:
    """Build the ascending seek predicate placing the cursor strictly after
    ``row`` in ``order_keys`` order::

        (k1 gt v1) or (k1 eq v1 and k2 gt v2) or …

    Returns ``None`` if any key's value is null (no comparable boundary —
    the caller falls back to ``$skip``). ``edm_types`` (property → declared
    Edm type, when the caller has the CSDL in reach) steers quoting via
    :func:`odata_literal_typed` — a guid boundary must render BARE, an
    ISO-looking string boundary must stay QUOTED."""
    types = edm_types or {}
    vals = []
    for k in order_keys:
        v = row.get(k)
        if v is None:
            return None
        vals.append((k, v))
    clauses = []
    for i, (k, v) in enumerate(vals):
        terms = [
            f"{vals[j][0]} eq {odata_literal_typed(vals[j][1], types.get(vals[j][0]))}"
            for j in range(i)
        ]
        terms.append(f"{k} gt {odata_literal_typed(v, types.get(k))}")
        clauses.append(" and ".join(terms))
    if len(clauses) == 1:
        return clauses[0]
    return " or ".join(f"({c})" for c in clauses)


def _pg_get_filter(url: str) -> str | None:
    """The URL's ``$filter`` value, matching the ``%24filter`` spelling a
    server-issued continuation may carry (mirroring ``_pg_parse_top`` /
    ``_pg_orderby_keys`` — the positional params already get this via
    ``_pg_param_name``, but the filter readers matched only the literal
    ``$``)."""
    val = _pg_get_query(url, "$filter")
    return val if val is not None else _pg_get_query(url, "%24filter")


def _pg_with_extra_filter(url: str, clause: str) -> str:
    """AND ``clause`` into the URL's ``$filter`` (replacing any prior seek —
    the caller always rebuilds from the original base URL, so seeks never
    accumulate). A ``%24filter`` spelling is folded into the one ``$filter``
    param (never left behind — two filter params make the server pick one
    arbitrarily or 400)."""
    existing = _pg_get_filter(url)
    combined = f"({existing}) and ({clause})" if existing else clause
    return _pg_set_query(_pg_strip_query(url, "%24filter"), "$filter", combined)


# Connector-private query option carrying the stable base ``$filter`` across
# cap-resume batches of a keyset walk. Stripped before any request is sent
# (see ``_fetch_page_payload``), so the server never sees it.
_PG_BASE = "__pgbase"


def _pg_strip_query(url: str, name: str) -> str:
    """Remove query option ``name`` from ``url`` (no-op if absent)."""
    head, _sep, query = url.partition("?")
    if not query:
        return url
    pref = name + "="
    kept = [p for p in query.split("&") if not p.startswith(pref)]
    return f"{head}?{'&'.join(kept)}" if kept else head


def _pg_base_filter(url: str) -> str | None:
    """The stable base ``$filter`` for a keyset walk: the stashed ``__pgbase``
    if present (a resumed walk), else the URL's current ``$filter`` (the first
    page, before any seek — matched in both the ``$`` and ``%24`` spellings,
    see :func:`_pg_get_filter`). An empty ``__pgbase`` marker means 'no
    base'."""
    marker = _pg_get_query(url, _PG_BASE)
    if marker is not None:
        return marker or None
    return _pg_get_filter(url)


def _pg_keyset_seek_url(url: str, base_filter: str | None, seek: str) -> str:
    """Build the next keyset page URL: ``$filter`` becomes
    ``base_filter AND seek`` (or just ``seek`` when there's no base), with
    ``base_filter`` stashed in the private ``__pgbase`` option.

    Carrying the base separately lets a resumed walk REPLACE the seek instead
    of AND-ing a fresh lower bound onto the previous one every cap-resume
    batch — otherwise the ``$filter`` grows one keyset clause per batch and
    eventually overflows the server's URL-length limit. The seeks are
    monotonic so the accumulated form is merely redundant, never wrong, but it
    is unbounded. ``__pgbase`` is stripped before the request is sent.

    Any positional param on the entry URL (``$skip`` from a resumed parked
    checkpoint or an inner-expand continuation, a stray ``$skiptoken``) is
    stripped: the seek is absolute, and a retained ``$skip=N`` would skip N
    rows inside the seek window on every seek page — see
    :func:`_pg_strip_positional`."""
    combined = f"({base_filter}) and ({seek})" if base_filter else seek
    # Strip an encoded ``%24filter`` spelling before setting ``$filter`` so a
    # server-issued continuation resumed into this walk never ends up with
    # two filter params (the base was already recovered via _pg_get_filter).
    cleaned = _pg_strip_query(_pg_strip_positional(url), "%24filter")
    out = _pg_set_query(cleaned, "$filter", combined)
    return _pg_set_query(out, _PG_BASE, base_filter or "")


def _pg_page_fingerprint(page_rows: list[dict]) -> int:
    """Order-sensitive fingerprint of a page's rows for the no-progress
    guard. ``hash(repr(...))`` is process-stable — only ever compared within
    a single walk — and costs one page's worth of work. Two consecutive
    non-empty pages with the same fingerprint mean the server returned the
    same data twice (it ignored our seek/``$skip`` or handed back a cyclic
    ``@odata.nextLink``), so the walk has stalled. Callers pass the RAW
    payload items (``@odata.*`` annotations included): with a
    low-cardinality ``$select``, two distinct consecutive pages can be
    identical after the annotation strip, and per-entity annotations
    (id/etag) are what disambiguate them."""
    return hash(repr(page_rows))


# Sentinel distinguishing "walk has no ancestor cursor" (leaf-cursor / $batch
# walks) from "ancestor cursor is None" (the ancestor-cursor walk, where a
# null stamped cursor is a real value the ordering must carry).
_NO_ANCESTOR_CURSOR = object()


def _chain_resume_key(
    chain: list[dict],
    ancestor_cursor: Any = _NO_ANCESTOR_CURSOR,
    cursor_level: int = 0,
) -> list:
    """Client-side ordering key for one ancestor key chain, mirroring the
    chain enumerations' server-side ordering. The enumeration is NESTED —
    each level orders within its parent (PK asc; the cursor level prefixes
    ``cursor asc`` WITHIN that level, see ``_iter_parent_chains_with_cursor``)
    — so the ancestor-cursor walk's cursor term is inserted at its LEVEL's
    position, never globally first: a globally-first cursor misorders every
    ``cursor_level >= 1`` path (a chain under a later top-level parent with
    a lower cursor would sort "before" the park and be skipped unwalked —
    permanent subtree loss on a completely stable source). Values stay RAW
    (the server's rendered text); all ordering happens in
    :func:`_chain_strictly_before` via the chronological comparators.

    MUST return a ``list`` (not a tuple): this key is parked in the resume
    offset and JSON round-trips to a list, so the resume-skip equality
    (``chain == parked_chain``) only holds if the freshly-built key is also a
    list. A tuple would silently defeat the skip (harmless — the walk just
    reprocesses the parked boundary, duplicate-safe — but slower)."""
    key: list = []
    for idx, level_keys in enumerate(chain):
        if ancestor_cursor is not _NO_ANCESTOR_CURSOR and idx == cursor_level:
            key.append(ancestor_cursor)
        key.extend(level_keys.values())
    if ancestor_cursor is not _NO_ANCESTOR_CURSOR and cursor_level >= len(chain):
        key.append(ancestor_cursor)
    return key


def _order_reproducible(a: Any, b: Any) -> bool:
    """Whether the client can reproduce the server's relative ORDER for a
    differing pair of enumeration-key elements. JSON numbers order
    numerically on every server; ISO-rendered instants order chronologically
    under every collation; and a real number paired with a numeric string
    proves the property is numeric (a rendering flip), so the exact-Decimal
    bridge holds. Anything else — plain text, GUID strings, digits-only
    ``Edm.String`` values — is ordered by the SERVER's collation
    (case-insensitive SQL Server defaults, ``uniqueidentifier`` byte-group
    order, ICU locales…), which Python's ordinal comparison cannot
    reproduce: trusting it turns "not yet walked" into "already walked" and
    silently drops whole subtrees at a park boundary."""
    if isinstance(a, bool) or isinstance(b, bool):
        return False
    a_num = isinstance(a, (int, float))
    b_num = isinstance(b, (int, float))
    if a_num and b_num:
        return True
    if (a_num or b_num) and _as_exact_number(a) is not None and _as_exact_number(b) is not None:
        return True

    def _chrono(v: Any) -> bool:
        return isinstance(v, datetime) or (isinstance(v, str) and looks_like_iso8601(v))

    return _chrono(a) and _chrono(b)


def _chain_seek_order(key_a: list, key_b: list) -> str:
    """Collation-honest three-way order of enumeration position ``key_a``
    vs ``key_b`` (both from :func:`_chain_resume_key`, raw values):
    ``"before"`` / ``"after"`` only when the first differing element's order
    is provable client-side (:func:`_order_reproducible`), else
    ``"unknown"`` — the seek loops keep seeking on their identity anchor
    for unknowns instead of guessing (see the call sites)."""
    try:
        for a, b in zip(key_a, key_b):
            # Same-RENDERING equality (raw, chronological-key, or a
            # number/numeric-string type flip) — two renderings of an
            # EQUAL element must fall through to the next element, not
            # decide the order. Deliberately NOT cursor_same_instant:
            # its numeric branch calls two numeric STRINGS with different
            # text ("007" vs "7") equal, which conflates two DISTINCT
            # parents into one position and lets a later element decide
            # order ACROSS parents — a false "before" skips an unwalked
            # subtree past the vanished-anchor reset (silent loss).
            if _cursor_same_rendering(a, b):
                continue
            if not _order_reproducible(a, b):
                return "unknown"
            # First differing element decides: a sorts before b iff b is
            # strictly newer/greater. cursor_newer is a strict total order
            # over comparable values, so this is well-defined.
            return "before" if _cursor_newer(b, a) else "after"
        if len(key_a) < len(key_b):
            return "before"
        return "after"
    except TypeError:
        return "unknown"


def _expand_resume_start(page_rows: list, boundary: list | None, skip: int, order_key) -> tuple:
    """Resume position for a re-fetched parked expand page: identity-anchor
    on the parked row itself (``order_key(row) == boundary``) and resume
    right after it — exact for EVERY key type with zero reliance on
    reproducing the server's collation client-side. Returns
    ``(start_idx, boundary)``: the anchor consumes the boundary (``None``);
    an anchor row that's gone (deleted / churned out of the page) keeps the
    boundary so the caller's per-row PROVABLE-order skip takes over
    (duplicate-safe). ``skip`` is the legacy positional fallback used only
    when no boundary was parked at all."""
    if boundary is None:
        return skip, None
    for anchor_idx, anchor_row in enumerate(page_rows):
        if order_key(anchor_row) == boundary:
            return anchor_idx + 1, None
    return 0, boundary


def _chain_strictly_before(key_a: list, key_b: list) -> bool:
    """Whether enumeration position ``key_a`` PROVABLY sorts strictly before
    ``key_b`` (both from :func:`_chain_resume_key`, raw values).

    This drives the "already walked in a prior capped batch" skip. The
    positional (index-based) skip it replaces silently desynchronizes
    under parent-set churn between batches — a deleted parent shifts
    every successor left one slot, so the resume skips an unwalked
    parent forever; an insert shifts right, so a parked mid-collection
    continuation link gets applied to the WRONG parent (its rows then
    FK-tagged with that parent's keys). Comparing by the enumeration's
    own ordering keys is churn-stable.

    Elements compare via :func:`cursor_newer` — chronological for
    ISO-rendered values INCLUDING the padded-fraction sub-microsecond
    tie-break. A µs-truncating comparison here re-opens the round-18 tie
    class one layer up: two 100ns-distinct cursors (SQL Server
    ``datetime2(7)``) tie, the seek loop stops one chain early, the
    parked continuation is silently dropped, and the walk re-parks a
    byte-identical offset — a permanently failing (no-progress) or
    silently starved stream. Pairs whose order isn't provable client-side
    (cross-type values after a metadata change, or plain-text/GUID keys
    the SERVER orders by ITS collation — see :func:`_order_reproducible`)
    return ``False`` — the chain is NOT skipped, degrading to a
    duplicate-safe re-read instead of a silent skip."""
    return _chain_seek_order(key_a, key_b) == "before"


# Re-export of the EDM namespace prefix used by the main module.
_NS_EDM = "{http://docs.oasis-open.org/odata/ns/edm}"


def parse_contained_path(table_name: str) -> list[str] | None:
    """Split double-underscore-delimited path; ``None`` for flat names."""
    if _URL_SEGMENT_SEP in table_name:
        # OData entity-set names cannot contain ``/`` (CSDL allows only
        # letters/digits/underscores), so a slash here always means the
        # caller used the wrong separator. Spell out the fix — the
        # generic "not found" error otherwise buries the cause under a
        # 200-entry "Available:" list.
        suggested = table_name.replace(_URL_SEGMENT_SEP, CONTAINED_PATH_SEP)
        raise ValueError(
            f"Contained-collection table names use {CONTAINED_PATH_SEP!r} "
            f"(double underscore) as the segment separator, not "
            f"{_URL_SEGMENT_SEP!r} — slashes aren't valid in Spark SQL "
            f"identifiers, which the SDP framework uses for view names. "
            f"Rename {table_name!r} to {suggested!r} in the pipeline "
            f"config."
        )
    if CONTAINED_PATH_SEP not in table_name:
        return None
    segments = table_name.split(CONTAINED_PATH_SEP)
    if any(not s for s in segments):
        raise ValueError(
            f"Empty path segment in contained table name {table_name!r}; "
            "expected 'Parent__Child' or 'Parent__Child__Grandchild' form."
        )
    if len(segments) > MAX_CONTAINED_DEPTH:
        raise ValueError(
            f"Contained path {table_name!r} exceeds max depth "
            f"{MAX_CONTAINED_DEPTH} (got {len(segments)})."
        )
    return segments


def resolve_segment_filters(
    table_options: dict[str, str] | None,
    segments: list[str],
) -> dict[int, str]:
    """Parse ``filters_at`` into a ``{level: filter_string}`` mapping.

    Per-segment filters let the user push a ``$filter`` to the exact
    walk level (or ``$expand`` clause) that owns the property. Without
    this, the table's single ``filter`` option lands at one URL only
    (leaf for N+1 mode, top for expand mode), leaving intermediate
    levels unfiltered and forcing a full fan-out.

    ``filters_at`` is a JSON object whose keys use either form:

    * **By segment name** — ``{"Instances": "Id eq 5"}`` matches the
      segment literally as it appears in the contained path / URL.
    * **By zero-based index** — ``{"0": "Id eq 5"}`` matches the level
      positionally. Useful when nav-property names repeat at different depths.

    Both forms may be set; the **index form wins on conflict**, since
    it's the more explicit of the two. Unknown segment names and
    out-of-range indices raise ``ValueError`` immediately so typos
    don't silently produce a full-fan-out walk.

    Legacy ``filter_at_<segment>`` / ``filter_at_<idx>`` keys remain accepted
    for direct callers, but the UC allowlist exposes only ``filters_at``.
    """
    if not table_options:
        return {}
    raw_filters = table_options.get("filters_at")
    filters: dict[str, str] = {}
    if raw_filters is not None:
        try:
            decoded = json.loads(raw_filters)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "Invalid table option filters_at: expected a JSON object "
                'such as {"Instances": "Id eq 5"}.'
            ) from exc
        if not isinstance(decoded, dict):
            raise ValueError("Invalid table option filters_at: expected a JSON object.")
        for key, value in decoded.items():
            if not isinstance(key, str) or not key:
                raise ValueError(
                    "Invalid table option filters_at: every key must be a non-empty string."
                )
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"Invalid table option filters_at[{key!r}]: filter must be a "
                    "non-empty string."
                )
            filters[key] = value

    # Backward compatibility for direct callers. These dynamic keys cannot be
    # represented by UC's exact-match external-options allowlist.
    for key, value in table_options.items():
        if key.startswith("filter_at_"):
            filters.setdefault(key[len("filter_at_") :], value)

    out: dict[int, str] = {}
    # Match segment names case-insensitively against the discovered path.
    # This also preserves compatibility with legacy dynamic option keys,
    # which Lakeflow lowercases before forwarding them to ``read_table``.
    seg_to_idx = {s.lower(): i for i, s in enumerate(segments)}
    # Pass 1: name-keyed. Index-keyed entries override these on
    # conflict, so process them after.
    for suffix, value in filters.items():
        if suffix.isdigit():
            continue
        idx = seg_to_idx.get(suffix.lower())
        if idx is None:
            raise ValueError(
                f"Invalid table option filters_at[{suffix!r}]={value!r}: segment "
                f"{suffix!r} not in path {segments!r}. Valid "
                f"segments (case-insensitive): {segments}."
            )
        out[idx] = value
    # Pass 2: index-keyed (overrides name form when both target the
    # same level).
    for suffix, value in filters.items():
        if not suffix.isdigit():
            continue
        idx = int(suffix)
        if not 0 <= idx < len(segments):
            raise ValueError(
                f"Invalid table option filters_at[{suffix!r}]={value!r}: index {idx} "
                f"out of range for path with {len(segments)} segments "
                f"(valid: 0..{len(segments) - 1})."
            )
        out[idx] = value
    return out


def combine_filters(*clauses: str | None) -> str | None:
    """``AND`` non-empty OData ``$filter`` clauses, wrapping each in
    parens to preserve precedence. Returns ``None`` when nothing is
    non-empty so callers can omit ``$filter`` entirely."""
    nonempty = [c for c in clauses if c]
    if not nonempty:
        return None
    if len(nonempty) == 1:
        return nonempty[0]
    return " and ".join(f"({c})" for c in nonempty)


def contained_nav_props(entity_type: ET.Element) -> list[tuple[str, str]]:
    """``[(nav_name, target_type_ref), ...]`` for ContainsTarget collection
    nav props declared directly on this type; singletons skipped."""
    out: list[tuple[str, str]] = []
    for np in entity_type.findall(f"{_NS_EDM}NavigationProperty"):
        if (np.get("ContainsTarget") or "").lower() != "true":
            continue
        type_ref = np.get("Type", "")
        if not (type_ref.startswith("Collection(") and type_ref.endswith(")")):
            continue
        out.append((np.get("Name"), type_ref[len("Collection(") : -1]))
    return out


def fk_column_name(segment: str, pk_name: str) -> str:
    """Default ancestor-FK column name: ``<segment>_<pkname>``.

    The actual column the connector writes may be further disambiguated
    (prefixed with leading underscores) if it collides with a leaf
    property or another FK column. See ``ContainedNavMixin._resolve_fk_columns``.
    """
    return f"{segment}_{pk_name}"


def validate_page_size(opts: dict) -> None:
    """Reject a non-positive / non-numeric ``page_size`` with a curated error.

    ``$top=0`` is the nasty case: it's a perfectly valid URL the server
    answers with an empty page, which the client-driven drain reads as
    exhaustion — every read of the table would silently emit zero rows. A
    non-numeric value would ride into the URL raw and surface only as a
    confusing server 400. Every other numeric table option raises a curated
    error on garbage; ``page_size`` shouldn't be the silent one. Called from
    ``read_table`` and the partition entry points (``is_partitioned`` /
    ``get_partitions``), which don't route through ``read_table``."""
    raw = opts.get("page_size")
    if raw is None:
        return
    text = str(raw).strip()
    if not text.isdigit() or int(text) < 1:
        raise ValueError(
            f"page_size={raw!r} is not a positive integer. page_size sets "
            f"the per-request $top; use a value >= 1, or unset it for the "
            f"default (1000 — or, under pagination=nextlink, no $top at "
            f"all on snapshot reads)."
        )


def _ancestor_pk_order_by(ancestor_pks: list[str]) -> str:
    """Build a stable PK-only ``$orderby`` clause for ancestor key
    enumeration. OData v4 §11.2.5.7 (server-driven paging) doesn't
    promise stable default ordering across pages without an explicit
    ``$orderby`` over a unique key set, so server skiptokens can
    silently drop or duplicate ancestor rows — every leaf row under a
    skipped ancestor would then be lost. The leaf-cursor path already
    composes ``cursor asc, pk asc`` for the same reason
    (``_leaf_cursor_order_by``); ancestor key fetches need the
    PK-only variant of the same guarantee.
    """
    return ",".join(f"{pk} asc" for pk in ancestor_pks)


class ContainedNavMixin:
    """Mixin providing contained-collection support for the OData connector.

    Plug in via ``class ODataLakeflowConnect(LakeflowConnect,
    SupportsNamespaces, ContainedNavMixin):``. All methods duck-type
    against the concrete class for HTTP/URL/metadata helpers.
    """

    def _all_contained_nav_props(self, entity_type: ET.Element) -> list[tuple[str, str]]:
        """Contained nav props on the type and its base chain (closest-
        descendant wins on name collision)."""
        out: dict[str, str] = {}
        for type_el in self._resolve_base_chain(entity_type):
            for name, ref in contained_nav_props(type_el):
                out.setdefault(name, ref)
        return list(out.items())

    def _enumerate_contained_paths(self, top_level_set: str, namespace: str | None) -> list[str]:
        """BFS contained nav-property graph; cap at MAX_CONTAINED_DEPTH;
        break cycles via target-type set."""
        try:
            root_et = self._flat_entity_type_for(top_level_set, namespace)
        except ValueError as exc:
            # The flat set still lists (its own read fails loudly), but its
            # contained children can't be enumerated — say so instead of
            # silently listing it childless.
            _LOG.warning(
                "Cannot enumerate contained paths under %r: %s. The set is "
                "listed without contained children.",
                top_level_set,
                exc,
            )
            return []
        paths: list[str] = []
        # Cycle detection: start with an empty ``seen`` so the very first
        # self-reference (e.g. ``Node.Self → Node``) still emits a depth-1
        # path. Recursion is bounded by adding each traversed type to
        # ``seen`` before recursing.
        queue: list[tuple[list[str], ET.Element, set[str]]] = [([top_level_set], root_et, set())]
        while queue:
            segments, et, seen = queue.pop(0)
            if len(segments) >= MAX_CONTAINED_DEPTH:
                continue
            for nav_name, target_ref in self._all_contained_nav_props(et):
                if target_ref in seen:
                    continue
                if CONTAINED_PATH_SEP in nav_name:
                    # A nav property whose NAME contains the path separator
                    # ("My__Kids" — CSDL SimpleIdentifiers legally allow
                    # consecutive underscores) would list as a table name
                    # the read path's ``__``-splitting can never resolve
                    # back (declared-flat-set longest-prefix matching covers
                    # entity SETS only, not nav-prop names). Emitting it
                    # breaks the discovery→read round trip: skip with a
                    # warning instead of listing an unreadable table.
                    _LOG.warning(
                        "Skipping contained collection %r under %r: navigation-"
                        "property names containing '__' cannot be addressed by "
                        "this connector's path syntax.",
                        nav_name,
                        CONTAINED_PATH_SEP.join(segments),
                    )
                    continue
                target_et = self._resolve_type_ref(target_ref)
                if target_et is None:
                    continue
                new_segments = segments + [nav_name]
                paths.append(CONTAINED_PATH_SEP.join(new_segments))
                queue.append((new_segments, target_et, seen | {target_ref}))
        return paths

    # --- option parsing ----------------------------------------------------

    def _expand_contained_mode(self, table_options: dict[str, str] | None) -> str:
        """Parse the ``expand_contained`` table option: ``true``, ``false``,
        or ``auto`` (**default** when unset).

        ``auto`` attempts the nested-``$expand`` read first: a one-shot
        behavioural preflight (:meth:`_verify_expand_support`) issues the real
        expand URL — with the same inner ``$top``/``$orderby``/``$filter``
        constructs the read would send — and verifies the server actually
        returns inline child collections (cross-checked against direct
        navigation, so a server that accepts the URL but silently ignores
        ``$expand`` is caught). ONLY a conclusive pass runs the expand read;
        anything else — a definitive failure, a transient blip, or an
        inconclusive sample — **falls back** to the N+1 walks
        (``expand_contained=false``) for that batch, never raising on a
        capability shortfall and never assuming ``$expand`` works before the
        verdict is in. The verdict persists in the resume offset as
        ``expand_ok`` (mirrors ``batch_ok``); any non-``auto`` value scrubs
        it so re-selecting ``auto`` re-runs the preflight."""
        raw = ((table_options or {}).get("expand_contained") or "auto").strip().lower()
        if raw not in {"true", "false", "auto"}:
            raise ValueError(
                f"Invalid expand_contained={raw!r}. Expected one of: true, false, auto."
            )
        return raw

    def _expand_contained_active(self, table_options: dict[str, str] | None) -> bool:
        """``True`` only for an explicit ``expand_contained=true`` (the strict
        opt-in). ``auto`` returns ``False`` here — validation gates that key on
        this (e.g. the ``cursor_probe`` conflict check) must not fire for
        ``auto``, whose expand attempt silently degrades instead."""
        return self._expand_contained_mode(table_options) == "true"

    def _cursor_probe_mode(self, table_options: dict[str, str] | None) -> str:
        """Parse the ``cursor_probe`` table option into a leaf-cursor read
        acceleration mode. One of:

        * ``auto`` (**default**, when the option is unset) — best-effort
          cascade. Use the nested-``$expand`` change-probe where it can pay off
          *and* the server is verified to honour ``$orderby``/``$top`` inside
          ``$expand``; otherwise fall back to an OData ``$batch`` hydrate (when
          the server supports ``$batch``); otherwise the plain N+1 walk. Never
          raises on a server-capability shortfall — it degrades to a correct,
          slower strategy.
        * ``nested-expand`` → ``probe`` — strict nested-``$expand`` probe.
          **Raises** if the path can't use it or the server mis-orders inner
          ``$expand`` (the original fail-fast semantics): "I require the probe."
        * ``batch`` — skip the probe; hydrate the changed leaves via OData
          ``$batch`` (server-driven paging, no ``$top``, ``@odata.nextLink``
          follow-up, chunked to :data:`_BATCH_MAX_OPS` ops/request), falling
          back to the plain N+1 walk if the server doesn't support ``$batch``.
          The ``batch:<N>`` form tunes the chunk size to ``N`` ops/request (a
          positive integer) — e.g. ``batch:50`` for servers that reject
          100-op batches; see :meth:`_cursor_probe_batch_size`.
        * ``false`` → ``off`` — force the plain N+1 walk.

        The change-probe issues one shallow
        ``$expand(<leaf>($orderby=cursor desc;$top=1;$select=cursor))`` per
        leaf-grandparent to find each leaf-parent's newest leaf, marks it dirty
        when that cursor is ``> since`` (client-side), then runs the normal N+1
        walk over only the dirty leaf-parents. The ``$batch`` hydrate skips the
        identify step entirely: it issues the plain per-leaf-parent
        ``cursor gt since`` reads, but packs them into ``$batch`` requests so M
        leaf-parent round-trips collapse to ``ceil(M / _BATCH_MAX_OPS)``. Both
        emit rows identical to ``off`` (the plain walk) — the probe relies on
        inner-``$expand`` ordering (hence the capability check), while ``$batch``
        relies only on top-level single-column ``cursor gt`` filters, so it is
        safe on servers (e.g. Hexagon Smart API) that reject nested ``$expand``
        options. See :meth:`_cursor_probe_applicable` and
        :meth:`_verify_batch_support`."""
        raw = ((table_options or {}).get("cursor_probe") or "auto").strip().lower()
        aliases = {"nested-expand": "probe", "false": "off", "batch": "batch", "auto": "auto"}
        base, sep, _suffix = raw.partition(":")
        if sep:
            # Only ``batch`` carries a ``:<N>`` chunk-size suffix.
            if base != "batch":
                raise ValueError(
                    f"Invalid cursor_probe={raw!r}. Only 'batch' accepts a "
                    "':<N>' size suffix (e.g. batch:50)."
                )
            self._cursor_probe_batch_size(table_options)  # validate N (raises on bad)
            return "batch"
        if raw not in aliases:
            raise ValueError(
                f"Invalid cursor_probe={raw!r}. Expected one of: "
                "nested-expand, batch, batch:<N>, auto, false."
            )
        return aliases[raw]

    def _cursor_probe_batch_size(self, table_options: dict[str, str] | None) -> int:
        """Ops per ``$batch`` request for ``cursor_probe=batch``. Defaults to
        :data:`_BATCH_MAX_OPS` (1000); the ``batch:<N>`` form overrides it with a
        positive integer ``N`` (``ceil(M / N)`` requests for M leaf-parents). The
        effective size is further reduced at runtime if the server rejects a
        batch for "too many parts" (see :meth:`_post_batch_adaptive`).
        Returns :data:`_BATCH_MAX_OPS` for every non-``batch:`` value."""
        raw = ((table_options or {}).get("cursor_probe") or "auto").strip().lower()
        base, sep, suffix = raw.partition(":")
        if not sep or base != "batch":
            return _BATCH_MAX_OPS
        try:
            size = int(suffix)
        except ValueError:
            raise ValueError(
                f"Invalid cursor_probe={raw!r}. The batch size suffix must be a "
                "positive integer (e.g. batch:50)."
            ) from None
        if size < 1:
            raise ValueError(
                f"Invalid cursor_probe={raw!r}. The batch size suffix must be a "
                "positive integer (>= 1)."
            )
        return size

    def _cursor_probe_applicable(
        self,
        segments: list[str],
        namespace: str | None,
        cursor_field: str,
        cursor_level: int,
    ) -> bool:
        """Whether the probe can actually save work on this path.

        Two conditions:

        1. The cursor lives on the **leaf** (``cursor_level`` is the last
           segment). An ancestor cursor already filters whole subtrees, so
           there's nothing to probe.
        2. The **distance from the leaf to the nearest batch-snapshot
           ancestor is > 1** — i.e. the leaf's *parent* collection is
           itself a cursor-bearing (incremental, typically high-fan-out)
           entity. The savings come from skipping leaf hydrates for *clean*
           leaf-parents; when the leaf-parent is a snapshot structural level
           (e.g. ``.../Projects/WorkPackageDetails`` where only the leaf
           carries the cursor), it has few rows that all read as dirty, so
           the probe only adds ``$expand`` payload with nothing to skip.

        A "snapshot ancestor" is one whose entity type does not declare
        ``cursor_field``. ``snapshot_idx`` is the deepest such ancestor
        (``-1`` if every ancestor declares it); the probe engages when
        ``leaf_idx - snapshot_idx > 1``.
        """
        leaf_idx = len(segments) - 1
        if cursor_level != leaf_idx:
            return False
        snapshot_idx = -1
        for idx in range(leaf_idx):  # ancestors only, leaf excluded
            ancestor_et = self._entity_type_for(
                CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
            )
            if not any(f.name == cursor_field for f in self._own_fields_for_et(ancestor_et)):
                snapshot_idx = idx
        return (leaf_idx - snapshot_idx) > 1

    # --- URL construction --------------------------------------------------

    def _format_key_predicate(
        self, pk_values: dict[str, Any], edm_types: dict[str, str] | None = None
    ) -> str:
        """``(value)`` for single key; ``(K1=v1,K2=v2)`` for composite.
        ``edm_types`` (the level's declared property types) steers quoting —
        a guid key renders BARE, an ISO-looking string key stays QUOTED."""
        types = edm_types or {}
        if len(pk_values) == 1:
            ((k, v),) = pk_values.items()
            return f"({odata_literal_typed(v, types.get(k))})"
        return (
            "("
            + ",".join(f"{k}={odata_literal_typed(v, types.get(k))}" for k, v in pk_values.items())
            + ")"
        )

    def _edm_types_for_level(
        self, segments: list[str], level: int, namespace: str | None
    ) -> dict[str, str]:
        """Best-effort property-type map for the collection at
        ``segments[level]``. Resolution failure (e.g. an entity-set name
        ambiguous across namespaces when the caller has no ``namespace`` in
        scope) degrades to ``{}`` — sniff-based literal rendering — rather
        than failing a URL build that worked untyped for years. The failure
        outcome is memoized per (path, namespace): this runs once per URL
        build (per parent chain on an N+1 walk), and re-raising +
        re-formatting ``_entity_type_for``'s sorted "Available: …" error
        100k times per cycle is pure waste (success is already memoized
        inside ``_entity_type_for``/``_edm_types_for_et``)."""
        path = CONTAINED_PATH_SEP.join(segments[: level + 1])
        failed = self.__dict__.setdefault("_edm_types_unresolvable", set())
        if (path, namespace) in failed:
            return {}
        try:
            return self._edm_types_for_et(self._entity_type_for(path, namespace))
        except Exception:  # noqa: BLE001 — metadata gaps must not break URL builds
            failed.add((path, namespace))
            return {}

    def _build_contained_path(
        self,
        segments: list[str],
        key_chain: list[dict[str, Any]],
        namespace: str | None = None,
    ) -> str:
        """``A(1)/B('x')/C`` — leaf segment has no key; ``key_chain`` len = N-1.
        Key values render TYPED per level (guid bare / string quoted); the
        type lookup is best-effort, so callers without a ``namespace`` in
        scope still work (sniff-rendered) on ambiguous multi-namespace
        services."""
        if len(key_chain) != len(segments) - 1:
            raise ValueError(
                f"key_chain length {len(key_chain)} does not match "
                f"non-leaf segment count {len(segments) - 1}"
            )

        def _render(i: int, seg: str) -> str:
            if i >= len(key_chain):
                return seg
            types = self._edm_types_for_level(segments, i, namespace)
            return f"{seg}{self._format_key_predicate(key_chain[i], types)}"

        return _URL_SEGMENT_SEP.join(_render(i, seg) for i, seg in enumerate(segments))

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _build_contained_url(
        self,
        segments: list[str],
        key_chain: list[dict[str, Any]],
        table_options: dict[str, str],
        extra_filter: str | None = None,
        order_by: str | None = None,
    ) -> str:
        """Full URL for a contained-collection read at one parent tuple."""
        base = join_url(
            self.service_url,
            self._build_contained_path(segments, key_chain, table_options.get("namespace")),
        )
        return f"{base}?{self._format_query_params(table_options, extra_filter, order_by)}"

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _build_expand_url(
        self,
        segments: list[str],
        table_options: dict[str, str],
        cursor_level: int | None = None,
        cursor_filter: str | None = None,
        cursor_order: str | None = None,
        cursor_select: str | None = None,
    ) -> str:
        """``A?...&$expand=B($top=N;$expand=C($top=N;$expand=D($top=N)))`` for the full chain.

        When ``cursor_level`` is set, ``cursor_filter``/``cursor_order``/
        ``cursor_select`` are injected at the segment that owns the
        cursor — at the top-level URL when ``cursor_level == 0``, or
        inside the corresponding ``$expand`` clause otherwise. The
        ``$select`` is necessary because some OData servers omit
        properties from ``$expand`` responses by default; explicitly
        requesting the cursor guarantees the server projects it onto
        the ancestor rows so it can be stamped onto leaf rows. OData
        v4 §5.1.1.13: inner ``$expand`` options are separated by ``;``.

        ``$top`` is emitted at every nested ``$expand`` level so the
        server's default doesn't surprise us (Hexagon SCApi for example
        caps inner expansions at 100 regardless of the request) and so
        the connector controls the per-response row count.

        Per-level ``$top`` values are computed dynamically by
        :func:`compute_dynamic_tops`: the ``page_size`` budget is
        distributed across all ``$top`` points with triangular weights
        — the top URL gets the largest share, each deeper level
        proportionally less — so the worst-case cross-product stays
        within ``page_size``. ``$top=1000`` at every level of a
        3-segment expand would ask for up to 1B rows in one response
        and times out every real server; the dynamic distribution
        keeps it bounded (e.g. ``[31, 10, 5]`` for depth 3 with
        ``page_size=1000``). Servers that don't honour ``$top`` inside
        ``$expand`` ignore it — the wire format stays valid OData v4.
        """
        segment_filters = resolve_segment_filters(table_options, segments)
        base = join_url(self.service_url, segments[0])
        opts = table_options or {}
        # ``$top`` is emitted across the expand levels only when the user
        # set ``page_size``; with none, no ``$top`` is sent at any level
        # and the server picks its own page size (see
        # ``_format_query_params``). ``per_level_tops`` is ``None`` then.
        per_level_tops = (
            compute_dynamic_tops(int(opts["page_size"]), len(segments))
            if opts.get("page_size")
            else None
        )
        return self._assemble_expand_url(
            base,
            segments,
            0,
            table_options,
            segment_filters,
            cursor_level,
            cursor_filter,
            cursor_order,
            cursor_select,
            per_level_tops,
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _expand_level_order_by(
        self,
        segments: list[str],
        level: int,
        namespace: str | None,
        cursor_level: int | None,
        cursor_order: str | None,
    ) -> str | None:
        """``$orderby`` for one expand level so server skiptoken paging is
        stable — for the top collection AND each expanded sub-collection.
        OData v4 §11.2.5.7 promises no stable default order across pages, so
        without a unique sort a value-based skiptoken can drop or duplicate
        rows (the same failure the N+1 path guards against via
        ``_ancestor_pk_order_by`` / ``_leaf_pk_order_by``). The cursor-owning
        level keeps its cursor-first composite (``cursor_order``); every
        other level falls back to PK-only. Servers that ignore ``$orderby``
        inside ``$expand`` leave the wire format valid OData v4 — same
        contract as ``$top``. The server-generated
        ``<NavProp>@odata.nextLink`` continuations preserve these options per
        §11.2.6.1, so paging stays ordered.

        Returns ``None`` when the segment isn't resolvable in ``$metadata``
        (only fires for synthetic paths; a real expand path is validated
        upstream) — degrade to the server default rather than crash the URL
        build.
        """
        if level == cursor_level and cursor_order:
            return cursor_order
        try:
            et = self._entity_type_for(CONTAINED_PATH_SEP.join(segments[: level + 1]), namespace)
        except ValueError:
            return None
        return _ancestor_pk_order_by(self._own_primary_keys_for_et(et)) or None

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _assemble_expand_url(
        self,
        base: str,
        segments: list[str],
        start_level: int,
        table_options: dict[str, str],
        segment_filters: dict[int, str],
        cursor_level: int | None,
        cursor_filter: str | None,
        cursor_order: str | None,
        cursor_select: str | None,
        per_level_tops: list[int] | None,
    ) -> str:
        """Render an expand URL rooted at ``base`` whose top collection is
        ``segments[start_level]`` and whose nested ``$expand`` chain covers
        ``segments[start_level + 1:]``.

        ``start_level == 0`` reproduces the full top-level request (used by
        :meth:`_build_expand_url`). ``start_level > 0`` roots the request at
        a contained path — ``base`` already carries the ancestor keys — and
        is used by :meth:`_build_expand_continuation_url` to page a single
        parent's inner collection client-side when the server omits its
        ``<NavProp>@odata.nextLink``.

        Filters, ``$top``, ``$orderby`` and the cursor injection are all
        keyed by ABSOLUTE segment level, so the same ``segment_filters`` /
        ``cursor_level`` resolved against the full path stay correct for any
        ``start_level``.
        """
        namespace = (table_options or {}).get("namespace")
        opts = table_options or {}
        has_children = start_level < len(segments) - 1
        # The table's ``filter`` and ``select`` options are LEAF-scoped —
        # same as N+1 mode, where they land at the leaf URL, and same as the
        # schema derivation / option validation, which resolve them against
        # the leaf entity type — so strip both from the start-level query
        # params when there are deeper levels. They re-enter at the innermost
        # ``$expand(...)`` clause below. Without this split, ``filter="Id eq
        # 3"`` (or ``select="Id,Label"``) on a ``Instances__Projects`` table
        # would land at Instances (wrong segment): a leaf-only column 400s
        # the server, and a name shared across levels silently projects the
        # TOP entity while the leaf comes back unprojected.
        top_opts = (
            {k: v for k, v in opts.items() if k not in ("filter", "select")}
            if has_children
            else dict(opts)
        )
        if per_level_tops is not None:
            # Override page_size in the opts dict ``_format_query_params``
            # reads from, so the start-level ``$top`` reflects the dynamic
            # allocation instead of the unscaled budget.
            top_opts = dict(top_opts)
            top_opts["page_size"] = str(per_level_tops[start_level])
        top_extra = combine_filters(
            cursor_filter if cursor_level == start_level else None,
            segment_filters.get(start_level),
        )
        query = self._format_query_params(
            top_opts,
            top_extra,
            self._expand_level_order_by(
                segments, start_level, namespace, cursor_level, cursor_order
            ),
        )
        if not has_children:
            return f"{base}?{query}"
        user_leaf_filter = opts.get("filter")
        user_leaf_select = opts.get("select")
        inner = ""
        for i in range(len(segments) - 1, start_level, -1):
            is_leaf = i == len(segments) - 1
            # ``per_level_tops`` is indexed by absolute segment level.
            parts: list[str] = []
            if per_level_tops is not None:
                parts.append(f"$top={per_level_tops[i]}")
            level_filter = combine_filters(
                cursor_filter if cursor_level == i else None,
                segment_filters.get(i),
                user_leaf_filter if is_leaf else None,
            )
            level_select = cursor_select if cursor_level == i and cursor_select else None
            if is_leaf and user_leaf_select:
                # Leaf-scoped user projection, merged (deduped, user order
                # first) with any cursor projection targeting the same level.
                merged = [c.strip() for c in user_leaf_select.split(",") if c.strip()]
                for col in (level_select or "").split(","):
                    if col.strip() and col.strip() not in merged:
                        merged.append(col.strip())
                level_select = ",".join(merged)
            if level_select:
                parts.append(f"$select={level_select}")
            if level_filter:
                parts.append(f"$filter={level_filter}")
            level_order = self._expand_level_order_by(
                segments, i, namespace, cursor_level, cursor_order
            )
            if level_order:
                parts.append(f"$orderby={level_order}")
            if inner:
                parts.append(f"$expand={inner}")
            # No options at all (no $top/filter/select/orderby/expand) ⇒
            # emit the bare nav-property name; ``Leaf()`` is not valid.
            inner = f"{segments[i]}({';'.join(parts)})" if parts else segments[i]
        return f"{base}?{query}&$expand={inner}"

    # --- read paths --------------------------------------------------------

    def _set_excluded_ancestor_columns(self, table_options: dict[str, str] | None) -> None:
        """Parse the ``exclude_ancestor_columns`` table option (a
        comma-separated list of FK column names) onto ``self`` for the
        duration of a schema/metadata/read call.

        Held on ``self`` — mirroring ``self._pagination`` — so the shared
        ``_resolve_fk_columns`` primitive (and everything that derives from
        it: schema, primary keys, row tagging) sees the exclusion without
        threading it through every internal call site. Reset on every
        entry point, so one table's exclusion can't leak into the next.
        """
        raw = (table_options or {}).get("exclude_ancestor_columns") or ""
        self._excluded_ancestor_columns = frozenset(c.strip() for c in raw.split(",") if c.strip())

    def _all_fk_column_names(self, segments: list[str], namespace: str | None) -> set[str]:
        """Full set of ancestor-FK column names for a contained path,
        BEFORE any ``exclude_ancestor_columns`` filtering — so callers can
        validate the option's names against what the path actually emits."""
        if len(segments) < 2:
            return set()
        self._resolve_fk_columns(segments, namespace)  # ensure cache populated
        full = self._metadata_state().fk_columns.get((tuple(segments), namespace)) or {}
        return set(full.values())

    def _resolve_fk_columns(
        self, segments: list[str], namespace: str | None
    ) -> dict[tuple[int, str], str]:
        """Map ``(level_index, pk_name) → unique FK column name`` for every
        non-leaf ancestor.

        OData v4 §13.4.3 makes contained-entity keys unique only within
        their immediate parent, so the destination composite key needs
        the full ancestor chain to be globally unique. Default name is
        ``<segment>_<pk>``; collisions get a leading ``_`` until unique.
        Empty mapping for flat tables.

        Keyed by the segment's **index**, not its name: a recursive
        containment path can repeat a nav-prop name at two non-leaf
        levels (``Folders__Children__Children__Files``), and a
        name-keyed map would collapse both levels into one entry —
        losing a composite-key component (silent MERGE collisions) and
        duplicating the surviving column in the schema.

        FK columns named in the ``exclude_ancestor_columns`` table option
        (parsed onto ``self._excluded_ancestor_columns`` at each entry
        point) are dropped from the returned mapping, so they vanish from
        the leaf schema, the composite primary key, and the stamped rows
        alike. A lone ``*`` drops every ancestor-FK column at once. The
        full mapping is cached untouched; the exclusion is a cheap
        post-filter so the same contained path can be read with different
        exclusions without poisoning the cache.
        """
        if len(segments) < 2:
            return {}
        state = self._metadata_state()
        cache_key = (tuple(segments), namespace)
        resolved = state.fk_columns.get(cache_key)
        if resolved is None:
            leaf_field_names = {
                f.name
                for f in self._own_fields_for_et(
                    self._entity_type_for(CONTAINED_PATH_SEP.join(segments), namespace)
                )
            }
            used = set(leaf_field_names)
            resolved = {}
            for idx in range(len(segments) - 1):
                ancestor_et = self._entity_type_for(
                    CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
                )
                seg = segments[idx]
                for pk in self._own_primary_keys_for_et(ancestor_et):
                    candidate = fk_column_name(seg, pk)
                    while candidate in used:
                        candidate = "_" + candidate
                    resolved[(idx, pk)] = candidate
                    used.add(candidate)
            state.fk_columns[cache_key] = resolved
        excluded = getattr(self, "_excluded_ancestor_columns", frozenset())
        if "*" in excluded:
            return {}
        if excluded:
            return {k: v for k, v in resolved.items() if v not in excluded}
        return resolved

    def _tag_with_ancestor_fks(
        self,
        row: dict,
        segments: list[str],
        chain: list[dict[str, Any]],
        fk_columns: dict[tuple[int, str], str],
    ) -> None:
        """Write every ancestor's primary-key values onto ``row`` under
        the resolved FK column names from ``fk_columns`` (keyed by level
        index, so repeated nav-prop names at different depths stay
        distinct columns)."""
        for idx, ancestor_keys in enumerate(chain):
            for pk_name, pk_val in ancestor_keys.items():
                col = fk_columns.get((idx, pk_name))
                if col is None:
                    continue
                if pk_val is None:
                    # A parent entity omitted or nulled its own primary key
                    # (non-conformant — OData keys are never null). Stamping
                    # ``None`` onto the non-nullable FK column would send a
                    # null MERGE key into ``apply_changes`` — the same silent
                    # null-key row the leaf-PK ``never_pad`` guard rejects
                    # loudly. Fail here rather than emit it (or build a
                    # malformed ``Parent('None')/child`` continuation URL).
                    raise ValueError(
                        f"Ancestor entity at path level {idx} is missing its key "
                        f"{pk_name!r} (got null); cannot form foreign-key column "
                        f"{col!r}. A parent omitted/nulled its primary key (a "
                        f"non-conformant response) — its child rows would carry a "
                        f"null MERGE key. Fix the source response, or exclude this "
                        f"path."
                    )
                row[col] = pk_val

    def _find_cursor_level(
        self,
        segments: list[str],
        namespace: str | None,
        cursor_field: str,
    ) -> int:
        """Return the segment index whose entity type declares
        ``cursor_field`` as a property. Walk leaf → root; the closest
        match wins. Returns ``-1`` if no segment has it."""
        for idx in range(len(segments) - 1, -1, -1):
            et = self._entity_type_for(CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace)
            if any(f.name == cursor_field for f in self._own_fields_for_et(et)):
                return idx
        return -1

    def _ancestor_cursor_field(
        self, table_name: str, namespace: str | None, cursor_field: str
    ) -> StructField | None:
        """``StructField`` for ``cursor_field`` when it lives on a non-leaf
        ancestor of a contained path; ``None`` when the leaf owns it or
        the path is flat. Used by ``get_table_schema`` to add the column
        to the leaf schema."""
        segments = self._table_segments(table_name) or [table_name]
        if len(segments) < 2:
            return None
        cursor_level = self._find_cursor_level(segments, namespace, cursor_field)
        if cursor_level in (-1, len(segments) - 1):
            return None
        ancestor_et = self._entity_type_for(
            CONTAINED_PATH_SEP.join(segments[: cursor_level + 1]), namespace
        )
        for field in self._own_fields_for_et(ancestor_et):
            if field.name == cursor_field:
                return field
        return None

    def _iter_parent_chains_with_cursor(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
        cursor_level: int,
        cursor_field: str,
        since: Any,
        top_parent_rows: list[dict] | None = None,
        tolerate_vanished: bool = False,
    ) -> Iterator[tuple[list[dict[str, Any]], Any]]:
        """Like ``_iter_parent_key_chains`` but applies a cursor filter at
        the ancestor that owns ``cursor_field``. Yields
        ``(chain, ancestor_cursor_value)`` pairs; the cursor value is the
        value at ``cursor_level`` for that chain.

        ``top_parent_rows`` lets a partitioned caller (PartitionMixin)
        supply a pre-discovered subset of level-0 rows instead of
        fetching the whole top-level set. Each row dict must carry the
        top-level entity's PKs (and, when ``cursor_level == 0``, the
        cursor value). The supplied subset is consumed in order without
        re-fetching.

        ``tolerate_vanished`` (partitioned callers): skip-with-warning a
        parent whose sub-level fetch 404/410s — see
        :meth:`_iter_parent_key_chains`."""
        segment_filters = resolve_segment_filters(table_options, segments)

        def _walk(level: int, chain: list[dict[str, Any]], cur_val: Any):
            if level == len(segments) - 1:
                yield list(chain), cur_val
                return
            sub_segments = segments[: level + 1]
            ancestor_et = self._entity_type_for(CONTAINED_PATH_SEP.join(sub_segments), namespace)
            ancestor_pks = self._own_primary_keys_for_et(ancestor_et)
            if not ancestor_pks:
                raise ValueError(
                    f"Cannot walk contained path: segment {segments[level]!r} "
                    f"has no primary key declared in $metadata."
                )
            row_source: Iterator[dict]
            if level == 0 and top_parent_rows is not None:
                # Skip the level-0 fetch; the partitioned caller has
                # already discovered + filtered + selected this subset.
                row_source = iter(top_parent_rows)
            else:
                select_cols = list(ancestor_pks)
                extra_filter: str | None = None
                # Default to PK-only ordering so server skiptoken
                # pagination is stable even at non-cursor levels —
                # OData v4 §11.2.5.7 doesn't promise stable default
                # ordering across pages without an explicit
                # ``$orderby``. The cursor level overrides this with a
                # cursor-first composite below.
                order_by: str | None = _ancestor_pk_order_by(ancestor_pks)
                if level == cursor_level:
                    if cursor_field not in select_cols:
                        select_cols.append(cursor_field)
                    extra_filter = self._cursor_filter(
                        cursor_field,
                        since,
                        edm_type=self._edm_types_for_et(ancestor_et).get(cursor_field),
                    )
                    terms = [f"{cursor_field} asc"]
                    terms.extend(f"{pk} asc" for pk in ancestor_pks if pk != cursor_field)
                    order_by = ",".join(terms)
                opts = {"select": ",".join(select_cols)}
                # Propagate the user's ``page_size`` only when set; with no
                # ``page_size`` no ``$top`` is sent (see
                # ``_format_query_params``).
                if (table_options or {}).get("page_size"):
                    opts["page_size"] = table_options["page_size"]
                if segment_filters.get(level):
                    opts["filter"] = segment_filters[level]
                url = (
                    self._build_url(segments[0], opts, extra_filter=extra_filter, order_by=order_by)
                    if level == 0
                    else self._build_contained_url(
                        sub_segments,
                        chain,
                        opts,
                        extra_filter=extra_filter,
                        order_by=order_by,
                    )
                )
                row_source = self._fetch_pages(url, self._edm_types_for_et(ancestor_et))
            for row in row_source:
                next_cur = row.get(cursor_field) if level == cursor_level else cur_val
                chain.append({pk: row.get(pk) for pk in ancestor_pks})
                try:
                    yield from _walk(level + 1, chain, next_cur)
                except requests.HTTPError as exc:
                    if not (tolerate_vanished and _is_vanished_error(exc)):
                        raise
                    _log_vanished_parent(chain, exc)
                finally:
                    chain.pop()

        yield from _walk(0, [], None)

    def _iter_parent_key_chains(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
        top_parent_rows: list[dict] | None = None,
        tolerate_vanished: bool = False,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield every ancestor key chain (len = len(segments) - 1) reaching
        the leaf. Each level fetched with ``$select=<pks>``; user ``filter``
        not forwarded — that string lands at the leaf URL only. To filter
        an ancestor walk use the corresponding ``filters_at`` entry.

        ``top_parent_rows`` lets a partitioned caller supply a pre-
        discovered subset of level-0 rows; when provided, the level-0
        HTTP fetch is skipped and the rows are consumed in order.

        ``tolerate_vanished``: a parent whose sub-level fetch 404/410s is
        skipped with a warning instead of failing the walk. ONLY the
        partitioned path sets this — its parent set is frozen into the
        task descriptor at planning, so a parent deleted mid-batch would
        otherwise fail every Spark task retry deterministically (the
        serial walks re-enumerate parents each trigger and self-heal
        without it)."""
        segment_filters = resolve_segment_filters(table_options, segments)

        def _walk(level: int, chain: list[dict[str, Any]]):
            if level == len(segments) - 1:
                yield list(chain)
                return
            sub_segments = segments[: level + 1]
            ancestor_et = self._entity_type_for(CONTAINED_PATH_SEP.join(sub_segments), namespace)
            ancestor_pks = self._own_primary_keys_for_et(ancestor_et)
            if not ancestor_pks:
                raise ValueError(
                    f"Cannot walk contained path: segment {segments[level]!r} "
                    f"has no primary key declared in $metadata."
                )
            row_source: Iterator[dict]
            if level == 0 and top_parent_rows is not None:
                row_source = iter(top_parent_rows)
            else:
                opts = {"select": ",".join(ancestor_pks)}
                # Propagate the user's ``page_size`` only when set; with no
                # ``page_size`` no ``$top`` is sent (see
                # ``_format_query_params``).
                if (table_options or {}).get("page_size"):
                    opts["page_size"] = table_options["page_size"]
                if segment_filters.get(level):
                    opts["filter"] = segment_filters[level]
                # PK-only ``$orderby`` so server skiptoken pagination
                # over the ancestor key set is stable across pages —
                # without this, sources whose default sort isn't PK
                # (or whose skiptoken doesn't encode the PK) can skip
                # or duplicate parents and silently lose every leaf
                # row under the skipped parent. See
                # ``_leaf_cursor_order_by`` for the leaf-side comment
                # documenting the same skiptoken concern one level
                # deeper.
                order_by = _ancestor_pk_order_by(ancestor_pks)
                url = (
                    self._build_url(segments[0], opts, order_by=order_by)
                    if level == 0
                    else self._build_contained_url(sub_segments, chain, opts, order_by=order_by)
                )
                row_source = self._fetch_pages(url, self._edm_types_for_et(ancestor_et))
            for row in row_source:
                chain.append({pk: row.get(pk) for pk in ancestor_pks})
                try:
                    yield from _walk(level + 1, chain)
                except requests.HTTPError as exc:
                    if not (tolerate_vanished and _is_vanished_error(exc)):
                        raise
                    _log_vanished_parent(chain, exc)
                finally:
                    chain.pop()

        yield from _walk(0, [])

    def _build_probe_url(
        self,
        segments: list[str],
        parent_chain: list[dict[str, Any]],
        table_options: dict[str, str],
        cursor_field: str,
    ) -> str:
        """Shallow change-probe over one leaf-parent collection.

        ``parent_chain`` (len = ``len(segments) - 2``) addresses the
        leaf-parent *collection* under its grandparent tuple; the URL asks
        only for the leaf-parent PKs plus the **single newest leaf by
        cursor**::

            A(a)/B(b)/C?$top=<page>&$select=<Cpk>&$orderby=<Cpk> asc
                       &$expand=D($orderby=<cursor> desc;$top=1;$select=<cursor>)

        The caller (:meth:`_iter_dirty_leaf_parent_chains`) marks a leaf-parent
        dirty when that newest leaf's cursor is ``> since`` — the change test
        is done **client-side**, with no inner ``$filter`` at all. Ordering the
        inner ``$expand`` by the cursor descending makes the one returned row
        the MAX-cursor leaf *by construction*, so a server that applies
        ``$top`` before anything else still returns the right row. That is the
        whole point: an earlier ``$top=1;$filter`` shape let servers slice the
        first expanded row *before* applying the inner ``$filter``, so a
        leaf-parent whose changed leaf wasn't first by default order was
        wrongly reported clean and its leaves silently dropped. Comparing the
        max cursor client-side removes that trap.

        NB: relies on the server honouring ``$orderby``/``$top`` *inside*
        ``$expand`` (basic expand options). A server that ignores ``$orderby``
        could return a non-newest row and under-report — that residual
        server-dependence is why ``cursor_probe`` is opt-in (default off):
        enable it only where the source is known to honour inner-``$expand``
        options. A leaf-segment ``filters_at`` entry is deliberately NOT
        applied in the probe (it has no inner ``$filter``); at worst that
        over-fetches a parent whose recent changes the filter excludes — the
        hydrate then emits nothing, never a miss.
        """
        namespace = (table_options or {}).get("namespace")
        parent_segments = segments[:-1]
        leaf_nav = segments[-1]
        segment_filters = resolve_segment_filters(table_options, segments)
        lp_pks = self._own_primary_keys_for_et(
            self._entity_type_for(CONTAINED_PATH_SEP.join(parent_segments), namespace)
        )
        inner = [f"$orderby={cursor_field} desc", "$top=1", f"$select={cursor_field}"]
        outer = []
        if (table_options or {}).get("page_size"):
            outer.append(f"$top={table_options['page_size']}")
        outer.append(f"$select={','.join(lp_pks)}")
        lp_filter = segment_filters.get(len(parent_segments) - 1)
        if lp_filter:
            outer.append(f"$filter={lp_filter}")
        order_by = _ancestor_pk_order_by(lp_pks)
        if order_by:
            outer.append(f"$orderby={order_by}")
        outer.append(f"$expand={leaf_nav}({';'.join(inner)})")
        base = join_url(
            self.service_url,
            self._build_contained_path(parent_segments, parent_chain, namespace),
        )
        return f"{base}?{'&'.join(outer)}"

    def _iter_dirty_leaf_parent_chains(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
        cursor_field: str,
        since: Any,
    ) -> Iterator[list[dict[str, Any]]]:
        """``cursor_probe`` chain source: yield only the full key chains
        (len = ``len(segments) - 1``) whose leaf collection has ≥1 changed
        row since ``since``.

        A drop-in for :meth:`_iter_parent_key_chains` in the leaf-cursor
        read: it enumerates leaf-grandparent tuples the same way, then runs
        one :meth:`_build_probe_url` per tuple and emits the leaf-parent key
        (extending the chain) only for parents the probe flags dirty. Like
        the plain enumerator it is consumed lazily and — LOAD-BEARING — it
        yields chains in **PK order at every level**: grandparents via
        ``_iter_parent_key_chains``' PK ``$orderby``, leaf-parents via the
        probe URL's own outer ``$orderby=<pks> asc``
        (:meth:`_build_probe_url`). The capped walk's key-based resume
        (``parent_keys``, :func:`_chain_strictly_before`) skips
        "already-walked" chains by exactly that ordering; dropping either
        ``$orderby`` would silently skip dirty parents that enumerate after
        a park. A resumed batch re-probes the skipped parents (cheap; no
        leaf fetches) exactly as the plain walk re-pages skipped ancestors.

        ``since`` is ``None`` on the first batch → every leaf-parent with any
        leaf reads as dirty, so the first incremental batch behaves like the
        standard full walk (correct, no speed-up until a watermark exists)."""
        parent_segments = segments[:-1]
        leaf_nav = segments[-1]
        lp_et = self._entity_type_for(CONTAINED_PATH_SEP.join(parent_segments), namespace)
        lp_pks = self._own_primary_keys_for_et(lp_et)
        lp_types = self._edm_types_for_et(lp_et)
        for pchain in self._iter_parent_key_chains(parent_segments, namespace, table_options):
            url = self._build_probe_url(segments, pchain, table_options, cursor_field)
            for row in self._fetch_pages(url, lp_types):
                # The probe returns the newest leaf (``$orderby cursor desc;
                # $top=1``). Max over the returned rows so we're still correct
                # if a server ignores ``$top`` and hands back several. Dirty
                # when that max cursor exceeds the watermark (or on the first
                # batch, ``since is None``, whenever the leaf-parent has a
                # leaf at all) — this matches the hydrate's ``cursor gt since``.
                # Chronological comparisons (``_cursor_newer``): a lexical
                # ``>`` against a value-dependently-fractional rendering
                # (``…00.5Z`` vs watermark ``…00Z``) would mark a genuinely
                # dirty leaf-parent CLEAN and skip its changed leaves.
                max_cursor = None
                for child in row.get(leaf_nav) or []:
                    val = child.get(cursor_field)
                    if val is not None and (max_cursor is None or _cursor_newer(val, max_cursor)):
                        max_cursor = val
                if max_cursor is not None and (since is None or _cursor_newer(max_cursor, since)):
                    yield pchain + [{pk: row.get(pk) for pk in lp_pks}]

    def _verify_cursor_probe_support(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
        cursor_field: str,
        start_offset: dict | None = None,
        strict: bool = True,
    ) -> tuple[bool, bool]:
        """Behavioural capability check for the nested-``$expand`` probe.

        Returns ``(supported, conclusive)``. ``supported`` is whether the
        server can be trusted to run the probe correctly; ``conclusive`` is
        whether the verdict is a *conclusive* pass the caller may persist.
        When ``strict`` (``cursor_probe=nested-expand``) and the server mis-orders inner
        ``$expand``, **raises** with the actionable message. When not strict
        (``auto`` cascade), a mis-ordering server returns ``(False, False)`` so
        the caller can fall back to ``$batch`` / the plain walk instead.

        ``cursor_probe`` (default on) silently drops rows on a server that
        mishandles ``$orderby``/``$top`` inside ``$expand``. This behavioural
        preflight catches that *before* any data is read and turns it into a
        clear error.

        Result is cached per ``(path, namespace)`` so the check runs once per
        connector instance. But the Spark Python Data Source recreates the
        reader per batch, so that instance cache is cold every batch — the
        preflight's handful of GETs would recur indefinitely. To avoid that, a
        *conclusive* pass is also persisted in the resume offset as
        ``cursor_probe_ok``; when a prior batch's offset carries it, the
        preflight requests are skipped entirely. Only a conclusive pass is
        trusted this way. Under the non-strict ``auto`` cascade BOTH
        definitive outcomes additionally ride the process/file capability
        cache (per contained path) — the offset never carries a fail, so
        without it a mis-ordering server would re-pay the preflight GETs on
        every framework-recreated reader. Strict mode neither consults nor
        records the shared cache (an explicit mode keeps no recorded
        verdicts, and its error must carry fresh evidence). An *inconclusive*
        result — no leaf-parent yet has ``>= 2`` distinct leaf cursors, so
        ordering can't cause a miss — is re-checked every batch, so a server
        that begins to mis-order once its data grows discriminating is still
        caught; a race-contaminated sample (see
        :meth:`_cursor_probe_check_sample`) likewise records nothing.

        ``(supported, conclusive)``: ``supported`` is ``True`` via the persisted
        offset flag or any non-mis-ordering preflight verdict; ``conclusive`` is
        ``True`` only on a conclusive pass the caller may persist as
        ``cursor_probe_ok`` (an *inconclusive* scan is re-checked every batch).
        Raises (``strict``) or returns ``(False, False)`` (non-strict) on a
        mis-ordering server. A preflight that errors out before reaching a
        verdict (transport/HTTP failure on the enumeration or trusted-
        reference fetch — indistinguishable from a transient) likewise
        degrades a non-strict read to the ``$batch``/plain cascade for this
        batch while caching and recording nothing; strict raises an
        actionable error instead of the raw HTTP failure."""
        if (start_offset or {}).get("cursor_probe_ok"):
            return (True, True)
        cache = self.__dict__.setdefault("_cursor_probe_verified", {})
        cache_key = (tuple(segments), namespace)
        if cache_key not in cache:
            # Process/file capability cache — ``auto`` cascade only. The
            # strict mode (``cursor_probe=nested-expand``) is an explicit
            # non-``auto`` selection: it neither consults nor records the
            # shared verdict (same rule as the offset scrub) and re-probes
            # so its error carries fresh, actionable evidence. Both
            # definitive outcomes are shared: a conclusive pass AND a
            # mis-ordering fail (otherwise ``auto`` against a mis-ordering
            # server would re-pay the preflight GETs on every framework-
            # recreated reader — the offset only ever carries the pass). A
            # shared hit fills the per-instance cache (like the other
            # verifiers) so repeat calls this instance skip the file read.
            shared_key = self._cursor_probe_shared_key(segments, namespace)
            shared = (
                None
                if strict
                else self._cached_capability("cursor_probe_ok", table_name=shared_key)
            )
            if shared is True:
                cache[cache_key] = (None, True, False)
            elif shared is False:
                cache[cache_key] = (_CURSOR_PROBE_SHARED_FAIL, False, False)
            else:
                try:
                    cache[cache_key] = self._run_cursor_probe_preflight(
                        segments, namespace, table_options, cursor_field
                    )
                except _CursorProbePreflightUnavailable as exc:
                    # A preflight fetch failed before reaching a verdict —
                    # the enumeration, the trusted-reference fetch, or (since
                    # round 41) a TRANSIENT failure of the probe-shaped fetch
                    # itself (only an immediate non-retryable HTTPError there
                    # is a definitive shape rejection; retry-exhausted
                    # throttling and network exhaustion land here — a
                    # programming error in the preflight's own logic still
                    # propagates). There is no evidence to distinguish a
                    # capability shortfall from a transient blip — so treat
                    # it like the other verifiers treat transients: degrade
                    # THIS read to the $batch/plain cascade, cache and record
                    # NOTHING (the next batch re-probes), and never let the
                    # raw HTTP error escape a ``cursor_probe=auto`` read.
                    msg = (
                        f"cursor_probe preflight against "
                        f"{CONTAINED_PATH_SEP.join(segments)!r} failed before reaching "
                        f"a verdict: {exc}. If this persists (the server rejects "
                        f"$orderby/$select on direct navigation to the leaf "
                        f"collection), use cursor_probe=batch or cursor_probe=false."
                    )
                    if strict:
                        raise ValueError(msg) from exc
                    _LOG.warning("%s Falling back to $batch / the plain N+1 walk.", msg)
                    return (False, False)
                problem, conclusive, _race = cache[cache_key]
                if not strict:
                    if problem:  # clean mis-ordering evidence — a definitive fail
                        self._store_capability("cursor_probe_ok", False, table_name=shared_key)
                    elif conclusive:
                        self._store_capability("cursor_probe_ok", True, table_name=shared_key)
                    # Inconclusive scans (no discriminating sample, or only
                    # concurrent-write races) record nothing and re-check next
                    # batch, so a server that starts mis-ordering once its data
                    # grows discriminating is still caught.
        problem, conclusive, race_tainted = cache[cache_key]
        if problem:
            if strict:
                raise ValueError(problem)
            return (False, False)
        if race_tainted and not conclusive:
            # The verdict-less scan contained a RACE skip — a sample that HAD
            # discriminating cursors but returned newer-than-reference. Unlike
            # a genuinely non-discriminating scan (where engaging the probe
            # unverified is safe — ordering can't cause a miss), this may be a
            # mis-ordering server hiding behind concurrent writes. Decline the
            # probe for this batch (the caller cascades to $batch / the plain
            # walk — rows stay correct; under strict mode this degrades the
            # REQUEST SHAPE for one batch rather than raising on a transient)
            # and record nothing; the next batch re-checks.
            _LOG.warning(
                "cursor_probe preflight for %r was race-contaminated (concurrent "
                "writes during every discriminating sample); declining the probe "
                "for this batch and reading via $batch / the plain walk. The next "
                "batch re-checks.",
                CONTAINED_PATH_SEP.join(segments),
            )
            return (False, False)
        return (True, conclusive)

    @staticmethod
    def _cursor_probe_shared_key(segments: list[str], namespace: str | None) -> str:
        """Per-path key for the shared ``cursor_probe_ok`` verdict — the
        contained path (the probe shape depends on it), namespace-qualified
        for multi-schema services."""
        path = CONTAINED_PATH_SEP.join(segments)
        return f"{namespace}:{path}" if namespace else path

    @staticmethod
    def _expand_shared_key(table_name: str, table_options: dict | None) -> str:
        """Memo/shared-cache key for the per-table ``expand_ok`` verdict —
        namespace-qualified like :meth:`_cursor_probe_shared_key`. The same
        contained path string (``Customers__Addresses``) can resolve to
        differently-shaped types in two namespaces of one service, so a
        bare-table key would share one verdict across both — the exact
        unverified-``$expand`` leak the per-table keying exists to
        prevent, one level up."""
        namespace = (table_options or {}).get("namespace")
        return f"{namespace}:{table_name}" if namespace else table_name

    def _run_cursor_probe_preflight(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
        cursor_field: str,
    ) -> tuple[str | None, bool]:
        """Behavioural capability check for :meth:`_iter_dirty_leaf_parent_chains`.

        Returns ``(problem, conclusive, race_tainted)``: ``problem`` is an
        actionable error message on clean mis-ordering evidence (inner leaf
        OLDER than / missing from the trusted reference — the direction a
        genuinely mis-ordering server produces), else ``None``. A ``problem``
        is always a *definitive* fail the caller may persist as
        ``cursor_probe_ok=false`` (and, in strict mode, raise on).
        ``conclusive`` is ``True`` only when a discriminating sample was found
        AND the probe shape returned the true newest leaf — the verdict the
        caller may persist as ``cursor_probe_ok=true``; ``False`` on an
        inconclusive scan (``problem`` ``None``), which must be re-checked
        rather than trusted. ``race_tainted`` is ``True`` when an inconclusive
        scan contained at least one RACE skip (a sample that HAD
        discriminating cursors but returned newer-than-reference): unlike a
        genuinely non-discriminating scan — where ordering can't cause a
        miss, so engaging the probe unverified is safe — a race-tainted scan
        may be hiding a mis-ordering server behind concurrent writes, so the
        caller must decline the probe for this batch (cascade to ``$batch`` /
        the plain walk) while still recording nothing.

        Finds a sample leaf-parent with ≥2 distinct leaf cursors and verifies
        that the probe's own ``$expand($orderby cursor desc;$top=1)`` returns
        the true newest leaf — cross-checked against a trusted direct-navigation
        ``$orderby`` query (basic collection ordering, far more universally
        honoured than inner-``$expand`` ordering). A sample that can't
        discriminate (≤1 distinct leaf cursor) or that races a concurrent write
        (inner leaf NEWER than the reference — see
        :meth:`_cursor_probe_check_sample`) is skipped and the scan moves on."""
        parent_segments = segments[:-1]
        leaf_nav = segments[-1]
        lp_et = self._entity_type_for(CONTAINED_PATH_SEP.join(parent_segments), namespace)
        lp_pks = self._own_primary_keys_for_et(lp_et)
        if not lp_pks:
            # Inconclusive, not race-tainted — the 3-tuple contract every
            # consumer unpacks. The walk itself then raises the actionable
            # "segment X has no primary key declared in $metadata" error.
            return (None, False, False)
        lp_types = self._edm_types_for_et(lp_et)
        page_size = (table_options or {}).get("page_size") or DEFAULT_PAGE_SIZE
        lp_order = _ancestor_pk_order_by(lp_pks)
        scanned = 0
        saw_race = False
        for pchain in self._iter_parent_key_chains(parent_segments, namespace, table_options):
            lp_base = join_url(
                self.service_url, self._build_contained_path(parent_segments, pchain, namespace)
            )
            next_url: str | None = f"{lp_base}?$select={','.join(lp_pks)}&$top={page_size}"
            if lp_order:
                next_url += f"&$orderby={lp_order}"
            while next_url:
                try:
                    lp_rows, next_url = self._fetch_one_expand_page(next_url, lp_types)
                except Exception as exc:  # enumeration fetch failed — no verdict
                    raise _CursorProbePreflightUnavailable(str(exc)) from exc
                for lp_row in lp_rows:
                    scanned += 1
                    lp_key = {pk: lp_row.get(pk) for pk in lp_pks}
                    status, message = self._cursor_probe_check_sample(
                        parent_segments, pchain, segments, namespace, lp_key, leaf_nav, cursor_field
                    )
                    if status == "ok":
                        return (None, True, False)
                    if status == "error":
                        return (message, False, False)
                    if status == "race":
                        saw_race = True
                    if scanned >= _CURSOR_PROBE_PREFLIGHT_SCAN:
                        return (None, False, saw_race)
        return (None, False, saw_race)

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _cursor_probe_check_sample(
        self,
        parent_segments: list[str],
        pchain: list[dict[str, Any]],
        segments: list[str],
        namespace: str | None,
        lp_key: dict[str, Any],
        leaf_nav: str,
        cursor_field: str,
    ) -> tuple[str, str | None]:
        """Verify the probe shape against trusted ordering for one leaf-parent.

        Returns ``("skip", None)`` when the sample can't be trusted as
        evidence — either it can't discriminate (< 2 distinct leaf cursors)
        or the probe returned a leaf NEWER than the reference (a concurrent
        write between the two non-atomic fetches, not ordering evidence) — so
        the scan should move on to another sample; ``("ok", None)`` when the
        probe's inner ``$expand`` ordering returns the true newest leaf; or
        ``("error", msg)`` when it returns an OLDER/missing leaf (the
        direction a genuinely mis-ordering server produces — clean
        evidence)."""
        full_chain = pchain + [lp_key]
        leaf_base = join_url(
            self.service_url, self._build_contained_path(segments, full_chain, namespace)
        )
        # Trusted reference: direct-navigation ordering on the leaf collection.
        try:
            direct_rows, _ = self._fetch_one_expand_page(
                f"{leaf_base}?$orderby={cursor_field} desc&$top=2&$select={cursor_field}"
            )
        except Exception as exc:  # reference fetch failed — no verdict either way
            raise _CursorProbePreflightUnavailable(str(exc)) from exc
        vals = [r.get(cursor_field) for r in direct_rows if r.get(cursor_field) is not None]
        if len(vals) < 2 or vals[0] == vals[1]:
            return ("skip", None)
        direct_max = vals[0]
        # The probe's own shape, targeted to this leaf-parent via an outer
        # key $filter (basic collection filtering, not an inner-$expand option).
        lp_coll = join_url(
            self.service_url, self._build_contained_path(parent_segments, pchain, namespace)
        )
        lp_et = self._entity_type_for(CONTAINED_PATH_SEP.join(parent_segments), namespace)
        lp_types = self._edm_types_for_et(lp_et)
        pk_filter = " and ".join(
            f"{k} eq {odata_literal_typed(v, lp_types.get(k))}" for k, v in lp_key.items()
        )
        lp_pks = self._own_primary_keys_for_et(lp_et)
        expand_url = (
            f"{lp_coll}?$select={','.join(lp_pks)}&$filter={pk_filter}"
            f"&$expand={leaf_nav}($orderby={cursor_field} desc;$top=1;$select={cursor_field})"
        )
        try:
            exp_rows, _ = self._fetch_one_expand_page(expand_url)
        except requests.HTTPError:  # server REJECTED the nested-$expand probe shape
            # e.g. Hexagon Smart API 400s on inner $orderby/$top/$select rather
            # than accepting it (or silently mis-ordering). An immediate
            # HTTPError is a NON-RETRYABLE 4xx (the retryable statuses exhaust
            # the retry budget into RuntimeError, and network failures re-raise
            # their own types — neither reaches this clause), so this is a
            # definitive capability rejection. Report it like the mis-order
            # case ("error") so ``auto`` cascades to $batch / the plain walk
            # (persisting cursor_probe_ok=False) and ``nested-expand`` raises
            # an actionable error — instead of the raw HTTP error escaping and
            # failing the read, which would break the "auto never raises on a
            # capability shortfall" contract.
            probe_path = self._build_contained_path(segments, full_chain, namespace)
            return (
                "error",
                "cursor_probe=nested-expand needs the source to accept "
                "$orderby/$top/$select inside $expand, but "
                f"{probe_path!r} rejected the probe "
                "query with an error (the server does not support these inner-$expand "
                "options). Use cursor_probe=batch or cursor_probe=auto (which falls back "
                "to $batch / the plain N+1 walk), or cursor_probe=false for the plain walk.",
            )
        except Exception as exc:  # noqa: BLE001 — transient, NOT a capability verdict
            # Retry-exhausted throttling (RuntimeError after the 429/503
            # budget), network exhaustion (ConnectionError/Timeout), auth —
            # a throttle window can open BETWEEN the sibling fetches, so
            # "the reference fetch just succeeded" is not evidence of a
            # capability rejection. Treating these as definitive pinned a
            # false cursor_probe_ok=False for the cache TTL under ``auto``
            # and raised a misleading capability error under strict mode.
            # Route to the same no-verdict path as the enumeration and
            # reference fetch sites: degrade this batch, record nothing,
            # re-probe next batch.
            raise _CursorProbePreflightUnavailable(str(exc)) from exc
        children = (exp_rows[0].get(leaf_nav) if exp_rows else None) or []
        # Chronological max (``_cursor_max``, not ``max``): a lexical max over
        # mixed fractional renderings can pick the wrong CHILD's value,
        # failing the equality below and fabricating a definitive mis-order
        # verdict against an honest server.
        inner_vals = [c.get(cursor_field) for c in children if c.get(cursor_field) is not None]
        inner_max = _cursor_max(inner_vals) if inner_vals else None
        # SAME-INSTANT equality, not raw text: the two fetches can hit
        # different LB backends rendering one instant differently
        # (…00Z vs …00.000Z). Raw equality would fall through, and the
        # direction whose raw tie-break reads as "older" would land in the
        # error branch below — fabricating definitive mis-ordering evidence
        # against an honest server (false cursor_probe_ok=false under
        # ``auto``; a spurious raise under strict ``nested-expand``). Same
        # hazard class the ``_cursor_max`` comment above guards against.
        if _cursor_same_instant(inner_max, direct_max):
            return ("ok", None)
        # Direction matters, because a fail verdict now outlives the instance
        # (shared capability cache) and can raise in strict mode. A newest-leaf
        # NEWER than the trusted reference is NOT ordering evidence: the two
        # fetches aren't atomic, so a leaf inserted between them makes an honest
        # server look mismatched. A genuinely mis-ordering server returns an
        # OLDER leaf, never a newer one — so the newer direction is skipped like
        # a non-discriminating sample (keep scanning for a clean one) rather
        # than treated as a failure. This keeps one concurrent write from
        # aborting the whole preflight or spuriously raising strict mode.
        try:
            if inner_max is not None and _cursor_newer(inner_max, direct_max):
                # Distinct status from the non-discriminating "skip": a race
                # skip means this sample HAD discriminating cursors — an
                # all-race scan must decline the probe for this batch rather
                # than engage it unverified (see _run_cursor_probe_preflight).
                return ("race", None)
        except TypeError:
            pass  # incomparable cursor values — keep the mismatch as evidence
        return (
            "error",
            "cursor_probe=nested-expand requires the source to honour $orderby/$top "
            f"inside $expand, but {self._build_contained_path(segments, full_chain, namespace)!r} "
            f"returned {inner_max!r} as its newest {leaf_nav} via $expand when the "
            f"true newest is {direct_max!r} (direct navigation). This server "
            "silently mis-orders inner $expand, so cursor_probe would drop changed "
            "rows. Use cursor_probe=batch or cursor_probe=auto (which falls back to "
            "$batch / the plain N+1 walk), or cursor_probe=false for the plain walk.",
        )

    def _with_probe_ok(self, offset: dict) -> dict:
        """Return ``offset`` carrying the persisted ``cursor_probe_ok`` flag.

        Records that this server's inner-``$expand`` ordering has been verified
        so a per-batch-recreated reader can skip the capability preflight next
        batch (see :meth:`_verify_cursor_probe_support`). Never mutates the
        input — the framework may retain the prior offset object — and is a
        no-op (returns the same object) when the flag is already present, so an
        idled overlap re-read that returns ``start_offset`` keeps its identity.
        The flag carries no cursor progress; :meth:`_finalize_cursor_read`
        excludes it from the no-progress comparison."""
        if offset.get("cursor_probe_ok"):
            return offset
        return {**offset, "cursor_probe_ok": True}

    def _with_batch_ok(self, offset: dict) -> dict:
        """Return ``offset`` carrying the persisted ``batch_ok`` flag — the
        ``$batch`` analogue of :meth:`_with_probe_ok`. Records that the server
        accepts OData ``$batch`` so a per-batch-recreated reader skips the
        capability POST next batch. Never mutates the input; no-op when already
        present. The flag carries no cursor progress (excluded from the
        no-progress comparison in :meth:`_finalize_cursor_read`)."""
        if offset.get("batch_ok"):
            return offset
        return {**offset, "batch_ok": True}

    def _batch_relative(self, url: str) -> str:
        """Make ``url`` service-root-relative for a JSON ``$batch`` sub-request.

        The OData v4 JSON batch format resolves a sub-request ``url`` against the
        service root, so an absolute URL under the root is stripped to its
        remainder; an already-relative URL (e.g. a resolved ``@odata.nextLink``
        that came back service-relative) is returned without a leading slash.

        The result is percent-encoded via ``requote_uri`` — the same encoding
        ``requests`` applies to a plain GET's URL — because a sub-request URL
        rides inside the JSON envelope and never passes through ``requests``'
        URL preparation: without this, generated ``$orderby=Id asc`` /
        ``$filter=… gt …`` shapes carry literal spaces, which a strict OData
        v4 server may reject. ``requote_uri`` preserves existing escapes, so
        the literal-level encoding from ``odata_literal`` is not doubled."""
        root = self.service_url if self.service_url.endswith("/") else self.service_url + "/"
        if url.startswith(root):
            return requote_uri(url[len(root) :])
        parsed = urlparse(url)
        if parsed.scheme:
            query = f"?{parsed.query}" if parsed.query else ""
            # Same origin under normalization (default port spelled out,
            # host-case difference) but a raw prefix miss: strip the service
            # root's PATH so the sub-request stays service-relative. Without
            # this the fallback below keeps the root's own path segments and
            # the sub-request 404s — self-healing via the plain-GET re-issue
            # in ``_checked_batch_subresponse``, but one wasted round-trip
            # plus an alarming warning per continuation.
            try:
                same_origin = _url_origin(url) == _url_origin(root)
            except ValueError:
                same_origin = False
            root_path = urlparse(root).path
            if same_origin and parsed.path.startswith(root_path):
                return requote_uri(parsed.path[len(root_path) :] + query)
            return requote_uri(parsed.path.lstrip("/") + query)
        return requote_uri(url.lstrip("/"))

    def _post_batch(self, urls: list[str]) -> list[dict]:
        """POST one OData v4 JSON ``$batch`` of GET sub-requests; return the
        per-sub-request response objects in the SAME order as ``urls``.

        Routes through :meth:`_http_get` with ``method="POST"`` so the batch
        shares the connector's throttle / transient / token-refresh retry path.
        Caller must keep ``len(urls) <= _BATCH_MAX_OPS``. Raises if the batch
        envelope itself fails (non-2xx, malformed JSON, or a missing sub-response
        id); per-sub-request HTTP errors are carried inside the envelope for the
        caller to inspect."""
        session = self._get_session()
        payload = {
            "requests": [
                {"id": str(i), "method": "GET", "url": self._batch_relative(u)}
                for i, u in enumerate(urls)
            ]
        }
        batch_url = join_url(self.service_url, "$batch")

        def _raise_for_batch_status(resp):
            if resp.status_code < 400:
                return
            body = resp.text or ""
            if _is_batch_too_large(body):
                raise _BatchTooManyParts(
                    f"OData $batch POST to {batch_url!r} rejected "
                    f"{len(urls)} parts: {resp.status_code} {body[:300]}"
                )
            raise RuntimeError(
                f"OData $batch POST to {batch_url!r} failed: " f"{resp.status_code} {body[:300]}"
            )

        resp = self._http_get(session, batch_url, method="POST", json=payload)
        _raise_for_batch_status(resp)
        try:
            data = resp.json()
        except ValueError:
            # Corrupt 200: same failure mode ``_fetch_page_payload`` retries
            # for — some servers (Hexagon) truncate LARGE bodies under load,
            # and the $batch envelope is the largest response the connector
            # ever receives. One re-POST is safe (GET-only sub-requests).
            resp = self._http_get(session, batch_url, method="POST", json=payload)
            # The retry can come back non-2xx (the corrupt 200 was a blip, the
            # real answer is an error): route it through the SAME status
            # handling — a "too many parts" 400 must still trigger the
            # adaptive shrink, and a plain 4xx must carry its status/body
            # instead of decoding as a responses-less envelope ("missing
            # sub-response id 0").
            _raise_for_batch_status(resp)
            try:
                data = resp.json()
            except ValueError:
                raise RuntimeError(
                    f"OData $batch response from {batch_url!r} returned "
                    f"{resp.status_code} with a malformed JSON body twice "
                    f"(likely truncated by the server). First 300 chars: "
                    f"{(resp.text or '')[:300]!r}"
                ) from None
        by_id = {str(r.get("id")): r for r in data.get("responses", [])}
        out = []
        for i in range(len(urls)):
            if str(i) not in by_id:
                raise RuntimeError(
                    f"OData $batch response from {batch_url!r} is missing "
                    f"sub-response id {i!r}; got ids {sorted(by_id)}."
                )
            out.append(by_id[str(i)])
        return out

    def _effective_batch_size(self, batch_size: int) -> int:
        """The chunk size to slice the next ``$batch`` round at: the requested
        ``batch_size`` clamped to the discovered working cap (``_batch_size_cap``,
        set once a "too many parts" rejection forced a shrink and seeded from the
        offset's ``batch_size_ok``). No cap discovered, or the give-up sentinel
        ``cap == 1`` (``$batch`` abandoned for plain GETs, handled in
        :meth:`_post_batch_adaptive`) → the requested size."""
        cap = self.__dict__.get("_batch_size_cap")
        return min(batch_size, cap) if cap and cap > 1 else batch_size

    def _shrink_batch_cap(self, attempted: int) -> bool:
        """Reduce the working ``$batch`` cap after a "too many parts" rejection of
        ``attempted`` parts: ``_BATCH_SHRINK_FACTOR`` × the current cap (or the
        attempted count if no cap yet), floored at 1. Returns ``False`` once the
        per-instance shrink budget (:data:`_BATCH_OVERFLOW_RETRIES`) is spent — the
        caller then falls back to a plain per-leaf-parent GET. Records the new cap
        so it is persisted in the offset (``batch_size_ok``) and reused."""
        shrinks = self.__dict__.get("_batch_shrinks", 0)
        if shrinks >= _BATCH_OVERFLOW_RETRIES:
            return False
        cap = self.__dict__.get("_batch_size_cap") or attempted
        new_cap = max(1, int(cap * _BATCH_SHRINK_FACTOR))
        if new_cap >= cap:  # ensure forward progress when the factor rounds up
            new_cap = max(1, cap - 1)
        self.__dict__["_batch_size_cap"] = new_cap
        self._store_capability("batch_size_ok", new_cap)
        self.__dict__["_batch_shrinks"] = shrinks + 1
        _LOG.warning(
            "OData $batch rejected %d parts (too many); reducing batch size to "
            "%d and retrying (shrink %d/%d).",
            attempted,
            new_cap,
            shrinks + 1,
            _BATCH_OVERFLOW_RETRIES,
        )
        return True

    def _get_as_batch_response(self, url: str, edm_types: dict[str, str] | None = None) -> dict:
        """Plain GET fall-back for one leaf-parent, shaped like a ``$batch``
        sub-response (``{"status", "body": {"value": [...]}}``) so the drain loops
        parse it identically. All pages are drained here (no ``@odata.nextLink``
        returned), so the loop emits every row without re-batching.

        The ``$batch``-shaped URL carries no ``$top`` (the server was meant to
        drive paging inside the batch). Outside ``$batch`` that starves the
        client-driven drain: with no ``$top``, ``_client_paginate_pages`` takes
        a single link-less page as the whole collection, so a server that
        page-limits while omitting ``@odata.nextLink`` would be silently
        truncated. Re-add the default ``$top`` under keyset/skip/auto so the
        drain can size its pages and seek until empty (nextlink mode is left
        untouched — it trusts the server's links either way).

        Server-issued continuation links are exempt from the ``$top``
        injection: a re-queued ``@odata.nextLink`` (recognisable by its
        ``$skiptoken``/``$skip``) can land here when the ``$batch`` give-up
        sentinel fires mid-walk, and OData v4 §11.2.5.7 requires the client
        to use the nextLink as-is — appending an option to an opaque
        skiptoken URL can 400 or corrupt the server's paging state. A
        continuation also proves the server emits links, so the
        starvation this injection defends against can't occur on it."""
        if (
            getattr(self, "_pagination", "nextlink") != "nextlink"
            and _pg_parse_top(url) is None
            and not _pg_is_continuation(url)
        ):
            url = _pg_set_query(url, "$top", DEFAULT_PAGE_SIZE)
        rows = list(self._fetch_pages(url, edm_types))
        return {"status": 200, "body": {"value": rows}}

    def _checked_batch_subresponse(
        self, resp: dict, req_url: str, edm_types: dict[str, str] | None = None
    ) -> dict:
        """Validate one ``$batch`` sub-response before the drain loops parse it.

        :meth:`_post_batch` deliberately carries per-sub-request HTTP errors
        inside the envelope for the caller to inspect — and this is that
        inspection. Without it a 2xx envelope holding one failed sub-response
        (a throttled or errored leaf-parent) parses as ``rows = []`` and that
        parent's whole collection is silently skipped; on the cursor walk the
        other parents still advance the watermark past the failed parent's
        changed rows, so ``cursor gt since`` never re-reads them — permanent
        loss.

        A sub-response with a < 400 status AND a dict ``body`` passes through
        untouched. Anything else — an error status, a shape that isn't a
        sub-response dict, OR a 2xx whose ``body`` is a string/absent (the
        JSON-batch spec lets a server serialize a sub-response body as a
        JSON *string* for non-JSON media; the drains take ``rows = []`` for
        a non-dict body, so that parent's whole collection would vanish and
        the cursor walk would advance past it) — is re-issued as a plain GET
        via :meth:`_get_as_batch_response`: a transient failure (429/5xx)
        recovers through ``_http_get``'s retry/backoff/token-refresh path,
        and a hard 4xx raises out of the read with the server's actual error
        body — never a silent skip."""
        status = resp.get("status") if isinstance(resp, dict) else None
        try:
            bad_status = status is None or int(status) >= 400
        except (TypeError, ValueError):
            bad_status = True
        body = resp.get("body") if isinstance(resp, dict) else None
        # A <400 sub-response the drains can't read is re-fetched as a plain
        # GET (so ``_fetch_page_payload`` parses it properly) rather than
        # dropping the parent. Every sub-request this connector issues is a
        # COLLECTION GET (both ``$batch`` producers build URLs via
        # ``_build_contained_url``), so a drainable body is exactly a JSON
        # object whose ``value`` is a LIST. Anything else — a string body
        # (the JSON-batch spec's form for non-JSON media), an OData ``error``
        # envelope behind a success status, a v2-style ``{"d": …}`` gateway
        # shape (no ``value`` at all — the drains would silently take
        # ``rows = []`` and the cursor walk would advance past the parent),
        # or ``{"value": null}`` / a non-list ``value`` (the drains would
        # crash iterating it) — is re-issued. An empty-collection 200
        # (``{"value": []}``) passes through untouched.
        undrainable_body = not isinstance(body, dict) or not isinstance(body.get("value"), list)
        if not bad_status and not undrainable_body:
            return resp
        _LOG.warning(
            "OData $batch sub-response for %r unusable (status %r, "
            "body type %s); re-issuing as a plain GET so the rows aren't "
            "silently skipped.",
            req_url,
            status,
            type(body).__name__,
        )
        return self._get_as_batch_response(req_url, edm_types)

    def _post_batch_adaptive(
        self, urls: list[str], edm_types: dict[str, str] | None = None
    ) -> list[dict]:
        """:meth:`_post_batch` with adaptive sizing: post ``urls`` in chunks no
        larger than the working cap, and on a "too many parts" rejection shrink
        the cap by 25% and retry the offending chunk re-split at the new cap — up
        to :data:`_BATCH_OVERFLOW_RETRIES` shrinks per instance. The discovered
        cap is recorded (persisted as ``batch_size_ok``) so later rounds and
        framework-recreated readers start there. Once a shrink would collapse the
        cap to a single part or the retry budget is spent, ``$batch`` is
        **given up** (cap pinned to the sentinel ``1``) and the remaining parts —
        plus every later round — fall back to a plain per-leaf-parent GET.
        Returns responses aligned with ``urls`` (``$batch`` sub-response shape)."""
        if self.__dict__.get("_batch_size_cap") == 1:  # give-up sentinel → plain GET
            return [self._get_as_batch_response(u, edm_types) for u in urls]
        out: list[dict] = []
        pending = list(urls)
        while pending:
            cap = self.__dict__.get("_batch_size_cap")
            if cap == 1:  # gave up mid-walk → plain GET the rest
                out.extend(self._get_as_batch_response(u, edm_types) for u in pending)
                break
            # Always slice the front at the CURRENT cap, so a shrink applies to
            # every remaining chunk — no stale oversized chunk wastes a retry.
            chunk = pending[:cap] if cap else pending
            try:
                out.extend(self._post_batch(chunk))
                pending = pending[len(chunk) :]
            except _BatchTooManyParts:
                if not self._shrink_batch_cap(len(chunk)) or self.__dict__["_batch_size_cap"] <= 1:
                    # Budget spent or batch collapsed to one part → give up on
                    # $batch and plain-GET everything still pending.
                    self.__dict__["_batch_size_cap"] = 1
                    self._store_capability("batch_size_ok", 1)
                    out.extend(self._get_as_batch_response(u, edm_types) for u in pending)
                    break
                # cap shrank; retry the (now smaller) front of pending.
        return out

    def _verify_batch_support(
        self,
        segments: list[str],
        table_options: dict[str, str] | None,
        start_offset: dict | None = None,
    ) -> bool:
        """Whether the server supports OData ``$batch`` (never raises).

        Used by ``cursor_probe=batch`` and the ``auto`` cascade to decide
        between a ``$batch`` hydrate and the plain N+1 walk. A verdict is cached
        per connector instance and persisted in the resume offset as ``batch_ok``
        (mirrors ``cursor_probe_ok``) so a per-batch-recreated reader skips the
        capability POST — but only a **definitive** verdict is cached/persisted:

        * definitive pass — 2xx envelope whose sub-response is < 400;
        * definitive fail — a hard rejection (404/405/any non-transient error
          status, a 2xx body that isn't a ``$batch`` envelope, or a hard-failed
          sub-response): the server doesn't speak ``$batch``.

        A transient outcome — transport error, auth hiccup, or a retryable
        status (408/429/5xx, :data:`_TRANSIENT_HTTP_STATUSES`) — returns
        ``False`` for THIS batch (degrade to the plain walk) but records
        nothing, so the next batch re-probes instead of permanently pinning the
        stream to the slow path (or permanently failing a strict
        ``contained_fetch=batch``) on a momentary blip. The probe is a SINGLE
        auth-aware attempt (``_http_get_once``, not the retrying ``_http_get``):
        a capability probe must fail FAST, not stall every ``auto`` read behind
        the transient-retry backoff loop — and routing through the auth-aware
        path means a 401/403 is surfaced as an auth failure rather than misread
        as "no ``$batch``"."""
        if (start_offset or {}).get("batch_ok"):
            return True
        cached = self.__dict__.get("_batch_supported")
        if cached is not None:
            return cached
        # Process/file cache: paths whose offsets can't carry the verdict
        # (contained snapshot streams — bare ``{}`` offsets — and the batch
        # reader) would otherwise re-pay this POST on every framework-
        # recreated instance. Pull the discovered chunk cap along with the
        # verdict so the adaptive shrink doesn't re-discover it either.
        cached = self._cached_capability("batch_ok")
        if cached is not None:
            self.__dict__["_batch_supported"] = cached
            cap = self._cached_capability("batch_size_ok")
            if cap is not None and "_batch_size_cap" not in self.__dict__:
                self.__dict__["_batch_size_cap"] = parse_batch_size_cap(
                    cap, "shared capability cache"
                )
            return cached
        # Probe with the SAME shape the real hydrate sends: no ``$top`` (the
        # sub-requests deliberately strip it and let the server drive paging
        # inside the batch). Probing with ``?$top=1`` would false-fail servers
        # that reject an explicit ``$top`` — a case the connector explicitly
        # accommodates on plain snapshot reads — and persist ``batch_ok=False``
        # even though the actual hydrate shape works.
        probe_url = join_url(self.service_url, segments[0])
        payload = {
            "requests": [{"id": "0", "method": "GET", "url": self._batch_relative(probe_url)}]
        }
        ok = False
        definitive = False
        try:
            resp = self._http_get_once(
                self._get_session(),
                join_url(self.service_url, "$batch"),
                method="POST",
                json=payload,
            )
        except Exception:  # transport/auth failure — no verdict on $batch itself
            resp = None
        if resp is not None:
            if resp.status_code < 400:
                try:
                    subs = resp.json().get("responses") or []
                    # Require the ``id`` echo, not just a sub-response: the
                    # real hydrate (``_post_batch``) keys sub-responses by the
                    # echoed id and hard-raises when it's missing, and that
                    # raise is not ``_BatchTooManyParts`` — so a pass verdict
                    # for an id-less server would pin ``batch_ok: true`` and
                    # then fail EVERY hydrate with "missing sub-response id"
                    # instead of degrading to plain GETs.
                    # Last-wins on duplicate ids, matching how _post_batch's
                    # by_id dict resolves them — probe and hydrate must judge
                    # the same sub-response.
                    by_id = {str(r.get("id")): r for r in subs if isinstance(r, dict)}
                    sub = by_id.get("0")
                    # No default for a MISSING ``status``: mapping it to 0
                    # would read as ``0 < 400`` — a definitive PASS minted
                    # from a malformed envelope. Missing/None flows to the
                    # ``sub_status is None`` branch below (definitive fail,
                    # same as an id-less envelope): the real hydrate treats
                    # a status-less sub-response as bad and re-issues plain
                    # GETs, so pinning ``batch_ok`` would just prepend a
                    # doomed ``$batch`` POST to every hydrate.
                    raw_status = sub.get("status") if sub else None
                    sub_status = int(raw_status) if raw_status is not None else None
                except Exception:
                    sub_status = None
                if sub_status is None:
                    definitive = True  # 2xx, but not a consumable $batch envelope
                elif sub_status < 400:
                    ok = definitive = True
                elif sub_status not in _TRANSIENT_HTTP_STATUSES:
                    definitive = True  # sub-request hard-rejected
            elif resp.status_code not in _TRANSIENT_HTTP_STATUSES:
                definitive = True  # e.g. 404/405 — no $batch endpoint
        if definitive:
            self.__dict__["_batch_supported"] = ok
            self._store_capability("batch_ok", ok)
        return ok

    def _expand_read_active(
        self,
        table_name: str,
        table_options: dict[str, str] | None,
        start_offset: dict | None = None,
    ) -> bool:
        """The RESOLVED expand decision for this table: an explicit
        ``expand_contained=true``, or ``auto`` whose preflight verifies the
        server (see :meth:`_verify_expand_support`). ``false`` (incl. unset)
        and ``auto``-with-a-failed-preflight return ``False`` — the N+1 shape.

        Shared by ``read_table`` (which passes ``start_offset`` so a persisted
        ``expand_ok`` skips the probe) and the partition activation
        (``is_partitioned`` / batch ``get_partitions``, no offset available —
        the instance cache dedupes the probe within one setup)."""
        mode = self._expand_contained_mode(table_options)
        if mode != "auto":
            return mode == "true"
        segments = self._table_segments(table_name)
        if segments is None:
            return False  # flat table — nothing to expand
        return self._verify_expand_support(table_name, segments, table_options, start_offset)

    def _verify_expand_support(
        self,
        table_name: str,
        segments: list[str],
        table_options: dict[str, str] | None,
        start_offset: dict | None = None,
    ) -> bool:
        """Whether the server supports the nested-``$expand`` read for this
        path (the ``expand_contained=auto`` preflight; never raises).

        Mirrors :meth:`_verify_batch_support`'s verdict discipline: a pass is
        persisted in the resume offset as ``expand_ok`` and cached per
        instance **per table** (unlike the genuinely server-wide
        ``_or_filter_ok`` / ``_batch_supported`` scalars — different nesting
        depths can verify differently, so one table's verdict must never
        answer for another on a multi-table instance), but only a
        **definitive** outcome is recorded —

        * definitive pass — the real expand URL returns 2xx AND inline child
          collections are present at every level down to the leaf;
        * definitive fail — a hard 4xx on the expand URL, a non-collection
          2xx body, or a level whose inline children are missing/empty while
          direct navigation shows children exist (the server accepted the URL
          but silently ignored ``$expand`` — using it would drop rows);
        * transient / inconclusive — transport errors, retryable statuses, or
          a sample too empty to discriminate (empty top set, or a genuinely
          childless probed branch): **fall back to the N+1 shape for THIS
          batch** and re-run the preflight next batch, recording nothing.

        Expand behaviour engages ONLY on a conclusive pass — ``auto`` never
        assumes the server can ``$expand`` before the verdict is in. The
        N+1 walk is always correct, so an unresolved verdict costs request
        shape, never rows; the risky direction (expand on an unverified
        server) is what silently drops every deep row. A childless-first-
        branch server that ignores ``$expand`` would otherwise read as
        inconclusive forever while losing data on every other branch."""
        if (start_offset or {}).get("expand_ok"):
            return True
        key = self._expand_shared_key(table_name, table_options)
        memo = self.__dict__.setdefault("_expand_supported", {})
        cached = memo.get(key)
        if cached is not None:
            return cached
        # Process/file cache (per-table, namespace-qualified — different
        # nesting depths / namespaces can verify differently): covers the
        # contained snapshot stream (bare ``{}`` offsets) and the batch
        # reader, where the offset channel can't carry ``expand_ok`` across
        # framework-recreated instances.
        cached = self._cached_capability("expand_ok", table_name=key)
        if cached is not None:
            memo[key] = cached
            return cached
        ok, definitive = self._run_expand_preflight(
            table_name, segments, table_options, start_offset
        )
        if definitive:
            memo[key] = ok
            self._store_capability("expand_ok", ok, table_name=key)
        return ok

    def _expand_preflight_url(
        self,
        table_name: str,
        segments: list[str],
        table_options: dict[str, str] | None,
        start_offset: dict | None,
    ) -> tuple[str, int, str | None]:
        """Build the preflight GET: the REAL expand URL for this table (same
        inner ``$top``/``$orderby``/``$filter`` construction as the read),
        with a small page budget and the top-level ``$top`` pinned to 1 so the
        probe response stays a single subtree. When a cursor is configured but
        no watermark exists yet, a synthetic floor value stands in so the
        inner ``cursor gt`` ``$filter`` construct is still exercised (later
        batches will send one; a server that rejects it must fail the
        preflight now, not the first filtered read). Returns
        ``(url, cursor_level, cursor_filter)`` — the filter pieces feed the
        direct-navigation cross-check."""
        namespace = (table_options or {}).get("namespace")
        cursor_field = (table_options or {}).get("cursor_field")
        since = (start_offset or {}).get("cursor")
        if cursor_field and since is None:
            try:
                since, _kind = self._cursor_floor(table_name, namespace, cursor_field)
            except ValueError:
                since = None  # no synthesisable floor — probe unfiltered
        if cursor_field:
            cursor_level, cursor_filter, cursor_order, cursor_select = self._cursor_expand_clause(
                segments, namespace, cursor_field, since
            )
        else:
            cursor_level, cursor_filter, cursor_order, cursor_select = -1, None, None, None
        probe_opts = {**(table_options or {}), "page_size": _EXPAND_PREFLIGHT_PAGE}
        url = self._build_expand_url(
            segments,
            probe_opts,
            cursor_level=cursor_level if cursor_field else None,
            cursor_filter=cursor_filter,
            cursor_order=cursor_order,
            cursor_select=cursor_select,
        )
        # Only the top-level ``$top`` follows ``?``/``&``; the inner expand
        # tops (after ``(``/``;``) are left at their per-level floor.
        return rewrite_top_in_url(url, 1), cursor_level, cursor_filter

    def _run_expand_preflight(
        self,
        table_name: str,
        segments: list[str],
        table_options: dict[str, str] | None,
        start_offset: dict | None,
    ) -> tuple[bool, bool]:
        """One-shot behavioural probe for ``expand_contained=auto``. Returns
        ``(ok, definitive)`` — see :meth:`_verify_expand_support` for the
        verdict semantics. SINGLE auth-aware attempt (``_http_get_once``):
        a capability probe must fail fast, and a transient blip only degrades
        this batch."""
        url, cursor_level, cursor_filter = self._expand_preflight_url(
            table_name, segments, table_options, start_offset
        )
        try:
            resp = self._http_get_once(self._get_session(), url)
        except Exception:  # transport/auth failure — no verdict on $expand itself
            return (False, False)
        if resp.status_code >= 400:
            return (False, resp.status_code not in _TRANSIENT_HTTP_STATUSES)
        try:
            top_rows = resp.json().get("value")
        except Exception:
            return (False, False)
        if not isinstance(top_rows, list):
            return (False, True)  # 2xx, but not an OData collection payload
        if not top_rows:
            # Empty top set — nothing to discriminate. N+1 this batch (it
            # emits the same nothing), re-probe next batch.
            return (False, False)
        return self._expand_preflight_walk(
            segments, top_rows, table_options, cursor_level, cursor_filter
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _expand_preflight_walk(
        self,
        segments: list[str],
        top_rows: list[dict],
        table_options: dict[str, str] | None,
        cursor_level: int,
        cursor_filter: str | None,
    ) -> tuple[bool, bool]:
        """Walk the probe response level by level verifying inline containment.

        At each level, descend through every row whose child nav property is a
        non-empty inline list. The first level with NO inline children anywhere
        is ambiguous — either the server ignored ``$expand`` (rows dropped!) or
        this branch is genuinely childless — so it is resolved with ONE direct-
        navigation ``$top=1`` GET on the first parent's child collection
        (carrying the same level ``$filter`` the expand sent, so a filtered-
        empty level isn't misread as ignored-``$expand``): children found ⇒
        definitive fail; none (or the check itself fails) ⇒ **inconclusive —
        fall back to N+1 for this batch** and re-probe next batch. Expand only
        ever runs on the one conclusive-pass outcome: inline rows present at
        every level down to the leaf."""
        namespace = (table_options or {}).get("namespace")
        pending: list[tuple[dict, list[dict[str, Any]]]] = [(r, []) for r in top_rows]
        for lvl in range(len(segments) - 1):
            try:
                pks = self._own_primary_keys_for_et(
                    self._entity_type_for(CONTAINED_PATH_SEP.join(segments[: lvl + 1]), namespace)
                )
            except ValueError:
                return (False, False)
            if not pks:
                return (False, False)  # can't address a child collection to verify
            child_key = segments[lvl + 1]
            nxt: list[tuple[dict, list[dict[str, Any]]]] = []
            for row, chain in pending:
                kids = row.get(child_key)
                if isinstance(kids, list) and kids:
                    parent_keys = {pk: row.get(pk) for pk in pks}
                    nxt.extend((k, chain + [parent_keys]) for k in kids)
            if nxt:
                pending = nxt
                continue
            row, chain = pending[0]
            full_chain = chain + [{pk: row.get(pk) for pk in pks}]
            check_url = self._expand_preflight_child_check_url(
                segments, lvl, full_chain, table_options, cursor_level, cursor_filter
            )
            try:
                r2 = self._http_get_once(self._get_session(), check_url)
                direct = (r2.json().get("value") or []) if r2.status_code < 400 else None
            except Exception:
                direct = None
            if direct is None:
                return (False, False)  # couldn't verify — N+1, re-probe next batch
            if direct:
                if any(f"{child_key}@odata.nextLink" in r for r, _ in pending):
                    # ANNOTATION-DEFERRING server: it acknowledged the expanded
                    # property with a ``<Nav>@odata.nextLink`` instead of
                    # inlining rows — the READ path follows exactly that
                    # annotation (see ``_flatten_expand_response``), so this
                    # level's containment IS honored; a definitive fail here
                    # would permanently pin N+1 on a server where
                    # ``expand_contained=true`` works end-to-end. Verify the
                    # DEEPER levels with a sub-rooted expand probe (the direct
                    # check's children were fetched WITHOUT ``$expand``, so
                    # descending through them raw would false-fail one level
                    # down).
                    if lvl + 2 >= len(segments):
                        return (True, True)  # deferring level's children ARE leaves
                    sub_url = self._assemble_expand_url(
                        join_url(
                            self.service_url,
                            self._build_contained_path(segments[: lvl + 2], full_chain, namespace),
                        ),
                        segments,
                        lvl + 1,
                        {**(table_options or {}), "page_size": _EXPAND_PREFLIGHT_PAGE},
                        resolve_segment_filters(table_options, segments),
                        cursor_level,
                        cursor_filter,
                        None,
                        None,
                        None,
                    )
                    try:
                        r3 = self._http_get_once(self._get_session(), sub_url)
                        sub_rows = (r3.json().get("value") or []) if r3.status_code < 400 else None
                    except Exception:
                        sub_rows = None
                    if not sub_rows:
                        return (False, False)  # deeper levels unverifiable — re-probe
                    pending = [(r, full_chain) for r in sub_rows]
                    continue
                return (False, True)  # children exist but $expand omitted them
            return (False, False)  # sampled branch genuinely childless — N+1, re-probe
        return (True, True)

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _expand_preflight_child_check_url(
        self,
        segments: list[str],
        lvl: int,
        chain: list[dict[str, Any]],
        table_options: dict[str, str] | None,
        cursor_level: int,
        cursor_filter: str | None,
    ) -> str:
        """Direct-navigation URL for the cross-check: the child collection at
        ``lvl + 1`` under ``chain``, ``$top=1``, carrying the SAME ``$filter``
        the expand's inner clause applied at that level (cursor filter /
        ``filters_at`` / the leaf ``filter``) so a legitimately
        filtered-empty level isn't misread as the server ignoring ``$expand``."""
        segment_filters = resolve_segment_filters(table_options, segments)
        is_leaf = (lvl + 1) == len(segments) - 1
        level_filter = combine_filters(
            cursor_filter if cursor_level == lvl + 1 else None,
            segment_filters.get(lvl + 1),
            (table_options or {}).get("filter") if is_leaf else None,
        )
        base = join_url(
            self.service_url,
            self._build_contained_path(
                segments[: lvl + 2], chain, (table_options or {}).get("namespace")
            ),
        )
        url = f"{base}?$top=1"
        if level_filter:
            url += f"&$filter={level_filter}"
        return url

    def _contained_fetch_batch_size(self, table_options: dict[str, str] | None) -> int:
        """Parse the ``contained_fetch`` table option into a requested ``$batch``
        chunk size: how the **full** contained walks (the snapshot read and the
        framework batch-reader stream) hydrate each leaf-parent collection.

        * ``auto`` (**default**) — pack the per-leaf-parent GETs into OData
          ``$batch`` requests of up to :data:`_BATCH_MAX_OPS` (1000) ops each
          (server-driven paging, ``@odata.nextLink`` follow-up), collapsing M
          round-trips into ``ceil(M / 1000)`` (auto-reduced on a server "too
          many parts" rejection). A one-shot capability preflight gates it; on a
          server without ``$batch`` it transparently **falls back** to ``single``.
        * ``batch`` — same hydrate, but **strict**: a server that fails the
          ``$batch`` capability preflight is an error (see
          :meth:`_contained_fetch_batch_n`), not a silent fall-back.
        * ``auto:<N>`` / ``batch:<N>`` — the ``auto`` / ``batch`` behaviour with
          the chunk size set to a positive integer ``N`` ops per ``$batch``
          request (``ceil(M / N)``), e.g. ``batch:200`` to start smaller.
        * ``single`` — the original behaviour: one GET per leaf-parent.
        * a bare positive integer ``N`` — like ``batch:<N>`` (strict): ``N == 1``
          is equivalent to ``single``, ``N > 1`` packs ``N`` GETs per request.

        Returns the requested chunk size (``single`` → 1, ``auto``/``batch`` →
        :data:`_BATCH_MAX_OPS`, the ``:<N>`` forms → ``N``). The strict-vs-fall-
        back axis is :meth:`_contained_fetch_strict`.

        Unlike ``cursor_probe`` (which accelerates the *incremental* leaf-cursor
        read), this governs the un-cursored full walks; the two are
        independent."""
        raw = ((table_options or {}).get("contained_fetch") or "auto").strip().lower()
        base, sep, suffix = raw.partition(":")
        if sep:
            # Only ``auto`` / ``batch`` carry a ``:<N>`` chunk-size suffix.
            if base not in ("auto", "batch"):
                raise ValueError(
                    f"Invalid contained_fetch={raw!r}. Only 'auto' and 'batch' accept a "
                    "':<N>' size suffix (e.g. batch:200)."
                )
            try:
                size = int(suffix)
            except ValueError:
                raise ValueError(
                    f"Invalid contained_fetch={raw!r}. The size suffix must be a "
                    "positive integer (e.g. batch:200)."
                ) from None
            if size < 1:
                raise ValueError(f"Invalid contained_fetch={raw!r}. The size suffix must be >= 1.")
            return size
        if raw in ("auto", "batch"):
            return _BATCH_MAX_OPS
        if raw == "single":
            return 1
        try:
            size = int(raw)
        except ValueError:
            raise ValueError(
                f"Invalid contained_fetch={raw!r}. Expected 'auto', 'batch', 'single', "
                "'auto:<N>', 'batch:<N>', or a positive integer."
            ) from None
        if size < 1:
            raise ValueError(f"Invalid contained_fetch={raw!r}. Must be a positive integer (>= 1).")
        return size

    def _contained_fetch_strict(self, table_options: dict[str, str] | None) -> bool:
        """``True`` when ``contained_fetch`` demands ``$batch`` strictly — the
        ``batch`` / ``batch:<N>`` forms or a bare integer ``N > 1``. In strict
        mode a server that fails the ``$batch`` capability preflight raises
        instead of falling back. ``auto`` / ``auto:<N>`` (default), ``single``,
        ``1`` → ``False`` (the preflight failure falls back to the N+1 walk)."""
        base = (
            ((table_options or {}).get("contained_fetch") or "auto")
            .strip()
            .lower()
            .partition(":")[0]
        )
        if base == "auto":
            return False
        if base == "batch":
            return True
        if base == "single":
            return False
        return self._contained_fetch_batch_size(table_options) > 1  # bare integer

    def _contained_fetch_is_auto(self, table_options: dict[str, str] | None) -> bool:
        """``True`` when ``contained_fetch`` is the ``auto`` family (``auto`` or
        ``auto:<N>``), including the unset default. Used to decide whether the
        persisted ``$batch`` verdicts should be cleared on a non-``auto`` run so
        a later switch back to ``auto`` re-runs the preflight."""
        raw = ((table_options or {}).get("contained_fetch") or "auto").strip().lower()
        return raw.partition(":")[0] == "auto"

    def _contained_fetch_batch_n(
        self, segments: list[str], table_options: dict[str, str] | None
    ) -> int:
        """Effective ``$batch`` chunk size for the full contained walk after the
        capability preflight: the requested :meth:`_contained_fetch_batch_size`
        when it is ``> 1`` *and* the server passes the ``$batch`` preflight, else
        ``1`` (plain one-GET-per-leaf-parent). On a preflight failure the
        behaviour splits on :meth:`_contained_fetch_strict`: ``auto`` (default) /
        ``single`` fall back to ``1``; ``batch`` / ``N > 1`` **raise** — the user
        asked for ``$batch`` and the server can't honour it."""
        size = self._contained_fetch_batch_size(table_options)
        if size <= 1:
            return 1
        if not self._verify_batch_support(segments, table_options):
            if self._contained_fetch_strict(table_options):
                raw = (table_options or {}).get("contained_fetch")
                raise ValueError(
                    f"contained_fetch={raw!r} requires OData $batch, but the server "
                    "failed the $batch capability preflight. Use contained_fetch=auto "
                    "to fall back to per-leaf-parent GETs, or contained_fetch=single."
                )
            return 1
        return size

    def _contained_fetch_forces_single(self, table_options: dict[str, str] | None) -> bool:
        """``True`` when ``contained_fetch`` is **explicitly** set to ``single`` or
        ``1`` — a user signal to avoid OData ``$batch`` for contained leaf
        hydration. The leaf-cursor read honours it everywhere: the probe's
        dirty-parent hydrate AND the ``auto`` no-probe cascade go down the plain
        N+1 walk (the probe still prunes which parents to read; only the
        ``$batch`` hydrate is suppressed). The one exception is an explicit
        ``cursor_probe=batch``, a direct demand for the ``$batch`` hydrate that
        wins the conflict. Unset, or any value ``> 1`` (incl. the ``auto``
        default), returns ``False``."""
        if (table_options or {}).get("contained_fetch") is None:
            return False
        return self._contained_fetch_batch_size(table_options) <= 1

    def _iter_contained_leaf_rows(
        self,
        segments: list[str],
        chain_meta_iter: Iterator[tuple[list[dict[str, Any]], Any]],
        table_options: dict[str, str],
        extra_filter: str | None,
        order_by: str | None,
        batch_size: int,
    ) -> Iterator[tuple[Any, dict]]:
        """Hydrate leaf collections for a lazy full walk, yielding
        ``(meta, raw_row)`` for every leaf row. ``chain_meta_iter`` pairs each
        key-chain with an opaque ``meta`` the caller needs per row (the chain
        for FK tagging, plus an ancestor cursor for the ancestor-cursor stream).

        ``single`` mode (``batch_size <= 1``) is the original behaviour: one
        :meth:`_fetch_pages` GET per chain (``$top``/pagination honoured).
        ``batch`` mode (``batch_size > 1``) buffers chains into groups of
        ``batch_size`` and hydrates each group with one ``$batch`` request —
        ``$top`` stripped so the server drives paging, and any sub-response
        ``@odata.nextLink`` is re-batched until drained. Lazy at group
        granularity (≤ one chunk of collections buffered at a time)."""
        leaf_types = self._edm_types_for_level(
            segments, len(segments) - 1, (table_options or {}).get("namespace")
        )
        if batch_size <= 1:
            for chain, meta in chain_meta_iter:
                url = self._build_contained_url(
                    segments, chain, table_options, extra_filter=extra_filter, order_by=order_by
                )
                for row in self._fetch_pages(url, leaf_types):
                    yield meta, row
            return
        # ``$batch``: drop ``page_size`` so sub-requests carry no ``$top`` and
        # the server drives paging (the keyset/$skip drain can't run inside a
        # batch sub-request — overflow comes back as @odata.nextLink instead).
        leaf_opts = {k: v for k, v in (table_options or {}).items() if k != "page_size"}
        group: list[tuple[list[dict[str, Any]], Any]] = []
        for chain, meta in chain_meta_iter:
            group.append((chain, meta))
            if len(group) >= batch_size:
                yield from self._drain_contained_group(
                    segments, group, leaf_opts, extra_filter, order_by, batch_size, leaf_types
                )
                group = []
        if group:
            yield from self._drain_contained_group(
                segments, group, leaf_opts, extra_filter, order_by, batch_size, leaf_types
            )

    def _drain_contained_group(
        self,
        segments: list[str],
        group: list[tuple[list[dict[str, Any]], Any]],
        leaf_opts: dict[str, str],
        extra_filter: str | None,
        order_by: str | None,
        batch_size: int,
        edm_types: dict[str, str] | None = None,
    ) -> Iterator[tuple[Any, dict]]:
        """Hydrate one group of leaf-parent chains via ``$batch`` (+ nextLink
        continuations), yielding ``(meta, raw_row)`` with ``@odata.*`` stripped.
        ``batch_size`` caps the ops per ``$batch`` round (including re-batched
        ``@odata.nextLink`` continuations)."""
        pending: list[tuple[int, str]] = []
        meta_by_key: dict[int, Any] = {}
        for key, (chain, meta) in enumerate(group):
            pending.append(
                (
                    key,
                    self._build_contained_url(
                        segments, chain, leaf_opts, extra_filter=extra_filter, order_by=order_by
                    ),
                )
            )
            meta_by_key[key] = meta
        while pending:
            eff = self._effective_batch_size(batch_size)
            round_ = pending[:eff]
            pending = pending[eff:]
            responses = self._post_batch_adaptive([u for _, u in round_], edm_types)
            for (key, req_url), resp in zip(round_, responses):
                resp = self._checked_batch_subresponse(resp, req_url, edm_types)
                body = resp.get("body") if isinstance(resp, dict) else None
                rows = body.get("value", []) if isinstance(body, dict) else []
                for row in rows:
                    clean = {k: v for k, v in row.items() if not k.startswith("@odata.")}
                    yield meta_by_key[key], clean
                raw_next = body.get("@odata.nextLink") if isinstance(body, dict) else None
                if raw_next:
                    pending.append((key, self._resolve_next_link(req_url, raw_next)))

    def _read_contained_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Walk the parent-key tree N+1 and emit leaf rows tagged with
        ancestor FKs, streamed lazily.

        Rows are yielded as each leaf page is fetched; no full
        materialisation. On wide subtrees (many parents, many pages
        per parent) this keeps peak memory bounded by one page worth
        of rows rather than the whole result set.
        """
        segments = self._table_segments(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        fk_columns = self._resolve_fk_columns(segments, namespace)
        segment_filters = resolve_segment_filters(table_options, segments)
        leaf_extra = segment_filters.get(len(segments) - 1)
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)
        batch_size = self._contained_fetch_batch_n(segments, table_options)

        def _emit() -> Iterator[dict]:
            chain_meta = (
                (chain, chain)
                for chain in self._iter_parent_key_chains(segments, namespace, table_options)
            )
            for chain, row in self._iter_contained_leaf_rows(
                segments, chain_meta, table_options, leaf_extra, leaf_order_by, batch_size
            ):
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                yield row

        return _emit(), {}

    def _warn_expand_inner_truncation_risk(self, segments: list[str]) -> None:
        """Warn when ``expand_contained=true`` runs under ``pagination=nextlink``.

        In nextlink mode the client-driven inner-``$expand`` continuation is
        disabled (:meth:`_inner_expand_continuation_url` returns ``None``), so a
        server that page-limits a nested ``$expand`` while omitting its
        ``<NavProp>@odata.nextLink`` silently drops the deeper rows (the exact
        failure this guard exists to surface). The default ``auto`` — and
        ``keyset``/``skip`` — drain those collections themselves; nextlink mode
        trusts the server's links entirely.

        (A missing ``$top`` is a related risk — the drainer can't size a
        continuation without one — but ``read_table`` always defaults
        ``page_size`` for the client-driven modes, and nextlink mode is caught
        here regardless, so that case can't independently arise.)
        """
        if len(segments) < 2:
            return
        if getattr(self, "_pagination", "nextlink") == "nextlink":
            _LOG.warning(
                "expand_contained=true with pagination=nextlink on %r: if the "
                "server page-limits a nested $expand collection without emitting "
                "its <NavProp>@odata.nextLink, the deeper rows (e.g. changed "
                "leaf-level records) are silently dropped. Use the default "
                "pagination=auto so the connector drains inner collections "
                "itself.",
                CONTAINED_PATH_SEP.join(segments),
            )

    def _read_contained_expand(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Iterative work-queue pull driven by nested ``$expand``;
        flatten each response into leaf rows tagged with ancestor FKs.
        When ``cursor_field`` is set, a ``$filter``/``$orderby`` is
        injected at the closest segment that owns the cursor (top-level
        query or inner ``$expand``), restricting the response to
        changed subtrees. Emitted leaf rows are stamped with the cursor
        value from that segment when they don't carry it themselves.
        Server depth caps surface as HTTP 4xx — no client-side
        fallback.

        The pull is capped at ``max_records_per_batch`` rows (default
        10000). When the cap fires, the remaining work queue — a list
        of self-contained ``{url, level, chain, cur_val, skip}`` fetch
        tasks (see ``_drain_expand_pages``) — is parked in the resume
        offset as ``pending_fetches`` so the next ``read()`` call
        resumes exactly where it left off: top-level pagination,
        inner-collection ``@odata.nextLink`` follows, and mid-page row
        positions all live in the queue. For cursor mode the watermark
        only advances once the chain fully drains — mid-chain advance
        would skip unread rows under the same ``since``. While a chain
        is in flight the running max cursor lives at
        ``running_max_cursor`` in the offset; on chain completion it
        becomes the new ``cursor`` value.
        """
        segments = self._table_segments(table_name) or [table_name]
        if len(segments) < 2:
            raise ValueError(f"expand_contained requires a contained path; {table_name!r} is flat.")
        namespace = (table_options or {}).get("namespace")
        cursor_field = (table_options or {}).get("cursor_field")
        # Resolve the read-floor window for THIS read (static value, or the
        # ``auto`` measurement carried in the offset) before building the
        # cursor clause — ``_apply_cursor_lookback`` reads it off ``self``.
        self._active_lookback_seconds = self._resolve_active_lookback(start_offset)
        self._arm_lookback_dedup(start_offset, table_name)
        cursor_level, cursor_filter, cursor_order, cursor_select = self._cursor_expand_clause(
            segments, namespace, cursor_field, (start_offset or {}).get("cursor")
        )
        # Read-scoped context the flatten recursion needs to synthesize a
        # client-driven continuation for an inner collection whose
        # ``<NavProp>@odata.nextLink`` the server omitted (see
        # ``_build_expand_continuation_url``). Stashed on ``self`` — like
        # ``self._pagination`` — so it survives into the lazy streaming
        # generator without threading through every flatten call site.
        # Leans on the framework's serial-calls contract (see the
        # ``_pagination`` stash in ``_read_table_dispatch``): the lazy
        # generator is drained before any other entry point runs on this
        # instance, so nothing can clobber the stash mid-drain.
        self._expand_cont_opts = table_options
        self._expand_cont_since = (start_offset or {}).get("cursor")
        # Per-level property→Edm-type maps for the same recursion (and the
        # queue drains), so keyset-seek boundaries built while paging a
        # collection at any depth render TYPED (guid bare / ISO-looking
        # string quoted) — same stash-on-self pattern as above.
        self._expand_types_per_level = [
            self._edm_types_for_level(segments, i, namespace) for i in range(len(segments))
        ]
        if cursor_field and cursor_level == -1:
            raise ValueError(
                f"cursor_field={cursor_field!r} is not a property of any "
                f"segment in {table_name!r}."
            )
        pks_per_level: list[list[str]] = []
        for idx in range(len(segments) - 1):
            et = self._entity_type_for(CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace)
            pks = self._own_primary_keys_for_et(et)
            if not pks:
                raise ValueError(
                    f"Cannot $expand contained path: segment {segments[idx]!r} "
                    f"has no primary key declared in $metadata."
                )
            pks_per_level.append(pks)
        fk_columns = self._resolve_fk_columns(segments, namespace)
        max_records = _parse_max_records(table_options)
        # Either resume from a parked work queue or seed it with the
        # top-level URL. Each queue item is self-contained (URL +
        # level + ancestor chain + captured cursor) so resume needs
        # no URL rebuild.
        pending_in = (start_offset or {}).get("pending_fetches")
        if pending_in:
            initial_queue = list(pending_in)
            resuming = True
        else:
            initial_queue = [
                {
                    "url": self._build_expand_url(
                        segments,
                        table_options,
                        cursor_level=cursor_level if cursor_field else None,
                        cursor_filter=cursor_filter,
                        cursor_order=cursor_order,
                        cursor_select=cursor_select,
                    ),
                    "level": 0,
                    "chain": [],
                    "cur_val": None,
                    "skip": 0,
                }
            ]
            resuming = False
        ctx = (cursor_field, cursor_level, None) if cursor_field else None
        # The page_size budget (``None`` when unset — no ``$top`` is sent at
        # any level). The drainer/streamer re-derive the per-level ``$top``
        # distribution per work item from its root level (see
        # :func:`compute_expand_tops_for_root`), so a continuation rooted deep
        # in the chain budgets across only its own collection levels rather than
        # the whole chain.
        page_size_opt = (table_options or {}).get("page_size")
        page_size = int(page_size_opt) if page_size_opt else None
        self._warn_expand_inner_truncation_risk(segments)
        if start_offset is None:
            # Batch reader: offset discarded, ``since`` is None (no cursor
            # filter), cap disabled, guard skipped — so the ``emitted``
            # accumulation the drainer does serves nothing. Stream leaf
            # rows one response at a time so an uncapped batch doesn't
            # materialise the whole result set. See ``read_table``.
            return (
                self._stream_expand_pages(
                    initial_queue, segments, pks_per_level, fk_columns, ctx, page_size
                ),
                {},
            )
        emitted: list[dict] = []
        # Wall-clock around the eager drain measures this batch's walk time,
        # feeding the ``auto`` lookback window (see ``_attach_lookback_state``).
        drain_start = time.monotonic()
        remaining_queue = self._drain_expand_pages(
            initial_queue,
            max_records,
            segments,
            pks_per_level,
            fk_columns,
            emitted,
            ctx,
            page_size,
            # New-rows-only cap accounting: the committed watermark (never
            # the lookback-floored read filter) is the counting floor.
            count_floor=(start_offset or {}).get("cursor") if cursor_field else None,
            leaf_pks=self._own_primary_keys_for_et(
                self._entity_type_for(CONTAINED_PATH_SEP.join(segments), namespace)
            ),
        )
        drain_elapsed = time.monotonic() - drain_start
        end_offset = self._build_expand_end_offset(
            emitted, cursor_field, start_offset, remaining_queue
        )
        if not cursor_field:
            return iter(emitted), end_offset
        if not emitted and not resuming and not remaining_queue:
            # Idle batch: nothing emitted, nothing resumed, nothing PARKED.
            # The ``not remaining_queue`` clause is defensive — echoing
            # ``start_offset`` when a batch parked a non-empty queue without
            # emitting a leaf would DROP that queue and re-do the same fetches
            # forever. The depth-first drain only parks after the cap trips
            # (which requires an emitted leaf), so a non-empty park implies
            # emitted rows — but the clause keeps the invariant robust
            # regardless of how the drain evolves.
            return iter([]), start_offset or {}
        records, out_offset = self._finalize_cursor_read(
            start_offset, end_offset, emitted, table_name, cursor_field
        )
        return records, self._attach_lookback_state(
            out_offset, start_offset, bool(remaining_queue), drain_elapsed
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    # pylint: disable=too-many-statements  # one cohesive DFS stack machine (see docstring);
    # splitting it fragments the tightly-coupled park/resume/emit invariants — matches the
    # inline-disable convention already used for _walk_contained_with_cursor below.
    def _drain_expand_pages(
        self,
        initial_queue: list[dict],
        max_records: int,
        segments: list[str],
        pks_per_level: list[list[str]],
        fk_columns: dict[tuple[int, str], str],
        emitted: list[dict],
        ctx: tuple | None,
        page_size: int | None,
        count_floor: Any = None,
        leaf_pks: list[str] | None = None,
    ) -> list[dict]:
        """Resumable depth-first stack machine over the contained ``$expand``.

        The parked offset (``pending_fetches``) is this method's return
        value, serialized every microbatch — so its size must stay bounded.
        A naive breadth-first queue that appended one continuation per
        truncated inner collection parked O(fan-out width) items (a wide top
        page over an inner-paging server → thousands of URL-carrying items, a
        multi-MB offset). This walks DEPTH-FIRST instead, so the parked
        frontier is O(depth) — one frame per contained segment on the current
        path — regardless of fan-out.

        The work stack holds two frame kinds:

        * **url**  — ``{url, level, chain, cur_val, boundary, skip}``: fetch
          ONE page, then push a ``rows`` frame for its contents. This is the
          only serializable shape (what a parked/seed queue item looks like).
        * **rows** — an in-memory page of sibling rows at ``level`` plus an
          ``idx`` cursor (``rows, level, chain, cur_val, idx, from_inline,
          base_url, page_next_url, boundary, tops``). A wide inline sibling
          collection is ONE rows frame, not N queued items — that is what
          keeps the frontier O(depth) rather than O(width).

        Per iteration the ``max_records`` cap is checked at the top; when
        reached the loop stops pulling new work and ``_park`` serializes the
        stack (bottom-to-top) into O(depth) ``url`` items. A ``rows`` frame is
        parked by re-fetching its collection past the last processed row: a
        fetched page re-fetches its own ``base_url`` with ``skip``/``boundary``;
        an inline-delivered collection synthesizes a seek continuation
        (``_build_expand_continuation_url``; ``nextlink`` mode has no
        churn-safe client seek, so it refetches FROM THE START and the parked
        chronological ``boundary`` elides already-processed rows on resume —
        bounded duplicates, never loss). An ancestor frame's in-flight row is
        skipped on resume because its remaining subtree is carried by the
        deeper parked frames — no re-emit, no loss.

        Resume re-fetches each parked url and skips rows at-or-below its
        ``boundary`` by chronological ORDER KEY comparison — NOT by position:
        the page is re-fetched from a mutating source, so a positional
        ``skip`` desynchronizes under churn (an update to an already-emitted
        row on a cursor-ordered page moves it to the tail and shifts an UNREAD
        row into the skipped prefix — its subtree lost behind the watermark; a
        delete on a PK-ordered page does the same). ``skip`` remains as the
        legacy/downgrade fallback for parked offsets without a boundary. Rows
        whose boundary comparison is incomparable are processed
        (duplicate-safe), never skipped. The returned queue is the work left
        to do — non-empty means "continuation pending", empty means "chain
        drained".

        ``count_floor`` — see ``_walk_contained_with_cursor``: only rows
        strictly above the committed watermark count toward
        ``max_records``, so a lookback overlap larger than the cap cannot
        wedge the stream into an eternal park/complete cycle. Cap deviation
        per batch is bounded by ONE HTTP response's worth of leaf rows
        (≤ ``page_size``) plus the lookback overlap — in every pagination
        mode, since parking never has to finish an in-flight collection.
        """
        cur_field, cur_level, _ = ctx or (None, -1, None)
        countable = 0
        lb_filter = getattr(self, "_lb_dedup_filter", None)

        def _count_new(rows_slice: list[dict]) -> int:
            if count_floor is None or cur_field is None:
                return len(rows_slice)
            return sum(
                1
                for r in rows_slice
                if r.get(cur_field) is not None and _cursor_newer(r.get(cur_field), count_floor)
            )

        def _row_order_key(row: dict, level: int) -> list:
            # The row's values for its page's own $orderby terms (see
            # _expand_level_order_by: cursor-first at the cursor level,
            # PK-only elsewhere) — the churn-stable within-page identity.
            # ``pks_per_level`` covers ancestor levels only; leaf-level
            # pages (inner continuations) use the leaf's own PKs.
            # MUST return a ``list``: parked as ``boundary`` in the offset and
            # compared (``row_key == boundary``) after a JSON round-trip that
            # makes it a list — a tuple would silently break the resume-skip
            # (duplicate-safe, never loss, just re-work).
            key: list = []
            if cur_field is not None and level == cur_level:
                key.append(row.get(cur_field))
            pks = pks_per_level[level] if level < len(pks_per_level) else (leaf_pks or [])
            key.extend(row.get(pk) for pk in pks)
            return key

        leaf_level = len(segments) - 1
        # Client-driven seek used to synthesize a resume URL for an
        # inline-delivered collection. ``nextlink`` has no churn-safe client
        # seek — a positional ``$skip`` the server HONOURS can shift an unread
        # row into the skipped prefix under between-batch churn (silent loss)
        # — so ``_park`` refetches the collection FROM THE START there
        # (``$skip=0``) and resume skips already-processed rows by the parked
        # chronological ``boundary`` instead: bounded duplicate re-reads
        # (≤ one inline page, elided client-side), never loss.
        mode = getattr(self, "_pagination", "nextlink")
        park_mode = mode if mode != "nextlink" else "skip"

        def _push_url(item: dict) -> None:
            stack.append(
                {
                    "kind": "url",
                    "url": item["url"],
                    "level": int(item["level"]),
                    "chain": [dict(p) for p in item.get("chain") or []],
                    "cur_val": item.get("cur_val"),
                    "boundary": item.get("boundary"),
                    "skip": int(item.get("skip", 0) or 0),
                    "rebuilt": bool(item.get("rebuilt")),
                }
            )

        # Reconstruct the work stack from the parked (or seed) queue. Parked
        # frames are stored bottom-to-top (see ``_park``), so pushing them in
        # order restores the DFS path with the deepest (leaf-most) frame on
        # top — resumed first, exactly where the prior batch stopped.
        stack: list[dict] = []
        for item in initial_queue:
            _push_url(item)

        def _park() -> list[dict]:
            # Serialize the whole stack (O(depth)) bottom-to-top. Each frame's
            # ``boundary`` is its CURRENT row's order key: the deepest frame's
            # current row is fully done (skip it, resume at the next); an
            # ancestor's current row is in flight but its remaining subtree is
            # carried by the deeper parked frames, so skipping it here and
            # resuming at the next sibling is exactly right — no re-emit, no
            # loss. This is what bounds the offset to O(depth), not O(width).
            parked: list[dict] = []
            for frame in stack:
                if frame["kind"] == "url":
                    parked.append(
                        {
                            "url": frame["url"],
                            "level": frame["level"],
                            "chain": frame["chain"],
                            "cur_val": frame["cur_val"],
                            "skip": frame.get("skip", 0),
                            "boundary": frame.get("boundary"),
                            "rebuilt": frame.get("rebuilt", False),
                        }
                    )
                    continue
                idx = frame["idx"]
                level = frame["level"]
                if idx >= len(frame["rows"]):
                    # Page fully processed this batch — no remaining rows to
                    # resume here. An inline-delivered collection is complete
                    # as delivered (nothing to synthesize); a fetched page
                    # parks its server next-link so the sibling pages follow.
                    if not frame.get("from_inline") and frame.get("page_next_url"):
                        parked.append(
                            {
                                "url": frame["page_next_url"],
                                "level": level,
                                "chain": frame["chain"],
                                "cur_val": frame["cur_val"],
                                "skip": 0,
                                "boundary": None,
                            }
                        )
                    continue
                boundary = (
                    _row_order_key(frame["rows"][idx - 1], level) if idx else frame.get("boundary")
                )
                if not frame.get("from_inline"):
                    park_url = frame["base_url"]
                    park_skip = idx
                else:
                    # Inline-delivered rows have no page URL of their own; re-fetch
                    # the collection under its parent (chain holds keys 0..level-1)
                    # seeking past the last processed row. ``nextlink`` has no
                    # churn-safe client seek (a server-honoured ``$skip=idx``
                    # can drop an unread row into the skipped prefix), so it
                    # refetches FROM THE START and lets the ``boundary`` skip
                    # already-processed rows on resume — bounded duplicates,
                    # never loss.
                    if mode != "nextlink":
                        seek_child, seek_count = (frame["rows"][idx - 1] if idx else {}), idx
                    else:
                        seek_child, seek_count = {}, 0
                    park_url = self._build_expand_continuation_url(
                        segments,
                        level - 1,
                        frame["chain"],
                        cur_field if cur_field else None,
                        park_mode,
                        seek_child,
                        seek_count,
                    )
                    park_skip = 0
                parked.append(
                    {
                        "url": park_url,
                        "level": level,
                        "chain": frame["chain"],
                        "cur_val": frame["cur_val"],
                        "skip": park_skip,
                        "boundary": boundary,
                    }
                )
            return parked

        while stack:
            if countable >= max_records:
                # Cap reached — stop pulling new work and park the O(depth)
                # frontier. Overshoot is bounded by the last page's leaf rows.
                break
            frame = stack[-1]
            if frame["kind"] == "url":
                stack.pop()
                level = frame["level"]
                try:
                    page_rows, page_next_url = self._fetch_one_expand_page(
                        frame["url"], self._expand_level_types(level)
                    )
                except requests.HTTPError as exc:
                    replacement = self._recover_expand_item(exc, frame, segments, cur_field)
                    if replacement is not None:
                        _push_url(replacement)
                    continue
                if not page_rows:
                    if page_next_url:
                        # Forward the resume ``boundary`` (content-based,
                        # churn-safe) so an empty first page doesn't drop it
                        # before the rows it guards; ``skip`` is positional to
                        # THIS page and so resets for the server continuation.
                        _push_url(
                            {
                                "url": page_next_url,
                                "level": level,
                                "chain": frame["chain"],
                                "cur_val": frame["cur_val"],
                                "boundary": frame.get("boundary"),
                            }
                        )
                    continue
                start_idx, resume_boundary = _expand_resume_start(
                    page_rows,
                    frame.get("boundary"),
                    frame.get("skip", 0),
                    lambda r, lv=level: _row_order_key(r, lv),
                )
                stack.append(
                    {
                        "kind": "rows",
                        "rows": page_rows,
                        "level": level,
                        "chain": frame["chain"],
                        "cur_val": frame["cur_val"],
                        "idx": start_idx,
                        "from_inline": False,
                        "base_url": frame["url"],
                        "page_next_url": page_next_url,
                        "boundary": resume_boundary,
                        # Tops budgeted over only THIS page's collection levels
                        # (root == fetch level downward); ancestors are fixed keys.
                        "tops": (
                            compute_expand_tops_for_root(page_size, len(segments), level)
                            if page_size
                            else None
                        ),
                    }
                )
                continue
            # rows frame
            if frame["idx"] >= len(frame["rows"]):
                stack.pop()
                if frame.get("page_next_url"):
                    _push_url(
                        {
                            "url": frame["page_next_url"],
                            "level": frame["level"],
                            "chain": frame["chain"],
                            "cur_val": frame["cur_val"],
                        }
                    )
                continue
            row = frame["rows"][frame["idx"]]
            frame["idx"] += 1
            level = frame["level"]
            if frame.get("boundary") is not None:
                row_key = _row_order_key(row, level)
                # Anchor row missing: skip only rows PROVABLY at-or-below the
                # parked boundary (collation-honest — see _chain_strictly_before);
                # anything else is processed (duplicate-safe, never silent loss).
                if row_key == frame["boundary"] or _chain_strictly_before(
                    row_key, frame["boundary"]
                ):
                    continue
            row_cur_val = frame["cur_val"]
            if cur_field and level == cur_level:
                row_cur_val = row.get(cur_field)
            if level == leaf_level:
                prev_len = len(emitted)
                self._emit_leaf_row(
                    row, segments, frame["chain"], fk_columns, emitted, cur_field, row_cur_val
                )
                # Streaming dedup on the FINAL emitted shape (annotations
                # stripped, FKs tagged, cursor stamped — what the hash must
                # match). The transient append/del keeps per-row memory O(1).
                if (
                    lb_filter is not None
                    and len(emitted) > prev_len
                    and not lb_filter.admit(emitted[-1])
                ):
                    del emitted[-1]
                    continue
                countable += _count_new(emitted[prev_len:])
                continue
            # Non-leaf: descend depth-first. Push the truncated-tail
            # continuation FIRST (bottom) and the inline children ON TOP, so
            # inline rows drain before the tail — and the inline collection is
            # ONE in-memory rows frame, never N queued items (kills the width
            # burst; the frontier stays O(depth)).
            child_chain = frame["chain"] + [{pk: row.get(pk) for pk in pks_per_level[level]}]
            next_ctx = (cur_field, cur_level, row_cur_val) if cur_field else None
            cont_url = self._derive_child_continuation(
                level,
                row,
                segments,
                child_chain,
                next_ctx,
                frame.get("tops"),
                page_size,
                frame["base_url"],
            )
            inline_children = row.get(segments[level + 1]) or []
            if cont_url is not None:
                _push_url(
                    {
                        "url": cont_url,
                        "level": level + 1,
                        "chain": child_chain,
                        "cur_val": row_cur_val,
                    }
                )
            if inline_children:
                stack.append(
                    {
                        "kind": "rows",
                        "rows": inline_children,
                        "level": level + 1,
                        "chain": child_chain,
                        "cur_val": row_cur_val,
                        "idx": 0,
                        "from_inline": True,
                        "base_url": frame["base_url"],
                        "page_next_url": None,
                        "boundary": None,
                        "tops": frame.get("tops"),
                    }
                )
        return _park() if stack else []

    def _stream_expand_pages(
        self,
        initial_queue: list[dict],
        segments: list[str],
        pks_per_level: list[list[str]],
        fk_columns: dict[tuple[int, str], str],
        ctx: tuple | None,
        page_size: int | None,
    ) -> Iterator[dict]:
        """Lazy variant of :meth:`_drain_expand_pages` for the batch reader.

        Pops fetch tasks FIFO, fetches one page each, flattens that page's
        rows into a short-lived local buffer and yields them, deferring
        inner-collection ``@odata.nextLink`` continuations back onto the
        queue exactly as the drainer does. No ``max_records`` cap and no
        cross-page accumulation: peak memory is one response's flattened
        cross-product (bounded by the ``page_size`` budget) plus the queue
        of pending fetch descriptors (URLs + chains, not rows). This queue
        is unbounded but never persists (no offset, no checkpoint) and its
        items are small descriptors; a server deferring every inner
        collection grows it to one descriptor per parent, modest even at
        100k parents. (The drainer, by contrast, walks depth-first, so its
        live frontier — and the ``pending_fetches`` it parks into the
        offset — stays O(path depth) regardless of fan-out.) Within each
        page rows flatten inline-first (parent then children), matching
        ``_emit_leaf_row`` order; ACROSS pages this
        reader is breadth-first (FIFO queue) whereas the capped drainer is
        depth-first, so cross-parent order differs between the two. That is
        immaterial here: the batch path is uncapped and never checkpoints, so
        emission order carries no resume semantics."""
        queue: list[dict] = list(initial_queue)
        cur_field, cur_level, _ = ctx or (None, -1, None)
        while queue:
            item = queue.pop(0)
            url = item["url"]
            level = item["level"]
            chain = [dict(p) for p in item.get("chain") or []]
            cur_val = item.get("cur_val")
            skip = int(item.get("skip", 0) or 0)
            item_ctx = (cur_field, cur_level, cur_val) if cur_field else None
            item_tops = (
                compute_expand_tops_for_root(page_size, len(segments), level) if page_size else None
            )
            # Same deleted-parent / stale-continuation recovery as the
            # drainer: inner continuations queued earlier in THIS walk can
            # 404 when the parent vanishes mid-walk.
            try:
                page_rows, page_next_url = self._fetch_one_expand_page(
                    url, self._expand_level_types(level)
                )
            except requests.HTTPError as exc:
                replacement = self._recover_expand_item(exc, item, segments, cur_field)
                if replacement is not None:
                    queue.insert(0, replacement)
                continue
            if not page_rows:
                if page_next_url:
                    queue.append(
                        {
                            "url": page_next_url,
                            "level": level,
                            "chain": [dict(p) for p in chain],
                            "cur_val": cur_val,
                            "skip": 0,
                        }
                    )
                continue
            for row_idx in range(skip, len(page_rows)):
                local_out: list[dict] = []
                self._flatten_expand_response(
                    level,
                    page_rows[row_idx],
                    segments,
                    pks_per_level,
                    chain,
                    fk_columns,
                    local_out,
                    item_ctx,
                    item_tops,
                    response_url=url,
                    pending_fetches=queue,
                    page_size=page_size,
                )
                yield from local_out
            if page_next_url:
                queue.append(
                    {
                        "url": page_next_url,
                        "level": level,
                        "chain": [dict(p) for p in chain],
                        "cur_val": cur_val,
                        "skip": 0,
                    }
                )

    def _recover_expand_item(
        self,
        exc: requests.HTTPError,
        item: dict,
        segments: list[str],
        cursor_field: str | None,
    ) -> dict | None:
        """Handle a 404/410 on a parked expand work item's URL.

        Level >= 1 items carry ENTITY-SCOPED URLs
        (``Parents(k)/Children?$skiptoken=…``) parked across batches in
        ``pending_fetches`` — and the world moves between batches. Two
        distinct causes produce the same status:

        * the parent entity was **deleted** → every URL under it is dead
          forever; re-raising turns the checkpoint into a permanently
          failing stream (the resume replays the same dead URL on every
          trigger — only a full refresh recovers);
        * the **continuation went stale** (e.g. an expired server
          ``$skiptoken``) while the parent still exists → dropping the
          item would silently lose the rest of that collection.

        Disambiguate by REBUILDING the item's URL from scratch and
        re-queueing it marked ``rebuilt``: a level >= 1 item's child
        collection via ``_build_expand_continuation_url`` (``mode="skip"``,
        ``inline_count=0`` — the read's cursor filter/order and grandchild
        expands intact); a level-0 item — a parked TOP-LEVEL server
        continuation, whose ``$skiptoken`` can equally expire while the
        collection lives — via the same seed-URL construction the read
        entry uses (``_cursor_expand_clause`` + ``_build_expand_url`` from
        the stashed options/watermark), re-reading the top collection from
        ``since``. If the rebuilt URL fails too, the SECOND recovery pass
        resolves by level: level >= 1 DROPS the item (the parent entity is
        gone — duplicate-safe; already-emitted rows stand), while level 0
        RE-RAISES (the whole collection vanishing is a config/service
        error, not row churn). Non-404/410 statuses always re-raise.

        Returns the replacement item, or ``None`` to drop.
        """
        status = exc.response.status_code if exc.response is not None else None
        level = item.get("level", 0)
        if status not in (404, 410):
            raise exc
        chain = [dict(p) for p in item.get("chain") or []]
        if item.get("rebuilt"):
            if level < 1:
                raise exc  # collection itself is gone — config error
            _LOG.warning(
                "expand resume: parent %s no longer exists (HTTP %s on the "
                "rebuilt collection URL too) — dropping its parked subtree. "
                "Rows already emitted for it remain; the deletion is not "
                "propagated (connector is insert/upsert-only).",
                chain,
                status,
            )
            return None
        _LOG.warning(
            "expand resume: parked continuation (level %s, parent %s) returned "
            "HTTP %s (deleted parent, or a stale server continuation) — "
            "rebuilding the URL from scratch and retrying once.",
            level,
            chain,
            status,
        )
        if level >= 1:
            fresh_url = self._build_expand_continuation_url(
                segments, level - 1, chain, cursor_field, "skip", {}, 0
            )
        else:
            # Rebuild the top-level seed exactly as the read entry does.
            table_options = getattr(self, "_expand_cont_opts", None) or {}
            since = getattr(self, "_expand_cont_since", None)
            namespace = table_options.get("namespace")
            if cursor_field:
                (
                    cursor_level,
                    cursor_filter,
                    cursor_order,
                    cursor_select,
                ) = self._cursor_expand_clause(segments, namespace, cursor_field, since)
            else:
                cursor_level, cursor_filter, cursor_order, cursor_select = -1, None, None, None
            fresh_url = self._build_expand_url(
                segments,
                table_options,
                cursor_level=cursor_level if cursor_field else None,
                cursor_filter=cursor_filter,
                cursor_order=cursor_order,
                cursor_select=cursor_select,
            )
        return {
            "url": fresh_url,
            "level": level,
            "chain": chain,
            "cur_val": item.get("cur_val"),
            "skip": 0,
            "rebuilt": True,
        }

    def _expand_level_types(self, level: int) -> dict[str, str] | None:
        """The stashed property→Edm-type map for the collection at ``level``
        of the current expand read (see the stash in
        ``_read_contained_expand``), or ``None`` outside one / past the
        stash's depth — the seek builder then falls back to value sniffing."""
        stash = getattr(self, "_expand_types_per_level", None)
        if stash and 0 <= level < len(stash):
            return stash[level]
        return None

    def _fetch_one_expand_page(
        self, url: str, edm_types: dict[str, str] | None = None
    ) -> tuple[list[dict], str | None]:
        """One HTTP GET; returns ``(page_rows, next_url)``. Thin wrapper
        over :meth:`_fetch_pages_with_links` that consumes a single
        iteration so the caller can check the cap between fetches.
        ``edm_types`` (the fetched collection's declared property types)
        types any keyset-seek boundary built for the returned ``next_url``.

        No-progress guard for the work-queue drainers: those slice pagination
        one page per call, so the in-generator guard in
        :meth:`_client_paginate_pages` is bypassed. The drainer instead drops
        the link when the resolved next URL equals the one we just fetched —
        i.e. the continuation didn't advance (server ignored the seek/``$skip``,
        or a self-referential ``@odata.nextLink``) — so the collection stops
        instead of looping forever.

        A continuation must keep going past a SHORT page, because a server that
        page-limits below the requested ``$top`` while omitting
        ``@odata.nextLink`` returns short pages that are NOT exhaustion —
        stopping there silently drops the rest of the inner collection (and in
        cursor mode the watermark then advances past the dropped rows, losing
        them permanently). The optimization that budgets a deep continuation's
        ``$top`` up to ``page_size`` makes this the common case: ``$top`` now
        routinely exceeds the server's per-response cap, so every continuation
        page is short. :meth:`_fetch_pages_with_links` drains short link-less
        pages; draining is safe even though its in-generator guard can't span
        these per-page re-entries: for keyset/skip the next seek differs from the
        current URL only when rows advanced, so a server that ignores the seek
        trips the ``page_next_url == url`` guard after at most one repeated page
        — and a repeated row is deduped at the destination by ``apply_changes``'
        MERGE on the primary key (a harmless duplicate, vs. the data loss a
        short-page stop causes)."""
        for page_rows, page_next_url in self._fetch_pages_with_links(url, edm_types):
            return page_rows, (None if page_next_url == url else page_next_url)
        return [], None

    def _build_expand_end_offset(
        self,
        emitted: list[dict],
        cursor_field: str | None,
        start_offset: dict | None,
        pending_queue: list[dict],
    ) -> dict:
        """Compose the resume offset for ``_read_contained_expand``.

        Three modes:

        * **Snapshot, chain in flight** → ``{pending_fetches: [...]}``.
        * **Snapshot, chain done** → ``{}`` (framework treats as
          terminal).
        * **Cursor mode** → the watermark stays at the original
          ``since`` while a chain is in flight, with the running max
          parked at ``running_max_cursor``. On chain exhaustion the
          running max becomes the new ``cursor`` value.

        ``pending_fetches`` is the work queue parked for the next
        batch — each entry is a self-contained
        ``{url, level, chain, cur_val, skip}`` (see
        :meth:`_drain_expand_pages`).
        """
        in_flight = bool(pending_queue)
        if not cursor_field:
            return {"pending_fetches": list(pending_queue)} if in_flight else {}
        prior_running = (start_offset or {}).get("running_max_cursor")
        batch_cursors = [r.get(cursor_field) for r in emitted if r.get(cursor_field) is not None]
        # Chronological max (``_cursor_max``): a lexical max over mixed
        # fractional renderings regresses the running watermark behind
        # emitted rows.
        if batch_cursors and prior_running is not None:
            new_running = _cursor_max([*batch_cursors, prior_running])
        elif batch_cursors:
            new_running = _cursor_max(batch_cursors)
        else:
            new_running = prior_running
        since = (start_offset or {}).get("cursor")
        if in_flight:
            offset: dict = {"pending_fetches": list(pending_queue)}
            if since is not None:
                offset["cursor"] = since
            if new_running is not None:
                offset["running_max_cursor"] = new_running
            return offset
        if new_running is not None:
            # Floored at ``since``: the lookback overlap re-reads below the
            # committed watermark, so after a deleted watermark row (or a
            # stale ``running_max_cursor`` resumed from a mode flip) the
            # running max alone can sit BELOW ``since`` — committing it
            # as-is would regress the watermark.
            return {"cursor": _max_or(new_running, since)}
        if since is not None:
            return {"cursor": since}
        # Chain drained AND no watermark to park (no prior ``since``, no
        # ``running_max_cursor``, no new cursor values this batch).
        # Returning ``dict(start_offset or {})`` would echo a resume
        # input like ``{"pending_fetches": [...]}`` back unchanged —
        # ``_read_contained_expand`` then sees ``start_offset ==
        # end_offset`` with ``emitted`` empty and returns the same
        # offset, and the framework re-issues it forever. Return ``{}``
        # so the offset advances and the chain terminates cleanly.
        return {}

    def _cursor_expand_clause(
        self,
        segments: list[str],
        namespace: str | None,
        cursor_field: str | None,
        since: Any,
    ) -> tuple[int, str | None, str | None, str | None]:
        """``(cursor_level, $filter, $orderby, $select)`` for ``$expand``
        mode. Returns ``(-1, None, None, None)`` when no cursor is set;
        the caller raises if the cursor isn't a property of any segment.
        The ``$select`` slot is always ``None`` today (see the comment
        below) but stays in the tuple: ``_build_expand_url`` merges it
        into the cursor level's clause, and the leaf level additionally
        merges the user's leaf-scoped ``select`` option."""
        if not cursor_field:
            return -1, None, None, None
        cursor_level = self._find_cursor_level(segments, namespace, cursor_field)
        if cursor_level == -1:
            return -1, None, None, None
        level_et = self._entity_type_for(
            CONTAINED_PATH_SEP.join(segments[: cursor_level + 1]), namespace
        )
        level_pks = self._own_primary_keys_for_et(level_et)
        order_terms = [f"{cursor_field} asc"]
        order_terms.extend(f"{p} asc" for p in level_pks if p != cursor_field)
        # No ``$select`` injection: the cursor column is returned by
        # default projection on declared CSDL properties, so it isn't
        # needed for stamping. Adding it silently trims other columns
        # the user didn't opt out of — particularly harmful when the
        # cursor segment is also the leaf (2-segment paths). Users who
        # want to trim can set ``select`` themselves on the leaf side.
        # Read floor lags the committed watermark by ``cursor_lookback_seconds``
        # so a non-atomic ``expand_contained=true`` walk re-scans rows that
        # arrived mid-walk and landed below the walk's final max (see
        # ``_apply_cursor_lookback``). Only the read filter is floored; the
        # committed watermark stays the true max of emitted rows
        # (``_build_expand_end_offset``), so the offset still advances.
        read_since = self._apply_cursor_lookback(since)
        return (
            cursor_level,
            self._cursor_filter(
                cursor_field,
                read_since,
                edm_type=self._edm_types_for_et(level_et).get(cursor_field),
            ),
            ",".join(order_terms),
            None,
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _emit_leaf_row(
        self,
        row: dict,
        segments: list[str],
        chain: list[dict[str, Any]],
        fk_columns: dict[tuple[int, str], str],
        out: list[dict],
        cur_field: str | None,
        cur_val: Any,
    ) -> None:
        """Emit one LEAF row: strip OData annotations, tag ancestor FKs, and
        stamp the inherited cursor value when the leaf doesn't carry its own.
        Shared by :meth:`_flatten_expand_response` (streaming recursion) and
        the :meth:`_drain_expand_pages` stack machine so both emit identically.
        """
        # Drop both top-level (``@odata.foo``) and per-property
        # (``Foo@odata.nextLink``) annotations from leaf rows; the framework
        # wouldn't know what to do with either.
        clean = {k: v for k, v in row.items() if "@odata." not in k}
        self._tag_with_ancestor_fks(clean, segments, chain, fk_columns)
        if cur_field and cur_val is not None and clean.get(cur_field) is None:
            clean[cur_field] = cur_val
        out.append(clean)

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _derive_child_continuation(
        self,
        level: int,
        row: dict,
        segments: list[str],
        chain: list[dict[str, Any]],
        next_ctx: tuple[str | None, int, Any] | None,
        per_level_tops: list[int] | None,
        page_size: int | None,
        base_url: str,
    ) -> str | None:
        """Resolve the continuation URL for ``row``'s inner collection at
        ``level + 1`` (or ``None`` when the collection is complete / not
        continuable). ``chain`` MUST already include ``row``'s own keys
        (levels ``0..level``). Extracted verbatim from
        :meth:`_flatten_expand_response` so the stack machine derives the
        exact same continuations.
        """
        next_seg = segments[level + 1]
        inner_next = row.get(f"{next_seg}@odata.nextLink")
        if inner_next:
            # _resolve_next_link, NOT a plain urljoin: some servers (Hexagon
            # SCApi, SAP Gateway) return per-property continuation links
            # relative to the SERVICE ROOT — urljoin against this deep page
            # URL would double the ancestor path (→ 404 and a full
            # rebuild-from-keys re-read on every inner continuation).
            resolved = self._resolve_next_link(base_url, inner_next)
            if per_level_tops:
                # Continuation pages the collection at ``level + 1`` under one
                # specific parent at ``level``. The original ``$top`` for that
                # level was sized against the FULL cross-product budget
                # (top × inner × …); the continuation is one outer level
                # shallower, so we have more budget to spend per response. New
                # $top is ``page_size_budget / inner_product`` where
                # ``inner_product`` is the cross-product of all levels deeper
                # than ``level + 1`` (which the server-side ``$expand`` chain
                # in the nextLink still applies).
                continuation_level = level + 1
                inner_product = 1
                for t in per_level_tops[continuation_level + 1 :]:
                    inner_product *= t
                # Budget is the full page_size: the ancestors 0..level are a
                # single fixed parent in the continuation, so they don't
                # multiply. ``page_size`` is passed explicitly rather than
                # re-derived from per_level_tops, whose entries below this
                # request's root level are placeholders. (per_level_tops is only
                # truthy when page_size was set, so page_size is present here.)
                new_top = max(MIN_DYNAMIC_TOP, (page_size or 0) // max(1, inner_product))
                resolved = rewrite_top_in_url(resolved, new_top)
            return resolved
        if next_seg in row and row[next_seg] is not None:
            # No ``<NavProp>@odata.nextLink``. In a client-driven pagination
            # mode (keyset/skip/auto), synthesize a direct-navigation
            # continuation when the inline page is a FULL page (== $top) and
            # so plausibly truncated; otherwise the inline page is taken as
            # the whole collection — today's nextlink-only behaviour. This
            # closes the inner-``$expand`` hole for servers that page-limit a
            # response but never emit the continuation link.
            return self._inner_expand_continuation_url(
                level, row, segments, chain, next_ctx, per_level_tops
            )
        # The expanded property is wholly ABSENT (or null) — spec-violating:
        # OData v4 requires every ``$expand``-ed property be PRESENT on each
        # row (a genuinely empty collection comes back as ``[]``, which the
        # branch above trusts). A partial-expansion server that inlines
        # children for some parents and omits the property for others would
        # otherwise have those subtrees silently dropped — absent is NOT
        # verified-empty. Fetch the collection directly from the start: mode
        # "skip" + count 0 yields a plain ``$skip=0`` from-the-beginning URL
        # with the deeper ``$expand`` chain intact, without engaging the
        # keyset OR-support probe an empty boundary couldn't use anyway.
        return self._build_expand_continuation_url(
            segments,
            level,
            chain,
            next_ctx[0] if next_ctx else None,
            "skip",
            {},
            0,
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _flatten_expand_response(
        self,
        level: int,
        row: dict,
        segments: list[str],
        pks_per_level: list[list[str]],
        chain: list[dict[str, Any]],
        fk_columns: dict[tuple[int, str], str],
        out: list[dict],
        cursor_ctx: tuple[str | None, int, Any] | None = None,
        per_level_tops: list[int] | None = None,
        response_url: str | None = None,
        pending_fetches: list[dict] | None = None,
        page_size: int | None = None,
    ) -> None:
        """Recurse into the nested $expand payload; tag and emit leaf rows.
        ``cursor_ctx`` is ``(cursor_field, cursor_level, captured_value)``
        threaded down the recursion: when ``level == cursor_level`` the
        captured value snaps to ``row[cursor_field]`` and propagates to
        every leaf row beneath, stamped only when the leaf doesn't
        already carry the column.

        OData v4 §11.2.6.1: when an expanded collection is server-paged
        the response carries a ``<NavProp>@odata.nextLink`` annotation
        alongside the inline page. The spec requires that link to
        preserve the original ``$expand`` chain, so following it yields
        the rest of the children with their grandchildren still
        expanded.

        ``per_level_tops`` is the per-level ``$top`` distribution from
        the initial request (see :func:`compute_dynamic_tops`). When
        deferring an inner-collection nextLink to the work queue, the
        connector rewrites the URL's ``$top`` to a value sized for
        that continuation's smaller cross-product, so wide inner
        collections don't take 100s of round trips paging at the
        original per-level ``$top``.

        ``response_url`` is the URL of the HTTP response that yielded
        ``row``. Used to resolve any relative ``<NavProp>@odata.nextLink``
        per OData v4 §11.2.5.7 / RFC 3986 (relative-reference
        resolution against the document's base URL). Falls back to
        ``service_url`` when not provided (only the unit tests do that).

        ``pending_fetches`` is the per-batch work queue used by
        :meth:`_drain_expand_pages`. Inner-collection nextLinks are
        APPENDED to it instead of followed inline — that lets the
        outer drainer check ``max_records_per_batch`` between fetches
        (at any level) rather than only after a full top-row subtree.
        The append captures the chain snapshot + captured cursor so
        the work item is self-contained for cross-batch resume.
        """
        base_url = response_url or self.service_url
        cur_field, cur_level, cur_val = cursor_ctx or (None, -1, None)
        if cur_field and level == cur_level:
            cur_val = row.get(cur_field)
        if level == len(segments) - 1:
            self._emit_leaf_row(row, segments, chain, fk_columns, out, cur_field, cur_val)
            return
        pks = pks_per_level[level]
        chain.append({pk: row.get(pk) for pk in pks})
        next_ctx = (cur_field, cur_level, cur_val) if cur_field else None
        next_seg = segments[level + 1]
        for child in row.get(next_seg) or []:
            self._flatten_expand_response(
                level + 1,
                child,
                segments,
                pks_per_level,
                chain,
                fk_columns,
                out,
                next_ctx,
                per_level_tops,
                response_url=base_url,
                pending_fetches=pending_fetches,
                page_size=page_size,
            )
        resolved = self._derive_child_continuation(
            level, row, segments, chain, next_ctx, per_level_tops, page_size, base_url
        )
        if resolved is not None:
            if pending_fetches is not None:
                # Defer the follow: the outer drainer pops one fetch
                # at a time and checks the cap between them. Snapshot
                # the ancestor chain so the work item is self-contained
                # for cross-batch resume.
                pending_fetches.append(
                    {
                        "url": resolved,
                        "level": level + 1,
                        "chain": [dict(p) for p in chain],
                        "cur_val": cur_val,
                        "skip": 0,
                    }
                )
                chain.pop()
                return
            # Track the URL that fetched each follow-up page so its
            # children resolve THEIR relative nextLinks correctly. In
            # keyset/skip mode ``_fetch_pages_with_links`` drives the
            # continuation via ``_client_paginate_pages`` (the synthesized
            # URL carries the seek/skip), draining the whole collection.
            inner_current = resolved
            for page_rows, page_next in self._fetch_pages_with_links(
                resolved, self._expand_level_types(level + 1)
            ):
                for child in page_rows:
                    self._flatten_expand_response(
                        level + 1,
                        child,
                        segments,
                        pks_per_level,
                        chain,
                        fk_columns,
                        out,
                        next_ctx,
                        per_level_tops,
                        response_url=inner_current,
                    )
                inner_current = page_next or inner_current
        chain.pop()

    def _inner_expand_continuation_url(
        self,
        level: int,
        row: dict,
        segments: list[str],
        chain: list[dict[str, Any]],
        cursor_ctx: tuple[str | None, int, Any] | None,
        per_level_tops: list[int] | None,
    ) -> str | None:
        """Synthesize a client-driven continuation for a parent's inner
        collection when the server returned a NON-EMPTY inline page but
        omitted its ``<NavProp>@odata.nextLink``.

        Returns ``None`` only when ``pagination`` is ``nextlink``, ``$top``
        isn't in force (``per_level_tops`` unset), or the inline collection
        is empty. Any non-empty inline page gets a continuation — a short
        page is NOT taken as proof of completeness (a server may page-limit
        a nested ``$expand`` below the requested per-level ``$top``); see
        the inline comment below for the full rationale and the one-empty-
        request cost this trades for closing that silent-truncation hole.
        """
        mode = getattr(self, "_pagination", "nextlink")
        if mode == "nextlink" or per_level_tops is None:
            return None
        child_level = level + 1
        if child_level >= len(segments):
            return None
        children = row.get(segments[child_level]) or []
        if not children:
            # Empty inline collection: an ``$expand`` returns ``[]`` for a
            # genuinely empty child collection, and there's no boundary row
            # to seek past — nothing to continue.
            return None
        # A SHORT inline page is NOT proof the collection is complete: a
        # server may page-limit a nested ``$expand`` below the requested
        # ``$top`` while omitting its ``<NavProp>@odata.nextLink`` (its
        # inner page size is smaller than our computed per-level ``$top``).
        # Mirroring the top-level ``auto`` contract (:meth:`_client_paginate_pages`
        # — seek until EMPTY, not until short), synthesize a continuation
        # past the last inline child on ANY non-empty page. When the inline
        # page was in fact complete the continuation's first page comes back
        # empty and the walk stops, costing one trailing empty request per
        # parent — the same price top-level ``auto`` pays. This closes the
        # silent-truncation hole that drops changed deep-level rows on
        # servers that don't emit inner-``$expand`` continuation links
        # (previously this returned ``None`` whenever the inline page was
        # shorter than the per-level ``$top``, taking a short page as proof
        # of exhaustion).
        cur_field = cursor_ctx[0] if cursor_ctx else None
        return self._build_expand_continuation_url(
            segments, level, chain, cur_field, mode, children[-1], len(children)
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _build_expand_continuation_url(
        self,
        segments: list[str],
        level: int,
        chain: list[dict[str, Any]],
        cursor_field: str | None,
        mode: str,
        last_child: dict,
        inline_count: int,
    ) -> str:
        """Direct-navigation URL paging the inner collection at ``level + 1``
        under the single parent identified by the current flatten ``chain``
        (keys for levels ``0..level``), with the grandchildren still
        ``$expand``-ed::

            Parent(k0)/.../Child?$top=N&$orderby=...&$expand=<grandchildren>

        plus a continuation marker that resumes *after* the inline page: a
        ``(k gt last_child)`` keyset seek on the ``$orderby`` keys (keyset /
        auto), or ``$skip=<inline_count>`` (skip, or keyset with a null
        boundary value). Fed back through :meth:`_fetch_pages_with_links`,
        which — in these modes — drives the rest of the collection via
        :meth:`_client_paginate_pages`.

        The cursor ``$filter``/``$orderby`` are re-derived from the read's
        stashed options so a child-level cursor stays applied across the
        continuation; the keyset seek subsumes ``cursor gt since`` for keyset
        mode, and the explicit cursor ``$filter`` keeps the filtered set
        intact for ``$skip``.
        """
        table_options = getattr(self, "_expand_cont_opts", None) or {}
        since = getattr(self, "_expand_cont_since", None)
        namespace = table_options.get("namespace")
        if cursor_field:
            cursor_level, cursor_filter, cursor_order, cursor_select = self._cursor_expand_clause(
                segments, namespace, cursor_field, since
            )
        else:
            cursor_level, cursor_filter, cursor_order, cursor_select = -1, None, None, None
        child_level = level + 1
        # ``chain`` holds keys for levels 0..level (== child_level - 1), so it
        # has exactly the prefix-key count ``_build_contained_path`` needs to
        # root the request at this parent's child collection.
        contained_base = join_url(
            self.service_url,
            self._build_contained_path(segments[: child_level + 1], chain, namespace),
        )
        # Budget the continuation's $top over only its own collection levels
        # (child_level..leaf); levels 0..level are now fixed keys in the path, so
        # they take no share. This is what gives the inner collection a real
        # $top (e.g. [100, 10] for the last two levels) rather than the
        # whole-chain floor (… 5, 5) the initial root-0 distribution would force.
        page_size_opt = table_options.get("page_size")
        cont_tops = (
            compute_expand_tops_for_root(int(page_size_opt), len(segments), child_level)
            if page_size_opt
            else None
        )
        segment_filters = resolve_segment_filters(table_options, segments)
        url = self._assemble_expand_url(
            contained_base,
            segments,
            child_level,
            table_options,
            segment_filters,
            cursor_level,
            cursor_filter,
            cursor_order,
            cursor_select,
            cont_tops,
        )
        order_keys = _pg_orderby_keys(url)
        # $orderby names properties of the CHILD collection's entity type —
        # its declared types steer seek-literal quoting (guid bare / string
        # quoted); best-effort so a resolution gap degrades to the sniff.
        child_types = self._edm_types_for_level(segments, child_level, namespace)
        # Skip the OR-across-columns keyset seek on servers that reject it
        # (preflight, cached) — fall through to $skip (mode B). Single-key
        # $orderby never builds an OR, so the probe short-circuits there.
        if (
            mode in ("keyset", "auto")
            and order_keys
            and self._verify_or_filter_support(url, order_keys, last_child, child_types)
        ):
            seek = _pg_keyset_filter(order_keys, last_child, child_types)
            if seek is not None:
                # Stash the clean child-level $filter as the keyset base so a
                # cross-batch resume REPLACES the seek instead of accumulating.
                return _pg_keyset_seek_url(url, _pg_get_filter(url), seek)
        return _pg_set_query(url, "$skip", str(inline_count))

    def _leaf_cursor_order_by(
        self, table_name: str, namespace: str | None, cursor_field: str
    ) -> str:
        """``cursor asc, pk1 asc, ...`` — unique total order so server
        skiptokens don't split same-cursor cohorts."""
        leaf_pks = self._own_primary_keys_for_et(self._entity_type_for(table_name, namespace))
        terms = [f"{cursor_field} asc"]
        terms.extend(f"{pk} asc" for pk in leaf_pks if pk != cursor_field)
        return ",".join(terms)

    def _leaf_pk_order_by(self, segments: list[str], namespace: str | None) -> str:
        """PK-only ``$orderby`` for a full leaf-collection fetch.

        Snapshot, ancestor-cursor, and partition reads pull the whole
        leaf collection under a parent with no cursor ``$filter``. Like
        the ancestor key fetches (``_ancestor_pk_order_by``), these page
        across server skiptokens, and OData v4 §11.2.5.7 doesn't promise
        a stable default order — without an explicit unique ``$orderby``
        the skiptoken can silently drop or duplicate leaf rows. Returns
        ``""`` when the leaf declares no PK (``_format_query_params``
        treats that as "no ``$orderby``")."""
        leaf_et = self._entity_type_for(CONTAINED_PATH_SEP.join(segments), namespace)
        return _ancestor_pk_order_by(self._own_primary_keys_for_et(leaf_et))

    # pylint: disable=too-many-statements,too-many-branches
    def _walk_contained_with_cursor(
        self,
        segments: list[str],
        chains_iter: Iterator[list[dict[str, Any]]],
        parent_idx_start: int,
        table_options: dict[str, str],
        order_by: str,
        cursor_field: str,
        since: Any,
        truncated_chain_cursor: Any,
        chain_next_link: str | None,
        max_records: int,
        fk_columns: dict[tuple[int, str], str],
        leaf_segment_filter: str | None = None,
        effective=None,
        skip_null: bool = False,
        parked_chain: list | None = None,
        count_floor: Any = None,
    ) -> tuple[list[dict], bool, int, list | None, str | None, Any]:
        """Drive the per-parent fetch loop (leaf-cursor mode).

        ``chains_iter`` is consumed lazily and the walk stops at the
        first parent that offers a valid resume checkpoint once the
        ``max_records`` cap is reached. Peak memory is normally bounded
        to one chain; the exception is a *complete* parent whose entire
        leaf collection shares a single cursor value (see below), which
        is emitted in full and absorbed into the walk rather than
        checkpointed.

        Resume positioning: ``parked_chain`` (the truncated parent's key
        chain, parked by the previous batch) is matched by the
        enumeration's own ordering keys — churn-stable, unlike the
        positional ``parent_idx_start`` skip it supersedes (see
        :func:`_chain_strictly_before` for the loss/mis-tagging modes a
        positional resume has under inserts/deletes between batches).
        Offsets written before ``parent_keys`` existed carry only
        ``parent_idx``, and fall back to the positional skip. A parked
        chain that vanished (parent deleted) resumes at the first chain
        past its position with the global ``since``. A park written by
        the ``$batch`` walk (no continuation keys) is EXCLUSIVE — the
        parked chain itself was fully drained and is skipped.

        Resume preference, applied to the resume-target chain:

        1. ``chain_next_link`` (server skiptoken) — fetched directly,
           bypassing URL rebuild. Used when the previous batch parked
           at a page boundary mid-chain.
        2. ``truncated_chain_cursor`` — used as ``cursor gt <value>``
           in a freshly-built URL. Used when the previous batch dropped
           a trailing same-cursor cohort at a complete-parent boundary.
        3. Otherwise the global ``since`` is used.

        Truncation checkpoint, decided when the cap is hit:

        * **Page boundary** (server returned an ``@odata.nextLink``) →
          ``chain_next_link_out`` is set; resume re-enters this parent.
        * **Complete parent with a distinct-cursor boundary** (no
          nextLink) → the trailing same-cursor cohort is dropped and
          ``truncated_chain_cursor_out`` is set; resume re-reads it.
        * **Complete parent, single cursor value** (no nextLink, no
          splittable boundary) → no checkpoint is possible and none is
          needed: the cohort is complete, so all its rows are kept and
          the walk continues to the next parent. The cap is overshot
          for that one parent (bounded by one server response).

        Returns ``(rows, truncated, parent_idx, parked_chain_out,
        chain_next_link_out, truncated_chain_cursor_out)`` —
        ``parked_chain_out`` is the truncated parent's key chain (for the
        next batch's key-based resume), ``None`` when not truncated.

        ``effective(row)`` supplies the cursor value used for filtering,
        the boundary trim and (via the caller) the watermark — the
        ``cursor_nulls`` resolver, so a null cursor can resolve to a
        synthetic floor without mutating the emitted row. ``skip_null``
        drops rows with a real null cursor (``cursor_nulls=ignore``)."""
        if effective is None:

            def effective(row):
                return row.get(cursor_field)

        emitted: list[dict] = []
        truncated = False
        parent_idx = 0
        chain_start_idx = 0
        # Only rows strictly above the COMMITTED watermark (``count_floor``,
        # the true ``since`` — not the lookback-floored read filter) count
        # toward ``max_records``: overlap re-reads ride on top, so a lookback
        # window holding >= max_records rows can't wedge the stream into an
        # eternal park/complete duplicate cycle — a pure-overlap walk always
        # completes and hits the suppressed-idle rule. The cap may overshoot
        # by the overlap size (bounded by the user's lookback window).
        countable = 0
        # Streaming overlap dedup (see ``_LookbackDedupFilter``): consulted
        # at the emit point so suppressed rows never join ``emitted``.
        lb_filter = getattr(self, "_lb_dedup_filter", None)
        parked_chain_out: list | None = None
        chain_next_link_out: str | None = None
        truncated_chain_cursor_out: Any = None
        parked_key = _chain_resume_key(parked_chain) if parked_chain is not None else None
        # A park with a continuation key resumes AT the parked chain; a park
        # without one (written by the $batch walk) is exclusive — the parked
        # chain was fully drained.
        resume_inclusive = chain_next_link is not None or truncated_chain_cursor is not None
        seeking = parked_key is not None
        # Leaf-collection property types: the compound keyset seek built while
        # paging a leaf collection (ALSO the cap-hit resume checkpoint) must
        # render its boundaries typed (guid bare / ISO-looking string quoted).
        leaf_types = self._edm_types_for_level(
            segments, len(segments) - 1, (table_options or {}).get("namespace")
        )
        for chain in chains_iter:
            # Skip the chains we already emitted in prior batches — by the
            # enumeration's own ordering keys when the offset parked them
            # (churn-stable), positionally for legacy index-only offsets.
            # The iterator still pays for the ancestor pages that produce
            # those chains (no way to skip without fetching the keys),
            # but no leaf fetches happen during the skip.
            if parked_key is not None:
                if seeking:
                    at_parked = chain == parked_chain
                    if (
                        not at_parked
                        and _chain_seek_order(_chain_resume_key(chain), parked_key) != "after"
                    ):
                        # "before": provably drained by a prior batch.
                        # "unknown" (server-collated text keys whose order
                        # isn't reproducible client-side): keep seeking on
                        # the identity anchor rather than trusting ordinal
                        # comparison, which silently skips unwalked subtrees
                        # on CI-collation servers. If the parked chain
                        # vanished, the seek runs out, this batch emits
                        # nothing, and the cleared park re-walks everything
                        # next batch (duplicate-safe, never loss — see the
                        # post-loop warning).
                        parent_idx += 1
                        continue
                    seeking = False
                    if at_parked and not resume_inclusive:
                        # Exclusive park: the parked chain itself is done.
                        parent_idx += 1
                        continue
                    is_resume_target = at_parked
                else:
                    is_resume_target = False
            else:
                if parent_idx < parent_idx_start:
                    parent_idx += 1
                    continue
                is_resume_target = parent_idx == parent_idx_start
            chain_start_idx = len(emitted)
            chain_since: Any
            initial_url: str
            if is_resume_target and chain_next_link is not None:
                # Resume from the server's own skiptoken; no client-side
                # filter — the link already encodes filter/order state.
                chain_since = None
                initial_url = chain_next_link
            else:
                if is_resume_target and truncated_chain_cursor is not None:
                    chain_since = truncated_chain_cursor
                else:
                    chain_since = since
                initial_url = self._build_contained_url(
                    segments,
                    chain,
                    table_options,
                    extra_filter=combine_filters(
                        self._cursor_filter(
                            cursor_field,
                            chain_since,
                            edm_type=leaf_types.get(cursor_field),
                        ),
                        leaf_segment_filter,
                    ),
                    order_by=order_by,
                )
            cap_hit_in_page = False
            page_next_url: str | None = None
            # Under the default ``auto``, a
            # server that page-limits a leaf below $top while omitting
            # @odata.nextLink is still drained (keyset seek until empty), so a
            # cursor read isn't silently truncated to one short page. The
            # synthesized seek that surfaces as ``page_next_url`` when the cap is
            # hit mid-leaf is itself the resume checkpoint: a compound
            # ``(cursor gt v) or (cursor eq v and pk gt p)`` seek that re-enters
            # this parent at the exact row, correctly continuing a same-cursor
            # cohort that spans the cap (better than the cursor-only trim below,
            # which is kept for nextlink mode / whole-leaf-in-one-response
            # servers where ``page_next_url`` is None).
            for page_rows, page_next_url in self._fetch_pages_with_links(initial_url, leaf_types):
                for row in page_rows:
                    if skip_null and row.get(cursor_field) is None:
                        continue
                    rec_cursor = effective(row)
                    # Chronological, not lexical — see the
                    # flat re-filter in ``_read_incremental``.
                    if (
                        chain_since is not None
                        and rec_cursor is not None
                        and _cursor_at_or_before_for_refilter(rec_cursor, chain_since)
                    ):
                        continue
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    if lb_filter is not None and not lb_filter.admit(row):
                        continue  # already delivered unchanged — never buffered
                    emitted.append(row)
                    if count_floor is None or (
                        rec_cursor is not None and _cursor_newer(rec_cursor, count_floor)
                    ):
                        countable += 1
                    if countable >= max_records:
                        cap_hit_in_page = True
                if cap_hit_in_page:
                    # Finish the current page (above) so its nextLink is a
                    # clean checkpoint, then stop fetching more pages of
                    # this chain and decide how to checkpoint below.
                    break
            if cap_hit_in_page:
                if page_next_url is not None:
                    # Page boundary mid-collection: the server skiptoken is
                    # a clean resume point — park it (with this chain's keys
                    # so the resume re-finds THIS parent under churn) and
                    # re-enter this parent next batch.
                    truncated = True
                    parked_chain_out = chain
                    chain_next_link_out = page_next_url
                    break
                # No nextLink ⇒ the server returned this parent's ENTIRE
                # leaf collection, so its cohort is complete. Prefer an
                # intra-parent boundary: drop the trailing same-cursor
                # cohort and resume this parent at ``cursor gt`` the last
                # distinct value (which re-reads that cohort).
                trimmed = _trim_to_distinct_cursor_boundary(emitted[chain_start_idx:], cursor_field)
                if trimmed:
                    del emitted[chain_start_idx + len(trimmed) :]
                    truncated = True
                    parked_chain_out = chain
                    # Effective value (synthetic floor for a null under
                    # coalesce) so the resumed ``cursor gt`` is a real,
                    # comparable boundary — never the restored-null column.
                    truncated_chain_cursor_out = effective(trimmed[-1])
                    break
                # Every row of this complete parent shares one cursor value
                # — no splittable boundary exists, and re-reading the parent
                # can't make progress. The cohort is COMPLETE, so keep all
                # its rows and continue to the next parent. The cap is
                # necessarily overshot for this parent (bounded by one
                # server response); there is no valid mid-walk checkpoint,
                # which beats failing the batch. (Formerly a RuntimeError.)
                parent_idx += 1
                continue
            parent_idx += 1
        if seeking:
            # The anchor was never re-found (parent deleted, or its key
            # rendering changed between batches) and the remaining chains
            # couldn't be PROVEN already-walked. Completing here would let
            # the caller fold ``running_max`` into the committed cursor —
            # locking every unwalked subtree's sub-max rows out forever —
            # so return a TRUNCATED reset instead: no park, positional
            # restart at 0, cursor floor kept. The next batch re-walks
            # everything above ``since`` (duplicate-safe, deduplicated by
            # the destination MERGE), never loss.
            _LOG.warning(
                "Parked resume position %r was not re-found while enumerating "
                "%r; this batch emitted nothing and reset the walk — the next "
                "batch re-walks from the committed cursor floor "
                "(duplicate-safe).",
                parked_chain,
                segments,
            )
            truncated = True
            parent_idx = 0
            parked_chain_out = None
            chain_next_link_out = None
            truncated_chain_cursor_out = None
        return (
            emitted,
            truncated,
            parent_idx,
            parked_chain_out,
            chain_next_link_out,
            truncated_chain_cursor_out,
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _batch_walk_contained_with_cursor(
        self,
        segments: list[str],
        chains_iter: Iterator[list[dict[str, Any]]],
        parent_idx_start: int,
        table_options: dict[str, str],
        order_by: str,
        cursor_field: str,
        since: Any,
        max_records: int,
        fk_columns: dict[tuple[int, str], str],
        leaf_segment_filter: str | None = None,
        effective=None,
        skip_null: bool = False,
        batch_size: int = _BATCH_MAX_OPS,
        parked_chain: list | None = None,
        resume_inclusive: bool = False,
        count_floor: Any = None,
    ) -> tuple[list[dict], bool, int, list | None, None, None]:
        """OData ``$batch`` counterpart to :meth:`_walk_contained_with_cursor`.

        Hydrates leaf collections via ``$batch`` instead of one GET per
        leaf-parent: chains are buffered into groups of ``batch_size`` (default
        :data:`_BATCH_MAX_OPS`, tunable via ``cursor_probe=batch:<N>``), each
        group sent as a single ``$batch`` of ``cursor gt since`` reads with
        **no ``$top``** (server-driven paging), and every per-sub-response
        ``@odata.nextLink`` is re-batched (also capped at ``batch_size``)
        until each collection is drained. Rows go through the same null-skip /
        below-floor trim / FK-tag pipeline as the plain walk, so the emitted set
        is identical — only the request shape differs.

        Resume + cap are **chunk-aligned**: the cap is checked after each
        fully-drained group, so truncation parks the LAST DRAINED chain's keys
        (an EXCLUSIVE park — that chain is complete; the resume skips through
        it by the enumeration's ordering keys, churn-stable). Legacy
        index-only offsets fall back to the positional ``parent_idx`` skip.
        ``chain_next_link`` / ``truncated_chain_cursor`` are unused (returned
        ``None``); a serial-walk park resumed here (``resume_inclusive``)
        re-drains the parked chain in full — duplicates, never loss. The cap
        is overshot by at most one group's worth of changed rows (the same
        bounded-overshoot tolerance the plain walk applies to a single
        complete parent).

        Returns the 6-tuple
        ``(emitted, truncated, parent_idx, parked_chain_out, None, None)``."""
        if effective is None:

            def effective(row):
                return row.get(cursor_field)

        emitted: list[dict] = []
        truncated = False
        parent_idx = 0
        # New-rows-only cap accounting — see _walk_contained_with_cursor.
        countable = 0
        lb_filter = getattr(self, "_lb_dedup_filter", None)
        group: list[list[dict[str, Any]]] = []
        # Drop ``page_size`` so the per-leaf-parent sub-requests carry NO ``$top``
        # — the server drives paging and emits ``@odata.nextLink`` for any
        # overflow (the keyset/$skip drain the plain ``auto`` walk would use to
        # continue a short link-less page can't run inside a batch sub-request).
        leaf_opts = {k: v for k, v in (table_options or {}).items() if k != "page_size"}
        leaf_types = self._edm_types_for_level(
            segments, len(segments) - 1, (table_options or {}).get("namespace")
        )

        def _drain_group(buffered: list[list[dict[str, Any]]]) -> None:
            nonlocal countable
            # idx-keyed initial URLs (no $top → server pages + emits nextLink).
            pending: list[tuple[int, str]] = []
            chain_by_key: dict[int, list[dict[str, Any]]] = {}
            for key, chain in enumerate(buffered):
                pending.append(
                    (
                        key,
                        self._build_contained_url(
                            segments,
                            chain,
                            leaf_opts,
                            extra_filter=combine_filters(
                                self._cursor_filter(
                                    cursor_field,
                                    since,
                                    edm_type=leaf_types.get(cursor_field),
                                ),
                                leaf_segment_filter,
                            ),
                            order_by=order_by,
                        ),
                    )
                )
                chain_by_key[key] = chain
            while pending:
                eff = self._effective_batch_size(batch_size)
                round_ = pending[:eff]
                pending = pending[eff:]
                responses = self._post_batch_adaptive([u for _, u in round_], leaf_types)
                for (key, req_url), resp in zip(round_, responses):
                    resp = self._checked_batch_subresponse(resp, req_url, leaf_types)
                    body = resp.get("body") if isinstance(resp, dict) else None
                    rows = body.get("value", []) if isinstance(body, dict) else []
                    chain = chain_by_key[key]
                    for row in rows:
                        if skip_null and row.get(cursor_field) is None:
                            continue
                        rec_cursor = effective(row)
                        # Chronological, not lexical — see
                        # the flat re-filter in ``_read_incremental``.
                        if (
                            since is not None
                            and rec_cursor is not None
                            and _cursor_at_or_before_for_refilter(rec_cursor, since)
                        ):
                            continue
                        clean = {k: v for k, v in row.items() if not k.startswith("@odata.")}
                        self._tag_with_ancestor_fks(clean, segments, chain, fk_columns)
                        if lb_filter is not None and not lb_filter.admit(clean):
                            continue  # already delivered unchanged
                        emitted.append(clean)
                        if count_floor is None or (
                            rec_cursor is not None and _cursor_newer(rec_cursor, count_floor)
                        ):
                            countable += 1
                    raw_next = body.get("@odata.nextLink") if isinstance(body, dict) else None
                    if raw_next:
                        pending.append((key, self._resolve_next_link(req_url, raw_next)))

        parked_key = _chain_resume_key(parked_chain) if parked_chain is not None else None
        seeking = parked_key is not None
        parked_chain_out: list | None = None
        for chain in chains_iter:
            if parked_key is not None:
                if seeking:
                    at_parked = chain == parked_chain
                    if (
                        not at_parked
                        and _chain_seek_order(_chain_resume_key(chain), parked_key) != "after"
                    ):
                        # Same seek contract as the plain leaf-cursor walk
                        # above: "before" is provably drained; "unknown"
                        # keeps seeking on the identity anchor (never trust
                        # ordinal order of server-collated text keys).
                        parent_idx += 1
                        continue
                    seeking = False
                    if at_parked and not resume_inclusive:
                        parent_idx += 1
                        continue
            else:
                if parent_idx < parent_idx_start:
                    parent_idx += 1
                    continue
            group.append(chain)
            parent_idx += 1
            if len(group) >= batch_size:
                last_drained = group[-1]
                _drain_group(group)
                group = []
                if countable >= max_records:
                    truncated = True
                    parked_chain_out = last_drained
                    break
        else:
            if group:
                _drain_group(group)
        if seeking:
            # See the plain walk above: a never-found anchor must reset via a
            # TRUNCATED no-park offset (positional restart, floor kept) — a
            # clean completion would fold running_max past unwalked subtrees.
            _LOG.warning(
                "Parked resume position %r was not re-found while enumerating "
                "%r; this batch emitted nothing and reset the walk — the next "
                "batch re-walks from the committed cursor floor "
                "(duplicate-safe).",
                parked_chain,
                segments,
            )
            truncated = True
            parent_idx = 0
            parked_chain_out = None
        return (emitted, truncated, parent_idx, parked_chain_out, None, None)

    def _no_progress_cursor_error(
        self, table_name: str, cursor_field: str, n_emitted: int
    ) -> RuntimeError:
        """Build the RuntimeError the caller raises when a cursor-mode
        batch emitted rows but the offset did not advance. Two causes
        share this symptom: every row's cursor is null (so
        ``running_max`` can't update), or the source returned rows whose
        cursor equals the prior ``since`` (server did not honor
        ``cursor gt``). Committing the rows would loop forever — the
        framework re-issues the same offset; dropping them silently
        would lose data. The caller raises this error so the operator
        sees the cause."""
        return RuntimeError(
            f"emitted {n_emitted} rows from {table_name!r} but cursor_field="
            f"{cursor_field!r} did not advance. Either every row in this "
            f"batch has a null {cursor_field}, or the source returned rows "
            f"whose {cursor_field} equals the prior offset (server did not "
            f"honor `{cursor_field} gt <since>`). Fix the cursor at the "
            f"source (non-nullable, strictly monotonic), exclude offending "
            f"rows with `filter`/`filters_at` (or drop null-cursor "
            f"rows entirely with cursor_nulls=ignore), or pick a different "
            f"cursor."
        )

    def _arm_lookback_dedup(self, start_offset: dict | None, table_name: str) -> None:
        """Arm (or disarm) the streaming dedup filter for THIS cursor read.

        Called by each contained cursor entry right after it resolves
        ``_active_lookback_seconds``; the walks consult the armed filter at
        their emit points so suppressed overlap rows never join the batch
        buffer, and ``_apply_lookback_dedup`` consumes it in finalize.
        Always assigns — a read that doesn't qualify (dedup off, window
        inactive, batch-reader ``start_offset=None``) disarms, so a filter
        can never leak across reads on a shared connector instance."""
        flt = None
        if (
            getattr(self, "_lookback_dedup_cap", 0)
            and getattr(self, "_active_lookback_seconds", 0) > 0
            and start_offset is not None
        ):
            prior = (start_offset or {}).get("lb_seen") or {}
            if not isinstance(prior, dict):
                # The checkpoint is user-visible state (same discipline as
                # the ``lb_history`` validation): corrupt ``lb_seen``
                # degrades to "nothing seen" — re-emits, never a crash.
                prior = {}
            pks = self._primary_keys_for(table_name, getattr(self, "_lookback_dedup_ns", None))
            flt = _LookbackDedupFilter(prior, pks)
        self._lb_dedup_filter = flt

    def _apply_lookback_dedup(
        self,
        start_offset: dict | None,
        end_offset: dict,
        emitted: list[dict],
        table_name: str,
        cursor_field: str,
    ) -> tuple[list[dict], dict]:
        """Finalize step of ``cursor_lookback_dedup`` (ON by default —
        ``off`` restores the blind re-emit): consume the streaming filter
        armed by ``_arm_lookback_dedup``, fold its carried entries with
        entries computed from the FINAL (post-trim) buffer, cap-evict, and
        attach ``lb_seen`` to the offset.

        With a lookback window active, every row still inside the window
        is re-fetched on every batch and re-emitted - correct
        (MERGE-idempotent) but write-amplifying at the destination — and,
        pre-filtering, memory-amplifying here: suppression now happens AT
        EMIT TIME (see ``_LookbackDedupFilter``), so peak memory scales
        with rows actually delivered plus ~100 bytes per suppressed entry,
        not with the window's churn. When enabled, the offset carries
        ``lb_seen``: an EXACT map of
        ``{composite-PK key: [cursor, content-hash]}`` for the rows
        delivered from the current window. A fetched row whose key AND
        full content hash match its entry has already been delivered
        unchanged - it is dropped from the batch. Hashing the whole row
        (not just PK+cursor) means a source that updates columns WITHOUT
        advancing the cursor still gets its in-window change delivered,
        exactly as the blind re-emit would have.

        The set is REBUILT each batch from the rows actually fetched:
        every in-window row is re-fetched every batch by definition, so
        the current fetch IS the next window's candidate population - no
        window arithmetic, aged-out and deleted rows drop out naturally.
        (A capped mid-cycle resume batch fetches only its continuation
        slice, shrinking the set to it - later re-emits, never loss.)
        Above the entry cap, the highest-cursor entries are kept (they
        stay in the window longest) and the remainder degrade to plain
        re-emits - the pre-dedup behavior. Every failure direction is
        re-emit, never suppression of an undelivered row, which is why
        the set must be exact and a probabilistic structure is ruled
        out. Suppressed rows always sit at-or-below the committed
        watermark (a changed row hashes differently), so suppression can
        never affect watermark progression, the batch cap (overlap rows
        are already excluded from it), or the no-progress guard
        (``lb_seen`` is ``lb_``-prefixed and stripped by
        ``offset_progress_view``)."""
        cap = getattr(self, "_lookback_dedup_cap", 0)
        flt = getattr(self, "_lb_dedup_filter", None)
        self._lb_dedup_filter = None  # consume once; never leaks across reads
        if not cap or getattr(self, "_active_lookback_seconds", 0) <= 0:
            return emitted, end_offset
        if flt is not None:
            # Streaming mode (the normal path): suppressed rows were dropped
            # at emit time — they never materialized in ``emitted`` — and
            # their entries were carried on the filter. Entries for the
            # DELIVERED rows are computed HERE, from the FINAL buffer (post
            # boundary-trim), so an admitted-then-trimmed row can never
            # leave an entry that would suppress its future re-fetch.
            if not emitted and not flt.carried:
                return emitted, end_offset
            kept = emitted
            new_seen: dict[str, list] = dict(flt.carried)
            for row in emitted:
                cur = row.get(cursor_field)
                new_seen[_lb_row_key(row, flt.pks)] = [
                    None if cur is None else str(cur),
                    _lb_row_digest(row),
                ]
        else:
            # Post-hoc fallback for a cursor path that reached finalize with
            # the window active but never armed the filter (none today; the
            # failure direction of NOT arming is plain re-emits, never loss).
            if not emitted:
                return emitted, end_offset
            prior = (start_offset or {}).get("lb_seen") or {}
            if not isinstance(prior, dict):
                # Corrupt checkpoint state degrades to "nothing seen" —
                # re-emits, never a crash (``lb_history`` discipline).
                prior = {}
            pks = self._primary_keys_for(table_name, getattr(self, "_lookback_dedup_ns", None))
            new_seen = {}
            kept = []
            for row in emitted:
                key = _lb_row_key(row, pks)
                digest = _lb_row_digest(row)
                entry = prior.get(key)
                # Shape check mirrors the ``prior`` guard: a corrupt entry
                # never matches — the row re-emits, entry rebuilt well-formed.
                if isinstance(entry, (list, tuple)) and len(entry) == 2 and entry[1] == digest:
                    new_seen[key] = entry
                    continue
                kept.append(row)
                cur = row.get(cursor_field)
                new_seen[key] = [None if cur is None else str(cur), digest]
        if len(new_seen) > cap:
            if not getattr(self, "_warned_lb_dedup_cap", False):
                _LOG.warning(
                    "cursor_lookback_dedup: window population (%s rows) exceeds "
                    "the entry cap (%s) on %r - keeping the newest %s and "
                    "re-emitting the rest (MERGE-idempotent; raise the cap or "
                    "shrink the lookback window to restore full dedup).",
                    len(new_seen),
                    cap,
                    table_name,
                    cap,
                )
                self._warned_lb_dedup_cap = True
            # The PK-key tiebreak makes eviction DETERMINISTIC across
            # batches. Broken by fetch order instead, cursor ties on an
            # order-unstable server flap the surviving set, and every flap
            # is an ``lb_seen`` delta that re-emits the newly evicted rows
            # on each trigger of an otherwise-quiescent source.
            ordered = sorted(
                new_seen.items(),
                key=lambda kv: (_lb_seen_order_key(kv[1][0]), kv[0]),
                reverse=True,
            )
            new_seen = dict(ordered[:cap])
        out = dict(end_offset)
        out["lb_seen"] = new_seen
        return kept, out

    def _finalize_cursor_read(
        self,
        start_offset: dict | None,
        end_offset: dict,
        emitted: list[dict],
        table_name: str,
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Apply the no-progress guard shared by every cursor-mode read
        path. Returns ``(iter(emitted), end_offset)`` on the happy path;
        raises when rows were emitted but the offset did not advance;
        returns ``(iter([]), start_offset)`` when nothing was emitted on
        a no-progress batch (terminal/empty). ``start_offset is None``
        is the batch-reader signal (``LakeflowBatchReader`` passes
        ``None`` and discards the returned offset) — no-progress can't
        loop in that mode, so the guard is skipped and rows are emitted
        as-is. Streaming first batch passes ``{}`` (see
        ``LakeflowStreamReader.initialOffset``); the plain ``==`` then
        catches both ``{}`` and populated equal-offsets. See
        ``_no_progress_cursor_error`` for the two causes that land in
        the raise branch."""
        if start_offset is None:
            return iter(emitted), end_offset
        emitted, end_offset = self._apply_lookback_dedup(
            start_offset, end_offset, emitted, table_name, cursor_field
        )

        # Compare progress on the cursor/continuation state only — see
        # ``offset_progress_view`` (shared with ``_attach_lookback_state``'s
        # quiescence check so the two stripped-key sets can never drift):
        # ``lb_*`` bookkeeping and the one-time capability flags are not
        # progress, and a batch that merely bakes in a flag must not bypass
        # the no-progress guard.
        if offset_progress_view(start_offset) == offset_progress_view(end_offset):
            if emitted:
                # With cursor_lookback the read floor lags the committed
                # watermark by the overlap window, so a quiescent trigger
                # re-reads the trailing overlap rows (cursor <= committed)
                # without the watermark advancing. That is expected, not a
                # stall: a row with cursor > committed (forward progress)
                # would have advanced end_offset and skipped this branch. So
                # idle — suppress the overlap re-reads (idempotent under
                # apply_changes MERGE anyway) rather than raising. The
                # late-arriver rows the overlap exists to catch are emitted on
                # the next PROGRESSING batch, when end_offset advances past
                # the prior watermark.
                if getattr(self, "_active_lookback_seconds", 0) > 0:
                    # With dedup active, the surviving rows are GENUINELY new
                    # content (changed hash or first delivery) — the blind
                    # re-reads were already filtered out — and the ``lb_seen``
                    # delta gives the framework real offset progress, so the
                    # next batch suppresses these rows instead of looping.
                    # A surviving row WITHOUT an lb_seen delta is a
                    # cap-evicted re-read: keep today's idle for it (it
                    # re-emits on the next progressing batch), otherwise an
                    # unchanged offset + rows would replay every trigger.
                    if (end_offset.get("lb_seen") or {}) != (
                        (start_offset or {}).get("lb_seen") or {}
                    ) and "lb_seen" in end_offset:
                        return iter(emitted), end_offset
                    return iter([]), start_offset
                raise self._no_progress_cursor_error(table_name, cursor_field, len(emitted))
            return iter([]), start_offset
        return iter(emitted), end_offset

    def _read_contained_incremental(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Walk every parent tuple with ``$filter=cursor gt since``; track
        global max cursor in the offset. Truncation parks ``parent_idx``
        for next-call resume. When the leaf entity doesn't declare
        ``cursor_field``, the closest ancestor that does owns the filter
        and its cursor value is propagated onto each leaf row."""
        segments = self._table_segments(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        cursor_level = self._find_cursor_level(segments, namespace, cursor_field)
        if cursor_level == -1:
            raise ValueError(
                f"cursor_field {cursor_field!r} is not a property on "
                f"{table_name!r} or any of its ancestors. Pick a column "
                f"declared on the leaf or one of the parent segments."
            )
        mode = self._cursor_probe_mode(table_options)  # auto | probe | batch | off
        explicit = "cursor_probe" in (table_options or {})
        is_leaf_cursor = cursor_level == len(segments) - 1
        probe_applicable = is_leaf_cursor and self._cursor_probe_applicable(
            segments, namespace, cursor_field, cursor_level
        )
        # Strict misconfig raises apply only to an EXPLICIT opt-in that names a
        # strategy this path structurally can't run. ``auto`` (the default) and
        # ``off`` never raise — they degrade to a correct fallback.
        if explicit and mode == "probe" and not probe_applicable:
            if not is_leaf_cursor:
                raise ValueError(
                    "cursor_probe=nested-expand requires cursor_field on the leaf "
                    f"segment (it lives on ancestor segment {segments[cursor_level]!r} "
                    f"of {table_name!r}). cursor_probe only accelerates leaf-owned "
                    "cursor reads; an ancestor cursor already filters whole "
                    "subtrees, so drop cursor_probe for this table."
                )
            raise ValueError(
                f"cursor_probe=nested-expand won't help on {table_name!r}: its leaf-parent "
                f"collection {segments[-2]!r} is a batch-snapshot level (it does "
                f"not declare {cursor_field!r}), so the distance from the leaf to "
                "the nearest snapshot ancestor is 1 — every leaf-parent is fetched "
                "anyway and there are no clean ones to skip. cursor_probe pays off "
                "only when the leaf's parent is itself an incremental, "
                "high-cardinality collection. Drop cursor_probe here."
            )
        if explicit and mode == "batch" and not is_leaf_cursor:
            raise ValueError(
                f"cursor_probe=batch only accelerates leaf-owned cursor reads, but "
                f"{cursor_field!r} lives on ancestor segment {segments[cursor_level]!r} "
                f"of {table_name!r} — an ancestor cursor already filters whole "
                "subtrees. Drop cursor_probe for this table."
            )
        if start_offset is None:
            # Batch reader: offset discarded, ``since`` is None (no cursor
            # filter), no cap, no no-progress guard — so the ``emitted``
            # list, watermark and truncation checkpoint the streaming
            # walks build all serve nothing. Stream leaf rows one page at
            # a time so an uncapped batch doesn't materialise the whole
            # result set. See ``read_table`` for why the cap is disabled.
            return (
                self._stream_contained_incremental(
                    table_name, segments, namespace, table_options, cursor_field, cursor_level
                ),
                {},
            )
        if cursor_level == len(segments) - 1:
            # Overlap re-read window for the (non-atomic) leaf-cursor walk: the
            # probe and the plain N+1 walk have the same mid-walk-arrival gap as
            # expand mode (a leaf inserted under an already-passed / probed-clean
            # parent lands below the committed max and is skipped forever by the
            # next ``cursor gt``). Floor the READ filter to ``committed - window``
            # while still committing the true max. Resolved here (the leaf
            # branch) so it never bleeds into the ancestor-cursor path. See
            # ``_read_contained_incremental_leaf_cursor`` for the read-side use.
            self._active_lookback_seconds = self._resolve_active_lookback(start_offset)
            self._arm_lookback_dedup(start_offset, table_name)
            read_since = self._apply_cursor_lookback((start_offset or {}).get("cursor"))
            # Engage the probe only where it pays off (``probe_applicable``):
            # the leaf-parent must itself be a cursor-bearing collection, so
            # there are clean leaf-parents to skip. A snapshot leaf-parent
            # (e.g. .../Projects/WorkPackageDetails) is enumerated in full
            # either way, so default-on cursor_probe stays inert there.
            chains_iter = None
            persist_probe_ok = False
            use_batch = False
            persist_batch_ok = False
            if mode in ("probe", "auto") and probe_applicable:
                # Capability-verify the nested-$expand probe. ``probe`` (explicit
                # ``true``) is STRICT — ``_verify_cursor_probe_support`` raises if
                # the server mis-orders inner $expand. ``auto`` is non-strict — a
                # mis-ordering verdict returns ``supported=False`` so we cascade
                # to $batch below instead of failing the read. A conclusive pass
                # (or a flag a prior batch persisted) lets a per-batch-recreated
                # reader skip the preflight next time.
                supported, conclusive = self._verify_cursor_probe_support(
                    segments,
                    namespace,
                    table_options,
                    cursor_field,
                    start_offset,
                    strict=(mode == "probe"),
                )
                if supported:
                    persist_probe_ok = conclusive or bool(
                        (start_offset or {}).get("cursor_probe_ok")
                    )
                    # The probe prunes nothing until a watermark exists: with
                    # ``read_since`` None (first batch) every leaf-parent reads as
                    # dirty, so the per-grandparent probe round-trips would only
                    # add overhead with nothing to skip. Fall back to the plain
                    # enumerator then — identical rows, fewer requests — and
                    # engage the probe once a watermark is established.
                    if read_since is not None:
                        chains_iter = self._iter_dirty_leaf_parent_chains(
                            segments,
                            namespace,
                            table_options,
                            cursor_field,
                            read_since,
                        )
            # Hydrate via $batch when the server supports it — for the probe's
            # pruned dirty chains (``probe``/``auto`` with ``used_probe``) AND for
            # the ``auto``/``batch`` cascade alike. The probe and $batch are
            # complementary, not exclusive: the probe (nested-$expand) prunes
            # WHICH leaf-parents to read, $batch batches the hydrate of whichever
            # remain. ``_verify_batch_support`` is fail-closed, so an unsupported
            # server leaves ``use_batch`` False and the SAME chains fall through to
            # the plain N+1 walk. ``off`` (force plain walk) never batches.
            #
            # An explicit ``contained_fetch=single`` / ``1`` suppresses the $batch
            # hydrate everywhere on this path — the probe's dirty-parent hydrate
            # AND ``auto``'s no-probe cascade go down the plain N+1 walk (the
            # preflight is skipped entirely). The one exception is an explicit
            # ``cursor_probe=batch``: that is a direct demand for the $batch
            # hydrate on the incremental read, so it wins the conflict.
            forces_single = self._contained_fetch_forces_single(table_options)
            explicit_batch = explicit and mode == "batch"
            if mode in ("probe", "auto", "batch") and (explicit_batch or not forces_single):
                if self._verify_batch_support(segments, table_options, start_offset):
                    use_batch = True
                    persist_batch_ok = True
            return self._read_contained_incremental_leaf_cursor(
                table_name,
                segments,
                start_offset,
                table_options,
                cursor_field,
                chains_iter=chains_iter,
                persist_probe_ok=persist_probe_ok,
                use_batch=use_batch,
                persist_batch_ok=persist_batch_ok,
            )
        return self._read_contained_incremental_ancestor_cursor(
            table_name, segments, start_offset, table_options, cursor_field, cursor_level
        )

    def _stream_contained_incremental(
        self,
        table_name: str,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str],
        cursor_field: str,
        cursor_level: int,
    ) -> Iterator[dict]:
        """Lazy batch-mode contained cursor read (leaf- or ancestor-cursor).

        Mirrors the per-row work of ``_read_contained_incremental_*``
        minus everything the batch reader makes moot (``since`` is None,
        offset discarded, cap disabled, guard skipped): no cursor
        ``$filter``, no ``emitted`` buffer, no watermark, no truncation
        checkpoint. Leaf rows stream one page at a time. The cursor lives
        on the leaf (``leaf`` branch — apply ``cursor_nulls=ignore``
        null-skip; ``coalesce`` keeps the real null since nothing consumes
        the synthetic value) or on a non-leaf ancestor (``ancestor``
        branch — stamp the ancestor's cursor value onto each leaf row,
        exactly as ``_walk_ancestor_chains`` does)."""
        fk_columns = self._resolve_fk_columns(segments, namespace)
        segment_filters = resolve_segment_filters(table_options, segments)
        leaf_filter = segment_filters.get(len(segments) - 1)
        batch_size = self._contained_fetch_batch_n(segments, table_options)
        if cursor_level == len(segments) - 1:
            order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
            skip_null, _effective = self._make_cursor_resolver(
                table_name, namespace, cursor_field, table_options
            )
            chain_meta = (
                (chain, chain)
                for chain in self._iter_parent_key_chains(segments, namespace, table_options)
            )
            for chain, row in self._iter_contained_leaf_rows(
                segments, chain_meta, table_options, leaf_filter, order_by, batch_size
            ):
                if skip_null and row.get(cursor_field) is None:
                    continue
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                yield row
            return
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)
        chains_iter = self._iter_parent_chains_with_cursor(
            segments, namespace, table_options, cursor_level, cursor_field, None
        )
        # meta = (chain, ancestor_cursor): chain for FK tagging, cursor to stamp.
        chain_meta = ((chain, (chain, ac)) for chain, ac in chains_iter)
        for (chain, ancestor_cursor), row in self._iter_contained_leaf_rows(
            segments, chain_meta, table_options, leaf_filter, leaf_order_by, batch_size
        ):
            self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
            row[cursor_field] = ancestor_cursor
            yield row

    def _read_contained_incremental_leaf_cursor(
        self,
        table_name: str,
        segments: list[str],
        start_offset: dict | None,
        table_options: dict[str, str],
        cursor_field: str,
        chains_iter: Iterator[list[dict[str, Any]]] | None = None,
        persist_probe_ok: bool = False,
        use_batch: bool = False,
        persist_batch_ok: bool = False,
    ) -> tuple[Iterator[dict], dict]:
        """Cursor lives on the leaf entity — filter at the leaf fetch.

        ``use_batch`` hydrates via :meth:`_batch_walk_contained_with_cursor`
        (OData ``$batch``, chunk-aligned resume) instead of the per-parent
        :meth:`_walk_contained_with_cursor`; ``persist_batch_ok`` stamps the
        ``batch_ok`` capability flag into the resume offset (mirrors
        ``persist_probe_ok`` / ``cursor_probe_ok``).

        ``chains_iter`` lets a caller substitute the parent-key source
        (default: every chain via :meth:`_iter_parent_key_chains`). The
        ``cursor_probe`` path passes :meth:`_iter_dirty_leaf_parent_chains`
        — the same chains pruned to parents with changed leaves — so the
        flat ``parent_idx`` resume, watermark and no-progress guard below
        all work unchanged over the reduced set.

        ``_walk_contained_with_cursor`` chooses the truncation
        checkpoint (and trims ``emitted`` to match); this method only
        serialises it into the resume offset. The checkpoint is scoped
        to the truncated chain — subsequent chains keep the original
        ``since`` since per-chain cursor distributions are independent:

        * **NextLink (preferred)**: truncation on a page boundary parks
          the server's @odata.nextLink as ``chain_next_link``; the
          resumed call hands it straight back to the server.
        * **Trim boundary**: a *complete* parent (no nextLink) with a
          distinct-cursor boundary drops its trailing same-cursor cohort
          and parks ``truncated_chain_cursor``; the resumed call rebuilds
          ``cursor gt truncated_chain_cursor`` for that chain only.

        A complete parent whose entire leaf collection shares one cursor
        value has no splittable boundary; the walk emits it in full and
        continues to the next parent (the cap is overshot for that one
        parent), so there is no failure case here.
        """
        namespace = (table_options or {}).get("namespace")
        since = (start_offset or {}).get("cursor")
        # Overlap re-read floor (see ``_read_contained_incremental`` dispatch):
        # the per-chain ``cursor gt`` filters and the in-walk client trim use
        # ``read_since`` (= committed − window) so a non-atomic walk re-scans
        # the overlap; the committed offset below stays the TRUE ``since``/max.
        # ``_active_lookback_seconds`` was resolved on ``self`` by the dispatch
        # (0 for a non-lookback read → ``read_since`` is ``since`` unchanged).
        read_since = self._apply_cursor_lookback(since)
        truncated_chain_cursor_in = (start_offset or {}).get("truncated_chain_cursor")
        chain_next_link_in = (start_offset or {}).get("chain_next_link")
        # Key-based resume position (churn-stable); legacy offsets carry only
        # ``parent_idx`` and fall back to the positional skip inside the walks.
        parked_chain_in = (start_offset or {}).get("parent_keys")
        max_records = _parse_max_records(table_options)
        order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
        if chains_iter is None:
            chains_iter = self._iter_parent_key_chains(segments, namespace, table_options)
        segment_filters = resolve_segment_filters(table_options, segments)
        # ``cursor_nulls`` resolver (synthetic floor for nulls under
        # coalesce; skip nulls under ignore). The cursor lives on the leaf
        # entity, so PKs/floor come from the full contained path's leaf.
        skip_null, effective = self._make_cursor_resolver(
            table_name, namespace, cursor_field, table_options
        )
        # Wall-clock around the walk feeds the ``auto`` lookback window
        # (see ``_attach_lookback_state``), exactly as the expand path does.
        walk_start = time.monotonic()
        if use_batch:
            (
                emitted,
                truncated,
                parent_idx,
                parent_keys_out,
                chain_next_link_out,
                truncated_chain_cursor_out,
            ) = self._batch_walk_contained_with_cursor(
                segments,
                chains_iter,
                int((start_offset or {}).get("parent_idx", 0)),
                table_options,
                order_by,
                cursor_field,
                read_since,
                max_records,
                self._resolve_fk_columns(segments, namespace),
                leaf_segment_filter=segment_filters.get(len(segments) - 1),
                effective=effective,
                skip_null=skip_null,
                batch_size=self._cursor_probe_batch_size(table_options),
                parked_chain=parked_chain_in,
                # A serial-walk park (continuation keys present) resumes AT
                # the parked chain; the batch walk re-drains it in full.
                resume_inclusive=(
                    chain_next_link_in is not None or truncated_chain_cursor_in is not None
                ),
                count_floor=since,
            )
        else:
            (
                emitted,
                truncated,
                parent_idx,
                parent_keys_out,
                chain_next_link_out,
                truncated_chain_cursor_out,
            ) = self._walk_contained_with_cursor(
                segments,
                chains_iter,
                int((start_offset or {}).get("parent_idx", 0)),
                table_options,
                order_by,
                cursor_field,
                read_since,
                truncated_chain_cursor_in,
                chain_next_link_in,
                max_records,
                self._resolve_fk_columns(segments, namespace),
                leaf_segment_filter=segment_filters.get(len(segments) - 1),
                effective=effective,
                skip_null=skip_null,
                parked_chain=parked_chain_in,
                count_floor=since,
            )
        walk_elapsed = time.monotonic() - walk_start
        if truncated:
            # The walk has already chosen the checkpoint and trimmed
            # ``emitted`` accordingly: ``chain_next_link_out`` for a page
            # boundary, else ``truncated_chain_cursor_out`` for a complete
            # parent with a distinct-cursor boundary. (A complete parent
            # with a single cursor value never truncates — the walk emits
            # it in full and continues — so there's no failure case here.)
            # ``parent_idx`` rides along for downgrade compatibility; the
            # resume itself positions on ``parent_keys`` (churn-stable).
            end_offset: dict = {"parent_idx": parent_idx}
            if parent_keys_out is not None:
                end_offset["parent_keys"] = parent_keys_out
            # The ``$batch`` walk's park is chunk-aligned and EXCLUSIVE (the
            # parked chain is fully drained) — it never parks a
            # mid-collection checkpoint, so its truncation offset carries
            # neither continuation key.
            if not use_batch:
                if chain_next_link_out is not None:
                    end_offset["chain_next_link"] = chain_next_link_out
                elif truncated_chain_cursor_out is not None:
                    # None/None with truncated=True is the vanished-anchor
                    # RESET (positional restart at 0, no park) — stamping an
                    # explicit null key would be inert but misleading.
                    end_offset["truncated_chain_cursor"] = truncated_chain_cursor_out
            if since is not None:
                end_offset["cursor"] = since
            # Accumulate the max cursor seen across the truncated cycle's
            # batches (mirrors ``_ancestor_cursor_offset``): the committed
            # ``cursor`` must stay at ``since`` while in flight, but WITHOUT
            # this a resume that completes EMPTY would clear the checkpoint
            # back to ``{"cursor": since}`` and lose every truncated batch's
            # progress — a permanent period-2 duplicate loop on a static
            # source whose new rows fit exactly in one capped batch.
            batch_cursors = [effective(r) for r in emitted if effective(r) is not None]
            running_max = _max_or(
                _cursor_max(batch_cursors) if batch_cursors else None,
                (start_offset or {}).get("running_max"),
            )
            if running_max is not None:
                end_offset["running_max"] = running_max
        else:
            if not emitted:
                empty = start_offset or {}
                # A resumed TRUNCATED walk that completes with nothing more to
                # emit must CLEAR its parked checkpoint: echoing the offset back
                # unchanged would freeze the walk at ``parent_idx`` forever —
                # every later batch skips the first N parents, emits nothing,
                # and returns the same offset again, silently dropping future
                # changes under the skipped parents. Deterministic for the
                # ``$batch`` walk when the cap fired exactly on its final chunk
                # (``parent_idx`` == total chain count, so the resume has no
                # re-entry work); reachable for the plain walk when the
                # checkpointed rows vanish between batches. Dropping the
                # checkpoint (keeping ``cursor`` and the bookkeeping keys)
                # marks the walk complete so the next batch starts fresh.
                checkpoint_keys = (
                    "parent_idx",
                    "parent_keys",
                    "parent_cursor",
                    "chain_next_link",
                    "truncated_chain_cursor",
                    "running_max",
                    # Foreign park keys from an ``expand_contained`` read of
                    # the same table (mode flipped off mid-park): a stale
                    # queue / running max must not ride every future N+1
                    # offset — its watermark is folded below like our own.
                    "pending_fetches",
                    "running_max_cursor",
                    # The capped cycle is OVER (this completion ends it), so
                    # the auto-lookback span anchor must not survive: this
                    # early return bypasses _attach_lookback_state, and a
                    # leaked anchor makes the NEXT progressing walk record
                    # the whole idle gap as a "cycle span" — lb_history then
                    # carries a bogus multi-hour entry and the window pins
                    # at the ceiling for the next 5 walks (an hour of
                    # overlap re-read per batch; duplicates, never loss).
                    # The measurement itself is deliberately dropped —
                    # a vanished-checkpoint completion is idle-shaped, not
                    # a real walk worth sizing the window from.
                    "lb_cycle_started",
                )
                if any(k in empty for k in checkpoint_keys):
                    # Fold the cycle's accumulated max into the committed
                    # cursor BEFORE clearing — the truncated batches' rows
                    # were emitted under it, and dropping it re-reads them
                    # forever (period-2 duplicate loop).
                    committed = _max_or(
                        _max_or(empty.get("running_max"), empty.get("running_max_cursor")),
                        empty.get("cursor"),
                    )
                    empty = {k: v for k, v in empty.items() if k not in checkpoint_keys}
                    if committed is not None:
                        empty["cursor"] = committed
                if persist_probe_ok:
                    empty = self._with_probe_ok(empty)
                if persist_batch_ok:
                    empty = self._with_batch_ok(empty)
                return iter([]), empty
            cursors = [effective(r) for r in emitted if effective(r) is not None]
            # Mirror ``_build_expand_end_offset`` /
            # ``_ancestor_cursor_offset``: when there's no cursor data this
            # batch and no prior ``since`` to carry, the offset is ``{}`` —
            # not ``{"cursor": None}`` (see ``_cursor_max_end_offset``).
            end_offset = self._cursor_max_end_offset(cursors, since)
            # Completing a previously-truncated cycle: fold the accumulated
            # ``running_max`` into the committed cursor (and drop the key —
            # terminal offsets stay clean).
            prior_running = (start_offset or {}).get("running_max")
            if prior_running is not None:
                committed = _max_or(end_offset.get("cursor"), prior_running)
                end_offset = {k: v for k, v in end_offset.items() if k != "cursor"}
                if committed is not None:
                    end_offset["cursor"] = committed
        records, out_offset = self._finalize_cursor_read(
            start_offset, end_offset, emitted, table_name, cursor_field
        )
        # Carry the ``auto`` walk-duration history (no-op for static/off and
        # for the idled overlap re-read). ``truncated`` ⇒ walk in flight, so
        # its partial duration isn't recorded as a completed walk.
        out_offset = self._attach_lookback_state(out_offset, start_offset, truncated, walk_elapsed)
        # Persist the verified cursor_probe capability so a freshly-constructed
        # reader on the next batch can skip the preflight requests. Applied
        # AFTER the no-progress finalize (whose comparison ignores
        # ``cursor_probe_ok`` — see ``_finalize_cursor_read``) so it never reads
        # as false forward progress, and an idled overlap re-read that already
        # carries the flag returns ``start_offset`` unchanged.
        if persist_probe_ok:
            out_offset = self._with_probe_ok(out_offset)
        # Same treatment for the ``$batch`` capability flag (excluded from the
        # no-progress comparison alongside ``cursor_probe_ok``).
        if persist_batch_ok:
            out_offset = self._with_batch_ok(out_offset)
        return records, out_offset

    def _read_contained_incremental_ancestor_cursor(
        self,
        table_name: str,
        segments: list[str],
        start_offset: dict | None,
        table_options: dict[str, str],
        cursor_field: str,
        cursor_level: int,
    ) -> tuple[Iterator[dict], dict]:
        """Cursor lives on a non-leaf ancestor. Filter at that ancestor
        level (changed subtrees only), fetch full leaf collections under
        each filtered ancestor, and stamp the ancestor's cursor value
        onto every emitted leaf row.

        Truncation uses **nextLink-based mid-chain resume** exclusively.
        Every leaf under a chain shares that chain's stamped cursor by
        construction, so a within-chain ``cursor gt`` rebuild would
        either re-fetch the whole chain or skip the whole chain — there
        is no meaningful split.

        The lookback floor applies to the ANCESTOR enumeration filter
        (``cursor gt <since - window>``): re-enumerating recently-dirty
        ancestors re-reads their whole subtrees, which is exactly the
        duplicate-safe recovery this walk needs — a parent updated
        mid-cycle whose cursor lands below the cycle's final
        ``running_max`` is otherwise excluded by ``cursor gt
        <watermark>`` forever. The committed watermark is never floored
        (``_ancestor_cursor_offset`` folds true stamped maxes), and the
        floor stays stable across a capped cycle's batches because the
        ``auto`` history is carried unchanged while in-flight.
        """
        namespace = (table_options or {}).get("namespace")
        since = (start_offset or {}).get("cursor")
        self._active_lookback_seconds = self._resolve_active_lookback(start_offset)
        self._arm_lookback_dedup(start_offset, table_name)
        read_since = self._apply_cursor_lookback(since)
        walk_start = time.monotonic()
        chains_iter = self._iter_parent_chains_with_cursor(
            segments, namespace, table_options, cursor_level, cursor_field, read_since
        )
        segment_filters = resolve_segment_filters(table_options, segments)
        walk_state = self._walk_ancestor_chains(
            segments,
            chains_iter,
            table_options,
            cursor_field,
            int((start_offset or {}).get("parent_idx", 0)),
            (start_offset or {}).get("chain_next_link"),
            _parse_max_records(table_options),
            self._resolve_fk_columns(segments, namespace),
            leaf_segment_filter=segment_filters.get(len(segments) - 1),
            parked_chain=(start_offset or {}).get("parent_keys"),
            parked_cursor=(start_offset or {}).get("parent_cursor"),
            cursor_level=cursor_level,
            # New-rows-only cap accounting keys on the COMMITTED watermark,
            # never the floored read filter — overlap re-reads don't burn cap.
            count_floor=since,
        )
        walk_elapsed = time.monotonic() - walk_start
        end_offset = self._ancestor_cursor_offset(walk_state, start_offset, since, cursor_field)
        records, out_offset = self._finalize_cursor_read(
            start_offset, end_offset, walk_state["emitted"], table_name, cursor_field
        )
        return records, self._attach_lookback_state(
            out_offset, start_offset, walk_state["truncated"], walk_elapsed
        )

    def _walk_ancestor_chains(
        self,
        segments: list[str],
        chains_iter: Iterator[tuple[list[dict[str, Any]], Any]],
        table_options: dict[str, str],
        cursor_field: str,
        parent_idx_start: int,
        chain_next_link_in: str | None,
        max_records: int,
        fk_columns: dict[tuple[int, str], str],
        leaf_segment_filter: str | None = None,
        parked_chain: list | None = None,
        parked_cursor: Any = None,
        cursor_level: int = 0,
        count_floor: Any = None,
    ) -> dict[str, Any]:
        """Walk ancestor chains, fetching each chain's leaf collection
        and stamping rows with the chain's cursor.

        ``chains_iter`` is consumed lazily: the per-ancestor enumeration
        stops as soon as the loop breaks on a ``max_records`` hit, so
        we never fetch ancestor pages beyond the chain we actually
        emit from. Peak memory is bounded to one chain.

        Page-aware: a truncation at a page boundary parks the chain's
        ``@odata.nextLink``; when the chain happens to end on the
        truncating page the park is EXCLUSIVE (no link — the chain is
        complete and the resume skips through it).

        Resume positioning is key-based (``parked_chain`` +
        ``parked_cursor``, matching the enumeration's nested ordering with
        the cursor term at ``cursor_level``'s position — see
        :func:`_chain_resume_key`): this enumeration is ordered by a
        MUTABLE cursor column and filtered by ``cursor gt since``, so
        positional resume desynchronizes under any churn — updates
        included, not just inserts/deletes (see
        :func:`_chain_strictly_before`). A parked parent whose cursor
        advanced between batches re-enters at its new position and is
        re-walked in full with a fresh stamp (duplicate-safe; its old
        mid-page link is correctly dropped). Legacy index-only offsets
        fall back to the positional skip. ``count_floor`` — see
        ``_walk_contained_with_cursor``: only rows stamped strictly above
        the committed watermark count toward the cap."""
        namespace = (table_options or {}).get("namespace")
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)
        parent_idx = 0
        emitted: list[dict] = []
        truncated = False
        countable = 0
        lb_filter = getattr(self, "_lb_dedup_filter", None)
        chain_next_link_out: str | None = None
        parked_chain_out: list | None = None
        parked_cursor_out: Any = None
        parked_key = (
            _chain_resume_key(parked_chain, parked_cursor, cursor_level)
            if parked_chain is not None
            else None
        )
        seeking = parked_key is not None
        # Same typed-seek requirement as the leaf-cursor walk below.
        leaf_types = self._edm_types_for_level(
            segments, len(segments) - 1, (table_options or {}).get("namespace")
        )
        for chain, ancestor_cursor in chains_iter:
            # Skip already-emitted chains — key-based when parked
            # (churn-stable), positional for legacy offsets. Ancestor-page
            # HTTP cost is unavoidable (we need the keys to identify the
            # chain), but no leaf fetches happen during the skip.
            use_link = False
            if parked_key is not None:
                if seeking:
                    # Park identity is PK **and** cursor INSTANT: a parked
                    # parent whose cursor advanced between batches is NOT "the
                    # parked chain" any more — it re-enumerates at its new
                    # (later) position and must be re-walked in full there,
                    # because the server is saying its subtree changed since
                    # we drained it. PK-only matching would skip it (exclusive
                    # park) or resume its STALE mid-page link (link park), and
                    # either way this batch's ``running_max`` then commits
                    # past its new cursor, so ``cursor gt <watermark>`` locks
                    # the update out forever. Same-INSTANT rendering changes
                    # (``…00Z`` vs ``…00.000Z`` — a mixed-version load
                    # balancer can alternate them PER REQUEST) still count as
                    # parked: the parent didn't change, so resuming its link
                    # is safe — and treating every text mismatch as a change
                    # would re-walk from page 1 each batch for as long as the
                    # alternation lasts, a livelock the no-progress guard
                    # can't see (the offset text alternates, reading as
                    # progress).
                    pk_match = chain == parked_chain
                    at_parked = pk_match and _cursor_same_instant(ancestor_cursor, parked_cursor)
                    # A PK-matched chain must NEVER take the strictly-before
                    # skip: when its cursor genuinely changed AND sorts before
                    # the parked key (a regression), the generic skip would
                    # drop the chain — and its parked link — losing the
                    # collection's undrained remainder while running_max
                    # commits past it. Ending the seek here re-walks it in
                    # full instead: duplicate-safe in BOTH mismatch
                    # directions, as promised above. Non-matched chains take
                    # the three-way seek: "before" is provably drained;
                    # "unknown" (server-collated text keys) keeps seeking on
                    # the PK anchor rather than trusting ordinal order,
                    # which silently skips unwalked subtrees on
                    # CI-collation servers.
                    if not pk_match and (
                        _chain_seek_order(
                            _chain_resume_key(chain, ancestor_cursor, cursor_level), parked_key
                        )
                        != "after"
                    ):
                        parent_idx += 1
                        continue
                    seeking = False
                    if at_parked and chain_next_link_in is None:
                        # Exclusive park: chain completed exactly at the cap.
                        parent_idx += 1
                        continue
                    use_link = at_parked and chain_next_link_in is not None
            else:
                if parent_idx < parent_idx_start:
                    parent_idx += 1
                    continue
                use_link = parent_idx == parent_idx_start and chain_next_link_in is not None
            if use_link:
                initial_url = chain_next_link_in
            else:
                initial_url = self._build_contained_url(
                    segments,
                    chain,
                    table_options,
                    extra_filter=leaf_segment_filter,
                    order_by=leaf_order_by,
                )
            page_next_url: str | None = None
            # See the leaf-cursor walk: the
            # default auto drains a link-omitting, sub-$top-capped leaf via the
            # keyset seek, and the synthesized seek doubles as the cap-hit resume
            # checkpoint.
            for page_rows, page_next_url in self._fetch_pages_with_links(initial_url, leaf_types):
                for row in page_rows:
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    row[cursor_field] = ancestor_cursor
                    if lb_filter is not None and not lb_filter.admit(row):
                        continue  # already delivered unchanged
                    emitted.append(row)
                    if count_floor is None or (
                        ancestor_cursor is not None and _cursor_newer(ancestor_cursor, count_floor)
                    ):
                        countable += 1
                if countable >= max_records:
                    truncated = True
                    break
            if truncated:
                if page_next_url is not None:
                    chain_next_link_out = page_next_url
                else:
                    parent_idx += 1
                parked_chain_out = chain
                parked_cursor_out = ancestor_cursor
                break
            parent_idx += 1
        if seeking:
            # See _walk_contained_with_cursor: a never-found anchor must
            # reset via a TRUNCATED no-park offset (positional restart,
            # floor kept) — a clean completion would fold running_max past
            # unwalked subtrees, locking their sub-max rows out forever.
            _LOG.warning(
                "Parked resume position %r was not re-found while enumerating "
                "%r; this batch emitted nothing and reset the walk — the next "
                "batch re-walks from the committed cursor floor "
                "(duplicate-safe).",
                parked_chain,
                segments,
            )
            truncated = True
            parent_idx = 0
            parked_chain_out = None
            parked_cursor_out = None
            chain_next_link_out = None
        return {
            "emitted": emitted,
            "truncated": truncated,
            "parent_idx": parent_idx,
            "parent_keys": parked_chain_out,
            "parent_cursor": parked_cursor_out,
            "chain_next_link": chain_next_link_out,
        }

    def _ancestor_cursor_offset(
        self,
        walk_state: dict[str, Any],
        start_offset: dict | None,
        since: Any,
        cursor_field: str,
    ) -> dict:
        """Build the offset for the ancestor-cursor read path.

        On truncation: preserve original ``since`` (the chain enumeration
        interleaves cursors across top-level parents, so advancing
        ``since`` to the global max would silently skip lower-cursor
        chains under not-yet-walked parents). Accumulate ``running_max``
        across resume batches so natural completion records the actual
        highest cursor seen — without it, a resume that started from
        ``since=None`` would lose the cursor on completion and re-walk
        the whole table on the next trigger.
        """
        emitted = walk_state["emitted"]
        cursors = [r.get(cursor_field) for r in emitted if r.get(cursor_field) is not None]
        this_batch_max = _cursor_max(cursors) if cursors else None
        prev_running_max = (start_offset or {}).get("running_max")
        new_running_max = _max_or(this_batch_max, prev_running_max)
        if walk_state["truncated"]:
            # ``parent_idx`` rides along for downgrade compatibility; the
            # resume positions on ``parent_keys``/``parent_cursor``
            # (churn-stable — this enumeration is ordered by a mutable
            # cursor column, see ``_walk_ancestor_chains``).
            offset: dict = {"parent_idx": walk_state["parent_idx"]}
            if walk_state["parent_keys"] is not None:
                offset["parent_keys"] = walk_state["parent_keys"]
                offset["parent_cursor"] = walk_state["parent_cursor"]
            if since is not None:
                offset["cursor"] = since
            if walk_state["chain_next_link"] is not None:
                offset["chain_next_link"] = walk_state["chain_next_link"]
            if new_running_max is not None:
                offset["running_max"] = new_running_max
            return offset
        if new_running_max is not None:
            return {"cursor": new_running_max}
        if since is not None:
            return {"cursor": since}
        return {}
