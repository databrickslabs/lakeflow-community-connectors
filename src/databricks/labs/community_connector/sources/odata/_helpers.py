"""Shared helpers used by ``odata.py``, ``_contained.py``, and
``_partition.py``.

These functions live in a separate module so the flat-path, contained-path,
and partition read code can share them without ``_contained``/``_partition``
having to import from ``odata`` (which would close a cycle — ``odata``
already mixes both mixins in at class definition time).
"""

import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse

# Synthetic columns appended to a table's declared schema when the
# server-driven delta-tracking read shape is active. Defined here (not in
# ``odata``) so the partition mixin can exempt them from emit padding
# without importing from ``odata`` (which would close a cycle).
DELETED_COL = "_deleted"
SEQUENCE_COL = "_lc_sequence"

_ISO_FRACTION_RE = re.compile(r"\.(\d+)")


def url_origin(url: str) -> tuple[str, str, int | None]:
    """``(scheme, host, port)`` for same-origin comparison, host lower-cased
    and the default port for the scheme filled in so ``https://h`` and
    ``https://h:443`` compare equal.

    A malformed port raises ``ValueError`` naming the URL — ``urlparse``
    defers port validation to the ``.port`` accessor, so without the wrap
    the bare "Port could not be cast to integer value" message escapes with
    no hint of WHICH url (service_url, a server-supplied @odata.nextLink, a
    $batch sub-response continuation) carried the garbage."""
    p = urlparse(url)
    scheme = (p.scheme or "").lower()
    host = (p.hostname or "").lower()
    try:
        port = p.port
    except ValueError as exc:
        raise ValueError(f"Invalid port in URL {url!r}: {exc}") from None
    if port is None:
        port = {"http": 80, "https": 443}.get(scheme)
    return (scheme, host, port)


def _fraction_digits(value: Any) -> str:
    """The fractional-second digit run of an ISO-rendered string, or ``""``.
    Used by :func:`cursor_newer`'s tie-break — see there for why the digits
    are compared zero-padded rather than as raw text."""
    if isinstance(value, str):
        match = _ISO_FRACTION_RE.search(value)
        if match:
            return match.group(1)
    return ""


def parse_iso8601(value: str) -> datetime:
    """``datetime.fromisoformat`` with version-uniform fraction handling.

    The connector's floor is Python 3.10 (DBR 13.3 LTS), where
    ``fromisoformat`` accepts fractional seconds of EXACTLY 3 or 6 digits —
    while real servers render value-dependent digit counts (Olingo/SAP trim
    trailing zeros → ``.5``; nanosecond servers emit 7+ digits, which even
    3.10 rejects). Left unnormalized, a ``…00.5Z`` watermark on a 3.10
    runtime fails the ISO sniff (→ ``odata_literal`` QUOTES it → wire 400
    every batch) and falls out of the chronological comparisons (→ back to
    the lexical silent-loss ordering those comparisons exist to prevent).
    Normalizing the fraction to exactly 6 digits (pad short, truncate
    long — sub-microsecond precision only affects ordering ties, which are
    duplicate-safe) makes parsing identical on every supported version.
    Also maps the ``Z`` designator 3.10 can't parse. Raises ``ValueError``
    like ``fromisoformat`` for non-ISO input."""
    s = value.replace("Z", "+00:00")
    frac = _ISO_FRACTION_RE.search(s)
    if frac:
        digits = frac.group(1)
        s = s[: frac.start(1)] + digits[:6].ljust(6, "0") + s[frac.end(1) :]
    return datetime.fromisoformat(s)


def _iso_shaped(s: str) -> bool:
    """Structural pre-check for extended-format ISO-8601 (``YYYY-MM-DD…``) —
    the same rule ``looks_like_iso8601`` applies before parsing. Applying it
    HERE too keeps the comparison keys version-uniform: ``fromisoformat`` on
    Python ≥3.11 also parses BASIC format (``"20240101"``), which the 3.10
    floor rejects — without the guard, an 8-digit numeric-string cursor
    (yyyymmdd date keys rendered as strings, a real ERP pattern) would
    datetime-key on one runtime and string-key on another, and would alias
    ``"20240101"`` with ``"2024-01-01"`` as the same instant."""
    return len(s) >= 10 and s[4] == "-" and s[7] == "-"


def cursor_sort_key(value: Any) -> Any:
    """Chronological sort key for one cursor value.

    The server orders cursor columns chronologically, but the client-side
    comparisons (re-filters, watermark maxes, probe dirty checks) receive the
    server's *rendered text* — and OData's JSON format makes fractional
    seconds optional per value (real stacks — Olingo, SAP — trim trailing
    zeros), so one column legitimately renders both ``…T23:00:00Z`` and
    ``…T23:00:00.5Z``. Python string order puts the LATER ``.5Z`` instant
    first (``.`` < ``Z``), which silently drops server-returned rows at the
    ``<= since`` re-filters and regresses watermark maxes. ISO-8601-looking
    strings therefore parse to a datetime for ordering (naive values are
    pinned to UTC so mixed naive/aware renderings still totally order).

    NUMERIC strings key as exact :class:`Decimal` for the same reason in the
    other direction: an IEEE754Compatible server renders Int64/Decimal
    cursors as JSON strings, and ordinal string order inverts at every
    digit-length boundary (``"1000" < "999"``) — which silently STALLS the
    stream (the ``<= since`` re-filter drops every genuinely-newer row the
    server returns, forever) and regresses watermark maxes.
    :func:`cursor_same_instant` already treated this pair class numerically;
    the sort key now agrees. A Decimal key still cross-compares with raw
    int/float values (one column mixing ``5000`` and ``"5000"``), while
    Decimal-vs-datetime / Decimal-vs-text pairs raise ``TypeError`` into
    :func:`cursor_newer`'s existing shape-mixed fallback.

    Anything else orders as itself. Offsets and ``$filter`` literals keep the
    server's raw text — this key is for COMPARISON only."""
    if isinstance(value, str):
        if _iso_shaped(value):
            try:
                dt = parse_iso8601(value)
            except (ValueError, TypeError):
                return value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        num = _as_exact_number(value)
        if num is not None:
            return num
    return value


def cursor_newer(a: Any, b: Any) -> bool:
    """Whether ``a`` is strictly newer (greater) than ``b`` in cursor order —
    chronological where both render as ISO-8601, exact-numeric where both
    render as numbers (see :func:`cursor_sort_key`), raw ordering otherwise.
    A shape-mixed column (one value parses, the other doesn't) falls back to
    comparing the raw values, preserving the pre-helper behavior for such
    data.

    Key TIES between different raw texts break on the FRACTION DIGITS,
    zero-padded to a common width: Python datetimes hold microseconds, so
    :func:`parse_iso8601` truncates sub-microsecond digits — two
    chronologically DIFFERENT 100ns-precision cursors (SQL Server
    ``datetime2(7)`` sources emit 7-digit fractions) would otherwise tie,
    and a tie at the ``<= since`` re-filter drops a strictly-newer row the
    server correctly returned. Padding before comparing matters: raw-text
    comparison inverts when digit counts differ (the shorter fraction's
    terminator ``Z``/``+`` sorts above any digit), whereas equal-width
    digit strings compare numerically. Genuinely equal instants rendered
    two ways (``…Z`` vs ``…+00:00``, trailing zeros) fall through to an
    arbitrary-but-consistent raw order that errs only in the
    duplicate-safe direction."""
    key_a, key_b = cursor_sort_key(a), cursor_sort_key(b)
    try:
        if key_a > key_b:
            return True
        if key_b > key_a:
            return False
    except TypeError:
        try:
            return a > b
        except TypeError:
            # Incomparable raw pair (str vs int — an IEEE754Compatible
            # server rendering one Int64 cursor as 5000 and "5000"). A
            # NUMERIC pair still orders truly via exact Decimal: without
            # this bridge, a server that PERMANENTLY switches int→string
            # rendering (gateway upgrade) against an int-typed checkpoint
            # makes cursor_le(new_row, since) read True for genuinely
            # NEWER rows — the client-side re-filter then drops every
            # returned row and the stream silently stalls forever with
            # data pending (a flat False here is duplicate-safe for the
            # watermark fold but NOT for the re-filter). Non-numeric
            # incomparable pairs keep the arbitrary-but-consistent False,
            # matching ``_chain_strictly_before``'s documented posture:
            # degrade duplicate-safe, never raise out of a fold.
            num_a, num_b = _as_exact_number(a), _as_exact_number(b)
            if num_a is not None and num_b is not None:
                return num_a > num_b
            return False
    if a == b:
        return False
    # Key tie between two NUMERIC renderings: compare as exact Decimals so
    # numerically-equal texts ("5000.0" vs "5000") read as the same instant
    # in BOTH directions (the lexical fallback below would call one of them
    # strictly newer — duplicate-safe, but not antisymmetric, so a server
    # that alternates renderings could flap the watermark text batch to
    # batch). Distinct values beyond float precision (Int64 cursors past
    # 2^53 tie on the float sort key) also order truly here. ISO timestamp
    # texts never Decimal-parse, so their sub-microsecond fraction
    # tie-break below is untouched.
    num_a, num_b = _as_exact_number(a), _as_exact_number(b)
    if num_a is not None and num_b is not None:
        return num_a > num_b
    # Exact-key tie with different texts: sub-microsecond digits (or two
    # renderings of one instant). Compare fractions numerically first.
    frac_a, frac_b = _fraction_digits(a), _fraction_digits(b)
    width = max(len(frac_a), len(frac_b))
    frac_a, frac_b = frac_a.ljust(width, "0"), frac_b.ljust(width, "0")
    if frac_a != frac_b:
        return frac_a > frac_b
    try:
        return a > b  # same instant — consistent arbitrary order
    except TypeError:
        return False


def _as_exact_number(value: Any) -> Decimal | None:
    """``value`` as an exact :class:`Decimal`, or ``None`` when it isn't a
    number (or a numeric string). Exactness matters: a float round-trip
    would collapse Int64 cursors beyond 2^53 (``9007199254740993`` vs
    ``…92``) into one value and mis-report distinct instants as equal."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            dec = Decimal(value.strip())
        except InvalidOperation:
            return None
        return dec if dec.is_finite() else None
    return None


def cursor_same_instant(a: Any, b: Any) -> bool:
    """Whether ``a`` and ``b`` denote the SAME instant/value, tolerating
    rendering differences — timestamp forms (``…00Z`` vs ``…00.000Z`` vs
    ``…00+00:00``) and numeric forms (``5000`` vs ``"5000"``, the
    IEEE754Compatible string rendering of an Int64/Decimal).

    Identical values are trivially the same instant. ISO-8601-parsing pairs
    (via :func:`cursor_sort_key`) must reach equal datetimes AND carry equal
    zero-padded fraction digits — the fraction check restores the
    sub-microsecond precision ``cursor_sort_key`` truncates, so two
    chronologically distinct 100ns cursors (SQL Server ``datetime2(7)``)
    never count as the same instant. Otherwise a numeric pair compares as
    exact :class:`Decimal` (see :func:`_as_exact_number`). Anything else is
    the same instant only if raw-equal.

    Used by the ancestor-walk park identity: a parked parent whose cursor
    TEXT changed but instant didn't (a mixed-version load balancer
    alternating renderings per request) hasn't been modified — resuming its
    parked link is safe and makes progress, where treating every text
    mismatch as a change re-walks from page 1 each batch and livelocks for
    as long as the alternation lasts."""
    if a == b:
        return True
    key_a, key_b = cursor_sort_key(a), cursor_sort_key(b)
    if isinstance(key_a, datetime) and isinstance(key_b, datetime):
        if key_a != key_b:
            return False
        frac_a, frac_b = _fraction_digits(a), _fraction_digits(b)
        width = max(len(frac_a), len(frac_b))
        return frac_a.ljust(width, "0") == frac_b.ljust(width, "0")
    num_a, num_b = _as_exact_number(a), _as_exact_number(b)
    if num_a is not None and num_b is not None:
        return num_a == num_b
    return False


def cursor_same_rendering(a: Any, b: Any) -> bool:
    """Whether ``a`` and ``b`` can plausibly be ONE value's two renderings —
    the narrower cousin of :func:`cursor_same_instant` for comparing
    IDENTITY key elements (chain positions), not cursor watermarks.

    The difference is the both-strings numeric case: a server renders one
    numeric VALUE as ``5000`` (JSON number) or ``"5000"`` (IEEE754Compatible
    string) — a type flip — but it never re-renders one string key's TEXT
    (``"007"`` stays ``"007"``). Two numeric STRINGS with different text
    (``"007"`` vs ``"7"``, ``"1.0"`` vs ``"1"``, ``"0"`` vs ``"-0"``) are
    therefore two DISTINCT identities that merely alias numerically —
    ``cursor_same_instant`` calls them equal (right for watermarks, where
    only the instant matters), but conflating them as one chain POSITION
    lets a later element decide order across two different parents,
    defeating the vanished-anchor reset and silently dropping a subtree.
    Chronological renderings (``…00Z`` vs ``…00.000Z``) still conflate:
    real servers do re-render one instant's text per request."""
    if isinstance(a, str) and isinstance(b, str):
        key_a, key_b = cursor_sort_key(a), cursor_sort_key(b)
        if isinstance(key_a, datetime) and isinstance(key_b, datetime):
            return cursor_same_instant(a, b)
        return a == b
    return cursor_same_instant(a, b)


def cursor_le(a: Any, b: Any) -> bool:
    """Whether ``a <= b`` in cursor order — the exact complement of
    :func:`cursor_newer` under its strict total order (including the
    raw-text tie-break), so the re-filter and the watermark max can never
    disagree about a boundary row."""
    return not cursor_newer(a, b)


def cursor_at_or_before_for_refilter(a: Any, b: Any) -> bool:
    """Whether a returned row is provably at or before its watermark.

    Client-side re-filters must fail open when a cursor column changes
    semantic shape mid-stream. For example, an ISO timestamp and a numeric
    watermark are not meaningfully orderable even if both arrived as strings;
    applying the raw-text fallback used by :func:`cursor_newer` could silently
    drop the row. Values are comparable here only when both are timestamps,
    both are numeric (including number/string bridges), or both are raw values.
    """

    def family(value: Any) -> str:
        if isinstance(cursor_sort_key(value), datetime):
            return "datetime"
        if _as_exact_number(value) is not None:
            return "numeric"
        return "raw"

    if family(a) != family(b):
        return False
    return cursor_le(a, b)


def cursor_max(values: Any) -> Any:
    """Max of an iterable of non-``None`` cursor values in cursor order.
    Pairwise via :func:`cursor_newer` (not ``max(key=…)``) so a shape-mixed
    iterable degrades per-pair instead of raising. With the raw-text
    tie-break in :func:`cursor_newer`, only IDENTICAL texts (or
    incomparable pairs) still tie — for those the first-seen value wins,
    matching ``max``'s tie behavior."""
    result = sentinel = object()
    for value in values:
        if result is sentinel or cursor_newer(value, result):
            result = value
    if result is sentinel:
        raise ValueError("cursor_max() arg is an empty sequence")
    return result


def trim_to_distinct_cursor_boundary(
    records: list[dict],
    cursor_field: str,
) -> list[dict]:
    """Drop trailing records sharing the boundary cursor value.

    Walks back from the tail until the cursor value changes, leaving a
    clean boundary that the next call's ``cursor gt <last>`` filter
    will pick up cleanly. Drops the boundary record itself — we can't
    tell whether the next page (or a concurrent insert before the next
    call) holds more records sharing that cursor value, so we
    surrender the whole group and let ``cursor gt <prev_distinct>``
    re-fetch them.

    Reads the **real** cursor column, never a ``cursor_nulls=coalesce``
    synthetic. That's deliberate: a same-cursor cohort is re-readable
    via ``cursor gt`` next call, but null-cursor rows are excluded by
    ``gt`` server-side, so they must not be trimmed — a batch of only
    null cursors trims to empty (every real value is the same ``None``)
    and the caller keeps the rows as-is.

    Returns an empty list when every record shares one cursor value;
    the caller decides whether that's recoverable (natural exhaustion)
    or a hard failure (truncated batch with too-small cap).

    Cohort membership is SAME-INSTANT (:func:`cursor_same_instant`), not
    raw equality: a mixed-rendering batch (page 1 renders an Int64 cursor
    as ints, page 2 as strings — a mixed-version LB) would otherwise
    split one value's cohort at the rendering seam, trim only the
    differently-rendered tail, and leave the watermark EQUAL to the
    trimmed rows' value — ``cursor gt <watermark>`` then never re-fetches
    them (permanent loss). Same-instant grouping trims the whole cohort
    as one, restoring the watermark-strictly-below-boundary invariant
    regardless of rendering.
    """
    if not records:
        return records
    boundary = records[-1].get(cursor_field)
    trim_idx = len(records)
    while trim_idx > 0 and cursor_same_instant(records[trim_idx - 1].get(cursor_field), boundary):
        trim_idx -= 1
    return records[:trim_idx]


def jsonify_complex_values(row: dict) -> dict:
    """Render structured (dict/list) property values in an emitted row as
    JSON text.

    The connector's schema maps complex-typed / ``Collection(...)`` /
    enum / untyped CSDL properties to ``StringType``, and the framework's
    string parser stringifies via ``str()`` — which for a dict/list
    produces a Python **repr** (``{'City': 'X'}``, single quotes) that
    downstream ``from_json`` can't parse. Every structured value still
    present in a row at the emit boundary is by construction destined for
    a string column (the connector never declares struct/array columns,
    and nav-collection structures are consumed by the expand flattener
    before emit), so serializing them here is schema-independent and
    lossless. Rows with only scalar values pass through untouched."""
    if any(isinstance(v, (dict, list)) for v in row.values()):
        return {
            k: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
            for k, v in row.items()
        }
    return row


def pad_row_to_fields(row: dict, field_names, never_pad=frozenset()) -> dict:
    """Return ``row`` with an explicit ``None`` for every name in
    ``field_names`` it doesn't already carry — except names in ``never_pad``.

    OData servers may legally omit null-valued properties from a JSON entity,
    and the framework's row parser rejects a declared column that is *absent*
    (even a nullable one is fine as an explicit ``None``, but a non-nullable
    absent column raises and kills the batch). Padding to the declared schema
    makes an omit-null response parse cleanly.

    ``never_pad`` holds the columns whose absence must STAY loud because the
    omit-null rationale can't apply to them: primary keys (a key is never
    null, so a missing one means a broken response — padding it would send a
    silent null-key row into the destination's ``apply_changes`` MERGE) and
    the connector-stamped delta synthetics (absence there is a connector
    stamping bug, not server behavior). Returns ``row`` unchanged (no copy)
    when nothing needs padding — the common case — otherwise a new dict,
    never mutating the caller's row (lookback re-emits the same object)."""
    missing = [n for n in field_names if n not in row and n not in never_pad]
    if not missing:
        return row
    padded = dict(row)
    for name in missing:
        padded[name] = None
    return padded


def parse_max_records(table_options: dict | None) -> int:
    """Parse the ``max_records_per_batch`` table option (default 10000) with
    curated validation. The cap counts EMITTED rows per batch, so ``0`` or a
    negative value would make every walk park (or livelock) without emitting
    a single row — reject it up front instead of silently reading nothing."""
    raw = (table_options or {}).get("max_records_per_batch", "10000")
    try:
        value = int(str(raw).strip())
    except (ValueError, TypeError):
        raise ValueError(
            f"Invalid max_records_per_batch={raw!r}: expected a positive integer."
        ) from None
    if value < 1:
        raise ValueError(
            f"Invalid max_records_per_batch={raw!r}: must be >= 1 — the cap "
            f"bounds rows emitted per batch, and a non-positive cap would "
            f"emit nothing forever."
        )
    return value


# One-time-set capability markers persisted on the offset (baked-in
# verdicts, not progress). Shared by the no-progress comparison in
# ``_finalize_cursor_read`` and the quiescence check in
# ``_attach_lookback_state`` so the two can never drift: a key one strips
# and the other doesn't turns a quiescent overlap re-read into a fake
# "real walk" measurement (or vice versa).
OFFSET_CAPABILITY_FLAGS = frozenset(
    {"cursor_probe_ok", "batch_ok", "batch_size_ok", "or_filter_ok", "expand_ok", "delta_ok"}
)


def offset_progress_view(off: dict | None) -> dict:
    """The offset's cursor/continuation progress state only: strips the
    ``lb_*`` lookback bookkeeping (its measurement fluctuates batch to
    batch without representing real cursor progress) and the one-time
    capability flags (:data:`OFFSET_CAPABILITY_FLAGS`) — otherwise a batch
    that merely baked in a flag would read as forward progress."""
    return {
        k: v
        for k, v in (off or {}).items()
        if not k.startswith("lb_") and k not in OFFSET_CAPABILITY_FLAGS
    }


LOOKBACK_DEDUP_DEFAULT_CAP = 5000


def parse_lookback_dedup(table_options: dict | None) -> int:
    """Parse the ``cursor_lookback_dedup`` table option into a seen-set
    entry cap: ``on``/``true`` (the default) -> the default cap,
    ``off``/``false``/``0`` -> 0 (disabled), a positive integer -> that
    cap (the boolean spellings match the connector's other flag options,
    e.g. ``expand_contained``). Defaulting on is safe: dedup only
    engages when a lookback window is active, every failure direction is
    a redundant MERGE re-emit (the pre-dedup behavior), and a
    pre-existing offset without ``lb_seen`` just re-emits one overlap
    before tracking engages. The seen-set must be EXACT — a
    probabilistic structure (e.g. a Bloom filter) can report a
    never-emitted row as seen and suppress it (silent loss) — so the only
    size control offered is a hard entry cap; above it the read degrades
    to plain overlap re-emits (MERGE-idempotent at the destination),
    never loss."""
    raw = (table_options or {}).get("cursor_lookback_dedup")
    if raw is None or str(raw).strip() == "":
        return LOOKBACK_DEDUP_DEFAULT_CAP
    norm = str(raw).strip().lower()
    if norm in ("off", "false", "0"):
        return 0
    if norm in ("on", "true"):
        return LOOKBACK_DEDUP_DEFAULT_CAP
    try:
        cap = int(norm)
    except ValueError:
        raise ValueError(
            f"Invalid cursor_lookback_dedup={norm!r}. Expected one of: on, off, "
            f"or a positive integer entry cap."
        ) from None
    if cap < 1:
        raise ValueError(
            f"Invalid cursor_lookback_dedup={norm!r}: the entry cap must be >= 1 "
            f"(use 'off' to disable)."
        )
    return cap


def max_or(a: Any, b: Any) -> Any:
    """Max of two values in CURSOR order (see :func:`cursor_newer`) where
    either may be ``None``. Returns the other when one is ``None``; ``None``
    only if both are. ``a`` wins ties (matching ``max``'s first-arg-wins)."""
    if a is None:
        return b
    if b is None:
        return a
    return b if cursor_newer(b, a) else a
