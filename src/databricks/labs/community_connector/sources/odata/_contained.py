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

import logging
import math
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

from pyspark.sql.types import StructField

from databricks.labs.community_connector.sources.odata._helpers import (
    max_or as _max_or,
    trim_to_distinct_cursor_boundary as _trim_to_distinct_cursor_boundary,
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

# Default ``page_size`` applied to **cursor-based** reads (cursor_field
# or delta) when the user didn't set one, so a ``$top`` is still sent.
# Snapshot reads deliberately omit ``$top`` entirely when ``page_size``
# is unset (see ``_format_query_params``) — letting the server choose
# its page size avoids servers that reject an explicit ``$top``. Cursor
# reads keep a bounded page for predictable incremental batches.
DEFAULT_PAGE_SIZE = "1000"


_TOP_PARAM_RE = re.compile(r"(?<=[?&])(\$top=|%24top=)\d+", re.IGNORECASE)


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
    """Cheap ISO-8601 sniff used by ``odata_literal`` to render bare timestamps."""
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return False
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def odata_literal(value: Any) -> str:
    """Render a Python value as an OData v4 literal for $filter."""
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return str(value)
    s = str(value)
    if looks_like_iso8601(s):
        return s
    return "'" + s.replace("'", "''") + "'"


# --- client-side pagination URL helpers -----------------------------------
# These manipulate the connector's own generated URLs, where query options
# (``$top``/``$filter``/``$orderby``/``$skip``) are stored raw (un-encoded),
# one per ``&``-separated segment, and no generated value contains a literal
# ``&``. So splitting on ``&`` and matching on a ``$name=`` prefix is safe —
# the same convention ``rewrite_top_in_url`` relies on. requests url-encodes
# the values when the request is actually sent. They live here (rather than in
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


def _pg_orderby_keys(url: str) -> list[str]:
    """Column names from the URL's ``$orderby``, in order. Returns ``[]``
    when there's no ``$orderby`` or any term is ``desc`` (a ``gt`` seek
    only walks ascending order; the connector only ever emits ``asc``)."""
    raw = _pg_get_query(url, "$orderby") or _pg_get_query(url, "%24orderby")
    if not raw:
        return []
    keys = []
    for term in raw.replace("%20", " ").split(","):
        term = term.strip()
        if term.endswith(" desc"):
            return []
        keys.append(term[:-4].strip() if term.endswith(" asc") else term)
    return [k for k in keys if k]


def _pg_keyset_filter(order_keys: list[str], row: dict) -> str | None:
    """Build the ascending seek predicate placing the cursor strictly after
    ``row`` in ``order_keys`` order::

        (k1 gt v1) or (k1 eq v1 and k2 gt v2) or …

    Returns ``None`` if any key's value is null (no comparable boundary —
    the caller falls back to ``$skip``)."""
    vals = []
    for k in order_keys:
        v = row.get(k)
        if v is None:
            return None
        vals.append((k, v))
    clauses = []
    for i, (k, v) in enumerate(vals):
        terms = [f"{vals[j][0]} eq {odata_literal(vals[j][1])}" for j in range(i)]
        terms.append(f"{k} gt {odata_literal(v)}")
        clauses.append(" and ".join(terms))
    if len(clauses) == 1:
        return clauses[0]
    return " or ".join(f"({c})" for c in clauses)


def _pg_with_extra_filter(url: str, clause: str) -> str:
    """AND ``clause`` into the URL's ``$filter`` (replacing any prior seek —
    the caller always rebuilds from the original base URL, so seeks never
    accumulate)."""
    existing = _pg_get_query(url, "$filter")
    combined = f"({existing}) and ({clause})" if existing else clause
    return _pg_set_query(url, "$filter", combined)


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
    page, before any seek). An empty ``__pgbase`` marker means 'no base'."""
    marker = _pg_get_query(url, _PG_BASE)
    if marker is not None:
        return marker or None
    return _pg_get_query(url, "$filter")


def _pg_keyset_seek_url(url: str, base_filter: str | None, seek: str) -> str:
    """Build the next keyset page URL: ``$filter`` becomes
    ``base_filter AND seek`` (or just ``seek`` when there's no base), with
    ``base_filter`` stashed in the private ``__pgbase`` option.

    Carrying the base separately lets a resumed walk REPLACE the seek instead
    of AND-ing a fresh lower bound onto the previous one every cap-resume
    batch — otherwise the ``$filter`` grows one keyset clause per batch and
    eventually overflows the server's URL-length limit. The seeks are
    monotonic so the accumulated form is merely redundant, never wrong, but it
    is unbounded. ``__pgbase`` is stripped before the request is sent."""
    combined = f"({base_filter}) and ({seek})" if base_filter else seek
    out = _pg_set_query(url, "$filter", combined)
    return _pg_set_query(out, _PG_BASE, base_filter or "")


def _pg_page_fingerprint(page_rows: list[dict]) -> int:
    """Order-sensitive fingerprint of a page's rows for the no-progress
    guard. ``hash(repr(...))`` is process-stable — only ever compared within
    a single walk — and costs one page's worth of work. Two consecutive
    non-empty pages with the same fingerprint mean the server returned the
    same data twice (it ignored our seek/``$skip`` or handed back a cyclic
    ``@odata.nextLink``), so the walk has stalled."""
    return hash(repr(page_rows))


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
    """Parse ``filter_at_<segment>`` and ``filter_at_<idx>`` table-option
    keys into a ``{level: filter_string}`` mapping.

    Per-segment filters let the user push a ``$filter`` to the exact
    walk level (or ``$expand`` clause) that owns the property. Without
    this, the table's single ``filter`` option lands at one URL only
    (leaf for N+1 mode, top for expand mode), leaving intermediate
    levels unfiltered and forcing a full fan-out.

    Two equivalent key forms are accepted:

    * **By segment name** — ``filter_at_Instances=Id eq 5`` matches the
      segment literally as it appears in the contained path / URL.
    * **By zero-based index** — ``filter_at_0=Id eq 5`` matches the
      level positionally. Useful when nav-property names repeat at
      different depths.

    Both forms may be set; the **index form wins on conflict**, since
    it's the more explicit of the two. Unknown segment names and
    out-of-range indices raise ``ValueError`` immediately so typos
    don't silently produce a full-fan-out walk.
    """
    if not table_options:
        return {}
    out: dict[int, str] = {}
    # Lakeflow Connect lowercases option keys before forwarding them
    # to ``read_table``, so a pipeline-config ``filter_at_Instances``
    # arrives here as ``filter_at_instances``. Match the segment-name
    # suffix case-insensitively against the discovered path so the
    # pipeline config doesn't have to special-case the framework's
    # normalisation rules. Values aren't normalised — only keys — so
    # the filter expression itself is preserved verbatim.
    seg_to_idx = {s.lower(): i for i, s in enumerate(segments)}
    # Pass 1: name-keyed. Index-keyed entries override these on
    # conflict, so process them after.
    for key, value in table_options.items():
        if not key.startswith("filter_at_"):
            continue
        suffix = key[len("filter_at_") :]
        if suffix.isdigit():
            continue
        idx = seg_to_idx.get(suffix.lower())
        if idx is None:
            raise ValueError(
                f"Invalid table option {key}={value!r}: segment "
                f"{suffix!r} not in path {segments!r}. Valid "
                f"segments (case-insensitive): {segments}."
            )
        out[idx] = value
    # Pass 2: index-keyed (overrides name form when both target the
    # same level).
    for key, value in table_options.items():
        if not key.startswith("filter_at_"):
            continue
        suffix = key[len("filter_at_") :]
        if not suffix.isdigit():
            continue
        idx = int(suffix)
        if not 0 <= idx < len(segments):
            raise ValueError(
                f"Invalid table option {key}={value!r}: index {idx} "
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
        except ValueError:
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
                target_et = self._resolve_type_ref(target_ref)
                if target_et is None:
                    continue
                new_segments = segments + [nav_name]
                paths.append(CONTAINED_PATH_SEP.join(new_segments))
                queue.append((new_segments, target_et, seen | {target_ref}))
        return paths

    # --- option parsing ----------------------------------------------------

    def _expand_contained_active(self, table_options: dict[str, str] | None) -> bool:
        """Parse the boolean ``expand_contained`` table option."""
        raw = ((table_options or {}).get("expand_contained") or "false").strip().lower()
        if raw not in {"true", "false"}:
            raise ValueError(f"Invalid expand_contained={raw!r}. Expected one of: true, false.")
        return raw == "true"

    # --- URL construction --------------------------------------------------

    def _format_key_predicate(self, pk_values: dict[str, Any]) -> str:
        """``(value)`` for single key; ``(K1=v1,K2=v2)`` for composite."""
        if len(pk_values) == 1:
            return f"({odata_literal(next(iter(pk_values.values())))})"
        return "(" + ",".join(f"{k}={odata_literal(v)}" for k, v in pk_values.items()) + ")"

    def _build_contained_path(self, segments: list[str], key_chain: list[dict[str, Any]]) -> str:
        """``A(1)/B('x')/C`` — leaf segment has no key; ``key_chain`` len = N-1."""
        if len(key_chain) != len(segments) - 1:
            raise ValueError(
                f"key_chain length {len(key_chain)} does not match "
                f"non-leaf segment count {len(segments) - 1}"
            )
        return _URL_SEGMENT_SEP.join(
            f"{seg}{self._format_key_predicate(key_chain[i])}" if i < len(key_chain) else seg
            for i, seg in enumerate(segments)
        )

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
        base = join_url(self.service_url, self._build_contained_path(segments, key_chain))
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
        # The table's ``filter`` option is the leaf filter — same as N+1
        # mode, where it lands at the leaf URL — so strip it from the
        # start-level query params when there are deeper levels. It re-enters
        # at the innermost ``$expand(...)`` clause below. Without this split,
        # ``filter="Id eq 3"`` on a ``Instances__Projects`` table would land
        # at Instances (wrong segment) and 400 the server.
        top_opts = {k: v for k, v in opts.items() if k != "filter"} if has_children else dict(opts)
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
            if cursor_level == i and cursor_select:
                parts.append(f"$select={cursor_select}")
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
    ) -> dict[tuple[str, str], str]:
        """Map ``(segment, pk_name) → unique FK column name`` for every
        non-leaf ancestor.

        OData v4 §13.4.3 makes contained-entity keys unique only within
        their immediate parent, so the destination composite key needs
        the full ancestor chain to be globally unique. Default name is
        ``<segment>_<pk>``; collisions get a leading ``_`` until unique.
        Empty mapping for flat tables.

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
                    resolved[(seg, pk)] = candidate
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
        fk_columns: dict[tuple[str, str], str],
    ) -> None:
        """Write every ancestor's primary-key values onto ``row`` under
        the resolved FK column names from ``fk_columns``."""
        for idx, ancestor_keys in enumerate(chain):
            seg = segments[idx]
            for pk_name, pk_val in ancestor_keys.items():
                col = fk_columns.get((seg, pk_name))
                if col is not None:
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
        segments = parse_contained_path(table_name) or [table_name]
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
        re-fetching."""
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
                    extra_filter = self._cursor_filter(cursor_field, since)
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
                row_source = self._fetch_pages(url)
            for row in row_source:
                next_cur = row.get(cursor_field) if level == cursor_level else cur_val
                chain.append({pk: row.get(pk) for pk in ancestor_pks})
                yield from _walk(level + 1, chain, next_cur)
                chain.pop()

        yield from _walk(0, [], None)

    def _iter_parent_key_chains(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
        top_parent_rows: list[dict] | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield every ancestor key chain (len = len(segments) - 1) reaching
        the leaf. Each level fetched with ``$select=<pks>``; user ``filter``
        not forwarded — that string lands at the leaf URL only. To filter
        an ancestor walk use ``filter_at_<segment>`` / ``filter_at_<idx>``.

        ``top_parent_rows`` lets a partitioned caller supply a pre-
        discovered subset of level-0 rows; when provided, the level-0
        HTTP fetch is skipped and the rows are consumed in order."""
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
                row_source = self._fetch_pages(url)
            for row in row_source:
                chain.append({pk: row.get(pk) for pk in ancestor_pks})
                yield from _walk(level + 1, chain)
                chain.pop()

        yield from _walk(0, [])

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
        segments = parse_contained_path(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        fk_columns = self._resolve_fk_columns(segments, namespace)
        segment_filters = resolve_segment_filters(table_options, segments)
        leaf_extra = segment_filters.get(len(segments) - 1)
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)

        def _emit() -> Iterator[dict]:
            for chain in self._iter_parent_key_chains(segments, namespace, table_options):
                url = self._build_contained_url(
                    segments, chain, table_options, extra_filter=leaf_extra, order_by=leaf_order_by
                )
                for row in self._fetch_pages(url):
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
        segments = parse_contained_path(table_name) or [table_name]
        if len(segments) < 2:
            raise ValueError(f"expand_contained requires a contained path; {table_name!r} is flat.")
        namespace = (table_options or {}).get("namespace")
        cursor_field = (table_options or {}).get("cursor_field")
        cursor_level, cursor_filter, cursor_order, cursor_select = self._cursor_expand_clause(
            segments, namespace, cursor_field, (start_offset or {}).get("cursor")
        )
        # Read-scoped context the flatten recursion needs to synthesize a
        # client-driven continuation for an inner collection whose
        # ``<NavProp>@odata.nextLink`` the server omitted (see
        # ``_build_expand_continuation_url``). Stashed on ``self`` — like
        # ``self._pagination`` — so it survives into the lazy streaming
        # generator without threading through every flatten call site.
        self._expand_cont_opts = table_options
        self._expand_cont_since = (start_offset or {}).get("cursor")
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
        max_records = int((table_options or {}).get("max_records_per_batch", "10000"))
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
        remaining_queue = self._drain_expand_pages(
            initial_queue,
            max_records,
            segments,
            pks_per_level,
            fk_columns,
            emitted,
            ctx,
            page_size,
        )
        end_offset = self._build_expand_end_offset(
            emitted, cursor_field, start_offset, remaining_queue
        )
        if not cursor_field:
            return iter(emitted), end_offset
        if not emitted and not resuming:
            return iter([]), start_offset or {}
        return self._finalize_cursor_read(
            start_offset, end_offset, emitted, table_name, cursor_field
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _drain_expand_pages(
        self,
        initial_queue: list[dict],
        max_records: int,
        segments: list[str],
        pks_per_level: list[list[str]],
        fk_columns: dict[str, str],
        emitted: list[dict],
        ctx: tuple | None,
        page_size: int | None,
    ) -> list[dict]:
        """Iterative work-queue processor.

        Each queue item is a self-contained "fetch this URL and
        process the rows it returns" task::

            {
                "url":     str,              # HTTP URL to GET (one page)
                "level":   int,              # level the URL's rows live at
                "chain":   list[dict],       # ancestor PK chain (snapshot)
                "cur_val": Any | None,       # captured cursor value
                "skip":    int,              # top_row index to start at
            }

        Items are popped FIFO; each pop performs ONE HTTP fetch and
        processes its top_rows starting from ``skip``. Inner-collection
        ``@odata.nextLink`` values discovered during a row's inline
        descent are APPENDED to the queue (via
        ``_flatten_expand_response``'s ``pending_fetches`` arg) rather
        than followed inline. After each fully-processed top_row the
        ``max_records`` cap is checked: when exceeded, the current
        item is re-queued at the front with ``skip`` advanced past the
        rows just emitted, and the loop exits. The returned queue is
        the work left to do — non-empty means "continuation pending",
        empty means "chain drained".

        Cap deviation per batch is bounded by ONE HTTP response's
        worth of leaf rows (≤ ``page_size``), not by the size of a
        single top_row's subtree as in the previous design.
        """
        # Take ownership: mutated in-place by appends from
        # ``_flatten_expand_response`` and by our own front re-queues.
        queue: list[dict] = list(initial_queue)
        cur_field, cur_level, _ = ctx or (None, -1, None)
        while queue and len(emitted) < max_records:
            item = queue.pop(0)
            url = item["url"]
            level = item["level"]
            chain = [dict(p) for p in item.get("chain") or []]
            cur_val = item.get("cur_val")
            skip = int(item.get("skip", 0) or 0)
            item_ctx = (cur_field, cur_level, cur_val) if cur_field else None
            # Tops budgeted over only THIS request's collection levels
            # (root == item level downward); ancestors above are fixed keys.
            item_tops = (
                compute_expand_tops_for_root(page_size, len(segments), level) if page_size else None
            )
            # Fetch one page only — pulling further pages of THIS
            # collection waits until the next dequeue so we can check
            # the cap between them.
            page_rows, page_next_url = self._fetch_one_expand_page(url)
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
            truncated = False
            for row_idx in range(skip, len(page_rows)):
                self._flatten_expand_response(
                    level,
                    page_rows[row_idx],
                    segments,
                    pks_per_level,
                    chain,
                    fk_columns,
                    emitted,
                    item_ctx,
                    item_tops,
                    response_url=url,
                    pending_fetches=queue,
                    page_size=page_size,
                )
                if len(emitted) >= max_records and row_idx + 1 < len(page_rows):
                    # Mid-page: re-queue the SAME URL at the front so
                    # the next batch resumes here without scrambling
                    # depth ordering.
                    queue.insert(
                        0,
                        {
                            "url": url,
                            "level": level,
                            "chain": [dict(p) for p in chain],
                            "cur_val": cur_val,
                            "skip": row_idx + 1,
                        },
                    )
                    truncated = True
                    break
            if not truncated and page_next_url:
                queue.append(
                    {
                        "url": page_next_url,
                        "level": level,
                        "chain": [dict(p) for p in chain],
                        "cur_val": cur_val,
                        "skip": 0,
                    }
                )
        return queue

    def _stream_expand_pages(
        self,
        initial_queue: list[dict],
        segments: list[str],
        pks_per_level: list[list[str]],
        fk_columns: dict[tuple[str, str], str],
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
        of pending fetch descriptors (URLs + chains, not rows). Emission
        order matches the drainer's ``emitted`` order — inline rows first,
        deferred continuations processed when their queue item is popped."""
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
            page_rows, page_next_url = self._fetch_one_expand_page(url)
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

    def _fetch_one_expand_page(self, url: str) -> tuple[list[dict], str | None]:
        """One HTTP GET; returns ``(page_rows, next_url)``. Thin wrapper
        over :meth:`_fetch_pages_with_links` that consumes a single
        iteration so the caller can check the cap between fetches.

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
        for page_rows, page_next_url in self._fetch_pages_with_links(url):
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
        if batch_cursors and prior_running is not None:
            new_running = max([*batch_cursors, prior_running])
        elif batch_cursors:
            new_running = max(batch_cursors)
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
            return {"cursor": new_running}
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
        ``$select`` is non-empty only when the cursor lives on a non-top
        segment — it forces the server to project the cursor column on
        the expanded ancestor (some servers default-omit it)."""
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
        return (
            cursor_level,
            self._cursor_filter(cursor_field, since),
            ",".join(order_terms),
            None,
        )

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    def _flatten_expand_response(
        self,
        level: int,
        row: dict,
        segments: list[str],
        pks_per_level: list[list[str]],
        chain: list[dict[str, Any]],
        fk_columns: dict[tuple[str, str], str],
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
            # Drop both top-level (``@odata.foo``) and per-property
            # (``Foo@odata.nextLink``) annotations from leaf rows; the
            # framework wouldn't know what to do with either.
            clean = {k: v for k, v in row.items() if "@odata." not in k}
            self._tag_with_ancestor_fks(clean, segments, chain, fk_columns)
            if cur_field and cur_val is not None and clean.get(cur_field) is None:
                clean[cur_field] = cur_val
            out.append(clean)
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
        inner_next = row.get(f"{next_seg}@odata.nextLink")
        if inner_next:
            resolved = urljoin(base_url, inner_next)
            if per_level_tops:
                # Continuation pages the collection at ``level + 1``
                # under one specific parent at ``level``. The original
                # ``$top`` for that level was sized against the FULL
                # cross-product budget (top × inner × …); the
                # continuation is one outer level shallower, so we
                # have more budget to spend per response. New $top is
                # ``page_size_budget / inner_product`` where
                # ``inner_product`` is the cross-product of all levels
                # deeper than ``level + 1`` (which the server-side
                # ``$expand`` chain in the nextLink still applies).
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
        else:
            # No ``<NavProp>@odata.nextLink``. In a client-driven pagination
            # mode (keyset/skip/auto), synthesize a direct-navigation
            # continuation when the inline page is a FULL page (== $top) and
            # so plausibly truncated; otherwise the inline page is taken as
            # the whole collection — today's nextlink-only behaviour. This
            # closes the inner-``$expand`` hole for servers that page-limit a
            # response but never emit the continuation link.
            resolved = self._inner_expand_continuation_url(
                level, row, segments, chain, next_ctx, per_level_tops
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
            for page_rows, page_next in self._fetch_pages_with_links(resolved):
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
        collection when the server returned a *full* inline page but omitted
        its ``<NavProp>@odata.nextLink``.

        Returns ``None`` unless ``pagination`` is keyset/skip/auto, ``$top``
        is in force (``per_level_tops`` set), and the inline child page is
        exactly ``$top`` rows (so it's plausibly truncated). A short page is
        proof the collection is complete, so it's taken at face value.
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
            self._build_contained_path(segments[: child_level + 1], chain),
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
        if mode in ("keyset", "auto") and order_keys:
            seek = _pg_keyset_filter(order_keys, last_child)
            if seek is not None:
                # Stash the clean child-level $filter as the keyset base so a
                # cross-batch resume REPLACES the seek instead of accumulating.
                return _pg_keyset_seek_url(url, _pg_get_query(url, "$filter"), seek)
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

    # pylint: disable=too-many-statements
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
        fk_columns: dict[tuple[str, str], str],
        leaf_segment_filter: str | None = None,
        effective=None,
        skip_null: bool = False,
    ) -> tuple[list[dict], bool, int, str | None, Any]:
        """Drive the per-parent fetch loop (leaf-cursor mode).

        ``chains_iter`` is consumed lazily and the walk stops at the
        first parent that offers a valid resume checkpoint once the
        ``max_records`` cap is reached. Peak memory is normally bounded
        to one chain; the exception is a *complete* parent whose entire
        leaf collection shares a single cursor value (see below), which
        is emitted in full and absorbed into the walk rather than
        checkpointed.

        Resume preference, applied to the chain at ``parent_idx_start``:

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

        Returns ``(rows, truncated, parent_idx, chain_next_link_out,
        truncated_chain_cursor_out)``.

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
        chain_next_link_out: str | None = None
        truncated_chain_cursor_out: Any = None
        for chain in chains_iter:
            # Skip the chains we already emitted in prior batches. The
            # iterator still pays for the ancestor pages that produce
            # those chains (no way to skip without knowing the keys),
            # but no leaf fetches happen here.
            if parent_idx < parent_idx_start:
                parent_idx += 1
                continue
            chain_start_idx = len(emitted)
            chain_since: Any
            initial_url: str
            if parent_idx == parent_idx_start and chain_next_link is not None:
                # Resume from the server's own skiptoken; no client-side
                # filter — the link already encodes filter/order state.
                chain_since = None
                initial_url = chain_next_link
            else:
                if parent_idx == parent_idx_start and truncated_chain_cursor is not None:
                    chain_since = truncated_chain_cursor
                else:
                    chain_since = since
                initial_url = self._build_contained_url(
                    segments,
                    chain,
                    table_options,
                    extra_filter=combine_filters(
                        self._cursor_filter(cursor_field, chain_since),
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
            for page_rows, page_next_url in self._fetch_pages_with_links(initial_url):
                for row in page_rows:
                    if skip_null and row.get(cursor_field) is None:
                        continue
                    rec_cursor = effective(row)
                    if (
                        chain_since is not None
                        and rec_cursor is not None
                        and rec_cursor <= chain_since
                    ):
                        continue
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    emitted.append(row)
                    if len(emitted) >= max_records:
                        cap_hit_in_page = True
                if cap_hit_in_page:
                    # Finish the current page (above) so its nextLink is a
                    # clean checkpoint, then stop fetching more pages of
                    # this chain and decide how to checkpoint below.
                    break
            if cap_hit_in_page:
                if page_next_url is not None:
                    # Page boundary mid-collection: the server skiptoken is
                    # a clean resume point — park it and re-enter this
                    # parent next batch.
                    truncated = True
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
        return (
            emitted,
            truncated,
            parent_idx,
            chain_next_link_out,
            truncated_chain_cursor_out,
        )

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
            f"rows with `filter`/`filter_at_<segment>`, or pick a different "
            f"cursor."
        )

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
        if start_offset == end_offset:
            if emitted:
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
        segments = parse_contained_path(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        cursor_level = self._find_cursor_level(segments, namespace, cursor_field)
        if cursor_level == -1:
            raise ValueError(
                f"cursor_field {cursor_field!r} is not a property on "
                f"{table_name!r} or any of its ancestors. Pick a column "
                f"declared on the leaf or one of the parent segments."
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
            return self._read_contained_incremental_leaf_cursor(
                table_name, segments, start_offset, table_options, cursor_field
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
        if cursor_level == len(segments) - 1:
            order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
            skip_null, _effective = self._make_cursor_resolver(
                table_name, namespace, cursor_field, table_options
            )
            for chain in self._iter_parent_key_chains(segments, namespace, table_options):
                url = self._build_contained_url(
                    segments, chain, table_options, extra_filter=leaf_filter, order_by=order_by
                )
                for row in self._fetch_pages(url):
                    if skip_null and row.get(cursor_field) is None:
                        continue
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    yield row
            return
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)
        chains_iter = self._iter_parent_chains_with_cursor(
            segments, namespace, table_options, cursor_level, cursor_field, None
        )
        for chain, ancestor_cursor in chains_iter:
            url = self._build_contained_url(
                segments, chain, table_options, extra_filter=leaf_filter, order_by=leaf_order_by
            )
            for row in self._fetch_pages(url):
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
    ) -> tuple[Iterator[dict], dict]:
        """Cursor lives on the leaf entity — filter at the leaf fetch.

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
        truncated_chain_cursor_in = (start_offset or {}).get("truncated_chain_cursor")
        chain_next_link_in = (start_offset or {}).get("chain_next_link")
        max_records = int((table_options or {}).get("max_records_per_batch", "10000"))
        order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
        chains_iter = self._iter_parent_key_chains(segments, namespace, table_options)
        segment_filters = resolve_segment_filters(table_options, segments)
        # ``cursor_nulls`` resolver (synthetic floor for nulls under
        # coalesce; skip nulls under ignore). The cursor lives on the leaf
        # entity, so PKs/floor come from the full contained path's leaf.
        skip_null, effective = self._make_cursor_resolver(
            table_name, namespace, cursor_field, table_options
        )
        (
            emitted,
            truncated,
            parent_idx,
            chain_next_link_out,
            truncated_chain_cursor_out,
        ) = self._walk_contained_with_cursor(
            segments,
            chains_iter,
            int((start_offset or {}).get("parent_idx", 0)),
            table_options,
            order_by,
            cursor_field,
            since,
            truncated_chain_cursor_in,
            chain_next_link_in,
            max_records,
            self._resolve_fk_columns(segments, namespace),
            leaf_segment_filter=segment_filters.get(len(segments) - 1),
            effective=effective,
            skip_null=skip_null,
        )
        if truncated:
            # The walk has already chosen the checkpoint and trimmed
            # ``emitted`` accordingly: ``chain_next_link_out`` for a page
            # boundary, else ``truncated_chain_cursor_out`` for a complete
            # parent with a distinct-cursor boundary. (A complete parent
            # with a single cursor value never truncates — the walk emits
            # it in full and continues — so there's no failure case here.)
            end_offset: dict = {"parent_idx": parent_idx}
            if chain_next_link_out is not None:
                end_offset["chain_next_link"] = chain_next_link_out
            else:
                end_offset["truncated_chain_cursor"] = truncated_chain_cursor_out
            if since is not None:
                end_offset["cursor"] = since
        else:
            if not emitted:
                return iter([]), start_offset or {}
            cursors = [effective(r) for r in emitted if effective(r) is not None]
            # Mirror ``_build_expand_end_offset`` /
            # ``_ancestor_cursor_offset``: when there's no cursor data
            # this batch and no prior ``since`` to carry, the offset is
            # ``{}`` — not ``{"cursor": None}``. The latter would advance
            # ``{}`` → ``{"cursor": None}`` on first-batch null-cursor
            # rows, silently committing one bad batch before the second
            # trigger (where ``start_offset == end_offset``) raises.
            if cursors:
                end_offset = {"cursor": max(cursors)}
            elif since is not None:
                end_offset = {"cursor": since}
            else:
                end_offset = {}
        return self._finalize_cursor_read(
            start_offset, end_offset, emitted, table_name, cursor_field
        )

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
        """
        namespace = (table_options or {}).get("namespace")
        since = (start_offset or {}).get("cursor")
        chains_iter = self._iter_parent_chains_with_cursor(
            segments, namespace, table_options, cursor_level, cursor_field, since
        )
        segment_filters = resolve_segment_filters(table_options, segments)
        walk_state = self._walk_ancestor_chains(
            segments,
            chains_iter,
            table_options,
            cursor_field,
            int((start_offset or {}).get("parent_idx", 0)),
            (start_offset or {}).get("chain_next_link"),
            int((table_options or {}).get("max_records_per_batch", "10000")),
            self._resolve_fk_columns(segments, namespace),
            leaf_segment_filter=segment_filters.get(len(segments) - 1),
        )
        end_offset = self._ancestor_cursor_offset(walk_state, start_offset, since, cursor_field)
        return self._finalize_cursor_read(
            start_offset, end_offset, walk_state["emitted"], table_name, cursor_field
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
        fk_columns: dict[tuple[str, str], str],
        leaf_segment_filter: str | None = None,
    ) -> dict[str, Any]:
        """Walk ancestor chains, fetching each chain's leaf collection
        and stamping rows with the chain's cursor.

        ``chains_iter`` is consumed lazily: the per-ancestor enumeration
        stops as soon as the loop breaks on a ``max_records`` hit, so
        we never fetch ancestor pages beyond the chain we actually
        emit from. Peak memory is bounded to one chain.

        Page-aware: a truncation at a page boundary parks the chain's
        ``@odata.nextLink``; when the chain happens to end on the
        truncating page, ``parent_idx`` simply advances past it."""
        namespace = (table_options or {}).get("namespace")
        leaf_order_by = self._leaf_pk_order_by(segments, namespace)
        parent_idx = 0
        emitted: list[dict] = []
        truncated = False
        chain_next_link_out: str | None = None
        for chain, ancestor_cursor in chains_iter:
            # Skip already-emitted chains. Ancestor-page HTTP cost is
            # unavoidable (we need the keys to identify the chain), but
            # no leaf fetches happen during the skip.
            if parent_idx < parent_idx_start:
                parent_idx += 1
                continue
            if parent_idx == parent_idx_start and chain_next_link_in is not None:
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
            for page_rows, page_next_url in self._fetch_pages_with_links(initial_url):
                for row in page_rows:
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    row[cursor_field] = ancestor_cursor
                    emitted.append(row)
                if len(emitted) >= max_records:
                    truncated = True
                    break
            if truncated:
                if page_next_url is not None:
                    chain_next_link_out = page_next_url
                else:
                    parent_idx += 1
                break
            parent_idx += 1
        return {
            "emitted": emitted,
            "truncated": truncated,
            "parent_idx": parent_idx,
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
        this_batch_max = max(cursors) if cursors else None
        prev_running_max = (start_offset or {}).get("running_max")
        new_running_max = _max_or(this_batch_max, prev_running_max)
        if walk_state["truncated"]:
            offset: dict = {"parent_idx": walk_state["parent_idx"]}
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
