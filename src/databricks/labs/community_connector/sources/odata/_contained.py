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
        top, *children = segments
        base = join_url(self.service_url, top)
        # The table's ``filter`` option is the leaf filter — same as
        # N+1 mode, where it lands at the leaf URL — so strip it from
        # the top-URL query params when there are children. It re-enters
        # at the innermost ``$expand(...)`` clause below. Without this
        # split, ``filter="Id eq 3"`` on a ``Instances__Projects`` table
        # would land at Instances (wrong segment) and 400 the server.
        opts = table_options or {}
        top_opts = {k: v for k, v in opts.items() if k != "filter"} if children else opts
        per_level_tops = compute_dynamic_tops(int(opts.get("page_size", "1000")), len(segments))
        if children:
            # Override page_size in the opts dict ``_format_query_params``
            # reads from, so the top-URL ``$top`` reflects the dynamic
            # allocation instead of the unscaled budget.
            top_opts = dict(top_opts)
            top_opts["page_size"] = str(per_level_tops[0])
        top_extra = combine_filters(
            cursor_filter if cursor_level == 0 else None,
            segment_filters.get(0),
        )
        query = self._format_query_params(
            top_opts,
            top_extra,
            cursor_order if cursor_level == 0 else None,
        )
        if not children:
            return f"{base}?{query}"
        user_leaf_filter = opts.get("filter")
        inner = ""
        for i in range(len(children) - 1, -1, -1):
            is_leaf = i == len(children) - 1
            # ``per_level_tops`` is indexed by segment (0 = top,
            # 1 = first child, …). ``children[i]`` is segment i+1.
            parts: list[str] = [f"$top={per_level_tops[i + 1]}"]
            level_filter = combine_filters(
                cursor_filter if cursor_level == i + 1 else None,
                segment_filters.get(i + 1),
                user_leaf_filter if is_leaf else None,
            )
            if cursor_level == i + 1 and cursor_select:
                parts.append(f"$select={cursor_select}")
            if level_filter:
                parts.append(f"$filter={level_filter}")
            if cursor_level == i + 1 and cursor_order:
                parts.append(f"$orderby={cursor_order}")
            if inner:
                parts.append(f"$expand={inner}")
            inner = f"{children[i]}({';'.join(parts)})"
        return f"{base}?{query}&$expand={inner}"

    # --- read paths --------------------------------------------------------

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
        """
        if len(segments) < 2:
            return {}
        state = self._metadata_state()
        cache_key = (tuple(segments), namespace)
        cached = state.fk_columns.get(cache_key)
        if cached is not None:
            return cached
        leaf_field_names = {
            f.name
            for f in self._own_fields_for_et(
                self._entity_type_for(CONTAINED_PATH_SEP.join(segments), namespace)
            )
        }
        used = set(leaf_field_names)
        resolved: dict[tuple[str, str], str] = {}
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
                order_by: str | None = None
                if level == cursor_level:
                    if cursor_field not in select_cols:
                        select_cols.append(cursor_field)
                    extra_filter = self._cursor_filter(cursor_field, since)
                    terms = [f"{cursor_field} asc"]
                    terms.extend(f"{pk} asc" for pk in ancestor_pks if pk != cursor_field)
                    order_by = ",".join(terms)
                opts = {
                    "page_size": (table_options or {}).get("page_size", "1000"),
                    "select": ",".join(select_cols),
                }
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
                opts = {
                    "page_size": (table_options or {}).get("page_size", "1000"),
                    "select": ",".join(ancestor_pks),
                }
                if segment_filters.get(level):
                    opts["filter"] = segment_filters[level]
                url = (
                    self._build_url(segments[0], opts)
                    if level == 0
                    else self._build_contained_url(sub_segments, chain, opts)
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

        def _emit() -> Iterator[dict]:
            for chain in self._iter_parent_key_chains(segments, namespace, table_options):
                url = self._build_contained_url(
                    segments, chain, table_options, extra_filter=leaf_extra
                )
                for row in self._fetch_pages(url):
                    self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                    yield row

        return _emit(), {}

    def _read_contained_expand(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Single GET with nested ``$expand``; flatten the response into
        leaf rows tagged with ancestor FKs. When ``cursor_field`` is
        set, a ``$filter``/``$orderby`` is injected at the closest
        segment that owns the cursor (top-level query or inner
        ``$expand``), restricting the response to changed subtrees.
        Emitted leaf rows are stamped with the cursor value from that
        segment when they don't carry it themselves. Server depth caps
        surface as HTTP 4xx — no client-side fallback.

        The pull is capped at ``max_records_per_batch`` rows (default
        100k). When the cap fires and the server has more top-level
        pages, the server's top-level ``@odata.nextLink`` is parked in
        the resume offset as ``chain_next_link`` so the next ``read()``
        call hands it straight back to the server (opaque skiptoken;
        no URL rebuild). For cursor mode the watermark only advances
        once the chain fully drains — mid-chain advance would skip
        unread rows under the same ``since``. While a chain is in
        flight the running max cursor lives at
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
        emitted: list[dict] = []
        ctx = (cursor_field, cursor_level, None) if cursor_field else None
        # Per-level $top values from the initial dynamic distribution.
        # Passed into the flatten recursion so inner-collection nextLink
        # follows can rewrite their $top to a larger value without
        # blowing the page_size budget.
        per_level_tops = compute_dynamic_tops(
            int((table_options or {}).get("page_size", "1000")), len(segments)
        )
        remaining_queue = self._drain_expand_pages(
            initial_queue,
            max_records,
            segments,
            pks_per_level,
            fk_columns,
            emitted,
            ctx,
            per_level_tops,
        )
        end_offset = self._build_expand_end_offset(
            emitted, cursor_field, start_offset, remaining_queue
        )
        if not cursor_field:
            return iter(emitted), end_offset
        if not emitted and not resuming:
            return iter([]), start_offset or {}
        if start_offset and start_offset == end_offset:
            if emitted:
                # Rows emitted but offset didn't advance — happens
                # when every row in this batch has a null cursor and
                # ``running_max_cursor`` couldn't update. Returning
                # the rows would loop forever (Spark calls again with
                # the same offset); dropping them silently would lose
                # data. Surface the problem instead.
                raise RuntimeError(
                    f"emitted {len(emitted)} rows from {table_name!r} but cursor_field="
                    f"{cursor_field!r} did not advance — every row in this batch "
                    f"has a null {cursor_field}. Make the cursor non-nullable in "
                    f"the source, filter out null-cursor rows with "
                    f"`filter`/`filter_at_<segment>=<field> ne null`, or pick a "
                    f"cursor that is always populated."
                )
            return iter([]), start_offset
        return iter(emitted), end_offset

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
        per_level_tops: list[int],
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
                    per_level_tops,
                    response_url=url,
                    pending_fetches=queue,
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

    def _fetch_one_expand_page(self, url: str) -> tuple[list[dict], str | None]:
        """One HTTP GET; returns ``(page_rows, next_url)``. Thin wrapper
        over :meth:`_fetch_pages_with_links` that consumes a single
        iteration so the caller can check the cap between fetches."""
        for page_rows, page_next_url in self._fetch_pages_with_links(url):
            return page_rows, page_next_url
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
        return dict(start_offset or {})

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
                page_budget = 1
                for t in per_level_tops:
                    page_budget *= t
                new_top = max(MIN_DYNAMIC_TOP, page_budget // max(1, inner_product))
                resolved = rewrite_top_in_url(resolved, new_top)
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
            # children resolve THEIR relative nextLinks correctly.
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

    def _leaf_cursor_order_by(
        self, table_name: str, namespace: str | None, cursor_field: str
    ) -> str:
        """``cursor asc, pk1 asc, ...`` — unique total order so server
        skiptokens don't split same-cursor cohorts."""
        leaf_pks = self._own_primary_keys_for_et(self._entity_type_for(table_name, namespace))
        terms = [f"{cursor_field} asc"]
        terms.extend(f"{pk} asc" for pk in leaf_pks if pk != cursor_field)
        return ",".join(terms)

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
    ) -> tuple[list[dict], bool, int, int, str | None]:
        """Drive the per-parent fetch loop (leaf-cursor mode).

        ``chains_iter`` is consumed lazily: the parent-chain enumeration
        only walks far enough to reach the chain that hits the
        ``max_records`` cap, and is abandoned (along with all its
        unfetched ancestor pages) the moment the loop breaks. Peak
        memory is bounded to one chain regardless of subtree fan-out.

        Resume preference, applied to the chain at ``parent_idx_start``:

        1. ``chain_next_link`` (server skiptoken) — fetched directly,
           bypassing URL rebuild. Used when the previous batch parked
           at a page boundary mid-chain.
        2. ``truncated_chain_cursor`` — used as ``cursor gt <value>``
           in a freshly-built URL. Used when the previous batch had
           to apply the Option A boundary trim at mid-page truncation
           (server skiptoken couldn't represent the row-level position).
        3. Otherwise the global ``since`` is used.

        Pagination is page-aware: rows are emitted as each page arrives,
        and a truncation that lands at a page boundary checkpoints with
        the response's @odata.nextLink. Mid-page truncations leave
        ``chain_next_link_out`` as ``None`` so the caller falls back to
        the Option A trim path.

        Returns ``(rows, truncated, parent_idx, truncated_chain_start_idx,
        chain_next_link_out)``."""
        emitted: list[dict] = []
        truncated = False
        parent_idx = 0
        chain_start_idx = 0
        chain_next_link_out: str | None = None
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
            for page_rows, page_next_url in self._fetch_pages_with_links(initial_url):
                for row in page_rows:
                    rec_cursor = row.get(cursor_field)
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
                    # Finish the current page so the server's nextLink
                    # (page-after-this-one) is a clean checkpoint, but
                    # don't fetch any more pages for this chain.
                    truncated = True
                    # ``page_next_url`` is None ⇒ chain ended on this page;
                    # caller falls back to Option A trim. Non-None ⇒
                    # caller parks chain_next_link.
                    chain_next_link_out = page_next_url
                    break
            if truncated:
                break
            parent_idx += 1
        return emitted, truncated, parent_idx, chain_start_idx, chain_next_link_out

    def _read_contained_incremental(
        self,
        table_name: str,
        start_offset: dict,
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
        if cursor_level == len(segments) - 1:
            return self._read_contained_incremental_leaf_cursor(
                segments, start_offset, table_options, cursor_field
            )
        return self._read_contained_incremental_ancestor_cursor(
            segments, start_offset, table_options, cursor_field, cursor_level
        )

    def _read_contained_incremental_leaf_cursor(
        self,
        segments: list[str],
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Cursor lives on the leaf entity — filter at the leaf fetch.

        Truncation has two recovery shapes, applied to the truncated
        chain only (subsequent chains keep using the original
        ``since``, since per-chain cursor distributions are
        independent):

        * **NextLink (preferred)**: when truncation lands on a page
          boundary, the server's @odata.nextLink is parked in the
          offset as ``chain_next_link`` and the resumed call hands it
          straight back to the server. No client-side filter
          reconstruction; no boundary-cohort math.
        * **Option A trim (fallback)**: when truncation lands
          mid-page (the skiptoken can't represent a row-level
          position), the trailing same-cursor cohort within the
          truncated chain's emit is dropped and the chain's last
          distinct cursor is parked as ``truncated_chain_cursor``.
          The resumed call rebuilds the URL with
          ``cursor gt truncated_chain_cursor`` for that chain only.
          If trimming leaves the chain with zero rows, the cohort
          exceeded ``max_records_per_batch`` on its own and the
          connector raises ``RuntimeError`` — same shape as the
          flat-path failure mode.
        """
        namespace = (table_options or {}).get("namespace")
        table_name = CONTAINED_PATH_SEP.join(segments)
        since = (start_offset or {}).get("cursor")
        truncated_chain_cursor_in = (start_offset or {}).get("truncated_chain_cursor")
        chain_next_link_in = (start_offset or {}).get("chain_next_link")
        max_records = int((table_options or {}).get("max_records_per_batch", "10000"))
        order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
        chains_iter = self._iter_parent_key_chains(segments, namespace, table_options)
        segment_filters = resolve_segment_filters(table_options, segments)
        (
            emitted,
            truncated,
            parent_idx,
            chain_start_idx,
            chain_next_link_out,
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
        )
        if truncated:
            if chain_next_link_out is not None:
                end_offset: dict = {
                    "parent_idx": parent_idx,
                    "chain_next_link": chain_next_link_out,
                }
                if since is not None:
                    end_offset["cursor"] = since
            else:
                trimmed_chain = _trim_to_distinct_cursor_boundary(
                    emitted[chain_start_idx:], cursor_field
                )
                if not trimmed_chain:
                    raise RuntimeError(
                        f"max_records_per_batch={max_records} is smaller than "
                        f"the largest same-cursor cohort under one parent in "
                        f"contained path {table_name!r}. Raise "
                        f"max_records_per_batch above that cohort, or pick a "
                        f"higher-cardinality cursor."
                    )
                emitted = emitted[:chain_start_idx] + trimmed_chain
                end_offset = {
                    "parent_idx": parent_idx,
                    "truncated_chain_cursor": trimmed_chain[-1].get(cursor_field),
                }
                if since is not None:
                    end_offset["cursor"] = since
        else:
            if not emitted:
                return iter([]), start_offset or {}
            cursors = [r.get(cursor_field) for r in emitted if r.get(cursor_field) is not None]
            end_offset = {"cursor": max(cursors) if cursors else since}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(emitted), end_offset

    def _read_contained_incremental_ancestor_cursor(
        self,
        segments: list[str],
        start_offset: dict,
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
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(walk_state["emitted"]), end_offset

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
                    segments, chain, table_options, extra_filter=leaf_segment_filter
                )
            page_next_url: str | None = None
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
