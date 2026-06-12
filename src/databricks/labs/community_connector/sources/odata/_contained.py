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

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator
from xml.etree import ElementTree as ET

from pyspark.sql.types import StructField


# Path-segment separator. ``__`` (double underscore), not ``/``, so
# the framework can interpolate slash-free table names directly into
# Spark SQL identifiers (view names, temp views). The OData URL path
# still uses ``/`` — that's hardcoded in ``_build_contained_path``.
CONTAINED_PATH_SEP = "__"


def _trim_to_distinct_cursor_boundary(
    records: list[dict],
    cursor_field: str,
) -> list[dict]:
    """Drop trailing records sharing the boundary cursor value. Same
    semantics as the flat-path helper in ``odata.py`` — duplicated here
    to avoid a circular import. Returns empty list when every record
    shares one cursor; caller decides recoverable vs hard failure."""
    if not records:
        return records
    boundary = records[-1].get(cursor_field)
    trim_idx = len(records)
    while trim_idx > 0 and records[trim_idx - 1].get(cursor_field) == boundary:
        trim_idx -= 1
    return records[:trim_idx]


# Inside generated OData request URLs the segment separator is always
# a forward slash (the wire format the spec mandates).
_URL_SEGMENT_SEP = "/"
# Cap on path depth. Prevents pathological discovery walks on services
# with cyclic containment graphs; cycles within the cap are also
# detected via target-type tracking.
MAX_CONTAINED_DEPTH = 5


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
        root = self._metadata_root()
        type_to_qname: dict[ET.Element, str] = {
            et: f"{schema.get('Namespace') or ''}.{et.get('Name')}"
            for schema in root.iter(f"{_NS_EDM}Schema")
            for et in schema.findall(f"{_NS_EDM}EntityType")
        }
        queue: list[tuple[list[str], ET.Element, set[str]]] = [
            ([top_level_set], root_et, {type_to_qname.get(root_et, "")})
        ]
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
        """``A?...&$expand=B($expand=C($expand=D))`` for the full chain.

        When ``cursor_level`` is set, ``cursor_filter``/``cursor_order``/
        ``cursor_select`` are injected at the segment that owns the
        cursor — at the top-level URL when ``cursor_level == 0``, or
        inside the corresponding ``$expand`` clause otherwise. The
        ``$select`` is necessary because some OData servers omit
        properties from ``$expand`` responses by default; explicitly
        requesting the cursor guarantees the server projects it onto
        the ancestor rows so it can be stamped onto leaf rows. OData
        v4 §5.1.1.13: inner ``$expand`` options are separated by ``;``."""
        top, *children = segments
        base = join_url(self.service_url, top)
        if cursor_level == 0:
            query = self._format_query_params(table_options, cursor_filter, cursor_order)
        else:
            query = self._format_query_params(table_options, None, None)
        if not children:
            return f"{base}?{query}"
        inner = ""
        for i in range(len(children) - 1, -1, -1):
            parts: list[str] = []
            if cursor_level == i + 1:
                if cursor_select:
                    parts.append(f"$select={cursor_select}")
                if cursor_filter:
                    parts.append(f"$filter={cursor_filter}")
                if cursor_order:
                    parts.append(f"$orderby={cursor_order}")
            if inner:
                parts.append(f"$expand={inner}")
            inner = f"{children[i]}({';'.join(parts)})" if parts else children[i]
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
    ) -> Iterator[tuple[list[dict[str, Any]], Any]]:
        """Like ``_iter_parent_key_chains`` but applies a cursor filter at
        the ancestor that owns ``cursor_field``. Yields
        ``(chain, ancestor_cursor_value)`` pairs; the cursor value is the
        value at ``cursor_level`` for that chain."""

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
            for row in self._fetch_pages(url):
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
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield every ancestor key chain (len = len(segments) - 1) reaching
        the leaf. Each level fetched with ``$select=<pks>``; user ``filter``
        not forwarded."""

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
            opts = {
                "page_size": (table_options or {}).get("page_size", "1000"),
                "select": ",".join(ancestor_pks),
            }
            url = (
                self._build_url(segments[0], opts)
                if level == 0
                else self._build_contained_url(sub_segments, chain, opts)
            )
            for row in self._fetch_pages(url):
                chain.append({pk: row.get(pk) for pk in ancestor_pks})
                yield from _walk(level + 1, chain)
                chain.pop()

        yield from _walk(0, [])

    def _read_contained_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Walk the parent-key tree N+1 and emit leaf rows tagged with
        ancestor FKs. Full result in one call."""
        segments = parse_contained_path(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        fk_columns = self._resolve_fk_columns(segments, namespace)
        emitted: list[dict] = []
        for chain in self._iter_parent_key_chains(segments, namespace, table_options):
            for row in self._fetch_pages(self._build_contained_url(segments, chain, table_options)):
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                emitted.append(row)
        return iter(emitted), {}

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
        surface as HTTP 4xx — no client-side fallback."""
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
        url = self._build_expand_url(
            segments,
            table_options,
            cursor_level=cursor_level if cursor_field else None,
            cursor_filter=cursor_filter,
            cursor_order=cursor_order,
            cursor_select=cursor_select,
        )
        emitted: list[dict] = []
        ctx = (cursor_field, cursor_level, None) if cursor_field else None
        for top_row in self._fetch_pages(url):
            self._flatten_expand_response(
                0, top_row, segments, pks_per_level, [], fk_columns, emitted, ctx
            )
        if not cursor_field:
            return iter(emitted), {}
        if not emitted:
            return iter([]), start_offset or {}
        cursors = [r.get(cursor_field) for r in emitted if r.get(cursor_field) is not None]
        end_offset: dict = {"cursor": max(cursors)} if cursors else dict(start_offset or {})
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(emitted), end_offset

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
    ) -> None:
        """Recurse into the nested $expand payload; tag and emit leaf rows.
        ``cursor_ctx`` is ``(cursor_field, cursor_level, captured_value)``
        threaded down the recursion: when ``level == cursor_level`` the
        captured value snaps to ``row[cursor_field]`` and propagates to
        every leaf row beneath, stamped only when the leaf doesn't
        already carry the column."""
        cur_field, cur_level, cur_val = cursor_ctx or (None, -1, None)
        if cur_field and level == cur_level:
            cur_val = row.get(cur_field)
        if level == len(segments) - 1:
            clean = {k: v for k, v in row.items() if not k.startswith("@odata.")}
            self._tag_with_ancestor_fks(clean, segments, chain, fk_columns)
            if cur_field and cur_val is not None and clean.get(cur_field) is None:
                clean[cur_field] = cur_val
            out.append(clean)
            return
        pks = pks_per_level[level]
        chain.append({pk: row.get(pk) for pk in pks})
        next_ctx = (cur_field, cur_level, cur_val) if cur_field else None
        for child in row.get(segments[level + 1]) or []:
            self._flatten_expand_response(
                level + 1, child, segments, pks_per_level, chain, fk_columns, out, next_ctx
            )
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
        chains: list[list[dict[str, Any]]],
        parent_idx_start: int,
        table_options: dict[str, str],
        order_by: str,
        cursor_field: str,
        since: Any,
        truncated_chain_cursor: Any,
        chain_next_link: str | None,
        max_records: int,
        fk_columns: dict[tuple[str, str], str],
    ) -> tuple[list[dict], bool, int, int, str | None]:
        """Drive the per-parent fetch loop (leaf-cursor mode).

        Resume preference, applied to ``chains[parent_idx_start]`` only:

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
        parent_idx = parent_idx_start
        chain_start_idx = 0
        chain_next_link_out: str | None = None
        while parent_idx < len(chains):
            chain = chains[parent_idx]
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
                    extra_filter=self._cursor_filter(cursor_field, chain_since),
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
        max_records = int((table_options or {}).get("max_records_per_batch", "100000"))
        order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
        chains = list(self._iter_parent_key_chains(segments, namespace, table_options))
        (
            emitted,
            truncated,
            parent_idx,
            chain_start_idx,
            chain_next_link_out,
        ) = self._walk_contained_with_cursor(
            segments,
            chains,
            int((start_offset or {}).get("parent_idx", 0)),
            table_options,
            order_by,
            cursor_field,
            since,
            truncated_chain_cursor_in,
            chain_next_link_in,
            max_records,
            self._resolve_fk_columns(segments, namespace),
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
        is no meaningful split. The server's @odata.nextLink, on the
        other hand, encodes the chain's pagination position
        independently of cursor values, so the resumed call hands it
        back and the server picks up exactly where it stopped.

        On a truncation:

        * If we stopped at a page boundary mid-chain (next page exists),
          park ``chain_next_link`` in the offset.
        * If we stopped because the chain happened to end on the
          truncating page, just advance ``parent_idx`` past it.
        """
        namespace = (table_options or {}).get("namespace")
        since = (start_offset or {}).get("cursor")
        chain_next_link_in = (start_offset or {}).get("chain_next_link")
        max_records = int((table_options or {}).get("max_records_per_batch", "100000"))
        fk_columns = self._resolve_fk_columns(segments, namespace)
        chains_with_cursor = list(
            self._iter_parent_chains_with_cursor(
                segments,
                namespace,
                table_options,
                cursor_level,
                cursor_field,
                since,
            )
        )
        parent_idx_start = int((start_offset or {}).get("parent_idx", 0))
        parent_idx = parent_idx_start
        emitted: list[dict] = []
        truncated = False
        chain_next_link_out: str | None = None
        while parent_idx < len(chains_with_cursor):
            chain, ancestor_cursor = chains_with_cursor[parent_idx]
            if parent_idx == parent_idx_start and chain_next_link_in is not None:
                initial_url = chain_next_link_in
            else:
                initial_url = self._build_contained_url(segments, chain, table_options)
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
                # Page-boundary checkpoint: mid-chain only if the chain has
                # more pages left. Otherwise the chain finished on the
                # truncating page and we can simply advance parent_idx.
                if page_next_url is not None:
                    chain_next_link_out = page_next_url
                else:
                    parent_idx += 1
                    chain_next_link_out = None
                break
            parent_idx += 1
        # Offset semantics:
        #
        # * On truncation, preserve the **original** ``since`` (start
        #   offset's cursor) so the resumed call's chain rebuild covers
        #   the same set as the prior batch. Ancestor cursors interleave
        #   across top-level parents (depth-first walk), so advancing
        #   ``since`` to the global max would silently skip lower-cursor
        #   chains under not-yet-walked parents.
        # * Carry ``running_max`` across resume batches: every batch
        #   max-merges its own emitted cursors with what's already in
        #   ``running_max``. On natural completion that accumulated
        #   value becomes the next regular trigger's ``cursor`` floor —
        #   without it, completing a resume that originated from
        #   ``since=None`` would drop the cursor entirely and re-walk
        #   the whole table on the next trigger.
        # * Cross-batch re-emission of already-seen chains during the
        #   resume cycle is deduped by ``apply_changes`` on the
        #   composite PK; correctness over minimal bandwidth.
        end_offset: dict
        cursors = [r.get(cursor_field) for r in emitted if r.get(cursor_field) is not None]
        this_batch_max = max(cursors) if cursors else None
        prev_running_max = (start_offset or {}).get("running_max")
        if this_batch_max is not None and prev_running_max is not None:
            new_running_max: Any = max(this_batch_max, prev_running_max)
        elif this_batch_max is not None:
            new_running_max = this_batch_max
        else:
            new_running_max = prev_running_max
        if truncated:
            end_offset = {"parent_idx": parent_idx}
            if since is not None:
                end_offset["cursor"] = since
            if chain_next_link_out is not None:
                end_offset["chain_next_link"] = chain_next_link_out
            if new_running_max is not None:
                end_offset["running_max"] = new_running_max
        else:
            if new_running_max is not None:
                end_offset = {"cursor": new_running_max}
            elif since is not None:
                end_offset = {"cursor": since}
            else:
                end_offset = {}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(emitted), end_offset
