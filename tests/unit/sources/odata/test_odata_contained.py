"""OData connector unit tests — contained group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import logging
import re

import pytest
import responses

from tests.unit.sources.odata._odata_test_helpers import *  # noqa: F401,F403


@responses.activate
def test_contained_leaf_service_root_relative_nextlink_does_not_double_path():
    """Regression: a leaf-collection ``@odata.nextLink`` returned as a
    path relative to the **service root** (``Parents(1)/Children(11)/
    Notes?$skiptoken=...`` — the Hexagon/SAP style) must not be naively
    ``urljoin``-ed against the deep request URL, which would duplicate
    the ancestor path and 404 the next page — silently dropping every
    page after the first on a contained snapshot. It must resolve
    against the service root."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": [{"Id": 11}]})
    # Leaf page 1 carries a SERVICE-ROOT-relative nextLink (no host, and
    # it restates the full ancestor path from the top entity set).
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes",
        json={
            "value": [{"Id": 101, "Text": "a"}],
            "@odata.nextLink": "Parents(1)/Children(11)/Notes?$skiptoken=n2",
        },
        match_querystring=False,
    )
    # Correct resolution = service_root + the relative link. The doubled
    # path (.../Notes/Parents(1)/Children(11)/Notes) is deliberately NOT
    # registered, so the old behavior would error / drop page 2.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes?$skiptoken=n2",
        json={"value": [{"Id": 102, "Text": "b"}]},
    )

    c = _make()
    records, _ = c.read_table("Parents__Children__Notes", None, {})
    rows = list(records)
    assert [r["Id"] for r in rows] == [101, 102]


# --- exclude_ancestor_columns ---------------------------------------------


@responses.activate
def test_exclude_ancestor_columns_drops_from_schema():
    """A named ancestor-FK column is removed from the leaf schema; the
    other ancestor FK and the leaf's own fields are untouched."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema(
        "Parents__Children__Notes", {"exclude_ancestor_columns": "Parents_Id"}
    )
    names = [f.name for f in schema.fields]
    assert names == ["Children_Id", "Id", "Text"]


@responses.activate
def test_exclude_ancestor_columns_drops_from_primary_key():
    """The excluded column also leaves the composite primary key — schema
    and key stay consistent (a key column can't reference a dropped
    schema field)."""
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata(
        "Parents__Children__Notes", {"exclude_ancestor_columns": "Parents_Id"}
    )
    assert meta["primary_keys"] == ["Children_Id", "Id"]


@responses.activate
def test_exclude_ancestor_columns_multiple_names():
    """A comma-separated list drops every named FK column at once."""
    _mock_nested_metadata()
    c = _make()
    opts = {"exclude_ancestor_columns": "Parents_Id, Children_Id"}
    schema = c.get_table_schema("Parents__Children__Notes", opts)
    assert [f.name for f in schema.fields] == ["Id", "Text"]
    meta = c.read_table_metadata("Parents__Children__Notes", opts)
    assert meta["primary_keys"] == ["Id"]


@responses.activate
def test_exclude_ancestor_columns_not_stamped_on_rows():
    """Emitted rows omit the excluded FK column — the exclusion reaches
    the row-tagging path, not just schema/metadata."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    rows, _ = c.read_table(
        "Parents__Children",
        None,
        {"exclude_ancestor_columns": "Parents_Id", "pagination": "nextlink"},
    )
    rows = list(rows)
    assert rows == [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]
    assert all("Parents_Id" not in r for r in rows)


@responses.activate
def test_exclude_ancestor_columns_keeps_leaf_table_column(caplog):
    """Only synthetic ancestor-FK columns can be dropped — naming a real
    leaf/own table column leaves it in place (and warns that it's kept)."""
    _mock_nested_metadata()
    c = _make()
    with caplog.at_level(logging.WARNING):
        schema = c.get_table_schema(
            # ``Label`` is one of Children's own properties, not an FK.
            "Parents__Children",
            {"exclude_ancestor_columns": "Label"},
        )
    names = [f.name for f in schema.fields]
    # Leaf column survives; the FK column is untouched too.
    assert "Label" in names
    assert "Parents_Id" in names
    assert names == ["Parents_Id", "Id", "Label", "ModifiedAt"]
    assert any(
        "table columns" in r.getMessage() and "Label" in r.getMessage() for r in caplog.records
    )


@responses.activate
def test_exclude_ancestor_columns_keeps_leaf_column_in_primary_key(caplog):
    """A leaf column that is part of the composite PK is never removed
    from the key by exclude_ancestor_columns — only ancestor FKs are."""
    _mock_nested_metadata()
    c = _make()
    with caplog.at_level(logging.WARNING):
        # ``Category`` is one of the Tag leaf's own PK columns.
        meta = c.read_table_metadata("Parents__Tags", {"exclude_ancestor_columns": "Category"})
    assert meta["primary_keys"] == ["Parents_Id", "Category", "Value"]


@responses.activate
def test_exclude_ancestor_columns_unknown_name_warns_and_noops(caplog):
    """A name matching no FK column has no effect and logs a warning so a
    typo doesn't silently leave the column in place."""
    _mock_nested_metadata()
    c = _make()
    with caplog.at_level(logging.WARNING):
        schema = c.get_table_schema("Parents__Children", {"exclude_ancestor_columns": "Nope_Id"})
    assert [f.name for f in schema.fields] == ["Parents_Id", "Id", "Label", "ModifiedAt"]
    assert any(
        "exclude_ancestor_columns" in r.getMessage() and "Nope_Id" in r.getMessage()
        for r in caplog.records
    )


@responses.activate
def test_exclude_ancestor_columns_ignored_on_flat_table(caplog):
    """Flat tables have no ancestor FK columns; the option is a harmless
    no-op and doesn't warn (a connection-wide default shouldn't spam the
    log for every flat table it touches)."""
    _mock_metadata()
    c = _make()
    with caplog.at_level(logging.WARNING):
        schema = c.get_table_schema("Customers", {"exclude_ancestor_columns": "Parents_Id"})
    # Same as the unadorned Customers schema — option has no effect.
    assert [f.name for f in schema.fields] == [
        f.name for f in c.get_table_schema("Customers", {}).fields
    ]
    assert not any("exclude_ancestor_columns" in r.getMessage() for r in caplog.records)


@responses.activate
def test_exclude_ancestor_columns_wildcard_drops_all_fk_columns():
    """A lone ``*`` drops every synthetic ancestor-FK column at once,
    leaving only the leaf's own fields in the schema."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children__Notes", {"exclude_ancestor_columns": "*"})
    assert [f.name for f in schema.fields] == ["Id", "Text"]


@responses.activate
def test_exclude_ancestor_columns_wildcard_drops_all_from_primary_key():
    """``*`` also strips every ancestor FK from the composite key, leaving
    just the leaf's own PK."""
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children__Notes", {"exclude_ancestor_columns": "*"})
    assert meta["primary_keys"] == ["Id"]


@responses.activate
def test_exclude_ancestor_columns_wildcard_does_not_warn(caplog):
    """``*`` is an intentional drop-all, not a typo — no warning."""
    _mock_nested_metadata()
    c = _make()
    with caplog.at_level(logging.WARNING):
        c.get_table_schema("Parents__Children", {"exclude_ancestor_columns": "*"})
    assert not any("exclude_ancestor_columns" in r.getMessage() for r in caplog.records)


@responses.activate
def test_exclude_ancestor_columns_wildcard_keeps_leaf_columns_in_rows():
    """Even under ``*`` the leaf's own columns are never dropped from the
    emitted rows — only ancestor FKs are."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    rows, _ = c.read_table(
        "Parents__Children",
        None,
        {"exclude_ancestor_columns": "*", "pagination": "nextlink"},
    )
    rows = list(rows)
    assert rows == [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]
    assert all("Parents_Id" not in r for r in rows)


@responses.activate
def test_contained_expand_drains_inner_collection_page_limited_below_top():
    """Regression: a server that page-limits a nested ``$expand`` BELOW the
    requested ``$top`` and omits its ``<NavProp>@odata.nextLink`` returns a
    SHORT inline leaf page that is NOT complete. Under the default ``auto``
    the connector must probe past the short inline page and drain the rest —
    otherwise the trailing leaf rows (the user-reported missing deep records)
    are silently lost AND, in a streaming read, the watermark advances past
    them. Before the fix a short inline page was taken as proof of exhaustion.

    Distinct from ``..._continues_inner_expand_when_nextlink_omitted`` (which
    truncates on a FULL inline page == ``$top``): here the inline page is
    SHORT, the case the old full-page-only continuation heuristic missed."""
    _mock_nested_metadata()
    # page_size=1000 over a 3-level expand → Notes $top=5 (compute_dynamic_tops).
    # The server hands back only 3 inline Notes (BELOW $top) with no
    # Notes@odata.nextLink, while two more (Ids 4,5) exist behind a probe.
    all_notes = [{"Id": i, "Text": f"n{i}"} for i in range(1, 6)]  # 1..5

    def _floor(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        m = re.search(r"Id gt (\d+)", flt)
        return int(m.group(1)) if m else 0

    def _parents(request):
        # Top-level drain probe past the single parent returns empty.
        if _floor(request):
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps({"value": [{"Id": 1, "Children": [{"Id": 10, "Notes": all_notes[:3]}]}]}),
        )

    def _notes(request):
        # The inner drain probe: return Notes after the seek boundary, so the
        # walk pulls Ids 4,5 then an empty page (Id gt 5) and stops.
        return (200, {}, json.dumps({"value": [n for n in all_notes if n["Id"] > _floor(request)]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    # The single inline child is itself a short link-less page, so its
    # collection is probed too — empty (only Child 10 exists).
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(1)/Children(10)/Notes", callback=_notes
    )
    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        None,
        {"expand_contained": "true", "page_size": "1000"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2, 3, 4, 5]  # none dropped past the short page
    assert all(r["Parents_Id"] == 1 and r["Children_Id"] == 10 for r in rows)


@responses.activate
def test_contained_expand_nextlink_mode_warns_inner_truncation_risk(caplog):
    """``expand_contained=true`` + ``pagination=nextlink`` disables the
    client-driven inner-$expand drain, so a link-omitting server silently
    truncates. The connector warns so the silent data loss isn't invisible."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    c = _make()
    with caplog.at_level("WARNING"):
        list(
            c.read_table(
                "Parents__Children",
                None,
                {"expand_contained": "true", "pagination": "nextlink"},
            )[0]
        )
    assert any(
        "pagination=nextlink" in r.message and "silently dropped" in r.message
        for r in caplog.records
    )


@responses.activate
def test_contained_expand_auto_mode_no_inner_truncation_warning(caplog):
    """The default ``auto`` self-heals inner collections, so no truncation
    warning fires — the warning is specific to nextlink mode."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    c = _make()
    with caplog.at_level("WARNING"):
        list(c.read_table("Parents__Children", None, {"expand_contained": "true"})[0])
    assert not any("silently dropped" in r.message for r in caplog.records)


@responses.activate
def test_contained_expand_cursor_drains_capped_inner_collection_multi_parent():
    """Regression (xmla_demo): ``expand_contained=true`` + cursor on a server
    that caps every response BELOW the requested $top and omits the
    continuation link. The deep-continuation $top budget can exceed the cap, so
    each inner-collection continuation page is SHORT — the drainer must keep
    seeking, not stop. If it stops, the inner collection is truncated AND (in
    cursor mode) the watermark advances past the dropped rows, losing them
    across batches. The two parents live in DISJOINT cursor ranges (parent 1
    high, parent 2 low): when parent 1's continuation drains/stops, the global
    watermark jumps into 2025; if parent 2's continuation then stops short, its
    dropped 2024 rows fall below that watermark and the next batch's
    ``cursor gt`` skips them forever. All rows must come through, exactly once."""
    from urllib.parse import parse_qs, unquote, urlparse

    _mock_nested_metadata()
    # ``cap`` equals the inner-expand $top (page_size default 1000 -> Children
    # $top=10), so the inline page is FULL and a continuation IS built; the
    # continuation's $top is the full budget (1000) >> cap, so its pages are
    # short and must be drained. Parent 1 in 2025 (high), parent 2 in 2024 (low).
    cap = 10
    kids = {
        1: [
            {"Id": 100 + i, "Label": f"a{i}", "ModifiedAt": f"2025-01-{i:02d}T00:00:00Z"}
            for i in range(1, 26)
        ],
        2: [
            {"Id": 200 + i, "Label": f"b{i}", "ModifiedAt": f"2024-01-{i:02d}T00:00:00Z"}
            for i in range(1, 26)
        ],
    }

    def _seek(flt):
        """Return a predicate from a cursor/keyset $filter string."""
        gt = re.search(r"ModifiedAt gt ([0-9T:\-Z]+)", flt)
        eqid = re.search(r"ModifiedAt eq ([0-9T:\-Z]+) and Id gt (\d+)", flt)

        def keep(r):
            if not flt:
                return True
            if gt and r["ModifiedAt"] > gt.group(1):
                return True
            return bool(eqid and r["ModifiedAt"] == eqid.group(1) and r["Id"] > int(eqid.group(2)))

        return keep

    def _page(rows, flt):
        kept = sorted((r for r in rows if _seek(flt)(r)), key=lambda r: (r["ModifiedAt"], r["Id"]))
        return kept[:cap]  # capped, no nextLink

    def _parents(request):
        q = parse_qs(urlparse(request.url).query)
        top_filter = unquote(q.get("$filter", [""])[0])  # Parents-level drain seek
        if "Id gt" in top_filter:
            return (200, {}, json.dumps({"value": []}))  # past the last parent
        expand = unquote(q.get("$expand", [""])[0])  # Children(...;$filter=...;...)
        m = re.search(r"\$filter=([^;)]*)", expand)
        cflt = m.group(1) if m else ""
        out = [{"Id": pid, "Name": f"P{pid}", "Children": _page(kids[pid], cflt)} for pid in (1, 2)]
        return (200, {}, json.dumps({"value": out}))

    def _children(pid):
        def cb(request):
            flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
            return (200, {}, json.dumps({"value": _page(kids[pid], flt)}))

        return cb

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_children(1)
    )
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(2)/Children", callback=_children(2)
    )

    c = _make()
    seen, dups, offset, b = [], 0, {}, 0
    while b < 50:
        b += 1
        recs, new = c.read_table(
            "Parents__Children",
            offset,
            {
                "cursor_field": "ModifiedAt",
                "expand_contained": "true",
                # Dedup off: this test pins the DRAIN's strict exactly-once
                # resume guarantee. Default-on dedup re-delivers the overlap
                # once after a capped cycle (its lb_seen shrank to the last
                # slice) — a documented, MERGE-idempotent re-emit that would
                # read as duplicates here.
                "cursor_lookback_dedup": "off",
            },
        )
        got = [(r["Parents_Id"], r["Id"]) for r in recs]
        for k in got:
            if k in seen:
                dups += 1
        seen.extend(got)
        if not got or new == offset:
            break
        offset = new
    assert dups == 0
    assert sorted(set(seen)) == sorted(
        [(1, 100 + i) for i in range(1, 26)] + [(2, 200 + i) for i in range(1, 26)]
    )  # all 50 rows (25/parent), none dropped


# --- N+1 snapshot read ---


@responses.activate
def test_contained_snapshot_two_level_walks_parents_and_tags_fks():
    _mock_nested_metadata()
    # Parent fetch (PKs only)
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 1}, {"Id": 2}]},
    )
    # Per-parent leaf fetches
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={
            "value": [
                {"Id": 21, "Label": "c", "ModifiedAt": "2024-02-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    records, offset = c.read_table("Parents__Children", None, {})
    rows = list(records)
    assert _drop_lb(offset) == {}
    assert len(rows) == 3
    # FK column populated correctly
    assert rows[0]["Parents_Id"] == 1
    assert rows[0]["Id"] == 11
    assert rows[2]["Parents_Id"] == 2
    assert rows[2]["Id"] == 21


@responses.activate
def test_contained_snapshot_three_level_walks_full_chain():
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 10}, {"Id": 20}]},
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "a"}, {"Id": 101, "Text": "b"}]},
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(20)/Notes",
        json={"value": [{"Id": 200, "Text": "c"}]},
    )
    c = _make()
    records, _ = c.read_table("Parents__Children__Notes", None, {})
    rows = list(records)
    assert len(rows) == 3
    # Every ancestor's FK tagged onto the row — required for unique
    # composite keys when leaf IDs only repeat within a parent.
    assert rows[0] == {
        "Parents_Id": 1,
        "Children_Id": 10,
        "Id": 100,
        "Text": "a",
    }
    assert rows[2]["Parents_Id"] == 1
    assert rows[2]["Children_Id"] == 20
    assert rows[2]["Id"] == 200


@responses.activate
def test_contained_snapshot_composite_parent_key_in_url():
    """When the parent has a composite key (Parents__Tags has Tag as a
    composite-PK contained type), the key predicate on nested traversal
    must use the named form. This test uses Parents__Children__Notes which
    has single-key parents — for composite parent URL coverage see
    test_key_predicate_composite + a hand-crafted metadata."""
    # Covered by unit test on _format_key_predicate above; this is a
    # placeholder reminder of the coverage matrix.


# --- $expand mode ---


@responses.activate
def test_contained_expand_two_level_flattens_nested_response():
    _mock_nested_metadata()
    # Single call with nested response
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Name": "P1",
                    "Children": [
                        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
                    ],
                },
                {
                    "Id": 2,
                    "Name": "P2",
                    "Children": [],
                },
            ]
        },
    )
    # The top-level Parents page is short (2 < $top); under the default auto the
    # drainer probes one more page to confirm exhaustion — a real server returns
    # empty past the last parent.
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    # Parent 1's inline Children page is short and link-less → the inner drainer
    # probes past the last child. (Parent 2's Children is empty → no probe.)
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"expand_contained": "true"})
    rows = list(records)
    assert len(rows) == 2
    assert rows[0]["Parents_Id"] == 1
    assert rows[0]["Id"] == 11
    # @odata.* control props are stripped from the flattened leaf rows too
    assert all(not k.startswith("@odata.") for r in rows for k in r)


@responses.activate
def test_contained_expand_three_level_flattens_nested():
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [
                        {
                            "Id": 10,
                            "Notes": [
                                {"Id": 100, "Text": "x"},
                                {"Id": 101, "Text": "y"},
                            ],
                        },
                    ],
                },
            ]
        },
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})  # drain probe past last parent
    # Short, link-less inline child + grandchild pages → inner drain probes.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(1)/Children(10)/Notes", json={"value": []})
    c = _make()
    records, _ = c.read_table("Parents__Children__Notes", None, {"expand_contained": "true"})
    rows = list(records)
    assert len(rows) == 2
    # Every ancestor's FK materialized — same contract as the N+1
    # snapshot path, just delivered via a single nested $expand call.
    assert all(r["Parents_Id"] == 1 and r["Children_Id"] == 10 for r in rows)
    assert {r["Id"] for r in rows} == {100, 101}


@responses.activate
def test_contained_expand_strips_odata_annotations_on_leaf_rows():
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "@odata.etag": "drop-on-parent",
                    "Children": [
                        {
                            "Id": 11,
                            "Label": "a",
                            "ModifiedAt": "2024-01-01T00:00:00Z",
                            "@odata.etag": "drop-on-child",
                        },
                    ],
                },
            ]
        },
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})  # drain probe past last parent
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})  # inner drain probe
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"expand_contained": "true"})
    rows = list(records)
    assert rows == [
        {
            "Parents_Id": 1,
            "Id": 11,
            "Label": "a",
            "ModifiedAt": "2024-01-01T00:00:00Z",
        }
    ]


@responses.activate
def test_contained_expand_inner_nextlink_rewrites_top_for_continuation():
    """When following ``<NavProp>@odata.nextLink``, the connector
    rewrites the URL's ``$top`` so the continuation can use the full
    page_size budget. Without this, a wide inner collection would
    take ``N / inner_top`` round trips at the small dynamic per-level
    ``$top`` (10 for depth-2)."""
    _mock_nested_metadata()
    captured = []

    def _initial(_req):
        # Initial request: Children inline + nextLink (server preserves
        # the original $top=10 from $expand=Children($top=10)).
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}
                            ],
                            "Children@odata.nextLink": (
                                f"{SERVICE_URL}Parents(1)/Children?$top=10&$skip=10"
                            ),
                        }
                    ]
                }
            ),
        )

    def _continuation(req):
        captured.append(req.url)
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_continuation
    )
    c = _make()
    records, _ = c.read_table(
        "Parents__Children", None, {"expand_contained": "true", "page_size": "1000"}
    )
    list(records)
    from urllib.parse import unquote

    # Depth 2, page_size=1000 → per_level_tops=[100, 10]. Continuation
    # at level 1 has no inner expansion, so $top is rewritten to the
    # full budget (1000).
    assert captured, "continuation URL not fetched"
    cont_url = unquote(captured[0])
    assert "$top=1000" in cont_url
    # Make sure the original tiny $top=10 was replaced, not appended.
    assert "$top=10&" not in cont_url


@responses.activate
def test_contained_expand_truncates_mid_page_and_parks_pending_fetches():
    """``_read_contained_expand`` checks the cap after each top_row;
    on overflow the current page URL is re-queued at the front of
    ``pending_fetches`` with ``skip`` advanced past the drained rows
    and the server's next-page URL appears later in the queue. On
    resume the connector re-fetches the same page and skips the
    parked count — wasting one HTTP round trip's worth of data but
    no inner-nextLink work."""
    _mock_nested_metadata()
    next_link = f"{SERVICE_URL}Parents?$skiptoken=p2"

    def _initial(_req):
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"Id": 1, "Children": [{"Id": 11, "Label": "a"}]},
                        {"Id": 2, "Children": [{"Id": 22, "Label": "b"}]},
                        {"Id": 3, "Children": [{"Id": 33, "Label": "c"}]},
                    ],
                    "@odata.nextLink": next_link,
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    c = _make()
    # Pass an empty dict, not None — None signals batch mode and
    # disables the cap. Streaming readers always pass {} on first call.
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"expand_contained": "true", "max_records_per_batch": "1"},
    )
    rows = list(records)
    assert len(rows) == 1, "cap fires after the first top_row, not after the full page"
    pending = offset.get("pending_fetches")
    assert pending, "in-flight chain must park pending_fetches"
    # Front of queue: re-fetch the SAME page, skip the row we drained.
    assert pending[0]["url"].startswith(f"{SERVICE_URL}Parents?")
    assert "$skiptoken=p2" not in pending[0]["url"]
    assert pending[0]["skip"] == 1
    assert pending[0]["level"] == 0
    # Snapshot mode: no cursor key in the resume offset.
    assert "cursor" not in offset


@responses.activate
def test_contained_expand_truncates_at_page_boundary_queues_only_next_page():
    """When the cap fires exactly at the top page's last row, that page is
    fully drained (NOT re-queued with a skip>0 resume position) and its
    server next-page URL is parked. Depth-first: each parent's inline
    Children page is a link-less auto page, so it's PROBED in-batch (drained,
    not parked) — except a probe still pending when the cap fires, which
    parks. The parked queue stays O(depth)-small, never O(fan-out width)."""
    _mock_nested_metadata()
    next_link = f"{SERVICE_URL}Parents?$skiptoken=p2"

    def _initial(_req):
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"Id": 1, "Children": [{"Id": 11, "Label": "a"}]},
                        {"Id": 2, "Children": [{"Id": 22, "Label": "b"}]},
                    ],
                    "@odata.nextLink": next_link,
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    # Auto mode probes each parent's short link-less inline Children page to
    # confirm exhaustion; depth-first fetches these in-batch (empty = done).
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []}, match_querystring=False)
    responses.get(f"{SERVICE_URL}Parents(2)/Children", json={"value": []}, match_querystring=False)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"expand_contained": "true", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert sorted(r["Id"] for r in rows) == [11, 22]
    pending = offset.get("pending_fetches")
    # The fully-drained top-level page is NOT re-queued — no item carries a
    # skip>0 resume position — and its server next-page URL is parked.
    assert any(
        it["url"] == next_link and it["level"] == 0 and it["chain"] == [] and it["skip"] == 0
        for it in pending
    )
    assert all(item["skip"] == 0 for item in pending)
    # O(depth): the frontier is the top next-page link plus at most one
    # in-flight inner continuation — never one-per-parent (O(width)).
    assert len(pending) <= 3
    assert "cursor" not in offset


@responses.activate
def test_contained_expand_resumes_from_pending_fetches_skip():
    """When the start offset's ``pending_fetches[0]`` has ``skip > 0``,
    the connector re-fetches that page and skips the parked rows."""
    _mock_nested_metadata()
    page_url = f"{SERVICE_URL}Parents?$skiptoken=p1"
    captured = []

    def _resume(req):
        captured.append(req.url)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"Id": 1, "Children": [{"Id": 11, "Label": "a"}]},
                        {"Id": 2, "Children": [{"Id": 22, "Label": "b"}]},
                        {"Id": 3, "Children": [{"Id": 33, "Label": "c"}]},
                    ],
                }
            ),
        )

    responses.add_callback(responses.GET, page_url, callback=_resume, match_querystring=True)
    # Only parent 3 is processed (skip=2); its short, link-less inline Children
    # page triggers an inner drain probe.
    responses.get(f"{SERVICE_URL}Parents(3)/Children", json={"value": []})
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {
            "pending_fetches": [
                {"url": page_url, "level": 0, "chain": [], "cur_val": None, "skip": 2}
            ]
        },
        {"expand_contained": "true"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [33]
    # Page exhausted, no next_url → terminal snapshot offset.
    # Terminal streaming-snapshot offset carries the quiesce marker
    # (a bare {} crashed the pyspark wrapper on non-empty batches).
    assert _drop_lb(offset) == {"snapshot_done": True}


@responses.activate
def test_contained_expand_resumes_from_pending_fetches_url():
    """When ``pending_fetches`` is set in the start offset, the
    connector hands the queued URL back to the server and does NOT
    rebuild / re-fetch the top-level entity set."""
    _mock_nested_metadata()
    resume_url = f"{SERVICE_URL}Parents?$skiptoken=p2"
    captured = []

    def _resume(req):
        captured.append(req.url)
        return (200, {}, json.dumps({"value": [{"Id": 3, "Children": [{"Id": 33, "Label": "c"}]}]}))

    def _bare_top(_req):
        raise AssertionError("connector must not refetch /Parents on resume")

    responses.add_callback(responses.GET, resume_url, callback=_resume, match_querystring=True)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents", callback=_bare_top, match_querystring=True
    )
    # Parent 3's short, link-less inline Children page → inner drain probe.
    responses.get(f"{SERVICE_URL}Parents(3)/Children", json={"value": []})
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {
            "pending_fetches": [
                {"url": resume_url, "level": 0, "chain": [], "cur_val": None, "skip": 0}
            ]
        },
        {"expand_contained": "true"},
    )
    rows = list(records)
    assert len(rows) == 1 and rows[0]["Id"] == 33
    assert captured == [resume_url]
    # Terminal streaming-snapshot offset carries the quiesce marker
    # (a bare {} crashed the pyspark wrapper on non-empty batches).
    assert _drop_lb(offset) == {"snapshot_done": True}


@responses.activate
def test_contained_expand_cursor_mid_chain_holds_watermark_steady():
    """While a chain is in flight (``pending_fetches`` non-empty) the
    ``cursor`` watermark must not advance — mid-chain advance would
    skip rows still pending under the same ``since`` predicate. The
    running max lives at ``running_max_cursor`` and only becomes
    ``cursor`` on chain completion."""
    _mock_nested_metadata()
    next_link = f"{SERVICE_URL}Parents?$skiptoken=p2"

    def _initial(_req):
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-06-05T00:00:00Z"}
                            ],
                        },
                    ],
                    "@odata.nextLink": next_link,
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-01T00:00:00Z"},
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "max_records_per_batch": "1",
        },
    )
    list(records)
    pending = offset.get("pending_fetches")
    assert pending and any(item["url"] == next_link for item in pending)
    assert offset.get("cursor") == "2024-01-01T00:00:00Z"
    assert offset.get("running_max_cursor") == "2024-06-05T00:00:00Z"


@responses.activate
def test_contained_expand_cursor_chain_completion_advances_watermark():
    """On chain exhaustion (empty queue after drain) the running max
    becomes the new ``cursor`` watermark."""
    _mock_nested_metadata()

    def _final(_req):
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 2,
                            "Children": [
                                {"Id": 22, "Label": "b", "ModifiedAt": "2024-07-10T00:00:00Z"}
                            ],
                        },
                    ],
                }
            ),
        )

    resume_url = f"{SERVICE_URL}Parents?$skiptoken=last"
    responses.add_callback(responses.GET, resume_url, callback=_final, match_querystring=True)
    # Parent 2's short, link-less inline Children page → inner drain probe.
    responses.get(f"{SERVICE_URL}Parents(2)/Children", json={"value": []})
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {
            "pending_fetches": [
                {"url": resume_url, "level": 0, "chain": [], "cur_val": None, "skip": 0}
            ],
            "cursor": "2024-01-01T00:00:00Z",
            "running_max_cursor": "2024-06-05T00:00:00Z",
        },
        {"expand_contained": "true", "cursor_field": "ModifiedAt"},
    )
    list(records)
    assert _drop_lb(offset) == {"cursor": "2024-07-10T00:00:00Z"}


@responses.activate
def test_contained_expand_cursor_resume_with_empty_chain_advances_offset():
    """Regression: when cursor-mode resume parks ``pending_fetches``
    only (no ``cursor`` / ``running_max_cursor`` yet because the prior
    batch's rows all had null cursors or the chain hadn't produced any
    cursor-bearing rows), and this batch drains the queue without
    emitting any cursor-bearing rows either, the end-offset must still
    advance. Previously the fallback echoed ``start_offset`` back
    unchanged, the caller saw ``start_offset == end_offset`` with
    ``emitted`` empty, and returned the same offset — the framework
    re-issued it forever."""
    _mock_nested_metadata()
    resume_url = f"{SERVICE_URL}Parents?$skiptoken=last"

    def _empty(_req):
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, resume_url, callback=_empty, match_querystring=True)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {
            "pending_fetches": [
                {"url": resume_url, "level": 0, "chain": [], "cur_val": None, "skip": 0}
            ]
        },
        {"expand_contained": "true", "cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert rows == []
    # Offset MUST advance — empty dict signals chain terminal so the
    # framework stops re-issuing the same resume offset.
    assert _drop_lb(offset) == {}
    # Follow-up trigger with the new (empty) offset must not loop: a
    # fresh top-level fetch returns whatever the table has now and
    # the connector goes through the first-call path without a
    # silent re-issue. Mock the top-level Parents fetch as empty so
    # the second trigger terminates cleanly.
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    records2, offset2 = c.read_table(
        "Parents__Children",
        offset,
        {"expand_contained": "true", "cursor_field": "ModifiedAt"},
    )
    assert list(records2) == []
    assert _drop_lb(offset2) == {}


@responses.activate
def test_contained_expand_first_batch_null_cursor_rows_raises():
    """Regression: streaming first batch passes ``start_offset = {}``
    (``LakeflowStreamReader.initialOffset``). The no-progress guard
    used to be ``if start_offset and start_offset == end_offset`` —
    ``bool({}) is False`` so the guard was bypassed on the first
    trigger, letting null-cursor rows commit with the offset stuck at
    ``{}`` and looping every subsequent trigger. The guard now uses
    bare ``==`` (safe because ``_finalize_cursor_read`` handles
    ``None`` — the batch-reader signal — explicitly before the
    equality check, and the streaming framework never passes ``None``)
    and raises so the operator sees the cause."""
    _mock_nested_metadata()

    def _initial(_req):
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": None},
                            ],
                        },
                    ],
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    # Inner drain probe past the single short, link-less inline child so the
    # chain fully drains and the no-progress guard (not a fetch error) fires.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        records, _ = c.read_table(
            "Parents__Children",
            {},
            {"expand_contained": "true", "cursor_field": "ModifiedAt"},
        )
        list(records)


@responses.activate
def test_contained_expand_batch_mode_null_cursor_rows_emit_without_raise():
    """Batch reader passes ``start_offset=None`` and discards the
    returned offset; the no-progress guard is streaming-only. Mirrors
    ``test_incremental_batch_mode_null_cursor_rows_emit_without_raise``
    for the expand path so a future refactor that re-normalizes None
    to {} inside ``_read_contained_expand`` (or its dispatch in
    ``read_table``) breaks loudly."""
    _mock_nested_metadata()

    def _initial(req):
        # Drain probe past the single short parent page → empty.
        if "gt" in (req.url.split("$filter=", 1)[1] if "$filter=" in req.url else ""):
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": None},
                            ],
                        },
                    ],
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    # Inner drain probe past the single short, link-less inline child.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        None,
        {"expand_contained": "true", "cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_contained_expand_caps_within_top_row_subtree():
    """Per-fetch cap: a single top_row whose inner-collection paginates
    must NOT blow past the cap by its whole subtree. The connector
    queues each inner @odata.nextLink and checks the cap between
    fetches, so the very first parent with many Children commits its
    inline rows + one inner page, then parks the rest in
    ``pending_fetches``."""
    _mock_nested_metadata()
    inner_next = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=k2"
    captured = []

    def _initial(_req):
        captured.append("initial")
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a"},
                                {"Id": 12, "Label": "b"},
                            ],
                            "Children@odata.nextLink": inner_next,
                        },
                    ]
                }
            ),
        )

    def _inner_unused(_req):
        captured.append("inner")
        return (200, {}, json.dumps({"value": [{"Id": 21, "Label": "c"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    responses.add_callback(
        responses.GET, inner_next, callback=_inner_unused, match_querystring=True
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        # Cap = 2: after the top-page is processed, emitted has 2
        # rows (the two inline Children). The inner nextLink for this
        # parent is queued but NOT followed in this batch.
        {"expand_contained": "true", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert len(rows) == 2
    # Inner nextLink fetch must NOT happen in this batch.
    assert "inner" not in captured
    pending = offset.get("pending_fetches")
    assert pending, "inner-nextLink fetch must be parked, not followed"
    # The queued inner fetch is at level 1 (Children under Parent 1)
    # with the parent's PK chain captured.
    assert any(
        item["url"].startswith(inner_next.split("?")[0])
        and item["level"] == 1
        and item["chain"] == [{"Id": 1}]
        for item in pending
    )


@responses.activate
def test_contained_expand_resolves_inner_nextlink_against_response_url():
    """OData v4 §11.2.5.7 / RFC 3986: relative ``@odata.nextLink``
    values resolve against the URL of the response they came from.
    Servers commonly emit query-only relative links (``?$skiptoken=...``)
    inside expanded collections; resolving them against the connector's
    base service URL drops the entity-set path and routes the request
    at the wrong endpoint. The fix scopes resolution to the response
    URL (here, the ``Parents`` collection)."""
    _mock_nested_metadata()
    captured = []

    def _initial(_req):
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [{"Id": 11, "Label": "a"}],
                            # Query-only relative — must resolve against
                            # the response URL, not service_url.
                            "Children@odata.nextLink": "Parents(1)/Children?$skiptoken=x",
                        }
                    ]
                }
            ),
        )

    def _follow(req):
        captured.append(req.url)
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_initial)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_follow)
    c = _make()
    list(c.read_table("Parents__Children", None, {"expand_contained": "true"})[0])
    assert captured, "inner nextLink not fetched"
    # Must be scoped to /Parents(1)/Children, not /?$skiptoken=...
    assert captured[0].startswith(f"{SERVICE_URL}Parents(1)/Children?")
    assert "$skiptoken=x" in captured[0]


@responses.activate
def test_contained_expand_follows_inner_collection_nextlink():
    """OData v4 §11.2.6.1: when an inner expanded collection is server-
    paged, the response carries ``<NavProp>@odata.nextLink`` alongside
    the inline page. Without following it we silently truncate to one
    page — the symptom the user reported (got 100 rows when the parent
    has 735 children)."""
    _mock_nested_metadata()
    inner_next = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p2"
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Name": "P1",
                    "Children": [
                        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
                    ],
                    "Children@odata.nextLink": inner_next,
                }
            ]
        },
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})  # drain probe past last parent
    responses.get(
        inner_next,
        json={
            "value": [
                {"Id": 13, "Label": "c", "ModifiedAt": "2024-01-03T00:00:00Z"},
                {"Id": 14, "Label": "d", "ModifiedAt": "2024-01-04T00:00:00Z"},
            ]
        },
    )
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"expand_contained": "true"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [11, 12, 13, 14]
    assert all(r["Parents_Id"] == 1 for r in rows)


@responses.activate
def test_contained_expand_follows_inner_nextlink_chain():
    """Multi-page inner nextLink: the second page's response also carries
    a nextLink; the connector must walk the whole chain, not just one
    follow-up."""
    _mock_nested_metadata()
    inner_p2 = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p2"
    inner_p3 = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p3"
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}],
                    "Children@odata.nextLink": inner_p2,
                }
            ]
        },
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})  # drain probe past last parent
    responses.get(
        inner_p2,
        json={
            "value": [{"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"}],
            "@odata.nextLink": inner_p3,
        },
    )
    responses.get(
        inner_p3,
        json={"value": [{"Id": 13, "Label": "c", "ModifiedAt": "2024-01-03T00:00:00Z"}]},
    )
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"expand_contained": "true"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [11, 12, 13]
    assert all(r["Parents_Id"] == 1 for r in rows)


@responses.activate
def test_contained_expand_follows_inner_nextlink_at_grandchild_level():
    """Three-segment path: the grandchild collection under a single
    child parent is paged. The continuation URL preserves the original
    request context (per OData spec), so the connector treats it the
    same as the inline page."""
    _mock_nested_metadata()
    notes_next = f"{SERVICE_URL}Parents(1)/Children(10)/Notes?$skiptoken=p2"
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [
                        {
                            "Id": 10,
                            "Notes": [{"Id": 100, "Text": "x"}],
                            "Notes@odata.nextLink": notes_next,
                        }
                    ],
                }
            ]
        },
    )
    responses.get(
        notes_next,
        json={"value": [{"Id": 101, "Text": "y"}, {"Id": 102, "Text": "z"}]},
    )
    # The followed Notes page ends short and link-less → probe past Id 102; the
    # single inline child is also a short, link-less page → probe past it.
    responses.get(f"{SERVICE_URL}Parents(1)/Children(10)/Notes", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table("Parents__Children__Notes", None, {"expand_contained": "true"})
    rows = list(records)
    assert {r["Id"] for r in rows} == {100, 101, 102}
    assert all(r["Parents_Id"] == 1 and r["Children_Id"] == 10 for r in rows)


@responses.activate
def test_contained_expand_strips_inner_nextlink_annotation_from_leaf():
    """When the leaf entity carries a ``<NavProp>@odata.nextLink`` key
    (e.g. for some further nav collection the connector didn't request),
    it must not leak as a column on the emitted row — that key contains
    ``@odata.`` but doesn't start with it, so the prior strip filter
    missed it."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [
                        {
                            "Id": 11,
                            "Label": "a",
                            "ModifiedAt": "2024-01-01T00:00:00Z",
                            "Notes@odata.nextLink": "ignored",
                        }
                    ],
                }
            ]
        },
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})  # drain probe past last parent
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})  # inner drain probe
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"expand_contained": "true"})
    rows = list(records)
    assert rows == [
        {
            "Parents_Id": 1,
            "Id": 11,
            "Label": "a",
            "ModifiedAt": "2024-01-01T00:00:00Z",
        }
    ]


@responses.activate
def test_contained_expand_invalid_value_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid expand_contained"):
        c.read_table("Parents__Children", None, {"expand_contained": "yes"})


# --- N+1 mode: filter_at_<seg> applied at each walk level ---


@responses.activate
def test_contained_ancestor_walks_force_pk_orderby_for_stable_skiptoken():
    """Every ancestor-key fetch must carry a PK-only ``$orderby`` so
    server skiptoken pagination is stable across pages. OData v4
    §11.2.5.7 doesn't promise stable default ordering without an
    explicit ``$orderby`` over a unique key set — without it sources
    whose default sort isn't PK can drop or duplicate parents, and
    every leaf row under a dropped parent is silently lost. Verifies
    both the top URL and the intermediate ancestor URL carry
    ``$orderby=Id asc`` on a 3-segment N+1 walk."""
    _mock_nested_metadata()
    captured: list[str] = []

    def _callback(req):
        captured.append(req.url)
        if req.url.startswith(f"{SERVICE_URL}Parents(1)/Children(10)/Notes"):
            return (200, {}, json.dumps({"value": [{"Id": 100, "Text": "n"}]}))
        if "Parents(1)/Children" in req.url:
            return (200, {}, json.dumps({"value": [{"Id": 10}]}))
        return (200, {}, json.dumps({"value": [{"Id": 1}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_callback)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_callback)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(1)/Children(10)/Notes", callback=_callback
    )
    c = _make()
    records, _ = c.read_table("Parents__Children__Notes", None, {"expand_contained": "false"})
    list(records)
    # Top-level + intermediate ancestor fetches both carry
    # ``$orderby=Id asc``. The leaf collection (Notes) doesn't need
    # an ancestor-style $orderby — it's a different code path and
    # its skiptoken stability is the caller's concern.
    top_call = next(u for u in captured if u.startswith(f"{SERVICE_URL}Parents?"))
    mid_call = next(u for u in captured if "Parents(1)/Children?" in u)
    # ``requests`` may emit the space in the order_by value as ``+`` or
    # ``%20`` depending on version; accept either encoding.
    for url in (top_call, mid_call):
        assert "$orderby=Id" in url and ("Id%20asc" in url or "Id+asc" in url or "Id asc" in url)


@responses.activate
def test_contained_npp_filter_at_top_prunes_parent_walk():
    """``filter_at_<top>`` lands on the level-0 walk; only matching
    parents are then traversed for children. Other parents skipped."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 5}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 5",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    # auto drains link-omitting collections: the trailing keyset probe
    # ((Id eq 5) and (Id gt 5)) falls through to this empty page and stops.
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    responses.get(
        f"{SERVICE_URL}Parents(5)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"filter_at_Parents": "Id eq 5"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]
    assert all(r["Parents_Id"] == 5 for r in rows)


@responses.activate
def test_contained_npp_filter_at_middle_prunes_middle_walk():
    """Three-segment path: ``filter_at_<middle>`` prunes the middle
    walk. Only ``Children`` matching the filter — under each Parent —
    have their Notes fetched."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 1}, {"Id": 2}]},
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 10}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 10",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    # auto's trailing keyset probe falls through to this empty page.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": []},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 10",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "x"}, {"Id": 101, "Text": "y"}]},
    )
    c = _make()
    records, _ = c.read_table("Parents__Children__Notes", None, {"filter_at_Children": "Id eq 10"})
    rows = list(records)
    assert {r["Id"] for r in rows} == {100, 101}
    assert all(r["Children_Id"] == 10 and r["Parents_Id"] == 1 for r in rows)


@responses.activate
def test_contained_npp_filter_at_leaf_applies_at_leaf_url():
    """``filter_at_<leaf>`` lands at the leaf URL (the same place the
    existing ``filter`` option would land in N+1 mode)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1000", "$filter": "Label eq 'a'", "$orderby": "Id asc"}
            )
        ],
    )
    # auto's trailing keyset probe falls through to this empty page.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"filter_at_Children": "Label eq 'a'"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_contained_npp_filter_at_all_levels_cascades():
    """All three segment filters AND'd through the full walk: top prunes
    parents → middle prunes children → leaf filters notes."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 5}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 5",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    # auto's trailing keyset probe at each level falls through to an empty page.
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    responses.get(
        f"{SERVICE_URL}Parents(5)/Children",
        json={"value": [{"Id": 10}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 10",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents(5)/Children", json={"value": []})
    responses.get(
        f"{SERVICE_URL}Parents(5)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "x"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1000", "$filter": "Id eq 100", "$orderby": "Id asc"}
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents(5)/Children(10)/Notes", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        None,
        {
            "filter_at_Parents": "Id eq 5",
            "filter_at_Children": "Id eq 10",
            "filter_at_Notes": "Id eq 100",
        },
    )
    assert [r["Id"] for r in list(records)] == [100]


@responses.activate
def test_contained_npp_filter_at_index_form_equivalent():
    """``filter_at_0`` is equivalent to ``filter_at_<top-segment-name>``."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 5}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$select": "Id",
                    "$filter": "Id eq 5",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    responses.get(
        f"{SERVICE_URL}Parents(5)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    records, _ = c.read_table("Parents__Children", None, {"filter_at_0": "Id eq 5"})
    assert [r["Id"] for r in list(records)] == [11]


# --- expand_contained=true mode ---


@responses.activate
def test_contained_expand_user_filter_lands_in_leaf_expand_not_top():
    """The table's ``filter`` option is the leaf filter in both modes.
    In expand mode it lands inside the innermost ``$expand(...)``,
    NOT on the top URL — same semantic as N+1 mode, where it goes
    to the leaf URL. Stripping it from the top is what makes
    ``filter_at_<top>`` and ``filter`` compose correctly on a
    table like ``Instances__Projects``."""
    _mock_nested_metadata()
    captured = []

    def callback(req):
        captured.append(req.url)
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=callback)
    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        None,
        {
            "expand_contained": "true",
            "filter": "Id eq 3",
            "filter_at_Parents": "Id eq 1",
        },
    )
    list(records)
    from urllib.parse import unquote

    url = unquote(captured[0])
    # filter_at_Parents lands at the top URL; user `filter` lands
    # inside $expand=Children(...).
    # Dynamic tops for N=2 page_size=1000 (default pagination=auto): [100, 10].
    assert "Parents?$top=100&$filter=Id eq 1" in url
    assert "$expand=Children($top=10;$filter=Id eq 3" in url
    # User filter must NOT be at the top URL.
    assert "(Id eq 1) and (Id eq 3)" not in url
    assert "(Id eq 3) and (Id eq 1)" not in url


@responses.activate
def test_contained_expand_filter_at_top_lands_on_top_url():
    _mock_nested_metadata()
    captured = []

    def callback(req):
        captured.append(req.url)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 5,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}
                            ],
                        },
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=callback)
    # Parent 5's short, link-less inline Children page → inner drain probe.
    responses.get(f"{SERVICE_URL}Parents(5)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        None,
        {"expand_contained": "true", "filter_at_Parents": "Id eq 5"},
    )
    list(records)
    from urllib.parse import unquote

    assert "$filter=Id eq 5" in unquote(captured[0])


@responses.activate
def test_contained_expand_filter_at_middle_lands_inside_expand():
    """``filter_at_<middle>`` is injected inside the matching
    ``$expand(...)`` clause (OData v4 §5.1.1.6)."""
    _mock_nested_metadata()
    captured = []

    def callback(req):
        captured.append(req.url)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 10, "Notes": [{"Id": 100, "Text": "x"}]},
                            ],
                        },
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=callback)
    # Short, link-less inline child + grandchild pages → inner drain probes.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(1)/Children(10)/Notes", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        None,
        {"expand_contained": "true", "filter_at_Children": "Id eq 10"},
    )
    list(records)
    from urllib.parse import unquote

    # Dynamic tops for N=3 page_size=1000: [34, 5, 5]. Middle level = 5.
    assert "Children($top=5;$filter=Id eq 10" in unquote(captured[0])


@responses.activate
def test_contained_expand_filter_at_leaf_lands_in_innermost_expand():
    _mock_nested_metadata()
    captured = []

    def callback(req):
        captured.append(req.url)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 10, "Notes": [{"Id": 100, "Text": "x"}]},
                            ],
                        },
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=callback)
    # Short, link-less inline child + grandchild pages → inner drain probes.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(1)/Children(10)/Notes", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        None,
        {"expand_contained": "true", "filter_at_Notes": "Id eq 100"},
    )
    list(records)
    from urllib.parse import unquote

    # Dynamic tops for N=3 page_size=1000: [34, 5, 5]. Leaf level = 5.
    assert "Notes($top=5;$filter=Id eq 100" in unquote(captured[0])


# --- Composition ---


@responses.activate
def test_contained_npp_filter_at_composes_with_cursor_at_same_level():
    """Cursor filter at the cursor segment AND-s with that segment's
    ``filter_at_<seg>``."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 1}]},
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-06-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    # Cursor-based read → default page_size, so $top is sent.
                    "$top": "1000",
                    "$filter": "(ModifiedAt gt 2024-01-01T00:00:00Z) and (Label eq 'a')",
                    "$orderby": "ModifiedAt asc,Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-01T00:00:00Z"},
        {
            "cursor_field": "ModifiedAt",
            "filter_at_Children": "Label eq 'a'",
        },
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_contained_npp_filter_at_leaf_composes_with_user_filter():
    """The leaf URL composes ``filter_at_<leaf>`` (sent as extra_filter)
    with the user's ``filter`` option (sent via opts["filter"]). Both
    AND together in the final URL."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$filter": "(Id lt 100) and (Label eq 'a')",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        None,
        {"filter": "Id lt 100", "filter_at_Children": "Label eq 'a'"},
    )
    assert [r["Id"] for r in list(records)] == [11]


@responses.activate
def test_contained_expand_with_ancestor_cursor_injects_filter_into_expand():
    """expand_contained + cursor on a middle ancestor injects
    $filter/$orderby into the ``$expand`` clause for that ancestor.
    Top-level URL has no $filter (cursor isn't on the top entity set)."""
    _mock_nested_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [
                        {
                            "Id": 11,
                            "ModifiedAt": "2024-01-02T00:00:00Z",
                            "Notes": [{"Id": 111, "Text": "a"}],
                        }
                    ],
                }
            ]
        },
        match_querystring=False,
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})  # drain probe past last parent
    # Short, link-less inline child + grandchild pages → inner drain probes.
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(1)/Children(11)/Notes", json={"value": []})
    c = _make()
    records, offset = c.read_table(
        "Parents__Children__Notes",
        {"cursor": "2024-01-01T00:00:00Z"},
        {"expand_contained": "true", "cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    call_url = responses.calls[1].request.url
    # cursor is on Children (level 1), so $filter/$orderby live inside
    # the Children $expand, not at the top level.
    assert "%24expand=Children" in call_url or "$expand=Children" in call_url
    # $filter inside the expand uses the cursor; ' gt ' encoded as %20gt%20 or +gt+.
    assert "ModifiedAt%20gt%20" in call_url or "ModifiedAt+gt+" in call_url
    assert "%24orderby" in call_url or "$orderby" in call_url
    # Leaf row was stamped with the ancestor's cursor value.
    assert rows == [
        {
            "Parents_Id": 1,
            "Children_Id": 11,
            "Id": 111,
            "Text": "a",
            "ModifiedAt": "2024-01-02T00:00:00Z",
        }
    ]
    assert _drop_lb(offset) == {"cursor": "2024-01-02T00:00:00Z"}


@responses.activate
def test_contained_expand_does_not_inject_select_inside_cursor_expand():
    """The connector must not inject $select inside the cursor segment's
    $expand clause. The cursor column is returned by default; injecting
    $select would silently strip every other column the user didn't
    explicitly opt out of — broken on the leaf-cursor case (2-segment
    paths) where the cursor segment is the destination."""
    _mock_nested_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": []},
        match_querystring=False,
    )
    c = _make()
    list(
        c.read_table(
            "Parents__Children__Notes",
            None,
            {"expand_contained": "true", "cursor_field": "ModifiedAt"},
        )[0]
    )
    call_url = responses.calls[1].request.url
    assert "%24select" not in call_url and "$select" not in call_url
    # $filter/$orderby remain — they're load-bearing for incremental.
    assert "%24orderby" in call_url or "$orderby" in call_url


@responses.activate
def test_contained_expand_cursor_orderby_includes_level_pks():
    """The $orderby injected at the cursor level uses ``cursor asc``
    plus that segment's primary keys as tie-breakers (proving
    `_find_cursor_level` returns the right level, not just the leaf)."""
    _mock_nested_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": []},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        None,
        {"expand_contained": "true", "cursor_field": "ModifiedAt"},
    )
    list(records)
    call_url = responses.calls[1].request.url
    # $orderby inside the Children expand includes Id (Children's PK).
    assert "ModifiedAt" in call_url and ("Id%20asc" in call_url or "Id+asc" in call_url)


@responses.activate
def test_contained_expand_cursor_not_on_any_segment_raises():
    """expand_contained + cursor_field that's not a property on any
    segment surfaces an actionable ValueError, same as N+1 mode."""
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not a property"):
        c.read_table(
            "Parents__Children__Notes",
            None,
            {"expand_contained": "true", "cursor_field": "DoesNotExist"},
        )


# --- Cursor incremental on contained ---


@responses.activate
def test_contained_incremental_first_call_no_filter():
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children", {}, {"cursor_field": "ModifiedAt", "expand_contained": "false"}
    )
    rows = list(records)
    assert len(rows) == 2
    assert _drop_lb(offset) == {"cursor": "2024-01-02T00:00:00Z"}
    # First leaf call has no cursor filter
    assert "$filter" not in responses.calls[1].request.url


@responses.activate
def test_contained_incremental_resume_applies_cursor_filter():
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 13, "Label": "c", "ModifiedAt": "2024-01-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-02T00:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert len(rows) == 1
    assert _drop_lb(offset) == {"cursor": "2024-01-03T00:00:00Z"}
    # Cursor filter present on the leaf call. Located by URL rather than a fixed
    # index: under the default ``cursor_probe=auto`` a one-shot ``$batch``
    # capability preflight (POST, fails closed on this no-$batch mock) precedes
    # the plain leaf walk, so the leaf call isn't at a fixed position.
    leaf_calls = [c.request.url for c in responses.calls if "Parents(1)/Children" in c.request.url]
    assert leaf_calls, "expected a leaf fetch under Parents(1)/Children"
    assert any("ModifiedAt%20gt%20" in u or "ModifiedAt+gt+" in u for u in leaf_calls)


@responses.activate
def test_contained_incremental_terminates_when_offset_unchanged():
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": []},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-02T00:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    assert list(records) == []
    assert _drop_lb(offset) == {"cursor": "2024-01-02T00:00:00Z"}


@responses.activate
def test_contained_incremental_leaf_cursor_first_batch_null_rows_raises():
    """Regression: first streaming batch passes ``start_offset = {}``.
    With null leaf cursors and ``since=None``, the leaf path used to
    compose ``end_offset = {'cursor': None}`` (via
    ``max(cursors) if cursors else since``) — distinct from ``{}`` so
    the no-progress guard didn't fire on batch 1 and one batch of
    null-cursor rows committed downstream before batch 2 raised. The
    fix normalizes the no-cursor-data + no-since case to ``{}``,
    mirroring the expand path's behavior so the first trigger surfaces
    the cause."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 10, "Label": "a", "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        records, _ = c.read_table(
            "Parents__Children",
            {},
            {"cursor_field": "ModifiedAt", "cursor_nulls": "error"},
        )
        list(records)


@responses.activate
def test_contained_incremental_leaf_cursor_batch_mode_null_rows_emit_without_raise():
    """Batch reader passes ``start_offset=None`` and discards the
    returned offset; the no-progress guard is streaming-only. Mirrors
    ``test_incremental_batch_mode_null_cursor_rows_emit_without_raise``
    for the contained leaf-cursor path so a future refactor that
    re-normalizes None to {} inside
    ``_read_contained_incremental_leaf_cursor`` (or its dispatch in
    ``_read_contained_incremental``) breaks loudly."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 10, "Label": "a", "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        None,
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [10]


@responses.activate
def test_contained_incremental_leaf_cursor_null_rows_raises():
    """Regression: the leaf-cursor path in
    ``_read_contained_incremental_leaf_cursor`` previously silently
    dropped rows when ``start_offset == end_offset`` — same data-loss
    class the PR fixed in the expand and ancestor paths. Streaming
    resume with ``{cursor: 'X'}`` and leaf rows whose cursor is null
    (``cursors=[]`` → ``end_offset = {cursor: since} = start_offset``);
    rows must surface a loud RuntimeError rather than vanish from the
    stream."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 10, "Label": "a", "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        records, _ = c.read_table(
            "Parents__Children",
            {"cursor": "2024-01-02T00:00:00Z"},
            {"cursor_field": "ModifiedAt", "cursor_nulls": "error"},
        )
        list(records)


@responses.activate
def test_contained_leaf_cursor_coalesce_default_emits_null_rows_and_advances():
    """Default ``cursor_nulls=coalesce`` on the contained leaf-cursor
    path: a null-cursor leaf row is emitted (column left null) and the
    watermark advances via a synthetic floor — no no-progress raise.
    This is the Hexagon ``WorkPackagesStepInstances`` failure mode."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 10, "Label": "a", "ModifiedAt": None}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table("Parents__Children", {}, {"cursor_field": "ModifiedAt"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [10]
    assert rows[0]["ModifiedAt"] is None
    assert offset["cursor"].startswith("2000-01-01T00:00:00.")


@responses.activate
def test_contained_leaf_cursor_ignore_skips_null_rows():
    """``cursor_nulls=ignore`` on the contained leaf-cursor path drops
    null-cursor leaf rows; only the real-cursor row is emitted."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 10, "Label": "a", "ModifiedAt": None},
                {"Id": 11, "Label": "b", "ModifiedAt": "2024-02-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "ignore"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]
    assert _drop_lb(offset) == {"cursor": "2024-02-01T00:00:00Z"}


@responses.activate
def test_contained_incremental_truncation_trims_boundary_cohort():
    """When the per-parent walk truncates, the trailing same-cursor cohort
    of the truncated chain is trimmed and the offset carries a
    ``truncated_chain_cursor`` so the resumed call re-picks up exactly
    that cohort without skipping it (Option A boundary trim, scoped to
    the truncated chain only).

    Pinned to ``pagination=nextlink``: this cursor-only boundary trim is the
    checkpoint used when a page carries no continuation link. Under the default
    ``auto`` the walk instead drains the leaf and parks a compound keyset seek
    (see ``test_contained_incremental_auto_drains_capped_leaf``)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2", "pagination": "nextlink"},
    )
    rows = list(records)
    # Trim drops the c2 boundary cohort; only c1 is emitted.
    assert len(rows) == 1
    assert rows[0]["ModifiedAt"] == "2024-01-01T00:00:00Z"
    # Resume re-fetches parent 0 from cursor gt c1, picking up c2 + beyond.
    assert _drop_lb(offset) == {
        "parent_idx": 0,
        "parent_keys": [{"Id": 1}],
        "truncated_chain_cursor": "2024-01-01T00:00:00Z",
        "running_max": "2024-01-01T00:00:00Z",
    }


def test_chain_resume_ordering_is_chronological_and_incomparable_safe():
    """The key-based resume orders chains like the server enumeration:
    ints numerically, ISO-rendered keys chronologically (``…00.5Z`` is
    NEWER than ``…00Z`` despite sorting lexically smaller). Incomparable
    pairs (cross-type after a metadata change) are never skipped —
    duplicate-safe, not silent loss."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _chain_resume_key,
        _chain_strictly_before,
    )

    assert _chain_strictly_before(_chain_resume_key([{"Id": 5}]), _chain_resume_key([{"Id": 20}]))
    assert _chain_strictly_before(
        _chain_resume_key([{"K": "2024-01-01T00:00:00Z"}]),
        _chain_resume_key([{"K": "2024-01-01T00:00:00.5Z"}]),
    )
    assert not _chain_strictly_before(
        _chain_resume_key([{"K": "2024-01-01T00:00:00.5Z"}]),
        _chain_resume_key([{"K": "2024-01-01T00:00:00Z"}]),
    )
    # Cross-type: incomparable → False both ways (re-read, never skip).
    assert not _chain_strictly_before(
        _chain_resume_key([{"Id": 5}]), _chain_resume_key([{"Id": "x"}])
    )
    assert not _chain_strictly_before(
        _chain_resume_key([{"Id": "x"}]), _chain_resume_key([{"Id": 5}])
    )
    # Ancestor-cursor walks put the cursor term at ITS level's position
    # (level 0 here → it is the major sort key).
    assert _chain_strictly_before(
        _chain_resume_key([{"Id": 9}], "2024-01-01T00:00:00Z"),
        _chain_resume_key([{"Id": 1}], "2024-06-01T00:00:00Z"),
    )
    # Sub-microsecond-distinct cursors must NOT tie: a µs-truncating
    # comparison stalls the seek loop and silently drops the parked
    # continuation (round-18 tie class, one layer up).
    assert _chain_strictly_before(
        _chain_resume_key([{"K": "2024-01-01T00:00:00.4876545+00:00"}]),
        _chain_resume_key([{"K": "2024-01-01T00:00:00.4876546Z"}]),
    )
    assert not _chain_strictly_before(
        _chain_resume_key([{"K": "2024-01-01T00:00:00.4876546Z"}]),
        _chain_resume_key([{"K": "2024-01-01T00:00:00.4876545+00:00"}]),
    )
    # Mid-level cursor (3-segment path, cursor on level 1): the enumeration
    # is NESTED — level-0 PKs order BEFORE the level-1 cursor ever applies,
    # so (A=2, cursor 2024-01) sorts AFTER (A=1, cursor 2024-06). A
    # globally-first cursor key would invert this and skip unwalked
    # subtrees under later top-level parents.
    assert not _chain_strictly_before(
        _chain_resume_key([{"A": 2}, {"B": 1}], "2024-01-01T00:00:00Z", cursor_level=1),
        _chain_resume_key([{"A": 1}, {"B": 9}], "2024-06-01T00:00:00Z", cursor_level=1),
    )
    assert _chain_strictly_before(
        _chain_resume_key([{"A": 1}, {"B": 9}], "2024-06-01T00:00:00Z", cursor_level=1),
        _chain_resume_key([{"A": 2}, {"B": 1}], "2024-01-01T00:00:00Z", cursor_level=1),
    )


@responses.activate
def test_ancestor_midlevel_cursor_resume_does_not_skip_later_parents():
    """3-segment path with the cursor on level 1 (Children.ModifiedAt,
    leaf = Notes): the enumeration orders by level-0 PK FIRST, cursor only
    within each parent. A resume key that put the cursor globally first
    skipped every (later parent, lower cursor) chain as "already walked" —
    permanent subtree loss on a completely stable source, then locked out
    by running_max."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "ModifiedAt": "2024-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes",
        json={"value": [{"Id": 111, "Text": "a"}, {"Id": 112, "Text": "b"}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children(21)/Notes",
        json={"value": [{"Id": 211, "Text": "c"}]},
        match_querystring=False,
    )
    c = _make()
    opts = {"cursor_field": "ModifiedAt", "max_records_per_batch": "2", "pagination": "nextlink"}
    recs1, offset1 = c.read_table("Parents__Children__Notes", {}, opts)
    # Batch 1: chain (P1, C11)@2024-06 emits its two notes and parks.
    assert [r["Id"] for r in recs1] == [111, 112]
    assert offset1["parent_keys"] == [{"Id": 1}, {"Id": 11}]
    assert offset1["parent_cursor"] == "2024-06-01T00:00:00Z"
    # Batch 2 (stable source): chain (P2, C21)@2024-01 sorts AFTER the park
    # (level-0 PK majors) — it must be walked, not skipped.
    recs2, offset2 = c.read_table("Parents__Children__Notes", offset1, opts)
    assert [r["Id"] for r in recs2] == [211]
    assert _drop_lb(offset2) == {"cursor": "2024-06-01T00:00:00Z"}


@responses.activate
def test_contained_schema_never_gains_delta_columns():
    """Contained paths never take the delta read path (dispatch rejects
    ``enabled``; metadata skips the probe), so their declared schema must
    not gain the non-nullable ``_deleted``/``_lc_sequence`` columns no
    emitted row would carry. Flat tables keep them."""
    _mock_nested_metadata()
    c = _make()
    names = [f.name for f in c.get_table_schema("Parents__Children", {"delta_tracking": "enabled"})]
    assert "_deleted" not in names and "_lc_sequence" not in names


@responses.activate
def test_contained_incremental_complete_parent_single_cursor_emits_all():
    """A *complete* parent (server returned the whole leaf collection in
    one page, no @odata.nextLink) whose rows all share one cursor value
    has no splittable boundary. Rather than fail when
    max_records_per_batch is smaller than that cohort, the connector
    emits the full cohort and advances the watermark — the cohort is
    complete, so ``cursor gt <value>`` next batch is safe (same exposure
    as natural completion). (Formerly raised RuntimeError.)"""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 13, "Label": "c", "ModifiedAt": "2024-01-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2", "pagination": "nextlink"},
    )
    rows = list(records)
    # All three same-cursor rows come through despite the cap of 2 ...
    assert [r["Id"] for r in rows] == [11, 12, 13]
    # ... and the watermark advances to that value with the terminal
    # offset shape — no parent_idx / truncated_chain_cursor parked.
    assert _drop_lb(offset) == {"cursor": "2024-01-01T00:00:00Z"}


@responses.activate
def test_contained_incremental_continues_past_single_cursor_parent_then_checkpoints():
    """When an all-one-cursor *complete* parent overruns the cap, the walk
    emits it in full and continues; it then truncates at the next parent
    that offers a distinct-cursor boundary (parking truncated_chain_cursor
    there). The single-cursor parent is not re-read on resume."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    # Parent 1: complete (no nextLink), both rows share one cursor value →
    # overruns cap=2, no splittable boundary → emitted in full, walk
    # continues.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    # Parent 2: distinct cursors → the trailing cohort is trimmed and the
    # last distinct cursor is parked as the checkpoint.
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={
            "value": [
                {"Id": 21, "Label": "x", "ModifiedAt": "2024-02-01T00:00:00Z"},
                {"Id": 22, "Label": "y", "ModifiedAt": "2024-02-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2", "pagination": "nextlink"},
    )
    rows = list(records)
    # Parent 1's full cohort + parent 2's trimmed prefix (22's cohort dropped).
    assert [r["Id"] for r in rows] == [11, 12, 21]
    # Checkpoint lands on parent 2 (index 1) at its last distinct cursor.
    assert _drop_lb(offset) == {
        "parent_idx": 1,
        "parent_keys": [{"Id": 2}],
        "truncated_chain_cursor": "2024-02-01T00:00:00Z",
        "running_max": "2024-02-01T00:00:00Z",
    }


@responses.activate
def test_contained_incremental_auto_drains_capped_leaf():
    """The xmla_demo scenario: a CONTAINED CURSOR read (cursor_field set) of a
    server that caps each leaf response below $top and omits @odata.nextLink.
    Under the default ``pagination=auto`` the leaf-cursor walk now drains the
    leaf via the keyset seek instead of stopping at the first short page, so the
    full leaf is read across batches with no rows dropped — no per-table
    pagination override needed. (Mirrors WorkPackageDetails on the live mock.)"""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    # One parent, a 7-row leaf, server caps every response at 3 rows and never
    # emits a continuation link — but honors the compound keyset $filter.
    children = [
        {"Id": 10 + i, "Label": f"c{i}", "ModifiedAt": f"2024-01-{i + 1:02d}T00:00:00Z"}
        for i in range(7)
    ]

    def cb(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        gt = re.search(r"ModifiedAt gt ([0-9T:\-Z]+)", flt)
        eq_id = re.search(r"ModifiedAt eq ([0-9T:\-Z]+) and Id gt (\d+)", flt)

        def keep(r):
            if not flt:
                return True
            if gt and r["ModifiedAt"] > gt.group(1):
                return True
            return bool(
                eq_id and r["ModifiedAt"] == eq_id.group(1) and r["Id"] > int(eq_id.group(2))
            )

        rows = [r for r in children if keep(r)]
        return (200, {}, json.dumps({"value": rows[:3]}))  # cap 3, no nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=cb)
    c = _make()
    # Drive the cursor read to completion the way SDP does: feed the offset back
    # until it stops advancing. Default pagination (auto), generous cap.
    seen, offset, batches = [], {}, 0
    while batches < 20:
        batches += 1
        recs, new = c.read_table(
            "Parents__Children", offset, {"cursor_field": "ModifiedAt", "expand_contained": "false"}
        )
        got = [r["Id"] for r in recs]
        seen.extend(got)
        if not got or new == offset:
            break
        offset = new
    # All 7 leaf rows, each exactly once.
    assert sorted(seen) == [10, 11, 12, 13, 14, 15, 16]
    assert len(seen) == len(set(seen))


@responses.activate
def test_contained_incremental_truncation_resume_uses_chain_cursor():
    """A resumed read with ``truncated_chain_cursor`` issues
    ``cursor gt <chain_cursor>`` to the truncated chain only — subsequent
    chains keep using the outer ``cursor`` value, since per-parent cursor
    distributions are independent."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"}]},
        match_querystring=False,
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "x", "ModifiedAt": "2024-01-05T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"parent_idx": 0, "truncated_chain_cursor": "2024-01-01T00:00:00Z"},
        {"cursor_field": "ModifiedAt", "expand_contained": "false"},
    )
    rows = list(records)
    # Both chains' rows come through; offset is back to natural-completion shape.
    assert {r["ModifiedAt"] for r in rows} == {
        "2024-01-02T00:00:00Z",
        "2024-01-05T00:00:00Z",
    }
    assert _drop_lb(offset) == {"cursor": "2024-01-05T00:00:00Z"}
    # First leaf call uses the chain cursor; second uses the outer cursor (None here).
    p1_call = next(c for c in responses.calls if "Parents(1)/Children" in c.request.url)
    assert "ModifiedAt%20gt%202024-01-01" in p1_call.request.url or (
        "ModifiedAt+gt+2024-01-01" in p1_call.request.url
    )


@responses.activate
def test_contained_incremental_truncation_uses_nextlink_at_page_boundary():
    """When the per-parent walk hits ``max_records_per_batch`` exactly at
    a page boundary and the chain has more pages, the connector parks
    ``chain_next_link`` (the server's @odata.nextLink) in the offset
    rather than rebuilding the URL with ``cursor gt …``. The resumed
    call hands the link back to the server unchanged."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    next_link = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=opaque-token"
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
            ],
            "@odata.nextLink": next_link,
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # Whole page emitted (page-boundary truncation; no Option A trim).
    assert len(rows) == 2
    assert _drop_lb(offset) == {
        "parent_idx": 0,
        "parent_keys": [{"Id": 1}],
        "chain_next_link": next_link,
        "running_max": "2024-01-02T00:00:00Z",
    }


@responses.activate
def test_contained_incremental_resume_from_chain_next_link():
    """A resumed read with ``chain_next_link`` in the offset hits the
    skiptoken URL directly (no URL rebuild), then carries on to the
    next chain when that page indicates the chain is done."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    skip_url = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=opaque-token"
    responses.add(
        responses.GET,
        skip_url,
        json={"value": [{"Id": 13, "Label": "c", "ModifiedAt": "2024-01-03T00:00:00Z"}]},
        match_querystring=False,
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "x", "ModifiedAt": "2024-01-05T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"parent_idx": 0, "chain_next_link": skip_url},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert {r["ModifiedAt"] for r in rows} == {
        "2024-01-03T00:00:00Z",
        "2024-01-05T00:00:00Z",
    }
    assert _drop_lb(offset) == {"cursor": "2024-01-05T00:00:00Z"}
    # Resumed URL is the skiptoken — no `$filter=` reconstruction.
    skip_call = next(c for c in responses.calls if "skiptoken" in c.request.url)
    assert skip_call is not None


@responses.activate
def test_ancestor_cursor_truncation_parks_chain_next_link():
    """Ancestor-cursor mode has no Option A fallback (every leaf under a
    chain shares the chain's stamped cursor by construction). On
    truncation it relies solely on the server's @odata.nextLink to
    resume the chain's leaf fetch."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match_querystring=False,
    )
    notes_next = f"{SERVICE_URL}Parents(1)/Children(11)/Notes?$skiptoken=tok"
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes",
        json={
            "value": [{"Id": 100, "Text": "a"}, {"Id": 101, "Text": "b"}],
            "@odata.nextLink": notes_next,
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children__Notes",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert len(rows) == 2
    # All leaf rows stamped with the ancestor cursor (unchanged behavior).
    assert all(r["ModifiedAt"] == "2024-01-01T00:00:00Z" for r in rows)
    # New: offset carries the nextLink for the truncated chain.
    assert offset["chain_next_link"] == notes_next
    assert offset["parent_idx"] == 0


@responses.activate
def test_ancestor_cursor_truncation_preserves_original_since():
    """On truncation in ancestor-cursor mode, the offset's ``cursor``
    preserves the original ``since`` rather than advancing to the global
    max emitted. This is the fix for the cross-chain interleaved-cursor
    bug: chain enumeration is depth-first by top-level parent, so
    ancestor cursors interleave across parents. If we used max(emitted)
    we'd filter out lower-cursor chains under later top-level parents
    on resume — even though they were never emitted."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    # Under Parent(1): Children with HIGHER cursors first (filtered/ordered server-side).
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "ModifiedAt": "2024-01-10T00:00:00Z"},
                {"Id": 12, "ModifiedAt": "2024-01-20T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    # Under Parent(2): Children with LOWER cursors — these interleave below
    # Parent(1)'s already-emitted max.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(2)/Children",
        json={
            "value": [
                {"Id": 21, "ModifiedAt": "2024-01-05T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    # Each Children's Notes (under Parent 1 only — Parent 2 not reached on batch 1).
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes",
        json={"value": [{"Id": 100, "Text": "a"}]},
        match_querystring=False,
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children(12)/Notes",
        json={"value": [{"Id": 200, "Text": "b"}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children__Notes",
        # since=2023-01-01 chosen to ensure the live filter includes all chains.
        {"cursor": "2023-01-01T00:00:00Z"},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    list(records)
    # Truncated: preserved since (NOT max emitted 2024-01-20).
    assert offset.get("cursor") == "2023-01-01T00:00:00Z"
    assert offset.get("parent_idx") is not None


# --- ancestor-cursor incremental ---


@responses.activate
def test_ancestor_cursor_schema_adds_cursor_column_from_ancestor():
    """Notes doesn't have ModifiedAt; Children does. The schema should
    surface ModifiedAt (from Children's type) on the leaf rows."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children__Notes", {"cursor_field": "ModifiedAt"})
    names = [f.name for f in schema.fields]
    assert "ModifiedAt" in names
    # The ancestor-supplied column carries Children's type (TimestampType).
    cursor_type = type(schema["ModifiedAt"].dataType).__name__
    assert cursor_type == "TimestampType"


@responses.activate
def test_ancestor_cursor_incremental_filters_at_ancestor_level():
    """Cursor lives on Children (the ancestor). Filter should apply
    when fetching Children's keys; leaf (Notes) is fetched unfiltered
    under each matching ancestor and stamped with the ancestor's cursor."""
    _mock_nested_metadata()
    # Top-level Parents enumeration (no cursor filter at this level).
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    # Children fetch — cursor_field is in $select and $filter at this level.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 10, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 11, "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    # Leaf fetches for each filtered Child.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "a"}, {"Id": 101, "Text": "b"}]},
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes",
        json={"value": [{"Id": 200, "Text": "c"}]},
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children__Notes", {}, {"cursor_field": "ModifiedAt", "expand_contained": "false"}
    )
    rows = list(records)
    # All 3 leaf rows emitted; cursor value propagated from ancestor.
    assert len(rows) == 3
    assert all(r["ModifiedAt"] for r in rows)
    # Children with Id=10 stamps its ModifiedAt onto its two notes.
    notes_under_10 = [r for r in rows if r["Children_Id"] == 10]
    assert all(r["ModifiedAt"] == "2024-01-01T00:00:00Z" for r in notes_under_10)
    # Offset advances to max ancestor cursor.
    assert _drop_lb(offset) == {"cursor": "2024-01-02T00:00:00Z"}
    # Children call carries $orderby + ModifiedAt in $select.
    # First call has no $filter because since=None (the resume test covers that).
    # Call order: 0=$metadata, 1=Parents (PKs), 2=Children (cursor level), 3,4=leaf fetches.
    children_call = responses.calls[2].request.url
    assert "ModifiedAt" in children_call
    assert "%24orderby" in children_call or "$orderby" in children_call


@responses.activate
def test_ancestor_cursor_incremental_resume_filters_with_since():
    """A resumed call passes `cursor gt since` to the ancestor fetch
    and skips ancestors whose cursor is below the offset."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    # Children fetch returns only the newer Child (the older one filtered server-side).
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(11)/Notes",
        json={"value": [{"Id": 200, "Text": "c"}]},
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children__Notes",
        {"cursor": "2024-01-01T00:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert len(rows) == 1
    assert rows[0]["ModifiedAt"] == "2024-01-02T00:00:00Z"
    assert _drop_lb(offset) == {"cursor": "2024-01-02T00:00:00Z"}
    # Cursor filter present on the Children call (call index 2).
    children_call = responses.calls[2].request.url
    assert "ModifiedAt%20gt%20" in children_call or "ModifiedAt+gt+" in children_call


@responses.activate
def test_ancestor_cursor_first_batch_null_cursor_rows_raises():
    """Regression: streaming first batch passes ``start_offset = {}``.
    The ancestor-cursor no-progress guard used to be
    ``if start_offset and start_offset == end_offset`` — ``bool({})``
    is False so the guard was bypassed on the first trigger; rows
    stamped with a null ancestor cursor would commit, the offset would
    stay ``{}``, and every subsequent trigger would silently drop the
    same rows. The guard now uses bare ``==`` (safe because
    ``_finalize_cursor_read`` handles ``None`` — the batch-reader
    signal — explicitly before the equality check, and the streaming
    framework never passes ``None``) and raises so the operator sees
    the cause."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 10, "ModifiedAt": None}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "a"}]},
    )
    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        records, _ = c.read_table(
            "Parents__Children__Notes",
            {},
            {"cursor_field": "ModifiedAt"},
        )
        list(records)


@responses.activate
def test_ancestor_cursor_batch_mode_null_cursor_rows_emit_without_raise():
    """Batch reader passes ``start_offset=None`` and discards the
    returned offset; the no-progress guard is streaming-only. Mirrors
    ``test_incremental_batch_mode_null_cursor_rows_emit_without_raise``
    for the ancestor-cursor path so a future refactor that
    re-normalizes None to {} inside
    ``_read_contained_incremental_ancestor_cursor`` (or its dispatch
    in ``_read_contained_incremental``) breaks loudly."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 10, "ModifiedAt": None}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "a"}]},
    )
    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        None,
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [100]


# --- read_table_metadata for contained paths ---


@responses.activate
def test_contained_metadata_snapshot_when_no_cursor():
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children", {})
    assert meta["ingestion_type"] == "snapshot"
    assert meta["cursor_field"] is None
    assert meta["primary_keys"] == ["Parents_Id", "Id"]


@responses.activate
def test_contained_metadata_cdc_when_cursor_field_set():
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children", {"cursor_field": "ModifiedAt"})
    assert meta["ingestion_type"] == "cdc"
    assert meta["cursor_field"] == "ModifiedAt"


@responses.activate
def test_contained_delta_tracking_enabled_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not supported on contained"):
        c.read_table("Parents__Children", None, {"delta_tracking": "enabled"})


@responses.activate
def test_contained_select_preserves_parent_fk_columns():
    """``select`` filters the leaf entity's own columns but must NOT
    strip the synthetic ancestor FK columns — those are how downstream
    Delta tables reconstruct the parent linkage."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children", {"select": "Id,Label"})
    names = [f.name for f in schema.fields]
    # FK column survives select; ModifiedAt is filtered out.
    assert "Parents_Id" in names
    assert "ModifiedAt" not in names
    assert "Id" in names
    assert "Label" in names


@responses.activate
def test_contained_path_cycle_detection_in_discovery():
    """A self-referential containment must not loop the discovery BFS."""
    cyclic_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Cycle" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Node">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Self" Type="Collection(Cycle.Node)" ContainsTarget="true"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Nodes" EntityType="Cycle.Node"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    responses.get(f"{SERVICE_URL}$metadata", body=cyclic_xml, status=200)
    c = _make()
    tables = c.list_tables_in_namespace(["Cycle"])
    # Self appears once (depth 2) but no further recursion.
    assert tables == ["Nodes", "Nodes__Self"]


@responses.activate
def test_contained_fk_name_clash_with_leaf_property_gets_underscore_prefix():
    """When the default FK column name (``<seg>_<pk>``) collides with a
    leaf entity property of the same name, the FK column gets a leading
    ``_`` prefix until it's unique. The leaf property keeps its name."""
    clash_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Clash" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Owner">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Items" Type="Collection(Clash.Item)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Item">
        <Key><PropertyRef Name="ItemId"/></Key>
        <Property Name="ItemId" Type="Edm.Int32" Nullable="false"/>
        <!-- Property that collides with the default FK column name
             ``Owners_Id`` (= the parent entity-set name + Id). The
             connector must prefix the FK column with ``_`` to keep
             both columns distinct. -->
        <Property Name="Owners_Id" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Owners" EntityType="Clash.Owner"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    responses.get(f"{SERVICE_URL}$metadata", body=clash_xml, status=200)
    c = _make()
    schema = c.get_table_schema("Owners__Items", {})
    names = [f.name for f in schema.fields]
    # FK gets the leading underscore; leaf property keeps the original name.
    assert "_Owners_Id" in names
    assert "Owners_Id" in names
    # Verify the FK is the FIRST column (prepended), property follows.
    assert names == ["_Owners_Id", "ItemId", "Owners_Id"]
    meta = c.read_table_metadata("Owners__Items", {})
    assert meta["primary_keys"] == ["_Owners_Id", "ItemId"]


@responses.activate
def test_contained_fk_default_naming_without_prefix():
    """When there's no name collision, FK columns use the plain
    ``<segment>_<pkname>`` form — no leading underscore."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children", {})
    names = [f.name for f in schema.fields]
    assert names[0] == "Parents_Id"  # default form, no prefix
    assert not names[0].startswith("_")


@responses.activate
def test_leaf_cursor_plain_walk_lookback_keeps_overlap_rows():
    """The plain N+1 leaf-cursor walk (cursor_probe=false) also utilises
    cursor_lookback: the per-chain `cursor gt` filter floors to read_since, so an
    overlap leaf (cursor <= since, > read_since) is re-emitted while the
    committed watermark stays the true max."""
    _mock_probe_metadata()
    since = "2020-06-10T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}, {"Id": 11}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-11T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(11)/Leaves",
        json={"value": [{"Id": 1101, "RecordLastModified": "2020-06-09T12:00:00Z"}]},  # overlap
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "false",  # plain N+1
            "pagination": "nextlink",
            "cursor_lookback_seconds": "86400",
            "expand_contained": "false",
        },
    )
    rows = list(recs)
    # Overlap leaf (1101, <= since) kept thanks to the floored filter.
    assert sorted(r["Id"] for r in rows) == [1001, 1101]
    assert offset["cursor"] == "2020-06-11T00:00:00Z"
    # No probe was issued (cursor_probe=false).
    assert not any("$expand" in c.request.url for c in responses.calls)


# ---------------------------------------------------------------------------
# contained_fetch — $batch for the full (snapshot / batch-reader) contained walks
# ---------------------------------------------------------------------------


@responses.activate
def test_contained_fetch_batch_snapshot_hydrates_via_batch():
    """Snapshot contained read (no cursor) with ``contained_fetch`` defaulting to
    ``batch``: per-leaf-parent hydrate goes through OData ``$batch`` — no
    per-parent GET to the leaf collection."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    responder = _batch_responder(
        [
            ("Parents(1)/Children", {"value": [{"Id": 11, "Label": "a"}]}),
            ("Parents(2)/Children", {"value": [{"Id": 21, "Label": "b"}]}),
            ("Parents", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        "Parents__Children", {}, {"expand_contained": "false"}
    )  # no cursor → snapshot
    rows = sorted((r["Parents_Id"], r["Id"]) for r in recs)
    assert rows == [(1, 11), (2, 21)]
    # The snapshot's terminal offset stays a bare {} — capability flags are NOT
    # merged in (a streaming snapshot quiesces on end == start; {} → {batch_ok}
    # would buy one extra full snapshot re-read).
    assert offset == {"snapshot_done": True}  # terminal snapshot marker (quiesce)
    # Both leaf collections hydrated via $batch; NO per-parent GET to /Children.
    assert any("Parents(1)/Children" in u for u in responder.seen)
    assert any("Parents(2)/Children" in u for u in responder.seen)
    assert not any(
        call.request.method == "GET" and "/Children" in call.request.url for call in responses.calls
    )
    # No $top on the batched sub-requests (server-driven paging).
    assert not any("Children" in u and "$top=" in u for u in responder.seen)
    # The capability probe matches the real sub-request shape — bare collection
    # URL, no $top (a server that rejects an explicit $top must not false-fail
    # the preflight and pin batch_ok=False for a hydrate shape that works).
    assert any("Children" not in u for u in responder.seen)
    assert not any("Children" not in u and "$top=" in u for u in responder.seen)


@responses.activate
def test_contained_fetch_single_uses_per_parent_gets():
    """``contained_fetch=single`` keeps the original behaviour: one GET per
    leaf-parent, and never touches ``$batch``."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"contained_fetch": "single"})
    assert [(r["Parents_Id"], r["Id"]) for r in recs] == [(1, 11)]
    assert not any(call.request.method == "POST" for call in responses.calls)
    assert any(
        call.request.method == "GET" and "Parents(1)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_contained_fetch_auto_falls_back_to_single_when_unsupported():
    """``auto`` (the default) against a server that rejects ``$batch`` (405)
    degrades to the per-parent GET walk — never raises."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})  # unset → auto → 405 → single
    assert [(r["Parents_Id"], r["Id"]) for r in recs] == [(1, 11)]
    assert any(
        call.request.method == "GET" and "Parents(1)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_contained_fetch_batch_strict_raises_when_unsupported():
    """``contained_fetch=batch`` is strict: a server that fails the ``$batch``
    capability preflight raises (no silent fall-back). An integer ``N > 1`` is
    likewise strict."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    c = _make()
    with pytest.raises(ValueError, match="requires OData .batch"):
        list(c.read_table("Parents__Children", {}, {"contained_fetch": "batch"})[0])
    c2 = _make()
    with pytest.raises(ValueError, match="requires OData .batch"):
        list(c2.read_table("Parents__Children", {}, {"contained_fetch": "5"})[0])


@responses.activate
def test_contained_fetch_batch_reader_stream_hydrates_via_batch():
    """The framework batch-reader stream (``start_offset=None`` on a cursor
    table) also honours ``contained_fetch=batch``: the lazy full walk hydrates
    each leaf-parent via ``$batch``."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responder = _batch_responder(
        [
            (
                "Parents(1)/Children",
                {"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
            ),
            ("Parents", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    # start_offset=None → LakeflowBatchReader path → _stream_contained_incremental
    recs, offset = c.read_table(
        "Parents__Children", None, {"cursor_field": "ModifiedAt", "expand_contained": "false"}
    )
    assert [(r["Parents_Id"], r["Id"]) for r in recs] == [(1, 11)]
    assert _drop_lb(offset) == {}  # batch reader discards the offset
    assert any("Parents(1)/Children" in u for u in responder.seen)
    assert not any(
        call.request.method == "GET" and "/Children" in call.request.url for call in responses.calls
    )


@responses.activate
def test_contained_fetch_one_uses_per_parent_gets():
    """``contained_fetch=1`` is equivalent to ``single``: one GET per leaf-parent,
    and never touches ``$batch``."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"contained_fetch": "1"})
    assert [(r["Parents_Id"], r["Id"]) for r in recs] == [(1, 11)]
    assert not any(call.request.method == "POST" for call in responses.calls)
    assert any(
        call.request.method == "GET" and "Parents(1)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_contained_fetch_numeric_chunks_batch_by_size():
    """``contained_fetch=2`` hydrates via ``$batch`` like ``batch`` but caps each
    request at 2 leaf-parent ops: 3 parents → two hydrate rounds (2 + 1)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}, {"Id": 3}]})
    responder = _batch_responder(
        [
            ("Parents(1)/Children", {"value": [{"Id": 11}]}),
            ("Parents(2)/Children", {"value": [{"Id": 21}]}),
            ("Parents(3)/Children", {"value": [{"Id": 31}]}),
            ("Parents", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table(
        "Parents__Children", {}, {"contained_fetch": "2", "expand_contained": "false"}
    )
    assert sorted((r["Parents_Id"], r["Id"]) for r in recs) == [(1, 11), (2, 21), (3, 31)]
    # No per-parent GET to /Children — all hydration went through $batch.
    assert not any(
        call.request.method == "GET" and "/Children" in call.request.url for call in responses.calls
    )
    # Ops per hydrate $batch POST (the ones carrying /Children) are capped at 2:
    # 3 leaf-parents, chunk size 2 → rounds of 2 then 1.
    op_counts = []
    for call in responses.calls:
        if call.request.method != "POST":
            continue
        reqs = json.loads(call.request.body)["requests"]
        if any("Children" in r["url"] for r in reqs):
            op_counts.append(len(reqs))
    assert sorted(op_counts) == [1, 2]


@responses.activate
def test_contained_fetch_invalid_value_raises():
    _mock_nested_metadata()
    c = _make()
    for bad in ("maybe", "0", "-1", "2.5", "auto:0", "batch:abc", "single:5", "5:2"):
        with pytest.raises(ValueError, match="Invalid contained_fetch"):
            c.read_table("Parents__Children", {}, {"contained_fetch": bad})


@responses.activate
def test_contained_fetch_auto_size_suffix_chunks_by_n():
    """``auto:2`` hydrates via ``$batch`` capped at 2 ops/request: 3 parents →
    two hydrate rounds (2 + 1)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}, {"Id": 3}]})
    responder = _batch_responder(
        [
            ("Parents(1)/Children", {"value": [{"Id": 11}]}),
            ("Parents(2)/Children", {"value": [{"Id": 21}]}),
            ("Parents(3)/Children", {"value": [{"Id": 31}]}),
            ("Parents", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"contained_fetch": "auto:2"})
    assert sorted((r["Parents_Id"], r["Id"]) for r in recs) == [(1, 11), (2, 21), (3, 31)]
    op_counts = []
    for call in responses.calls:
        if call.request.method != "POST":
            continue
        reqs = json.loads(call.request.body)["requests"]
        if any("Children" in r["url"] for r in reqs):
            op_counts.append(len(reqs))
    assert sorted(op_counts) == [1, 2]


@responses.activate
def test_contained_fetch_auto_size_suffix_falls_back_when_unsupported():
    """``auto:<N>`` keeps ``auto``'s fall-back: a server without ``$batch`` (405)
    degrades to the per-parent GET walk — never raises."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"contained_fetch": "auto:50"})
    assert [(r["Parents_Id"], r["Id"]) for r in recs] == [(1, 11)]
    assert any(
        call.request.method == "GET" and "Parents(1)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_contained_fetch_batch_size_suffix_strict_raises_when_unsupported():
    """``batch:<N>`` keeps ``batch``'s strictness: a server that fails the
    ``$batch`` preflight raises (no fall-back)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    c = _make()
    with pytest.raises(ValueError, match="requires OData .batch"):
        list(c.read_table("Parents__Children", {}, {"contained_fetch": "batch:200"})[0])


@responses.activate
def test_contained_fetch_single_suppresses_auto_batch_cascade():
    """An explicit ``contained_fetch=single`` also suppresses ``auto``'s
    no-probe ``$batch`` cascade (the probe is not applicable here — the
    leaf-parent is a snapshot level): the hydrate goes down the plain N+1 walk
    and no ``$batch`` POST is ever attempted."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Plains", json={"value": [{"Id": 5}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Plains(5)/Items",
        json={"value": [{"Id": 501, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(
        "Roots__Plains__Items",
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "contained_fetch": "single",
            "pagination": "nextlink",
        },
    )
    assert [(r["Roots_Id"], r["Plains_Id"], r["Id"]) for r in recs] == [(1, 5, 501)]
    assert not any(call.request.method == "POST" for call in responses.calls)


@responses.activate
def test_n1_truncation_offset_switch_to_true_ignores_parent_idx():
    """MID-FLIGHT switch, other direction: the N+1 walk truncated (parked
    ``parent_idx``, watermark held). Switching to ``true`` must ignore the N+1
    resume state, read the full $expand from the HELD watermark (parent 0's
    unread rows are re-covered — never skipped), and drop ``parent_idx`` from
    the outgoing offset."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body=_switch_tree(1001, "2020-06-01T00:00:00Z")
        ),
    )
    truncated = {"cursor": since, "parent_idx": 1}  # watermark held at truncation
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, dict(truncated), _switch_opts("true"))
    # parent_idx=1 would have SKIPPED Root 1 — the expand read must not honour it.
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert "parent_idx" not in offset
    assert any(f"gt {since}" in u for u in _expand_urls())


@responses.activate
def test_leaf_empty_completion_clears_foreign_expand_keys():
    """An ``expand_contained`` park flipped to the N+1 walk: on empty
    completion the leaf caller must clear the FOREIGN expand keys
    (``pending_fetches`` / ``running_max_cursor`` — and the ancestor walk's
    ``parent_cursor``) rather than let them ride every future offset, and
    must fold the stale running max into the committed cursor so those
    already-emitted rows aren't re-read forever."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]}, match_querystring=False)
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]}, match_querystring=False
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves", json={"value": []}, match_querystring=False
    )
    start = {
        "cursor": "2020-06-01T00:00:00Z",
        "parent_idx": 5,  # resumed checkpoint past every chain → empty completion
        "parent_cursor": "2020-03-01T00:00:00Z",
        "pending_fetches": [
            {"url": f"{SERVICE_URL}Roots?$marker=stale", "level": 0, "chain": [], "skip": 0}
        ],
        "running_max_cursor": "2020-06-05T00:00:00Z",
    }
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, start, _switch_opts("false"))
    assert list(recs) == []
    assert offset == {"cursor": "2020-06-05T00:00:00Z"}


@responses.activate
def test_ancestor_cursor_explicit_lookback_floors_enumeration():
    """An explicit ``cursor_lookback_seconds`` on an ANCESTOR-level
    ``cursor_field`` floors the ancestor ENUMERATION filter (it used to be
    rejected as a no-op — but re-enumerating recently-dirty ancestors
    re-reads their whole subtrees, which is exactly the duplicate-safe
    overlap this walk needs). Committed watermark 2020-01-01T00:00:00Z with
    a 3600s window must put ``gt 2019-12-31T23:00:00Z`` on the wire."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]}, match_querystring=False)
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": []}, match_querystring=False)
    c = _make()
    rows, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z"},
        {
            "cursor_field": "MidOnly",  # lives on Mids — an ancestor level
            "cursor_lookback_seconds": "3600",
            "expand_contained": "false",
            "cursor_probe": "false",
        },
    )
    list(rows)
    mids_urls = [
        unquote(call.request.url) for call in responses.calls if "/Mids" in call.request.url
    ]
    assert mids_urls and all("MidOnly gt 2019-12-31T23:00:00Z" in u for u in mids_urls)


@responses.activate
def test_leaf_cursor_walk_keyset_seek_guid_boundary_bare():
    """Round-28: the leaf-cursor N+1 cap walk's compound keyset seek (ALSO its
    cap-resume checkpoint) must render a guid PK boundary BARE. Round 27 only
    typed the flat walks; with a pre-recorded ``or_filter_ok=True`` (a typed
    walk probed first) the untyped seek went to the wire unprobed and 400d on
    strict servers."""
    from urllib.parse import unquote

    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}Accounts", json={"value": [{"AccountId": _GUID}]}, match_querystring=False
    )

    def _contacts_cb(request):
        url = unquote(request.url)
        if f"ContactId gt {_GUID2}" in url:  # correctly-typed bare seek
            return (200, {}, json.dumps({"value": []}))
        if "ContactId gt" in url:  # quoted seek — server would 400; loop the page
            return (
                200,
                {},
                json.dumps(
                    {"value": [{"ContactId": _GUID2, "ModifiedAt": "2020-06-01T00:00:00Z"}]}
                ),
            )
        return (
            200,
            {},
            json.dumps({"value": [{"ContactId": _GUID2, "ModifiedAt": "2020-06-01T00:00:00Z"}]}),
        )

    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Accounts({_GUID})/Contacts", callback=_contacts_cb
    )
    c = _make()
    c.__dict__["_or_filter_ok"] = True  # typed-first poisoning scenario: no probe shield
    recs, offset = c.read_table(
        "Accounts__Contacts",
        {"cursor": "2020-01-01T00:00:00Z"},
        {
            "cursor_field": "ModifiedAt",
            "expand_contained": "false",
            "cursor_probe": "false",
            "contained_fetch": "single",
            "pagination": "keyset",
            "cursor_lookback_seconds": "off",
        },
    )
    assert [r["ContactId"] for r in recs] == [_GUID2]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    seek_urls = [
        unquote(call.request.url)
        for call in responses.calls
        if "ContactId gt" in unquote(call.request.url)
    ]
    assert seek_urls, "leaf walk never issued a keyset seek"
    assert all(f"ContactId gt {_GUID2}" in u for u in seek_urls)
    assert not any(f"gt '{_GUID2}'" in u for u in seek_urls)


@responses.activate
def test_contained_shape_options_validated_on_flat_dispatch():
    """`contained_fetch` / `expand_contained` garbage was silently accepted
    on flat tables (their parsers only ran on contained paths) — the one
    place a typo'd enum option wasn't loud. Both now validate at dispatch,
    before any table HTTP."""
    _mock_metadata()
    c = _make({"token": "t"})
    with pytest.raises(ValueError, match="contained_fetch"):
        c.read_table("Customers", None, {"contained_fetch": "garbadge"})
    with pytest.raises(ValueError, match="expand_contained"):
        c.read_table("Customers", None, {"expand_contained": "yes"})
    # Only $metadata was fetched — the errors fired pre-HTTP.
    assert all("$metadata" in call.request.url for call in responses.calls)


# ---------------------------------------------------------------------------
# Round 36 — parked-parent cursor advance, expand-mode leaf select,
# redirect error surfacing, $batch value-list gate, lookback cycle span,
# option-parse hygiene
# ---------------------------------------------------------------------------


@responses.activate
def test_ancestor_walk_parked_parent_cursor_advance_rewalked():
    """A cap-parked parent whose ANCESTOR cursor advanced between batches must
    be re-walked in full at its new enumeration position — PK-only park
    matching skipped it (exclusive park) or resumed its stale mid-page link,
    and the batch's running_max then committed past the parent's new cursor,
    locking its updated subtree out of every future ``cursor gt`` filter:
    permanent silent loss on the most common churn shape (only the parked
    parent changes between triggers)."""
    _mock_nested_metadata()
    parents = [
        {"Id": 10, "Name": "2024-01-01T00:00:00Z"},
        {"Id": 20, "Name": "2024-06-01T00:00:00Z"},
    ]
    children = {
        10: [{"Id": 101, "Label": "v1"}, {"Id": 102, "Label": "v1"}],
        20: [{"Id": 201, "Label": "v1"}],
    }

    def _parents_cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(req.url).query).get("$filter", [""])[0])
        rows = list(parents)
        m = re.search(r"Name gt '?([^'&]+?)'?(?:\s|$)", flt)
        if m:
            rows = [r for r in rows if r["Name"] > m.group(1)]
        rows.sort(key=lambda r: (r["Name"], r["Id"]))
        return (200, {}, json.dumps({"value": rows}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    for pid in (10, 20):
        responses.add_callback(
            responses.GET,
            f"{SERVICE_URL}Parents({pid})/Children",
            callback=lambda _r, pid=pid: (200, {}, json.dumps({"value": children[pid]})),
        )
    c = _make()
    opts = {
        "cursor_field": "Name",  # lives on Parents — ancestor level 0
        "max_records_per_batch": "2",
        "pagination": "nextlink",
        "expand_contained": "false",
    }
    recs1, off1 = c.read_table("Parents__Children", {}, opts)
    rows1 = list(recs1)
    # Batch 1 drains P10 (2 rows = cap) and parks it exclusively.
    assert off1.get("parent_keys") == [{"Id": 10}]

    # Between batches ONLY the parked parent churns: cursor advances,
    # children updated. No other chain enumerates between its old and new
    # position, so a PK-only seek would skip it outright.
    parents[0]["Name"] = "2024-03-01T00:00:00Z"
    children[10] = [{"Id": 101, "Label": "v2"}, {"Id": 102, "Label": "v2"}]

    emitted = list(rows1)
    offset = off1
    for _ in range(4):  # bounded: cap=2 → at most 4 batches to quiesce
        recs, offset = c.read_table("Parents__Children", offset, opts)
        emitted.extend(recs)
        if set(offset) - {"lb_history", "lb_cycle_started"} == {"cursor"}:
            break
    got = {(r["Id"], r["Label"]) for r in emitted}
    # The updated subtree must be re-emitted (duplicate-safe), never lost.
    assert {(101, "v2"), (102, "v2"), (201, "v1")} <= got
    # And the final watermark is the true max.
    assert offset["cursor"] == "2024-06-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Round 37 — park identity both mismatch directions, delta_ok offset pinning,
# never-pad primary keys, rotation-aware OAuth error, metadata cache cap
# ---------------------------------------------------------------------------


@responses.activate
def test_ancestor_parked_link_survives_cursor_rendering_flip():
    """A PK-matched parked chain must NEVER take the strictly-before seek
    skip. When the parked parent's cursor TEXT changes to a form that sorts
    before the parked key — a same-instant rendering flip (``…00Z`` →
    ``…00.000Z``, e.g. a mixed-version load balancer) or a genuine
    regression — the round-36 identity fell through to the generic skip,
    dropping the parked link and losing the collection's undrained
    remainder while running_max committed past it. The fix re-walks the
    chain in full: duplicate-safe in BOTH mismatch directions."""
    _mock_nested_metadata()
    parents = [
        {"Id": 10, "Name": "2024-01-01T00:00:00Z"},
        {"Id": 20, "Name": "2024-06-01T00:00:00Z"},
    ]
    page1 = [{"Id": 101, "Label": "a"}, {"Id": 102, "Label": "b"}]
    page2 = [{"Id": 103, "Label": "c"}, {"Id": 104, "Label": "d"}]

    def _parents_cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(req.url).query).get("$filter", [""])[0])
        rows = list(parents)
        m = re.search(r"Name gt '?([^'&]+?)'?(?:\s|$)", flt)
        if m:
            rows = [r for r in rows if r["Name"] > m.group(1)]
        rows.sort(key=lambda r: (r["Name"], r["Id"]))
        return (200, {}, json.dumps({"value": rows}))

    def _children10_cb(req):
        if "%24skiptoken=abc" in req.url or "$skiptoken=abc" in req.url:
            return (200, {}, json.dumps({"value": page2}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": page1,
                    "@odata.nextLink": f"{SERVICE_URL}Parents(10)/Children?$skiptoken=abc",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(10)/Children", callback=_children10_cb
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 201, "Label": "e"}]})),
    )
    c = _make()
    opts = {
        "cursor_field": "Name",  # ancestor level 0
        "max_records_per_batch": "2",
        "pagination": "nextlink",
        "expand_contained": "false",
    }
    recs1, offset = c.read_table("Parents__Children", {}, opts)
    emitted = list(recs1)
    # Batch 1 drains page 1 (cap) and parks P10 WITH its page-2 link.
    assert offset.get("parent_keys") == [{"Id": 10}]
    assert offset.get("chain_next_link")

    # Same instant, new rendering — sorts strictly BEFORE the parked text
    # in the raw tie-break ('.' < 'Z').
    parents[0]["Name"] = "2024-01-01T00:00:00.000Z"

    for _ in range(6):
        recs, offset = c.read_table("Parents__Children", offset, opts)
        emitted.extend(recs)
        if set(offset) - {"lb_history", "lb_cycle_started"} == {"cursor"}:
            break
    ids = {r["Id"] for r in emitted}
    # Page 2 must be delivered (duplicates of page 1 are fine).
    assert {103, 104} <= ids and 201 in ids
    assert offset["cursor"] == "2024-06-01T00:00:00Z"


@responses.activate
def test_ancestor_parked_link_resumes_under_sustained_rendering_alternation():
    """A load balancer alternating same-instant renderings PER REQUEST
    (…00Z ↔ …00.000Z during a rolling deploy) must not livelock the capped
    ancestor walk. Round 37 treated every parked-cursor text mismatch as a
    change and re-walked from page 1 each batch — with per-request
    alternation the text NEVER matched, page 1 was re-fetched forever, and
    the alternating offset blinded the no-progress guard. Same-instant
    renderings now count as parked: the link resumes and the walk
    progresses."""
    _mock_nested_metadata()
    renders = ["2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000Z"]
    call_n = {"n": 0}
    page1 = [{"Id": 101, "Label": "a"}, {"Id": 102, "Label": "b"}]
    page2 = [{"Id": 103, "Label": "c"}, {"Id": 104, "Label": "d"}]

    def _parents_cb(_req):
        call_n["n"] += 1
        rows = [
            {"Id": 10, "Name": renders[call_n["n"] % 2]},  # alternates every request
            {"Id": 20, "Name": "2024-06-01T00:00:00Z"},
        ]
        return (200, {}, json.dumps({"value": rows}))

    def _children10_cb(req):
        if "skiptoken=abc" in req.url:
            return (200, {}, json.dumps({"value": page2}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": page1,
                    "@odata.nextLink": f"{SERVICE_URL}Parents(10)/Children?$skiptoken=abc",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents(10)/Children", callback=_children10_cb
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 201, "Label": "e"}]})),
    )
    c = _make()
    opts = {
        "cursor_field": "Name",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
        "expand_contained": "false",
    }
    emitted = []
    offset = {}
    completed = False
    for _ in range(6):
        recs, offset = c.read_table("Parents__Children", offset, opts)
        emitted.extend(recs)
        if set(offset) - {"lb_history", "lb_cycle_started"} == {"cursor"}:
            completed = True
            break
    assert completed, f"walk never completed under alternation; last offset {offset}"
    ids = {r["Id"] for r in emitted}
    assert {101, 102, 103, 104, 201} <= ids


@responses.activate
def test_ancestor_walk_int_str_rendering_alternation_progresses():
    """An ancestor cursor whose JSON rendering alternates 5000 ↔ "5000" per
    request (IEEE754Compatible flip) used to crash batch 2 with an uncaught
    TypeError from the running_max fold; with the incomparable-pair guard
    alone it would livelock like round 37. Same-instant numeric matching
    resumes the park and the walk completes."""
    _mock_nested_metadata()
    renders = [5000, "5000"]
    call_n = {"n": 0}

    def _parents_cb(_req):
        call_n["n"] += 1
        rows = [
            {"Id": 10, "Name": renders[call_n["n"] % 2]},
            {"Id": 20, "Name": 6000},
        ]
        return (200, {}, json.dumps({"value": rows}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=lambda _r: (
            200,
            {},
            json.dumps({"value": [{"Id": 101, "Label": "a"}, {"Id": 102, "Label": "b"}]}),
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 201, "Label": "e"}]})),
    )
    c = _make()
    opts = {
        "cursor_field": "Name",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
        "expand_contained": "false",
    }
    emitted = []
    offset = {}
    completed = False
    for _ in range(6):
        recs, offset = c.read_table("Parents__Children", offset, opts)
        emitted.extend(recs)
        if set(offset) - {"lb_history", "lb_cycle_started"} == {"cursor"}:
            completed = True
            break
    assert completed, f"walk never completed; last offset {offset}"
    assert {101, 102, 201} <= {r["Id"] for r in emitted}


def test_chain_seek_order_collation_honest_units():
    """Plain-text keys are ordered by the SERVER's collation, which ordinal
    Python comparison can't reproduce — they must compare as "unknown"
    (never a skip). Numbers, ISO instants, and numeric rendering flips stay
    decidable; same-instant renderings fall through to the next element."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _chain_seek_order,
        _chain_strictly_before,
    )

    # The round-43 loss shape: ordinal says "B2" < "a1"; a CI server says after.
    assert _chain_strictly_before(["B2"], ["a1"]) is False
    assert _chain_seek_order(["B2"], ["a1"]) == "unknown"
    assert _chain_seek_order(["a1"], ["B2"]) == "unknown"
    # Provable orders still decide.
    assert _chain_strictly_before([1], [2]) is True
    assert _chain_seek_order([2], [1]) == "after"
    assert _chain_strictly_before(["2024-01-01T00:00:00Z"], ["2024-02-01T00:00:00Z"]) is True
    assert _chain_seek_order([9], ["10"]) == "before"  # numeric rendering flip
    # Same-instant renderings are EQUAL at their position, not an order signal.
    assert _chain_seek_order(["2024-01-01T00:00:00Z", 1], ["2024-01-01T00:00:00.000Z", 2]) == (
        "before"
    )
    assert _chain_seek_order([True], [False]) == "unknown"  # bools never decide


def test_chain_seek_order_distinct_digit_string_pks_are_unknown():
    """`cursor_same_instant` calls "007"/"7" equal (right for watermarks);
    conflating them as one chain POSITION lets a later element decide order
    across two DIFFERENT parents — a false "before" skips an unwalked
    subtree past the vanished-anchor reset. Chain elements now conflate
    only same-RENDERING pairs."""
    from databricks.labs.community_connector.sources.odata._contained import _chain_seek_order
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_same_rendering,
    )

    # The round-45 loss shape: distinct parents, later element decided.
    assert _chain_seek_order(["7", 5], ["007", 9]) == "unknown"
    assert _chain_seek_order(["7", 9], ["007", 9]) == "unknown"
    # A number/numeric-string TYPE flip is one value's two renderings —
    # still conflates, later element still decides.
    assert _chain_seek_order([5000, 1], ["5000", 2]) == "before"
    # Chronological rendering flips still conflate (round-43 semantics).
    assert (
        _chain_seek_order(["2024-01-01T00:00:00Z", 1], ["2024-01-01T00:00:00.000Z", 2]) == "before"
    )
    # The predicate itself.
    assert cursor_same_rendering("007", "7") is False
    assert cursor_same_rendering("1.0", "1") is False
    assert cursor_same_rendering("0", "-0") is False
    assert cursor_same_rendering("5000", 5000) is True
    assert cursor_same_rendering("2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000Z") is True


@responses.activate
def test_vanished_anchor_with_digit_string_pks_resets_not_skips():
    """E2E pin of the round-45 loss: 3-level walk parks at parent '007';
    the parent vanishes; the resume seek used to conflate '007'/'7'
    numerically and let the child PK decide order ACROSS parents — ending
    the seek without the reset and folding running_max past parent '7's
    unwalked subtree (LId 3 permanently lost). Now all-unknown → reset →
    full recovery."""
    responses.get(f"{SERVICE_URL}$metadata", body=R45_DIGIT_PK_METADATA, status=200)
    state = {"batch": 0}
    leaves = {
        ("007", 9): [
            {"LId": 1, "ModifiedAt": "2024-06-01T00:00:00Z"},
            {"LId": 2, "ModifiedAt": "2024-06-02T00:00:00Z"},
        ],
        ("7", 5): [{"LId": 3, "ModifiedAt": "2024-03-01T00:00:00Z"}],
        ("7", 9): [{"LId": 4, "ModifiedAt": "2024-04-01T00:00:00Z"}],
    }

    def _leaf_cb(request, key):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        m = re.search(r"ModifiedAt gt (\S+)", flt)
        rows = [r for r in leaves.get(key, []) if not m or r["ModifiedAt"] > m.group(1)]
        return (200, {}, json.dumps({"value": rows}))

    def _roots_cb(_req):
        live = ["007", "7"] if state["batch"] == 0 else ["7"]
        return (200, {}, json.dumps({"value": [{"Id": i} for i in sorted(live)]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots", callback=_roots_cb)
    for p in ("007", "7"):
        responses.add_callback(
            responses.GET,
            f"{SERVICE_URL}Roots('{p}')/Mids",
            callback=lambda _r, p=p: (
                200,
                {},
                json.dumps({"value": [{"MId": 9}] if p == "007" else [{"MId": 5}, {"MId": 9}]}),
            ),
        )
        for mid in (5, 9):
            responses.add_callback(
                responses.GET,
                f"{SERVICE_URL}Roots('{p}')/Mids({mid})/Leaves",
                callback=lambda r, k=(p, mid): _leaf_cb(r, k),
            )
    opts = {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
        "cursor_probe": "false",
        "expand_contained": "false",
        "cursor_lookback_seconds": "off",
    }
    emitted, offset = [], {}
    for _ in range(6):
        recs, offset = _make().read_table("Roots__Mids__Leaves", offset, opts)
        rows = list(recs)
        emitted.extend(rows)
        state["batch"] += 1
        if emitted and not rows and "parent_keys" not in offset and "parent_idx" not in offset:
            break
    got = sorted({r["LId"] for r in emitted})
    # LId 2 was cap-trimmed in batch 1 and its parent vanished — not owed.
    assert {1, 3, 4} <= set(got)
