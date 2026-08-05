"""OData connector unit tests — pagination group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import logging
import re

import pytest
import responses

from tests.unit.sources.odata._odata_test_helpers import *  # noqa: F401,F403


# ---------------------------------------------------------------------------
# Snapshot read
# ---------------------------------------------------------------------------


@responses.activate
def test_snapshot_walks_nextlink_and_strips_control_props():
    _mock_metadata()
    page1 = {
        "@odata.context": "ignored",
        "value": [
            {"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z", "@odata.etag": "drop-me"},
        ],
        "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=p2",
    }
    page2 = {
        "value": [
            {"Id": 2, "Name": "B", "ModifiedAt": "2024-02-01T00:00:00Z"},
        ],
    }
    responses.add(responses.GET, f"{SERVICE_URL}Customers", json=page1, match_querystring=False)
    responses.get(f"{SERVICE_URL}Customers?$skiptoken=p2", json=page2)

    c = _make()
    records, offset = c.read_table("Customers", None, {})
    rows = list(records)
    assert _drop_lb(offset) == {}
    assert rows == [
        {"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"},
        {"Id": 2, "Name": "B", "ModifiedAt": "2024-02-01T00:00:00Z"},
    ]


@responses.activate
def test_snapshot_resolves_relative_nextlink_against_request_url():
    """Some OData servers return @odata.nextLink as a relative URL
    (e.g. just 'Customers?$skiptoken=...'). The connector must resolve
    it against the request URL rather than issuing a request with no
    scheme/host."""
    _mock_metadata()
    page1 = {
        "value": [
            {"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"},
        ],
        # Relative URL — only path + query, no scheme/host.
        "@odata.nextLink": "Customers?$skiptoken=p2",
    }
    page2 = {
        "value": [
            {"Id": 2, "Name": "B", "ModifiedAt": "2024-02-01T00:00:00Z"},
        ],
    }
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=page1,
        match_querystring=False,
    )
    # The resolved next URL must include the service root.
    responses.get(f"{SERVICE_URL}Customers?$skiptoken=p2", json=page2)

    c = _make()
    records, _ = c.read_table("Customers", None, {})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]


@responses.activate
def test_snapshot_path_absolute_nextlink_resolves_against_host():
    """A nextLink starting with '/' is resolved against the request's
    scheme+host, replacing the service-root path."""
    _mock_metadata()
    page1 = {
        "value": [{"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"}],
        "@odata.nextLink": "/V4/Northwind/Northwind.svc/Customers?$skiptoken=p2",
    }
    page2 = {
        "value": [{"Id": 2, "Name": "B", "ModifiedAt": "2024-02-01T00:00:00Z"}],
    }
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=page1,
        match_querystring=False,
    )
    # SERVICE_URL is https://example.com/odata/ ; the path-absolute next
    # link replaces /odata/ with /V4/Northwind/Northwind.svc/Customers...
    responses.get(
        "https://example.com/V4/Northwind/Northwind.svc/Customers?$skiptoken=p2",
        json=page2,
    )

    c = _make()
    records, _ = c.read_table("Customers", None, {})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]


@responses.activate
def test_snapshot_auto_drains_server_that_propagates_top_through_skiptokens():
    """Regression: a spec-compliant server may treat the connector's
    ``$top`` as a TOTAL-result limit (OData §11.2.5.3) and propagate the
    *remaining* budget through its ``@odata.nextLink`` skiptokens — e.g.
    Northwind: ``$top=1000`` → page 1's link carries ``$top=500`` → after
    1000 rows it emits no further link, even though the collection has
    more rows.

    The ``auto`` walk follows that link chain, so it stops when the link
    disappears. The bug was trusting that link-less short final page as
    end-of-collection, silently capping any table larger than ``$top`` at
    exactly ``$top`` rows (observed live: Northwind ``Order_Details`` /
    ``Invoices`` / ``Order_Details_Extendeds``, 2155 rows each → 1000).

    The fix: when the link chain terminates at exactly the ``$top`` budget
    (``fetched >= top``), don't trust it — issue a keyset/``$skip`` seek
    past the budget and keep draining until an empty page.

    This models the server with ``$top``-budget=4 (the connector's
    ``page_size``) and a server page of 2 over a 10-row corpus, so the
    full table must drain to all 10 despite the chain self-terminating at
    every 4-row budget.
    """
    _mock_metadata()
    corpus = [
        {"Id": i, "Name": f"r{i}", "ModifiedAt": "2024-01-01T00:00:00Z"} for i in range(1, 11)
    ]
    SERVER_PAGE = 2

    def _callback(request):
        url = request.url.replace("%20", " ")

        def _q(name):
            m = re.search(rf"[?&]\${name}=([^&]+)", url)
            return m.group(1) if m else None

        top = int(_q("top"))  # the connector always sizes the request
        skiptoken = _q("skiptoken")
        # Lower bound = max of the skiptoken (last Id of the prior page in
        # this budgeted chain) and any keyset-seek `Id gt N` filter.
        lower = int(skiptoken) if skiptoken is not None else 0
        fm = re.search(r"Id gt (\d+)", url)
        if fm:
            lower = max(lower, int(fm.group(1)))
        candidate = sorted((r for r in corpus if r["Id"] > lower), key=lambda r: r["Id"])
        page = candidate[:SERVER_PAGE]
        body = {"value": page}
        remaining = top - len(page)
        # Emit a continuation link ONLY while budget remains AND more rows
        # exist — and propagate the *decremented* $top, like Northwind.
        if remaining > 0 and len(candidate) > len(page):
            link = f"{SERVICE_URL}Customers?$top={remaining}&$skiptoken={page[-1]['Id']}"
            if fm:
                link += f"&$filter=Id gt {fm.group(1)}"
            body["@odata.nextLink"] = link
        return (200, {}, json.dumps(body))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    records, offset = c.read_table("Customers", None, {"page_size": "4"})
    rows = list(records)
    assert _drop_lb(offset) == {}
    # The whole collection drains despite the $top-budget chain ending at
    # every 4th row. (Pre-fix: stopped at the first short link-less page → 4.)
    assert [r["Id"] for r in rows] == list(range(1, 11))


@responses.activate
def test_no_auth_configured_401_raises_actionable_permission_error():
    """Connection without any auth fields. A 401 here means the
    service requires auth — the connector tells the operator which
    auth_type values are valid."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="anon")
    c = _make()  # no auth options at all
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "No authentication" in msg
    assert "bearer, basic, api_key" in msg
    assert "COMMUNITY OAuth connection" in msg  # the UC-managed alternative


@responses.activate
def test_page_size_rejects_non_positive_and_non_numeric():
    """``page_size`` must be a positive integer. ``$top=0`` is a valid URL
    the server answers with an empty page — the client-driven drain reads
    that as exhaustion, so every read would silently emit ZERO rows; a
    non-numeric value rides into the URL raw and surfaces only as a
    confusing server 400. Reject both up front like every other numeric
    table option."""
    _mock_metadata()
    c = _make()
    for bad in ("0", "-5", "abc", "4.5"):
        with pytest.raises(ValueError, match="positive integer"):
            c.read_table("Customers", None, {"page_size": bad})


@responses.activate
def test_page_size_validated_on_partition_entry_points():
    """A partitionable table streams through is_partitioned/get_partitions,
    never read_table — its page_size validation must fire there too."""
    _mock_nested_metadata()
    c = _make({"page_size": "0"})  # is_partitioned reads self.options
    with pytest.raises(ValueError, match="positive integer"):
        c.is_partitioned("Parents__Children")
    c2 = _make()
    with pytest.raises(ValueError, match="positive integer"):
        c2.get_partitions("Parents__Children", {"page_size": "abc"})


@responses.activate
def test_no_top_emitted_when_page_size_unset():
    """With no ``page_size`` the connector sends no ``$top`` at all and
    lets the server choose its page size. Covers flat, contained N+1,
    and ``expand_contained=true`` URL builders."""
    _mock_nested_metadata()
    c = _make()
    flat = c._build_url("Parents", {})
    assert "$top" not in flat
    leaf = c._build_contained_url(["Parents", "Children"], [{"Id": 7}], {})
    assert "$top" not in leaf
    expand = c._build_expand_url(["Parents", "Children", "Notes"], {})
    assert "$top" not in expand
    # Nested $expand clauses still nest and still carry $orderby — only
    # $top is dropped; ``Leaf()`` empty-paren forms are not produced.
    assert "$expand=Children($orderby=Id asc;$expand=Notes($orderby=Id asc))" in expand


@responses.activate
def test_top_emitted_when_page_size_set():
    """Setting ``page_size`` restores the ``$top`` (flat = the value
    verbatim)."""
    _mock_nested_metadata()
    c = _make()
    assert "$top=250" in c._build_url("Parents", {"page_size": "250"})


@responses.activate
def test_page_size_default_split_by_ingest_type():
    """``read_table`` defaults ``page_size`` to ``1000`` (→ ``$top=1000``)
    for both cursor-based and snapshot ingest, because the default
    ``pagination=auto`` needs a ``$top`` to detect a full page. Setting
    ``pagination=nextlink`` restores the $top-free snapshot scan (server
    picks the page size)."""
    _mock_metadata()
    captured = []

    def cb(req):
        captured.append(req.url)
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    # Snapshot under default pagination=auto → $top=1000.
    list(c.read_table("Customers", None, {})[0])
    assert "$top=1000" in captured[-1]
    # Cursor-based (cursor_field, no page_size) → default $top=1000.
    list(c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})[0])
    assert "$top=1000" in captured[-1]
    # Opting back into nextlink drops $top on a snapshot scan.
    list(c.read_table("Customers", None, {"pagination": "nextlink"})[0])
    assert "$top" not in captured[-1]


# --- client-driven pagination (keyset / skip / auto) ----------------------


def test_pagination_url_helpers():
    from databricks.labs.community_connector.sources.odata.odata import (
        _pg_get_query,
        _pg_orderby_keys,
        _pg_parse_top,
        _pg_set_query,
        _pg_with_extra_filter,
    )

    u = "https://x/Set?$top=2&$orderby=ModifiedAt asc,Id asc"
    assert _pg_parse_top(u) == 2
    assert _pg_orderby_keys(u) == ["ModifiedAt", "Id"]
    assert _pg_get_query(u, "$top") == "2"
    # descending sort can't be walked with a `gt` seek
    assert _pg_orderby_keys("https://x/S?$orderby=Id desc") == []
    # set/replace/append $skip
    assert _pg_set_query("https://x/S?$top=2", "$skip", "4").endswith("&$skip=4")
    assert "$skip=6" in _pg_set_query("https://x/S?$top=2&$skip=4", "$skip", "6")
    # add a $filter when none, AND into an existing one
    assert (
        _pg_with_extra_filter("https://x/S?$top=2", "Id gt 5")
        == "https://x/S?$top=2&$filter=Id gt 5"
    )
    assert (
        _pg_with_extra_filter("https://x/S?$filter=A eq 1&$top=2", "Id gt 5")
        == "https://x/S?$filter=(A eq 1) and (Id gt 5)&$top=2"
    )


def test_pagination_keyset_filter_compound():
    from databricks.labs.community_connector.sources.odata.odata import _pg_keyset_filter

    assert _pg_keyset_filter(["Id"], {"Id": 2}) == "Id gt 2"
    # compound seek continues *within* a same-cursor cohort
    assert _pg_keyset_filter(
        ["ModifiedAt", "Id"], {"ModifiedAt": "2024-01-01T00:00:00Z", "Id": 2}
    ) == (
        "(ModifiedAt gt 2024-01-01T00:00:00Z) or "
        "(ModifiedAt eq 2024-01-01T00:00:00Z and Id gt 2)"
    )
    # null boundary value → no comparable seek (caller falls back to $skip)
    assert _pg_keyset_filter(["ModifiedAt", "Id"], {"ModifiedAt": None, "Id": 2}) is None


@responses.activate
def test_pagination_keyset_drains_collection_without_nextlink():
    """A server that page-limits but never emits @odata.nextLink: keyset
    mode seeks the next page via `Id gt <last>` and drains all rows."""
    _mock_metadata()
    data = _pagination_dataset()
    seen_filters = []

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(req.url).query)
        top = int(q.get("$top", ["1000"])[0])
        flt = unquote(q.get("$filter", [""])[0])
        seen_filters.append(flt)
        rows = data
        if "Id gt" in flt:
            n = int(re.search(r"Id gt (\d+)", flt).group(1))
            rows = [r for r in data if r["Id"] > n]
        return (200, {}, json.dumps({"value": rows[:top]}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    rows, _ = c.read_table("Customers", None, {"pagination": "keyset", "page_size": "2"})
    assert [r["Id"] for r in rows] == [1, 2, 3, 4, 5]
    assert any("Id gt" in f for f in seen_filters)  # actually seeked, not one page


@responses.activate
def test_pagination_skip_drains_collection_without_nextlink():
    """`skip` mode pages via $top + $skip for keyless/non-seekable sources."""
    _mock_metadata()
    data = _pagination_dataset()

    def cb(req):
        from urllib.parse import parse_qs, urlparse

        q = parse_qs(urlparse(req.url).query)
        top = int(q.get("$top", ["1000"])[0])
        skip = int(q.get("$skip", ["0"])[0])
        return (200, {}, json.dumps({"value": data[skip : skip + top]}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    rows, _ = c.read_table("Customers", None, {"pagination": "skip", "page_size": "2"})
    assert [r["Id"] for r in rows] == [1, 2, 3, 4, 5]


def test_keyset_seek_url_strips_positional_params():
    """A keyset seek positions absolutely via its $filter — any positional
    param retained from the entry URL ($skip on a resumed parked checkpoint
    or inner-expand continuation, a stray $skipToken in any casing) would
    ALSO be applied by the server, skipping rows INSIDE the seek window on
    every seek page."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _pg_keyset_seek_url,
    )

    url = "https://svc/Coll?$top=100&$orderby=Id%20asc&$skip=40&%24skipToken=abc"
    out = _pg_keyset_seek_url(url, None, "Id gt 140")
    assert "$skip" not in out and "skipToken" not in out
    assert "$filter=Id gt 140" in out
    assert "$top=100" in out and "$orderby=Id%20asc" in out  # non-positional kept


@responses.activate
def test_keyset_seek_from_resumed_skip_checkpoint_drops_the_skip():
    """A keyset walk that fell back to $skip (null boundary) parks $skip
    continuation URLs in the offset. On cap-resume the drain re-derives
    can_keyset from mode + $orderby alone; once a boundary row has non-null
    keys the seek is built from the parked URL — retaining its $skip would
    make the server skip N rows inside every seek window (silent, repeating
    loss)."""
    _mock_metadata()
    data = [{"Id": i} for i in range(1, 7)]
    seen_filters = []

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(req.url).query)
        top = int(q.get("$top", ["1000"])[0])
        skip = int(q.get("$skip", ["0"])[0])
        flt = unquote(q.get("$filter", [""])[0])
        seen_filters.append((flt, skip))
        rows = data
        if "Id gt" in flt:
            n = int(re.search(r"Id gt (\d+)", flt).group(1))
            rows = [r for r in data if r["Id"] > n]
        return (200, {}, json.dumps({"value": rows[skip : skip + top]}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    # Resumed parked checkpoint: rows 1-2 were emitted by a previous batch.
    parked = f"{SERVICE_URL}Customers?$top=2&$orderby=Id%20asc&$skip=2"
    got = [r["Id"] for page, _n in c._client_paginate_pages(parked, "keyset") for r in page]
    assert got == [3, 4, 5, 6]
    # The walk actually re-engaged keyset (not a silent skip-mode pass), and
    # no seek request carried a residual $skip.
    assert any("Id gt" in flt for flt, _ in seen_filters)
    assert all(skip == 0 for flt, skip in seen_filters if "Id gt" in flt)


@responses.activate
def test_no_progress_guard_ignores_identical_projected_pages():
    """With a low-cardinality $select, two DISTINCT consecutive pages can be
    identical after the @odata.* strip. The no-progress fingerprint must use
    the RAW items (per-entity annotations disambiguate) or the guard stops
    the walk with rows unread."""
    _mock_metadata()

    def cb(req):
        from urllib.parse import parse_qs, urlparse

        q = parse_qs(urlparse(req.url).query)
        top = int(q.get("$top", ["1000"])[0])
        skip = int(q.get("$skip", ["0"])[0])
        data = [{"@odata.id": f"e{i}", "Status": "A"} for i in range(1, 5)]
        return (200, {}, json.dumps({"value": data[skip : skip + top]}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    url = f"{SERVICE_URL}Customers?$top=2&$select=Status"
    pages = [page for page, _n in c._client_paginate_pages(url, "skip")]
    assert sum(len(p) for p in pages) == 4  # both identical-looking pages kept


@responses.activate
def test_pagination_auto_follows_nextlink_then_falls_back_to_keyset():
    """`auto`: trust @odata.nextLink while emitted; when a full page arrives
    without one, fall back to keyset for the rest of the collection."""
    _mock_metadata()
    data = _pagination_dataset()

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(req.url).query)
        top = int(q.get("$top", ["2"])[0])
        if "skiptoken" in req.url:
            # page 2: server-paged, full, but NO nextLink → triggers fallback
            return (200, {}, json.dumps({"value": data[2:4]}))
        flt = unquote(q.get("$filter", [""])[0])
        if "Id gt" in flt:
            n = int(re.search(r"Id gt (\d+)", flt).group(1))
            return (200, {}, json.dumps({"value": [r for r in data if r["Id"] > n][:top]}))
        # page 1: full page WITH a nextLink the connector should follow
        return (
            200,
            {},
            json.dumps(
                {
                    "value": data[0:2],
                    "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=p2&$top=2",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    rows, _ = c.read_table("Customers", None, {"pagination": "auto", "page_size": "2"})
    assert [r["Id"] for r in rows] == [1, 2, 3, 4, 5]


@responses.activate
def test_pagination_invalid_value_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid pagination"):
        c.read_table("Customers", None, {"pagination": "bogus"})


@responses.activate
def test_pagination_keyset_splits_same_cursor_cohort_in_contained_leaf_walk():
    """Phase 2: the contained leaf-cursor walk paginates via keyset too.
    A parent whose leaf collection is a single cursor value larger than a
    page — and a server that omits @odata.nextLink — is drained in full by
    the compound ``(cursor eq V and pk gt last)`` seek. Under ``nextlink``
    this same setup would silently stop after the first page."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    same = "2024-01-01T00:00:00Z"
    children = [{"Id": i, "Label": chr(96 + i), "ModifiedAt": same} for i in (11, 12, 13)]

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(req.url).query)
        top = int(q.get("$top", ["1000"])[0])
        flt = unquote(q.get("$filter", [""])[0])
        rows = children
        if flt:
            # Our keyset predicate, possibly AND-ed with the cursor filter:
            #   (ModifiedAt gt X) or (ModifiedAt eq X and Id gt N)
            gt = re.search(r"ModifiedAt gt ([0-9T:\-Z]+)", flt)
            eq_id = re.search(r"ModifiedAt eq ([0-9T:\-Z]+) and Id gt (\d+)", flt)

            def keep(r):
                if gt and r["ModifiedAt"] > gt.group(1):
                    return True
                if eq_id and r["ModifiedAt"] == eq_id.group(1) and r["Id"] > int(eq_id.group(2)):
                    return True
                return False

            rows = [r for r in children if keep(r)]
        return (200, {}, json.dumps({"value": rows[:top]}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=cb)
    c = _make()
    rows, offset = c.read_table(
        "Parents__Children",
        {},
        {"cursor_field": "ModifiedAt", "pagination": "keyset", "page_size": "2"},
    )
    assert [r["Id"] for r in rows] == [11, 12, 13]
    assert _drop_lb(offset) == {"cursor": same}


@responses.activate
def test_pagination_keyset_continues_inner_expand_when_nextlink_omitted():
    """Part B: ``expand_contained=true`` + ``pagination=keyset``. A parent's
    inline child collection arrives as a FULL page (== inner ``$top``) with
    NO ``Children@odata.nextLink``. The connector synthesizes a direct-nav
    keyset continuation (``Parents(1)/Children?...&$filter=Id gt <last>``)
    and drains the rest instead of silently dropping them — the inner-expand
    hole that nextlink-only mode leaves open."""
    _mock_nested_metadata()
    # page_size=1000 over a 2-level expand → child $top = 10 (see
    # compute_dynamic_tops). A 10-row inline page therefore looks truncated.
    child_top = 10
    inline = [
        {"Id": i, "Label": f"c{i}", "ModifiedAt": "2024-01-01T00:00:00Z"}
        for i in range(11, 11 + child_top)  # 11..20 — a full page
    ]

    def _floor(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        gts = re.findall(r"Id gt (\d+)", flt)
        return max(int(g) for g in gts) if gts else None

    def _parents(request):
        # Full inline child page, NO Children@odata.nextLink. Honor the keyset
        # seek so the top-level walk terminates (empty past the one parent).
        if _floor(request) is not None:
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "p", "Children": inline}]}))

    cont_urls = []
    after = [
        {"Id": i, "Label": f"c{i}", "ModifiedAt": "2024-01-02T00:00:00Z"} for i in range(21, 26)
    ]

    def _children(request):
        cont_urls.append(request.url.replace("%20", " ").replace("%24", "$"))
        floor = _floor(request) or 0
        return (200, {}, json.dumps({"value": [r for r in after if r["Id"] > floor]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_children)

    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"expand_contained": "true", "pagination": "keyset", "page_size": "1000"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == list(range(11, 26))  # all 15, none dropped
    # Terminal streaming-snapshot offset carries the quiesce marker
    # (a bare {} crashed the pyspark wrapper on non-empty batches).
    assert _drop_lb(offset) == {"snapshot_done": True}
    # First continuation seeks past the last inline child (Id 20), NOT a $skip;
    # a second (empty) request terminates the drain (keyset stops on empty).
    assert "Parents(1)/Children" in cont_urls[0]
    assert "Id gt 20" in cont_urls[0]
    assert "$skip" not in cont_urls[0]
    assert len(cont_urls) == 2
    # The continuation roots at Children with Parents(1) a fixed key, so the
    # page_size budget is spent entirely on the one remaining collection level:
    # $top=1000, NOT the [100, 10] root-level share (10) the initial request
    # gave the inline Children expand.
    assert "$top=1000" in cont_urls[0]


@responses.activate
def test_pagination_skip_continues_inner_expand_when_nextlink_omitted():
    """Part B, ``pagination=skip``: same inner-expand truncation, but the
    synthesized continuation resumes via ``$skip=<inline_count>`` rather than
    a keyset seek."""
    _mock_nested_metadata()
    inline = [{"Id": i, "Label": f"c{i}"} for i in range(11, 21)]  # 10 == child $top
    all_children = [{"Id": i, "Label": f"c{i}"} for i in range(11, 26)]  # full direct collection

    def _skip(request):
        from urllib.parse import parse_qs, urlparse

        return int(parse_qs(urlparse(request.url).query).get("$skip", ["0"])[0])

    def _parents(request):
        # Honor $skip so the top-level walk terminates past the one parent.
        if _skip(request) > 0:
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "p", "Children": inline}]}))

    cont_urls = []

    def _children(request):
        cont_urls.append(request.url.replace("%20", " ").replace("%24", "$"))
        from urllib.parse import parse_qs, urlparse

        top = int(parse_qs(urlparse(request.url).query).get("$top", ["1000"])[0])
        skip = _skip(request)
        return (200, {}, json.dumps({"value": all_children[skip : skip + top]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_children)

    c = _make()
    records, _ = c.read_table(
        "Parents__Children",
        {},
        {"expand_contained": "true", "pagination": "skip", "page_size": "1000"},
    )
    assert [r["Id"] for r in list(records)] == list(range(11, 26))
    # First continuation skips past the inline page; a second (empty) request
    # past the end terminates the drain.
    assert "$skip=10" in cont_urls[0]
    assert " gt " not in cont_urls[0]
    assert len(cont_urls) == 2


@responses.activate
def test_pagination_keyset_continued_inner_expand_reexpands_grandchildren():
    """Part B, 3-level: when a truncated MID-level child collection is
    continued, the synthesized URL re-expands the grandchildren
    (``Parents(1)/Children?...&$expand=Notes(...)``) so leaf rows under the
    continued children still flow, FK-tagged with the full ancestor chain."""
    _mock_nested_metadata()

    # page_size=1000 over a 3-level expand → Children $top = 5, Notes $top = 5.
    def _child(cid):
        return {"Id": cid, "Label": f"c{cid}", "Notes": [{"Id": 1000 + cid, "Text": f"n{cid}"}]}

    inline_children = [_child(cid) for cid in range(11, 16)]  # 5 == Children $top → truncated
    after_children = [_child(cid) for cid in (16, 17)]

    def _floor(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        gts = re.findall(r"Id gt (\d+)", flt)
        return max(int(g) for g in gts) if gts else None

    def _parents(request):
        if _floor(request) is not None:
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps({"value": [{"Id": 1, "Name": "p", "Children": inline_children}]}),
        )

    cont_urls = []

    def _children(request):
        cont_urls.append(request.url.replace("%20", " ").replace("%24", "$"))
        floor = _floor(request) or 0
        return (200, {}, json.dumps({"value": [c for c in after_children if c["Id"] > floor]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_children)
    # Each child carries a single inline Note (short, link-less) → the inner
    # drainer probes past it. One empty page per child confirms exhaustion.
    responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(SERVICE_URL)}Parents\(1\)/Children\(\d+\)/Notes"),
        callback=lambda req: (200, {}, json.dumps({"value": []})),
    )

    c = _make()
    records, _ = c.read_table(
        "Parents__Children__Notes",
        {},
        {"expand_contained": "true", "pagination": "keyset", "page_size": "1000"},
    )
    rows = list(records)
    # One leaf Note per child, for children 11..17 — including the four
    # continued children (16, 17 from the continuation; 14, 15 were the tail
    # of the inline page). Every leaf row carries the full ancestor chain.
    assert sorted(r["Children_Id"] for r in rows) == [11, 12, 13, 14, 15, 16, 17]
    assert all(r["Parents_Id"] == 1 for r in rows)
    assert {r["Id"] for r in rows} == {1000 + cid for cid in range(11, 18)}
    # First continuation re-expands the grandchildren and seeks past child 15;
    # a second (empty) request terminates the drain.
    assert "$expand=Notes" in cont_urls[0]
    assert "Id gt 15" in cont_urls[0]
    assert len(cont_urls) == 2


@responses.activate
def test_pagination_keyset_inner_expand_continuation_resumes_across_batches():
    """Part B, streaming: when ``max_records_per_batch`` fires partway
    through a synthesized inner-expand continuation, the parked work queue
    carries the keyset continuation URL so the next ``read()`` resumes the
    child collection exactly where it stopped — no rows dropped, none
    duplicated."""
    _mock_nested_metadata()
    # child $top = 10 at page_size=1000. Full pages of 10 keep the
    # continuation going across multiple keyset seeks.
    universe = {i: {"Id": i, "Label": f"c{i}"} for i in range(11, 34)}  # 11..33

    def _parents(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        if "Id gt" in flt:  # honor the keyset seek so the top-level walk ends
            return (200, {}, json.dumps({"value": []}))
        inline = [universe[i] for i in range(11, 21)]  # full page → continuation
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "p", "Children": inline}]}))

    def _children(request):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(request.url).query)
        top = int(q.get("$top", ["10"])[0])
        flt = unquote(q.get("$filter", [""])[0])
        # The connector rebuilds the seek from the original continuation URL
        # each page, so a parked seek (``Id gt 20``) gets the next page's seek
        # AND-ed on (``... and Id gt 30``) — bounded at two clauses, strictest
        # wins. Honour the max so the keyset advances.
        floors = [int(m) for m in re.findall(r"Id gt (\d+)", flt)]
        floor = max(floors) if floors else 0
        rows = [universe[i] for i in sorted(universe) if i > floor]
        return (200, {}, json.dumps({"value": rows[:top]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_children)

    c = _make()
    opts = {
        "expand_contained": "true",
        "pagination": "keyset",
        "page_size": "1000",
        "max_records_per_batch": "15",
    }
    seen, offset, batches = [], {}, 0
    while True:
        records, offset = c.read_table("Parents__Children", offset, opts)
        rows = list(records)
        seen.extend(r["Id"] for r in rows)
        batches += 1
        if not offset.get("pending_fetches"):
            break
        assert batches < 10  # guard against a non-terminating resume loop
    assert seen == list(range(11, 34))  # every child, in order, exactly once
    assert batches > 1  # the cap genuinely forced a cross-batch resume


@responses.activate
def test_pagination_no_progress_guard_stops_repeated_keyset_page(caplog):
    """A server that returns the same full page regardless of the keyset
    seek would loop forever. The no-progress guard detects the identical
    continuation page and stops, emitting each row exactly once and
    logging a warning."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        # Always the same full page (== $top=2), no nextLink, $filter ignored.
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "a"}, {"Id": 2, "Name": "b"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    with caplog.at_level(logging.WARNING):
        records, _ = c.read_table("Customers", None, {"pagination": "keyset", "page_size": "2"})
        rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]  # emitted once, not duplicated/looped
    assert len(calls) == 2  # page 1 + the one dup-detection fetch, then stop
    assert "made no progress" in caplog.text


@responses.activate
def test_pagination_no_progress_guard_stops_ignored_skip():
    """``skip`` against a server that ignores ``$skip`` returns the same
    page each time; the guard stops instead of looping (no $orderby keys,
    so the keyset path never engages — this exercises the skip branch)."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "a"}, {"Id": 2, "Name": "b"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    records, _ = c.read_table("Customers", None, {"pagination": "skip", "page_size": "2"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert len(calls) == 2


@responses.activate
def test_pagination_nextlink_guard_stops_self_referential_link():
    """pagination=nextlink: a server that points @odata.nextLink back at the
    just-fetched URL would loop forever; the guard stops after emitting the
    current page."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        n = len(calls)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": n, "Name": "x"}],
                    "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=x",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    records, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]  # page 1, then the self-referential page 2
    assert len(calls) == 2


@responses.activate
def test_pagination_nextlink_guard_stops_identical_page_cycle():
    """pagination=nextlink: a server that returns the same rows but a fresh
    nextLink token each time (URL keeps changing) is caught by the page
    fingerprint guard — the duplicate page is dropped, not re-emitted."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        n = len(calls)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": 1, "Name": "x"}],
                    "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=t{n}",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    records, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert len(calls) == 2


@responses.activate
def test_pagination_auto_guard_stops_self_referential_link():
    """pagination=auto: while following the server's @odata.nextLink, a
    self-referential link is caught by the URL-equality backstop."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        n = len(calls)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": n, "Name": "x"}],
                    "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=x",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=cb)
    c = _make()
    records, _ = c.read_table("Customers", None, {"pagination": "auto"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert len(calls) == 2


@responses.activate
def test_pagination_keyset_does_not_accumulate_filter_across_batches():
    """Regression: a contained leaf-cursor keyset walk that caps and resumes
    across many batches must NOT AND a fresh seek onto the previous one each
    batch (which grew the URL unboundedly toward HTTP 414). The base $filter
    is carried out-of-band so each batch's seek REPLACES the prior one."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    universe = [
        {"Id": 10 + i, "Label": chr(97 + i), "ModifiedAt": f"2024-01-0{i + 1}T00:00:00Z"}
        for i in range(7)  # 7 children, all distinct cursor values
    ]
    seen_filters = []

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        assert "__pgbase" not in req.url  # private marker never reaches the server
        q = parse_qs(urlparse(req.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        seen_filters.append(flt)
        top = int(q.get("$top", ["1000"])[0])
        # Honor the keyset seek: rows strictly after the greatest lower bound.
        gts = re.findall(r"ModifiedAt gt ([0-9T:\-Z]+)", flt)
        floor = max(gts) if gts else ""
        rows = [r for r in universe if r["ModifiedAt"] > floor]
        return (200, {}, json.dumps({"value": rows[:top]}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=cb)
    c = _make()
    opts = {
        "cursor_field": "ModifiedAt",
        "pagination": "keyset",
        "page_size": "2",
        "max_records_per_batch": "2",
    }
    seen, offset, batches = [], {}, 0
    while True:
        recs, offset = c.read_table("Parents__Children", offset, opts)
        seen.extend(r["Id"] for r in list(recs))
        batches += 1
        if not offset.get("chain_next_link"):
            break
        assert batches < 12  # guard against a non-terminating resume loop
    assert seen == [10, 11, 12, 13, 14, 15, 16]  # every child, in order, once
    assert batches > 2  # genuinely resumed across several batches
    # The fix: no request's $filter carries more than one keyset seek. The old
    # behaviour AND-ed one disjunction per batch, so this would have grown to 3+.
    assert max(f.count(" or (") for f in seen_filters) <= 1


@responses.activate
def test_pagination_keyset_drains_server_pages_below_requested_top():
    """Regression (xmla_demo mock): a server that caps each response BELOW the
    requested ``$top`` and omits ``@odata.nextLink``. A short page is NOT proof
    of exhaustion, so ``keyset`` keeps seeking until empty and reads every row.
    ``nextlink``/``auto`` would stop at the first short page (see the auto
    test below)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    server_cap = 3  # server returns at most 3 rows/response, ignoring $top=1000
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
        # Capped below the requested $top, and NO @odata.nextLink.
        return (200, {}, json.dumps({"value": rows[:server_cap]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=cb)
    c = _make()
    rows, offset = c.read_table(
        "Parents__Children",
        {},
        {"cursor_field": "ModifiedAt", "pagination": "keyset", "page_size": "1000"},
    )
    assert [r["Id"] for r in rows] == [10, 11, 12, 13, 14, 15, 16]  # all 7, not just first 3
    assert _drop_lb(offset) == {"cursor": "2024-01-07T00:00:00Z"}


@responses.activate
def test_pagination_auto_drains_snapshot_server_pages_below_top():
    """The xmla_demo scenario: a SNAPSHOT read (no cursor_field) of a server
    that caps each response below the requested ``$top`` and never emits an
    ``@odata.nextLink``. With the default ``pagination=auto``, a snapshot read
    falls back to the keyset seek and drains until empty — so every leaf row is
    read with no per-table override. (Cursor/incremental reads stay conservative
    here — see ``test_pagination_keyset_drains_server_pages_below_requested_top``
    for the explicit-keyset path that drains those.)"""
    _mock_nested_metadata()
    # Parents enumeration: one short page, then the drain probe sees empty.
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    children = [{"Id": 10 + i, "Label": f"c{i}"} for i in range(7)]

    def cb(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        gt = re.search(r"Id gt (\d+)", flt)  # snapshot keyset seeks on the PK
        rows = [r for r in children if (not flt) or (gt and r["Id"] > int(gt.group(1)))]
        return (200, {}, json.dumps({"value": rows[:3]}))  # cap 3, no nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=cb)
    c = _make()
    rows, _ = c.read_table(
        "Parents__Children",
        None,
        {"pagination": "auto", "page_size": "1000", "expand_contained": "false"},
    )
    # auto drains every capped page — all 7 leaf rows, not just the first 3.
    assert [r["Id"] for r in rows] == [10, 11, 12, 13, 14, 15, 16]


@responses.activate
def test_retry_honours_retry_after_seconds_header(monkeypatch):
    """``Retry-After: <seconds>`` from the server is the sleep duration."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    call_count = {"n": 0}

    def _customers(request):  # pylint: disable=unused-argument
        call_count["n"] += 1
        if call_count["n"] == 1:
            return (429, {"Retry-After": "7"}, '{"error": "throttled"}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "A"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make({"token": "t"})
    # pagination=nextlink keeps this focused on retry: the default auto would
    # add a trailing drain probe (an extra GET) after the short link-less page.
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]
    assert call_count["n"] == 2
    assert sleeps == [7.0]


@responses.activate
def test_retry_honours_retry_after_http_date_header(monkeypatch):
    """``Retry-After: <HTTP-date>`` is parsed to a delta-from-now."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    # 30 seconds in the future, formatted as an HTTP-date.
    from email.utils import format_datetime
    from datetime import datetime, timedelta, timezone as tz

    target = datetime.now(tz.utc) + timedelta(seconds=30)
    http_date = format_datetime(target, usegmt=True)
    call_count = {"n": 0}

    def _customers(request):  # pylint: disable=unused-argument
        call_count["n"] += 1
        if call_count["n"] == 1:
            return (503, {"Retry-After": http_date}, '{"error": "unavailable"}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "A"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make({"token": "t"})
    # pagination=nextlink: focus on retry, skip the default auto drain probe.
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]
    assert call_count["n"] == 2
    # Allow ±5 s wiggle for test scheduling jitter; importantly it should
    # be close to 30, not 0 (parse failure) or 60 (cap miscompare).
    assert len(sleeps) == 1
    assert 20.0 <= sleeps[0] <= 30.0


@responses.activate
def test_retry_no_header_uses_exponential_backoff(monkeypatch):
    """No Retry-After → backoff doubles per attempt (1, 2, 4 …)."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    call_count = {"n": 0}

    def _customers(request):  # pylint: disable=unused-argument
        call_count["n"] += 1
        if call_count["n"] < 4:
            return (429, {}, '{"error": "throttled"}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "A"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make({"token": "t"})
    # pagination=nextlink: focus on retry, skip the default auto drain probe.
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]
    assert call_count["n"] == 4
    assert sleeps == [1.0, 2.0, 4.0]


@responses.activate
def test_retry_503_also_retried(monkeypatch):
    """503 is treated the same as 429 — server temporarily unavailable."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    call_count = {"n": 0}

    def _customers(request):  # pylint: disable=unused-argument
        call_count["n"] += 1
        if call_count["n"] == 1:
            return (503, {"Retry-After": "2"}, "")
        return (200, {}, '{"value": [{"Id": 1, "Name": "A"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [1]
    assert sleeps == [2.0]


@responses.activate
def test_retry_exhaustion_raises_actionable_runtime_error(monkeypatch):
    """After max_retries 429s in a row, raise with an actionable message."""
    _mock_metadata()
    _patch_sleep(monkeypatch)
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"error": "rate-limited"},
        status=429,
        headers={"Retry-After": "1"},
    )
    c = _make({"token": "t", "max_retries": "2"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(RuntimeError) as ei:
        list(rows)
    msg = str(ei.value)
    assert "429" in msg
    assert "throttl" in msg.lower() or "unavailable" in msg.lower()
    assert "max_retries" in msg
    assert "retry_max_delay_seconds" in msg
    assert "Retry-After" in msg


@responses.activate
def test_retry_500_transient_then_recovers(monkeypatch):
    """A 500 Internal Server Error from the source is treated as
    transient (Hexagon SCApi's "Unexpected server failure" template
    is the prototype case) — the connector retries with exponential
    backoff and succeeds when the second attempt returns 200."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"error": {"code": "500", "message": "Unexpected server failure"}},
        status=500,
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 7}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [7]
    # Exponential backoff: first retry waits 1s (2**0).
    assert sleeps == [1.0]


@responses.activate
def test_retry_502_and_504_treated_as_transient(monkeypatch):
    """Bad Gateway (502) and Gateway Timeout (504) — almost always
    upstream-proxy issues — must also be retried. Sequence: 502, 504,
    200 → succeeds on the third attempt."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body="Bad Gateway",
        status=502,
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body="Gateway Timeout",
        status=504,
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 3}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [3]
    assert sleeps == [1.0, 2.0]


@responses.activate
def test_retry_after_capped_at_retry_max_delay_seconds(monkeypatch):
    """A pathological ``Retry-After: 9999`` is clamped at the cap."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    call_count = {"n": 0}

    def _customers(request):  # pylint: disable=unused-argument
        call_count["n"] += 1
        if call_count["n"] == 1:
            return (429, {"Retry-After": "9999"}, "")
        return (200, {}, '{"value": [{"Id": 1, "Name": "A"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make({"token": "t", "retry_max_delay_seconds": "10"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [1]
    assert sleeps == [10.0]


@responses.activate
def test_retry_disabled_when_max_retries_zero(monkeypatch):
    """``max_retries=0`` opts out — a single 429 raises immediately."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"error": "rate-limited"},
        status=429,
        headers={"Retry-After": "30"},
    )
    c = _make({"token": "t", "max_retries": "0"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(RuntimeError):
        list(rows)
    assert sleeps == []


# ---------------------------------------------------------------------------
# Transient network errors (TCP reset / timeout / mid-body disconnect)
# ---------------------------------------------------------------------------


@responses.activate
def test_retry_connection_error_recovers(monkeypatch):
    """``RemoteDisconnected`` mid-request retries on backoff (no header)."""
    import requests as _requests

    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    # First call: simulate the exact failure pattern observed in
    # production (RemoteDisconnected -> ConnectionError). Second call:
    # legitimate 200 with rows.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body=_requests.exceptions.ConnectionError("Connection aborted."),
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "Name": "A"}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [1]
    # No Retry-After possible on a connection error -> exponential.
    assert sleeps == [1.0]


@responses.activate
def test_retry_read_timeout_recovers(monkeypatch):
    """``requests.Timeout`` is treated like ConnectionError."""
    import requests as _requests

    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body=_requests.exceptions.ReadTimeout("server slow"),
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 7}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [7]
    assert sleeps == [1.0]


@responses.activate
def test_retry_chunked_encoding_error_recovers(monkeypatch):
    """Mid-body server disconnect surfaces as ChunkedEncodingError."""
    import requests as _requests

    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body=_requests.exceptions.ChunkedEncodingError("incomplete response"),
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 3}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [3]
    assert sleeps == [1.0]


@responses.activate
def test_retry_connection_error_exhausted_reraises_same_type(monkeypatch):
    """After max_retries+1 ConnectionErrors, re-raise as ConnectionError
    (not RuntimeError) so callers catching ConnectionError keep working."""
    import requests as _requests

    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    for _ in range(3):  # max_retries=2 -> 3 attempts total
        responses.add(
            responses.GET,
            f"{SERVICE_URL}Customers",
            body=_requests.exceptions.ConnectionError("Connection aborted."),
        )
    c = _make({"token": "t", "max_retries": "2"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(_requests.exceptions.ConnectionError) as ei:
        list(rows)
    msg = str(ei.value)
    assert "3 attempts" in msg
    assert "max_retries" in msg
    assert sleeps == [1.0, 2.0]


@responses.activate
def test_retry_emits_warning_log_on_transient_429(monkeypatch, caplog):
    """Every retried 429/503/network blip writes one WARNING line — so
    operators reading pipeline logs see how often the source flakes
    without enabling anything verbose. Mirrors the existing
    ``test_429_retry_after_seconds_used`` setup but with caplog
    instead of a response-count check."""
    import logging as _logging

    _mock_metadata()
    _patch_sleep(monkeypatch)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"error": "rate-limited"},
        status=429,
        headers={"Retry-After": "1"},
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1}]},
        status=200,
    )
    c = _make({"token": "t"})
    with caplog.at_level(
        _logging.WARNING, logger="databricks.labs.community_connector.sources.odata.odata"
    ):
        rows, _ = c.read_table("Customers", None, {})
        list(rows)
    warns = [r.getMessage() for r in caplog.records if r.levelno == _logging.WARNING]
    assert any("OData 429 on GET" in m and "retrying" in m for m in warns)


@responses.activate
def test_retry_json_decode_error_recovers(monkeypatch):
    """Some sources (e.g. Hexagon SCApi) intermittently emit a 200
    response with a truncated JSON body under load. The connector
    must treat that as transient and retry the GET — same shape as the
    `ChunkedEncodingError` recovery path."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    # First attempt: 200 with malformed JSON (single brace, EOF — exactly
    # the failure mode the SCApi customer hit).
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body="{",
        status=200,
        content_type="application/json",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 9}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [9]
    assert sleeps == [1.0]


@responses.activate
def test_retry_connection_error_then_throttle_then_success(monkeypatch):
    """ConnectionError -> 429 -> 200 in the same logical request all
    flow through the same retry loop without losing track of the
    attempt counter."""
    import requests as _requests

    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        body=_requests.exceptions.ConnectionError("aborted"),
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=429,
        headers={"Retry-After": "3"},
        body="",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1}]},
        status=200,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [1]
    # Attempt 0: ConnectionError -> 1s backoff.
    # Attempt 1: 429 with Retry-After: 3 -> 3s.
    # Attempt 2: 200 -> done.
    assert sleeps == [1.0, 3.0]


@responses.activate
def test_snapshot_contained_stream_preflight_cached_across_microbatches():
    """The user-visible fix the capability cache exists for: a contained
    SNAPSHOT stream keeps its offsets bare (``{}``), so the ``expand_contained
    =auto`` preflight can't ride the checkpoint — and the framework recreates
    the connector instance each microbatch. The process-wide cache must make
    microbatch 2 (fresh instance, bare offset) skip the probe entirely."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    tree = {
        "value": [
            {"Id": 1, "Mids": [{"Id": 10, "Leaves": [{"Id": 1001}]}]},
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    opts = {"pagination": "nextlink"}  # no cursor_field → snapshot; expand auto by default

    # Microbatch 1: preflight probe + expand read = 2 $expand GETs; the
    # terminal snapshot offset stays bare so the stream can quiesce.
    c1 = _make()
    recs1, offset1 = c1.read_table(PROBE_TABLE, {}, dict(opts))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs1] == [(1, 10, 1001)]
    assert offset1 == {"snapshot_done": True}  # terminal snapshot marker (quiesce)
    n_expand_1 = sum(1 for call in responses.calls if "$expand" in unquote(call.request.url))
    assert n_expand_1 == 2

    # Microbatch 2: FRESH instance, bare offset — the process cache serves the
    # verdict, so exactly ONE more $expand GET (the read), no probe.
    c2 = _make()
    recs2, offset2 = c2.read_table(PROBE_TABLE, {}, dict(opts))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs2] == [(1, 10, 1001)]
    assert offset2 == {"snapshot_done": True}  # terminal snapshot marker (quiesce)
    n_expand_2 = sum(1 for call in responses.calls if "$expand" in unquote(call.request.url))
    assert n_expand_2 == n_expand_1 + 1


@responses.activate
def test_snapshot_contained_stream_pin_false_purges_cache_then_auto_reprobes():
    """The reset contract must hold for the SNAPSHOT path too (bare offsets that
    the offset scrub never sees): auto records ``expand_ok`` → pinning ``false``
    purges the shared cache on the very next read (not just an offset-carrying
    transition) → re-selecting ``auto`` re-runs the preflight instead of reusing
    the stale verdict."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    tree = {"value": [{"Id": 1, "Mids": [{"Id": 10, "Leaves": [{"Id": 1001}]}]}]}
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=_expand_auto_roots_callback(expand_body=tree)
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001}]},
        match_querystring=False,
    )

    def n_expand():
        return sum(1 for c in responses.calls if "$expand" in unquote(c.request.url))

    # Microbatch 1 — auto: preflight + read, verdict recorded in the cache.
    c1 = _make()
    list(c1.read_table(PROBE_TABLE, {}, {"pagination": "nextlink"})[0])
    assert c1._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Microbatch 2 — pinned false (still a bare-offset snapshot): the read
    # purges the per-table verdict from the shared cache even though no offset
    # carried it, and issues no $expand.
    n_before = n_expand()
    c2 = _make()
    list(c2.read_table(PROBE_TABLE, {}, {"pagination": "nextlink", "expand_contained": "false"})[0])
    assert n_expand() == n_before  # pinned false never expands
    assert c2._cached_capability("expand_ok", table_name=PROBE_TABLE) is None  # purged

    # Microbatch 3 — back to auto: nothing cached → the preflight RE-RUNS.
    n_before = n_expand()
    c3 = _make()
    list(c3.read_table(PROBE_TABLE, {}, {"pagination": "nextlink"})[0])
    assert n_expand() == n_before + 2  # probe + read, freshly re-verified


@responses.activate
def test_keyset_seek_guid_boundary_renders_bare():
    """A keyset walk over a guid ``$orderby`` column must render the seek
    boundary BARE: ``AccountId gt '<guid>'`` is a type mismatch on strict
    servers (400 on every page-2 fetch)."""
    from urllib.parse import unquote

    _mock_guid_metadata()
    state = {"calls": 0}

    def _accounts_cb(request):
        state["calls"] += 1
        url = unquote(request.url)
        if "gt" in url:
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps({"value": [{"AccountId": _GUID, "Name": "a"}]}),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Accounts", callback=_accounts_cb)
    c = _make()
    recs, _ = c.read_table("Accounts", {}, {"pagination": "keyset", "page_size": "1"})
    assert [r["AccountId"] for r in recs] == [_GUID]
    seek_urls = [
        unquote(call.request.url) for call in responses.calls if "gt" in unquote(call.request.url)
    ]
    assert seek_urls, "keyset never issued a seek"
    assert any(f"AccountId gt {_GUID}" in u for u in seek_urls)
    assert not any(f"gt '{_GUID}'" in u for u in seek_urls)


@responses.activate
def test_retry_408_treated_as_transient(monkeypatch):
    """408 was in the probes' transient set but NOT the retry set — a flaky
    proxy emitting 408s killed a read that a 503-emitting one survived. It
    now retries like every other transient status."""
    _mock_metadata()
    sleeps = _patch_sleep(monkeypatch)
    call_count = {"n": 0}

    def _customers(request):  # pylint: disable=unused-argument
        call_count["n"] += 1
        if call_count["n"] == 1:
            return (408, {}, '{"error": "request timeout"}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "A"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]
    assert call_count["n"] == 2
    assert sleeps == [1.0]


def test_backoff_delay_is_jittered(monkeypatch):
    """Backoff multiplies by uniform(0.5, 1.0) so `num_partitions` tasks a
    throttling source knocked back together don't retry in lockstep."""
    c = _make()
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.random.uniform",
        lambda a, b: a,
    )
    assert c._backoff_delay(1) == 1.0  # 2**1 = 2, floor of the jitter band
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.random.uniform",
        lambda a, b: b,
    )
    assert c._backoff_delay(1) == 2.0  # ceiling: the pre-jitter value


@responses.activate
def test_stream_property_forced_nullable_in_schema():
    """Stream values never appear in JSON payloads (§11.2.4), so honoring
    Nullable="false" on an Edm.Stream property would fail EVERY row of the
    table on the framework's absent-non-nullable check."""
    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_STREAM_METADATA_XML, status=200)
    c = _make()
    schema = c.get_table_schema("Docs", {})
    (content,) = [f for f in schema.fields if f.name == "Content"]
    assert content.nullable is True


@responses.activate
def test_client_paginate_value_null_tolerated():
    """The client-driven pagination walk has its own value-array site —
    a spec-invalid `"value": null` reads as an empty page there too."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": None}, match_querystring=False)
    c = _make({"token": "t"})
    records, _ = c.read_table("Customers", None, {"pagination": "skip"})
    assert list(records) == []


@responses.activate
def test_snapshot_stream_marker_quiesces_flat():
    """Streaming snapshots mark the first pass done and quiesce with an EMPTY
    batch + unchanged offset — the only quiesce shape pyspark's simple-reader
    wrapper accepts (a non-empty batch with end==start raises
    SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE; the old bare-{} contract
    crashed on trigger 1). Idle triggers cost zero HTTP."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]},
        match_querystring=False,
    )
    c = _make({"token": "t"})
    rows1, off1 = c.read_table("Customers", {}, {})
    assert [r["Id"] for r in rows1] == [1]
    assert off1.get("snapshot_done") is True
    data_calls_before = sum(1 for call in responses.calls if "Customers" in call.request.url)
    rows2, off2 = c.read_table("Customers", off1, {})
    assert list(rows2) == [] and off2 == off1
    data_calls_after = sum(1 for call in responses.calls if "Customers" in call.request.url)
    assert data_calls_after == data_calls_before  # idle trigger: zero HTTP


@responses.activate
def test_snapshot_stream_marker_quiesces_contained():
    """Same marker rule for contained snapshot streams (N+1 shape)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]}, match_querystring=False)
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "x"}]},
        match_querystring=False,
    )
    c = _make()
    opts = {"expand_contained": "false", "contained_fetch": "single"}
    rows1, off1 = c.read_table("Parents__Children", {}, opts)
    assert [r["Id"] for r in rows1] == [11]
    assert off1.get("snapshot_done") is True
    n_before = len(responses.calls)
    rows2, off2 = c.read_table("Parents__Children", off1, opts)
    assert list(rows2) == [] and off2 == off1
    assert len(responses.calls) == n_before


def test_no_progress_guard_ignores_delta_ok_flag():
    """``delta_ok`` was the one persisted verdict missing from the
    no-progress comparison's strip list: an offset differing only by the
    flag would read as forward progress and bypass the guard."""
    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        c._finalize_cursor_read(
            {"cursor": "5", "delta_ok": False},
            {"cursor": "5"},
            [{"Id": 1}],
            "T",
            "M",
        )


@responses.activate
def test_streaming_snapshot_warns_ignored_cap(caplog):
    """Streaming snapshots (flat and contained N+1) ignore
    max_records_per_batch BY DESIGN (no park state in a quiesce-marker
    offset — truncating would be silent loss), but a user capping a
    snapshot stream used to get one unbounded batch with no signal."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]},
        match_querystring=False,
    )
    c = _make()
    with caplog.at_level(logging.WARNING):
        records, offset = c.read_table("Customers", {}, {"max_records_per_batch": "1"})
    assert len(list(records)) == 1  # cap NOT applied — full snapshot
    assert offset.get("snapshot_done") is True
    assert "max_records_per_batch=1 ignored" in caplog.text
    assert "snapshot" in caplog.text


# ---------------------------------------------------------------------------
# Round 48 — period-N pagination cycle guard, capability memo-race gate
# ---------------------------------------------------------------------------


@responses.activate
def test_pagination_period2_nextlink_cycle_stops(caplog):
    """A server/proxy alternating skiptokens (tokA->tokB->tokA...) yields
    pages whose consecutive fingerprints AND links both differ, so the
    period-1 guards (prev_fp / self-referential link) never fire and the
    walk loops forever (OOM in batch mode, non-advancing stream otherwise).
    The bounded URL-repeat guard now stops it. Covers both the default
    `auto` and explicit `nextlink` modes."""
    calls = {"n": 0}

    def _cb(request):
        calls["n"] += 1
        from urllib.parse import parse_qs, unquote, urlparse

        tok = unquote(parse_qs(urlparse(request.url).query).get("$skiptoken", ["start"])[0])
        nxt = {"start": "tokA", "tokA": "tokB", "tokB": "tokA"}[tok]
        row = {"start": 1, "tokA": 2, "tokB": 3}[tok]
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": row}],
                    "@odata.nextLink": f"{SERVICE_URL}T?$top=1&$skiptoken={nxt}",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}T", callback=_cb)
    for mode in ("auto", "nextlink"):
        calls["n"] = 0
        c = _make()
        c._pagination = mode
        with caplog.at_level(logging.WARNING):
            rows = []
            for page_rows, _nxt in c._fetch_pages_with_links(
                f"{SERVICE_URL}T?$top=1&$orderby=Id asc"
            ):
                rows.extend(page_rows)
                assert len(rows) < 50, f"{mode}: cycle guard did not stop the walk"
        assert "continuation URL repeated" in caplog.text
        # The guard fires within the bounded window, not after 50+ fetches.
        assert calls["n"] < 20


@responses.activate
def test_pagination_long_distinct_walk_not_false_flagged():
    """A legitimate long walk (all-distinct continuation URLs) must not
    trip the cycle guard — the window slides, never false-positives."""

    def _cb(request):
        from urllib.parse import parse_qs, unquote, urlparse

        i = int(unquote(parse_qs(urlparse(request.url).query).get("$skiptoken", ["0"])[0]))
        if i >= 30:
            return (200, {}, json.dumps({"value": [{"Id": i}]}))  # no next link — done
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": i}],
                    "@odata.nextLink": f"{SERVICE_URL}T?$top=1&$skiptoken={i + 1}",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}T", callback=_cb)
    c = _make()
    c._pagination = "nextlink"
    rows = []
    for page_rows, _nxt in c._fetch_pages_with_links(f"{SERVICE_URL}T?$top=1&$skiptoken=0"):
        rows.extend(page_rows)
    assert [r["Id"] for r in rows] == list(range(31))  # 0..30, no premature stop


def test_page_cycle_guard_bounded_window():
    """The guard's memory is bounded: a cycle whose period exceeds the
    window escapes (documented trade), but distinct URLs past the window
    are forgotten so a legit walk never grows unboundedly."""
    from databricks.labs.community_connector.sources.odata.odata import _PageCycleGuard

    g = _PageCycleGuard()
    assert g.seen_before("u0") is False
    assert g.seen_before("u0") is True  # immediate repeat caught
    g2 = _PageCycleGuard()
    for i in range(_PageCycleGuard._WINDOW + 10):
        assert g2.seen_before(f"u{i}") is False  # all distinct — never flagged
    assert len(g2._seen) == _PageCycleGuard._WINDOW  # bounded
    assert g2.seen_before("u0") is False  # evicted from the window — forgotten


@responses.activate
def test_nextlink_no_progress_message_omits_switch_advice(caplog):
    """The period-1 no-progress warning shared by both loops must not advise
    'Use pagination=nextlink' when the nextlink loop itself emits it (that
    mode is already in use). The connector-driven modes still carry the tip."""
    responses.get(
        f"{SERVICE_URL}T",
        json={"value": [{"Id": 1}], "@odata.nextLink": f"{SERVICE_URL}T?$skiptoken=same"},
    )
    responses.get(
        f"{SERVICE_URL}T?$skiptoken=same",
        json={"value": [{"Id": 1}], "@odata.nextLink": f"{SERVICE_URL}T?$skiptoken=same2"},
    )
    responses.get(
        f"{SERVICE_URL}T?$skiptoken=same2",
        json={"value": [{"Id": 1}]},
    )
    c = _make()
    c._pagination = "nextlink"
    with caplog.at_level(logging.WARNING):
        for _pr, _nxt in c._fetch_pages_with_links(f"{SERVICE_URL}T?$top=1&$orderby=Id asc"):
            pass
    if "made no progress" in caplog.text:
        assert "Use pagination=nextlink" not in caplog.text
