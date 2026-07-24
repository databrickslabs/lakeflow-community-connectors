"""Tests for the OData LakeflowConnect connector.

Two layers:

* The class-based ``TestODataConnector`` runs the shared
  ``LakeflowConnectTests`` contract suite against the simulator at
  ``source_simulator/specs/odata/`` (Northwind-shaped Customers + Orders).
  This is what CI runs.

* The module-level ``@responses.activate`` tests below exercise narrow
  invariants of the connector that the contract suite doesn't cover:
  literal escaping, ``@odata.nextLink`` resolution edge cases, boundary
  trim shapes, auth wiring, and multi-schema disambiguation. They mock
  HTTP with ``responses`` and run independently of the simulator.
"""

import json
import logging
import os
import re
import time

import pytest
import requests
import responses

from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal
from pyspark.sql.types import DecimalType, IntegerType, StringType, TimestampType
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests


class TestODataConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    """Contract test suite for the OData connector against the simulator.

    The simulator stands up a Northwind-shaped service at
    ``/odata/`` with a fixed ``$metadata`` document and Customers /
    Orders entity sets seeded from the JSON corpus. Connector reads
    flow through the simulator's custom OData handler (entity_set.py)
    which implements just enough ``$top``/``$skip``/``$filter``/
    ``$orderby``/``@odata.nextLink`` semantics to drive the suite.

    ``SupportsPartitionedStreamTests`` is mounted because the connector
    implements ``SupportsPartitionedStream`` (``PartitionMixin``). Its
    partitioned-table contract tests ``skip`` here — the connector only
    partitions *contained* N+1 snapshot paths (``Parent__Child``), and the
    flat Northwind corpus (Customers/Orders) has no partitionable table — so
    ``test_is_partitioned`` runs against the simulator while the contained
    partitioning behaviour is covered by the bespoke ``test_partition_*``
    tests below (which build nested ``$metadata`` fixtures the flat corpus
    can't express).
    """

    connector_class = ODataLakeflowConnect
    simulator_source = "odata"
    sample_records = 50
    # The simulator never validates these — they only need to satisfy
    # ``__init__`` so a session is built. The actual HTTP traffic is
    # intercepted before it leaves the connector.
    replay_config = {
        "service_url": "https://services.odata.org/V4/Northwind/Northwind.svc/",
        "auth_type": "bearer",
        "token": "simulator-fake-token",
    }
    # Orders is the only CDC-shaped table in the corpus. The cursor
    # field has duplicate values (multiple OrderIDs per OrderDate), so
    # this configuration also exercises the boundary trim.
    table_configs = {
        "Orders": {"cursor_field": "OrderDate"},
    }


SERVICE_URL = "https://example.com/odata/"

METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Demo" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="Order">
        <Key><PropertyRef Name="OrderId"/></Key>
        <Property Name="OrderId" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Total" Type="Edm.Decimal"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Customers" EntityType="Demo.Customer"/>
        <EntitySet Name="Orders" EntityType="Demo.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _mock_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=METADATA_XML, status=200)


def _make(options=None):
    base = {"service_url": SERVICE_URL}
    if options:
        base.update(options)
    return ODataLakeflowConnect(base)


def _drop_lb(offset):
    """Strip non-logical bookkeeping from an offset for stable equality asserts:
    the ``auto`` cursor_lookback state (every ``lb_*`` key — the duration
    history and the in-flight cycle anchor, both non-deterministic
    wall-clock) and the persisted capability verdicts (``cursor_probe_ok`` /
    ``batch_ok`` / ``batch_size_ok`` / ``or_filter_ok``, one-time-set markers
    threaded across microbatches). Tests assert the cursor/resume state, not this
    bookkeeping — mirrors the no-progress comparison in ``_finalize_cursor_read``."""
    _bookkeeping = {
        "cursor_probe_ok",
        "batch_ok",
        "batch_size_ok",
        "or_filter_ok",
        "expand_ok",
        "delta_ok",
    }
    return {
        k: v for k, v in (offset or {}).items() if k not in _bookkeeping and not k.startswith("lb_")
    }


# ---------------------------------------------------------------------------
# Static helpers
# ---------------------------------------------------------------------------


def test_odata_literal_quotes_strings_and_escapes():
    assert _odata_literal("O'Brien") == "'O''Brien'"
    assert _odata_literal(5) == "5"
    assert _odata_literal(True) == "true"


def test_odata_literal_passes_iso_timestamps_bare():
    assert _odata_literal("2024-05-01T00:00:00Z") == "2024-05-01T00:00:00Z"
    # Odd-digit fractions must stay bare too — on Python 3.10 (DBR 13.3 LTS)
    # a bare fromisoformat rejects '.5', which would QUOTE the watermark in
    # $filter and 400 every incremental batch. parse_iso8601 normalizes the
    # digit count so the sniff verdict is version-uniform.
    assert _odata_literal("2024-05-01T00:00:00.5Z") == "2024-05-01T00:00:00.5Z"
    assert _odata_literal("2024-05-01T00:00:00.1234567Z") == "2024-05-01T00:00:00.1234567Z"


def test_parse_iso8601_normalizes_fraction_digit_count():
    """Version-uniform parsing: Python 3.10 (the declared floor, DBR 13.3
    LTS) accepts only 3- or 6-digit fractional seconds, while servers render
    value-dependent digit counts (Olingo/SAP trim trailing zeros) and
    nanosecond servers emit 7+. The helper pads/truncates to 6 so parsing —
    and everything built on it: the ISO sniff, the chronological
    comparisons, the lookback floor — behaves identically everywhere."""
    from datetime import datetime, timezone

    from databricks.labs.community_connector.sources.odata._helpers import parse_iso8601

    base = datetime(2024, 1, 1, 23, 0, 0, 500000, tzinfo=timezone.utc)
    assert parse_iso8601("2024-01-01T23:00:00.5Z") == base  # 1 digit → padded
    assert parse_iso8601("2024-01-01T23:00:00.50000Z") == base  # 5 digits → padded
    assert parse_iso8601("2024-01-01T23:00:00.5000000Z") == base  # 7 digits → truncated
    # Sub-microsecond digits truncate (ordering-tie territory, duplicate-safe).
    assert parse_iso8601("2024-01-01T23:00:00.1234567Z") == parse_iso8601(
        "2024-01-01T23:00:00.123456Z"
    )
    # Non-fractional and offset forms pass through untouched.
    assert parse_iso8601("2024-01-01T23:00:00+10:00").utcoffset().total_seconds() == 36000
    with pytest.raises(ValueError):
        parse_iso8601("not-a-timestamp")


def test_cursor_comparisons_are_chronological_not_lexical():
    """Client-side cursor ordering must match the SERVER's chronological
    ordering. OData's JSON format makes fractional seconds optional per
    value (Olingo/SAP trim trailing zeros), so one column renders both
    ``…00Z`` and ``…00.5Z`` — and Python string order puts the LATER
    ``.5Z`` first (``.`` < ``Z``), which silently drops re-filtered rows
    and regresses watermark maxes."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_le,
        cursor_max,
        cursor_newer,
        max_or,
    )

    # The bug cases: fractional vs whole second, differing precision.
    assert cursor_newer("2024-01-01T23:00:00.5Z", "2024-01-01T23:00:00Z")
    assert not cursor_le("2024-01-01T23:00:00.5Z", "2024-01-01T23:00:00Z")
    assert cursor_newer("2024-01-01T23:00:00.51Z", "2024-01-01T23:00:00.5Z")
    assert cursor_max(["2024-01-01T23:00:00.5Z", "2024-01-01T23:00:00Z"]) == (
        "2024-01-01T23:00:00.5Z"
    )
    assert max_or("2024-01-01T23:00:00Z", "2024-01-01T23:00:00.5Z") == ("2024-01-01T23:00:00.5Z")
    # Sub-microsecond precision (SQL Server datetime2(7) emits 7-digit
    # fractions): Python datetimes truncate to µs, so the PARSED keys tie —
    # the raw-text tie-break must still order chronologically, or the
    # <= since re-filter drops a strictly-newer row the server correctly
    # returned (the round-13 loss mechanism one scale down).
    assert cursor_newer("2024-01-01T23:00:00.1234568Z", "2024-01-01T23:00:00.1234567Z")
    assert not cursor_le("2024-01-01T23:00:00.1234568Z", "2024-01-01T23:00:00.1234567Z")
    assert (
        cursor_max(["2024-01-01T23:00:00.1234567Z", "2024-01-01T23:00:00.1234568Z"])
        == "2024-01-01T23:00:00.1234568Z"
    )  # true max regardless of order
    # Differing digit counts below the µs boundary compare zero-padded
    # (.12345675 > .1234567 == .12345670) — raw-text comparison would
    # invert here because the shorter fraction's 'Z' sorts above digits.
    assert cursor_newer("2024-01-01T23:00:00.12345675Z", "2024-01-01T23:00:00.1234567Z")
    # Equal instants rendered two ways: the consistent raw tie-break errs
    # only in the duplicate-safe direction at the re-filter — a same-instant
    # re-read is either dropped (correct) or kept (MERGE-deduped duplicate),
    # never a lost newer row.
    assert cursor_le("2024-01-01T23:00:00+00:00", "2024-01-01T23:00:00Z")  # dropped: correct
    assert not cursor_le("2024-01-01T23:00:00Z", "2024-01-01T23:00:00+00:00")  # kept: dup-safe
    assert cursor_max(["2024-01-01T23:00:00Z", "2024-01-01T23:00:00+00:00"]) == (
        "2024-01-01T23:00:00Z"
    )
    # Identical texts still tie exactly.
    assert not cursor_newer("2024-01-01T23:00:00Z", "2024-01-01T23:00:00Z")
    assert cursor_le("2024-01-01T23:00:00Z", "2024-01-01T23:00:00Z")
    # Offsets order chronologically, not textually.
    assert cursor_newer("2024-01-01T23:00:00Z", "2024-01-02T08:59:00+10:00")
    # Non-ISO values keep their natural ordering; ints untouched.
    assert cursor_newer("b", "a") and not cursor_newer("A", "a")
    assert cursor_newer(10, 9) and cursor_le(9, 10)
    # A shape-mixed pair degrades to raw comparison instead of raising.
    assert cursor_newer("zzz", "2024-01-01T00:00:00Z")


def test_odata_literal_percent_encodes_url_reserved_characters():
    """Generated literals ride into URL strings that ``requests`` sends
    without encoding reserved characters: a raw ``+`` is decoded as a SPACE
    by form-decoding servers (a non-UTC ISO watermark → malformed timestamp
    → 400 every batch; ``+`` in a quoted seek boundary → silent wrong
    comparison), ``&`` splits the query, ``#`` truncates the request, and
    ``?`` starts the query when the literal sits in a key-predicate path
    segment. ``odata_literal`` must pre-encode them (requests preserves
    existing escapes, so this decodes correctly server-side)."""
    from datetime import datetime, timedelta, timezone

    # The bug case: a non-UTC ISO watermark string keeps its offset ``+``.
    assert _odata_literal("2025-06-01T12:00:00+10:00") == "2025-06-01T12:00:00%2B10:00"
    # Same via a tz-aware datetime; UTC still normalizes to a bare Z.
    tz10 = timezone(timedelta(hours=10))
    assert _odata_literal(datetime(2025, 6, 1, 12, tzinfo=tz10)) == "2025-06-01T12:00:00%2B10:00"
    assert _odata_literal(datetime(2025, 6, 1, 12, tzinfo=timezone.utc)) == "2025-06-01T12:00:00Z"
    # Reserved characters inside quoted string values.
    assert _odata_literal("A&B") == "'A%26B'"
    assert _odata_literal("A#B") == "'A%23B'"
    assert _odata_literal("AB+1") == "'AB%2B1'"
    assert _odata_literal("A?B") == "'A%3FB'"
    assert _odata_literal("100%") == "'100%25'"
    # Quote doubling still composes with the encoding.
    assert _odata_literal("O'Brien & sons") == "'O''Brien %26 sons'"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


@responses.activate
def test_list_tables_returns_all_entity_sets():
    _mock_metadata()
    c = _make()
    assert sorted(c.list_tables()) == ["Customers", "Orders"]


@responses.activate
def test_get_table_schema_maps_edm_types():
    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {})
    names = [f.name for f in schema.fields]
    types = [type(f.dataType).__name__ for f in schema.fields]
    assert names == ["Id", "Name", "ModifiedAt"]
    assert types == ["IntegerType", "StringType", "TimestampType"]
    assert schema.fields[0].nullable is False


@responses.activate
def test_get_table_schema_respects_select():
    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {"select": "Id,ModifiedAt"})
    assert [f.name for f in schema.fields] == ["Id", "ModifiedAt"]


@responses.activate
def test_read_table_metadata_snapshot_when_no_cursor():
    _mock_metadata()
    c = _make()
    meta = c.read_table_metadata("Customers", {})
    assert meta == {
        "primary_keys": ["Id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    }


@responses.activate
def test_read_table_metadata_cdc_when_cursor_set():
    _mock_metadata()
    c = _make()
    meta = c.read_table_metadata("Customers", {"cursor_field": "ModifiedAt"})
    assert meta["ingestion_type"] == "cdc"
    assert meta["cursor_field"] == "ModifiedAt"


@responses.activate
def test_unknown_entity_set_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not found"):
        c.get_table_schema("Nope", {})


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


# ---------------------------------------------------------------------------
# Incremental read
# ---------------------------------------------------------------------------


@responses.activate
def test_incremental_first_call_has_no_cursor_filter():
    """No wall-clock ceiling means the first call (`since=None`) sends
    no `$filter` clause derived from the cursor. The server returns rows
    from the natural start of the table; `max_records_per_batch` is the
    per-call cap. This is what makes the connector usable for both
    continuous polling and non-timestamp cursor types."""
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        # First (unfiltered) request returns the row; the default `auto`
        # drain issues one confirming keyset seek (carrying `gt`) — the
        # seek-honouring server returns empty, ending the collection.
        if " gt " in request.url.replace("%20", " "):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "10"},
    )
    rows = list(records)
    # ``Name`` is padded to None: the emit boundary fills declared columns the
    # server omitted so a null-omitting response parses cleanly.
    assert rows == [{"Id": 1, "Name": None, "ModifiedAt": "2024-03-01T00:00:00Z"}]
    assert _drop_lb(offset) == {"cursor": "2024-03-01T00:00:00Z"}
    # Neither `le` nor `gt` should appear on the FIRST call — no cursor
    # filter at all when resuming from an empty offset.
    normalised = captured_urls[0].replace("%20", " ")
    assert " le " not in normalised
    assert " gt " not in normalised


@responses.activate
def test_incremental_non_utc_offset_watermark_is_percent_encoded():
    """A source emitting local-offset timestamps (SAP-style) puts a ``+`` in
    the watermark. The generated ``$filter`` must carry it as ``%2B`` — a raw
    ``+`` is decoded as a SPACE by form-decoding servers, turning the filter
    into a malformed timestamp and 400-ing every incremental batch."""
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        if " gt " in request.url.replace("%20", " "):
            return (
                200,
                {},
                '{"value": [{"Id": 2, "ModifiedAt": "2024-03-02T00:00:00+10:00"}]}'
                if len(captured_urls) == 1
                else '{"value": []}',
            )
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-03-01T00:00:00+10:00"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [2]
    assert _drop_lb(offset) == {"cursor": "2024-03-02T00:00:00+10:00"}
    # The offset's ``+`` reached the wire percent-encoded, never raw.
    first = captured_urls[0]
    assert "%2B10:00" in first
    assert "+10:00" not in first


@responses.activate
def test_incremental_fractional_second_rendering_not_dropped():
    """A server that renders fractional seconds only when non-zero (spec-
    allowed; Olingo/SAP trim trailing zeros) returns ``…00.5Z`` for a row
    newer than the ``…00Z`` watermark. Lexically ``.`` < ``Z``, so a raw
    ``<=`` re-filter dropped the row the server correctly returned — with
    nothing else new the batch came back empty and the stream quiesced with
    the row permanently invisible. The chronological comparison keeps it,
    and the watermark max must not regress behind it either."""
    _mock_metadata()

    def _callback(request):
        if " gt " in request.url.replace("%20", " "):
            # Server-side chronological gt correctly returns the .5Z row
            # for since=…00Z; the confirming drain seek returns empty.
            if "23:00:00Z" in request.url.replace("%3A", ":"):
                return (
                    200,
                    {},
                    '{"value": [{"Id": 2, "ModifiedAt": "2024-01-01T23:00:00.5Z"}]}',
                )
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-01-01T23:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [2]  # not silently dropped
    # Watermark advanced to the fractional value (no lexical regression).
    assert _drop_lb(offset) == {"cursor": "2024-01-01T23:00:00.5Z"}


@responses.activate
def test_incremental_supports_integer_cursor():
    """Cursor type is opaque to the filter logic — monotonic IDs work
    just like timestamps. Verifies the resume URL carries an `OrderID gt
    N` clause with an unquoted integer literal."""
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Orders", callback=_callback)

    c = _make()
    start = {"cursor": 10248}
    records, offset = c.read_table("Orders", start, {"cursor_field": "OrderId"})
    assert list(records) == []
    assert offset == start
    normalised = captured_urls[0].replace("%20", " ")
    assert "OrderId gt 10248" in normalised
    # The literal is unquoted (matches Edm.Int32 syntax, not Edm.String).
    assert "'10248'" not in normalised


@responses.activate
def test_incremental_resume_uses_gt_filter_and_terminates():
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        # Return no new rows so termination kicks in.
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    start = {"cursor": "2024-03-01T00:00:00Z"}
    records, offset = c.read_table(
        "Customers",
        start,
        {"cursor_field": "ModifiedAt"},
    )
    assert list(records) == []
    # Caller passes start_offset back unchanged on the "no data" path.
    assert offset == start
    # We tried the API once (cursor < init_ts), URL must include the `gt` clause.
    assert any("gt" in u for u in captured_urls)


@responses.activate
def test_incremental_continuous_polling_picks_up_new_rows():
    """A connector instance reused across multiple `read_table` calls
    sees fresh source state on each call. Mirrors what a continuous
    SDP pipeline does: one connector, many micro-batches, source
    growing under us. Each subsequent call should advance through the
    new rows.

    The mock is a seek-honouring server (the only faithful model now
    that the default `auto` flat cursor read drains): it filters the
    corpus by the connector's `ModifiedAt gt <v>` resume / keyset-seek
    lower bound, so each batch returns exactly the rows above the
    parked watermark."""
    _mock_metadata()
    corpus = [
        {"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"},
        {"Id": 2, "ModifiedAt": "2024-03-02T00:00:00Z"},
    ]

    def _callback(request):
        url = request.url.replace("%20", " ")
        rows = corpus
        # Honour every `ModifiedAt gt <v>` lower bound on the URL (the
        # cross-batch resume filter and the in-batch keyset-seek drain
        # both carry one); the tightest bound wins.
        bounds = re.findall(r"ModifiedAt gt ([0-9T:\-Z]+)", url)
        if bounds:
            lo = max(bounds)
            rows = [r for r in rows if r["ModifiedAt"] > lo]
        rows = sorted(rows, key=lambda r: (r["ModifiedAt"], r["Id"]))
        return (200, {}, json.dumps({"value": rows}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    # Batch 1: no offset, both rows drain. Trim of the trailing distinct
    # cursor cohort holds Id=2 back; emits [Id=1]; offset = 2024-03-01.
    rows1, offset1 = c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows1] == [1]
    assert _drop_lb(offset1) == {"cursor": "2024-03-01T00:00:00Z"}

    # Batch 2: feeding offset1 back, the held-back Id=2 is re-read above
    # the watermark and emitted; offset advances to 2024-03-02.
    rows2, offset2 = c.read_table("Customers", offset1, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows2] == [2]
    assert _drop_lb(offset2) == {"cursor": "2024-03-02T00:00:00Z"}

    # A new row arrives while the stream is idle.
    corpus.append({"Id": 3, "ModifiedAt": "2024-03-05T00:00:00Z"})

    # Batch 3: the same connector instance picks up the fresh row.
    rows3, offset3 = c.read_table("Customers", offset2, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows3] == [3]
    assert _drop_lb(offset3) == {"cursor": "2024-03-05T00:00:00Z"}

    # Batch 4: caught up — no rows above the watermark, stable offset
    # signals "no more data" to Spark.
    rows4, offset4 = c.read_table("Customers", offset3, {"cursor_field": "ModifiedAt"})
    assert list(rows4) == []
    assert offset4 == offset3

    # Batch 3: new row appeared in the source. The continuous-polling
    # connector picks it up using only the `gt` filter — no frozen
    # snapshot ceiling getting in the way.
    rows3, offset3 = c.read_table("Customers", offset2, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows3] == [3]
    assert _drop_lb(offset3) == {"cursor": "2024-03-05T00:00:00Z"}


@responses.activate
def test_incremental_trims_trailing_same_cursor_cohort_when_truncated():
    """Cap-hit boundary: trim the trailing same-cursor cohort so the next
    call's `cursor gt <last>` doesn't drop the cohort's unread siblings.
    Re-fetched cohort members are deduped at the destination by MERGE on
    the primary key."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-03T00:00:00Z"},
                {"Id": 4, "ModifiedAt": "2024-05-03T00:00:00Z"},  # trimmed
                {"Id": 5, "ModifiedAt": "2024-05-03T00:00:00Z"},  # trimmed (cap)
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "5"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert _drop_lb(offset) == {"cursor": "2024-05-02T00:00:00Z"}


@responses.activate
def test_incremental_trims_boundary_cohort_on_natural_exhaustion_too():
    """Trim also runs on naturally-exhausted batches. With a
    low-cardinality cursor, same-cursor siblings could arrive between
    this batch and a future call (stop/restart, concurrent insert) —
    trimming forces the next call's `cursor gt <previous_distinct>` to
    re-fetch the whole cohort plus any new arrivals."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},  # trimmed
                {"Id": 3, "ModifiedAt": "2024-05-02T00:00:00Z"},  # trimmed
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert _drop_lb(offset) == {"cursor": "2024-05-01T00:00:00Z"}


@responses.activate
def test_incremental_all_same_cursor_truncated_raises():
    """If the whole truncated batch shares one cursor, the cap is smaller
    than the same-cursor cohort and we can't trim without losing data —
    surface that loudly."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    with pytest.raises(RuntimeError, match="max_records_per_batch"):
        records, _ = c.read_table(
            "Customers",
            {},
            {"cursor_field": "ModifiedAt", "max_records_per_batch": "3"},
        )
        list(records)


@responses.activate
def test_incremental_all_same_cursor_natural_exhaustion_emits_as_is():
    """When the whole batch shares one cursor AND it's the natural end
    of the result set, there's nowhere to retreat to — emit the cohort
    rather than losing it. Accept the residual race that same-cursor
    rows arriving later won't be picked up; resolved by giving the
    cursor field higher cardinality."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2, 3]
    assert _drop_lb(offset) == {"cursor": "2024-05-01T00:00:00Z"}


@responses.activate
def test_incremental_first_batch_null_cursor_rows_raises():
    """Regression: flat incremental path used to build
    ``end_offset = {"cursor": records[-1].get(cursor_field)}``,
    which becomes ``{"cursor": None}`` when the trailing record carries
    a null cursor (and the same-cohort fall-through keeps the rows).
    Combined with the old truthy guard ``if start_offset and
    start_offset == end_offset``, the first streaming batch
    (``start_offset = {}``) bypassed the guard and committed null-cursor
    rows with the offset advancing to ``{"cursor": None}`` — subsequent
    triggers re-emit the same rows. The fix normalizes the
    no-cursor-data case to ``{}`` and routes through
    ``_finalize_cursor_read``, which raises so the operator sees the
    cause."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": None},
                {"Id": 2, "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        records, _ = c.read_table(
            "Customers",
            {},
            {
                "cursor_field": "ModifiedAt",
                "max_records_per_batch": "100",
                "cursor_nulls": "error",
            },
        )
        list(records)


@responses.activate
def test_incremental_batch_mode_null_cursor_rows_emit_without_raise():
    """Batch reader (`LakeflowBatchReader`) passes ``start_offset=None``
    and discards the returned offset. The no-progress guard is a
    streaming concern — without an offset that the framework re-issues,
    null-cursor data can't loop. ``_finalize_cursor_read`` treats
    ``start_offset is None`` as the batch-reader signal and emits rows
    as-is. The companion streaming test
    (``test_incremental_first_batch_null_cursor_rows_raises``) shows
    the same data raises when ``start_offset={}`` — this test locks
    the batch/streaming split so a future refactor that re-normalizes
    None to {} (re-introducing the bug class) breaks loudly."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": None},
                {"Id": 2, "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, _ = c.read_table(
        "Customers",
        None,
        {
            "cursor_field": "ModifiedAt",
            "max_records_per_batch": "100",
            "cursor_nulls": "error",
        },
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]


@responses.activate
def test_batch_mode_flat_cursor_drains_fully_despite_explicit_cap():
    """Batch reader (``start_offset=None``) with an explicit cap reads the
    whole table. The offset is discarded, so the cap is force-disabled —
    honouring it could only truncate-and-lose — and rows stream lazily.
    Three distinct-cursor rows that the *streaming* path with ``cap=1``
    would truncate then trim to empty (and raise) all come through here,
    with the terminal ``{}`` offset."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-01-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-01-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", None, {"cursor_field": "ModifiedAt", "max_records_per_batch": "1"}
    )
    assert [r["Id"] for r in records] == [1, 2, 3]
    assert _drop_lb(offset) == {}


@responses.activate
def test_batch_mode_contained_cursor_streams_lazily_per_parent():
    """The batch-mode contained cursor read yields lazily: consuming only
    the first parent's leaf row must not have fetched the second parent's
    leaf collection. This is the property that bounds peak memory to one
    page instead of materialising the whole result set (which the
    streaming walk's ``emitted`` list does). Draining the rest then
    reaches parent 2."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    fetched: list[str] = []

    def _leaf(request):
        fetched.append(request.url)
        n = "1" if "Parents(1)" in request.url else "2"
        return (
            200,
            {},
            '{"value": [{"Id": 1' + n + ', "Label": "x", '
            '"ModifiedAt": "2024-01-0' + n + 'T00:00:00Z"}]}',
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=_leaf)
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(2)/Children", callback=_leaf)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "1"},
    )
    it = iter(records)
    first = next(it)
    assert first["Id"] == 11
    # Lazy: only parent 1's leaf fetched so far; parent 2 untouched.
    assert any("Parents(1)/Children" in u for u in fetched)
    assert not any("Parents(2)/Children" in u for u in fetched)
    # Draining the rest reaches parent 2 — full coverage, uncapped.
    rest = [r["Id"] for r in it]
    assert rest == [12]
    assert any("Parents(2)/Children" in u for u in fetched)
    assert _drop_lb(offset) == {}


@responses.activate
def test_batch_mode_expand_streams_lazily_and_uncapped():
    """``expand_contained=true`` under the batch reader streams flattened
    leaf rows one $expand response at a time and ignores an explicit cap
    (offset discarded → a cap could only truncate-and-lose). All leaf
    rows across the inline cross-product come through with a ``{}``
    offset."""
    _mock_nested_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Children": [
                        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
                        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T00:00:00Z"},
                        {"Id": 13, "Label": "c", "ModifiedAt": "2024-01-03T00:00:00Z"},
                    ],
                }
            ]
        },
        match_querystring=False,
    )
    # Short top-level page → the drainer probes once more to confirm exhaustion.
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    # Short, link-less inline Children page → the inner drainer probes past the
    # last inline child to confirm exhaustion (mirrors the top-level auto drain).
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        None,
        {"expand_contained": "true", "max_records_per_batch": "1"},
    )
    assert [r["Id"] for r in records] == [11, 12, 13]
    assert _drop_lb(offset) == {}


@responses.activate
def test_incremental_coalesce_default_emits_null_rows_and_advances():
    """Default ``cursor_nulls=coalesce``: a null-only streaming batch is
    emitted (column left null) and the watermark advances via a
    synthetic floor, so no no-progress RuntimeError fires."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": None}, {"Id": 2, "ModifiedAt": None}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    # The real null is preserved in the emitted rows (synthetic is internal).
    assert all(r["ModifiedAt"] is None for r in rows)
    # Watermark advanced to the default synthetic floor (year 2000), not {}.
    assert offset["cursor"].startswith("2000-01-01T00:00:00.")


@responses.activate
def test_incremental_coalesce_floor_year_configurable():
    """``cursor_nulls=coalesce:<YYYY>`` overrides the temporal synthetic
    floor year (default 2000)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": None}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "coalesce:1990"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert rows[0]["ModifiedAt"] is None
    assert offset["cursor"].startswith("1990-01-01T00:00:00.")


@responses.activate
def test_cursor_nulls_floor_year_with_non_coalesce_raises():
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    with pytest.raises(ValueError, match="floor year is only valid with 'coalesce'"):
        records, _ = c.read_table(
            "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "error:1990"}
        )
        list(records)


@responses.activate
def test_incremental_ignore_skips_null_rows():
    """``cursor_nulls=ignore`` drops null-cursor rows entirely; only the
    real-cursor row is emitted and drives the watermark."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": None},
                {"Id": 2, "ModifiedAt": "2024-01-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "ignore"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [2]
    assert _drop_lb(offset) == {"cursor": "2024-01-01T00:00:00Z"}


@responses.activate
def test_cursor_nulls_invalid_value_raises():
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    with pytest.raises(ValueError, match="cursor_nulls"):
        records, _ = c.read_table(
            "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "bogus"}
        )
        list(records)


@responses.activate
def test_incremental_orderby_appends_primary_key_tiebreaker():
    """`$orderby` must be a total order, not just by cursor.

    OData servers that paginate via `@odata.nextLink` typically derive
    the skiptoken from the order-by columns. When the cursor (here
    ModifiedAt) has duplicates and `$orderby` is cursor-only, the
    skiptoken's strict `>` on the cursor drops the unread tail of a
    same-cursor cohort that straddles a page boundary. Appending the
    primary key forces a unique total order so the skiptoken is stable.
    """
    _mock_metadata()
    captured = {}

    def _callback(request):
        captured["url"] = request.url
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})
    # `Id` is Customers' Key in METADATA_XML.
    url = captured["url"]
    assert "ModifiedAt" in url and "asc" in url
    assert "Id" in url
    # Both terms must appear consecutively in the orderby clause. The
    # comma between them may be raw `,` or `%2C`; the space may be raw
    # ` ` or `%20`. Use a normalised check.
    normalised = url.replace("%20", " ").replace("%2C", ",")
    assert "$orderby=ModifiedAt asc,Id asc" in normalised


@responses.activate
def test_incremental_client_strict_gt_drops_boundary_row():
    """A defensive client-side strict-`>` filter guards against any
    server returning a record equal to `since`. The previous batch's
    boundary record never appears twice — the client filter drops it
    before the trim runs."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                # Server returned a record at the boundary cursor (would
                # happen if a server treated `gt` as `ge`).
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-05-01T00:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    # Id 1 dropped by the strict-`>` client filter. Id 3 (the trailing
    # cohort at 2024-05-03) is then trimmed so the next call's
    # `cursor gt 2024-05-02` re-fetches it.
    assert [r["Id"] for r in rows] == [2]
    assert _drop_lb(offset) == {"cursor": "2024-05-02T00:00:00Z"}


@responses.activate
def test_incremental_max_records_caps_batch_with_boundary_trim():
    """When the cap is hit, the trailing same-cursor cohort (here just one
    distinct row at the boundary) is trimmed. The next call re-fetches it
    via `cursor gt <prev_distinct>` and the destination MERGEs on PK."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-04-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-04-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-04-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert _drop_lb(offset) == {"cursor": "2024-04-01T00:00:00Z"}


# ---------------------------------------------------------------------------
# Auth wiring
# ---------------------------------------------------------------------------


@responses.activate
def test_bearer_auth_attaches_header():
    _mock_metadata()
    c = _make({"auth_type": "bearer", "token": "abc"})
    # Trigger session creation via list_tables.
    c.list_tables()
    assert c._get_session().headers["Authorization"] == "Bearer abc"


@responses.activate
def test_api_key_custom_header():
    _mock_metadata()
    c = _make(
        {
            "auth_type": "api_key",
            "api_key": "k",
            "api_key_header": "X-My-Key",
        }
    )
    c.list_tables()
    assert c._get_session().headers["X-My-Key"] == "k"


def test_missing_service_url_raises():
    with pytest.raises(ValueError, match="service_url"):
        ODataLakeflowConnect({})


# ---------------------------------------------------------------------------
# 401 / 403 UX when there's no OAuth refresh path
# ---------------------------------------------------------------------------

# The connector never refreshes tokens itself — OAuth is owned by the
# Unity Catalog COMMUNITY connection, which runs the flow, refreshes
# server-side, and injects a fresh ``access_token`` at query time. A
# 401/403 is therefore always terminal for the read, and the raw
# HTTPError gives the operator nothing actionable — the connector
# raises PermissionError with auth-mode-specific remediation instead.


@responses.activate
def test_bearer_401_without_refresh_raises_actionable_permission_error():
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=401,
        json={"error": {"code": "InvalidAuthenticationToken"}},
    )
    c = _make({"auth_type": "bearer", "token": "stale"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    # Diagnostics that triage the failure for a bearer-auth operator
    # without making them dig into the request/response cycle.
    assert "auth_type=bearer" in msg
    assert "expired" in msg
    assert "COMMUNITY OAuth connection" in msg  # suggested upgrade path
    assert "community_oauth_flow" in msg
    assert "InvalidAuthenticationToken" in msg  # server body echoed


@responses.activate
def test_basic_401_without_refresh_raises_actionable_permission_error():
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="denied")
    c = _make({"auth_type": "basic", "username": "u", "password": "p"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "auth_type=basic" in msg
    assert "username" in msg
    assert "password" in msg


@responses.activate
def test_api_key_401_without_refresh_raises_actionable_permission_error():
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="denied")
    c = _make({"auth_type": "api_key", "api_key": "k"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "auth_type=api_key" in msg
    assert "api_key" in msg
    assert "api_key_header" in msg


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


def test_service_url_with_embedded_credentials_rejected():
    """The service URL is echoed verbatim in logs and error messages on
    every request — embedded userinfo credentials would leak everywhere.
    Reject up front with the remediation (auth_type=basic options)."""
    for bad in (
        "https://user:hunter2@example.com/odata/",
        "https://tokenuser@example.com/odata/",
    ):
        with pytest.raises(ValueError, match="must not embed credentials"):
            _make({"service_url": bad})


def test_sequence_counter_is_picklable_and_monotonic():
    """The ``_lc_sequence`` tie-breaker must survive pickling: in the merged
    bundle cloudpickle serializes the connector class BY VALUE and walks the
    closure cell holding this counter — a bare ``itertools.count`` is a
    TypeError on Python >= 3.14 (see the bundle round-trip test). A clone
    restarts at zero (benign: the ns timestamp dominates the sequence)."""
    import pickle

    from databricks.labs.community_connector.sources.odata.odata import (
        _SEQUENCE_COUNTER,
        _next_sequence,
    )

    first, second = _next_sequence(), _next_sequence()
    assert first < second  # still strictly increasing
    clone = pickle.loads(pickle.dumps(_SEQUENCE_COUNTER))
    assert isinstance(next(clone), int)


@responses.activate
def test_403_on_bearer_raises_permission_error():
    """403 means authenticated-but-not-authorized — an *authorization*
    failure, so the message must point at permissions/scope, not at the
    per-mode token-expiry hints, and must NOT claim "no automatic
    token-refresh path is configured" (false on a UC OAuth connection
    whose principal is simply forbidden)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=403,
        json={"error": {"code": "Forbidden"}},
    )
    c = _make({"auth_type": "bearer", "token": "valid-but-no-scope"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "403" in msg
    assert "not authorized" in msg
    assert "no automatic token-refresh path" not in msg


@responses.activate
def test_uc_injected_access_token_authenticates():
    """A UC COMMUNITY OAuth connection injects ``access_token`` into the
    options at query time (no ``auth_type``). The connector must use it as
    an opaque bearer token — no minting, no refresh, no client creds."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"CustomerID": "A", "CompanyName": "Acme"}]},
    )
    c = _make({"access_token": "uc-injected-token"})
    rows = list(c.read_table("Customers", None, {})[0])
    assert [r["CustomerID"] for r in rows] == ["A"]
    sent = {
        call.request.headers.get("Authorization")
        for call in responses.calls
        if "Customers" in call.request.url
    }
    assert sent == {"Bearer uc-injected-token"}  # every request carried it


def test_auth_type_oauth2_retired_raises_actionable():
    """``auth_type=oauth2`` (the old connector-side minting/refresh mode)
    is retired: the curated error names the UC COMMUNITY OAuth connection,
    the flow option, and the parameters the user must move to."""
    c = _make({"auth_type": "oauth2", "oauth2_client_id": "x", "oauth2_client_secret": "y"})
    with pytest.raises(ValueError) as ei:
        c._get_session()
    msg = str(ei.value)
    assert "retired" in msg
    assert "community_oauth_flow" in msg
    assert "token_endpoint" in msg
    assert "access_token" in msg


@responses.activate
def test_uc_injected_token_401_names_the_connection_layer():
    """A 401 under an injected token must say the token came from the UC
    OAuth connection and that the connection layer refreshes it at query
    start (mid-read expiry → retry gets a fresh token) — not suggest any
    connector-side refresh configuration."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="expired")
    c = _make({"access_token": "was-fresh-at-query-start"})
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "Unity Catalog OAuth connection" in msg
    assert "refreshes the token at query start" in msg
    assert "no automatic token-refresh path" not in msg


def test_unknown_auth_type_names_static_modes_and_uc_oauth():
    """The unknown-auth_type error lists the static modes and points at
    the UC-managed OAuth alternative (oauth2 is gone from the list)."""
    c = _make({"auth_type": "kerberos"})
    with pytest.raises(ValueError) as ei:
        c._get_session()
    msg = str(ei.value)
    assert "bearer" in msg and "basic" in msg and "api_key" in msg
    assert "oauth2" not in msg
    assert "COMMUNITY OAuth connection" in msg


MULTI_SCHEMA_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Sales" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Account" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Order">
        <Key><PropertyRef Name="OrderId"/></Key>
        <Property Name="OrderId" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="SalesContainer">
        <EntitySet Name="Customers" EntityType="Sales.Customer"/>
        <EntitySet Name="Orders" EntityType="Sales.Order"/>
      </EntityContainer>
    </Schema>
    <Schema Namespace="HR" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="EmployeeId"/></Key>
        <Property Name="EmployeeId" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Department" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="HRContainer">
        <EntitySet Name="Customers" EntityType="HR.Customer"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _mock_multi_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=MULTI_SCHEMA_METADATA, status=200)


@responses.activate
def test_list_namespaces_returns_all_schemas():
    _mock_multi_metadata()
    c = _make()
    assert sorted(c.list_namespaces()) == [["HR"], ["Sales"]]


@responses.activate
def test_list_namespaces_with_prefix_is_empty():
    """OData has a single flat level — anything under a namespace returns []."""
    _mock_multi_metadata()
    c = _make()
    assert c.list_namespaces(["Sales"]) == []


@responses.activate
def test_list_tables_in_namespace_filters_by_schema():
    _mock_multi_metadata()
    c = _make()
    assert sorted(c.list_tables_in_namespace(["Sales"])) == ["Customers", "Orders"]
    assert c.list_tables_in_namespace(["HR"]) == ["Customers"]


@responses.activate
def test_list_tables_in_root_namespace_is_empty():
    _mock_multi_metadata()
    c = _make()
    # OData entity sets always live inside a Schema — never at the root.
    assert c.list_tables_in_namespace([]) == []


@responses.activate
def test_list_tables_dedupes_across_namespaces():
    _mock_multi_metadata()
    c = _make()
    # 'Customers' appears in both Sales and HR — should appear once.
    assert sorted(c.list_tables()) == ["Customers", "Orders"]


@responses.activate
def test_ambiguous_table_name_raises_without_namespace():
    _mock_multi_metadata()
    c = _make()
    with pytest.raises(ValueError, match="multiple namespaces"):
        c.get_table_schema("Customers", {})


@responses.activate
def test_namespace_disambiguates_schema_lookup():
    _mock_multi_metadata()
    c = _make()
    sales_schema = c.get_table_schema("Customers", {"namespace": "Sales"})
    hr_schema = c.get_table_schema("Customers", {"namespace": "HR"})
    assert [f.name for f in sales_schema.fields] == ["Id", "Account"]
    assert [f.name for f in hr_schema.fields] == ["EmployeeId", "Department"]


@responses.activate
def test_unknown_namespace_lists_available_entities():
    _mock_multi_metadata()
    c = _make()
    with pytest.raises(ValueError, match=r"namespace 'Nope'"):
        c.get_table_schema("Customers", {"namespace": "Nope"})


@responses.activate
def test_read_table_metadata_picks_correct_primary_key_per_namespace():
    _mock_multi_metadata()
    c = _make()
    sales = c.read_table_metadata("Customers", {"namespace": "Sales"})
    hr = c.read_table_metadata("Customers", {"namespace": "HR"})
    assert sales["primary_keys"] == ["Id"]
    assert hr["primary_keys"] == ["EmployeeId"]


@responses.activate
def test_unique_name_does_not_require_namespace():
    """When a name appears in only one schema, namespace is optional."""
    _mock_multi_metadata()
    c = _make()
    schema = c.get_table_schema("Orders", {})  # only in Sales
    assert [f.name for f in schema.fields] == ["OrderId"]


# ---------------------------------------------------------------------------
# CSDL BaseType inheritance (OData v4 §8.4)
# ---------------------------------------------------------------------------

# Microsoft Graph and most real OData v4 services declare keys and
# properties on abstract base types and inherit them through a chain of
# derived types. The connector must walk that chain on metadata lookups
# or it returns empty PKs and incomplete schemas.

INHERITED_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="microsoft.graph" Alias="graph" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <!-- Abstract root: declares the Key + id property everything inherits. -->
      <EntityType Name="entity" Abstract="true">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <!-- Mid-level: adds deletedDateTime; alias-qualified BaseType. -->
      <EntityType Name="directoryObject" BaseType="graph.entity">
        <Property Name="deletedDateTime" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <!-- Leaf: adds user-specific fields, FQN BaseType. -->
      <EntityType Name="user" BaseType="microsoft.graph.directoryObject">
        <Property Name="displayName" Type="Edm.String"/>
        <Property Name="mail" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="GraphService">
        <EntitySet Name="users" EntityType="microsoft.graph.user"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _mock_inherited_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=INHERITED_METADATA_XML, status=200)


@responses.activate
def test_inheritance_primary_key_walks_base_chain():
    """``user`` has no <Key> of its own — Key is on ``entity`` two
    levels up. Without chain walking the connector returns no PK; with
    it, MERGE-on-PK at the destination works correctly."""
    _mock_inherited_metadata()
    c = _make()
    meta = c.read_table_metadata("users", {})
    assert meta["primary_keys"] == ["id"]


@responses.activate
def test_inheritance_schema_aggregates_properties_root_to_leaf():
    """Inherited properties (``id``, ``deletedDateTime``) appear before
    the leaf's own additions. Reflects the order a developer reading
    the CSDL would expect: base type first, derived overlays after."""
    _mock_inherited_metadata()
    c = _make()
    schema = c.get_table_schema("users", {})
    names = [f.name for f in schema.fields]
    assert names == ["id", "deletedDateTime", "displayName", "mail"]


@responses.activate
def test_inheritance_alias_resolution():
    """A BaseType referenced via the schema's ``Alias`` (e.g.
    ``graph.entity`` when the schema declares ``Alias="graph"``) must
    resolve to the same EntityType as the full namespace
    (``microsoft.graph.entity``). Graph relies on this for every
    derived type."""
    _mock_inherited_metadata()
    c = _make()
    # directoryObject's BaseType uses the alias; user's uses the full
    # namespace. If alias resolution were broken, one would resolve and
    # the other wouldn't.
    et = c._entity_type_for("users")
    chain = c._resolve_base_chain(et)
    type_names = [t.get("Name") for t in chain]
    assert type_names == ["user", "directoryObject", "entity"]


@responses.activate
def test_inheritance_id_in_schema_when_only_declared_on_base():
    """Concrete regression for the Graph-compatibility bug: ``id`` is
    only declared on ``graph.entity``, but every Graph entity set needs
    it as a column."""
    _mock_inherited_metadata()
    c = _make()
    schema = c.get_table_schema("users", {})
    id_field = next(f for f in schema.fields if f.name == "id")
    assert type(id_field.dataType).__name__ == "StringType"
    assert id_field.nullable is False


CYCLE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="bad" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="A" BaseType="bad.B">
        <Property Name="a_field" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="B" BaseType="bad.A">
        <Key><PropertyRef Name="b_field"/></Key>
        <Property Name="b_field" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="things" EntityType="bad.A"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_inheritance_cycle_guard_terminates():
    """Malformed CSDL with a BaseType cycle must not loop. The walker
    halts at the first repeat, returning whatever Key/Properties it
    found along the way."""
    responses.get(f"{SERVICE_URL}$metadata", body=CYCLE_METADATA_XML, status=200)
    c = _make()
    # Should terminate (no infinite loop) and surface SOME schema /
    # PK info from whatever chain was walked before the cycle.
    schema = c.get_table_schema("things", {})
    pks = c.read_table_metadata("things", {})["primary_keys"]
    assert {f.name for f in schema.fields} == {"a_field", "b_field"}
    assert pks == ["b_field"]


@responses.activate
def test_inheritance_unresolvable_base_returns_what_can_be_resolved():
    """BaseType references that point at a non-existent type
    (e.g. an external schema we didn't fetch) just truncate the
    chain — they're not a hard error."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="x" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Item" BaseType="external.Missing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Items" EntityType="x.Item"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    responses.get(f"{SERVICE_URL}$metadata", body=xml, status=200)
    c = _make()
    # External BaseType reference can't be resolved — connector still
    # produces the local Key + Property data.
    meta = c.read_table_metadata("Items", {})
    schema = c.get_table_schema("Items", {})
    assert meta["primary_keys"] == ["Id"]
    assert [f.name for f in schema.fields] == ["Id"]


# ---------------------------------------------------------------------------
# Delta tracking (Prefer: odata.track-changes)
# ---------------------------------------------------------------------------


# Realistic delta link shape — server-minted opaque token. The connector
# treats this URL as the offset payload to resume from.
DELTA_LINK_V1 = f"{SERVICE_URL}Customers?$deltatoken=tok-1"
DELTA_LINK_V2 = f"{SERVICE_URL}Customers?$deltatoken=tok-2"


def _delta_bootstrap_body(value, delta_link=DELTA_LINK_V1, next_link=None):
    """Construct a delta-bootstrap response body. Defaults match the
    OData v4 spec: full snapshot + terminal ``@odata.deltaLink``."""
    body = {"@odata.context": f"{SERVICE_URL}$metadata#Customers", "value": value}
    if delta_link is not None:
        body["@odata.deltaLink"] = delta_link
    if next_link is not None:
        body["@odata.nextLink"] = next_link
    return body


@responses.activate
def test_delta_metadata_returns_cdc_with_synthetic_sequence_cursor():
    """When delta is active for a table, the connector advertises
    ``ingestion_type=cdc`` with the synthetic ``_lc_sequence`` cursor.
    Primary keys still come from the entity type's CSDL ``<Key>`` —
    apply_changes uses them as the MERGE key at the destination."""
    _mock_metadata()
    c = _make()
    meta = c.read_table_metadata("Customers", {"delta_tracking": "enabled"})
    assert meta == {
        "primary_keys": ["Id"],
        "cursor_field": "_lc_sequence",
        "ingestion_type": "cdc",
    }


@responses.activate
def test_delta_schema_appends_deleted_and_sequence_columns():
    """The destination needs the synthetic columns in the Spark schema
    so Delta accepts the emitted records. ``_deleted`` carries the
    in-band tombstone signal; ``_lc_sequence`` is apply_changes'
    sequence_by column."""
    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {"delta_tracking": "enabled"})
    names = [f.name for f in schema.fields]
    assert names == ["Id", "Name", "ModifiedAt", "_deleted", "_lc_sequence"]
    deleted_field = schema.fields[3]
    sequence_field = schema.fields[4]
    assert type(deleted_field.dataType).__name__ == "BooleanType"
    assert type(sequence_field.dataType).__name__ == "StringType"
    assert deleted_field.nullable is False
    assert sequence_field.nullable is False


@responses.activate
def test_delta_enabled_with_cursor_field_raises():
    """``delta_tracking=enabled`` and ``cursor_field`` are mutually
    exclusive — the server-driven delta stream provides its own
    sequencing, layering cursor filtering on top would over-constrain
    the read."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="mutually exclusive"):
        c.read_table_metadata(
            "Customers",
            {"delta_tracking": "enabled", "cursor_field": "ModifiedAt"},
        )


@responses.activate
def test_delta_invalid_setting_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="auto, enabled, disabled"):
        c.read_table_metadata("Customers", {"delta_tracking": "sometimes"})


@responses.activate
def test_delta_disabled_default_sends_no_prefer_header():
    """Default ``delta_tracking=disabled`` means existing snapshot /
    cursor pipelines see zero behavior change and zero extra HTTP cost.
    No ``Prefer`` header is sent on any request."""
    _mock_metadata()
    captured_headers = []

    def _callback(request):
        captured_headers.append(dict(request.headers))
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    c.read_table("Customers", None, {})
    assert all("Prefer" not in h for h in captured_headers)


@responses.activate
def test_delta_auto_probe_positive_routes_through_delta_path():
    """``delta_tracking=auto`` probes once. If the server returns
    ``Preference-Applied: odata.track-changes``, the connector marks
    the table delta-capable and reads via the delta path."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        # Probe call ($top=1) — return Preference-Applied to acknowledge.
        if call_count["n"] == 1:
            assert request.headers.get("Prefer") == "odata.track-changes"
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(_delta_bootstrap_body([])),
            )
        # Bootstrap call (after probe) — same header, but tests above
        # only care that the read path was reached.
        return (
            200,
            {"Preference-Applied": "odata.track-changes"},
            json.dumps(
                _delta_bootstrap_body(
                    [{"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"}]
                )
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    records, offset = c.read_table("Customers", None, {"delta_tracking": "auto"})
    rows = list(records)
    # Bootstrap row plus synthetic columns.
    assert len(rows) == 1
    assert rows[0]["Id"] == 1
    assert rows[0]["_deleted"] is False
    assert "_lc_sequence" in rows[0]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_auto_probe_silent_ignore_falls_back():
    """Some servers accept the ``Prefer`` request, return data, but
    don't echo ``Preference-Applied``. The connector treats that as
    "not supported" and falls back to snapshot — silently, so the
    auto path stays usable without extra config."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        # Probe: no Preference-Applied → probe says "not supported".
        # Snapshot follow-up: returns regular data.
        return (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, offset = c.read_table("Customers", None, {"delta_tracking": "auto"})
    rows = list(records)
    assert rows == [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]
    # Empty offset = snapshot mode. No delta_link in there.
    assert _drop_lb(offset) == {}


@responses.activate
def test_delta_auto_probe_transient_failure_records_nothing():
    """A transient failure during the ``delta_tracking=auto`` probe degrades
    that call to the snapshot/cursor path but caches NO verdict — the same
    definitive-only discipline as the other capability probes. Pinning
    ``False`` for the instance's lifetime on a momentary 503 would keep a
    delta-capable stream on the wrong path until the reader is recreated."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"error": "down"}, status=503)
    c = _make({"max_retries": "0", "retry_max_delay_seconds": "0"})
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is False
    assert not c._delta_capable  # transient → no verdict cached
    # The server recovers: the SAME instance re-probes and gets the verdict.
    responses.reset()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": []},
        headers={"Preference-Applied": "odata.track-changes"},
    )
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is True
    assert list(c._delta_capable.values()) == [True]  # definitive → cached


@responses.activate
def test_delta_auto_probe_408_is_transient_not_a_verdict():
    """A 408 sits outside the retry set, so ``_http_get`` RETURNS it rather
    than raising after the budget — the probe must classify it as transient
    (no verdict cached), not as a definitive "server doesn't acknowledge"."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"error": "timeout"}, status=408)
    c = _make()
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is False
    assert not c._delta_capable  # transient → no verdict cached, re-probes


@responses.activate
def test_delta_auto_probe_400_falls_back():
    """Servers can outright reject the ``Prefer`` header with 4xx. The
    probe surfaces False and the connector falls back to snapshot."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Probe rejected.
            return (400, {}, '{"error": "Bad prefer"}')
        return (200, {}, '{"value": [{"Id": 7, "Name": "G", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, _ = c.read_table("Customers", None, {"delta_tracking": "auto"})
    assert [r["Id"] for r in list(records)] == [7]


@responses.activate
def test_delta_enabled_without_preference_applied_raises():
    """``delta_tracking=enabled`` is the user's positive assertion that
    the server supports it. If the bootstrap response is missing the
    ``Preference-Applied`` header, surface a clear error pointing at
    ``delta_tracking=disabled``."""
    _mock_metadata()
    # No Preference-Applied in the response → connector raises.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=_delta_bootstrap_body([]),
        status=200,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="Preference-Applied"):
        records, _ = c.read_table("Customers", None, {"delta_tracking": "enabled"})
        list(records)


@responses.activate
def test_delta_bootstrap_emits_full_snapshot_with_deleted_false():
    """Initial bootstrap call emits all current rows with
    ``_deleted=False`` and a monotonic ``_lc_sequence``. Offset is the
    server's first delta link."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=_delta_bootstrap_body(
            [
                {"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 2, "Name": "B", "ModifiedAt": "2024-02-01T00:00:00Z"},
            ]
        ),
        headers={"Preference-Applied": "odata.track-changes"},
    )
    c = _make()
    records, offset = c.read_table("Customers", None, {"delta_tracking": "enabled"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert all(r["_deleted"] is False for r in rows)
    # Sequences are strictly increasing per emit.
    seqs = [r["_lc_sequence"] for r in rows]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 2
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_resume_emits_changes_and_removes_via_in_band_deleted_flag():
    """Resume call (offset has ``delta_link``) walks that URL. Regular
    entries become ``_deleted=False`` records, ``@removed`` entries
    become ``_deleted=True`` records carrying only the primary key."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "@odata.context": f"{SERVICE_URL}$metadata#Customers/$delta",
            "value": [
                {"Id": 5, "Name": "E", "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"@removed": {"reason": "deleted"}, "Id": 2},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert len(rows) == 2
    change, tombstone = rows
    assert change["Id"] == 5
    assert change["Name"] == "E"
    assert change["_deleted"] is False
    # Non-key columns ride the tombstone as EXPLICIT NULLs — the framework
    # parser rejects an ABSENT non-nullable column but accepts a null one.
    assert tombstone == {
        "Id": 2,
        "Name": None,
        "ModifiedAt": None,
        "_deleted": True,
        "_lc_sequence": tombstone["_lc_sequence"],
    }
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_resume_walks_nextlink_chain_to_captured_deltalink():
    """The delta response itself can paginate via ``@odata.nextLink``.
    The terminal page carries the new ``@odata.deltaLink`` — the
    connector follows the chain to completion before returning."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 10, "Name": "Ten", "ModifiedAt": "x"}],
            "@odata.nextLink": next_link,
        },
    )
    responses.add(
        responses.GET,
        next_link,
        json={
            "value": [{"Id": 11, "Name": "Eleven", "ModifiedAt": "y"}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [10, 11]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_no_op_response_preserves_prior_delta_link():
    """Graph-rotation guard: even when the server mints a fresh
    deltaLink on every response, an empty change set means "no
    progress" — the connector hands the prior link back so the
    framework sees ``end_offset == start_offset`` and AvailableNow can
    terminate."""
    _mock_metadata()
    # Server returns no records AND a rotated deltaLink. Without the
    # rotation guard the offset would advance and the framework would
    # commit forever.
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    assert list(records) == []
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_410_triggers_full_rebootstrap():
    """The server can expire a delta token (410 Gone). The connector
    re-bootstraps automatically: emits the fresh snapshot as
    ``_deleted=False`` rows and returns a brand-new delta link."""
    _mock_metadata()
    # First call: 410 on the stored delta link.
    responses.add(responses.GET, DELTA_LINK_V1, status=410)
    # Re-bootstrap: fresh snapshot via Prefer.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json=_delta_bootstrap_body(
            [{"Id": 99, "Name": "Reborn", "ModifiedAt": "x"}],
            delta_link=DELTA_LINK_V2,
        ),
        headers={"Preference-Applied": "odata.track-changes"},
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [99]
    assert all(r["_deleted"] is False for r in rows)
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_sparse_entity_raises_runtimeerror():
    """OData v4 §11.4 lets the server return only the changed
    properties on an update. Applying that as-is would write NULLs over
    good values at the destination — silent corruption. The connector
    refuses sparse responses with an actionable error."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                # Missing "Name" and "ModifiedAt" — schema declares them.
                {"Id": 5},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="sparse entity"):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": DELTA_LINK_V1},
            {"delta_tracking": "enabled"},
        )
        list(records)


@responses.activate
def test_delta_sparse_check_runs_on_every_entity_not_just_the_first():
    """Mixed payloads are the norm for real delta services: full entities
    for creates, changed-properties-only for updates. A full-bodied create
    at the head of the batch must not wave the sparse update behind it
    through to a NULL-writing MERGE — the guard runs per entity."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                # Full entity first (a create) — the old first-entry-only
                # sampling stopped checking here.
                {"Id": 5, "Name": "E", "ModifiedAt": "2024-01-01T00:00:00Z"},
                # Sparse update behind it — missing ModifiedAt.
                {"Id": 6, "Name": "F"},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="sparse entity"):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": DELTA_LINK_V1},
            {"delta_tracking": "enabled"},
        )
        list(records)


@responses.activate
def test_delta_page_decode_retries_corrupt_200_body():
    """Delta pages get the same corrupt-200-body retry as cursor/snapshot
    pages (``_fetch_page_payload``): a truncated JSON body under load is
    retried with a fresh GET instead of hard-failing the stream."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        body='{"value": [{"Id": 99, "Name": "trunc',  # cut mid-serialization
        status=200,
        content_type="application/json",
    )
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 99, "Name": "ok", "ModifiedAt": "2024-01-01T00:00:00Z"}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [99]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_sparse_check_honors_select():
    """When the user restricts the projection via ``$select``, only the
    selected fields are expected in every delta entry. Returning only
    those (and nothing else) is no longer "sparse"."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                # Only Id + Name, matching the select clause exactly.
                {"Id": 5, "Name": "E"},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, _ = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled", "select": "Id,Name"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [5]
    # No exception — schema only requires Id + Name (+ synthetic columns).


@responses.activate
def test_delta_max_records_caps_at_page_boundary_and_stashes_next_link():
    """A long catch-up after a paused pipeline can return more rows than
    ``max_records_per_batch``. The connector caps at the **page boundary**
    (stops following ``@odata.nextLink``) and stashes the unfollowed link as
    the resume point. The cap must NOT truncate mid-page: the stashed link
    points at the NEXT page, so any rows dropped from the current page would
    never be re-fetched — permanent loss during bootstrap. The cap therefore
    overshoots by up to one server page instead."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                {"Id": 1, "Name": "A", "ModifiedAt": "x"},
                {"Id": 2, "Name": "B", "ModifiedAt": "x"},
                {"Id": 3, "Name": "C", "ModifiedAt": "x"},
            ],
            "@odata.nextLink": next_link,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # The whole cap-hit page is emitted (bounded overshoot, never loss).
    assert [r["Id"] for r in rows] == [1, 2, 3]
    # Offset carries both prior delta_link (fallback) AND next_link
    # (preferred resume point) — pagination stopped at the page boundary.
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1, "next_link": next_link}


@responses.activate
def test_delta_resume_via_next_link_continues_pagination():
    """After a cap-hit batch the next call's offset has ``next_link``.
    The connector resumes from that URL directly, no fresh ``Prefer``
    header, no probe."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    captured_headers = []

    def _callback(request):
        captured_headers.append(dict(request.headers))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": 3, "Name": "C", "ModifiedAt": "x"}],
                    "@odata.deltaLink": DELTA_LINK_V2,
                }
            ),
        )

    responses.add_callback(responses.GET, next_link, callback=_callback)
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"next_link": next_link, "delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [3]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}
    # Resume must not re-send the bootstrap-only Prefer header.
    assert all("Prefer" not in h for h in captured_headers)


@responses.activate
def test_delta_dispatch_recognizes_delta_link_offset_without_enabled_flag():
    """A pipeline started with ``delta_tracking=enabled`` checkpoints a
    delta-shaped offset; if the next run loses that table option (config
    drift, partial rollout) the dispatch must still take the delta path
    based on the offset shape alone — losing the offset shape and
    treating it as a fresh snapshot would re-fetch the whole table."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    # No delta_tracking option set, but the offset carries a delta_link.
    records, offset = c.read_table("Customers", {"delta_link": DELTA_LINK_V1}, {})
    assert list(records) == []
    # Rotation guard: prior link preserved on no-op.
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


# ---------------------------------------------------------------------------
# Contained navigation properties (ContainsTarget="true")
# ---------------------------------------------------------------------------


NESTED_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Nested" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Parent">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <NavigationProperty Name="Children" Type="Collection(Nested.Child)" ContainsTarget="true"/>
        <NavigationProperty Name="Tags" Type="Collection(Nested.Tag)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Child">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Label" Type="Edm.String"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Notes" Type="Collection(Nested.Note)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Note">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Text" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Tag">
        <Key>
          <PropertyRef Name="Category"/>
          <PropertyRef Name="Value"/>
        </Key>
        <Property Name="Category" Type="Edm.String" Nullable="false"/>
        <Property Name="Value" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Parents" EntityType="Nested.Parent"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _mock_nested_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=NESTED_METADATA_XML, status=200)


RECURSIVE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Rec" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Node">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Label" Type="Edm.String"/>
        <NavigationProperty Name="Children" Type="Collection(Rec.Node)" ContainsTarget="true"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Nodes" EntityType="Rec.Node"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def _mock_recursive_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=RECURSIVE_METADATA_XML, status=200)


@responses.activate
def test_recursive_containment_fk_columns_stay_distinct_per_level():
    """A hand-written recursive containment path repeats the same nav-prop
    name at two non-leaf levels. The FK mapping is keyed by level INDEX —
    a name-keyed map would collapse both levels into one entry, duplicating
    the surviving column in the schema and dropping a composite-key
    component (silent MERGE collisions between leaves under different
    level-1 parents)."""
    _mock_recursive_metadata()
    c = _make()
    table = "Nodes__Children__Children__Children"
    schema = c.get_table_schema(table, {})
    names = [f.name for f in schema.fields]
    assert len(names) == len(set(names)), f"duplicate columns in schema: {names}"
    # One distinct FK column per non-leaf level, collision-suffixed.
    assert names[:3] == ["Nodes_Id", "Children_Id", "_Children_Id"]
    # The composite key carries every level's component plus the leaf PK.
    assert c._primary_keys_for(table) == ["Nodes_Id", "Children_Id", "_Children_Id", "Id"]


@responses.activate
def test_recursive_containment_rows_tagged_with_each_levels_fk():
    """The N+1 walk stamps each ancestor level's PK into its OWN column —
    the deeper repeated level must not overwrite the shallower one."""
    _mock_recursive_metadata()
    responses.get(f"{SERVICE_URL}Nodes", json={"value": [{"Id": 1, "Label": "root"}]})
    responses.get(f"{SERVICE_URL}Nodes(1)/Children", json={"value": [{"Id": 10, "Label": "l1"}]})
    responses.get(
        f"{SERVICE_URL}Nodes(1)/Children(10)/Children",
        json={"value": [{"Id": 100, "Label": "l2"}]},
    )
    responses.get(
        f"{SERVICE_URL}Nodes(1)/Children(10)/Children(100)/Children",
        json={"value": [{"Id": 1000, "Label": "leaf"}]},
    )
    c = _make()
    recs, _ = c.read_table(
        "Nodes__Children__Children__Children",
        {},
        {"expand_contained": "false", "contained_fetch": "single", "pagination": "nextlink"},
    )
    rows = list(recs)
    assert rows == [
        {"Nodes_Id": 1, "Children_Id": 10, "_Children_Id": 100, "Id": 1000, "Label": "leaf"}
    ]


DECIMAL_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Dec" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Money">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Exact" Type="Edm.Decimal" Precision="10" Scale="2"/>
        <Property Name="Wide" Type="Edm.Decimal"/>
        <Property Name="Varying" Type="Edm.Decimal" Precision="20" Scale="variable"/>
        <Property Name="BigId" Type="Edm.Decimal" Precision="38"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Moneys" EntityType="Dec.Money"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_decimal_precision_scale_facets_honoured():
    """``Edm.Decimal`` honours declared CSDL ``Precision``/``Scale`` facets.
    A hardcoded ``DecimalType(38, 18)`` leaves only 20 digits left of the
    point — it can't hold a ``Decimal(38, 0)`` ID column's large values.
    Absent facets (and ``Scale="variable"``) keep the historical wide
    default so existing destinations don't shift types; ``Scale`` absent
    with ``Precision`` declared is scale 0 (the CSDL default)."""
    responses.get(f"{SERVICE_URL}$metadata", body=DECIMAL_METADATA_XML, status=200)
    c = _make()
    types = {f.name: f.dataType for f in c.get_table_schema("Moneys", {}).fields}
    assert types["Exact"] == DecimalType(10, 2)
    assert types["Wide"] == DecimalType(38, 18)
    assert types["Varying"] == DecimalType(38, 18)
    assert types["BigId"] == DecimalType(38, 0)


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


# --- Path parsing / discovery ---


def test_parse_contained_path_flat_returns_none():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    assert _parse_contained_path("Customers") is None


def test_parse_contained_path_multi_segment():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    assert _parse_contained_path("A__B__C") == ["A", "B", "C"]


def test_parse_contained_path_rejects_empty_segment():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    with pytest.raises(ValueError, match="Empty path segment"):
        _parse_contained_path("A____B")


def test_parse_contained_path_rejects_slash_with_actionable_message():
    """Old-form slash paths are common when the user copied the table
    name from OData URL syntax or from a pre-fix version of
    ``list_tables``. The error must spell out the rename so the user
    isn't left staring at a "not found" with a 200-entry available list.
    """
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    with pytest.raises(
        ValueError, match="Rename 'Instances/AssetPacks' to 'Instances__AssetPacks'"
    ):
        _parse_contained_path("Instances/AssetPacks")


def test_parse_contained_path_rejects_over_depth():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    # 11 segments exceeds the depth-10 cap.
    with pytest.raises(ValueError, match="exceeds max depth"):
        _parse_contained_path("A__B__C__D__E__F__G__H__I__J__K")


@responses.activate
def test_list_tables_includes_nested_paths():
    _mock_nested_metadata()
    c = _make()
    flat = c.list_tables()
    # Top-level + every reachable contained path.
    assert "Parents" in flat
    assert "Parents__Children" in flat
    assert "Parents__Tags" in flat
    assert "Parents__Children__Notes" in flat


@responses.activate
def test_list_tables_in_namespace_includes_nested_paths():
    _mock_nested_metadata()
    c = _make()
    tables = c.list_tables_in_namespace(["Nested"])
    assert tables == [
        "Parents",
        "Parents__Children",
        "Parents__Children__Notes",
        "Parents__Tags",
    ]


# --- Entity type resolution / schema / PK ---


@responses.activate
def test_get_table_schema_for_two_level_contained():
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children", {})
    names = [f.name for f in schema.fields]
    # Parent FK prepended, then child's own fields in CSDL order.
    assert names == ["Parents_Id", "Id", "Label", "ModifiedAt"]
    fk_field = schema["Parents_Id"]
    assert isinstance(fk_field.dataType, IntegerType)
    assert fk_field.nullable is False


@responses.activate
def test_get_table_schema_for_three_level_contained_emits_full_ancestor_chain():
    """For ``A__B__C`` every non-leaf ancestor contributes FK columns
    (OData v4 §13.4.3 — contained-entity keys are unique within parent
    only, so the full chain is required for global uniqueness)."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children__Notes", {})
    names = [f.name for f in schema.fields]
    assert names == ["Parents_Id", "Children_Id", "Id", "Text"]


@responses.activate
def test_get_table_schema_for_contained_with_composite_parent_pk():
    """Parents__Tags has a composite-key leaf; FK prepend on a single-PK
    parent yields exactly one ancestor column. Inverse test (composite
    parent) requires a different fixture — covered indirectly via the
    Tag leaf's own composite key showing up in primary_keys_for."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Tags", {})
    names = [f.name for f in schema.fields]
    assert names == ["Parents_Id", "Category", "Value"]


@responses.activate
def test_primary_keys_for_two_level_contained():
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children", {})
    assert meta["primary_keys"] == ["Parents_Id", "Id"]
    assert meta["ingestion_type"] == "snapshot"


@responses.activate
def test_primary_keys_for_three_level_contained_full_ancestor_chain():
    """Composite PK is every ancestor's FK + leaf PK — required for
    global uniqueness when leaf IDs only repeat within a parent."""
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children__Notes", {})
    assert meta["primary_keys"] == ["Parents_Id", "Children_Id", "Id"]


@responses.activate
def test_primary_keys_for_composite_leaf_in_contained():
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Tags", {})
    # Composite PK on the leaf — both columns surface alongside parent FK.
    assert meta["primary_keys"] == ["Parents_Id", "Category", "Value"]


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
def test_entity_type_for_invalid_nav_prop_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not a contained-collection"):
        c.read_table_metadata("Parents__NotAThing", {})


# --- URL construction ---


@responses.activate
def test_key_predicate_single_key():
    _mock_nested_metadata()
    c = _make()
    assert c._format_key_predicate({"Id": 42}) == "(42)"


@responses.activate
def test_key_predicate_composite():
    _mock_nested_metadata()
    c = _make()
    pred = c._format_key_predicate({"Category": "fruit", "Value": "apple"})
    assert pred == "(Category='fruit',Value='apple')"


@responses.activate
def test_build_contained_url_two_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_contained_url(["Parents", "Children"], [{"Id": 7}], {"page_size": "1000"})
    assert url.startswith(f"{SERVICE_URL}Parents(7)/Children?")
    assert "$top=1000" in url


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


def _pagination_dataset():
    return [{"Id": i, "ModifiedAt": f"2024-01-{i:02d}T00:00:00Z"} for i in range(1, 6)]


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


def test_pg_is_continuation_recognizes_any_casing():
    """Server continuations use arbitrary casing (Microsoft stacks emit
    $skipToken). Missing one would let the $top injection append onto an
    opaque token URL — the §11.2.5.7 hazard the guard exists to avoid."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _pg_is_continuation,
    )

    assert _pg_is_continuation("https://svc/Coll?$skipToken=abc") is True
    assert _pg_is_continuation("https://svc/Coll?%24skipToken=abc") is True
    assert _pg_is_continuation("https://svc/Coll?$SKIP=5") is True
    assert _pg_is_continuation("https://svc/Coll?$top=5") is False


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
def test_expand_cursor_lookback_floors_read_filter_not_offset():
    """``cursor_lookback_seconds`` floors the read filter by the overlap
    window (so a non-atomic walk re-scans mid-walk arrivals) but commits the
    TRUE max watermark, not the floored value."""
    from urllib.parse import unquote

    _mock_nested_metadata()
    captured: list[str] = []

    def _parents(req):
        captured.append(unquote(req.url))
        if "Id gt" in unquote(req.url):  # top-level auto drain probe
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-02T00:00:00Z"},
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",  # 1h overlap
        },
    )
    rows = list(records)
    # Read filter floored by 1h behind the committed 2024-01-02T00:00:00Z.
    assert any("ModifiedAt gt 2024-01-01T23:00:00" in u for u in captured), captured
    # Committed offset is the TRUE max emitted, NOT the floored read value.
    assert _drop_lb(offset) == {"cursor": "2024-01-03T00:00:00Z"}
    assert [r["Id"] for r in rows] == [11, 12]


@responses.activate
def test_lookback_dedup_suppresses_unchanged_overlap_re_emits():
    """``cursor_lookback_dedup=on``: rows re-fetched by the overlap window
    that were already delivered UNCHANGED are suppressed; the offset carries
    the exact ``lb_seen`` map. A pure-overlap follow-up batch emits nothing
    and idles (offset echo), exactly like today's pure-overlap batch."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):  # top-level auto drain probe
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "on",
    }
    records, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert [r["Id"] for r in records] == [11, 12]
    assert offset["cursor"] == "2024-01-03T00:00:00Z"
    assert len(offset["lb_seen"]) == 2  # both delivered rows tracked
    # Batch 2: the same rows come back through the 1h overlap — all suppressed.
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert list(records2) == []
    assert offset2 == offset  # idle echo; lb_seen preserved


@responses.activate
def test_lookback_dedup_reemits_changed_row_same_cursor():
    """A row whose NON-CURSOR column changed between batches (cursor
    untouched — a source that updates without advancing the cursor) must be
    re-emitted: the content hash differs. The unchanged sibling stays
    suppressed. This is the hazard that keying on (PK, cursor) alone would
    silently swallow."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "on",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    children[0] = {"Id": 11, "Label": "CHANGED", "ModifiedAt": "2024-01-02T12:00:00Z"}
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    got = [(r["Id"], r["Label"]) for r in records2]
    assert got == [(11, "CHANGED")]  # changed row re-emitted, sibling suppressed
    # The changed row's new hash replaces its entry (batch 3 suppresses it).
    records3, _ = c.read_table("Parents__Children", offset2, opts)
    assert list(records3) == []


@responses.activate
def test_lookback_dedup_cap_overflow_keeps_newest_and_reemits_rest():
    """Above the entry cap, the highest-cursor entries are kept and the
    evicted rows degrade to plain re-emits — the pre-dedup behavior, never
    loss. cap=1 over two in-window rows: the newer row stays suppressed;
    the evicted older row follows the pre-dedup idle rule (deferred on a
    quiescent trigger, re-emitted on the next PROGRESSING batch)."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "1",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert len(offset["lb_seen"]) == 1  # capped; newest (Id 12) kept
    # Quiescent trigger: the evicted row's re-read produces no lb_seen delta,
    # so the pre-dedup idle rule holds — deferred, not replayed per trigger.
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert list(records2) == []
    # A PROGRESSING batch (new row 13) delivers the evicted re-read alongside
    # the new row; the capped entry (Id 12) stays suppressed.
    children.append({"Id": 13, "Label": "c", "ModifiedAt": "2024-01-04T00:00:00Z"})
    records3, offset3 = c.read_table("Parents__Children", offset2, opts)
    assert sorted(r["Id"] for r in records3) == [11, 13]
    assert offset3["cursor"] == "2024-01-04T00:00:00Z"
    assert len(offset3["lb_seen"]) == 1  # still capped; newest (Id 13) kept


@responses.activate
def test_lookback_dedup_explicit_off_reemits_overlap():
    """``cursor_lookback_dedup=off`` restores the pre-dedup behavior:
    overlap rows re-flow every batch (idled on quiescent triggers) and no
    ``lb_seen`` rides the offset."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"}
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "off",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert "lb_seen" not in offset
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    # Pure-overlap batch: the row was re-FETCHED and flowed through the
    # pre-existing suppressed-idle rule (returns [] with lookback on, echoes
    # the offset) — the pre-dedup semantics hold, no lb_seen rides.
    assert list(records2) == []
    assert offset2 == offset
    assert "lb_seen" not in offset2


def test_lookback_dedup_parse_modes():
    """Option grammar: absent/empty → the DEFAULT cap (dedup is on by
    default); off/false/0 → 0; on/true → default cap (the boolean
    spellings match the connector's other flag options); positive int →
    that cap; garbage and non-positive raise curated errors."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        LOOKBACK_DEDUP_DEFAULT_CAP,
        parse_lookback_dedup,
    )

    assert parse_lookback_dedup({}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup(None) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": ""}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": "off"}) == 0
    assert parse_lookback_dedup({"cursor_lookback_dedup": "false"}) == 0
    assert parse_lookback_dedup({"cursor_lookback_dedup": "0"}) == 0
    assert parse_lookback_dedup({"cursor_lookback_dedup": "on"}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": "true"}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": "250"}) == 250
    with pytest.raises(ValueError, match="cursor_lookback_dedup"):
        parse_lookback_dedup({"cursor_lookback_dedup": "sometimes"})
    with pytest.raises(ValueError, match="cursor_lookback_dedup"):
        parse_lookback_dedup({"cursor_lookback_dedup": "-3"})


@responses.activate
def test_lookback_dedup_quiescent_delta_keeps_auto_history():
    """A quiescent trigger whose only offset movement is ``lb_seen``
    bookkeeping (the dedup delta-delivery branch) must NOT record its
    overlap-only walk duration into the ``auto`` lookback history —
    ``_LOOKBACK_AUTO_WINDOW`` such triggers in a row would flush every
    real walk duration out of the rolling window and shrink the ``auto``
    window below the walks it must cover."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):  # top-level auto drain probe
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "auto",
        "cursor_lookback_dedup": "on",
    }
    # Batch 1 (progressing): records a real walk duration. The auto window
    # was still 0 during it (no history yet), so dedup stayed inert.
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    history = offset["lb_history"]
    assert len(history) == 1
    assert "lb_seen" not in offset
    # Batch 2 (quiescent): the window is active now; the overlap re-reads
    # enter tracking for the first time — a one-time lb_seen delta delivery
    # with NO cursor progress. The history must carry through UNCHANGED.
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert sorted(r["Id"] for r in records2) == [11, 12]
    assert offset2["lb_history"] == history  # not polluted by the re-read
    assert len(offset2["lb_seen"]) == 2
    # Batch 3 (quiescent, tracked): fully suppressed — identity idle.
    records3, offset3 = c.read_table("Parents__Children", offset2, opts)
    assert list(records3) == []
    assert offset3 == offset2


@responses.activate
def test_lookback_dedup_cap_eviction_deterministic_on_cursor_ties():
    """Cap eviction breaks cursor ties by PK key, not fetch order: an
    order-unstable server must not flap the surviving cap-set between
    batches — each flap would be an ``lb_seen`` delta that re-emits the
    newly evicted (already-delivered) row on every quiescent trigger."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T12:00:00Z"},  # cursor TIE
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "1",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert len(offset["lb_seen"]) == 1
    # Deterministic winner: the tie broke on the PK key (Id 12), not on
    # the fetch order that happened to put Id 11 first.
    assert "12" in next(iter(offset["lb_seen"]))
    # Quiescent trigger with the server returning the tie in the OPPOSITE
    # order: the same entry must survive (no lb_seen delta), so the batch
    # idles instead of re-emitting the flapped-out row every trigger.
    children.reverse()
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert list(records2) == []
    assert offset2 == offset


@responses.activate
def test_lookback_dedup_tolerates_corrupt_lb_seen_state():
    """``lb_seen`` rides the user-visible checkpoint (same discipline as
    the ``lb_history`` validation in ``_resolve_active_lookback``): a
    hand-edited/corrupt shape must degrade to plain re-emits — never a
    crash and never suppression — and the entry is rebuilt well-formed."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "on",
    }
    # "[1, 11]" is Id 11's real composite-PK key, so the corrupt entry IS
    # looked up; non-dict lb_seen exercises the container guard instead.
    for corrupt in (
        "garbage",
        ["not", "a", "dict"],
        {"[1, 11]": []},
        {"[1, 11]": 42},
        {"[1, 11]": ["x"]},
    ):
        records, offset = c.read_table(
            "Parents__Children",
            {"cursor": "2024-01-02T00:00:00Z", "lb_seen": corrupt},
            opts,
        )
        assert sorted(r["Id"] for r in records) == [11, 12], corrupt
        assert offset["cursor"] == "2024-01-03T00:00:00Z"
        assert len(offset["lb_seen"]) == 2  # rebuilt exact and well-formed
        assert all(isinstance(v, list) and len(v) == 2 for v in offset["lb_seen"].values())


@responses.activate
def test_lookback_dedup_cap_eviction_non_timestamp_cursor_no_crash():
    """Cap eviction must not assume a timestamp cursor: the connector
    supports any server-orderable cursor (integer IDs, GUIDs, strings —
    see ``_read_incremental``), and a bare ``parse_iso8601`` on such a
    value would crash the read at the first cap overflow. Non-ISO cursors
    fall back to best-effort classes; ties break on the PK key."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Label",  # Edm.String — never ISO-parses
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "1",
    }
    # First streaming batch (no floor): both rows emitted, the 1-entry cap
    # overflows, and eviction sorts the non-ISO cursors — no crash. "a" and
    # "b" tie in the length-fallback class, so the PK key decides,
    # deterministically.
    records, offset = c.read_table("Parents__Children", {}, opts)
    assert sorted(r["Id"] for r in records) == [11, 12]
    assert offset["cursor"] == "b"
    assert len(offset["lb_seen"]) == 1
    assert "12" in next(iter(offset["lb_seen"]))  # PK tiebreak, fetch-order-free


def test_lb_seen_order_key_never_raises_and_orders():
    """Eviction order-key contract: never raises (non-timestamp cursors
    are supported, and the cursor slot rides the user-visible checkpoint)
    and orders ISO timestamps chronologically, numerics numerically, and
    everything else by string length, with ``None`` at the very bottom."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _lb_seen_order_key as key,
    )

    # Never raises: non-ISO strings, GUIDs, non-str shapes, nan/inf, empty.
    for v in (
        None,
        "a",
        "12345",
        "550e8400-e29b-41d4-a716-446655440000",
        {"weird": 1},
        ["x"],
        "nan",
        "inf",
        "",
    ):
        key(v)
    # Numerics: exact magnitude order (lexical order would invert this).
    assert key("99") < key("100")
    # Timestamps: chronological, and ranked above every non-ISO class.
    assert key("2024-01-01T00:00:00Z") < key("2024-01-02T00:00:00Z")
    assert key("100") < key("2024-01-01T00:00:00Z")
    # Fallback class: longer string wins; None sorts at the very bottom.
    assert key("ab") < key("abc")
    assert key(None) < key("a")


def test_lookback_dedup_filter_streams_suppression():
    """The streaming half of ``cursor_lookback_dedup``: a proven-unchanged
    overlap row is rejected AT EMIT TIME (never buffered) with its entry
    carried; changed/new rows are admitted and deliberately leave NO entry
    on the filter — finalize computes delivered-row entries from the final
    post-trim buffer, so an admitted-then-trimmed row can't poison
    ``lb_seen``. Corrupt checkpoint entries never match."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _lb_row_digest,
        _lb_row_key,
        _LookbackDedupFilter,
    )

    row = {"Id": 1, "Label": "a"}
    prior = {_lb_row_key(row, ["Id"]): ["c1", _lb_row_digest(row)]}
    flt = _LookbackDedupFilter(prior, ["Id"])
    assert flt.admit(dict(row)) is False  # unchanged → suppressed, unbuffered
    assert flt.carried == prior  # entry carried forward
    assert flt.admit({"Id": 1, "Label": "B"}) is True  # changed → delivered
    assert flt.admit({"Id": 2, "Label": "x"}) is True  # new → delivered
    assert set(flt.carried) == set(prior)  # delivered rows left no entry
    for bad in ([], 42, ["x"], "junk"):  # corrupt entries never match
        f2 = _LookbackDedupFilter({_lb_row_key(row, ["Id"]): bad}, ["Id"])
        assert f2.admit(dict(row)) is True


def test_cursor_newer_numeric_rendering_symmetry():
    """Numerically-equal but textually-different cursor renderings must be
    the same instant in BOTH directions ("5000.0" vs "5000" — the lexical
    tie fallback called one strictly newer), and Int64 values beyond float
    precision (tied float sort keys) must order truly via exact Decimal."""
    from databricks.labs.community_connector.sources.odata._helpers import cursor_newer

    assert cursor_newer("5000.0", "5000") is False
    assert cursor_newer("5000", "5000.0") is False
    assert cursor_newer("9007199254740993", "9007199254740992") is True  # > 2^53
    assert cursor_newer("9007199254740992", "9007199254740993") is False
    # Timestamp semantics untouched (ISO text never Decimal-parses).
    assert cursor_newer("2024-01-02T00:00:00.5Z", "2024-01-02T00:00:00Z") is True
    assert cursor_newer("2024-01-02T00:00:00Z", "2024-01-02T00:00:00.5Z") is False


def test_bin_pack_hits_requested_partition_count():
    """Balanced ``divmod`` sizing yields every partition the user asked for
    (uniform ``ceil(n/p)`` slicing collapsed n=9,p=4 to 3 bins), with
    exactly-once contiguous coverage and no empty bins."""
    from databricks.labs.community_connector.sources.odata._partition import _bin_pack

    rows = [{"Id": i} for i in range(9)]
    parts = _bin_pack(rows, 4, None)
    assert [len(p["top_parent_rows"]) for p in parts] == [3, 2, 2, 2]
    assert [r["Id"] for p in parts for r in p["top_parent_rows"]] == list(range(9))
    assert len(_bin_pack([{"Id": i} for i in range(10)], 6, None)) == 6
    assert len(_bin_pack([{"Id": 1}, {"Id": 2}], 5, None)) == 2  # never empty bins
    assert _bin_pack([], 4, None) == []


@responses.activate
def test_batch_probe_missing_substatus_is_definitive_fail():
    """A 2xx ``$batch`` envelope whose sub-response omits ``status`` is a
    malformed envelope, not a pass: the old ``.get("status", 0)`` read it
    as ``0 < 400`` and minted a definitive ``batch_ok=True`` from garbage
    (every hydrate then pays a doomed ``$batch`` POST before degrading to
    per-op GETs). Same discipline as the id-less envelope: definitive
    FAIL, hydrate goes straight to plain GETs."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={"responses": [{"id": "0", "body": {"value": []}}]},  # no status
    )
    c = _make()
    assert c._verify_batch_support(["Roots"], {}) is False
    assert c.__dict__["_batch_supported"] is False  # pinned definitively


@responses.activate
def test_cursor_lookback_non_utc_watermark_single_escape_on_wire():
    """The lookback floor returns a BARE ISO string; the single percent-escape
    happens at literal generation (``_cursor_filter`` → ``odata_literal``). A
    pre-escaped floor would be re-fed through ``odata_literal``, where the
    ``%2B`` fails the ISO sniff and double-escapes into a QUOTED garbage
    string (``'…%252B10:00'``) — a wrong-type comparison or 400 on every
    lookback-floored batch against a non-UTC source."""
    _mock_nested_metadata()
    captured: list[str] = []

    def _parents(req):
        captured.append(req.url)  # RAW url — escape fidelity is the point here
        if "Id%20gt" in req.url or "Id gt" in req.url:  # top-level drain probe
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00+10:00"}
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-02T00:00:00+10:00"},
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",
        },
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]
    assert _drop_lb(offset) == {"cursor": "2024-01-02T12:00:00+10:00"}
    filtered = [u for u in captured if "ModifiedAt" in u]
    # The floored filter reached the wire ONCE-escaped and unquoted.
    assert any("2024-01-01T23:00:00%2B10:00" in u for u in filtered), captured
    assert all("%252B" not in u for u in captured)  # never double-escaped
    assert all("'2024" not in u for u in captured)  # never a quoted string literal


def test_apply_cursor_lookback_returns_bare_iso_string():
    """The floor stays in raw cursor value space — bare ISO text, not an
    escaped OData literal — so client-side row comparisons and the single
    escape at URL build both see consistent input."""
    c = _make()
    c.__dict__["_active_lookback_seconds"] = 3600
    assert c._apply_cursor_lookback("2024-01-02T00:00:00+10:00") == "2024-01-01T23:00:00+10:00"
    assert c._apply_cursor_lookback("2024-01-02T00:00:00Z") == "2024-01-01T23:00:00Z"


@responses.activate
def test_expand_cursor_lookback_idles_on_no_progress_instead_of_raising():
    """Quiescent re-read: the floored filter re-returns the overlap rows
    (cursor <= committed) but no row exceeds the watermark. With lookback
    this never raises the no-progress error (which is what the plain
    ``cursor gt`` path would do): under default-on dedup the FIRST such
    batch delivers the overlap rows once (they enter ``lb_seen`` tracking
    — real offset progress), and every later quiescent trigger idles
    (empty, offset unchanged)."""
    from urllib.parse import unquote

    _mock_nested_metadata()

    def _parents(req):
        if "Id gt" in unquote(req.url):
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
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    start = {"cursor": "2024-01-02T00:00:00Z"}  # == the only child's cursor
    records, offset = c.read_table(
        "Parents__Children",
        start,
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",
        },
    )
    # First quiescent batch: the overlap row enters dedup tracking and is
    # delivered once (lb_seen delta = offset progress) — still no raise.
    assert [r["Id"] for r in records] == [11]
    assert _drop_lb(offset) == start  # cursor did not advance
    assert len(offset["lb_seen"]) == 1
    # Second quiescent batch: tracked and unchanged — idles.
    records2, offset2 = c.read_table(
        "Parents__Children",
        offset,
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",
        },
    )
    assert list(records2) == []
    assert offset2 == offset  # idled, no advance, no RuntimeError


@responses.activate
def test_cursor_lookback_explicit_rejected_on_flat_table():
    """An explicit cursor_lookback_seconds is only meaningful for the non-atomic
    contained walks (expand or leaf-cursor/probe); on a flat table it has
    nothing to floor and is rejected. (It IS now allowed on a leaf-cursor
    contained path without expand_contained — see the leaf-cursor lookback
    tests.)"""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="explicit value.*contained path"):
        c.read_table(
            "Customers",
            {"cursor": "2024-01-01T00:00:00Z"},
            {"cursor_field": "ModifiedAt", "cursor_lookback_seconds": "300"},
        )


@responses.activate
def test_cursor_lookback_non_timestamp_cursor_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="datetime/timestamp cursor|not ISO-8601"):
        c.read_table(
            "Parents__Children",
            {"cursor": 11},  # int cursor value
            {
                "expand_contained": "true",
                "cursor_field": "Id",
                "cursor_lookback_seconds": "300",
            },
        )


@responses.activate
def test_cursor_lookback_invalid_value_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid cursor_lookback_seconds|must be >= 0"):
        c.read_table(
            "Parents__Children",
            {"cursor": "2024-01-01T00:00:00Z"},
            {
                "expand_contained": "true",
                "cursor_field": "ModifiedAt",
                "cursor_lookback_seconds": "-5",
            },
        )


def test_cursor_lookback_parse_modes():
    """Default is ``auto``; ``off`` disables; an integer is static seconds."""
    c = _make()
    assert c._parse_cursor_lookback({}) == "auto"
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "auto"}) == "auto"
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "off"}) == 0
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "0"}) == 0
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "300"}) == 300


def test_cursor_lookback_factor_and_ceiling_parse():
    """``auto`` tuning knobs parse with defaults and validate positivity."""
    c = _make()
    assert c._parse_cursor_lookback_factor({}) == 1.5
    assert c._parse_cursor_lookback_factor({"cursor_lookback_factor": "2.5"}) == 2.5
    assert c._parse_cursor_lookback_ceiling({}) == 3600
    assert c._parse_cursor_lookback_ceiling({"cursor_lookback_max_seconds": "600"}) == 600
    with pytest.raises(ValueError, match="cursor_lookback_factor must be > 0"):
        c._parse_cursor_lookback_factor({"cursor_lookback_factor": "0"})
    with pytest.raises(ValueError, match="Invalid cursor_lookback_factor"):
        c._parse_cursor_lookback_factor({"cursor_lookback_factor": "abc"})
    with pytest.raises(ValueError, match="cursor_lookback_max_seconds must be > 0"):
        c._parse_cursor_lookback_ceiling({"cursor_lookback_max_seconds": "0"})


def test_cursor_lookback_auto_resolve_max_of_recent_scaled_clamped():
    """``auto`` sizes the window from the MAX of the last-N walk durations
    × factor, clamped to the ceiling; static mode ignores the history."""
    c = _make()
    c._cursor_lookback = "auto"
    c._cursor_lookback_factor = 1.5
    c._cursor_lookback_max_seconds = 3600
    assert c._resolve_active_lookback({}) == 0  # no history yet
    assert c._resolve_active_lookback({"lb_history": [40, 100, 60]}) == 150  # max(100) × 1.5
    assert c._resolve_active_lookback({"lb_history": [100000]}) == 3600  # clamped
    # sub-second history -> sub-second window (no floor to 0)
    assert c._resolve_active_lookback({"lb_history": [0.3]}) == 0.45  # max(0.3) × 1.5
    assert c._resolve_active_lookback({"lb_history": [0.02, 0.3, 0.1]}) == 0.45  # max(0.3) × 1.5
    # nanosecond-scale history survives (9 dp), not floored to zero
    assert c._resolve_active_lookback({"lb_history": [0.000000002]}) == 0.000000003  # ×1.5
    # custom factor / ceiling
    c._cursor_lookback_factor = 3.0
    c._cursor_lookback_max_seconds = 250
    assert c._resolve_active_lookback({"lb_history": [50, 80]}) == 240  # max(80) × 3.0
    assert c._resolve_active_lookback({"lb_history": [200]}) == 250  # clamped to custom ceiling
    c._cursor_lookback = 50
    assert c._resolve_active_lookback({"lb_history": [100]}) == 50  # static ignores history


def test_cursor_lookback_auto_attach_history():
    """``auto`` appends every completed progressing walk (including sub-second,
    at ms precision) to a rolling last-N history, carries prior while
    in-flight, leaves idle/static offsets untouched."""
    c = _make()
    c._cursor_lookback = "auto"
    # completed progressing walk -> append
    assert c._attach_lookback_state({"cursor": "X"}, {}, False, 12.0) == {
        "cursor": "X",
        "lb_history": [12],
    }
    # append onto prior, capped to the window (5) — oldest dropped
    assert c._attach_lookback_state(
        {"cursor": "X"}, {"lb_history": [1, 2, 3, 4, 5]}, False, 9.0
    ) == {
        "cursor": "X",
        "lb_history": [2, 3, 4, 5, 9],
    }
    # sub-second walk -> NOW recorded (down to nanosecond precision), so a fast
    # source still gets a (fast) overlap window instead of zero
    assert c._attach_lookback_state({"cursor": "X"}, {}, False, 0.2) == {
        "cursor": "X",
        "lb_history": [0.2],
    }
    # nanosecond-scale walk is kept (rounded to 9 dp), not floored to zero
    assert c._attach_lookback_state({"cursor": "X"}, {"lb_history": [7]}, False, 0.000000123) == {
        "cursor": "X",
        "lb_history": [7, 0.000000123],
    }
    # in-flight carries the prior history unchanged and stamps the cycle
    # anchor (wall clock; exact value asserted in the dedicated cycle-span
    # test) so completion can measure the whole capped cycle
    in_flight = c._attach_lookback_state({"pending_fetches": []}, {"lb_history": [9]}, True, 0.0)
    assert in_flight["pending_fetches"] == [] and in_flight["lb_history"] == [9]
    assert isinstance(in_flight["lb_cycle_started"], float)
    # idle (out is the same object as start) -> untouched
    start = {"cursor": "X", "lb_history": [7]}
    assert c._attach_lookback_state(start, start, False, 5.0) is start
    # static mode never writes bookkeeping
    c._cursor_lookback = 50
    assert c._attach_lookback_state({"cursor": "X"}, {}, False, 12.0) == {"cursor": "X"}


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
def test_delta_walk_guard_stops_self_referential_link():
    """The delta walk guards against a self-referential @odata.nextLink: the
    server points the continuation back at the same URL. The self-loop is
    detected before re-fetching, and — since the broken chain produced
    records with no advanced change cursor — the no-progress guard raises
    rather than emitting the same records against the same offset forever
    (round-30: previously this returned rows + the unchanged prior link,
    which was byte-for-byte the infinite-churn shape)."""
    _mock_metadata()
    calls = []

    def cb(req):
        calls.append(req.url)
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [{"Id": 10, "Name": "x", "ModifiedAt": "t"}],
                    "@odata.nextLink": DELTA_LINK_V1,
                }
            ),
        )

    responses.add_callback(responses.GET, DELTA_LINK_V1, callback=cb)
    c = _make()
    with pytest.raises(RuntimeError, match="no terminal @odata.deltaLink"):
        records, _ = c.read_table(
            "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
        )
        list(records)
    assert len(calls) == 1  # self-loop detected before re-fetching


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
def test_build_contained_url_three_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_contained_url(
        ["Parents", "Children", "Notes"],
        [{"Id": 7}, {"Id": 9}],
        {},
    )
    assert url.startswith(f"{SERVICE_URL}Parents(7)/Children(9)/Notes?")


def test_rewrite_top_in_url():
    """Inner-collection nextLink continuations inherit the small
    per-level ``$top`` from the original ``$expand`` clause. The
    rewrite helper bumps that ``$top`` so paging through a wide inner
    collection doesn't take 100s of round trips at the dynamic per-
    level value."""
    from databricks.labs.community_connector.sources.odata._contained import (
        rewrite_top_in_url,
    )

    # Bare $top
    assert (
        rewrite_top_in_url("https://x.com/A?$top=10&$skip=100", 1000)
        == "https://x.com/A?$top=1000&$skip=100"
    )
    # URL-encoded %24top
    assert (
        rewrite_top_in_url("https://x.com/A?%24top=10&%24skip=100", 500)
        == "https://x.com/A?%24top=500&%24skip=100"
    )
    # Preserves other params verbatim
    assert (
        rewrite_top_in_url("https://x.com/A?$filter=Id+eq+5&$top=10&$skip=20", 200)
        == "https://x.com/A?$filter=Id+eq+5&$top=200&$skip=20"
    )
    # No $top → unchanged
    assert (
        rewrite_top_in_url("https://x.com/A?$skiptoken=abc", 1000)
        == "https://x.com/A?$skiptoken=abc"
    )


def test_compute_dynamic_tops():
    """``compute_dynamic_tops`` distributes ``page_size`` across all
    levels with triangular weights so the cross-product fits in the
    budget. Top gets the largest share; minimum per level is 5."""
    from databricks.labs.community_connector.sources.odata._contained import (
        compute_dynamic_tops,
    )

    assert compute_dynamic_tops(1000, 1) == [1000]
    # 100 × 10 = 1000 (exactly fits)
    assert compute_dynamic_tops(1000, 2) == [100, 10]
    # 34 × 5 × 5 = 850. Bottom clamps to MIN, remaining 200-budget split
    # across the upper two levels: 200^(2/3) ≈ 34, 200^(1/3) ≈ 5.
    assert compute_dynamic_tops(1000, 3) == [34, 5, 5]
    # 8 × 5 × 5 × 5 = 1000. Bottom three clamp to MIN, top gets the
    # remaining 1000 / 125 = 8.
    assert compute_dynamic_tops(1000, 4) == [8, 5, 5, 5]
    # Cross-product never exceeds page_size when it's mathematically
    # possible (i.e. MIN ** N <= page_size).
    for n in (2, 3, 4):
        tops = compute_dynamic_tops(1000, n)
        product = 1
        for t in tops:
            product *= t
        assert product <= 1000, f"N={n} product={product} exceeds budget"
        assert all(t >= 5 for t in tops)
    # Small budget: every level clamps to minimum (5**3 = 125 > 10).
    assert compute_dynamic_tops(10, 3) == [5, 5, 5]


def test_compute_expand_tops_for_root():
    """A continuation rooted below level 0 budgets ``page_size`` across only its
    own collection levels (root..leaf); the fixed-key ancestors above take no
    share. Entries below the root are placeholders (never read)."""
    from databricks.labs.community_connector.sources.odata._contained import (
        compute_expand_tops_for_root,
    )

    # root_level=0 over a 4-segment chain == the full distribution.
    assert compute_expand_tops_for_root(1000, 4, 0) == [8, 5, 5, 5]
    # The xmla_demo case: a 4-segment chain, continuation rooted at level 2
    # (Instances(k)/Projects(k)/WorkPackageDetails?...$expand=WorkPackagesStepDetails).
    # Only levels 2,3 are collections → [100, 10] there, not the [5, 5] the
    # whole-chain distribution would force. Levels 0,1 are placeholders.
    assert compute_expand_tops_for_root(1000, 4, 2) == [0, 0, 100, 10]
    # Continuation rooted at the leaf level gets the entire budget.
    assert compute_expand_tops_for_root(1000, 4, 3) == [0, 0, 0, 1000]


@responses.activate
def test_build_expand_url_three_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children", "Notes"], {"page_size": "1000"})
    # Dynamic distribution for N=3, page_size=1000: [34, 5, 5] (product 850).
    # PK-only $orderby is injected at every (non-cursor) level for
    # skiptoken stability.
    assert "Parents?$top=34" in url
    assert "$orderby=Id asc" in url
    assert "$expand=Children($top=5;$orderby=Id asc;$expand=Notes($top=5;$orderby=Id asc))" in url


@responses.activate
def test_build_expand_url_four_level_nests_correctly():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["A", "B", "C", "D"], {"page_size": "1000"})
    # Dynamic distribution for N=4, page_size=1000: [8, 5, 5, 5] (product 1000).
    # A/B/C/D aren't declared in the fixture metadata, so the per-level
    # PK $orderby degrades to none — this test pins the $top nesting
    # structure only (real-entity $orderby is covered above).
    assert "A?$top=8" in url
    assert "$expand=B($top=5;$expand=C($top=5;$expand=D($top=5)))" in url


@responses.activate
def test_build_expand_url_dynamic_tops_for_two_level():
    """User's stated rule: for a 2-segment expand with page_size=1000,
    the top URL gets ``$top=100`` and the single inner expand gets
    ``$top=10`` — product equals the budget exactly."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children"], {"page_size": "1000"})
    assert "Parents?$top=100" in url
    assert "$expand=Children($top=10;$orderby=Id asc)" in url


@responses.activate
def test_build_expand_url_page_size_scales_dynamic_tops():
    """Reducing ``page_size`` scales every level proportionally."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children"], {"page_size": "100"})
    # For N=2 page_size=100: inner = 100^(1/3) ≈ 4.6 → clamped to 5,
    # then upper level absorbs remaining budget = 100 // 5 = 20.
    # Product 20 × 5 = 100 (exact).
    assert "Parents?$top=20" in url
    assert "$expand=Children($top=5;$orderby=Id asc)" in url


@responses.activate
def test_build_expand_url_inner_top_with_cursor_clause():
    """Inner ``$top`` composes with ``$filter``/``$orderby`` when a
    cursor is injected at that level."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(
        ["Parents", "Children"],
        {"page_size": "500"},
        cursor_level=1,
        cursor_filter="ModifiedAt gt 2024-01-01T00:00:00Z",
        cursor_order="ModifiedAt asc,Id asc",
    )
    # Dynamic distribution for N=2, page_size=500: [62, 7]. $filter and
    # $orderby compose with the inner $top at the cursor's level.
    assert "Parents?$top=62" in url
    assert "$expand=Children($top=7" in url
    assert "$filter=ModifiedAt gt 2024-01-01T00:00:00Z" in url
    assert "$orderby=ModifiedAt asc,Id asc" in url


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
def test_read_table_disables_cap_when_start_offset_none_and_cap_unset(caplog):
    """Spark's batch reader (``LakeflowBatchReader``) calls
    ``read_table`` with ``start_offset=None`` and discards the
    returned end-offset. ``read_table`` detects that signal and
    raises ``max_records_per_batch`` to a near-infinite sentinel so
    the cap can't fire and the chain drains fully in one call —
    parked ``pending_fetches`` would otherwise be silently dropped.

    Streaming readers always pass a dict (``{}`` initial or parked
    offset), so this override does not touch the streaming path.

    A user-set ``max_records_per_batch`` is **also** overridden in
    batch mode (with a warning), because the discarded offset means a
    cap there can only truncate-and-lose — honouring it would silently
    drop the remainder. Resumable caps only make sense for streaming."""
    _mock_nested_metadata()
    captured: list[dict] = []

    def _spy(self_, table_name, start_offset, table_options):
        captured.append(dict(table_options))
        return iter([]), {}

    c = _make()
    # start_offset=None, cap unset → override applies.
    from databricks.labs.community_connector.sources.odata.odata import (
        ODataLakeflowConnect,
        _BATCH_UNCAPPED,
    )

    original = ODataLakeflowConnect._read_contained_expand
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    try:
        c.read_table("Parents__Children", None, {"expand_contained": "true"})
    finally:
        ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]

    assert captured[0]["max_records_per_batch"] == str(_BATCH_UNCAPPED)

    # start_offset=None AND cap explicitly set → still overridden to the
    # uncapped sentinel, and a warning names the ignored value.
    captured.clear()
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    with caplog.at_level(logging.WARNING):
        try:
            c.read_table(
                "Parents__Children",
                None,
                {"expand_contained": "true", "max_records_per_batch": "50"},
            )
        finally:
            ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]
    assert captured[0]["max_records_per_batch"] == str(_BATCH_UNCAPPED)
    assert any("max_records_per_batch=50 ignored" in r.getMessage() for r in caplog.records)

    # start_offset={} (streaming) → override never applies.
    captured.clear()
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    try:
        c.read_table("Parents__Children", {}, {"expand_contained": "true"})
    finally:
        ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]
    assert "max_records_per_batch" not in captured[0]


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


# --- Per-segment filters (filter_at_<segment>, filter_at_<idx>) ---


def test_resolve_segment_filters_name_form():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {
            "filter_at_Parents": "Id eq 5",
            "filter_at_Children": "Status eq 'active'",
            "filter_at_Notes": "Text ne null",
            "filter": "ignored — different key",
        },
        ["Parents", "Children", "Notes"],
    )
    assert out == {0: "Id eq 5", 1: "Status eq 'active'", 2: "Text ne null"}


def test_resolve_segment_filters_index_form():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {"filter_at_0": "Id eq 5", "filter_at_2": "Text ne null"},
        ["Parents", "Children", "Notes"],
    )
    assert out == {0: "Id eq 5", 2: "Text ne null"}


def test_resolve_segment_filters_case_insensitive_segment_name():
    """Lakeflow Connect lowercases option keys before forwarding them
    to ``read_table``, so a pipeline-config ``filter_at_Instances``
    arrives as ``filter_at_instances``. The segment-name match must
    be case-insensitive."""
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {
            "filter_at_instances": "Id eq 1",  # lowercased by framework
            "filter_at_PROJECTS": "Id eq 2",  # any casing accepted
        },
        ["Instances", "Projects", "WorkPackageDetails"],
    )
    assert out == {0: "Id eq 1", 1: "Id eq 2"}


def test_resolve_segment_filters_index_overrides_name_on_conflict():
    """Index form is the more explicit of the two — wins when both
    target the same level."""
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {"filter_at_Children": "by name", "filter_at_1": "by index"},
        ["Parents", "Children", "Notes"],
    )
    assert out[1] == "by index"


def test_resolve_segment_filters_unknown_segment_raises():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    with pytest.raises(ValueError, match="Bogus"):
        resolve_segment_filters(
            {"filter_at_Bogus": "Id eq 5"},
            ["Parents", "Children", "Notes"],
        )


def test_resolve_segment_filters_out_of_range_index_raises():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    with pytest.raises(ValueError, match="out of range"):
        resolve_segment_filters(
            {"filter_at_5": "Id eq 5"},
            ["Parents", "Children", "Notes"],
        )


def test_combine_filters():
    from databricks.labs.community_connector.sources.odata._contained import (
        combine_filters,
    )

    assert combine_filters(None, None) is None
    assert combine_filters("A", None) == "A"
    assert combine_filters(None, "B") == "B"
    assert combine_filters("A", "B") == "(A) and (B)"
    assert combine_filters("A", None, "C") == "(A) and (C)"


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


# --- Flat table ---


@responses.activate
def test_flat_filter_at_segment_applies_to_flat_table_read():
    """For a flat (non-contained) table, ``filter_at_<table>`` is
    equivalent to the existing ``filter`` option — both AND into the
    single URL's ``$filter`` clause."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"CustomerID": "ALFKI", "CompanyName": "Alfreds"}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1000",
                    "$filter": "CustomerID eq 'ALFKI'",
                    "$orderby": "Id asc",
                }
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Customers", json={"value": []})
    c = _make()
    records, _ = c.read_table("Customers", None, {"filter_at_Customers": "CustomerID eq 'ALFKI'"})
    assert len(list(records)) == 1


# --- Errors ---


@responses.activate
def test_filter_at_unknown_segment_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Bogus"):
        records, _ = c.read_table("Parents__Children", None, {"filter_at_Bogus": "Id eq 5"})
        list(records)


@responses.activate
def test_filter_at_out_of_range_index_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="out of range"):
        records, _ = c.read_table("Parents__Children", None, {"filter_at_5": "Id eq 5"})
        list(records)


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


def _churn_walk_opts():
    return {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "3",
        "pagination": "nextlink",
    }


def _churn_children_cb(rows):
    """Children endpoint callback honoring the walk's ``cursor gt`` filter."""

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(req.url).query).get("$filter", [""])[0])
        out = rows
        m = re.search(r"ModifiedAt gt (\S+)", flt)
        if m:
            out = [r for r in rows if r["ModifiedAt"] > m.group(1)]
        return (200, {}, json.dumps({"value": out}))

    return cb


@responses.activate
def test_capped_walk_resume_survives_parent_delete():
    """The truncation checkpoint parks the truncated parent's KEY CHAIN, not
    just its position: a parent deleted below the park shifts every
    successor left one slot, and a positional resume then skips the parked
    parent forever — its unread tail excluded by ``cursor gt <watermark>``
    on every later batch (permanent loss; beyond lookback during a capped
    bootstrap). The key-based resume re-finds the parked parent."""
    _mock_nested_metadata()
    parents_state = [{"Id": 10}, {"Id": 20}, {"Id": 30}]
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents",
        callback=lambda _r: (200, {}, json.dumps({"value": parents_state})),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=_churn_children_cb([{"Id": 101, "ModifiedAt": "2024-01-01T00:00:00Z"}]),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=_churn_children_cb(
            [
                {"Id": 201, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 202, "ModifiedAt": "2024-01-02T00:00:00Z"},
                {"Id": 203, "ModifiedAt": "2024-01-03T00:00:00Z"},
            ]
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(30)/Children",
        callback=_churn_children_cb([{"Id": 301, "ModifiedAt": "2024-02-01T00:00:00Z"}]),
    )
    c = _make()
    recs1, offset1 = c.read_table("Parents__Children", {}, _churn_walk_opts())
    # Batch 1: parent 10 in full + parent 20 trimmed at the c2 boundary.
    assert [r["Id"] for r in recs1] == [101, 201, 202]
    assert offset1["parent_keys"] == [{"Id": 20}]
    # Parent 10 is deleted between batches — every survivor shifts left.
    parents_state[:] = [{"Id": 20}, {"Id": 30}]
    recs2, offset2 = c.read_table("Parents__Children", offset1, _churn_walk_opts())
    # Batch 2 must resume AT parent 20 (its unread tail), then walk 30.
    # The positional resume skipped 20 entirely and lost row 203.
    assert [r["Id"] for r in recs2] == [203, 301]
    assert _drop_lb(offset2) == {"cursor": "2024-02-01T00:00:00Z"}


@responses.activate
def test_capped_walk_parked_link_follows_parent_keys_not_position():
    """A parent inserted below the park shifts the enumeration right; a
    positional resume then applies the parked mid-collection continuation
    link to the WRONG parent — its rows FK-tagged with that parent's keys
    (corrupt ancestor attribution). The key-based resume applies the link
    only to the parent that parked it. (The inserted parent's own rows are
    the documented mid-walk-arrival class — recovered via
    ``cursor_lookback`` on a later cycle, never mis-tagged.)"""
    _mock_nested_metadata()
    parents_state = [{"Id": 10}, {"Id": 20}, {"Id": 30}]
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents",
        callback=lambda _r: (200, {}, json.dumps({"value": parents_state})),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=_churn_children_cb([{"Id": 101, "ModifiedAt": "2024-01-01T00:00:00Z"}]),
    )
    token_page = {"value": [{"Id": 203, "ModifiedAt": "2024-01-03T00:00:00Z"}]}

    def p20_cb(req):
        if "$skiptoken=t1" in req.url:
            return (200, {}, json.dumps(token_page))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"Id": 201, "ModifiedAt": "2024-01-01T00:00:00Z"},
                        {"Id": 202, "ModifiedAt": "2024-01-02T00:00:00Z"},
                    ],
                    "@odata.nextLink": f"{SERVICE_URL}Parents(20)/Children?$skiptoken=t1",
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(20)/Children", callback=p20_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(30)/Children",
        callback=_churn_children_cb([{"Id": 301, "ModifiedAt": "2024-02-01T00:00:00Z"}]),
    )
    # Parents(15)/Children deliberately unregistered: any fetch of the
    # inserted parent (e.g. the parked link misapplied to it under the old
    # positional resume) fails the test via ConnectionError.
    c = _make()
    recs1, offset1 = c.read_table("Parents__Children", {}, _churn_walk_opts())
    # Batch 1: parent 10 (1 row) + parent 20 page 1 (2 rows) = cap; the
    # page's nextLink is the checkpoint.
    assert [r["Id"] for r in recs1] == [101, 201, 202]
    assert offset1["parent_keys"] == [{"Id": 20}]
    assert offset1["chain_next_link"].endswith("$skiptoken=t1")
    # Parent 15 is inserted below the park between batches.
    parents_state[:] = [{"Id": 10}, {"Id": 15}, {"Id": 20}, {"Id": 30}]
    recs2, _ = c.read_table("Parents__Children", offset1, _churn_walk_opts())
    # The link's rows must be tagged with parent 20 — the parent that
    # parked it — never with the inserted parent occupying its old slot.
    assert [(r["Parents_Id"], r["Id"]) for r in recs2] == [(20, 203), (30, 301)]


@responses.activate
def test_capped_walk_legacy_positional_offset_still_resumes():
    """Offsets written before ``parent_keys`` existed carry only
    ``parent_idx`` — they must keep resuming positionally (stable parent
    set), so an upgrade mid-stream doesn't strand a parked checkpoint."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 10}, {"Id": 20}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(10)/Children",
        callback=_churn_children_cb(
            [
                {"Id": 101, "ModifiedAt": "2024-01-01T00:00:00Z"},
                {"Id": 102, "ModifiedAt": "2024-01-02T00:00:00Z"},
            ]
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(20)/Children",
        callback=_churn_children_cb([{"Id": 201, "ModifiedAt": "2024-02-01T00:00:00Z"}]),
    )
    c = _make()
    legacy = {"parent_idx": 0, "truncated_chain_cursor": "2024-01-01T00:00:00Z"}
    recs, offset = c.read_table("Parents__Children", legacy, _churn_walk_opts())
    # Positional resume: parent at index 0 re-read from cursor gt c1.
    assert [r["Id"] for r in recs] == [102, 201]
    assert _drop_lb(offset) == {"cursor": "2024-02-01T00:00:00Z"}


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
def test_expand_midpage_park_resumes_by_row_key_not_position():
    """The expand drainer's mid-page park must carry the last processed
    row's ORDER KEY, not a positional skip: on a cursor-ordered top page,
    updating an already-emitted row moves it to the tail of the re-fetched
    page and shifts an UNREAD row into the skipped prefix — its whole
    subtree lost behind the watermark under a positional resume."""
    _mock_nested_metadata()
    # Mutable source: parents with ISO ``Name`` as the level-0 cursor, one
    # inline child each.
    state = [
        {"Id": 10, "Name": "2024-01-01T00:00:00Z", "kid": 101},
        {"Id": 20, "Name": "2024-01-02T00:00:00Z", "kid": 201},
        {"Id": 30, "Name": "2024-01-03T00:00:00Z", "kid": 301},
        {"Id": 40, "Name": "2024-01-04T00:00:00Z", "kid": 401},
    ]

    def parents_cb(_req):
        rows = [
            {
                "Id": p["Id"],
                "Name": p["Name"],
                "Children": [{"Id": p["kid"], "Label": "x", "ModifiedAt": p["Name"]}],
            }
            for p in sorted(state, key=lambda p: (p["Name"], p["Id"]))
        ]
        return (200, {}, json.dumps({"value": rows}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=parents_cb)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
    }
    recs1, offset1 = c.read_table("Parents__Children", {}, opts)
    # Batch 1: children of parents 10 and 20 emitted; mid-page park.
    assert sorted(r["Id"] for r in recs1) == [101, 201]
    assert offset1["pending_fetches"][0]["boundary"] == ["2024-01-02T00:00:00Z", 20]
    # Parent 10 is updated between batches → moves to the TAIL of the
    # cursor-ordered page; parent 30 shifts into the old positional prefix.
    state[0]["Name"] = "2024-01-05T00:00:00Z"
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    got = [r["Id"] for r in recs2]
    if offset2.get("pending_fetches"):
        recs3, _ = c.read_table("Parents__Children", offset2, opts)
        got += [r["Id"] for r in recs3]
    # Key-based resume: parents 30, 40, and the updated 10 all emit (across
    # the remaining capped batches). The positional skip=2 resume lost
    # parent 30's subtree entirely.
    assert sorted(got) == [101, 301, 401]


def _expand_inner_park_batch1():
    """Shared setup: a parked LEVEL-1 inner-collection continuation under
    parent 1 (the server paged Children with ``Children@odata.nextLink``),
    constructed DIRECTLY.

    The depth-first drainer drains an inner continuation in the same batch it
    discovers it, so a real batch-1 seldom parks one mid-collection — but a
    parked inner continuation is still a valid resume state (the cap can fire
    with one pending, and old offsets carry them). The recovery path
    (:meth:`_recover_expand_item`) is unchanged; resuming from this hand-built
    offset exercises it in isolation, exactly as a real park would."""
    inner_link = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=t1"
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "3",
        "pagination": "nextlink",
    }
    offset1 = {
        "pending_fetches": [
            {
                "url": inner_link,
                "level": 1,
                "chain": [{"Id": 1}],
                "cur_val": "2024-01-01T00:00:00Z",
                "skip": 0,
            }
        ],
        "cursor": "2024-01-01T00:00:00Z",
        "running_max_cursor": "2024-01-01T00:00:00Z",
    }
    return opts, offset1


@responses.activate
def test_expand_parked_continuation_for_deleted_parent_drops_subtree():
    """A parked ``pending_fetches`` continuation is an entity-scoped URL;
    if its parent is deleted between batches the URL 404s FOREVER —
    re-raising turned the checkpoint into a permanently failing stream
    only a full refresh could recover. The resume must instead confirm
    the parent is gone (the from-scratch rebuild 404s too) and drop the
    subtree, duplicate-safe."""
    _mock_nested_metadata()
    c = _make()
    opts, offset1 = _expand_inner_park_batch1()
    # Parent 1 deleted: BOTH the parked continuation and any rebuilt
    # collection URL under it now 404.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda _r: (404, {}, json.dumps({"error": {"message": "not found"}})),
    )
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    # No raise; the dead subtree is dropped and the walk completes.
    assert list(recs2) == []
    assert "pending_fetches" not in offset2


@responses.activate
def test_expand_stale_inner_continuation_rebuilds_from_scratch():
    """The SAME 404/410 can mean the server continuation went stale
    (expired ``$skiptoken``) while the parent still exists — dropping the
    item there would silently lose the rest of the collection. The
    resume rebuilds the collection URL from the parked chain and re-reads
    it from scratch: bounded duplicates, never loss."""
    _mock_nested_metadata()
    c = _make()
    opts, offset1 = _expand_inner_park_batch1()

    def children_cb(req):
        if "skiptoken" in req.url:
            return (410, {}, json.dumps({"error": {"message": "token expired"}}))
        # The rebuilt from-scratch URL: the collection's remaining row.
        return (200, {}, json.dumps({"value": [{"Id": 13, "Label": "d"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=children_cb)
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    rows = list(recs2)
    # The rebuilt read recovered the collection's remaining row, tagged
    # with the PARKED chain's parent.
    assert [(r["Parents_Id"], r["Id"]) for r in rows] == [(1, 13)]
    assert "pending_fetches" not in offset2


def _expand_l0_park_batch1(c, parents_cb):
    """Batch 1 of an expand read that parks a LEVEL-0 top continuation
    (the server's top-level $skiptoken link) in ``pending_fetches``."""
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=parents_cb)
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "1",
        "pagination": "nextlink",
    }
    recs1, offset1 = c.read_table("Parents__Children", {}, opts)
    assert [r["Id"] for r in recs1] == [11]
    pending = offset1["pending_fetches"]
    assert len(pending) == 1 and pending[0]["level"] == 0
    assert "skiptoken=top1" in pending[0]["url"]
    return opts, offset1


def _expand_l0_page1():
    return {
        "value": [
            {
                "Id": 1,
                "Name": "2024-01-01T00:00:00Z",
                "Children": [{"Id": 11, "Label": "a"}],
            }
        ],
        "@odata.nextLink": f"{SERVICE_URL}Parents?$skiptoken=top1",
    }


@responses.activate
def test_expand_stale_top_level_continuation_rebuilds_from_scratch():
    """A parked LEVEL-0 continuation (the top collection's $skiptoken) can
    expire exactly like an inner one — 410 is the spec-sanctioned signal.
    Re-raising made the checkpoint a permanently failing stream; the
    recovery must rebuild the top-level seed URL from the stashed
    options/watermark and re-read the collection (bounded duplicates)."""
    _mock_nested_metadata()

    state = {"seed_calls": 0}

    def parents_cb(req):
        if "skiptoken" in req.url:
            return (410, {}, json.dumps({"error": {"message": "token expired"}}))
        state["seed_calls"] += 1
        if state["seed_calls"] == 1:  # batch 1's seed fetch
            return (200, {}, json.dumps(_expand_l0_page1()))
        # batch 2's REBUILT seed: the collection's remaining page.
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 2,
                            "Name": "2024-01-02T00:00:00Z",
                            "Children": [{"Id": 21, "Label": "b"}],
                        }
                    ]
                }
            ),
        )

    c = _make()
    opts, offset1 = _expand_l0_park_batch1(c, parents_cb)
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    # The rebuilt seed re-read the top collection; the remaining parent's
    # child is recovered, and the stream is healthy again.
    assert [(r["Parents_Id"], r["Id"]) for r in recs2] == [(2, 21)]
    assert "pending_fetches" not in offset2


@responses.activate
def test_expand_top_level_collection_truly_gone_still_raises():
    """When the REBUILT top-level seed also 404s, the whole collection is
    gone — that's a config/service error, not row churn, and it must
    surface loudly rather than silently dropping the table."""
    _mock_nested_metadata()
    state = {"first": True}

    def parents_cb(_req):
        if state["first"]:
            state["first"] = False
            return (200, {}, json.dumps(_expand_l0_page1()))
        return (404, {}, json.dumps({"error": {"message": "gone"}}))

    c = _make()
    opts, offset1 = _expand_l0_park_batch1(c, parents_cb)
    with pytest.raises(requests.HTTPError):
        records, _ = c.read_table("Parents__Children", offset1, opts)
        list(records)


@responses.activate
def test_expand_parked_queue_is_depth_bounded_not_width():
    """The parked ``pending_fetches`` frontier is bounded by the contained
    path DEPTH, not by the top page's fan-out WIDTH. A wide top page over an
    inner-paging server (here 6 parents, each with a server-paged Children
    collection) would, under a breadth-first drain, park one continuation per
    parent — O(width), a multi-MB offset at scale. The depth-first stack
    machine drains each parent's subtree before the next, so at any park the
    frontier is just the current top-page position plus the O(depth) path
    being walked — never one-per-parent. Every child still arrives exactly
    once across the capped batches."""
    _mock_nested_metadata()
    parents = []
    for i in range(1, 7):
        parents.append(
            {
                "Id": i,
                "Name": f"2024-01-0{i}T00:00:00Z",
                "Children": [{"Id": i * 100 + 1, "Label": "inline"}],
                "Children@odata.nextLink": f"{SERVICE_URL}Parents({i})/Children?$skiptoken=k{i}",
            }
        )
        responses.get(
            f"{SERVICE_URL}Parents({i})/Children",
            json={"value": [{"Id": i * 100 + 2, "Label": "paged"}]},
            match_querystring=False,
        )
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents}, match_querystring=False)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        # LOW cap so the CAP (not any ceiling) drives parking every batch.
        "max_records_per_batch": "2",
        "pagination": "nextlink",
    }
    # Parents__Children has 2 contained segments; the frontier is O(depth):
    # the top-page resume item plus at most one in-flight inner continuation.
    depth_bound = 2 + 2  # len(segments) + a small constant
    got: list[int] = []
    offset: dict = {}
    parked = False
    for _ in range(50):
        records, offset = c.read_table("Parents__Children", offset, opts)
        got.extend(r["Id"] for r in records)
        pending = offset.get("pending_fetches")
        if not pending:
            break
        parked = True
        # The load-bearing assertion: the frontier stays DEPTH-bounded even
        # though the fan-out (6 parents) is far wider.
        assert len(pending) <= depth_bound, f"offset grew to {len(pending)} — width leaked in"
    else:
        raise AssertionError("expand queue never drained")
    # The cap must actually park for this test to prove anything.
    assert parked, "never parked — cap too high, test vacuous"
    # Every inline and every paged child arrived exactly once.
    assert sorted(got) == sorted(
        [i * 100 + 1 for i in range(1, 7)] + [i * 100 + 2 for i in range(1, 7)]
    )


@responses.activate
def test_expand_nextlink_parks_mid_inline_from_start_with_boundary():
    """``pagination=nextlink`` has no churn-safe client seek, so a mid-way
    INLINE collection parks as a FROM-START refetch (``$skip=0``, never a
    positional seek into the page) plus the chronological ``boundary`` of the
    last processed row; resume elides the already-emitted prefix client-side.
    Two properties are load-bearing: (1) the cap parks IMMEDIATELY — batch 1
    emits cap rows, not the whole in-flight inline collection's subtree; and
    (2) deleting an already-emitted row between batches cannot shift an
    unread row out of the refetch (a server-honoured ``$skip=2`` would drop
    it silently — the boundary resume must not)."""
    _mock_nested_metadata()
    children = [{"Id": 100 + i, "Label": f"c{i}"} for i in range(1, 7)]
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 1, "Name": "2024-01-01T00:00:00Z", "Children": children}]},
        match_querystring=False,
    )
    # The from-start refetch of the parked inline collection. Mutable so a
    # later batch observes between-batch churn (c1 deleted).
    live: list[dict] = list(children)
    responses.add_callback(
        responses.GET,
        re.compile(rf"{re.escape(SERVICE_URL)}Parents\(1\)/Children.*"),
        callback=lambda req: (200, {}, json.dumps({"value": list(live)})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",
        "pagination": "nextlink",
    }
    records, offset = c.read_table("Parents__Children", {}, opts)
    got = [r["Id"] for r in records]
    # (1) Immediate park at the cap: 2 rows, not all 6 inline children.
    assert got == [101, 102]
    pending = offset.get("pending_fetches")
    assert pending
    inline_item = next(p for p in pending if p["level"] == 1)
    plain_url = inline_item["url"].replace("%24", "$")
    assert "$skip=0" in plain_url, f"expected from-start refetch, got {plain_url}"
    assert "$skip=2" not in plain_url, f"positional seek leaked into park: {plain_url}"
    assert inline_item["boundary"] is not None
    # (2) Churn: an ALREADY-EMITTED child vanishes between batches.
    del live[0]  # c1 == Id 101, emitted in batch 1
    for _ in range(10):
        records, offset = c.read_table("Parents__Children", offset, opts)
        got.extend(r["Id"] for r in records)
        if not offset.get("pending_fetches"):
            break
    else:
        raise AssertionError("expand queue never drained")
    # No loss (103-106 all arrive despite the deletion) and no duplicates.
    assert sorted(got) == [101, 102, 103, 104, 105, 106]


@responses.activate
def test_expand_three_level_parked_offset_stays_depth_bounded():
    """O(depth), not O(width), on a genuinely DEEP path. Parents__Children__
    Notes with a fan-out (2 parents × 2 children) far wider than its depth
    (3 segments); each child's Notes are server-paged. A breadth-first drain
    would accumulate a continuation per child (O(width)); the depth-first
    stack machine walks one subtree at a time so the parked frontier is the
    O(depth) path only. In nextlink mode a mid-way inline collection parks
    as a FROM-START refetch (positional $skip resume would be churn-unsafe)
    whose boundary elides the already-processed prefix on resume — so the
    cap parks immediately at any depth. Every note arrives exactly once."""
    _mock_nested_metadata()
    parents = []
    for p in (1, 2):
        children = []
        for cidx in (1, 2):
            cid = p * 10 + cidx
            notes_link = f"{SERVICE_URL}Parents({p})/Children({cid})/Notes?$skiptoken=n"
            children.append(
                {
                    "Id": cid,
                    "Label": f"c{cid}",
                    "Notes": [],  # notes deferred behind the nextLink
                    "Notes@odata.nextLink": notes_link,
                }
            )
            responses.get(
                f"{SERVICE_URL}Parents({p})/Children({cid})/Notes",
                json={
                    "value": [
                        {"Id": cid * 100 + 1, "Text": "a"},
                        {"Id": cid * 100 + 2, "Text": "b"},
                    ]
                },
                match_querystring=False,
            )
        # The from-start refetch a mid-inline nextlink park issues on resume
        # (boundary elides already-processed children client-side).
        responses.get(
            f"{SERVICE_URL}Parents({p})/Children",
            json={"value": children},
            match_querystring=False,
        )
        parents.append({"Id": p, "Name": f"2024-01-0{p}T00:00:00Z", "Children": children})
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents}, match_querystring=False)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",  # LOW: force parking across batches
        "pagination": "nextlink",
    }
    depth_bound = 3 + 2  # len(segments) + small constant
    got: list[int] = []
    offset: dict = {}
    parked = False
    for _ in range(50):
        records, offset = c.read_table("Parents__Children__Notes", offset, opts)
        got.extend(r["Id"] for r in records)
        pending = offset.get("pending_fetches")
        if not pending:
            break
        parked = True
        assert len(pending) <= depth_bound, f"offset grew to {len(pending)} — width leaked in"
    else:
        raise AssertionError("expand queue never drained")
    assert parked, "never parked — cap too high, test vacuous"
    # 2 parents × 2 children × 2 notes = 8 leaves, each exactly once.
    expected = [
        cid * 100 + n for p in (1, 2) for cidx in (1, 2) for cid in [p * 10 + cidx] for n in (1, 2)
    ]
    assert sorted(got) == sorted(expected)


@responses.activate
def test_expand_deep_single_collection_pages_with_bounded_offset():
    """The DEPTH case (a single parent whose inner collection pages across
    many server pages) must still page via ``$top``/``@odata.nextLink`` with
    an O(1) parked offset and bounded per-batch memory — the property the
    width-bounding redesign must NOT regress. One parent, Children paged
    across three server pages, cap=1: each batch emits ~one page and parks a
    single continuation; every child arrives exactly once."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={
            "value": [
                {
                    "Id": 1,
                    "Name": "2024-01-01T00:00:00Z",
                    "Children": [{"Id": 101, "Label": "p1"}],
                    "Children@odata.nextLink": f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p2",
                }
            ]
        },
        match_querystring=False,
    )

    def children_cb(req):
        if "skiptoken=p2" in req.url:
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [{"Id": 102, "Label": "p2"}],
                        "@odata.nextLink": f"{SERVICE_URL}Parents(1)/Children?$skiptoken=p3",
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 103, "Label": "p3"}]}))  # last page

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=children_cb)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "1",
        "pagination": "nextlink",
    }
    got: list[int] = []
    offset: dict = {}
    max_pending = 0
    for _ in range(20):
        records, offset = c.read_table("Parents__Children", offset, opts)
        got.extend(r["Id"] for r in records)
        pending = offset.get("pending_fetches") or []
        max_pending = max(max_pending, len(pending))
        if not pending:
            break
    else:
        raise AssertionError("deep collection never drained")
    # O(1) offset — the parked frontier is one in-flight continuation, never
    # grows with the collection length.
    assert max_pending <= 2
    assert sorted(got) == [101, 102, 103]


@responses.activate
def test_capped_walk_watermark_survives_empty_resume_completion():
    """A truncated batch's max cursor must survive a resume that completes
    EMPTY: without running_max the checkpoint clear fell back to the old
    watermark and the stream re-read the same rows forever (period-2
    duplicate loop on a static source)."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})

    def children_cb(req):
        from urllib.parse import unquote

        url = unquote(req.url)
        rows = [
            {"Id": 11, "ModifiedAt": "2024-02-01T00:00:00Z"},
            {"Id": 12, "ModifiedAt": "2024-03-01T00:00:00Z"},
            {"Id": 13, "ModifiedAt": "2024-04-01T00:00:00Z"},
        ]
        m = re.findall(r"ModifiedAt gt (\S+?)[)&]", url + "&")
        if m:
            floor = max(m)
            rows = [r for r in rows if r["ModifiedAt"] > floor]
        return (200, {}, json.dumps({"value": rows}))  # NO nextLink

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents(1)/Children", callback=children_cb)
    c = _make()
    # Default pagination=auto: the cap fires inside the one full-collection
    # page and the synthesized keyset seek becomes the parked link.
    opts = {"cursor_field": "ModifiedAt", "max_records_per_batch": "3"}
    start = {"cursor": "2024-01-01T00:00:00Z"}
    recs1, offset1 = c.read_table("Parents__Children", start, opts)
    assert [r["Id"] for r in recs1] == [11, 12, 13]
    assert offset1.get("running_max") == "2024-04-01T00:00:00Z"
    # Resume: the parked seek returns nothing — the clear must FOLD the
    # accumulated max into the committed cursor, not fall back to the old
    # watermark (which replays the same three rows forever).
    recs2, offset2 = c.read_table("Parents__Children", offset1, opts)
    assert list(recs2) == []
    assert _drop_lb(offset2) == {"cursor": "2024-04-01T00:00:00Z"}


@responses.activate
def test_lookback_overlap_larger_than_cap_completes_and_idles():
    """Overlap re-reads (rows at-or-below the committed watermark) must not
    count toward max_records_per_batch: a lookback window holding >= cap
    rows otherwise wedges the stream into an eternal park/complete cycle
    that re-emits the same duplicates on every trigger and never reaches
    the end == start idle."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=_churn_children_cb(
            [
                {"Id": 11, "ModifiedAt": "2024-05-01T00:10:00Z"},
                {"Id": 12, "ModifiedAt": "2024-05-01T00:20:00Z"},
                {"Id": 13, "ModifiedAt": "2024-05-01T00:30:00Z"},
            ]
        ),
    )
    c = _make()
    watermark = "2024-05-01T00:30:00Z"
    opts = {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "2",  # smaller than the 3-row overlap
        "cursor_lookback_seconds": "3600",
        "pagination": "nextlink",
    }
    records, offset = c.read_table("Parents__Children", {"cursor": watermark}, opts)
    list(records)
    # The pure-overlap walk completes (no park) and idles at the watermark.
    assert _drop_lb(offset) == {"cursor": watermark}
    for stale in ("parent_idx", "parent_keys", "chain_next_link", "truncated_chain_cursor"):
        assert stale not in offset


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


@responses.activate
def test_cursor_field_not_on_any_segment_raises():
    """When cursor_field isn't a property anywhere along the contained
    path, the connector should raise with an actionable message."""
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not a property"):
        c.read_table("Parents__Children__Notes", None, {"cursor_field": "DoesNotExist"})


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
def test_lookup_in_type_only_namespace_lists_namespaces_with_entity_sets():
    """When the user picks a type-only namespace (no <EntityContainer>),
    "Available in this namespace: []" is unhelpful. The error should
    list the namespaces that DO contain entity sets so the user can
    pick the right one."""
    type_only_xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="My.Types.V1">
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
    </Schema>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="My.Service.V1">
      <EntityContainer Name="Container">
        <EntitySet Name="Things" EntityType="My.Types.V1.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    responses.get(f"{SERVICE_URL}$metadata", body=type_only_xml, status=200)
    c = _make()
    with pytest.raises(
        ValueError,
        match=r"declares no entity sets.*Namespaces with entity sets:.*My\.Service\.V1",
    ):
        c.read_table_metadata("Things", {"namespace": "My.Types.V1"})


# ---------------------------------------------------------------------------
# Partitioning (SupportsPartitionedStream)
# ---------------------------------------------------------------------------


@responses.activate
def test_partition_is_partitioned_rejects_flat_table():
    """Flat tables aren't partitioned — we'd be partitioning a single
    keyspace without distribution info."""
    _mock_nested_metadata()
    c = _make()
    assert c.is_partitioned("Parents") is False


@responses.activate
def test_partition_is_partitioned_rejects_expand_contained():
    """expand_contained does the whole table in one HTTP — no fan-out."""
    _mock_nested_metadata()
    c = _make({"expand_contained": "true"})
    assert c.is_partitioned("Parents__Children") is False


@responses.activate
def test_partition_is_partitioned_accepts_contained_snapshot():
    """Contained N+1 snapshot reads are the prime partition target."""
    _mock_nested_metadata()
    c = _make()
    assert c.is_partitioned("Parents__Children") is True


@responses.activate
def test_partition_get_partitions_bin_packs_contained_snapshot():
    """Snapshot batch path: top-level rows are bin-packed across
    ``num_partitions`` descriptors, each carrying its slice of parents."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": i} for i in range(1, 9)]},
    )
    c = _make()
    parts = c.get_partitions("Parents__Children", {"num_partitions": "4"})
    assert len(parts) == 4
    # Slices contiguous and exhaustive.
    flat = [row for p in parts for row in p["top_parent_rows"]]
    assert [r["Id"] for r in flat] == list(range(1, 9))


@responses.activate
def test_partition_get_partitions_applies_filter_at_top():
    """``filter_at_<top>`` (or its lowercased form from the framework)
    is applied to the partition pre-fetch so we don't bin-pack — and
    later walk — parents the user explicitly excluded."""
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
    c = _make()
    parts = c.get_partitions(
        "Parents__Children",
        {"num_partitions": "4", "filter_at_Parents": "Id eq 5"},
    )
    flat = [row for p in parts for row in p["top_parent_rows"]]
    assert [r["Id"] for r in flat] == [5]


@responses.activate
def test_partition_read_partition_applies_filter_at_leaf():
    """``filter_at_<leaf>`` is applied at the leaf URL inside the
    partitioned walk, not just in the non-partitioned snapshot path."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1000", "$filter": "Label eq 'a'", "$orderby": "Id asc"}
            )
        ],
    )
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []})
    c = _make()
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": None}
    rows = list(
        c.read_partition("Parents__Children", partition, {"filter_at_Children": "Label eq 'a'"})
    )
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_partition_read_partition_walks_only_assigned_parents():
    """Executor never fetches level-0 leaves outside its partition.
    Parents(99)/Children is deliberately unregistered — if the
    partition walker over-fetches the test fails."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 99}]})
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": None}
    rows = list(c.read_partition("Parents__Children", partition, {}))
    assert len(rows) == 1
    assert rows[0]["Id"] == 11
    # Verify no Parents(99)/Children call was made.
    leaf_urls = [c.request.url for c in responses.calls]
    assert not any("Parents(99)" in u for u in leaf_urls)


@responses.activate
def test_partition_empty_descriptor_falls_back_to_read_table():
    """get_partitions returns ``[{}]`` for flat tables; read_partition
    on that descriptor must produce the same rows as serial read_table."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "Name": "x"}]},
        match_querystring=False,
    )
    c = _make()
    rows = list(c.read_partition("Customers", {}, {}))
    # ``ModifiedAt`` padded to None (declared column the mock omitted).
    assert rows == [{"Id": 1, "Name": "x", "ModifiedAt": None}]


@responses.activate
def test_partition_latest_offset_probes_top_level_max_cursor():
    """In streaming mode the fence comes from a single
    ``?$top=1&$orderby=<cursor> desc`` probe against the top set."""
    _mock_nested_metadata()

    # Filter-aware: the round-45 desc self-check asks for ``Name gt 'z'``
    # and must see an empty set on this compliant server.
    def _parents(request):
        from urllib.parse import unquote as _unq

        if "gt" in _unq(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 9, "Name": "z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name"},
        None,
    )
    assert _drop_lb(offset) == {"cursor": "z"}


@responses.activate
def test_partition_latest_offset_snapshot_returns_wall_clock():
    """Without a cursor_field, snapshot streams advance via wall-clock
    epoch so Spark sees fresh end != start and triggers each batch."""
    _mock_nested_metadata()
    c = _make()
    offset = c.latest_offset("Parents__Children", {}, None)
    assert "snapshot_id" in offset
    assert isinstance(offset["snapshot_id"], int)


@responses.activate
def test_partition_get_partitions_empty_when_offsets_equal():
    """Streaming: when start_offset == end_offset Spark expects an
    empty partition list — no work to do."""
    _mock_nested_metadata()
    c = _make()
    parts = c.get_partitions(
        "Parents__Children",
        {"cursor_field": "Name"},
        {"cursor": "z"},
        {"cursor": "z"},
    )
    assert parts == []


@responses.activate
def test_partition_fence_probe_scopes_to_top_level_filter():
    """The ``latest_offset`` fence must be the max over the SAME population
    the read walks — ``filter_at_<top>`` rows only, non-null cursors first.
    An unfiltered probe fences past the filtered population's max (a fresher
    row OUTSIDE the filter), permanently skipping any filtered-in row that
    later lands at-or-below that fence (``cursor gt fence`` excludes it on
    every subsequent batch). The matcher below IS the assertion: a probe
    without this exact ``$filter`` finds no registered response."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 5, "Name": "2024-05-01T00:00:00Z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1",
                    "$select": "Name",
                    "$filter": "(Id eq 5) and (Name ne null)",
                    "$orderby": "Name desc",
                }
            )
        ],
    )
    # The round-45 desc self-check keeps the population filter and asks for
    # anything above the probed max (typed literal — Edm.String quotes).
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": []},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1",
                    "$select": "Name",
                    "$filter": "(Id eq 5) and (Name gt '2024-05-01T00:00:00Z')",
                }
            )
        ],
    )
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name", "filter_at_Parents": "Id eq 5"},
        None,
    )
    assert offset == {"cursor": "2024-05-01T00:00:00Z"}


@responses.activate
def test_partition_fence_probe_retries_without_null_guard_on_400():
    """A backend that rejects the ``ne null`` comparison (400) gets one
    retry without the null guard, so the hardening never breaks a stream
    that worked before it existed."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"error": {"message": "null comparison not supported"}},
        status=400,
        match=[
            responses.matchers.query_param_matcher(
                {
                    "$top": "1",
                    "$select": "Name",
                    "$filter": "Name ne null",
                    "$orderby": "Name desc",
                }
            )
        ],
    )
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 9, "Name": "z"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1", "$select": "Name", "$orderby": "Name desc"}
            )
        ],
    )
    # Round-45 desc self-check: nothing above the probed max.
    responses.get(
        f"{SERVICE_URL}Parents",
        json={"value": []},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1", "$select": "Name", "$filter": "Name gt 'z'"}
            )
        ],
    )
    c = _make()
    offset = c.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)
    assert offset == {"cursor": "z"}


@responses.activate
def test_partition_latest_offset_never_regresses_fence():
    """Replica lag / deletion of the max row must not move the committed
    fence backwards: the docstring's monotonic-progression promise is what
    makes ``cursor gt fence`` a safe dedup boundary."""
    _mock_nested_metadata()

    def _parents(request):
        from urllib.parse import unquote as _unq

        # Round-45 desc self-check: nothing above the probed max.
        if "gt" in _unq(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "2024-01-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name"},
        {"cursor": "2024-06-01T00:00:00Z"},
    )
    assert offset == {"cursor": "2024-06-01T00:00:00Z"}


@responses.activate
def test_partition_lookback_floors_read_boundary_not_fence():
    """``cursor_lookback_seconds`` must floor the partitioned READ boundary
    (discovery filter + descriptor ``cursor_lower``) — it was silently
    ignored on this path, leaving the probe→discovery race with no overlap
    protection. The committed fence itself is never floored."""
    from urllib.parse import unquote

    _mock_nested_metadata()

    def _parents(request):
        # Filter-aware: the fenced-batch ``eq null`` guard probe must see
        # no null-cursor parents (a filter-blind mock would feed the probe
        # the discovery rows and false-positive the guard).
        if "eq null" in unquote(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "2024-05-01T00:05:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    parts = c.get_partitions(
        "Parents__Children",
        {"cursor_field": "Name", "cursor_lookback_seconds": "600"},
        {"cursor": "2024-05-01T00:10:00Z"},
        {"cursor": "2024-05-01T00:20:00Z"},
    )
    # Descriptors carry the FLOORED boundary so every executor re-scans
    # the overlap window.
    assert parts
    assert all(p["cursor_lower"] == "2024-05-01T00:00:00Z" for p in parts)
    # And the discovery fetch used the floored boundary on the wire —
    # QUOTED: the cursor is declared Edm.String, and typed rendering
    # (round 41) quotes string literals even when they look like ISO
    # timestamps (the bare sniff form 400s strict servers).
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any("Name gt '2024-05-01T00:00:00Z'" in u for u in urls)


@responses.activate
def test_partition_num_partitions_garbage_rejected():
    """Garbage ``num_partitions`` must fail fast with a curated error —
    a bare ``int()`` crash is swallowed by the batch planner, which then
    silently degrades to a serial read."""
    _mock_nested_metadata()
    c = _make({"num_partitions": "abc"})
    with pytest.raises(ValueError, match="num_partitions"):
        c.is_partitioned("Parents__Children")
    c2 = _make()
    with pytest.raises(ValueError, match="num_partitions"):
        c2.get_partitions("Parents__Children", {"num_partitions": "0"})


@responses.activate
def test_partition_leaf_cursor_refilter_is_chronological_not_lexical():
    """The leaf-level client-side re-filter must compare cursor text
    chronologically: ``…00.5Z`` is NEWER than a ``…00Z`` boundary but
    lexically smaller (``.`` < ``Z``), so the old raw ``<=`` silently
    dropped exactly the newest rows."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={
            "value": [
                {"Id": 11, "Label": "new", "ModifiedAt": "2024-01-01T23:00:00.5Z"},
                {"Id": 12, "Label": "old", "ModifiedAt": "2024-01-01T22:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": "2024-01-01T23:00:00Z"}
    rows = list(c.read_partition("Parents__Children", partition, {"cursor_field": "ModifiedAt"}))
    assert [r["Id"] for r in rows] == [11]


@responses.activate
def test_partition_read_partition_resets_stale_ancestor_exclusions():
    """``read_partition`` never routes through ``read_table``'s
    ``exclude_ancestor_columns`` reset, so a stale exclusion from another
    table on a shared instance would silently strip this table's FK
    columns (declared non-nullable → hard parse failure downstream)."""
    _mock_nested_metadata()
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    c._excluded_ancestor_columns = frozenset({"Parents_Id"})  # stale, another table's
    partition = {"top_parent_rows": [{"Id": 1}], "cursor_lower": None}
    rows = list(c.read_partition("Parents__Children", partition, {}))
    assert rows and rows[0]["Parents_Id"] == 1


def test_expand_verdict_key_is_namespace_qualified():
    """The same contained path string can resolve to differently-shaped
    types in two namespaces of one service — mirroring
    ``_cursor_probe_shared_key``, the ``expand_ok`` verdict key must be
    namespace-qualified so one namespace's pass can't skip the other's
    preflight (and get baked into its offset)."""
    c = _make()
    assert c._expand_shared_key("Customers__Addresses", {"namespace": "Sales"}) == (
        "Sales:Customers__Addresses"
    )
    assert c._expand_shared_key("Customers__Addresses", {}) == "Customers__Addresses"
    c._seed_capability_caches(
        "Customers__Addresses", {"namespace": "Sales"}, {"cursor": "x", "expand_ok": True}
    )
    _, off_hr = c._with_capabilities(
        ([], {"cursor": "y"}), {"namespace": "HR"}, "Customers__Addresses"
    )
    assert "expand_ok" not in off_hr
    _, off_sales = c._with_capabilities(
        ([], {"cursor": "y"}), {"namespace": "Sales"}, "Customers__Addresses"
    )
    assert off_sales.get("expand_ok") is True


@responses.activate
def test_structured_values_emitted_as_json_not_python_repr():
    """Complex-typed / collection values map to string columns, and the
    framework stringifies via ``str()`` — a Python repr downstream
    ``from_json`` can't parse. The connector renders structured values as
    JSON at the emit boundary instead."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {
                    "Id": 1,
                    "Name": "x",
                    "Address": {"City": "Y", "Zip": 10001},
                    "Tags": ["a", "b"],
                }
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", None, {})
    row = next(iter(records))
    assert row["Address"] == '{"City":"Y","Zip":10001}'
    assert row["Tags"] == '["a","b"]'
    assert json.loads(row["Address"]) == {"City": "Y", "Zip": 10001}
    assert row["Id"] == 1  # scalars untouched


# ---------------------------------------------------------------------------
# 429 / 503 retry with backoff
# ---------------------------------------------------------------------------


def _patch_sleep(monkeypatch):
    """Capture every ``time.sleep`` call from the connector retry loop.

    Returns the list the sleeps are appended into — tests assert on
    durations directly. The lambda short-circuits the real sleep so the
    suite stays sub-second. Backoff jitter is pinned to its upper bound
    (``random.uniform → 1.0``) so the captured durations are the
    deterministic exponential sequence (1, 2, 4 …); jitter itself is
    covered by ``test_backoff_delay_is_jittered``.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.time.sleep",
        lambda s: sleeps.append(s),
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.random.uniform",
        lambda a, b: b,
    )
    return sleeps


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
def test_500_exhausted_error_message_calls_out_request_shape(monkeypatch):
    """After ``max_retries`` consecutive 500s, the raised RuntimeError
    must mention that a deterministic 500 likely points at a request
    shape the source can't handle — e.g. ``$top`` above SCApi's
    per-page cap — and surface the server response body. Without this
    hint the operator chases retry-budget knobs instead of the actual
    cause."""
    _mock_metadata()
    _patch_sleep(monkeypatch)
    server_body = (
        '{"error":{"code":"500","message":"Unexpected server failure. '
        'Error ID: [2026-06-24T05:16:46Z]."}}'
    )
    for _ in range(3):  # max_retries=2 → 3 attempts total
        responses.add(
            responses.GET,
            f"{SERVICE_URL}Customers",
            body=server_body,
            status=500,
        )
    c = _make({"token": "t", "max_retries": "2"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(RuntimeError) as ei:
        list(rows)
    msg = str(ei.value)
    assert "500" in msg
    assert "page_size" in msg  # remediation hint
    assert "Unexpected server failure" in msg  # body echoed


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
def test_verbose_http_logging_off_by_default_no_info_logs(caplog):
    """Without ``verbose_http_logging=true``, per-request INFO logs
    must not appear. Diagnostic noise should be opt-in — every request
    in a streaming pipeline shouldn't flood the log stream by
    default."""
    import logging as _logging

    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": [{"Id": 1}]})
    c = _make({"token": "t"})
    with caplog.at_level(
        _logging.INFO, logger="databricks.labs.community_connector.sources.odata.odata"
    ):
        rows, _ = c.read_table("Customers", None, {})
        list(rows)
    info_lines = [r.getMessage() for r in caplog.records if r.levelno == _logging.INFO]
    assert not any("OData GET" in m for m in info_lines)


@responses.activate
def test_verbose_http_logging_on_emits_request_and_response(caplog):
    """``verbose_http_logging=true`` emits one INFO line per request
    URL and one INFO line per response (status + body snippet). Used
    for triaging silent partial-data or under-row-count problems
    against flaky upstream sources."""
    import logging as _logging

    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 42, "Name": "Acme"}]},
    )
    c = _make({"token": "t", "verbose_http_logging": "true"})
    with caplog.at_level(
        _logging.INFO, logger="databricks.labs.community_connector.sources.odata.odata"
    ):
        rows, _ = c.read_table("Customers", None, {})
        list(rows)
    messages = [r.getMessage() for r in caplog.records]
    # Outgoing request URL line.
    assert any("OData GET" in m and "/Customers" in m for m in messages)
    # Response line includes status + body snippet (we just need the
    # source row to be visible somewhere in the log stream).
    assert any("→ 200" in m for m in messages)
    assert any('"Id": 42' in m or "Id': 42" in m or "Acme" in m for m in messages)


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
def test_json_decode_error_exhausted_includes_body_in_message(monkeypatch):
    """After max_retries exhausted JSON decode failures, the raised
    JSONDecodeError must include the offending URL + a truncated
    response body so the operator can escalate to the upstream owner
    with concrete evidence — not just the bare "Expecting property
    name" parser message."""
    import requests as _requests

    _mock_metadata()
    _patch_sleep(monkeypatch)
    body = "{<unexpected-html-error-page-from-proxy>"
    for _ in range(3):  # max_retries=2 → 3 attempts total
        responses.add(
            responses.GET,
            f"{SERVICE_URL}Customers",
            body=body,
            status=200,
            content_type="application/json",
        )
    c = _make({"token": "t", "max_retries": "2"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(_requests.exceptions.JSONDecodeError) as ei:
        list(rows)
    msg = str(ei.value)
    assert f"{SERVICE_URL}Customers" in msg
    assert "Server response body" in msg
    assert "<unexpected-html-error-page-from-proxy>" in msg


@responses.activate
def test_400_error_message_includes_server_body():
    """4xx that the retry layer doesn't handle (anything other than
    401/403/429/503) must surface the server's response body in the
    raised exception — otherwise downstream pipeline logs show a
    cryptic ``400 Client Error: Bad Request for url ...`` with no
    indication of *why* the server rejected the request."""
    import requests as _requests

    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"error": {"code": "BadRequest", "message": "Page size 1000 exceeds maximum 500"}},
        status=400,
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {})
    with pytest.raises(_requests.HTTPError) as ei:
        list(rows)
    msg = str(ei.value)
    assert "400" in msg
    assert "Page size 1000 exceeds maximum 500" in msg
    assert SERVICE_URL in msg


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


# ---------------------------------------------------------------------------
# cursor_probe — sparse-change optimization for deep leaf-cursor reads
# ---------------------------------------------------------------------------

PROBE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Probe" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Mids" Type="Collection(Probe.Mid)" ContainsTarget="true"/>
        <NavigationProperty Name="Plains" Type="Collection(Probe.Plain)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
        <Property Name="MidOnly" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Leaves" Type="Collection(Probe.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="Plain">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Items" Type="Collection(Probe.Item)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Roots" EntityType="Probe.Root"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

PROBE_TABLE = "Roots__Mids__Leaves"


def _mock_probe_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=PROBE_METADATA_XML, status=200)


def _skip_probe_preflight(c, table=PROBE_TABLE):
    """Pre-seed the cursor_probe capability cache as verified, so a test can
    exercise probe READ behaviour without also mocking the preflight requests.
    The preflight itself is covered by dedicated tests."""
    segs = tuple(table.split("__"))
    # Cache value is ``(problem, conclusive, race_tainted)``: no problem,
    # conclusively verified, no race contamination.
    c.__dict__.setdefault("_cursor_probe_verified", {})[(segs, None)] = (None, True, False)


def _probe_filter_floor(request):
    """Parse the ``RecordLastModified gt <iso>`` floor from a request's
    ``$filter`` (ISO timestamps go on the wire bare). ``None`` when no
    cursor floor is present (first batch)."""
    from urllib.parse import parse_qs, unquote, urlparse

    flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
    m = re.search(r"RecordLastModified gt ([0-9T:\-.Z]+)", flt)
    return m.group(1) if m else None


@responses.activate
def test_cursor_probe_hydrates_only_dirty_parents():
    """The probe issues one shallow ``$expand($orderby=cursor desc;$top=1)`` per
    leaf-grandparent tuple, reads each leaf-parent's newest leaf, and hydrates
    ONLY those whose newest leaf cursor is > since. Clean leaf-parents are never
    fetched (their hydrate URL is unregistered — a request would error)."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"

    # Level-0 enumeration of Roots (nextlink mode → short page is the end).
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}, {"Id": 2}]})
    # Probe per root returns each Mid's newest leaf cursor. Mid 10 + Mid 21 are
    # dirty (newest > since); 11 + 20 are clean (newest <= since).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids",
        json={
            "value": [
                {"Id": 20, "Leaves": [{"RecordLastModified": "2019-01-01T00:00:00Z"}]},
                {"Id": 21, "Leaves": [{"RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ]
        },
    )
    # Hydrate ONLY the dirty leaf-parents. Clean ones (Mids(11), Mids(20))
    # are deliberately left unregistered.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids(21)/Leaves",
        json={"value": [{"Id": 2101, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
    )

    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
            "expand_contained": "false",
        },
    )
    rows = list(recs)
    # Only the two dirty leaves, each with the full ancestor FK chain.
    assert sorted((r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows) == [
        (1, 10, 1001),
        (2, 21, 2101),
    ]
    # Watermark advanced to the global max leaf cursor.
    assert offset["cursor"] == "2020-07-01T00:00:00Z"
    # The probe orders the inner $expand by the cursor desc and takes top 1 —
    # the max-cursor leaf by construction, with NO inner $filter to mis-order.
    from urllib.parse import unquote

    probe_calls = [unquote(c.request.url) for c in responses.calls if "/Mids?" in c.request.url]
    assert probe_calls
    for u in probe_calls:
        assert (
            "$expand=Leaves($orderby=RecordLastModified desc;$top=1;$select=RecordLastModified)"
            in u
        )
        assert "$filter=" not in u.split("$expand=", 1)[1]  # no inner filter
    # No hydrate request was ever made for a clean leaf-parent.
    hydrate_urls = [c.request.url for c in responses.calls if "/Leaves" in c.request.url]
    assert not any("Mids(11)" in u or "Mids(20)" in u for u in hydrate_urls)


@responses.activate
def test_cursor_probe_first_batch_no_watermark_reads_all():
    """With no committed cursor yet (first batch, since=None) the probe is
    bypassed entirely — it would mark every leaf-parent dirty, so its
    per-grandparent ``$expand`` round-trips prune nothing. The connector falls
    back to the plain N+1 enumerator: identical full set, no probe requests."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2020-05-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(11)/Leaves",
        json={"value": [{"Id": 1101, "RecordLastModified": "2020-05-01T00:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {},  # streaming first batch: no cursor
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert sorted(r["Id"] for r in rows) == [1001, 1101]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # First batch bypasses the probe: no inner ``$expand`` round-trips at all.
    assert not any("%24expand" in call.request.url for call in responses.calls)


@responses.activate
def test_cursor_probe_resumes_across_cap_with_dirty_chain_iterator():
    """The injected dirty-chain iterator composes with the leaf-cursor cap /
    ``parent_idx`` resume: with ``max_records_per_batch=1`` over two dirty
    parents (two distinct-cursor leaves each), driving ``read_table`` to
    completion captures every changed leaf exactly once with its FK chain,
    re-probing skipped parents on each resumed batch."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    all_leaves = {
        "Roots(1)/Mids(10)/Leaves": [
            {"Id": 1001, "RecordLastModified": "2020-03-01T00:00:00Z"},
            {"Id": 1002, "RecordLastModified": "2020-04-01T00:00:00Z"},
        ],
        "Roots(2)/Mids(21)/Leaves": [
            {"Id": 2101, "RecordLastModified": "2020-05-01T00:00:00Z"},
            {"Id": 2102, "RecordLastModified": "2020-06-01T00:00:00Z"},
        ],
    }

    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=lambda r: (200, {}, json.dumps({"value": [{"Id": 1}, {"Id": 2}]})),
    )
    # Probe returns each Mid's newest leaf cursor (max over its leaves) — both
    # exceed `since`, so both stay dirty across every re-probe on resume.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=lambda r: (
            200,
            {},
            json.dumps(
                {"value": [{"Id": 10, "Leaves": [{"RecordLastModified": "2020-04-01T00:00:00Z"}]}]}
            ),
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(2)/Mids",
        callback=lambda r: (
            200,
            {},
            json.dumps(
                {"value": [{"Id": 21, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]}]}
            ),
        ),
    )

    def _make_leaf_cb(path):
        def _cb(request):
            floor = _probe_filter_floor(request)
            leaves = [
                l for l in all_leaves[path] if floor is None or l["RecordLastModified"] > floor
            ]
            return (200, {}, json.dumps({"value": leaves}))

        return _cb

    for path in all_leaves:
        responses.add_callback(responses.GET, f"{SERVICE_URL}{path}", callback=_make_leaf_cb(path))

    c = _make()
    _skip_probe_preflight(c)
    opts = {
        "cursor_field": "RecordLastModified",
        "cursor_probe": "nested-expand",
        "pagination": "nextlink",
        "max_records_per_batch": "1",
        # Dedup off: this test pins the probe walk's strict exactly-once
        # capped-resume guarantee; default-on dedup re-delivers the overlap
        # once after a capped cycle (documented MERGE-idempotent re-emit).
        "cursor_lookback_dedup": "off",
    }
    offset = {"cursor": since}
    seen = []
    for _ in range(30):
        recs, offset = c.read_table(PROBE_TABLE, offset, opts)
        batch = list(recs)
        if not batch:
            break
        seen.extend(batch)
    # Every changed leaf captured exactly once, with the full FK chain.
    assert sorted((r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in seen) == [
        (1, 10, 1001),
        (1, 10, 1002),
        (2, 21, 2101),
        (2, 21, 2102),
    ]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"


@responses.activate
def test_cursor_probe_invalid_value_raises():
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid cursor_probe"):
        c.read_table(
            PROBE_TABLE,
            {},
            {"cursor_field": "RecordLastModified", "cursor_probe": "maybe"},
        )


@responses.activate
def test_cursor_probe_conflicts_with_expand_contained():
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="conflicts with expand_contained"):
        c.read_table(
            PROBE_TABLE,
            {},
            {
                "cursor_field": "RecordLastModified",
                "cursor_probe": "nested-expand",
                "expand_contained": "true",
            },
        )


@responses.activate
def test_cursor_probe_on_flat_table_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="only on contained-collection paths"):
        c.read_table(
            "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_probe": "nested-expand"}
        )


@responses.activate
def test_cursor_probe_without_cursor_field_raises():
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="requires a cursor_field"):
        c.read_table(PROBE_TABLE, {}, {"cursor_probe": "nested-expand"})


@responses.activate
def test_cursor_probe_with_ancestor_cursor_raises():
    """``MidOnly`` lives on the Mid ancestor, not the leaf — cursor_probe only
    accelerates leaf-owned cursors, so it must reject an ancestor cursor."""
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="requires cursor_field on the leaf"):
        c.read_table(
            PROBE_TABLE,
            {},
            {"cursor_field": "MidOnly", "cursor_probe": "nested-expand"},
        )


@responses.activate
def test_cursor_probe_explicit_raises_when_leaf_parent_is_snapshot():
    """``Roots__Plains__Items``: 3 segments, but the leaf-parent ``Plains`` is a
    batch-snapshot level (no cursor field) — distance from the leaf to the
    nearest snapshot ancestor is 1, so the probe can't save work. The exact
    ``Instances/Projects/WorkPackageDetails`` shape: an explicit opt-in is
    rejected (depth alone does not qualify a path)."""
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="batch-snapshot level"):
        c.read_table(
            "Roots__Plains__Items",
            {},
            {"cursor_field": "RecordLastModified", "cursor_probe": "nested-expand"},
        )


@responses.activate
def test_cursor_probe_default_inert_when_leaf_parent_is_snapshot():
    """Even default-on, a depth-3 path whose leaf-parent is snapshot
    (``Roots__Plains__Items``) is INAPPLICABLE — distance to the nearest
    snapshot ancestor is 1 — so it uses the plain N+1 leaf walk, issues NO
    ``$expand`` probe, and skips the preflight. Matches the user's
    ``Instances/Projects/WorkPackageDetails`` case."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Plains", json={"value": [{"Id": 5}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Plains(5)/Items",
        json={"value": [{"Id": 50, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    recs, offset = c.read_table(
        "Roots__Plains__Items",
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Plains_Id"], r["Id"]) for r in rows] == [(1, 5, 50)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert not any("%24expand" in call.request.url for call in responses.calls)


@responses.activate
def test_cursor_probe_default_on_engages_without_opt_in():
    """cursor_probe defaults to AUTO: on a probe-eligible deep path whose server
    honours inner-$expand ordering, the cascade uses the nested-$expand probe
    with no option set — the probe runs and only dirty leaf-parents are
    hydrated. (Preflight pre-seeded; covered separately.)"""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe: Mid 10 dirty (newest > since), Mid 11 clean (newest <= since).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        # No cursor_probe key — relies on the default (on).
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = list(recs)
    # Probe engaged: only the dirty Mid 10 hydrated; clean Mid 11 skipped.
    assert [(r["Mids_Id"], r["Id"]) for r in rows] == [(10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert any("$expand=Leaves" in c.request.url for c in responses.calls)
    assert not any("Mids(11)/Leaves" in c.request.url for c in responses.calls)


@responses.activate
def test_cursor_probe_skips_parent_whose_newest_leaf_predates_watermark():
    """The client-side comparison is what fixes the original bug: a leaf-parent
    that HAS leaves but whose newest leaf cursor is <= since must be skipped.
    Mid 10's newest leaf is newer than since (dirty → hydrated); Mid 11 has a
    leaf but its newest predates since (clean → never hydrated)."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-12-31T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Mids_Id"], r["Id"]) for r in rows] == [(10, 1001)]
    # Mid 11 has a leaf, but its newest predates `since` → never hydrated.
    assert not any("Mids(11)/Leaves" in c.request.url for c in responses.calls)


@responses.activate
def test_cursor_probe_default_inert_on_two_segment_path():
    """Even default-on, ``Roots__Mids`` is INAPPLICABLE (the leaf-parent
    ``Roots`` is a snapshot level, distance 1), so it uses the plain N+1 leaf
    walk, issues NO ``$expand`` probe, and skips the preflight."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    recs, offset = c.read_table(
        "Roots__Mids",
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Id"]) for r in rows] == [(1, 10)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # No probe: the standard leaf walk never emits an $expand.
    assert not any("%24expand" in call.request.url for call in responses.calls)


def _probe_mids_callback(inner_expand_newest):
    """Callback for ``Roots(1)/Mids``: returns Mid 10's key for the preflight's
    leaf-parent enumeration, and Mid 10 with a probe-shaped ``Leaves`` whose
    newest cursor is ``inner_expand_newest`` for the inner-$expand check."""

    def _cb(request):
        from urllib.parse import unquote

        if "$expand=Leaves" in unquote(request.url):
            return (
                200,
                {},
                json.dumps(
                    {"value": [{"Id": 10, "Leaves": [{"RecordLastModified": inner_expand_newest}]}]}
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 10}]}))

    return _cb


def _mids_reject_expand_callback(request):
    """Callback for ``Roots(1)/Mids``: 400 on the nested-``$expand`` probe (a
    server that rejects inner ``$orderby``/``$top``/``$select``, e.g. Hexagon
    Smart API), and a plain Id list for the N+1 enumeration / fallback."""
    from urllib.parse import unquote

    if "$expand=Leaves" in unquote(request.url):
        return (400, {}, json.dumps({"error": {"message": "inner $expand not supported"}}))
    return (200, {}, json.dumps({"value": [{"Id": 10}]}))


@responses.activate
def test_cursor_probe_preflight_passes_when_inner_orderby_honored():
    """The capability check passes (no raise, cached verified) when the inner
    ``$expand($orderby desc;$top=1)`` returns the same newest leaf as trusted
    direct navigation."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),  # matches direct max
    )
    # Direct-nav desc top2: two distinct cursors → discriminating, true max 2020-09.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {"page_size": "1000"}, "RecordLastModified"
    )
    # Conclusive pass: the discriminating sample's inner-$expand matched the
    # trusted direct-nav max, so the caller may persist the verdict.
    assert supported is True
    assert conclusive is True
    assert c.__dict__["_cursor_probe_verified"][(("Roots", "Mids", "Leaves"), None)] == (
        None,
        True,
        False,
    )


@responses.activate
def test_cursor_probe_misorder_verdict_shared_across_instances():
    """Under the ``auto`` cascade a mis-order FAIL rides the process cache —
    the offset only ever carries the pass, so without this a mis-ordering
    server would re-pay the preflight GETs on every recreated reader."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # NOT the true max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c1 = _make()
    assert c1._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert c1._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is False
    n_before = len(responses.calls)
    c2 = _make()
    assert c2._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert len(responses.calls) == n_before  # no preflight re-run


@responses.activate
def test_cursor_probe_conclusive_pass_shared_across_instances():
    """A conclusive pass reaches a fresh instance through the process cache
    even with no offset to carry ``cursor_probe_ok``."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),  # matches direct max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c1 = _make()
    assert c1._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (True, True)
    n_before = len(responses.calls)
    c2 = _make()
    assert c2._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (True, True)
    assert len(responses.calls) == n_before


@responses.activate
def test_cursor_probe_strict_ignores_shared_cache():
    """Strict mode (explicit ``cursor_probe=nested-expand``) neither trusts nor
    writes the shared cache: a cached False doesn't spare the probe (it runs
    and passes on this healthy server), and the strict pass doesn't overwrite
    the recorded verdict — explicit modes keep no recorded state."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    c._store_capability("cursor_probe_ok", False, table_name="Roots__Mids__Leaves")
    n_before = len(responses.calls)
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
    ) == (True, True)
    assert len(responses.calls) > n_before  # the probe really ran
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is False


@responses.activate
def test_cursor_probe_strict_raises_despite_cached_pass():
    """The inverse: a cached True must not let strict mode skip the probe — a
    genuinely mis-ordering server still raises with fresh evidence."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # NOT the true max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    c._store_capability("cursor_probe_ok", True, table_name="Roots__Mids__Leaves")
    with pytest.raises(ValueError, match=r"honour \$orderby/\$top inside \$expand"):
        c._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
        )


@responses.activate
def test_cursor_probe_auto_cascades_when_server_rejects_expand_probe():
    """A server that REJECTS the nested-``$expand`` probe with an HTTP error
    (not a silent mis-order — e.g. Hexagon Smart API 400s on inner-``$expand``
    options) must make ``auto`` **cascade**, not raise: the preflight returns
    ``(False, False)`` and records a definitive ``cursor_probe_ok=False`` instead
    of letting the raw HTTP error escape and fail the read."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_reject_expand_callback
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is False


@responses.activate
def test_cursor_probe_strict_raises_actionable_when_server_rejects_expand_probe():
    """Strict ``nested-expand`` surfaces a ``$expand`` REJECTION as an actionable
    ``ValueError`` (pointing at cursor_probe=batch/false) — not the raw HTTP
    error a bare fetch would let escape."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_reject_expand_callback
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    with pytest.raises(ValueError, match=r"rejected the probe query"):
        c._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
        )


@responses.activate
def test_cursor_probe_auto_read_succeeds_when_server_rejects_expand_probe():
    """End-to-end: ``read_table`` with ``cursor_probe=auto`` on a server that
    400s the nested-``$expand`` probe must **complete** via the N+1 fallback
    (rows emitted, no exception) — the bug was that the raw HTTP error escaped
    and failed the read. ``contained_fetch=single`` keeps the fallback a plain
    walk so no ``$batch`` mock is needed."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_reject_expand_callback
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"Id": 1001, "RecordLastModified": "2020-09-01T00:00:00Z"},
                {"Id": 1000, "RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "auto",
            "contained_fetch": "single",  # plain N+1 fallback (no $batch)
            "pagination": "nextlink",
        },
    )
    rows = list(recs)  # must not raise
    assert sorted(r["Id"] for r in rows) == [1000, 1001]
    assert offset["cursor"] == "2020-09-01T00:00:00Z"


@responses.activate
def test_cursor_probe_race_newer_leaf_is_skipped_not_failed():
    """A probe-shaped ``$expand`` newest NEWER than the direct-nav reference is
    a concurrent-write race (the two fetches aren't atomic), not mis-ordering
    evidence — a genuinely mis-ordering server returns an OLDER leaf. The
    sample is skipped (never a definitive fail, never a raise) and NOTHING is
    persisted. But unlike a genuinely non-discriminating scan, a verdict-less
    scan that contained a RACE skip DECLINES the probe for this batch
    (``(False, False)`` — cascade to $batch / the plain walk): the race may be
    hiding a mis-ordering server, and engaging unverified could drop rows
    behind the advancing watermark. The next batch re-checks."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-12-01T00:00:00Z"),  # NEWER than reference
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    # Race-tainted inconclusive: declined this batch, nothing persisted.
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is None
    assert c.__dict__["_cursor_probe_verified"][(("Roots", "Mids", "Leaves"), None)] == (
        None,
        False,
        True,
    )


@responses.activate
def test_cursor_probe_race_does_not_abort_scan_to_clean_sample():
    """A racing sample must not abort the scan: with the first leaf-parent
    racing (newer) and a second cleanly discriminating (probe returns the true
    newest), the preflight skips the racer, reaches the clean parent, and
    records a conclusive PASS — rather than being derailed by the race."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}, {"Id": 2}]})
    # Parent 1: probe-shaped $expand newest is NEWER than reference → race/skip.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-12-01T00:00:00Z"),
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    # Parent 2: probe newest MATCHES the reference max → clean conclusive pass.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(2)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (True, True)
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is True


@responses.activate
def test_cursor_probe_strict_does_not_raise_on_race():
    """Strict mode must not raise the pipeline on a transient concurrent-write
    race: a newer-than-reference sample is skipped, and with no other sample
    the race-tainted scan DECLINES the probe for this batch
    (``(False, False)`` — the read degrades to the $batch/plain cascade for
    one batch, rows identical) rather than raising OR engaging unverified."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-12-01T00:00:00Z"),  # NEWER → race/skip
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
    ) == (False, False)


@responses.activate
def test_cursor_probe_preflight_fetch_error_degrades_instead_of_raising():
    """A preflight that errors out BEFORE reaching a verdict — the trusted
    direct-navigation reference fetch 400s (e.g. a server that rejects
    ``$orderby … desc``/``$select`` on direct navigation) — must not escape a
    ``cursor_probe=auto`` read as a raw HTTP error. Unlike the probe-shape
    rejection (whose sibling fetches just succeeded → definitive), this is
    indistinguishable from a transient: non-strict degrades to the
    ``$batch``/plain cascade for THIS batch and records NOTHING (the next
    batch re-probes); strict raises an actionable message instead of the raw
    failure."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"error": {"message": "The query specified in the URI is not valid."}},
        status=400,
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    # Nothing cached or recorded anywhere — neither the instance cache nor the
    # shared capability cache — so the next batch re-probes.
    assert (("Roots", "Mids", "Leaves"), None) not in c.__dict__.get("_cursor_probe_verified", {})
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is None

    c2 = _make()
    with pytest.raises(ValueError, match="failed before reaching a verdict"):
        c2._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
        )


@responses.activate
def test_cursor_probe_preflight_programming_error_propagates(monkeypatch):
    """The never-raise contract covers HTTP/capability failures ONLY: the
    degrade-and-continue handler catches the dedicated fetch-failure type,
    not ``Exception`` — a latent programming error inside the preflight's
    own logic must surface, not silently pin the stream to the slow path."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})

    def _boom(self, *args, **kwargs):
        raise AttributeError("latent bug in preflight logic")

    monkeypatch.setattr(ODataLakeflowConnect, "_cursor_probe_check_sample", _boom)
    c = _make()
    with pytest.raises(AttributeError, match="latent bug"):
        c._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
        )


@responses.activate
def test_cursor_probe_read_table_raises_when_server_misorders_inner_expand():
    """Fail fast: when the inner ``$expand($orderby desc;$top=1)`` returns a
    non-newest leaf (server ignores inner ordering), read_table raises during
    the preflight instead of silently dropping rows."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # NOT the true max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    with pytest.raises(ValueError, match=r"honour \$orderby/\$top inside \$expand"):
        c.read_table(
            PROBE_TABLE,
            {"cursor": "2020-01-01T00:00:00Z"},
            {
                "cursor_field": "RecordLastModified",
                "cursor_probe": "nested-expand",
                "pagination": "nextlink",
            },
        )


@responses.activate
def test_cursor_probe_conclusive_pass_persists_ok_flag_in_offset():
    """Under ``cursor_probe=auto`` a conclusive preflight pass stamps
    ``cursor_probe_ok`` into the resume offset, so a per-batch-recreated reader
    can trust it next batch. (Non-``auto`` modes don't persist it — see
    ``test_nonauto_clears_recorded_preflight_verdicts``.)"""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe + preflight enumeration of Mid 10 (one dirty leaf-parent).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]}]},
    )
    # Preflight direct-nav reference: two distinct cursors → discriminating,
    # true max 2020-06 (matches the inner-$expand newest above → conclusive ok).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"},
                {"Id": 1000, "RecordLastModified": "2020-02-01T00:00:00Z"},
            ]
        },
    )
    c = _make()  # no _skip_probe_preflight: the real preflight runs and passes
    _, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "auto",
            "pagination": "nextlink",
        },
    )
    assert offset.get("cursor_probe_ok") is True


@responses.activate
def test_cursor_probe_offset_flag_skips_preflight_requests():
    """When the resume offset already carries ``cursor_probe_ok`` (set by an
    earlier batch), a freshly-constructed reader skips the preflight entirely —
    no direct-navigation capability requests are issued — and still hydrates
    only the dirty leaf-parent via the probe."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe: Mid 10 dirty, Mid 11 clean.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    # NOTE: Mids(11)/Leaves is left unregistered — the preflight would hit it
    # (direct-nav reference) if it ran; trusting the offset flag must avoid that.
    c = _make()  # cold instance cache; trust comes from the offset flag alone
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since, "cursor_probe_ok": True},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "auto",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Mids_Id"], r["Id"]) for r in rows] == [(10, 1001)]
    assert offset.get("cursor_probe_ok") is True
    # The preflight's direct-navigation reference query (``$orderby cursor
    # desc;$top=2``) was never issued — the only leaf fetch is Mid 10's hydrate
    # (ascending cursor walk, no ``desc``), and the clean Mid 11 is untouched.
    from urllib.parse import unquote

    leaf_calls = [
        unquote(call.request.url) for call in responses.calls if "/Leaves" in call.request.url
    ]
    assert leaf_calls == [
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves?$top=1000"
        "&$filter=RecordLastModified gt 2020-01-01T00:00:00Z&$orderby=RecordLastModified asc,Id asc"
    ]
    assert not any("desc" in u for u in leaf_calls)
    assert not any("Mids(11)" in u for u in leaf_calls)


@responses.activate
def test_cursor_probe_lookback_floors_filter_and_reincludes_overlap_parent():
    """cursor_probe utilises cursor_lookback: with a window set, the probe's
    dirty-detection AND the hydrate filter floor to (committed - window), so a
    leaf-parent whose newest leaf fell in the overlap (<= since, > read_since) is
    re-flagged dirty and re-hydrated — catching a mid-walk arrival. The committed
    watermark stays the TRUE max (never floored)."""
    _mock_probe_metadata()
    since = "2020-06-10T00:00:00Z"  # read_since = since - 1 day = 2020-06-09
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe newest-leaf per Mid: 10 new (> since), 11 in overlap (> read_since,
    # <= since), 12 below the window (clean).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-11T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2020-06-09T12:00:00Z"}]},
                {"Id": 12, "Leaves": [{"RecordLastModified": "2020-06-08T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-11T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(11)/Leaves",
        json={"value": [{"Id": 1101, "RecordLastModified": "2020-06-09T12:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
            "cursor_lookback_seconds": "86400",  # 1 day
        },
    )
    rows = list(recs)
    # Overlap parent (Mid 11) re-included; below-window parent (Mid 12) skipped.
    assert sorted((r["Mids_Id"], r["Id"]) for r in rows) == [(10, 1001), (11, 1101)]
    assert not any("Mids(12)/Leaves" in call.request.url for call in responses.calls)
    # Committed watermark = TRUE max, not floored.
    assert offset["cursor"] == "2020-06-11T00:00:00Z"
    # The hydrate filter floored to read_since (2020-06-09), not `since`.
    from urllib.parse import unquote

    hydrate = [unquote(c.request.url) for c in responses.calls if "/Mids(1" in c.request.url]
    assert hydrate and all("2020-06-09" in u for u in hydrate)
    assert not any("gt 2020-06-10" in u for u in hydrate)


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
# cursor_probe=batch — $batch hydrate fallback + auto cascade
# ---------------------------------------------------------------------------


def _batch_responder(route_map):
    """Build a ``responses`` POST callback for the OData ``$batch`` endpoint.

    ``route_map`` is a list of ``(url_substring, body_dict)`` pairs; for each
    posted sub-request the first substring that occurs in its ``url`` wins and
    its ``body`` is returned with sub-status 200 (404 + empty when none match).
    Records every posted sub-request URL on ``.seen`` for assertions."""
    seen: list[str] = []

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            url = r["url"]
            seen.append(url)
            body = next((b for sub, b in route_map if sub in url), None)
            status = 200 if body is not None else 404
            out.append({"id": r["id"], "status": status, "body": body or {}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    _cb.seen = seen
    return _cb


def _too_many_parts_responder(route_map, max_parts, message="contains too many parts"):
    """``$batch`` callback that rejects any POST carrying more than ``max_parts``
    sub-requests with a 400 carrying ``message`` (the adaptive-shrink trigger),
    and otherwise behaves like :func:`_batch_responder`. Records the sub-request
    count of each *accepted* POST on ``.accepted`` and the number of rejections
    on ``.rejections``."""
    seen: list[str] = []
    accepted: list[int] = []
    rejections = [0]

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        if len(reqs) > max_parts:
            rejections[0] += 1
            return (
                400,
                {"Content-Type": "application/json"},
                json.dumps({"error": {"message": message}}),
            )
        accepted.append(len(reqs))
        out = []
        for r in reqs:
            url = r["url"]
            seen.append(url)
            body = next((b for sub, b in route_map if sub in url), None)
            status = 200 if body is not None else 404
            out.append({"id": r["id"], "status": status, "body": body or {}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    _cb.seen = seen
    _cb.accepted = accepted
    _cb.rejections = rejections
    return _cb


@responses.activate
def test_cursor_probe_batch_hydrates_via_batch_endpoint():
    """``cursor_probe=batch`` skips the nested-$expand probe and hydrates the
    per-leaf-parent ``cursor gt since`` reads through OData ``$batch``: no probe
    ``$expand``, no per-leaf-parent GET, and ``batch_ok`` is persisted."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}, {"Id": 11}]})
    responder = _batch_responder(
        [
            # dirty leaf-parent → one changed leaf
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            # clean leaf-parent → server-filtered empty page
            ("Mids(11)/Leaves", {"value": []}),
            # $batch capability preflight
            ("Roots", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "batch",
            "pagination": "nextlink",
            "expand_contained": "false",
        },
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert offset.get("batch_ok") is True
    # No nested-$expand probe anywhere, and the leaf hydrate went through
    # $batch — never a per-leaf-parent GET to /Leaves.
    assert not any("$expand" in call.request.url for call in responses.calls)
    assert not any(
        call.request.method == "GET" and "/Leaves" in call.request.url for call in responses.calls
    )
    # Both leaf-parents were hydrated via the batch (filter pushed server-side).
    assert any("Mids(10)/Leaves" in u for u in responder.seen)
    assert any("Mids(11)/Leaves" in u for u in responder.seen)
    # No $top on the hydrate sub-requests (server-driven paging).
    assert not any("Mids(10)/Leaves" in u and "$top=" in u for u in responder.seen)


@responses.activate
def test_cursor_probe_batch_size_suffix_chunks_requests():
    """``cursor_probe=batch:2`` hydrates via ``$batch`` like ``batch`` but caps
    each request at 2 leaf-parent ops: 3 leaf-parents → rounds of 2 + 1."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}, {"Id": 11}, {"Id": 12}]},
    )
    responder = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            ("Mids(11)/Leaves", {"value": []}),
            (
                "Mids(12)/Leaves",
                {"value": [{"Id": 1201, "RecordLastModified": "2020-06-02T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch:2", "pagination": "nextlink"},
    )
    assert sorted(r["Id"] for r in recs) == [1001, 1201]
    # Hydrate $batch POSTs (those carrying /Leaves) are capped at 2 ops:
    # 3 leaf-parents, chunk size 2 → rounds of 2 then 1.
    op_counts = []
    for call in responses.calls:
        if call.request.method != "POST":
            continue
        reqs = json.loads(call.request.body)["requests"]
        if any("/Leaves" in r["url"] for r in reqs):
            op_counts.append(len(reqs))
    assert sorted(op_counts) == [1, 2]


@responses.activate
def test_cursor_probe_batch_size_invalid_suffix_raises():
    """A non-positive / non-integer ``:N`` suffix, or a suffix on a non-batch
    mode, is rejected before any network call."""
    _mock_probe_metadata()
    c = _make()
    for bad in ("batch:0", "batch:-1", "batch:abc", "auto:2", "nested-expand:5"):
        with pytest.raises(ValueError, match="Invalid cursor_probe"):
            c.read_table(
                PROBE_TABLE,
                {"cursor": "2020-01-01T00:00:00Z"},
                {"cursor_field": "RecordLastModified", "cursor_probe": bad},
            )


@responses.activate
def test_cursor_probe_batch_follows_nextlink_continuation():
    """A batched leaf sub-response carrying ``@odata.nextLink`` is re-batched
    until the collection drains — all pages are collected."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responder = _batch_responder(
        [
            # continuation page (matched first — more specific)
            (
                "$skiptoken=p2",
                {"value": [{"Id": 1002, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ),
            # first page emits a service-relative nextLink
            (
                "Mids(10)/Leaves",
                {
                    "value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    "@odata.nextLink": "Roots(1)/Mids(10)/Leaves?$skiptoken=p2",
                },
            ),
            ("Roots", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    rows = sorted(r["Id"] for r in recs)
    assert rows == [1001, 1002]  # both pages collected across batch rounds
    assert offset["cursor"] == "2020-07-01T00:00:00Z"
    assert any("$skiptoken=p2" in u for u in responder.seen)


@responses.activate
def test_cursor_probe_batch_falls_back_to_plain_walk_when_unsupported():
    """``cursor_probe=batch`` against a server that rejects ``$batch`` (405)
    degrades to the plain N+1 GET walk — never raises, rows still correct."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    # $batch unsupported.
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    # Plain N+1 leaf GET still serves the hydrate.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    rows = [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs]
    assert rows == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # Probed and found unsupported → persisted False (not True) so later
    # microbatches skip the probe and go straight to the plain walk.
    assert offset.get("batch_ok") is False
    # A real GET hydrate happened (plain walk fallback).
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_cursor_probe_auto_cascades_to_batch_when_server_misorders_inner_expand():
    """DEFAULT (unset → auto): when the probe preflight finds the server
    mis-orders inner ``$expand``, ``auto`` does NOT raise — it cascades to the
    ``$batch`` hydrate (drop-safe) and persists ``batch_ok``."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe enumeration: inner-$expand newest is WRONG (server mis-orders);
    # plain enumeration (no $expand) lists Mid 10 for the hydrate.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # not the true max
    )
    # Preflight direct-nav reference: 2 distinct cursors, true max 2020-09 →
    # discriminating, and != the inner-$expand newest → mis-order verdict.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    responder = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-09-01T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    # No cursor_probe key → default auto. Must NOT raise.
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs]
    assert rows == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-09-01T00:00:00Z"
    assert offset.get("batch_ok") is True
    # Cascaded: the leaf hydrate went through $batch, not the probe.
    assert any("Mids(10)/Leaves" in u for u in responder.seen)


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
def test_batch_subrequest_urls_are_percent_encoded():
    """Sub-request URLs ride inside the JSON ``$batch`` envelope and never
    pass through ``requests``' URL preparation — they must be pre-encoded
    the way requests would encode a plain GET (spaces → %20): a strict
    OData v4 server may reject a sub-request URL carrying raw spaces."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responder = _batch_responder(
        [
            ("Parents(1)/Children", {"value": [{"Id": 11, "Label": "a"}]}),
            ("Parents", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    assert [r["Id"] for r in recs] == [11]
    assert all(" " not in u for u in responder.seen), responder.seen
    # The leaf hydrate carries a stable $orderby — its space arrives as %20.
    assert any("%20" in u for u in responder.seen if "Children" in u)


@responses.activate
def test_batch_subresponse_transient_error_falls_back_to_plain_get():
    """A 2xx ``$batch`` envelope carrying one FAILED sub-response (a throttled
    leaf-parent, status 500) must not silently skip that parent's rows —
    ``rows = []`` with no error would be permanent loss on a cursor walk (the
    watermark advances past the failed parent). The drain re-issues the failed
    part as a plain GET and every row still arrives."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            url = r["url"]
            if "Parents(1)/Children" in url:
                out.append(
                    {"id": r["id"], "status": 200, "body": {"value": [{"Id": 11, "Label": "a"}]}}
                )
            elif "Parents(2)/Children" in url:
                out.append(
                    {"id": r["id"], "status": 500, "body": {"error": {"message": "throttled"}}}
                )
            else:  # capability preflight
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    # Plain-GET recovery target for the failed part.
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "b"}]},
        match_querystring=False,
    )

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    rows = sorted((r["Parents_Id"], r["Id"]) for r in recs)
    assert rows == [(1, 11), (2, 21)]  # nothing silently skipped
    assert any(
        call.request.method == "GET" and "Parents(2)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_batch_subresponse_hard_error_raises_instead_of_silent_skip():
    """A hard 4xx sub-response is re-issued as a plain GET, which raises with
    the server's actual error body — a failed part must surface, never quietly
    drop its parent's rows."""
    import requests as _requests

    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            if "Children" in r["url"]:
                out.append(
                    {"id": r["id"], "status": 400, "body": {"error": {"message": "bad filter"}}}
                )
            else:  # capability preflight
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    # The plain-GET re-issue hits the same 400 and raises with the body.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"error": {"message": "bad filter"}},
        status=400,
        match_querystring=False,
    )

    c = _make()
    with pytest.raises(_requests.exceptions.HTTPError, match="bad filter"):
        list(c.read_table("Parents__Children", {}, {"expand_contained": "false"})[0])


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
def test_batch_too_many_parts_shrinks_and_records_size():
    """When the server rejects a ``$batch`` with "too many parts", the connector
    shrinks the chunk size by 25% and retries until it fits, hydrates every
    leaf-parent, and records the discovered size in the offset (``batch_size_ok``)."""
    _mock_nested_metadata()
    parents = [{"Id": i} for i in range(1, 6)]  # 5 leaf-parents
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents})
    responder = _too_many_parts_responder(
        [(f"Parents({i})/Children", {"value": [{"Id": i * 10 + 1}]}) for i in range(1, 6)]
        + [("Parents", {"value": [{"Id": 1}]})],  # 1-part preflight (accepted)
        max_parts=2,
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})  # default contained_fetch=batch (1000)
    assert sorted(r["Id"] for r in recs) == [11, 21, 31, 41, 51]
    # Server rejected the oversized batch at least once, then every accepted
    # hydrate POST fit within the shrunk cap (<= 2 parts).
    assert responder.rejections[0] >= 1
    assert all(n <= 2 for n in responder.accepted)
    # The working size was discovered and recorded on the instance for reuse.
    # (The snapshot offset is built lazily before the generator runs, so the
    # persisted ``batch_size_ok`` is exercised by the cursor path below.)
    assert c.__dict__["_batch_size_cap"] == 2


@responses.activate
def test_batch_too_many_parts_falls_back_to_single_gets():
    """A server that rejects *any* multi-part ``$batch`` drives the cap down to 1
    and falls back to a plain per-leaf-parent GET — every row still arrives."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})
    responder = _too_many_parts_responder([("Parents", {"value": [{"Id": 1}]})], max_parts=1)
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)
    # Plain-GET fall-back targets.
    responses.get(
        f"{SERVICE_URL}Parents(1)/Children",
        json={"value": [{"Id": 11}]},
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21}]},
        match_querystring=False,
    )

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})
    assert sorted(r["Id"] for r in recs) == [11, 21]
    # Fell back to per-parent GETs for the leaf collections.
    assert any(
        call.request.method == "GET" and "Parents(1)/Children" in call.request.url
        for call in responses.calls
    )
    assert c.__dict__["_batch_size_cap"] == 1  # give-up sentinel
    # The plain-GET fall-back re-adds a $top (the $batch-shaped URL carries
    # none) so the client-driven drain under the default pagination=auto can
    # page a server that page-limits while omitting @odata.nextLink.
    assert all(
        "$top=" in call.request.url
        for call in responses.calls
        if call.request.method == "GET" and "/Children" in call.request.url
    )


@responses.activate
def test_batch_size_ok_seeded_from_offset_avoids_oversized_batch():
    """``batch_size_ok`` in the resume offset seeds the cap, so the connector
    chunks at that size from the first round — no oversized batch is attempted."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}, {"Id": 3}]})
    responder = _too_many_parts_responder(
        [(f"Parents({i})/Children", {"value": [{"Id": i * 10 + 1}]}) for i in range(1, 4)]
        + [("Parents", {"value": [{"Id": 1}]})],
        max_parts=2,
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    # Snapshot read seeds capability caches from start_offset.
    recs, _ = c.read_table("Parents__Children", {"batch_size_ok": 2}, {})
    assert sorted(r["Id"] for r in recs) == [11, 21, 31]
    # Never overflowed (chunked at the seeded cap from the start): no rejection.
    assert responder.rejections[0] == 0
    # Accepted POSTs: the 1-part capability preflight + two hydrate rounds (2 + 1).
    assert sorted(responder.accepted) == [1, 1, 2]


@responses.activate
def test_batch_too_many_parts_persists_size_in_cursor_offset():
    """The **eager** cursor-incremental ``$batch`` walk (``cursor_probe=batch``)
    discovers the working size on a "too many parts" rejection and records it in
    the resume offset (``batch_size_ok``) so the next microbatch reuses it."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}, {"Id": 11}, {"Id": 12}]},
    )
    responder = _too_many_parts_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            (
                "Mids(11)/Leaves",
                {"value": [{"Id": 1101, "RecordLastModified": "2020-06-02T00:00:00Z"}]},
            ),
            (
                "Mids(12)/Leaves",
                {"value": [{"Id": 1201, "RecordLastModified": "2020-06-03T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # capability preflight
        ],
        max_parts=2,
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    assert sorted(r["Id"] for r in recs) == [1001, 1101, 1201]
    assert responder.rejections[0] >= 1
    assert all(n <= 2 for n in responder.accepted)
    # Eager walk → cap discovered before the offset is finalized → persisted.
    assert offset.get("batch_size_ok") == 2


@responses.activate
def test_batch_too_many_parts_converges_below_100_cap():
    """The retry budget lets the 1000-op default shrink below a ~100-part server
    cap and keep batching (rather than giving up): the recorded size settles
    between 1 and 100, every accepted batch fits the cap, and all rows arrive."""
    import re

    _mock_nested_metadata()
    n = 1000
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": i} for i in range(1, n + 1)]})

    accepted: list[int] = []
    rejections = [0]

    def cb(request):
        reqs = json.loads(request.body)["requests"]
        if len(reqs) > 100:  # server caps a batch at 100 parts
            rejections[0] += 1
            return (
                400,
                {"Content-Type": "application/json"},
                json.dumps({"error": {"message": "OData batch message contains too many parts"}}),
            )
        accepted.append(len(reqs))
        out = []
        for r in reqs:
            m = re.search(r"Parents\((\d+)\)/Children", r["url"])
            rows = [{"Id": int(m.group(1)) * 1000 + 1}] if m else []
            out.append({"id": r["id"], "status": 200, "body": {"value": rows}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=cb)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})  # default batch (1000)
    assert sorted(r["Id"] for r in recs) == sorted(i * 1000 + 1 for i in range(1, n + 1))
    assert rejections[0] >= 1
    # Converged below the cap and kept batching — NOT the give-up sentinel (1).
    assert 1 < c.__dict__["_batch_size_cap"] <= 100
    assert all(s <= 100 for s in accepted)


@responses.activate
def test_batch_overflow_detects_exceeds_maximum_message():
    """The shrink trigger matches phrasing variants, not just "too many parts":
    a server that rejects with "$batch exceeds the maximum of 100 operations"
    (the live Hexagon Smart API wording) still shrinks instead of hard-failing."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": i} for i in range(1, 6)]})
    responder = _too_many_parts_responder(
        [(f"Parents({i})/Children", {"value": [{"Id": i * 10 + 1}]}) for i in range(1, 6)]
        + [("Parents", {"value": [{"Id": 1}]})],
        max_parts=2,
        message="$batch exceeds the maximum of 100 operations",
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {})
    assert sorted(r["Id"] for r in recs) == [11, 21, 31, 41, 51]
    assert responder.rejections[0] >= 1  # the message was recognized → shrank
    assert all(n <= 2 for n in responder.accepted)
    assert c.__dict__["_batch_size_cap"] == 2


@responses.activate
def test_batch_preflight_transient_failure_not_persisted():
    """A transient failure of the ``$batch`` capability preflight (e.g. a 503)
    degrades THIS batch to the plain N+1 walk but records NO verdict — the next
    read re-probes, instead of persisting ``batch_ok=False`` and permanently
    pinning the stream to the slow path on a momentary blip. (Contrast the 405
    tests, where the definitive rejection IS persisted.)"""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "busy"}, status=503)
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    opts = {
        "cursor_field": "RecordLastModified",
        "cursor_probe": "batch",
        "pagination": "nextlink",
    }
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, opts)
    # Degraded to the plain N+1 walk for this batch — rows still correct.
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    # Transient → nothing cached on the instance, nothing persisted.
    assert "batch_ok" not in offset
    assert "_batch_supported" not in c.__dict__
    # The next read re-probes: a second preflight POST goes out.
    list(c.read_table(PROBE_TABLE, {"cursor": since}, opts)[0])
    posts = [call for call in responses.calls if call.request.method == "POST"]
    assert len(posts) == 2


@responses.activate
def test_batch_walk_cap_on_final_chunk_resume_clears_checkpoint():
    """The ``$batch`` walk's cap can fire exactly on its FINAL chunk (dirty
    parents an exact multiple of the chunk size). The truncated offset parks
    ``parent_idx`` == the total chain count, so the resumed batch has no
    re-entry work and emits nothing — it must CLEAR the checkpoint (offset back
    to the plain watermark) rather than echo it back forever, which would
    freeze the walk and silently skip all future changes under those parents."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}, {"Id": 11}]})
    responder = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            (
                "Mids(11)/Leaves",
                {"value": [{"Id": 1101, "RecordLastModified": "2020-06-02T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    opts = {
        "cursor_field": "RecordLastModified",
        "cursor_probe": "batch:2",  # chunk size == the number of leaf-parents
        "max_records_per_batch": "1",  # cap fires as the final chunk drains
        "pagination": "nextlink",
    }
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, opts)
    assert sorted(r["Id"] for r in recs) == [1001, 1101]  # chunk-aligned overshoot
    assert offset["parent_idx"] == 2  # truncated at the (final) chunk boundary
    assert offset["cursor"] == since  # watermark held while "in flight"

    # Resume: every chain is skipped and nothing is left to emit — the parked
    # checkpoint is cleared so the walk terminates instead of parking forever.
    recs2, offset2 = c.read_table(PROBE_TABLE, offset, opts)
    assert list(recs2) == []
    assert "parent_idx" not in offset2
    # The clear folds the truncated cycle's running_max into the committed
    # cursor — batch 1's progress is never lost (no period-2 re-read loop).
    assert offset2["cursor"] == "2020-06-02T00:00:00Z"


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


# ---------------------------------------------------------------------------
# expand_contained=auto — preflighted nested-$expand with N+1 fallback
# ---------------------------------------------------------------------------

_EXPAND_AUTO_OPTS = {
    "cursor_field": "RecordLastModified",
    "expand_contained": "auto",
    "cursor_probe": "false",  # keep the N+1 fallback a plain walk (no $batch)
    "pagination": "nextlink",
}


def _expand_auto_roots_callback(expand_body=None, expand_status=200):
    """GET Roots callback: requests carrying ``$expand`` get ``expand_body`` /
    ``expand_status``; plain requests (N+1 ancestor enumeration) get bare Ids."""
    from urllib.parse import unquote

    def _cb(request):
        if "$expand" in unquote(request.url):
            body = expand_body if expand_body is not None else {"value": [{"Id": 1}]}
            return (expand_status, {}, json.dumps(body))
        return (200, {}, json.dumps({"value": [{"Id": 1}]}))

    return _cb


@responses.activate
def test_expand_contained_auto_uses_expand_when_supported():
    """``auto`` preflights the real nested-$expand URL; a conclusive pass
    (inline children at every level) runs the expand read and persists
    ``expand_ok``, which a recreated reader uses to skip the preflight."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=lambda request: (200, {}, json.dumps(tree)),
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert offset.get("expand_ok") is True
    # Expand read: never a per-parent keyed GET (no N+1 ancestor walk).
    assert not any("Roots(" in call.request.url for call in responses.calls)
    # Exactly two $expand GETs: the preflight probe + the actual read.
    n_expand = sum(1 for call in responses.calls if "$expand" in unquote(call.request.url))
    assert n_expand == 2
    # The preflight probe pins the top-level $top to 1 (small subtree).
    probe_urls = [
        unquote(c_.request.url) for c_ in responses.calls if "$top=1&" in unquote(c_.request.url)
    ]
    assert probe_urls  # probe present

    # A RECREATED reader seeded from the offset skips the preflight entirely.
    n_before = len(responses.calls)
    c2 = _make()
    recs2, _ = c2.read_table(PROBE_TABLE, offset, dict(_EXPAND_AUTO_OPTS))
    list(recs2)
    new_roots = [call for call in responses.calls[n_before:] if "/Roots?" in call.request.url]
    assert len(new_roots) == 1  # just the read — no second probe


@responses.activate
def test_expand_contained_auto_falls_back_when_expand_ignored():
    """A server that accepts the $expand URL but returns rows WITHOUT the
    inline child collections would silently drop every deep row. The preflight
    cross-checks direct navigation, sees the children exist, records the
    definitive fail (``expand_ok=false``) and falls back to the N+1 walk."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=_expand_auto_roots_callback()
    )
    # Serves both the preflight's direct-nav cross-check and the N+1 walk.
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    # Round-30: the FAIL never rides the checkpoint (offsets are immortal —
    # a baked-in false would skip the preflight even after the server is
    # fixed). It lives in the TTL'd shared cache instead, like
    # cursor_probe_ok.
    assert "expand_ok" not in offset
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is False
    # Fallback hydrated via per-parent GETs.
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_expand_contained_auto_definitive_4xx_falls_back_and_persists():
    """A hard 4xx on the expand URL is a definitive verdict: fall back to N+1
    and persist ``expand_ok=false`` so the next microbatch skips the probe."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body={"error": "expand not supported"}, expand_status=400
        ),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    # Round-30: the fail is persisted in the TTL'd shared cache, never the
    # checkpoint — offsets are immortal, and a baked-in false would skip the
    # preflight even after the server is fixed.
    assert "expand_ok" not in offset
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is False
    # A recreated reader consults the shared cache and never retries $expand.
    n_before = len(responses.calls)
    c2 = _make()
    list(c2.read_table(PROBE_TABLE, offset, dict(_EXPAND_AUTO_OPTS))[0])
    assert not any("$expand" in unquote(call.request.url) for call in responses.calls[n_before:])
    # Once the cached fail expires (TTL / process restart), a fresh reader
    # RE-PROBES — exactly the recovery a fixed server needs.
    from databricks.labs.community_connector.sources.odata.odata import _clear_capability_cache

    _clear_capability_cache()
    n_before = len(responses.calls)
    c3 = _make()
    list(c3.read_table(PROBE_TABLE, offset, dict(_EXPAND_AUTO_OPTS))[0])
    assert any("$expand" in unquote(call.request.url) for call in responses.calls[n_before:])


@responses.activate
def test_expand_contained_auto_transient_failure_not_persisted():
    """A transient failure (503) on the expand preflight degrades THIS batch to
    the N+1 walk but records NO verdict — the next batch re-probes instead of
    pinning the stream to the fallback."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(expand_body={"detail": "busy"}, expand_status=503),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert "expand_ok" not in offset
    # Transient: the per-table memo dict may exist but must hold no verdict.
    assert not c.__dict__.get("_expand_supported")
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is None


@responses.activate
def test_expand_contained_default_is_auto():
    """With ``expand_contained`` UNSET, contained reads default to ``auto``:
    the preflight runs and a verified server is read via nested-$expand."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},  # no expand_contained
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset.get("expand_ok") is True
    assert not any("Roots(" in call.request.url for call in responses.calls)  # no N+1 walk


def test_expand_verdict_seed_and_merge_are_table_scoped():
    """A resume offset's ``expand_ok`` belongs to ITS table only. Seeding
    table A's verdict must not ride into table B's returned offset on a
    multi-table instance — baked in there it persists in B's checkpoint and
    skips B's own preflight forever, though B's (deeper) path may verify
    differently. That's the silent-deep-row-loss direction the preflight
    exists to prevent."""
    c = _make()
    c._seed_capability_caches("Roots__Mids__Leaves", None, {"cursor": "x", "expand_ok": True})
    merged_other = c._merge_capability_caches({"cursor": "y"}, "Other__Deep__Path")
    assert "expand_ok" not in merged_other
    merged_own = c._merge_capability_caches({"cursor": "y"}, "Roots__Mids__Leaves")
    assert merged_own.get("expand_ok") is True


@responses.activate
def test_expand_preflight_not_short_circuited_by_another_tables_verdict():
    """A verdict memoized for one table must not answer for another: with
    the instance memo pre-poisoned by a DIFFERENT table's ``False``, this
    table's ``auto`` preflight still runs, verifies expand, and reads via
    ``$expand`` (no N+1 walk) — and both tables' verdicts coexist in the
    per-table memo."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    c = _make()
    c.__dict__["_expand_supported"] = {"Some__Other__Table": False}
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset.get("expand_ok") is True
    assert not any("Roots(" in call.request.url for call in responses.calls)  # no N+1 walk
    assert c.__dict__["_expand_supported"] == {"Some__Other__Table": False, PROBE_TABLE: True}


@responses.activate
def test_expand_contained_auto_inconclusive_falls_back_to_n1():
    """An INCONCLUSIVE preflight must resolve to the N+1 shape, not expand.

    The trap: a server that silently ignores ``$expand`` whose first sampled
    branch is genuinely childless reads as inconclusive forever — assuming the
    expand shape there would silently drop every OTHER branch's rows on every
    batch. The safe resolution is N+1 for this batch (always correct), record
    nothing, re-probe next batch."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"

    def _roots_cb(request):
        from urllib.parse import unquote

        # $expand ignored by the server: rows come back with NO inline Mids —
        # for the probe AND for any read. Two parents; the first is childless.
        _ = unquote(request.url)
        return (200, {}, json.dumps({"value": [{"Id": 1}, {"Id": 2}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots", callback=_roots_cb)
    # Preflight cross-check on the FIRST parent: genuinely childless → the
    # probe cannot tell "ignored $expand" from "no children" → inconclusive.
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": []})
    # The second parent HAS children — only the N+1 walk can see them.
    responses.get(f"{SERVICE_URL}Roots(2)/Mids", json={"value": [{"Id": 20}]})
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids(20)/Leaves",
        json={"value": [{"Id": 2001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, {"cursor": since}, dict(_EXPAND_AUTO_OPTS))
    # N+1 fallback found the second parent's leaf — the expand shape would
    # have silently emitted nothing.
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(2, 20, 2001)]
    # Inconclusive: nothing recorded, nothing persisted — re-probed next batch.
    assert "expand_ok" not in offset
    # Transient: the per-table memo dict may exist but must hold no verdict.
    assert not c.__dict__.get("_expand_supported")
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is None


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
def test_pin_false_on_one_table_leaves_sibling_table_verdict_intact():
    """The snapshot purge is table-scoped: pinning ``expand_contained=false`` on
    one contained table must not evict a SIBLING table's cached ``expand_ok``
    (the drop of a per-table verdict touches only its own key)."""
    _mock_probe_metadata()
    c = _make()
    c._store_capability("expand_ok", True, table_name="Roots__Mids__Leaves")
    c._store_capability("expand_ok", True, table_name="Roots__Mids")
    # Read the two-segment sibling pinned false → purges only its own entry.
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    list(
        c.read_table("Roots__Mids", {}, {"pagination": "nextlink", "expand_contained": "false"})[0]
    )
    assert c._cached_capability("expand_ok", table_name="Roots__Mids") is None
    assert c._cached_capability("expand_ok", table_name="Roots__Mids__Leaves") is True


@responses.activate
def test_capability_cache_shares_batch_verdict_across_instances():
    """``batch_ok`` (and the discovered ``batch_size_ok`` cap) reach a fresh
    instance through the process cache — the capability POST runs once per
    process, not once per framework-recreated reader."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={"responses": [{"id": "0", "status": 200, "body": {"value": []}}]},
    )
    c1 = _make()
    assert c1._verify_batch_support(["Roots"], {}) is True
    c1._shrink_batch_cap(100)  # discovered cap must travel with the verdict
    discovered_cap = c1.__dict__["_batch_size_cap"]
    n_posts = sum(1 for call in responses.calls if call.request.method == "POST")
    assert n_posts == 1

    c2 = _make()
    assert c2._verify_batch_support(["Roots"], {}) is True
    assert c2.__dict__["_batch_size_cap"] == discovered_cap
    assert sum(1 for call in responses.calls if call.request.method == "POST") == n_posts


@responses.activate
def test_capability_cache_definitive_false_survives_process_cache_clear():
    """A definitive fail is shared too, and the on-disk JSON mirror covers a
    fresh process (simulated by clearing BOTH process-memory dicts — the verdict
    cache and its mtime memo — while leaving the disk file intact): the fresh
    'process' loads the verdict from the file instead of re-probing."""
    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_DISK_MTIME,
    )

    responses.post(f"{SERVICE_URL}$batch", json={"error": "no batch"}, status=405)
    c1 = _make()
    assert c1._verify_batch_support(["Roots"], {}) is False
    assert sum(1 for call in responses.calls if call.request.method == "POST") == 1

    # Fresh process = empty verdict cache AND empty mtime memo (a real fork
    # inherits both via copy-on-write; a brand-new process has neither). The
    # disk file is untouched, so the reload rehydrates the verdict from it.
    _CAPABILITY_CACHE.clear()
    _CAPABILITY_DISK_MTIME.clear()
    c2 = _make()
    assert c2._verify_batch_support(["Roots"], {}) is False
    assert sum(1 for call in responses.calls if call.request.method == "POST") == 1


def test_capability_cache_disk_merge_unions_per_table_maps():
    """The disk merge must union BOTH per-table maps (``expand_ok`` AND
    ``cursor_probe_ok``) table-by-table, process verdicts winning. A plain
    ``setdefault`` would shadow a sibling worker's whole on-disk map as soon as
    this process holds ANY table's verdict — re-probing exactly what the merge
    exists to prevent."""
    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_DISK_MTIME,
        _capability_cache_flush,
    )

    c = _make()
    # This process already holds table-A verdicts for both per-table maps.
    c._store_capability("cursor_probe_ok", False, table_name="A__Path")
    c._store_capability("expand_ok", False, table_name="A__Tbl")
    # A sibling worker's on-disk state: table A plus its own table-B verdicts.
    _capability_cache_flush(
        c.service_url,
        json.dumps(
            {
                "cursor_probe_ok": {"A__Path": True, "B__Path": True},
                "expand_ok": {"A__Tbl": True, "B__Tbl": True},
                "batch_ok": True,
            }
        ),
    )
    _CAPABILITY_DISK_MTIME.clear()  # force the next load to re-merge the file
    # The sibling's table-B verdicts merged in; table-A keeps the process value.
    assert c._cached_capability("cursor_probe_ok", table_name="B__Path") is True
    assert c._cached_capability("cursor_probe_ok", table_name="A__Path") is False
    assert c._cached_capability("expand_ok", table_name="B__Tbl") is True
    assert c._cached_capability("expand_ok", table_name="A__Tbl") is False
    assert c._cached_capability("batch_ok") is True


@responses.activate
def test_metadata_id_keyed_memos_dropped_at_pickle_boundary():
    """Spark pickles the reader (and the parsed-CSDL bundle) to executor
    tasks, where the unpickled tree's elements have NEW addresses: the
    ``id(et)``-keyed memos' driver-address keys are dead weight at best and
    a silently-wrong-schema false hit at worst. ``__getstate__`` must drop
    them; the executor re-derives per element, yielding the same schema."""
    import pickle

    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {})
    pks = c._primary_keys_for("Customers")
    state = c._metadata_state()
    assert state.own_fields and state.own_pks  # id()-keyed memos populated

    c2 = pickle.loads(pickle.dumps(c))
    state2 = c2._metadata_state()
    assert state2.own_fields == {}
    assert state2.own_pks == {}
    assert state2.base_chain == {}
    # Name-keyed memos are process-portable and survive.
    assert state2.fields
    # Executor-side re-derivation produces identical results.
    assert c2.get_table_schema("Customers", {}) == schema
    assert c2._primary_keys_for("Customers") == pks


def test_generated_bundle_registers_and_connector_survives_cloudpickle():
    """The merged single-file bundle is the artifact that actually deploys
    (SDP pipelines can't import package modules), yet the unit suite runs
    against the modules. Execute the bundle for real: register against a
    fake Spark, instantiate the connector, spot-check behavioral parity,
    and cloudpickle-round-trip the connector — which is what PySpark does
    to ship readers to executors. In the bundle every class is
    function-local, so cloudpickle serializes it BY VALUE, walking closure
    cells: a module-level ``itertools.count`` there is a TypeError on
    Python >= 3.14 (this venv) that the module-layout tests can never see."""
    import os
    import types

    from pyspark import cloudpickle

    import databricks.labs.community_connector.sources.odata as odata_pkg

    bundle_path = os.path.join(
        os.path.dirname(odata_pkg.__file__), "_generated_odata_python_source.py"
    )
    ns: dict = {"__name__": "_odata_bundle_under_test"}
    with open(bundle_path, encoding="utf-8") as fh:
        exec(compile(fh.read(), bundle_path, "exec"), ns)  # pylint: disable=exec-used

    captured: dict = {}
    fake_spark = types.SimpleNamespace(
        dataSource=types.SimpleNamespace(register=lambda cls: captured.setdefault("cls", cls))
    )
    ns["register_lakeflow_source"](fake_spark)
    source_cls = captured["cls"]

    ds = source_cls({"service_url": SERVICE_URL})
    connector = ds.lakeflow_connect
    assert type(connector).__name__ == "ODataLakeflowConnect"
    assert connector.service_url == SERVICE_URL
    # Behavioral parity spot-checks running the BUNDLE's own code: the
    # round-11 literal encoding through the bundle's _cursor_filter, and
    # the userinfo rejection through the bundle's __init__.
    assert (
        connector._cursor_filter("F", "2025-06-01T12:00:00+10:00")
        == "F gt 2025-06-01T12:00:00%2B10:00"
    )
    with pytest.raises(ValueError, match="must not embed credentials"):
        source_cls({"service_url": "https://user:secret@example.com/odata/"})

    # The executor-shipping round trip: by-value class serialization.
    clone = cloudpickle.loads(cloudpickle.dumps(connector))
    assert clone.service_url == SERVICE_URL
    assert type(clone).__name__ == "ODataLakeflowConnect"


@responses.activate
def test_plain_get_fallback_leaves_continuation_links_untouched():
    """The plain-GET fall-back injects the default ``$top`` only into fresh
    collection URLs. A server-issued continuation (``$skiptoken``/``$skip``) —
    which can reach the fall-back when the ``$batch`` give-up sentinel fires
    after a nextLink was re-queued — is used AS-IS (OData v4 §11.2.5.7):
    appending an option to an opaque skiptoken URL can 400 or corrupt the
    server's paging state."""
    seen: list[str] = []

    def _cb(request):
        seen.append(request.url)
        return (200, {"Content-Type": "application/json"}, json.dumps({"value": []}))

    responses.add_callback(
        responses.GET, re.compile(rf"{re.escape(SERVICE_URL)}Parents\(1\)/Children.*"), callback=_cb
    )
    c = _make()
    c.__dict__["_pagination"] = "auto"  # client-driven mode → injection active
    c._get_as_batch_response(f"{SERVICE_URL}Parents(1)/Children")
    c._get_as_batch_response(f"{SERVICE_URL}Parents(1)/Children?$skiptoken=opaque-42")
    fresh = [u for u in seen if "skiptoken" not in u]
    continuations = [u for u in seen if "skiptoken" in u]
    assert fresh and all("$top=" in u for u in fresh)  # fresh URL: $top injected
    assert continuations and all("$top=" not in u for u in continuations)  # as-is


def test_capability_cache_concurrent_access_is_thread_safe(monkeypatch):
    """The shared process cache is read-modify-written and serialized from
    multiple threads: concurrent streaming queries on one driver share
    ``_CAPABILITY_CACHE`` by ``service_url``, and ``json.dump`` / the load-merge
    iterate that live dict. Under ``_CAPABILITY_LOCK`` that's safe; without it a
    mutation landing mid-iteration trips "dictionary changed size during
    iteration".

    On the standard GIL build the C JSON encoder holds the GIL across a dict
    encode, so the race can't surface with default settings. To exercise the real
    hazard here (and to stand in for a free-threaded interpreter, PEP 703), force
    the *pure-Python* JSON encoder — whose ``for k, v in dct.items()`` yields the
    GIL between elements — and drop the thread-switch interval so a switch lands
    mid-encode. With the lock this stays green; remove the lock and it reliably
    raises "dictionary changed size during iteration"."""
    import json as _json
    import sys
    import threading

    from databricks.labs.community_connector.sources.odata.odata import _capability_cache_drop

    monkeypatch.setattr(_json.encoder, "c_make_encoder", None)  # force pure-Python dump

    c = _make()
    errors: list = []

    def worker(base: int) -> None:
        try:
            for i in range(400):
                tbl = f"T{base}_{i % 16}"
                c._store_capability("expand_ok", True, table_name=tbl)
                c._cached_capability("expand_ok", table_name=tbl)
                c._store_capability("batch_ok", True)  # server-wide churn
                if i % 4 == 0:
                    _capability_cache_drop(c.service_url, {"expand_ok"}, table_name=tbl)
        except Exception as exc:  # RuntimeError from an unlocked race, etc.
            errors.append(exc)

    prev_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)  # switch aggressively so a mutation lands mid-encode
    try:
        threads = [threading.Thread(target=worker, args=(b,)) for b in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(prev_interval)
    assert not errors, errors


def test_scrub_nonauto_strips_offset_and_purges_server_wide_batch_cache():
    """The offset scrub owns two things: (1) strip every non-``auto`` verdict
    from the outgoing offset; (2) purge the SERVER-WIDE ``$batch`` verdicts from
    the shared cache, but only on the transition (the offset still carries them)
    — conservative, since they aren't table-scoped and a sibling table may have
    a live ``auto`` consumer. The per-table verdicts are purged elsewhere (see
    ``_purge_nonauto_table_verdicts``), so scrub must leave them in the cache."""
    c = _make()
    c._store_capability("expand_ok", True, table_name=PROBE_TABLE)
    c._store_capability("batch_ok", True)
    pinned = {"expand_contained": "false", "contained_fetch": "single", "cursor_probe": "false"}

    # Offset always stripped of the pinned keys.
    assert c._scrub_nonauto_verdicts({"cursor": "x", "expand_ok": True}, pinned) == {"cursor": "x"}
    # Per-table ``expand_ok`` is NOT the offset scrub's job → left in the cache.
    assert c._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Steady state (no batch verdict in the offset) → server-wide cache kept.
    assert c._scrub_nonauto_verdicts({"cursor": "x"}, pinned) == {"cursor": "x"}
    assert c._cached_capability("batch_ok") is True

    # Transition (offset carries batch_ok) → server-wide cache purged.
    assert c._scrub_nonauto_verdicts({"cursor": "x", "batch_ok": True}, pinned) == {"cursor": "x"}
    assert c._cached_capability("batch_ok") is None


def test_purge_nonauto_table_verdicts_is_table_scoped_and_mode_gated():
    """``_purge_nonauto_table_verdicts`` drops the per-table ``expand_ok`` /
    ``cursor_probe_ok`` only when the governing option is non-``auto``, and only
    for the named table."""
    c = _make()
    c._store_capability("expand_ok", True, table_name="Roots__Mids__Leaves")
    c._store_capability("expand_ok", True, table_name="Roots__Mids")

    # auto (unset) → no purge.
    c._purge_nonauto_table_verdicts("Roots__Mids__Leaves", {"cursor_probe": "false"})
    assert c._cached_capability("expand_ok", table_name="Roots__Mids__Leaves") is True

    # pinned false → drops only this table's entry.
    c._purge_nonauto_table_verdicts("Roots__Mids__Leaves", {"expand_contained": "false"})
    assert c._cached_capability("expand_ok", table_name="Roots__Mids__Leaves") is None
    assert c._cached_capability("expand_ok", table_name="Roots__Mids") is True


# ---------------------------------------------------------------------------
# expand_contained mode switches — streaming resume across false/true/auto
# ---------------------------------------------------------------------------


def _switch_opts(mode):
    """Table options for the mode-switch tests: leaf cursor on PROBE_TABLE,
    N+1 fallback kept a plain walk (no $batch), server-driven paging. The
    ``auto`` cursor-lookback is disabled so the read filter equals the
    committed watermark exactly — these tests assert the ``gt <watermark>``
    literal to prove the switched mode resumed from the shared cursor key."""
    return {
        "cursor_field": "RecordLastModified",
        "expand_contained": mode,
        "cursor_probe": "false",
        "pagination": "nextlink",
        "cursor_lookback_seconds": "off",
    }


def _switch_tree(leaf_id, ts):
    """One-root/one-mid $expand response whose single leaf is ``leaf_id``."""
    return {
        "value": [
            {"Id": 1, "Mids": [{"Id": 10, "Leaves": [{"Id": leaf_id, "RecordLastModified": ts}]}]}
        ]
    }


def _expand_urls():
    from urllib.parse import unquote

    return [unquote(c.request.url) for c in responses.calls if "$expand" in unquote(c.request.url)]


@responses.activate
@pytest.mark.parametrize("second_mode", ["true", "auto"])
def test_expand_contained_switch_false_to_expand_resumes_from_watermark(second_mode):
    """Batch 1 reads N+1 (``expand_contained=false``) and commits a watermark;
    switching to ``true`` (or ``auto``) resumes from that same ``cursor`` key —
    the expand read filters ``gt <watermark>`` and picks up exactly the new
    rows, no re-ingest of batch 1's rows, no error."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body=_switch_tree(1002, "2020-07-01T00:00:00Z")
        ),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    # Batch 1: N+1 walk.
    c1 = _make()
    recs1, offset1 = c1.read_table(PROBE_TABLE, {"cursor": since}, _switch_opts("false"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs1] == [(1, 10, 1001)]
    assert offset1["cursor"] == "2020-06-01T00:00:00Z"
    assert not _expand_urls()  # pure N+1 so far

    # Batch 2: switched mode, resumed from batch 1's checkpoint.
    c2 = _make()
    recs2, offset2 = c2.read_table(PROBE_TABLE, offset1, _switch_opts(second_mode))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs2] == [(1, 10, 1002)]
    assert offset2["cursor"] == "2020-07-01T00:00:00Z"
    # The expand read resumed from the SHARED watermark, not from scratch.
    assert any("gt 2020-06-01T00:00:00Z" in u for u in _expand_urls())
    # No stale N+1 resume state rides forward.
    for stale in ("parent_idx", "parent_keys", "chain_next_link", "truncated_chain_cursor"):
        assert stale not in offset2


@responses.activate
def test_expand_contained_switch_true_to_false_resumes_from_watermark():
    """The reverse switch: batch 1 reads via $expand and commits a watermark;
    ``expand_contained=false`` resumes from it — the N+1 leaf walk filters
    ``gt <watermark>`` and no $expand request is ever issued again."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=_expand_auto_roots_callback(
            expand_body=_switch_tree(1001, "2020-06-01T00:00:00Z")
        ),
    )
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1002, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
        match_querystring=False,
    )
    # Batch 1: explicit expand read.
    c1 = _make()
    recs1, offset1 = c1.read_table(PROBE_TABLE, {"cursor": since}, _switch_opts("true"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs1] == [(1, 10, 1001)]
    assert offset1["cursor"] == "2020-06-01T00:00:00Z"
    n_expand_batch1 = len(_expand_urls())
    assert n_expand_batch1 >= 1

    # Batch 2: N+1, resumed from the expand checkpoint.
    c2 = _make()
    recs2, offset2 = c2.read_table(PROBE_TABLE, offset1, _switch_opts("false"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs2] == [(1, 10, 1002)]
    assert offset2["cursor"] == "2020-07-01T00:00:00Z"
    assert len(_expand_urls()) == n_expand_batch1  # no $expand after the switch
    # The leaf walk filtered from the shared watermark.
    from urllib.parse import unquote

    leaf_urls = [
        unquote(c.request.url) for c in responses.calls if "Mids(10)/Leaves" in c.request.url
    ]
    assert any("gt 2020-06-01T00:00:00Z" in u for u in leaf_urls)


@responses.activate
def test_expand_truncation_offset_switch_to_false_ignores_pending_fetches():
    """MID-FLIGHT switch: the expand read truncated (parked ``pending_fetches``
    + ``running_max_cursor``, watermark held). Switching to ``false`` must
    ignore the parked expand state, re-read from the HELD watermark (re-emitted
    rows are MERGE-deduped downstream — never loss), and drop the stale expand
    keys from the outgoing offset so they can't resurrect on a later switch
    back."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    truncated = {
        "cursor": since,  # watermark held while the chain was in flight
        "running_max_cursor": "2020-06-05T00:00:00Z",
        "pending_fetches": [
            {
                "url": f"{SERVICE_URL}Roots?$marker=stale",
                "level": 0,
                "chain": [],
                "cur_val": None,
                "skip": 0,
            }
        ],
    }
    c = _make()
    recs, offset = c.read_table(PROBE_TABLE, dict(truncated), _switch_opts("false"))
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # The parked expand work queue was never resumed...
    assert not any("marker=stale" in c_.request.url for c_ in responses.calls)
    # ...and neither expand key leaks into the N+1 checkpoint.
    assert "pending_fetches" not in offset
    assert "running_max_cursor" not in offset
    # Read floor came from the held watermark, not the in-flight running max.
    from urllib.parse import unquote

    leaf_urls = [unquote(c_.request.url) for c_ in responses.calls if "Leaves" in c_.request.url]
    assert any(f"gt {since}" in u for u in leaf_urls)


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
def test_expand_contained_auto_pin_unpin_lifecycle_across_stream():
    """Full verdict lifecycle over three microbatches of one stream:
    ``auto`` records ``expand_ok`` (offset + shared cache) → pinning ``false``
    reads N+1, scrubs the flag from the checkpoint AND purges the shared cache
    → re-selecting ``auto`` re-runs the preflight from scratch. Rows flow
    correctly at every step."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"

    # Since-aware $expand body: a real server honors the ``gt <since>``
    # cursor filter, so once microbatch 2 advances the watermark to
    # 2020-07-01 the expand read must serve a NEWER leaf — an ignored
    # filter returning only stale rows now (correctly) trips the
    # no-progress guard, since completion cursors are floored at ``since``
    # instead of regressing.
    def _roots_cb(request):
        url = unquote(request.url)
        if "$expand" not in url:
            return (200, {}, json.dumps({"value": [{"Id": 1}]}))
        if "gt 2020-07-01" in url:
            body = _switch_tree(1003, "2020-08-01T00:00:00Z")
        else:
            body = _switch_tree(1001, "2020-06-01T00:00:00Z")
        return (200, {}, json.dumps(body))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots", callback=_roots_cb)
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1002, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
        match_querystring=False,
    )
    # Microbatch 1 — auto: preflight + expand read, verdict recorded twice.
    c1 = _make()
    recs1, off1 = c1.read_table(PROBE_TABLE, {"cursor": since}, _switch_opts("auto"))
    assert [(r["Id"]) for r in recs1] == [1001]
    assert off1.get("expand_ok") is True
    assert c1._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Microbatch 2 — pinned false: N+1 read; the switch scrubs the checkpoint
    # flag and purges the shared cache entry.
    c2 = _make()
    n_expand_before = len(_expand_urls())
    recs2, off2 = c2.read_table(PROBE_TABLE, off1, _switch_opts("false"))
    assert [(r["Id"]) for r in recs2] == [1002]
    assert off2["cursor"] == "2020-07-01T00:00:00Z"
    assert "expand_ok" not in off2
    assert len(_expand_urls()) == n_expand_before  # pinned false never expands
    assert c2._cached_capability("expand_ok", table_name=PROBE_TABLE) is None

    # Microbatch 3 — back to auto: nothing recorded anywhere → the preflight
    # RE-RUNS (probe + read = two more $expand GETs), then re-records.
    c3 = _make()
    recs3, off3 = c3.read_table(PROBE_TABLE, off2, _switch_opts("auto"))
    list(recs3)
    assert len(_expand_urls()) == n_expand_before + 2
    assert off3.get("expand_ok") is True


def test_expand_contained_nonauto_scrubs_expand_ok():
    """An explicit non-``auto`` ``expand_contained`` scrubs the recorded
    ``expand_ok`` verdict, so re-selecting ``auto`` re-runs the preflight;
    ``auto`` — explicit or the unset default — keeps it."""
    c = _make()
    off = {"cursor": "x", "expand_ok": True}
    assert c._scrub_nonauto_verdicts(off, {"expand_contained": "false"}) == {"cursor": "x"}
    assert c._scrub_nonauto_verdicts(off, {"expand_contained": "true"}) == {"cursor": "x"}
    assert c._scrub_nonauto_verdicts(off, {}) == off  # unset default is auto → kept
    assert c._scrub_nonauto_verdicts(off, {"expand_contained": "auto"}) == off


@responses.activate
def test_is_partitioned_expand_auto_follows_preflight_verdict():
    """``expand_contained=auto`` partition activation follows the RESOLVED
    shape: a verified server (expand read, no fan-out) is not partitioned;
    explicit ``true`` never is; explicit ``false``/unset always may be."""
    _mock_probe_metadata()
    tree = {
        "value": [
            {
                "Id": 1,
                "Mids": [
                    {
                        "Id": 10,
                        "Leaves": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    }
                ],
            }
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda request: (200, {}, json.dumps(tree))
    )
    c = _make({"expand_contained": "auto"})
    assert c.is_partitioned(PROBE_TABLE) is False  # preflight verified → expand shape
    # The batch get_partitions reuses the cached verdict → serial deferral.
    assert c.get_partitions(PROBE_TABLE, {"expand_contained": "auto"}) == [{}]
    assert _make({"expand_contained": "true"}).is_partitioned(PROBE_TABLE) is False
    assert _make().is_partitioned(PROBE_TABLE) is False  # unset default = auto → verified
    assert _make({"expand_contained": "false"}).is_partitioned(PROBE_TABLE) is True


@responses.activate
def test_is_partitioned_expand_auto_fallback_stays_partitionable():
    """When the ``auto`` preflight fails (server ignores ``$expand``), the
    table resolves to the N+1 shape and KEEPS its partitioned parallelism —
    both activation and the batch ``get_partitions`` fan-out."""
    _mock_probe_metadata()
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=_expand_auto_roots_callback()
    )
    # Preflight cross-check finds real children → definitive ignored-$expand.
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    c = _make({"expand_contained": "auto"})
    assert c.is_partitioned(PROBE_TABLE) is True
    parts = c.get_partitions(PROBE_TABLE, {"expand_contained": "auto"})
    assert parts and "top_parent_rows" in parts[0]  # real partition fan-out


@responses.activate
def test_partitioned_pin_false_resets_shared_verdict_via_partition_path():
    """The reset contract must hold on the PARTITION path too. A partitionable
    contained snapshot pinned ``expand_contained=false`` streams through
    ``is_partitioned`` / ``get_partitions`` (never ``read_table``), so those
    must purge the per-table shared-cache verdict — otherwise a later switch
    back to ``auto`` would reuse a stale verdict without re-probing."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    tree = {"value": [{"Id": 1, "Mids": [{"Id": 10, "Leaves": [{"Id": 1001}]}]}]}
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots", callback=lambda r: (200, {}, json.dumps(tree))
    )

    # auto snapshot: the preflight records expand_ok=True in the shared cache.
    c_auto = _make({"expand_contained": "auto"})
    assert c_auto._expand_read_active(PROBE_TABLE, {"expand_contained": "auto"}) is True
    assert c_auto._cached_capability("expand_ok", table_name=PROBE_TABLE) is True

    # Pinned false, partitionable snapshot: is_partitioned purges the verdict
    # (it would otherwise never be reset — this path skips read_table).
    c_false = _make({"expand_contained": "false"})
    assert c_false.is_partitioned(PROBE_TABLE) is True
    assert c_false._cached_capability("expand_ok", table_name=PROBE_TABLE) is None

    # And get_partitions on the pinned-false path resets it too (idempotent).
    c_auto._store_capability("expand_ok", True, table_name=PROBE_TABLE)  # re-seed
    c_false2 = _make({"expand_contained": "false"})
    c_false2.get_partitions(PROBE_TABLE, {"expand_contained": "false"})
    assert c_false2._cached_capability("expand_ok", table_name=PROBE_TABLE) is None

    # Switching back to auto now genuinely re-probes (nothing cached).
    n_before = sum(1 for c in responses.calls if "$expand" in unquote(c.request.url))
    c_reauto = _make({"expand_contained": "auto"})
    assert c_reauto.is_partitioned(PROBE_TABLE) is False  # verified → expand shape
    assert sum(1 for c in responses.calls if "$expand" in unquote(c.request.url)) > n_before


# ---------------------------------------------------------------------------
# cursor_probe=nested-expand → $batch hydrate of the probe's dirty parents
# ---------------------------------------------------------------------------


@responses.activate
def test_cursor_probe_nested_expand_hydrates_dirty_via_batch():
    """nested-expand identifies dirty leaf-parents via the nested-``$expand``
    probe, then — when the server supports ``$batch`` — hydrates ONLY those via
    ``$batch`` (no per-parent GET). Both verdicts (cursor_probe_ok, batch_ok)
    persist."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}, {"Id": 2}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids",
        json={
            "value": [
                {"Id": 20, "Leaves": [{"RecordLastModified": "2019-01-01T00:00:00Z"}]},
                {"Id": 21, "Leaves": [{"RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ]
        },
    )
    responder = _batch_responder(
        [
            (
                "Roots(1)/Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            (
                "Roots(2)/Mids(21)/Leaves",
                {"value": [{"Id": 2101, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # $batch preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert sorted((r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows) == [
        (1, 10, 1001),
        (2, 21, 2101),
    ]
    assert offset["cursor"] == "2020-07-01T00:00:00Z"
    # Probe ran (identify via nested-$expand) ...
    assert any(
        "$expand=Leaves" in unquote(call.request.url)
        for call in responses.calls
        if "/Mids?" in call.request.url
    )
    # ... and the dirty hydrate went through $batch — never a per-parent GET.
    assert not any(
        call.request.method == "GET" and "/Leaves" in call.request.url for call in responses.calls
    )
    assert any("Mids(10)/Leaves" in u for u in responder.seen)
    assert any("Mids(21)/Leaves" in u for u in responder.seen)
    # Clean leaf-parents are never hydrated.
    assert not any("Mids(11)/Leaves" in u or "Mids(20)/Leaves" in u for u in responder.seen)
    # cursor_probe=nested-expand is non-auto → its probe verdict is scrubbed from
    # the offset (so a later switch to auto re-probes). batch_ok is owned by
    # contained_fetch (default auto here) and persists.
    assert "cursor_probe_ok" not in offset
    assert offset.get("batch_ok") is True


@responses.activate
def test_nonauto_clears_recorded_preflight_verdicts():
    """A non-``auto`` option scrubs its recorded preflight verdict from the
    outgoing offset, so re-selecting ``auto`` later re-runs the preflight:
    ``cursor_probe`` non-auto drops ``cursor_probe_ok``; ``contained_fetch``
    non-auto drops the ``$batch`` verdicts (``batch_ok`` / ``batch_size_ok``)."""
    _mock_probe_metadata()
    c = _make()

    # contained_fetch=single (non-auto): a previously-recorded $batch verdict in
    # the incoming offset is not carried forward.
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    _, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z", "batch_ok": True, "batch_size_ok": 200},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "false",  # non-auto → drops cursor_probe_ok (absent here)
            "contained_fetch": "single",  # non-auto → drops batch_ok / batch_size_ok
            "pagination": "nextlink",
        },
    )
    assert "batch_ok" not in offset
    assert "batch_size_ok" not in offset


@responses.activate
def test_auto_retains_recorded_preflight_verdicts():
    """``auto`` (default) keeps its recorded verdicts in the offset so a
    recreated reader skips the preflight — the counterpart to the scrub."""
    _mock_probe_metadata()
    c = _make()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    _, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z", "batch_ok": True, "batch_size_ok": 200},
        {"cursor_field": "RecordLastModified", "cursor_probe": "auto", "pagination": "nextlink"},
    )
    # Both options default/auto → the seeded verdicts survive.
    assert offset.get("batch_ok") is True
    assert offset.get("batch_size_ok") == 200


@responses.activate
def test_cursor_probe_nested_expand_falls_back_to_n1_when_batch_unsupported():
    """When the ``$batch`` preflight fails (fail-closed), nested-expand still
    prunes to the dirty parents but hydrates them via the plain N+1 walk —
    one per-parent GET, clean parents untouched."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )

    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows] == [(1, 10, 1001)]
    # Dirty parent hydrated via plain GET (N+1 fallback); clean parent untouched.
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )
    assert not any("Mids(11)/Leaves" in call.request.url for call in responses.calls)
    assert offset.get("batch_ok") is not True  # preflight failed → batch not used


@responses.activate
def test_cursor_probe_nested_expand_contained_fetch_single_forces_n1():
    """An explicit ``contained_fetch=single`` overrides the probe's ``$batch``
    hydrate: the probe still prunes to dirty parents, but they go down the plain
    N+1 walk — no ``$batch`` POST at all (preflight skipped)."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )

    c = _make()
    _skip_probe_preflight(c)
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "contained_fetch": "single",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows] == [(1, 10, 1001)]
    # No $batch was even attempted (the explicit single skips the preflight).
    assert not any(call.request.method == "POST" for call in responses.calls)
    # Dirty parent hydrated via plain GET; clean parent untouched.
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )
    assert not any("Mids(11)/Leaves" in call.request.url for call in responses.calls)


# ---------------------------------------------------------------------------
# OR-across-columns keyset-seek preflight → fall back to $skip (mode B)
# ---------------------------------------------------------------------------


def _leaves_or_probe_callback(seen, reject_or):
    """Callback for the leaf collection under `auto` pagination. Page 1 returns
    one row with no `@odata.nextLink` (forces the client-driven seek). The
    composite `(cursor,pk)` seek builds an OR-across-columns `$filter`; the
    `$top=1` probe carrying that OR is answered 400 when `reject_or`, and the
    subsequent `$skip` drain returns empty. Records what shapes were seen."""
    from urllib.parse import parse_qs, unquote, urlparse

    def _cb(request):
        qs = parse_qs(urlparse(request.url).query)
        flt = unquote(qs.get("$filter", [""])[0])
        top = qs.get("$top", [""])[0]
        has_skip = "$skip" in qs
        if " or " in flt and top == "1":
            seen["or_probe"] += 1
            if reject_or:
                return (
                    400,
                    {},
                    json.dumps({"error": {"message": "on different columns, only AND"}}),
                )
            return (200, {}, json.dumps({"value": []}))
        if " or " in flt:
            seen["keyset_seek"] += 1
            return (200, {}, json.dumps({"value": []}))  # keyset drain → empty
        if has_skip:
            seen["skip_seek"] += 1
            return (200, {}, json.dumps({"value": []}))  # $skip drain → empty
        return (
            200,
            {},
            json.dumps({"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]}),
        )

    return _cb


@responses.activate
def test_or_filter_preflight_falls_back_to_skip_when_rejected():
    """When the composite keyset seek's OR-across-columns probe is rejected
    (400), the walk drops to `$skip` paging (mode B) instead — no data lost,
    no crash."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    seen = {"or_probe": 0, "keyset_seek": 0, "skip_seek": 0}
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        callback=_leaves_or_probe_callback(seen, reject_or=True),
    )
    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z"},
        {"cursor_field": "RecordLastModified", "cursor_probe": "false", "pagination": "auto"},
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert seen["or_probe"] == 1  # OR preflight fired and was rejected
    assert seen["skip_seek"] >= 1  # fell back to $skip (mode B)
    assert seen["keyset_seek"] == 0  # never issued the rejected OR seek for real


@responses.activate
def test_or_filter_preflight_uses_keyset_when_supported():
    """When the OR probe succeeds, the walk uses the composite keyset seek as
    before (no `$skip` fallback)."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    seen = {"or_probe": 0, "keyset_seek": 0, "skip_seek": 0}
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        callback=_leaves_or_probe_callback(seen, reject_or=False),
    )
    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": "2020-01-01T00:00:00Z"},
        {"cursor_field": "RecordLastModified", "cursor_probe": "false", "pagination": "auto"},
    )
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    assert seen["or_probe"] == 1  # probe fired
    assert seen["keyset_seek"] >= 1  # used the keyset OR seek
    assert seen["skip_seek"] == 0  # no $skip fallback


@responses.activate
def test_or_filter_probe_transient_fails_open_without_persisting():
    """A transient (429/5xx) on the OR-across-columns probe is NOT evidence
    about OR support: fail OPEN (True) for this seek and record NOTHING (no
    instance verdict, no shared-cache verdict), so the next seek re-probes
    instead of durably pinning the slower $skip walk on a momentary throttle."""
    calls = {"n": 0}

    def _cb(_request):
        calls["n"] += 1
        return (429, {}, json.dumps({"error": "slow down"}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Coll", callback=_cb)
    c = _make()
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is True
    assert calls["n"] == 1  # probed once (single attempt, no retry storm)
    assert "_or_filter_ok" not in c.__dict__  # nothing cached on the instance
    assert c._cached_capability("or_filter_ok") is None  # nothing persisted


@responses.activate
def test_or_filter_probe_408_is_transient_not_a_verdict():
    """A 408 (request timeout) is transient like 429/5xx but sits outside the
    retry set — it must still fail OPEN and record nothing. Pre-fix it fell
    through to the 4xx test and persisted a definitive or_filter_ok=False,
    which has NO reset path: one timeout durably pinned the $skip walk."""
    responses.add_callback(responses.GET, f"{SERVICE_URL}Coll", callback=lambda _r: (408, {}, ""))
    c = _make()
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is True
    assert "_or_filter_ok" not in c.__dict__  # nothing cached on the instance
    assert c._cached_capability("or_filter_ok") is None  # nothing persisted


@responses.activate
def test_or_filter_probe_auth_401_not_mislabeled_as_unsupported():
    """A 401 (expired token) on the OR probe must NOT be read as 'OR
    unsupported'. Routed through the auth-aware _http_get_once, a 401 without an
    OAuth refresh path raises PermissionError, which fails open (True) and
    records nothing — rather than the pre-fix raw session.get that treated the
    401 as a definitive 4xx rejection and pinned $skip."""
    responses.add_callback(responses.GET, f"{SERVICE_URL}Coll", callback=lambda _r: (401, {}, ""))
    c = _make()  # bearer auth → no OAuth refresh path
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is True
    assert "_or_filter_ok" not in c.__dict__
    assert c._cached_capability("or_filter_ok") is None


@responses.activate
def test_or_filter_probe_definitive_400_still_falls_back_and_persists():
    """Regression: a genuine non-transient 4xx (the 'only AND operators are
    supported' 400) is still a definitive rejection — cached False on the
    instance AND persisted to the shared cache so later seeks skip the probe."""
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Coll",
        callback=lambda _r: (400, {}, json.dumps({"error": "only AND operators are supported"})),
    )
    c = _make()
    assert c._verify_or_filter_support(f"{SERVICE_URL}Coll", ["a", "b"], {"a": 1, "b": 2}) is False
    assert c.__dict__["_or_filter_ok"] is False
    assert c._cached_capability("or_filter_ok") is False


@responses.activate
def test_capability_verdicts_thread_through_offset():
    """The OR / $batch capability verdicts ride the resume offset so a reader
    the framework recreates each microbatch skips re-probing. Seed-from-offset,
    seeded-verdict-skips-the-probe, merge-into-offset, and never-overwrite."""
    _mock_probe_metadata()
    c = _make()
    # Seed instance caches from a prior batch's offset.
    c._seed_capability_caches(
        PROBE_TABLE, None, {"cursor": "x", "or_filter_ok": False, "batch_ok": True}
    )
    assert c.__dict__["_or_filter_ok"] is False
    assert c.__dict__["_batch_supported"] is True
    # A seeded OR verdict is returned WITHOUT issuing a probe (cached short-circuit).
    assert c._verify_or_filter_support("https://x/Coll", ["a", "b"], {"a": 1, "b": 2}) is False
    assert not responses.calls  # no network for the seeded verdict
    # Merge threads the verdicts back into a fresh offset...
    merged = c._merge_capability_caches({"cursor": "y"})
    assert merged == {"cursor": "y", "or_filter_ok": False, "batch_ok": True}
    # ...but never overwrites a value a read path already wrote.
    assert c._merge_capability_caches({"batch_ok": True, "or_filter_ok": True}) == {
        "batch_ok": True,
        "or_filter_ok": True,
    }
    # Single-key $orderby never builds an OR → never probed (short-circuits True).
    c.__dict__.pop("_or_filter_ok", None)
    assert c._verify_or_filter_support("https://x/Coll", ["a"], {"a": 1}) is True
    assert not responses.calls


def test_scrub_batch_verdicts_kept_while_auto_consumer_live():
    """The shared ``$batch`` verdicts (``batch_ok`` / ``batch_size_ok``) survive
    a pinned ``contained_fetch`` as long as the ``cursor_probe`` auto cascade
    still consumes them; they are scrubbed only when every consumer is pinned
    non-auto or the hydrate is suppressed by an explicit ``single``."""
    c = _make()
    off = {"cursor": "x", "batch_ok": True, "batch_size_ok": 200}
    # contained_fetch pinned, but default cursor_probe (auto) still consumes
    # and refreshes the verdicts → kept (no per-microbatch re-discovery churn).
    assert c._scrub_nonauto_verdicts(off, {"contained_fetch": "batch:200"}) == off
    # Explicit single suppresses the auto hydrate → no live consumer → scrub.
    assert c._scrub_nonauto_verdicts(off, {"contained_fetch": "single"}) == {"cursor": "x"}
    # Every consumer pinned non-auto → scrub.
    assert c._scrub_nonauto_verdicts(
        off, {"contained_fetch": "batch", "cursor_probe": "false"}
    ) == {"cursor": "x"}
    # contained_fetch auto keeps the batch verdicts regardless of cursor_probe.
    assert c._scrub_nonauto_verdicts(off, {"cursor_probe": "false"}) == off


# ---------------------------------------------------------------------------
# Round-27 fixes: typed literals, queue-park preservation, watermark floors,
# %24filter folding, $batch envelope retry, curated option validation
# ---------------------------------------------------------------------------

GUID_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="G" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Account">
        <Key><PropertyRef Name="AccountId"/></Key>
        <Property Name="AccountId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <NavigationProperty Name="Contacts" Type="Collection(G.Contact)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Contact">
        <Key><PropertyRef Name="ContactId"/></Key>
        <Property Name="ContactId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="DayBatch">
        <Key><PropertyRef Name="Day"/></Key>
        <Property Name="Day" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Items" Type="Collection(G.DayItem)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="DayItem">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Accounts" EntityType="G.Account"/>
        <EntitySet Name="DayBatches" EntityType="G.DayBatch"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

_GUID = "550e8400-e29b-41d4-a716-446655440000"


def _mock_guid_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_METADATA_XML, status=200)


@responses.activate
def test_guid_key_predicate_renders_bare():
    """An ``Edm.Guid`` key arrives as a JSON string, but its key predicate
    must be UNQUOTED per the OData v4 ABNF — strict servers (Olingo, SAP)
    400 on ``Accounts('<guid>')``. The value sniff can't know this; the
    declared type must win."""
    from urllib.parse import unquote

    _mock_guid_metadata()
    responses.get(
        f"{SERVICE_URL}Accounts", json={"value": [{"AccountId": _GUID}]}, match_querystring=False
    )
    # ONLY the bare-predicate URL is registered — a quoted predicate would
    # hit an unregistered URL and fail the read outright.
    responses.get(
        f"{SERVICE_URL}Accounts({_GUID})/Contacts",
        json={"value": [{"ContactId": _GUID, "ModifiedAt": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(
        "Accounts__Contacts", {}, {"contained_fetch": "single", "pagination": "nextlink"}
    )
    assert [r["ContactId"] for r in recs] == [_GUID]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any(f"Accounts({_GUID})/Contacts" in u for u in urls)
    assert not any("Accounts('" in u for u in urls)


@responses.activate
def test_string_key_iso_lookalike_stays_quoted():
    """The inverse hole: an ``Edm.String`` key whose VALUE happens to look
    ISO-8601 (``"2024-01-01"``) passed the bare-timestamp sniff and rendered
    UNQUOTED — an invalid key predicate for a string-typed key."""
    from urllib.parse import unquote

    _mock_guid_metadata()
    responses.get(
        f"{SERVICE_URL}DayBatches", json={"value": [{"Day": "2024-01-01"}]}, match_querystring=False
    )
    responses.get(
        f"{SERVICE_URL}DayBatches('2024-01-01')/Items",
        json={"value": [{"Id": 7}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(
        "DayBatches__Items", {}, {"contained_fetch": "single", "pagination": "nextlink"}
    )
    assert [r["Id"] for r in recs] == [7]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any("DayBatches('2024-01-01')/Items" in u for u in urls)


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


def test_pg_keyset_filter_typed_literals():
    """Unit shape of the typed seek: guid boundary bare, ISO-looking string
    boundary quoted; untyped columns keep the value sniff."""
    from databricks.labs.community_connector.sources.odata._contained import _pg_keyset_filter

    row = {"g": _GUID, "s": "2024-01-01"}
    types = {"g": "Edm.Guid", "s": "Edm.String"}
    seek = _pg_keyset_filter(["g", "s"], row, types)
    assert f"g gt {_GUID}" in seek
    assert "s gt '2024-01-01'" in seek
    assert f"g eq {_GUID}" in seek
    # Untyped fallback preserves the pre-round-27 sniff behavior.
    sniffed = _pg_keyset_filter(["g", "s"], row)
    assert f"g gt '{_GUID}'" in sniffed
    assert "s gt 2024-01-01" in sniffed


def test_odata_literal_numeric_and_slash_edges():
    """Exponent ``+`` percent-encoded (form-decoding servers read a raw
    ``+`` as a space), non-finite floats use the OData spellings, and ``/``
    in a string literal can't split a path segment."""
    assert _odata_literal(1e20) == "1e%2B20"
    assert _odata_literal(float("inf")) == "INF"
    assert _odata_literal(float("-inf")) == "-INF"
    assert _odata_literal(float("nan")) == "NaN"
    assert _odata_literal("A/B") == "'A%2FB'"


def test_pg_filter_percent24_spelling_folded():
    """A server-issued continuation can carry ``%24filter=`` instead of
    ``$filter=``. The filter readers must see it and the writers must FOLD
    it into the one ``$filter`` param — two filter params make the server
    pick one arbitrarily (or 400)."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _pg_base_filter,
        _pg_keyset_seek_url,
        _pg_with_extra_filter,
    )

    url = "https://x/E?%24filter=a eq 1&%24top=5"
    assert _pg_base_filter(url) == "a eq 1"
    out = _pg_with_extra_filter(url, "b gt 2")
    assert "%24filter" not in out
    assert "$filter=(a eq 1) and (b gt 2)" in out
    seek_url = _pg_keyset_seek_url(url, _pg_base_filter(url), "k gt 3")
    assert "%24filter" not in seek_url
    assert seek_url.count("$filter=") == 1
    assert "$filter=(a eq 1) and (k gt 3)" in seek_url


@responses.activate
def test_expand_all_children_deferred_drain_without_dropping_queue():
    """A server that defers EVERY inner collection behind
    ``<Nav>@odata.nextLink`` (nothing inline) must still drain fully across
    capped batches, and the parked ``pending_fetches`` must never be silently
    dropped by the idle shortcut in ``_read_contained_expand`` (round-26: an
    empty-``emitted`` batch that echoes ``start_offset`` discards the queue
    and livelocks at zero rows). Depth-first drains each deferred child in the
    batch it's discovered, so the queue makes progress every batch and stays
    depth-bounded; every paged child arrives exactly once. (The specific
    "park before the first emit" trigger the old ceiling produced is no longer
    reachable under depth-first — the drainer emits as it descends — but the
    idle-shortcut guard remains as defense and this exercises the drain it
    protects.)"""
    _mock_nested_metadata()
    parents = []
    for i in range(1, 7):
        parents.append(
            {
                "Id": i,
                "Name": f"2024-01-0{i}T00:00:00Z",
                "Children": [],  # nothing inline — all children deferred
                "Children@odata.nextLink": f"{SERVICE_URL}Parents({i})/Children?$skiptoken=k{i}",
            }
        )
        responses.get(
            f"{SERVICE_URL}Parents({i})/Children",
            json={"value": [{"Id": i * 100 + 2, "Label": "paged"}]},
            match_querystring=False,
        )
    responses.get(f"{SERVICE_URL}Parents", json={"value": parents}, match_querystring=False)
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "2",  # LOW: park across batches
        "pagination": "nextlink",
    }
    got: list[int] = []
    offset: dict = {}
    parked = False
    for _ in range(50):
        records, offset = c.read_table("Parents__Children", offset, opts)
        rows = [r["Id"] for r in records]
        got.extend(rows)
        pending = offset.get("pending_fetches")
        if not pending:
            break
        parked = True
        # Progress every batch (queue never dropped/stalled) and depth-bounded.
        assert rows, "batch parked a queue but emitted nothing — possible livelock"
        assert len(pending) <= 4
    else:
        raise AssertionError("expand queue never drained")
    assert parked, "never parked — cap too high, test vacuous"
    assert sorted(got) == sorted(i * 100 + 2 for i in range(1, 7))


def test_cursor_completion_floored_at_since():
    """A completing batch whose max cursor sits BELOW the committed
    watermark (lookback overlap after the watermark-defining row was
    deleted) must not regress the committed cursor."""
    c = _make()
    assert c._cursor_max_end_offset(["2020-05-30T00:00:00Z"], "2020-06-01T00:00:00Z") == {
        "cursor": "2020-06-01T00:00:00Z"
    }
    assert c._cursor_max_end_offset(["2020-06-02T00:00:00Z"], "2020-06-01T00:00:00Z") == {
        "cursor": "2020-06-02T00:00:00Z"
    }
    # Same floor on the expand walk's completion fold.
    assert c._build_expand_end_offset(
        [{"M": "2020-05-30T00:00:00Z"}], "M", {"cursor": "2020-06-01T00:00:00Z"}, []
    ) == {"cursor": "2020-06-01T00:00:00Z"}


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
def test_max_records_per_batch_curated_validation():
    """``max_records_per_batch`` caps EMITTED rows — 0/negative would park
    (or livelock) forever without emitting, and a non-numeric value crashed
    with a bare int() traceback. Both get a curated error now."""
    _mock_metadata()
    c = _make()
    for bad in ("0", "-3", "abc"):
        with pytest.raises(ValueError, match="max_records_per_batch"):
            c.read_table(
                "Customers",
                {"cursor": "2020-01-01T00:00:00Z"},
                {"cursor_field": "ModifiedAt", "max_records_per_batch": bad},
            )


@responses.activate
def test_inner_next_link_service_root_relative_resolves_against_root():
    """A per-property ``<Nav>@odata.nextLink`` may be SERVICE-ROOT-relative
    (Hexagon SCApi, SAP Gateway). Resolving it with a plain ``urljoin``
    against the deep continuation URL doubles the ancestor path
    (``Roots(1)/Roots(1)/…`` → 404 + a rebuild-recovery full re-read); it
    must route through ``_resolve_next_link`` like top-level links."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    responses.get(
        f"{SERVICE_URL}Roots",
        json={
            "value": [
                {
                    "Id": 1,
                    "Mids": [],
                    "Mids@odata.nextLink": f"{SERVICE_URL}Roots(1)/Mids?$skiptoken=m",
                }
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {
                    "Id": 10,
                    "Leaves": [],
                    # service-root-relative — restates the path from the root
                    "Leaves@odata.nextLink": "Roots(1)/Mids(10)/Leaves?$skiptoken=z",
                }
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(PROBE_TABLE, {}, {"expand_contained": "true", "pagination": "nextlink"})
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs] == [(1, 10, 1001)]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert not any("Roots(1)/Roots(1)" in u for u in urls), "ancestor path doubled"


@responses.activate
def test_batch_envelope_corrupt_200_retried_once():
    """The ``$batch`` envelope is the LARGEST response the connector ever
    receives — the exact truncated-200 shape ``_fetch_page_payload`` retries
    for on plain GETs. One corrupt envelope must re-POST (GET-only
    sub-requests, safe), not kill the whole read."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    good = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
        ]
    )
    state = {"hydrates": 0}

    def _cb(request):
        body = request.body.decode() if isinstance(request.body, bytes) else request.body
        if "Leaves" in body:
            state["hydrates"] += 1
            if state["hydrates"] == 1:
                return (200, {"Content-Type": "application/json"}, '{"responses": [{"id"')
        return good(request)

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since, "batch_ok": True},  # verdict seeded → no preflight POST
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    assert [r["Id"] for r in recs] == [1001]
    assert state["hydrates"] == 2  # corrupt once, re-POSTed once


@responses.activate
def test_batch_envelope_corrupt_200_twice_raises_actionable():
    """Twice-corrupt envelope: raise with the URL and a truncated body
    excerpt instead of a bare JSONDecodeError."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.add_callback(
        responses.POST,
        f"{SERVICE_URL}$batch",
        callback=lambda request: (200, {"Content-Type": "application/json"}, "{trunc"),
    )
    c = _make()
    with pytest.raises(RuntimeError, match="malformed JSON body twice"):
        recs, _ = c.read_table(
            PROBE_TABLE,
            {"cursor": "2020-01-01T00:00:00Z", "batch_ok": True},
            {
                "cursor_field": "RecordLastModified",
                "cursor_probe": "batch",
                "pagination": "nextlink",
            },
        )
        list(recs)


GUID_CURSOR_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="G" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Account">
        <Key><PropertyRef Name="AccountId"/></Key>
        <Property Name="AccountId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <NavigationProperty Name="Contacts" Type="Collection(G.Contact)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Contact">
        <Key><PropertyRef Name="ContactId"/></Key>
        <Property Name="ContactId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Accounts" EntityType="G.Account"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

_GUID2 = "0a1b2c3d-4e5f-6789-abcd-ef0123456789"


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
def test_partition_walks_keyset_seek_guid_boundaries_bare():
    """Round-28: the partition path's discovery AND per-partition leaf fetches
    build keyset seeks too — both must render guid PK boundaries bare."""
    from urllib.parse import unquote

    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)

    def _accounts_cb(request):
        url = unquote(request.url)
        if f"AccountId gt {_GUID}" in url:
            return (200, {}, json.dumps({"value": []}))
        if "AccountId gt" in url:  # quoted — keep returning the page
            return (200, {}, json.dumps({"value": [{"AccountId": _GUID}]}))
        return (200, {}, json.dumps({"value": [{"AccountId": _GUID}]}))

    def _contacts_cb(request):
        url = unquote(request.url)
        if f"ContactId gt {_GUID2}" in url:
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps({"value": [{"ContactId": _GUID2, "ModifiedAt": "2020-06-01T00:00:00Z"}]}),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Accounts", callback=_accounts_cb)
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Accounts({_GUID})/Contacts", callback=_contacts_cb
    )
    c = _make()
    opts = {
        "expand_contained": "false",
        "pagination": "keyset",
        "num_partitions": "2",
    }
    parts = c.get_partitions("Accounts__Contacts", opts)
    rows = []
    for part in parts:
        rows.extend(c.read_partition("Accounts__Contacts", part, opts))
    assert [r["ContactId"] for r in rows] == [_GUID2]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any(f"AccountId gt {_GUID}" in u for u in urls), "discovery never seeked"
    assert any(f"ContactId gt {_GUID2}" in u for u in urls), "leaf fetch never seeked"
    assert not any(f"gt '{_GUID}'" in u or f"gt '{_GUID2}'" in u for u in urls)


def test_expand_level_types_stash_bounds():
    """The per-level type stash used by the expand queue drains: in-range
    levels return their map, out-of-range/absent stash returns None (sniff
    fallback), never raises."""
    c = _make()
    assert c._expand_level_types(0) is None  # no expand read yet
    c._expand_types_per_level = [{"a": "Edm.Guid"}, {}]
    assert c._expand_level_types(0) == {"a": "Edm.Guid"}
    assert c._expand_level_types(1) == {}
    assert c._expand_level_types(2) is None
    assert c._expand_level_types(-1) is None


@responses.activate
def test_post_batch_corrupt_200_then_error_surfaces_status():
    """Round-28: when the corrupt-200 re-POST comes back a real 4xx, the
    status handling must repeat — a plain 400 carries its status/body (not a
    misleading "missing sub-response id"), and a "too many parts" 400 still
    raises the adaptive-shrink trigger."""
    from databricks.labs.community_connector.sources.odata._contained import _BatchTooManyParts

    state = {"n": 0}

    def _cb_plain_400(request):
        state["n"] += 1
        if state["n"] == 1:
            return (200, {"Content-Type": "application/json"}, "{trunc")
        return (400, {}, json.dumps({"error": {"message": "bad request"}}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb_plain_400)
    c = _make()
    with pytest.raises(RuntimeError, match="failed: 400"):
        c._post_batch([f"{SERVICE_URL}Roots"])

    responses.reset()
    state["n"] = 0

    def _cb_too_many(request):
        state["n"] += 1
        if state["n"] == 1:
            return (200, {"Content-Type": "application/json"}, "{trunc")
        return (400, {}, json.dumps({"error": {"message": "contains too many parts"}}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb_too_many)
    with pytest.raises(_BatchTooManyParts):
        c._post_batch([f"{SERVICE_URL}Roots"])


@responses.activate
def test_latest_offset_honours_pagination_option():
    """Round-28: ``latest_offset`` parses/applies ``pagination=`` like
    ``get_partitions``/``read_partition`` do — the fence probe must not walk
    under a stale or default mode (and an invalid value must raise the same
    curated error)."""
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)

    def _accounts(request):
        from urllib.parse import unquote as _unq

        # Round-45 desc self-check: nothing above the probed max.
        if "gt" in _unq(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Name": "2020-06-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Accounts", callback=_accounts)
    c = _make()
    with pytest.raises(ValueError, match="pagination"):
        c.latest_offset("Accounts__Contacts", {"cursor_field": "Name", "pagination": "bogus"})
    off = c.latest_offset("Accounts__Contacts", {"cursor_field": "Name", "pagination": "nextlink"})
    assert off == {"cursor": "2020-06-01T00:00:00Z"}
    assert c._pagination == "nextlink"


@responses.activate
def test_partition_discovery_rejects_null_cursor_parents():
    """Round-28: null-cursor top parents are visible only to the UNFENCED
    first discovery — every fenced batch's ``cursor gt`` filter hides them
    server-side and their subtrees' changes drop silently. Discovery must
    refuse loudly instead (the serial ancestor path raises on the same
    configuration)."""
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}Accounts",
        json={
            "value": [
                {"AccountId": _GUID, "Name": "2020-06-01T00:00:00Z"},
                {"AccountId": _GUID2, "Name": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(ValueError, match="null"):
        c.get_partitions(
            "Accounts__Contacts",
            {"cursor_field": "Name", "expand_contained": "false", "num_partitions": "2"},
            {},
            {"cursor": "2020-06-01T00:00:00Z"},
        )


@responses.activate
def test_edm_types_for_level_memoizes_failure():
    """Round-28: an unresolvable path must not re-run entity-type resolution
    (and re-format its "Available: ..." error) on every URL build — the
    failure is memoized per (path, namespace)."""
    _mock_metadata()
    c = _make()
    calls = {"n": 0}
    orig = c._entity_type_for

    def _counting(name, namespace=None):
        calls["n"] += 1
        return orig(name, namespace)

    c._entity_type_for = _counting
    assert c._edm_types_for_level(["NoSuchSet"], 0, None) == {}
    assert calls["n"] == 1
    assert c._edm_types_for_level(["NoSuchSet"], 0, None) == {}
    assert calls["n"] == 1  # second call short-circuits on the failure memo
    del c._entity_type_for


# ---------------------------------------------------------------------------
# Round-29 fixes: delta $top removal + maxpagesize, entity-reference
# tombstones, next_link-410 fallback, delta no-progress, partition batch
# null tolerance, select validation, connection-int validation
# ---------------------------------------------------------------------------


@responses.activate
def test_delta_bootstrap_sends_no_top_and_maps_page_size_to_maxpagesize():
    """OData $top is a TOTAL-RESULT limit (§11.2.5.3): sent on a delta
    bootstrap it ends change tracking at page_size rows and silently drops
    the rest of the table forever. The bootstrap must carry NO $top; an
    explicit page_size rides Prefer: odata.maxpagesize instead."""
    _mock_metadata()
    seen = {}

    def _cb(request):
        seen["url"] = request.url
        seen["prefer"] = request.headers.get("Prefer", "")
        return (
            200,
            {"Preference-Applied": "odata.track-changes"},
            json.dumps(_delta_bootstrap_body([{"Id": 1, "Name": "A", "ModifiedAt": "x"}])),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table(
        "Customers", None, {"delta_tracking": "enabled", "page_size": "500"}
    )
    assert [r["Id"] for r in list(records)] == [1]
    assert "$top" not in seen["url"] and "%24top" not in seen["url"]
    assert "odata.track-changes" in seen["prefer"]
    assert "odata.maxpagesize=500" in seen["prefer"]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V1}


@responses.activate
def test_delta_bootstrap_default_pagination_sends_no_top():
    """Even under the default pagination=auto (which injects a client-paging
    page_size for other reads), the delta bootstrap must carry no $top and
    no maxpagesize (the user asked for nothing)."""
    _mock_metadata()
    seen = {}

    def _cb(request):
        seen["url"] = request.url
        seen["prefer"] = request.headers.get("Prefer", "")
        return (
            200,
            {"Preference-Applied": "odata.track-changes"},
            json.dumps(_delta_bootstrap_body([{"Id": 1, "Name": "A", "ModifiedAt": "x"}])),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, _ = c.read_table("Customers", None, {"delta_tracking": "enabled"})
    list(records)
    assert "$top" not in seen["url"] and "%24top" not in seen["url"]
    assert "maxpagesize" not in seen["prefer"]


@responses.activate
def test_delta_tombstone_key_parsed_from_entity_reference():
    """A spec-shaped tombstone carries its key only in @odata.id — the
    connector must parse it (typed: int PK coerced so it MERGE-matches the
    upserts), not emit a keyless no-op tombstone."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                {"@removed": {"reason": "deleted"}, "@odata.id": f"{SERVICE_URL}Customers(2)"},
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
    )
    (tomb,) = list(records)
    assert tomb["Id"] == 2 and isinstance(tomb["Id"], int)
    assert tomb["_deleted"] is True
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}


@responses.activate
def test_delta_v40_deleted_entity_context_is_tombstone_not_sparse_error():
    """A v4.0-format deleted entry ($deletedEntity context + id, no
    @removed) must become a tombstone — pre-fix it was misread as a regular
    entity and tripped the sparse-entity guard with a misleading
    'partial updates' error."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [
                {
                    "@odata.context": f"{SERVICE_URL}$metadata#Customers/$deletedEntity",
                    "id": "Customers(3)",
                    "reason": "deleted",
                },
            ],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, _ = c.read_table(
        "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
    )
    (tomb,) = list(records)
    assert tomb["Id"] == 3
    assert tomb["_deleted"] is True


@responses.activate
def test_delta_tombstone_without_resolvable_key_raises():
    """A tombstone with neither inline keys nor a parsable entity reference
    would MERGE against nothing — the deletion silently lost. Raise."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"@removed": {"reason": "deleted"}}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="resolvable primary key"):
        records, _ = c.read_table(
            "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
        )
        list(records)


def test_tombstone_keys_from_id_shapes():
    """Unit coverage of the entity-reference parser: composite named keys,
    quoted-string un-escaping, bare guids, absolute URLs, and non-matching
    shapes returning None."""
    c = _make()
    types = {"OrderID": "Edm.Int32", "Lang": "Edm.String", "G": "Edm.Guid"}
    assert c._tombstone_keys_from_id(
        "Orders(OrderID=1,Lang='en''x')", ["OrderID", "Lang"], types
    ) == {"OrderID": 1, "Lang": "en'x"}
    assert c._tombstone_keys_from_id(f"https://x/svc/Accounts({_GUID})?x=1", ["G"], types) == {
        "G": _GUID
    }
    assert c._tombstone_keys_from_id("Customers('A,B')", ["Id"], {}) == {"Id": "A,B"}
    assert c._tombstone_keys_from_id("Customers", ["Id"], {}) is None
    assert c._tombstone_keys_from_id("Orders(OrderID=1)", ["OrderID", "Lang"], types) is None
    assert c._tombstone_keys_from_id("Customers(7)", ["A", "B"], {}) is None


@responses.activate
def test_delta_next_link_410_falls_back_to_retained_delta_link():
    """A 410 on the parked mid-pagination next_link must replay the retained
    prior delta_link (changes-since window) — not re-bootstrap the whole
    entity set."""
    _mock_metadata()
    next_link = f"{SERVICE_URL}Customers?$deltatoken=tok-1&$skiptoken=page2"
    responses.add(responses.GET, next_link, status=410)
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 9, "Name": "N", "ModifiedAt": "z"}],
            "@odata.deltaLink": DELTA_LINK_V2,
        },
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"next_link": next_link, "delta_link": DELTA_LINK_V1},
        {"delta_tracking": "enabled"},
    )
    assert [r["Id"] for r in list(records)] == [9]
    assert _drop_lb(offset) == {"delta_link": DELTA_LINK_V2}
    # The plain entity-set bootstrap GET never happened.
    assert not any(call.request.url.rstrip("/").endswith("Customers") for call in responses.calls)


@responses.activate
def test_delta_same_link_with_records_raises_no_progress():
    """Change records + the SAME deltaLink as the prior batch would re-read
    that change set forever — raise like the cursor paths do."""
    _mock_metadata()
    responses.add(
        responses.GET,
        DELTA_LINK_V1,
        json={
            "value": [{"Id": 4, "Name": "D", "ModifiedAt": "w"}],
            "@odata.deltaLink": DELTA_LINK_V1,  # did not advance
        },
    )
    c = _make()
    with pytest.raises(RuntimeError, match="SAME @odata.deltaLink"):
        records, _ = c.read_table(
            "Customers", {"delta_link": DELTA_LINK_V1}, {"delta_tracking": "enabled"}
        )
        list(records)


@responses.activate
def test_partition_null_cursor_parents_allowed_on_batch_invocation():
    """The null-cursor rejection is a STREAMING-fence hazard: the batch
    invocation re-discovers unfenced every run, so null-cursor parents are
    always visible and must keep working (round-28 guard was over-broad)."""
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}Accounts",
        json={
            "value": [
                {"AccountId": _GUID, "Name": "2020-06-01T00:00:00Z"},
                {"AccountId": _GUID2, "Name": None},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    parts = c.get_partitions(
        "Accounts__Contacts",
        {"cursor_field": "Name", "expand_contained": "false", "num_partitions": "2"},
    )
    assert parts and all("top_parent_rows" in p for p in parts)


@responses.activate
def test_select_omitting_pk_or_cursor_raises():
    """A user select that strips the PK desyncs schema from
    read_table_metadata's MERGE keys; one that strips the cursor_field
    silently re-reads the whole table forever under coalesce. Both raise."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="primary-key"):
        c.read_table(
            "Customers",
            {"cursor": "x"},
            {"cursor_field": "ModifiedAt", "select": "Name,ModifiedAt"},
        )
    with pytest.raises(ValueError, match="cursor_field"):
        c.read_table(
            "Customers", {"cursor": "x"}, {"cursor_field": "ModifiedAt", "select": "Id,Name"}
        )


def test_connection_int_options_curated_validation():
    """Connection-level numerics get the same curated validation as the
    per-table numeric options — a negative max_retries previously made the
    retry loops run zero iterations (UnboundLocalError on resp)."""
    for key, bad in (
        ("max_retries", "-1"),
        ("timeout_seconds", "0"),
        ("timeout_seconds", "abc"),
        ("retry_max_delay_seconds", "-5"),
    ):
        with pytest.raises(ValueError, match=key):
            ODataLakeflowConnect({"service_url": SERVICE_URL, key: bad})


# ---------------------------------------------------------------------------
# Round-30 fixes: per-user cache hardening, verdict reset paths, pass-only
# expand_ok, root-wins typing, Edm.Stream delta exclusion
# ---------------------------------------------------------------------------


def test_cache_paths_are_per_user_and_reader_checks_ownership(monkeypatch):
    """Both tempdir caches previously sat at predictable world-writable paths
    keyed only by service_url — the pickle one feeds pickle.load (arbitrary
    code execution if pre-planted by another local user), the JSON one could
    force an unverified $expand read. Paths now embed the owner tag, and the
    readers refuse foreign-owned files."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    tag = odata_mod._cache_owner_tag()
    assert f"_{tag}_" in odata_mod._metadata_cache_path(SERVICE_URL)
    assert f"_{tag}_" in odata_mod._capability_cache_path(SERVICE_URL)

    # Wiring: a file the ownership check rejects is never unpickled.
    c = _make()
    path = odata_mod._metadata_cache_path(SERVICE_URL)
    import pickle as _pickle
    from xml.etree import ElementTree as _ET

    with open(path, "wb") as fh:
        _pickle.dump((METADATA_XML, _ET.fromstring(METADATA_XML)), fh)
    try:
        monkeypatch.setattr(odata_mod, "_cache_file_owned_by_us", lambda p: False)
        assert c._read_metadata_file_cache() is None
        monkeypatch.setattr(odata_mod, "_cache_file_owned_by_us", lambda p: True)
        assert c._read_metadata_file_cache() is not None
    finally:
        import os as _os

        _os.remove(path)


def test_or_filter_ok_scrubbed_on_explicit_nonconsuming_pagination():
    """`or_filter_ok` previously had NO reset path — a wrongly-false verdict
    (e.g. persisted by a pre-typed-seek build's quoted-guid probe) pinned the
    fragile $skip walk forever. An explicit pagination mode that never
    consumes the verdict (skip / nextlink) now scrubs it, giving checkpoints
    an escape hatch."""
    c = _make()
    off = {"cursor": "x", "or_filter_ok": False}
    c.__dict__["_or_filter_ok"] = False
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "skip"}) == {"cursor": "x"}
    assert "_or_filter_ok" not in c.__dict__  # instance memo cleared too
    c.__dict__["_or_filter_ok"] = False
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "nextlink"}) == {"cursor": "x"}
    # Modes that CONSUME the verdict keep it.
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "keyset"})["or_filter_ok"] is False
    assert c._scrub_nonauto_verdicts(dict(off), {"pagination": "auto"})["or_filter_ok"] is False
    assert c._scrub_nonauto_verdicts(dict(off), {})["or_filter_ok"] is False


def test_expand_ok_offset_carries_pass_only():
    """The checkpoint is immortal, so only the PASS may ride it: a memoized
    fail must stay out of the outgoing offset, and a poisoned checkpoint's
    ``expand_ok: false`` must not seed the memo (the preflight re-runs)."""
    c = _make()
    key = c._expand_shared_key("Roots__Mids__Leaves", None)
    c.__dict__["_expand_supported"] = {key: False}
    assert "expand_ok" not in c._merge_capability_caches({"cursor": "y"}, key)
    c.__dict__["_expand_supported"] = {key: True}
    assert c._merge_capability_caches({"cursor": "y"}, key)["expand_ok"] is True
    # Seed side: a false from an old (pre-fix) checkpoint is ignored.
    c2 = _make()
    c2._seed_capability_caches("Roots__Mids__Leaves", None, {"cursor": "x", "expand_ok": False})
    assert not c2.__dict__.get("_expand_supported")
    c2._seed_capability_caches("Roots__Mids__Leaves", None, {"cursor": "x", "expand_ok": True})
    assert c2.__dict__["_expand_supported"] == {key: True}


REDECLARE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="R" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Base">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="V" Type="Edm.Int32"/>
      </EntityType>
      <EntityType Name="Derived" BaseType="R.Base">
        <Property Name="V" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Deriveds" EntityType="R.Derived"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_edm_types_root_wins_matching_schema_resolution():
    """On (spec-forbidden) redeclaring metadata the literal-typing map must
    agree with the SCHEMA resolver (closest-to-root wins) — a seek boundary
    quoted for the leaf declaration while the schema parses the root type
    would desync the wire filter from the declared column."""
    responses.get(f"{SERVICE_URL}$metadata", body=REDECLARE_METADATA_XML, status=200)
    c = _make()
    assert c._edm_types_for_table("Deriveds", None)["V"] == "Edm.Int32"
    schema = c.get_table_schema("Deriveds", {})
    (v_field,) = [f for f in schema.fields if f.name == "V"]
    assert v_field.dataType == IntegerType()


STREAM_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="S" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Doc">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <Property Name="Content" Type="Edm.Stream"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Docs" EntityType="S.Doc"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_delta_stream_property_not_expected_in_payload():
    """Edm.Stream values are media references the JSON payload never carries
    (§11.2.4): the sparse-entity guard must not demand them — pre-fix every
    healthy entity on a stream-bearing type failed delta with a misleading
    'partial updates' error. A genuinely sparse entity still raises."""
    responses.get(f"{SERVICE_URL}$metadata", body=STREAM_METADATA_XML, status=200)
    delta_link = f"{SERVICE_URL}Docs?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={
            "value": [{"Id": 1, "Name": "ok"}],  # no Content — always absent
            "@odata.deltaLink": f"{SERVICE_URL}Docs?$deltatoken=t2",
        },
    )
    c = _make()
    records, _ = c.read_table("Docs", {"delta_link": delta_link}, {"delta_tracking": "enabled"})
    (row,) = list(records)
    assert row["Id"] == 1 and row["_deleted"] is False

    responses.add(
        responses.GET,
        f"{SERVICE_URL}Docs?$deltatoken=t2",
        json={
            "value": [{"Id": 2}],  # missing Name — genuinely sparse
            "@odata.deltaLink": f"{SERVICE_URL}Docs?$deltatoken=t3",
        },
    )
    with pytest.raises(RuntimeError, match="missing"):
        records, _ = c.read_table(
            "Docs",
            {"delta_link": f"{SERVICE_URL}Docs?$deltatoken=t2"},
            {"delta_tracking": "enabled"},
        )
        list(records)


# ---------------------------------------------------------------------------
# Round-31 fixes: metadata process-cache TTL, symlink-safe cache writes,
# TypeDefinition typing, __-named flat sets, 408 retry, auth_type-gated
# refresh, wall-clock token deadline, jittered backoff, Decimal literals
# ---------------------------------------------------------------------------


METADATA_XML_V2 = METADATA_XML.replace('Name="Customers"', 'Name="CustomersV2"')


@responses.activate
def test_metadata_process_cache_honors_ttl(monkeypatch):
    """The process-wide _METADATA_CACHE previously had NO TTL check — in a
    long-running driver $metadata never refreshed regardless of the setting.
    Entries are now stamped and expire after metadata_cache_ttl_seconds."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    responses.get(f"{SERVICE_URL}$metadata", body=METADATA_XML, status=200)
    a = _make({"metadata_cache_ttl_seconds": "60"})
    assert "Customers" in a.list_tables()

    # Serve a changed document and jump past the TTL (both the process
    # stamp and the on-disk mtime check read the same clock).
    responses.replace(responses.GET, f"{SERVICE_URL}$metadata", body=METADATA_XML_V2, status=200)
    real_time = odata_mod.time.time
    monkeypatch.setattr(odata_mod.time, "time", lambda: real_time() + 61)
    b = _make({"metadata_cache_ttl_seconds": "60"})
    tables = b.list_tables()
    assert "CustomersV2" in tables and "Customers" not in tables


@responses.activate
def test_metadata_cache_ttl_zero_disables_process_cache():
    """metadata_cache_ttl_seconds=0 is documented as 'disable' but previously
    only skipped the on-disk pickle — the process dict still served (and was
    fed) stale documents. TTL 0 now bypasses the process layer entirely."""
    responses.get(f"{SERVICE_URL}$metadata", body=METADATA_XML, status=200)
    a = _make({"metadata_cache_ttl_seconds": "0"})
    assert "Customers" in a.list_tables()

    responses.replace(responses.GET, f"{SERVICE_URL}$metadata", body=METADATA_XML_V2, status=200)
    b = _make({"metadata_cache_ttl_seconds": "0"})
    tables = b.list_tables()
    assert "CustomersV2" in tables and "Customers" not in tables


def test_private_tmp_write_refuses_preplanted_symlink(tmp_path, monkeypatch):
    """Cache writers previously opened a PREDICTABLE `{path}.{pid}.tmp` name
    with plain open() — a pre-planted symlink there redirected the write onto
    any victim-writable file (and os.replace then hid the evidence). The tmp
    name now embeds os.urandom and opens O_CREAT|O_EXCL|O_NOFOLLOW, so a
    planted name makes the write fail instead of following the link."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    victim = tmp_path / "victim.txt"
    victim.write_bytes(b"precious")
    target = tmp_path / "cache.json"
    real_urandom = os.urandom
    # Deterministic tmp name so the attacker (this test) can pre-plant it.
    monkeypatch.setattr(odata_mod.os, "urandom", lambda n: b"\x00" * n)
    planted = f"{target}.{odata_mod.os.getpid()}.{'00000000'}.tmp"
    odata_mod.os.symlink(victim, planted)
    try:
        assert odata_mod._replace_with_private_tmp(str(target), b"attacker-view") is False
        assert victim.read_bytes() == b"precious"  # not clobbered
        assert not target.exists()
    finally:
        if odata_mod.os.path.lexists(planted):
            odata_mod.os.remove(planted)

    # Clean path: write lands atomically with owner-only permissions.
    monkeypatch.setattr(odata_mod.os, "urandom", real_urandom)
    assert odata_mod._replace_with_private_tmp(str(target), b"payload") is True
    assert target.read_bytes() == b"payload"
    assert (target.stat().st_mode & 0o777) == 0o600


def test_capability_flush_writes_private_file():
    """Wiring: the capability mirror goes through the hardened writer, so the
    published file carries owner-only permissions (pre-fix: umask default)."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    path = odata_mod._capability_cache_path(SERVICE_URL)
    odata_mod._capability_cache_flush(SERVICE_URL, "{}")
    try:
        assert (os.stat(path).st_mode & 0o777) == 0o600
    finally:
        os.remove(path)


def test_cache_ownership_check_uses_lstat(tmp_path):
    """The ownership check previously followed symlinks (os.stat): a foreign
    symlink pointing at a victim-owned file passed. lstat judges the link
    itself — pinned via a dangling symlink, where stat raises (False) but
    lstat sees our own link (True; the subsequent open just misses)."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "nonexistent-target")
    assert odata_mod._cache_file_owned_by_us(str(dangling)) is True


TYPEDEF_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="T" Alias="ta" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <TypeDefinition Name="Code" UnderlyingType="Edm.String"/>
      <TypeDefinition Name="Qty" UnderlyingType="Edm.Int64"/>
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Code" Type="T.Code"/>
        <Property Name="Qty" Type="ta.Qty"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Items" EntityType="T.Item"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_typedef_properties_resolve_to_underlying_edm_type():
    """A property typed via <TypeDefinition> previously recorded the
    definition name verbatim, falling out of typed literal rendering — an
    ISO-looking string on an Edm.String-backed definition rendered BARE
    (invalid predicate), the exact misfire typed rendering exists to stop.
    The index now resolves definitions to their underlying primitive,
    accepting both namespace- and alias-qualified references."""
    from databricks.labs.community_connector.sources.odata._contained import (
        odata_literal_typed,
    )

    responses.get(f"{SERVICE_URL}$metadata", body=TYPEDEF_METADATA_XML, status=200)
    c = _make()
    types = c._edm_types_for_table("Items", None)
    assert types["Code"] == "Edm.String"
    assert types["Qty"] == "Edm.Int64"
    assert odata_literal_typed("2024-01-01", types["Code"]) == "'2024-01-01'"
    assert odata_literal_typed("42", types["Qty"]) == "42"


@responses.activate
def test_namespace_option_accepts_schema_alias():
    """CSDL lets type references use the schema Alias interchangeably with
    its Namespace; the `namespace` table option now does too (previously an
    alias failed with the type-only-schema error)."""
    responses.get(f"{SERVICE_URL}$metadata", body=TYPEDEF_METADATA_XML, status=200)
    c = _make()
    schema = c.get_table_schema("Items", {"namespace": "ta"})
    assert {f.name for f in schema.fields} >= {"Id", "Code", "Qty"}


@responses.activate
def test_list_tables_in_namespace_rejects_multi_segment():
    """OData namespaces are single-level; a multi-segment path names nothing.
    Previously segment[0]'s tables were returned, fabricating rows under a
    nonexistent namespace path."""
    _mock_metadata()
    c = _make()
    assert c.list_tables_in_namespace(["Demo"]) == ["Customers", "Orders"]
    assert c.list_tables_in_namespace(["Demo", "bogus"]) == []
    assert c.list_tables_in_namespace([]) == []


DUNDER_SET_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="My__Set" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_flat_entity_set_with_double_underscore_is_readable():
    """CSDL SimpleIdentifiers legally allow consecutive underscores, so
    list_tables can emit `My__Set` — but the read path previously split it
    into a nonexistent containment path and failed with a misleading
    "Entity set 'My' not found". A verbatim flat declaration now wins over
    the containment-path interpretation everywhere."""
    responses.get(f"{SERVICE_URL}$metadata", body=DUNDER_SET_METADATA_XML, status=200)
    responses.get(
        f"{SERVICE_URL}My__Set",
        json={"value": [{"Id": 1, "Name": "x"}]},
    )
    c = _make()
    assert "My__Set" in c.list_tables()
    assert {f.name for f in c.get_table_schema("My__Set", {}).fields} == {"Id", "Name"}
    rows, _ = c.read_table("My__Set", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in rows] == [1]


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


def test_decimal_nonfinite_renders_odata_wire_literals():
    """Decimal('Infinity')/NaN previously fell through the float-only guard
    and rendered Python's spellings (invalid on the wire)."""
    from decimal import Decimal as _D

    assert _odata_literal(_D("Infinity")) == "INF"
    assert _odata_literal(_D("-Infinity")) == "-INF"
    assert _odata_literal(_D("NaN")) == "NaN"
    assert _odata_literal(_D("1.5")) == "1.5"


DUPSET_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C1">
        <EntitySet Name="Things" EntityType="D.Thing"/>
      </EntityContainer>
      <EntityContainer Name="C2">
        <EntitySet Name="Things" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_duplicate_set_in_single_namespace_gets_precise_error():
    """Same set name twice in ONE namespace (malformed CSDL): the old message
    suggested the `namespace` option, which cannot disambiguate here."""
    responses.get(f"{SERVICE_URL}$metadata", body=DUPSET_METADATA_XML, status=200)
    c = _make()
    with pytest.raises(ValueError, match="more than once in namespace"):
        c.get_table_schema("Things", {})


# ---------------------------------------------------------------------------
# Round-32 fixes: tombstone NULL padding, exclusion-aware memos, stream
# nullability, shared delta verdict, dunder-prefix children, rotation stash
# ---------------------------------------------------------------------------


NONNULL_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="N" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Customers" EntityType="N.Customer"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_delta_tombstone_parses_against_non_nullable_schema():
    """A tombstone carries only its keys, but the framework parser rejects
    a non-nullable column that is ABSENT (while accepting an explicit
    null) — pre-fix the first delete on any schema with a
    Nullable="false" non-key property killed the batch. Tombstones are
    now padded with explicit NULLs for every remaining schema column."""
    from databricks.labs.community_connector.libs.utils import parse_value

    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_METADATA_XML, status=200)
    delta_link = f"{SERVICE_URL}Customers?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={
            "value": [{"Id": 2, "@removed": {"reason": "deleted"}}],
            "@odata.deltaLink": f"{SERVICE_URL}Customers?$deltatoken=t2",
        },
    )
    c = _make()
    schema = c.get_table_schema("Customers", {"delta_tracking": "enabled"})
    records, _ = c.read_table(
        "Customers", {"delta_link": delta_link}, {"delta_tracking": "enabled"}
    )
    (tombstone,) = list(records)
    assert tombstone["Name"] is None and tombstone["ModifiedAt"] is None
    # The framework must accept the padded record against the declared
    # (non-nullable Name) schema — this is the actual pre-fix crash site.
    parsed = parse_value(tombstone, schema)
    assert parsed["Id"] == 2 and parsed["_deleted"] is True


@responses.activate
def test_fields_and_pk_memos_are_exclusion_aware():
    """The `_fields_for`/`_primary_keys_for` memos embedded the
    exclusion-FILTERED FK columns under a (table, namespace)-only key, so
    a shared instance froze schema AND composite PK at the first call's
    `exclude_ancestor_columns` while row stamping followed the current
    one — hard parse failures one way, silent MERGE collisions the other."""
    _mock_nested_metadata()
    c = _make()
    with_fk = c.get_table_schema("Parents__Children", {})
    assert "Parents_Id" in {f.name for f in with_fk.fields}
    without_fk = c.get_table_schema("Parents__Children", {"exclude_ancestor_columns": "Parents_Id"})
    assert "Parents_Id" not in {f.name for f in without_fk.fields}
    # And back again on the SAME instance — pre-fix this returned the
    # excluded shape from the memo.
    again = c.get_table_schema("Parents__Children", {})
    assert "Parents_Id" in {f.name for f in again.fields}

    # Composite PK follows the same rule.
    pks = c.read_table_metadata("Parents__Children", {})["primary_keys"]
    assert pks == ["Parents_Id", "Id"]
    pks_excl = c.read_table_metadata(
        "Parents__Children", {"exclude_ancestor_columns": "Parents_Id"}
    )["primary_keys"]
    assert pks_excl == ["Id"]
    pks_back = c.read_table_metadata("Parents__Children", {})["primary_keys"]
    assert pks_back == ["Parents_Id", "Id"]


NONNULL_STREAM_METADATA_XML = STREAM_METADATA_XML.replace(
    '<Property Name="Content" Type="Edm.Stream"/>',
    '<Property Name="Content" Type="Edm.Stream" Nullable="false"/>',
)


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
def test_delta_auto_verdict_shared_across_instances():
    """Schema inference and the streaming read run in different forked
    workers; an instance-only probe verdict lets a flapping server desync
    the declared schema from the emitted rows. The definitive verdict now
    rides the process/file capability cache: a second instance resolves
    delta WITHOUT re-probing (no probe mock is registered for it — an
    attempted probe would come back as no-verdict and resolve False)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": []},
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c1 = _make()
    assert c1._delta_active_for("Customers", {"delta_tracking": "auto"}) is True
    responses.reset()  # no $metadata, no probe endpoint from here on
    c2 = _make()
    c2._metadata = c1._metadata  # metadata via cache; probe is the question
    assert c2._delta_active_for("Customers", {"delta_tracking": "auto"}) is True


DUNDER_KIDS_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Kid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Note" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Kids" Type="Collection(D.Kid)" ContainsTarget="true"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="My__Set" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_dunder_set_contained_children_are_readable():
    """Round 31 made a flat `My__Set` readable, but its contained children
    (`My__Set__Kids`) still split into a nonexistent containment path —
    the same listed-but-unreadable class one level deeper. The longest
    declared flat prefix now becomes the root segment."""
    responses.get(f"{SERVICE_URL}$metadata", body=DUNDER_KIDS_METADATA_XML, status=200)
    responses.get(f"{SERVICE_URL}My__Set", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}My__Set(1)/Kids",
        json={"value": [{"Id": 7, "Note": "n"}]},
    )
    c = _make()
    assert c.list_tables() == ["My__Set", "My__Set__Kids"]
    assert c._table_segments("My__Set__Kids") == ["My__Set", "Kids"]
    schema = c.get_table_schema("My__Set__Kids", {})
    assert {f.name for f in schema.fields} == {"My__Set_Id", "Id", "Note"}
    records, _ = c.read_table("My__Set__Kids", None, {})
    (row,) = list(records)
    assert row["Id"] == 7 and row["My__Set_Id"] == 1


COLLIDE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Sub">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Set" Type="Collection(D.Sub)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="My" EntityType="D.Root"/>
        <EntitySet Name="My__Set" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_flat_set_shadows_colliding_containment_path_and_listing_dedups():
    """When a declared flat `My__Set` collides with the containment-path
    spelling of `My`→`Set`, the flat set wins (documented shadowing) and
    namespace listings must not fabricate a duplicate table entry."""
    responses.get(f"{SERVICE_URL}$metadata", body=COLLIDE_METADATA_XML, status=200)
    c = _make()
    listed = c.list_tables_in_namespace(["D"])
    assert listed.count("My__Set") == 1
    assert c._table_segments("My__Set") is None  # flat wins


@responses.activate
def test_value_null_tolerated_as_empty_page():
    """A spec-invalid `"value": null` previously crashed iterating None;
    it now reads as an empty page."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": None}, match_querystring=False)
    c = _make({"token": "t"})
    records, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert list(records) == []


@responses.activate
def test_malformed_delta_entry_raises_precise_error():
    """A null entry in a delta `value` array previously died with an
    AttributeError inside the tombstone sniff; it now raises a precise
    RuntimeError naming the malformed entry."""
    _mock_metadata()
    delta_link = f"{SERVICE_URL}Customers?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={"value": [None], "@odata.deltaLink": f"{SERVICE_URL}Customers?$deltatoken=t2"},
    )
    c = _make()
    with pytest.raises(RuntimeError, match="malformed entry"):
        records, _ = c.read_table(
            "Customers", {"delta_link": delta_link}, {"delta_tracking": "enabled"}
        )
        list(records)


@responses.activate
def test_alias_error_message_names_requested_alias():
    """A user passing the schema Alias should see the alias they typed in
    the not-found error (with the canonical resolution alongside), not a
    namespace name they never wrote."""
    responses.get(f"{SERVICE_URL}$metadata", body=TYPEDEF_METADATA_XML, status=200)
    c = _make()
    with pytest.raises(ValueError, match=r"'ta' \(alias of 'T'\)"):
        c.get_table_schema("Nope", {"namespace": "ta"})


# ---------------------------------------------------------------------------
# Round-33 fixes: vanished-parent tolerance, per-batch null-cursor probe,
# dispatch-validated shape options, delta_ok reset path
# ---------------------------------------------------------------------------


@responses.activate
def test_partitioned_read_skips_vanished_parent():
    """The partition descriptor is frozen at planning, so a parent deleted
    mid-batch previously 404'd every Spark task retry and killed the
    streaming query. The vanished chain is now skipped with a warning and
    the surviving parents' rows still emit."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"error": "gone"}, status=404)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "ok"}]},
    )
    c = _make()
    rows = list(
        c.read_partition(
            "Parents__Children",
            {"top_parent_rows": [{"Id": 1}, {"Id": 2}], "cursor_lower": None},
            {},
        )
    )
    assert [r["Id"] for r in rows] == [21]
    assert rows[0]["Parents_Id"] == 2


@responses.activate
def test_partitioned_stream_null_cursor_probe_catches_late_arrivals():
    """The round-29 null-cursor guard inspected discovery rows only — dead
    after batch 1, because the fence filter hides null-cursor parents
    server-side. Fenced batches now run a one-request ``eq null`` probe, so
    a parent INSERTED with a null cursor mid-stream raises instead of its
    subtree being silently dropped forever."""
    from urllib.parse import unquote as _unq

    _mock_nested_metadata()

    def _parents(request):
        if "eq null" in _unq(request.url):
            # The mid-stream-inserted null-cursor parent.
            return (200, {}, '{"value": [{"Id": 9}]}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "2024-05-02T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    with pytest.raises(ValueError, match="null 'Name'"):
        c.get_partitions(
            "Parents__Children",
            {"cursor_field": "Name"},
            {"cursor": "2024-05-01T00:00:00Z"},
            {"cursor": "2024-05-02T00:00:00Z"},
        )


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


@responses.activate
def test_delta_ok_purged_on_explicit_setting():
    """`delta_ok` previously had no reset path — asymmetric with
    `expand_ok`/`cursor_probe_ok`, whose explicit non-auto values purge the
    shared cache entry so a later switch back to auto re-probes."""
    _mock_metadata()
    c = _make()
    c._store_capability("delta_ok", False, table_name="Customers")
    assert c._delta_active_for("Customers", {"delta_tracking": "disabled"}) is False
    assert c._cached_capability("delta_ok", table_name="Customers") is None
    c._store_capability("delta_ok", False, table_name="Customers")
    assert c._delta_active_for("Customers", {"delta_tracking": "enabled"}) is True
    assert c._cached_capability("delta_ok", table_name="Customers") is None
    # auto keeps (and uses) the verdict.
    c._store_capability("delta_ok", True, table_name="Customers")
    assert c._delta_active_for("Customers", {"delta_tracking": "auto"}) is True
    assert c._cached_capability("delta_ok", table_name="Customers") is True


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
def test_select_restricted_delta_tombstone_pads_to_selected_schema():
    """With `select`, the tombstone pad set and the declared schema must be
    the SAME subset: selected non-key columns padded to None, non-selected
    columns absent from both — the framework parse is the referee."""
    from databricks.labs.community_connector.libs.utils import parse_value

    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_METADATA_XML, status=200)
    delta_link = f"{SERVICE_URL}Customers?$deltatoken=t1"
    responses.add(
        responses.GET,
        delta_link,
        json={
            "value": [{"Id": 3, "@removed": {"reason": "deleted"}}],
            "@odata.deltaLink": f"{SERVICE_URL}Customers?$deltatoken=t2",
        },
    )
    c = _make()
    opts = {"delta_tracking": "enabled", "select": "Id,Name"}
    schema = c.get_table_schema("Customers", opts)
    assert {f.name for f in schema.fields} == {"Id", "Name", "_deleted", "_lc_sequence"}
    records, _ = c.read_table("Customers", {"delta_link": delta_link}, opts)
    (tombstone,) = list(records)
    assert tombstone["Name"] is None and "ModifiedAt" not in tombstone
    parsed = parse_value(tombstone, schema)
    assert parsed["Id"] == 3 and parsed["_deleted"] is True


# ---------------------------------------------------------------------------
# Round-34 fixes: cross-origin credential guard, $batch non-dict body re-issue,
# partitioned contained_fetch validation
# ---------------------------------------------------------------------------


@responses.activate
def test_cross_host_nextlink_refused_not_credential_leak():
    """A server-supplied @odata.nextLink pointing at a DIFFERENT host must be
    refused — following it would send the session's Authorization header to
    that host (requests' own cross-host redirect auth-stripping never engages
    because the connector builds the next request directly). Same-origin
    nextLinks still work."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [{"Id": 1, "Name": "A"}],
            "@odata.nextLink": "https://evil.attacker.com/collect?p=2",
        },
        match_querystring=False,
    )
    c = _make({"token": "secret-xyz"})
    with pytest.raises(PermissionError, match="different origin"):
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)
    # The attacker host was never contacted.
    assert not any("evil.attacker.com" in call.request.url for call in responses.calls)


@responses.activate
def test_same_origin_nextlink_still_followed():
    """The origin guard must not break legitimate same-host pagination
    (absolute nextLink on the service's own host)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [{"Id": 1, "Name": "A"}],
            "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=p2",
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Customers?$skiptoken=p2",
        json={"value": [{"Id": 2, "Name": "B"}]},
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in list(rows)] == [1, 2]


@responses.activate
def test_batch_subresponse_string_body_reissued_not_dropped():
    """A 2xx $batch sub-response whose `body` is a JSON STRING (spec-legal for
    non-JSON media) previously drained to rows=[] — silently dropping that
    parent's whole collection and letting the cursor walk advance past it. It's
    now re-issued as a plain GET so every row still arrives."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            if "Parents(1)/Children" in r["url"]:
                out.append(
                    {"id": r["id"], "status": 200, "body": {"value": [{"Id": 11, "Label": "a"}]}}
                )
            elif "Parents(2)/Children" in r["url"]:
                # Body serialized as a STRING rather than an inline object.
                out.append(
                    {"id": r["id"], "status": 200, "body": '{"value":[{"Id":21,"Label":"b"}]}'}
                )
            else:  # capability preflight
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 21, "Label": "b"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    rows = sorted((r["Parents_Id"], r["Id"]) for r in recs)
    assert rows == [(1, 11), (2, 21)]  # parent 2 not silently dropped
    assert any(
        call.request.method == "GET" and "Parents(2)/Children" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_partitioned_contained_fetch_garbage_rejected():
    """`contained_fetch` garbage was silently accepted on the partitioned
    streaming path — validation lived only in read_table's dispatch, which a
    partitioned stream never routes through. is_partitioned now parses it."""
    _mock_nested_metadata()
    c = _make({"num_partitions": "2", "contained_fetch": "garbadge"})
    with pytest.raises(ValueError, match="contained_fetch"):
        c.is_partitioned("Parents__Children")


# ---------------------------------------------------------------------------
# Round-35 fixes: cross-host redirect credential guard, $batch error-body
# re-issue, non-delta schema padding
# ---------------------------------------------------------------------------


@responses.activate
def test_cross_host_redirect_refused_not_credential_leak():
    """Round 34 guarded the @odata.nextLink vector but session.request
    followed 3xx redirects internally with the credential attached — and
    requests strips only Authorization cross-host, leaking api_key /
    extra_headers. allow_redirects=False + the origin check now refuses an
    off-host Location."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=302,
        headers={"Location": "https://evil.attacker.com/collect"},
    )
    captured = {}

    def _evil(request):
        captured["x-api-key"] = request.headers.get("x-api-key")
        captured["X-Tenant"] = request.headers.get("X-Tenant")
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, "https://evil.attacker.com/collect", callback=_evil)
    c = _make(
        {
            "auth_type": "api_key",
            "api_key": "SECRET-APIKEY",
            "extra_headers": "X-Tenant:SECRET-TENANT",
        }
    )
    with pytest.raises(PermissionError, match="different origin"):
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)
    # The attacker host never received the request (never followed).
    assert captured == {}
    assert not any("evil.attacker.com" in call.request.url for call in responses.calls)


@responses.activate
def test_same_host_redirect_followed():
    """A same-origin 3xx (server-side URL normalization) is still followed
    manually so legitimate redirects keep working."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=301,
        headers={"Location": f"{SERVICE_URL}Customers/"},
    )
    responses.get(f"{SERVICE_URL}Customers/", json={"value": [{"Id": 1, "Name": "A"}]})
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in list(rows)] == [1]


@responses.activate
def test_batch_subresponse_error_body_under_200_reissued():
    """A 200-status $batch sub-response whose body is an OData error envelope
    ({"error": {...}}, no "value") is a dict, so round 34's non-dict gate let
    it through — draining to rows=[] and silently dropping that parent. It's
    now re-issued as a plain GET."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            elif "Parents(2)/Children" in r["url"]:
                # 200 status but an error envelope body (spec-violating, but
                # seen in the wild) — must not drain to [].
                out.append({"id": r["id"], "status": 200, "body": {"error": {"message": "x"}}})
            else:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 22}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    ids = sorted(r["Id"] for r in recs)
    assert ids == [11, 22]  # parent 2 recovered via plain GET, not dropped


@responses.activate
def test_batch_empty_collection_dict_body_not_reissued():
    """A genuine empty-collection 200 ({"value": []}) is a drainable dict and
    must NOT be re-issued — only error/non-dict bodies are."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            elif "Parents(2)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": []}})
            else:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 1}]}})
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    assert sorted(r["Id"] for r in recs) == [11]
    # No plain-GET re-issue happened for parent 2 (empty is legit).
    assert not any(
        call.request.method == "GET" and "Parents(2)/Children" in call.request.url
        for call in responses.calls
    )


NONNULL_FLAT_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="N" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Req" Type="Edm.String" Nullable="false"/>
        <Property Name="Opt" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Items" EntityType="N.Item"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_non_delta_read_pads_omitted_nonnullable_property():
    """A non-delta read no longer hard-fails when the server omits a
    Nullable="false" property (OData servers may omit null-valued
    properties) — the emit boundary pads it to explicit None, which
    parse_value accepts, so the row flows instead of killing the batch."""
    from databricks.labs.community_connector.libs.utils import parse_value

    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_FLAT_METADATA_XML, status=200)
    responses.get(f"{SERVICE_URL}Items", json={"value": [{"Id": 2, "Opt": "y"}]})  # Req omitted
    c = _make({"token": "t"})
    schema = c.get_table_schema("Items", {})
    rows, _ = c.read_table("Items", None, {"pagination": "nextlink"})
    (row,) = list(rows)
    assert row["Req"] is None and row["Opt"] == "y"
    # And the padded row parses against the (non-nullable Req) schema.
    parsed = parse_value(row, schema)
    assert parsed["Id"] == 2 and parsed["Req"] is None


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


@responses.activate
def test_expand_url_user_select_lands_on_leaf_clause_not_top():
    """The ``select`` option is LEAF-scoped (docs, schema derivation, and the
    N+1 path all agree) — in expand mode it must ride the innermost
    ``$expand(...)`` clause, not the top segment's URL, where a leaf-only
    column 400s the server and a cross-level name silently mis-projects."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children"], {"select": "Id,Label", "page_size": "100"})
    top, expand_clause = url.split("$expand=", 1)
    assert "$select" not in top
    assert "$select=Id,Label" in expand_clause


@responses.activate
def test_expand_url_user_select_merges_with_cursor_select_at_leaf():
    """When a cursor projection targets the leaf level too, the user's leaf
    ``select`` merges with it — deduped, user's order first."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(
        ["Parents", "Children"],
        {"select": "Id,Label"},
        cursor_level=1,
        cursor_filter="ModifiedAt gt 2020-01-01T00:00:00Z",
        cursor_order="ModifiedAt asc,Id asc",
        cursor_select="ModifiedAt,Label",
    )
    expand_clause = url.split("$expand=", 1)[1]
    assert "$select=Id,Label,ModifiedAt" in expand_clause


@responses.activate
def test_data_url_3xx_without_location_curated_error():
    """A 3xx the same-origin follow loop can't act on (no Location header)
    must raise naming the status — not flow onward and die as a bare
    'Expecting value' JSON parse error on the empty body."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=302)
    c = _make({"token": "t"})
    with pytest.raises(RuntimeError, match="HTTP 302 without a followable"):
        rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
        list(rows)


@responses.activate
def test_batch_subresponse_dict_without_value_reissued():
    """A 200 $batch sub-response whose dict body has NEITHER "value" NOR
    "error" (an OData-v2-style {"d": …} gateway shape, a JSON proxy page)
    drained to rows=[] — a silent parent skip the cursor walk then advances
    past. Every sub-request is a collection GET, so a drainable body is
    exactly one whose "value" is a LIST; anything else is re-issued."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            else:
                # v2-gateway shape: success status, no "value", no "error".
                out.append(
                    {"id": r["id"], "status": 200, "body": {"d": {"results": [{"Id": 999}]}}}
                )
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 22}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    ids = sorted(r["Id"] for r in recs)
    assert ids == [11, 22]  # parent 2 recovered via plain GET, not dropped


@responses.activate
def test_batch_subresponse_null_value_reissued():
    """{"value": null} (non-list value) used to crash the drain loudly
    (TypeError iterating None); it is now re-issued as a plain GET like every
    other undrainable body."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}, {"Id": 2}]})

    def _cb(request):
        out = []
        for r in json.loads(request.body)["requests"]:
            if "Parents(1)/Children" in r["url"]:
                out.append({"id": r["id"], "status": 200, "body": {"value": [{"Id": 11}]}})
            else:
                out.append({"id": r["id"], "status": 200, "body": {"value": None}})
        return (200, {}, json.dumps({"responses": out}))

    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=_cb)
    responses.get(
        f"{SERVICE_URL}Parents(2)/Children",
        json={"value": [{"Id": 22}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table("Parents__Children", {}, {"expand_contained": "false"})
    assert sorted(r["Id"] for r in recs) == [11, 22]


def test_lookback_history_records_cycle_span_not_final_batch(monkeypatch):
    """A capped walk spanning several batches must record the WHOLE cycle's
    wall-clock span (first capped batch to completion, trigger intervals
    included) into lb_history — the churn-exposure window of the cycle is the
    full span, not the final batch's drain time. In-flight batches stamp
    lb_cycle_started once; completion consumes and clears it."""
    c = _make()
    now = {"t": 1000.0}
    monkeypatch.setattr(time, "time", lambda: now["t"])
    # Batch 1 (capped, walk took 5s): anchors the cycle at 995.0.
    out1 = c._attach_lookback_state({"parent_idx": 1}, {}, True, 5.0)
    assert out1["lb_cycle_started"] == 995.0
    # Batch 2 (still capped, later trigger): anchor carried unchanged.
    now["t"] = 1300.0
    out2 = c._attach_lookback_state({"parent_idx": 2}, out1, True, 3.0)
    assert out2["lb_cycle_started"] == 995.0
    # Completion at t=1600 with a 2s final drain: records the 605s span
    # (1600 - 995), not 2s, and drops the anchor from the offset.
    now["t"] = 1600.0
    out3 = c._attach_lookback_state({"cursor": "x"}, out2, False, 2.0)
    assert out3["lb_history"] == [605.0]
    assert "lb_cycle_started" not in out3
    # Single-batch walk (no anchor): records its own elapsed as before.
    out4 = c._attach_lookback_state({"cursor": "y"}, {"cursor": "x"}, False, 2.5)
    assert out4["lb_history"] == [2.5]


def test_api_key_header_stripped_and_empty_defaults():
    """A padded api_key_header is stripped (requests raises an uncurated
    InvalidHeader on whitespace); an explicitly-empty one falls back to the
    documented x-api-key default instead of an empty header name."""
    c = _make({"auth_type": "api_key", "api_key": "k", "api_key_header": " X-My-Key "})
    assert c._get_session().headers["X-My-Key"] == "k"
    c2 = _make({"auth_type": "api_key", "api_key": "k", "api_key_header": ""})
    assert c2._get_session().headers["x-api-key"] == "k"


def test_api_key_header_invalid_raises_curated():
    """Garbage header names fail fast with the option named, not at first
    request deep inside requests."""
    c = _make({"auth_type": "api_key", "api_key": "k", "api_key_header": "bad header"})
    with pytest.raises(ValueError, match="api_key_header"):
        c._get_session()


@responses.activate
def test_select_star_keeps_full_schema():
    """``select=*`` is valid OData (§11.2.4.2.1: all structural properties)
    and passes option validation — the schema must stay unfiltered too. The
    literal-name filter used to drop every non-FK column (flat tables then
    failed the non-empty-schema check outright)."""
    _mock_metadata()
    c = _make({"token": "t"})
    schema = c.get_table_schema("Customers", {"select": "*"})
    assert [f.name for f in schema.fields] == ["Id", "Name", "ModifiedAt"]


def test_verbose_http_logging_strict_parse():
    """Enum-strict like every other option: a typo'd "1"/"yes" would
    otherwise silently mean OFF — the opposite of what a triaging user
    asked for."""
    with pytest.raises(ValueError, match="verbose_http_logging"):
        _make({"token": "t", "verbose_http_logging": "1"})


def test_cursor_nulls_empty_string_defaults():
    """cursor_nulls="" means unset → coalesce default, consistent with
    delta_tracking/pagination/expand_contained empty-string handling."""
    c = _make({"token": "t"})
    assert c._parse_cursor_nulls({"cursor_nulls": ""}) == ("coalesce", 2000)


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
def test_delta_auto_fallback_stamps_delta_ok_into_offset():
    """A definitive negative delta probe under ``delta_tracking=auto`` pins
    the fallback decision into the outgoing offset (``delta_ok: false``), so
    the stream's read shape can't flip later (see the offset-wins test
    below)."""
    _mock_metadata()
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Customers",
        callback=lambda _r: (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}'),
    )
    c = _make()
    records, offset = c.read_table("Customers", {}, {"delta_tracking": "auto"})
    list(records)
    assert offset.get("delta_ok") is False


@responses.activate
def test_delta_auto_offset_verdict_wins_over_flapping_server():
    """An offset carrying ``delta_ok: false`` keeps the stream on the
    fallback path even when the server NOW acknowledges the delta probe
    (Preference-Applied flap after the 15-min shared cache expired). Without
    the pin, the read would flip onto the delta path mid-stream and emit
    ``_deleted``/``_lc_sequence`` columns the setup-frozen schema never
    declared — the framework parser drops them silently and a tombstone
    MERGEs as a live all-null row."""
    _mock_metadata()
    prefer_seen = {"n": 0}

    def _cb(request):
        if request.headers.get("Prefer"):
            prefer_seen["n"] += 1
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(_delta_bootstrap_body([{"Id": 9, "Name": "Z", "ModifiedAt": "y"}])),
            )
        return (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table("Customers", {"delta_ok": False}, {"delta_tracking": "auto"})
    rows = list(records)
    # Snapshot fallback held: no probe ran, no synthetic columns emitted.
    assert prefer_seen["n"] == 0
    assert rows and all("_deleted" not in r and "_lc_sequence" not in r for r in rows)
    assert offset.get("delta_ok") is False


@responses.activate
def test_delta_ok_scrubbed_on_explicit_setting():
    """Explicit ``delta_tracking`` (or the disabled default) scrubs a
    persisted ``delta_ok`` from the outgoing offset — the same non-auto
    reset discipline as every other verdict, so returning to ``auto``
    re-probes."""
    _mock_metadata()
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Customers",
        callback=lambda _r: (
            200,
            {},
            '{"value": [{"Id": 2, "Name": "B", "ModifiedAt": "2024-05-01T00:00:00Z"}]}',
        ),
    )
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-01-01T00:00:00Z", "delta_ok": False},
        {"cursor_field": "ModifiedAt"},
    )
    list(records)
    assert "delta_ok" not in offset


@responses.activate
def test_missing_primary_key_not_padded_stays_loud():
    """The emit-boundary padding must NOT pad primary-key columns: a server
    never legally omits a KEY (the omit-null rationale can't apply), so a
    missing one is a broken response that must keep failing loudly in the
    framework parser — padding it would send a silent null-key row into the
    destination MERGE."""
    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_FLAT_METADATA_XML, status=200)
    responses.get(f"{SERVICE_URL}Items", json={"value": [{"Opt": "y"}]})  # Id AND Req omitted
    c = _make({"token": "t"})
    rows, _ = c.read_table("Items", None, {"pagination": "nextlink"})
    (row,) = list(rows)
    # Non-key column padded (omit-null tolerance)…
    assert row["Req"] is None
    # …but the PK stays ABSENT so the framework parser still raises.
    assert "Id" not in row


def test_metadata_cache_capped_eviction():
    """The process-wide metadata cache evicts oldest-first beyond its cap,
    so a long-lived driver serving many distinct services doesn't retain one
    multi-MB parsed tree per service forever."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    saved = dict(odata_mod._METADATA_CACHE)
    odata_mod._METADATA_CACHE.clear()
    try:
        cap = odata_mod._METADATA_CACHE_MAX_SERVICES
        for i in range(cap + 3):
            odata_mod._metadata_cache_put(f"https://svc{i}/", ("x", None, None, float(i)))
        assert len(odata_mod._METADATA_CACHE) == cap
        # The three oldest (0, 1, 2) were evicted; the newest survive.
        assert f"https://svc{cap + 2}/" in odata_mod._METADATA_CACHE
        assert "https://svc0/" not in odata_mod._METADATA_CACHE
        assert "https://svc2/" not in odata_mod._METADATA_CACHE
    finally:
        odata_mod._METADATA_CACHE.clear()
        odata_mod._METADATA_CACHE.update(saved)


# ---------------------------------------------------------------------------
# Round 38 — same-instant park identity, partitioned cursor_nulls parity,
# service_url query rejection, wire hygiene, metadata cache eviction
# ---------------------------------------------------------------------------


def test_cursor_same_instant_helper():
    from databricks.labs.community_connector.sources.odata._helpers import cursor_same_instant

    # Rendering variants of one instant.
    assert cursor_same_instant("2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000Z")
    assert cursor_same_instant("2024-01-01T00:00:00Z", "2024-01-01T00:00:00+00:00")
    assert cursor_same_instant("2024-01-01T00:00:00.5Z", "2024-01-01T00:00:00.500Z")
    # Genuinely different instants — including sub-microsecond (7-digit).
    assert not cursor_same_instant("2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z")
    assert not cursor_same_instant("2024-01-01T00:00:00.0000001Z", "2024-01-01T00:00:00.0000002Z")
    # Non-ISO values: same instant only when raw-equal.
    assert cursor_same_instant(5, 5) and not cursor_same_instant(5, 6)
    assert not cursor_same_instant("abc", "abd")
    assert cursor_same_instant(None, None)


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
def test_partitioned_batch_leaf_cursor_respects_cursor_nulls_ignore():
    """The partitioned BATCH path (LakeflowBatchReader plans partitions
    without consulting is_partitioned, so leaf-cursor tables DO reach it)
    must apply the cursor_nulls policy like every other path — it used to
    emit null-cursor rows the user configured ``ignore`` to drop, and
    nondeterministically so (the framework silently falls back to the
    correct serial read on any planning exception)."""
    _mock_nested_metadata()
    children = [
        {"Id": 101, "Label": "a", "ModifiedAt": "2024-01-01T00:00:00Z"},
        {"Id": 102, "Label": "b", "ModifiedAt": None},  # ignore must drop it
    ]
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 1}]})),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": children})),
    )
    opts = {
        "cursor_field": "ModifiedAt",  # LEAF-level cursor
        "cursor_nulls": "ignore",
        "pagination": "nextlink",
        "expand_contained": "false",
        "contained_fetch": "single",
        "cursor_probe": "false",
    }
    c = _make()
    serial_rows, _ = c.read_table("Parents__Children", None, opts)
    serial_ids = sorted(r["Id"] for r in serial_rows)
    parts = c.get_partitions("Parents__Children", opts)
    part_ids = sorted(
        r["Id"] for p in parts for r in c.read_partition("Parents__Children", p, opts)
    )
    assert serial_ids == part_ids == [101]


def test_service_url_query_or_fragment_rejected():
    """A query-carrying service root (SAP Gateway '?sap-client=100') breaks
    every built URL (the entity path lands inside the query) and used to die
    as a bare $metadata ParseError. It must fail at construction with the
    header-form alternative named."""
    with pytest.raises(ValueError, match="sap-client"):
        _make({"service_url": "https://example.com/odata?sap-client=100"})
    with pytest.raises(ValueError, match="query string or fragment"):
        _make({"service_url": "https://example.com/odata#frag"})


def test_metadata_cache_put_never_evicts_new_entry():
    """Inserting an entry whose fetched_at is OLDER than everything cached
    (the file-cache-hit path stamps entries with the file's mtime) must not
    evict the just-inserted entry itself — that service would re-parse its
    pickle on every fresh instance while idle services stay cached."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    saved = dict(odata_mod._METADATA_CACHE)
    odata_mod._METADATA_CACHE.clear()
    try:
        cap = odata_mod._METADATA_CACHE_MAX_SERVICES
        for i in range(cap):
            odata_mod._metadata_cache_put(f"https://svc{i}/", ("x", None, None, 1000.0 + i))
        # Newcomer with the OLDEST stamp: must survive; an existing oldest
        # entry is evicted instead.
        odata_mod._metadata_cache_put("https://old-file/", ("x", None, None, 1.0))
        assert "https://old-file/" in odata_mod._METADATA_CACHE
        assert len(odata_mod._METADATA_CACHE) == cap
        assert "https://svc0/" not in odata_mod._METADATA_CACHE  # oldest other
    finally:
        odata_mod._METADATA_CACHE.clear()
        odata_mod._METADATA_CACHE.update(saved)


@responses.activate
def test_flat_select_whitespace_stripped_on_wire():
    """User whitespace in ``select`` ("Id, ModifiedAt") is stripped on the
    flat wire path — the validation set and the expand-leaf merge already
    strip, and a strict server may 400 the padded $select=Id,%20ModifiedAt
    form."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": []}, match_querystring=False)
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"select": "Id, ModifiedAt"})
    list(rows)
    data_urls = [call.request.url for call in responses.calls if "Customers" in call.request.url]
    assert data_urls
    for u in data_urls:
        select_param = u.split("$select=")[1].split("&")[0]
        assert select_param == "Id,ModifiedAt"


def test_decimal_literal_never_carries_exponent():
    """OData's decimalValue ABNF has no exponent form — Decimal literals
    render in plain positional notation (Edm.Double floats keep their
    spec-valid exponent)."""
    from decimal import Decimal

    from databricks.labs.community_connector.sources.odata._contained import odata_literal

    assert odata_literal(Decimal("1.5E+7")) == "15000000"
    assert odata_literal(Decimal("1E-6")) == "0.000001"
    assert odata_literal(Decimal("-2.5")) == "-2.5"
    # Floats (Edm.Double) legitimately keep exponents; '+' stays escaped.
    assert "e" in odata_literal(1e20).lower()


# ---------------------------------------------------------------------------
# Round 39 — incomparable cursor pairs, numeric same-instant, preflight
# verdict hygiene (rendering flip / annotation deferral / race taint),
# absent expanded property, streaming-snapshot quiesce marker
# ---------------------------------------------------------------------------

R39_FLIP_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="P" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Mids" Type="Collection(P.Mid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Leaves" Type="Collection(P.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Roots" EntityType="P.Root"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_cursor_newer_incomparable_pair_never_raises():
    """cursor_newer's TypeError fallback used to re-raise for str-vs-int
    (an IEEE754Compatible server alternating 5000 and "5000" across
    requests) and propagate through cursor_max/max_or — killing the batch
    from a watermark fold. Incomparable pairs now degrade to a consistent
    False, matching _chain_strictly_before's documented posture."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_max,
        cursor_newer,
        max_or,
    )

    assert cursor_newer(5000, "5000") is False
    assert cursor_newer("5000", 5000) is False
    assert cursor_max([5000, "5000", 4999]) is not None  # no raise
    assert max_or(5000, "5000") is not None  # no raise


def test_cursor_same_instant_numeric_strings():
    """IEEE754Compatible numeric-string renderings of one value are the same
    instant; sub-float-precision Int64 pairs stay distinct (exact Decimal
    compare, no float collapse)."""
    from databricks.labs.community_connector.sources.odata._helpers import cursor_same_instant

    assert cursor_same_instant(5000, "5000")
    assert cursor_same_instant("5000", 5000)
    assert cursor_same_instant("5000.0", 5000)
    assert cursor_same_instant("9007199254740993", 9007199254740993)
    assert not cursor_same_instant("9007199254740992", 9007199254740993)
    assert not cursor_same_instant("5001", 5000)
    assert not cursor_same_instant("abc", 5000)


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


def _run_flip_preflight(direct_suffix, expand_suffix):
    """Run the cursor_probe preflight against a server whose direct-nav and
    probe-shaped fetches render the same instant with different suffixes."""
    responses.get(f"{SERVICE_URL}$metadata", body=R39_FLIP_METADATA, status=200)
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]}, match_querystring=False)
    newest, older = "2024-05-02T00:00:00", "2024-05-01T00:00:00"

    def _mids_cb(req):
        from urllib.parse import unquote

        if "$expand=" in unquote(req.url):
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [
                            {
                                "Id": 7,
                                "Leaves": [
                                    {"Id": 71, "RecordLastModified": newest + expand_suffix}
                                ],
                            }
                        ]
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 7}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids(7)/Leaves",
        callback=lambda _r: (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"RecordLastModified": newest + direct_suffix},
                        {"RecordLastModified": older + direct_suffix},
                    ]
                }
            ),
        ),
    )
    c = _make()
    return c._run_cursor_probe_preflight(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified"
    )


@responses.activate
def test_probe_preflight_rendering_flip_is_a_pass_not_condemnation():
    """The preflight's final newest-leaf comparison is SAME-INSTANT, not raw
    text: a load balancer rendering one instant as …00Z on one backend and
    …00.000Z on the other must not produce definitive mis-ordering evidence
    (false cursor_probe_ok=false under auto; a spurious raise under strict
    nested-expand). Both flip directions now verify as a conclusive pass."""
    problem, conclusive, race = _run_flip_preflight("Z", ".000Z")
    assert problem is None and conclusive and not race
    responses.reset()
    problem, conclusive, race = _run_flip_preflight(".000Z", "Z")
    assert problem is None and conclusive and not race


def test_probe_preflight_all_race_scan_declines_probe(monkeypatch):
    """A verdict-less scan containing RACE skips (discriminating samples that
    returned newer-than-reference) must decline the probe for the batch —
    engaging it unverified could hide a mis-ordering server behind concurrent
    writes — while still recording nothing."""
    c = _make()
    monkeypatch.setattr(
        ODataLakeflowConnect,
        "_run_cursor_probe_preflight",
        lambda self, *a, **k: (None, False, True),
    )
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (False, False)
    # A genuinely non-discriminating scan (no races) still engages unverified.
    c2 = _make()
    monkeypatch.setattr(
        ODataLakeflowConnect,
        "_run_cursor_probe_preflight",
        lambda self, *a, **k: (None, False, False),
    )
    supported, conclusive = c2._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (True, False)


@responses.activate
def test_expand_preflight_annotation_deferral_verified_not_condemned():
    """A server that defers inner collections behind <Nav>@odata.nextLink
    (inline [] + annotation) used to read as 'children exist but $expand
    omitted them' — a definitive expand_ok=false pinning N+1 forever on a
    server the read fully supports. Annotation presence is containment
    evidence: the preflight now verifies through it and passes."""
    _mock_nested_metadata()

    def _parents_cb(req):
        from urllib.parse import unquote

        if "$expand=" in unquote(req.url):
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [
                            {
                                "Id": 1,
                                "Name": "p",
                                "Children": [],
                                "Children@odata.nextLink": (f"{SERVICE_URL}Parents(1)/Children"),
                            }
                        ]
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 1, "Name": "p"}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 11, "Label": "x"}]})),
    )
    c = _make()
    ok, definitive = c._run_expand_preflight("Parents__Children", ["Parents", "Children"], {}, None)
    assert (ok, definitive) == (True, True)


@responses.activate
def test_expand_flatten_absent_child_property_fetched_directly():
    """A spec-violating partial-expansion server that wholly OMITS the
    expanded property for some parents (no inline list, no annotation) used
    to have those subtrees silently dropped — absent is NOT verified-empty.
    The flatten now fetches the collection directly for such parents."""
    _mock_nested_metadata()

    def _parents_cb(req):
        from urllib.parse import unquote

        if "$expand=" in unquote(req.url):
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [
                            {"Id": 1, "Name": "a", "Children": [{"Id": 11, "Label": "x"}]},
                            {"Id": 2, "Name": "b"},  # property wholly absent
                        ]
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 1}, {"Id": 2}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(2)/Children",
        callback=lambda _r: (200, {}, json.dumps({"value": [{"Id": 22, "Label": "y"}]})),
    )
    c = _make()
    rows, _ = c.read_table(
        "Parents__Children", None, {"expand_contained": "true", "pagination": "nextlink"}
    )
    assert {r["Id"] for r in rows} == {11, 22}


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


def test_service_url_bare_trailing_question_rejected():
    """A bare trailing '?' has an EMPTY urlparse query but still corrupts
    every built URL (svc?/Customers) — the raw-char check catches it."""
    with pytest.raises(ValueError, match="query string or fragment"):
        _make({"service_url": "https://example.com/odata?"})


@responses.activate
def test_select_strips_to_empty_omits_param():
    """select=',' is rejected upstream by the PK validation (it strips to no
    columns, so the PKs are 'omitted') — and the wire builder independently
    guards the empty case: a select that strips to nothing emits no $select
    param at all rather than an empty '$select='."""
    _mock_metadata()
    c = _make({"token": "t"})
    with pytest.raises(ValueError, match="omits primary-key"):
        c.read_table("Customers", None, {"select": ","})
    # Defense-in-depth at the URL builder (other callers bypass validation).
    assert "$select" not in c._format_query_params({"select": ", ,"})
    assert "$select=Id" in c._format_query_params({"select": " Id ,"})


# ---------------------------------------------------------------------------
# Round 40 — mixed-type cursor ordering bridge, delta wire-shape hardening,
# CSDL TypeDefinition/complex-key/__-nav resolution
# ---------------------------------------------------------------------------

R40_TYPEDEF_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="ta" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <TypeDefinition Name="Qty64" UnderlyingType="Edm.Int64"/>
      <TypeDefinition Name="When" UnderlyingType="Edm.DateTimeOffset"/>
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Qty" Type="ta.Qty64"/>
        <Property Name="At" Type="ta.When"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Items" EntityType="ta.Item"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

R40_PATHKEY_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="pk" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <ComplexType Name="Info"><Property Name="Code" Type="Edm.String"/></ComplexType>
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Info/Code" Alias="IC"/></Key>
        <Property Name="Info" Type="pk.Info"/>
        <Property Name="Label" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Things" EntityType="pk.Thing"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

R40_DUNDER_NAV_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="dn" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Parent">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="My__Kids" Type="Collection(dn.Kid)" ContainsTarget="true"/>
        <NavigationProperty Name="Pets" Type="Collection(dn.Kid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Kid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="dn.Parent"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_cursor_ordering_numeric_string_bridge():
    """Mixed numeric renderings order TRULY (exact Decimal), not as a flat
    tie: a server that permanently switches an Int64 cursor to string
    rendering against an int checkpoint made cursor_le(new_row, since) read
    True for genuinely newer rows — the client re-filter dropped every
    returned row and the stream silently stalled with data pending."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_le,
        cursor_max,
        cursor_newer,
    )

    assert cursor_newer("6000", 5000) is True
    assert cursor_newer(5000, "6000") is False
    assert cursor_le("6000", 5000) is False  # the stall's exact predicate
    assert cursor_le("4000", 5000) is True
    assert cursor_max([5000, "6000", 4000]) == "6000"
    # Non-numeric incomparable pairs keep the consistent-False posture.
    assert cursor_newer("abc", 5000) is False and cursor_newer(5000, "abc") is False


@responses.activate
def test_flat_cursor_survives_permanent_int_to_string_rendering_switch():
    """End-to-end: int checkpoint {"cursor": 5000} + a server now rendering
    the cursor as strings — rows must emit and the watermark must advance
    (pre-fix: every batch dropped all rows client-side, offset frozen)."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 7, "Name": "N", "ModifiedAt": "6000"},
                {"Id": 8, "Name": "M", "ModifiedAt": "7000"},
            ]
        },
        match_querystring=False,
    )
    c = _make({"token": "t"})
    rows, offset = c.read_table(
        "Customers", {"cursor": 5000}, {"cursor_field": "ModifiedAt", "pagination": "nextlink"}
    )
    # Row 8 is the designed same-cursor boundary trim (re-fetched next batch
    # via gt "6000"); the stall signature was ZERO rows and a frozen cursor.
    assert [r["Id"] for r in rows] == [7]
    assert offset["cursor"] == "6000"


@responses.activate
def test_delta_page_with_both_links_continues_to_next_page():
    """A spec-violating page carrying BOTH @odata.nextLink and
    @odata.deltaLink must follow the continuation (stopping at the deltaLink
    silently dropped every trailing page) while retaining the deltaLink."""
    _mock_metadata()
    page2_url = f"{SERVICE_URL}Customers?$skiptoken=p2"
    call_n = {"n": 0}

    def _cb(request):
        call_n["n"] += 1
        if "skiptoken=p2" in request.url:
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(
                    _delta_bootstrap_body(
                        [{"Id": 2, "Name": "B", "ModifiedAt": "y"}],
                        delta_link=f"{SERVICE_URL}Customers?$deltatoken=tok-final",
                    )
                ),
            )
        body = _delta_bootstrap_body(
            [{"Id": 1, "Name": "A", "ModifiedAt": "x"}], next_link=page2_url
        )
        # Both links on page 1 (delta_link default + explicit next_link).
        return (200, {"Preference-Applied": "odata.track-changes"}, json.dumps(body))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]  # page 2 delivered
    # The terminal page's deltaLink wins.
    assert offset["delta_link"].endswith("tok-final")


@responses.activate
def test_delta_relationship_link_entries_skipped_not_sparse_error():
    """v4.01 relationship-change entries ($link / $deletedLink contexts,
    carrying source/relationship/target) are valid delta shapes — they used
    to fall into the regular-entity branch and die in the sparse-entity
    guard with a misleading diagnosis. They're skipped (nav properties are
    never ingested on this path)."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [
            {"Id": 1, "Name": "A", "ModifiedAt": "x"},
            {
                "@odata.context": f"{SERVICE_URL}$metadata#Customers/$deletedLink",
                "source": "Customers(1)",
                "relationship": "Orders",
                "target": "Orders(9)",
            },
            {
                "@odata.context": f"{SERVICE_URL}$metadata#Customers/$link",
                "source": "Customers(1)",
                "relationship": "Orders",
                "target": "Orders(10)",
            },
        ]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]  # link entries skipped, no raise


@responses.activate
def test_delta_cross_set_tombstone_refuses_wrong_key():
    """A tombstone whose entity reference names a DIFFERENT entity set
    (…/Suppliers(77) in a Customers feed) must not delete Customers 77 —
    the reference is treated as unresolvable and the loud keyless-tombstone
    raise fires. A container-qualified same-set reference still resolves."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [{"@removed": {"reason": "deleted"}, "@odata.id": f"{SERVICE_URL}Suppliers(77)"}]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="no resolvable primary key"):
        records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
        list(records)
    # Dotted (container-qualified) same-set form still resolves.
    keys = c._tombstone_keys_from_id(
        "Container.Customers(5)", ["Id"], {"Id": "Edm.Int32"}, entity_set="Customers"
    )
    assert keys == {"Id": 5}


@responses.activate
def test_delta_property_scoped_annotations_stripped():
    """Prop@odata.type-style property-scoped control info must not survive
    into emitted records."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [{"Id": 1, "Name": "A", "Name@odata.type": "#String", "ModifiedAt": "x"}]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    (row,) = list(records)
    assert "Name@odata.type" not in row and row["Name"] == "A"


@responses.activate
def test_typedef_property_gets_underlying_spark_type():
    """A TypeDefinition-typed property maps to its underlying primitive's
    Spark type — it used to fall to StringType while the literal-rendering
    map correctly resolved Edm.Int64, silently degrading every
    TypeDefinition-backed column (SAP production shape) to a string."""
    from pyspark.sql.types import LongType, TimestampType

    responses.get(f"{SERVICE_URL}$metadata", body=R40_TYPEDEF_METADATA, status=200)
    c = _make({"token": "t"})
    schema = {f.name: f.dataType for f in c.get_table_schema("Items", {}).fields}
    assert isinstance(schema["Qty"], LongType)
    assert isinstance(schema["At"], TimestampType)


@responses.activate
def test_complex_path_key_raises_loudly():
    """A complex-path key (<PropertyRef Name="Info/Code" Alias="IC"/>) can't
    be addressed or MERGEd on (neither the path nor the alias is an emitted
    column) — it used to silently report primary_keys=['Info/Code'], a MERGE
    key matching no schema column. It now fails loudly and honestly."""
    responses.get(f"{SERVICE_URL}$metadata", body=R40_PATHKEY_METADATA, status=200)
    c = _make({"token": "t"})
    with pytest.raises(ValueError, match="complex type"):
        c.read_table_metadata("Things", {})


@responses.activate
def test_dunder_nav_property_skipped_in_discovery():
    """A nav property named with '__' would list as a table the read path
    can never resolve back (discovery→read round-trip failure). Discovery
    now skips it with a warning; normal siblings still list."""
    responses.get(f"{SERVICE_URL}$metadata", body=R40_DUNDER_NAV_METADATA, status=200)
    c = _make({"token": "t"})
    tables = c.list_tables()
    assert "Parents" in tables and "Parents__Pets" in tables
    assert not any("My__Kids" in t for t in tables)


# ---------------------------------------------------------------------------
# Round 41 — same-instant boundary trim, typed cursor-filter rendering,
# probe-fetch transient classification, +-encoded orderby
# ---------------------------------------------------------------------------

R41_INT64_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="sq" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Event">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Seq" Type="Edm.Int64"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Events" EntityType="sq.Event"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


def test_trim_boundary_groups_mixed_renderings():
    """The boundary trim groups the cohort by SAME-INSTANT, not raw
    equality: a same-value cohort spanning a page-rendering seam (ints on
    page 1, strings on page 2) used to trim only the differently-rendered
    tail while the watermark landed EQUAL to the trimmed rows' value — gt
    never re-fetched them (permanent loss)."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        trim_to_distinct_cursor_boundary,
    )

    records = [{"Id": 1, "Seq": 5000}, {"Id": 2, "Seq": 6000}, {"Id": 3, "Seq": "6000"}]
    trimmed = trim_to_distinct_cursor_boundary(records, "Seq")
    assert [r["Id"] for r in trimmed] == [1]  # whole 6000-cohort trimmed as one
    # Timestamp rendering variants group too.
    records = [
        {"Id": 1, "T": "2024-01-01T00:00:00Z"},
        {"Id": 2, "T": "2024-02-01T00:00:00Z"},
        {"Id": 3, "T": "2024-02-01T00:00:00.000Z"},
    ]
    assert [r["Id"] for r in trim_to_distinct_cursor_boundary(records, "T")] == [1]
    # Null-cohort behavior preserved: all-null trims to empty.
    assert trim_to_distinct_cursor_boundary([{"T": None}, {"T": None}], "T") == []


@responses.activate
def test_flat_cursor_filter_typed_rendering_numeric_string_watermark():
    """A numeric-string watermark against an Edm.Int64-declared cursor
    renders BARE on the wire (Seq gt 7000) — the untyped sniff quoted it
    (Seq gt '7000'), which strict servers 400. This is batch 2 of the
    round-40 rendering-switch scenario."""
    from urllib.parse import unquote

    responses.get(f"{SERVICE_URL}$metadata", body=R41_INT64_METADATA, status=200)
    responses.get(f"{SERVICE_URL}Events", json={"value": []}, match_querystring=False)
    c = _make({"token": "t"})
    rows, _ = c.read_table(
        "Events", {"cursor": "7000"}, {"cursor_field": "Seq", "pagination": "nextlink"}
    )
    list(rows)
    data_urls = [
        unquote(call.request.url) for call in responses.calls if "Events" in call.request.url
    ]
    assert data_urls and all("Seq gt 7000" in u for u in data_urls)
    assert not any("'7000'" in u for u in data_urls)


@responses.activate
def test_probe_preflight_transient_fetch_is_no_verdict_not_definitive():
    """A retry-exhausted transient (503) on the probe-shaped $expand fetch
    is NOT capability evidence — it used to return the definitive 'error'
    status, pinning a false cursor_probe_ok=false for the cache TTL under
    auto and raising a misleading capability error under strict. It now
    routes to the no-verdict path: auto degrades this batch and records
    NOTHING; strict raises the accurate 'before reaching a verdict'."""
    from urllib.parse import unquote

    def _mock_all():
        responses.get(f"{SERVICE_URL}$metadata", body=R39_FLIP_METADATA, status=200)
        responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]}, match_querystring=False)

        def _mids_cb(req):
            if "$expand=" in unquote(req.url):
                return (503, {}, "busy")  # probe-shaped fetch: throttled
            return (200, {}, json.dumps({"value": [{"Id": 7}]}))

        responses.add_callback(responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_cb)
        responses.get(
            f"{SERVICE_URL}Roots(1)/Mids(7)/Leaves",
            json={
                "value": [
                    {"RecordLastModified": "2024-05-02T00:00:00Z"},
                    {"RecordLastModified": "2024-05-01T00:00:00Z"},
                ]
            },
            match_querystring=False,
        )

    _mock_all()
    c = _make({"max_retries": "0"})
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (False, False)
    # Nothing recorded anywhere — the next batch re-probes.
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is None
    responses.reset()
    _mock_all()
    c2 = _make({"max_retries": "0"})
    with pytest.raises(ValueError, match="before reaching a verdict"):
        c2._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=True
        )


def test_pg_orderby_keys_plus_encoded_spaces():
    """'+' is a legal space encoding in query strings: 'Id+asc' must parse
    to key 'Id' and 'Name+desc' must trip the desc guard — not produce the
    bogus key 'Id+asc' that seeks on a nonexistent column."""
    from databricks.labs.community_connector.sources.odata._contained import _pg_orderby_keys

    assert _pg_orderby_keys("https://x/S?$orderby=Id+asc") == ["Id"]
    assert _pg_orderby_keys("https://x/S?$orderby=Name+desc") == []
    assert _pg_orderby_keys("https://x/S?$orderby=A+asc,B+asc") == ["A", "B"]


@responses.activate
def test_delta_removed_with_link_context_still_tombstones():
    """A contradictory entry carrying BOTH @removed and a $deletedLink
    context takes the TOMBSTONE branch (keys resolve-or-raise — loud, never
    a silently dropped delete)."""
    _mock_metadata()
    body = _delta_bootstrap_body(
        [
            {
                "@removed": {"reason": "deleted"},
                "@odata.context": f"{SERVICE_URL}$metadata#Customers/$deletedLink",
                "@odata.id": f"{SERVICE_URL}Customers(5)",
            }
        ]
    )
    responses.get(
        f"{SERVICE_URL}Customers",
        json=body,
        headers={"Preference-Applied": "odata.track-changes"},
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", {}, {"delta_tracking": "enabled"})
    (row,) = list(records)
    assert row["Id"] == 5 and row["_deleted"] is True


# ---------------------------------------------------------------------------
# Round 42 — fallback-shape delta pin, keyless-parent preflight 3-tuple,
# $batch probe id echo, _batch_relative origin normalization, header-name
# validation, curated origin errors, delta_ok in the no-progress strip
# ---------------------------------------------------------------------------


@responses.activate
def test_delta_auto_snapshot_shaped_offset_pins_fallback_without_stamp():
    """A NON-empty fallback-shaped offset (``snapshot_done``, no ``delta_ok``
    — the first batch's probe was transient, so nothing was stamped) must pin
    the fallback by SHAPE: it proves earlier batches ran the cursor/snapshot
    shape against the setup-frozen schema. Without the pin, a later batch's
    re-probe (recovered transient / Preference-Applied flap) flips the stream
    ONTO the delta path mid-stream — emitting ``_deleted``/``_lc_sequence``
    columns the frozen schema never declared (a tombstone then MERGEs as a
    live row) and committing a sticky ``delta_link`` offset."""
    _mock_metadata()
    prefer_seen = {"n": 0}

    def _cb(request):
        if request.headers.get("Prefer"):
            prefer_seen["n"] += 1
            return (
                200,
                {"Preference-Applied": "odata.track-changes"},
                json.dumps(_delta_bootstrap_body([{"Id": 9, "Name": "Z", "ModifiedAt": "y"}])),
            )
        return (200, {}, '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_cb)
    c = _make()
    records, offset = c.read_table("Customers", {"snapshot_done": True}, {"delta_tracking": "auto"})
    rows = list(records)
    assert prefer_seen["n"] == 0  # no delta probe ran at all
    assert rows == []  # the quiesced snapshot stays quiesced
    assert "delta_link" not in offset and "next_link" not in offset
    assert offset.get("snapshot_done") is True
    # The pin is persisted so framework-recreated instances inherit it.
    assert offset.get("delta_ok") is False


R42_KEYLESS_MID_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="kq" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Mids" Type="Collection(kq.Mid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Property Name="Code" Type="Edm.String"/>
        <NavigationProperty Name="Leaves" Type="Collection(kq.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Roots" EntityType="kq.Root"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_probe_preflight_keyless_parent_returns_three_tuple():
    """The keyless-leaf-parent early return was the one preflight exit the
    round-39 3-tuple migration missed: both consumers unpack
    ``problem, conclusive, race`` from the cached result, so the stale
    2-tuple crashed a probe-engaged read over a keyless parent with an
    undiagnosable unpack ValueError instead of reaching the walk's
    actionable "no primary key declared in $metadata" error."""
    responses.get(f"{SERVICE_URL}$metadata", body=R42_KEYLESS_MID_METADATA, status=200)
    c = _make({"token": "t"})
    result = c._run_cursor_probe_preflight(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified"
    )
    assert result == (None, False, False)
    # The consumer path must survive it too: inconclusive, race-free →
    # engage-unverified semantics, same as any non-discriminating scan.
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", None, strict=False
    )
    assert (supported, conclusive) == (True, False)


@responses.activate
def test_batch_probe_requires_id_echo():
    """A 2xx $batch envelope whose sub-response lacks the echoed ``id`` is a
    server ``_post_batch`` can never consume (it keys sub-responses by id and
    hard-raises without one, and that raise is not _BatchTooManyParts — so
    nothing would ever degrade to plain GETs). The probe used to pass it on
    status alone, pinning a definitive-but-unusable ``batch_ok: true``."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={"responses": [{"status": 200, "body": {"value": []}}]},
    )
    c = _make()
    assert c._verify_batch_support(["Roots"], {}) is False
    # Definitive fail — recorded, not retried every batch.
    assert c._cached_capability("batch_ok") is False


def test_batch_relative_normalizes_same_origin_spelling():
    """An absolute same-origin continuation spelled with the default port or
    a different host case must still resolve service-relative — the raw
    prefix match missed it and kept the service root's own path segments,
    404ing the sub-request (self-healing via the plain-GET re-issue, but one
    wasted round-trip plus an alarming warning per continuation)."""
    c = _make()
    assert (
        c._batch_relative("https://example.com:443/odata/Orders?$skiptoken=5")
        == "Orders?$skiptoken=5"
    )
    assert c._batch_relative("https://EXAMPLE.com/odata/Orders") == "Orders"
    # Off-origin (or unparseable-port) absolute URLs keep the legacy
    # path-only fallback; the same-origin guard upstream handles policy.
    assert c._batch_relative("https://other.example.com/odata/Orders") == "odata/Orders"
    assert c._batch_relative("https://example.com:banana/odata/Orders") == "odata/Orders"


def test_extra_headers_invalid_name_fails_eagerly():
    """Header NAMES in extra_headers get the same eager RFC 7230 token check
    as api_key_header — http.client's send-time regex tolerates interior
    spaces, so a malformed name used to go out on the wire as-is and fail at
    a strict server with nothing pointing at the option."""
    c = _make({"extra_headers": "bad name: 1", "token": "t"})
    with pytest.raises(ValueError, match="extra_headers"):
        c._get_session()
    ok = _make({"extra_headers": "sap-client: 100", "token": "t"})
    assert ok._get_session().headers["sap-client"] == "100"


def test_service_url_malformed_port_curated_error():
    """urlparse defers port validation to the ``.port`` accessor, so a
    malformed port used to escape as a bare "Port could not be cast to
    integer value" with no hint of which URL carried it."""
    with pytest.raises(ValueError, match="Invalid port in URL"):
        _make({"service_url": "https://example.com:banana/odata"})


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


# ---------------------------------------------------------------------------
# Round 43 — collation-honest park-resume seeks (identity anchors, three-way
# order, vanished-anchor reset), NaN lookback factor, alias-aware namespace
# listing, $batch probe duplicate-id consistency, discovery warning
# ---------------------------------------------------------------------------

R43_CI_COLLATION_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="cq" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Parent">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Children" Type="Collection(cq.Child)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Child">
        <Key><PropertyRef Name="Cid"/></Key>
        <Property Name="Cid" Type="Edm.Int32" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="cq.Parent"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_expand_park_boundary_survives_ci_collation():
    """A case-insensitive server orders parents 'a1' < 'B2'; Python ordinal
    order says the opposite. The expand drainer's park-boundary skip used to
    trust ordinal order, classifying the unwalked 'B2' as "already walked"
    on resume and silently dropping its whole subtree. The resume now
    anchors on the parked row's identity (exact for every key type) and
    only trusts client-side order where it's provable."""
    responses.get(f"{SERVICE_URL}$metadata", body=R43_CI_COLLATION_METADATA, status=200)
    page = {
        "value": [
            {"Id": "a1", "Children": [{"Cid": 1}]},
            {"Id": "B2", "Children": [{"Cid": 2}]},
        ]
    }
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Parents", callback=lambda _r: (200, {}, json.dumps(page))
    )
    opts = {"expand_contained": "true", "max_records_per_batch": "1", "pagination": "nextlink"}
    emitted = []
    offset = {}
    for _ in range(6):
        recs, offset = _make().read_table("Parents__Children", offset, opts)
        emitted.extend(list(recs))
        if not offset or offset.get("snapshot_done"):
            break
    assert sorted(r["Cid"] for r in emitted) == [1, 2]


@responses.activate
def test_capped_walk_vanished_string_key_park_resets_and_recovers(caplog):
    """String parent keys + the parked parent deleted between batches: the
    resume seek can no longer trust ordinal order to prove the remaining
    parents already-walked, and completing the batch would fold running_max
    past them (permanent sub-max loss — the pre-fix behavior). The seek now
    exhausts, resets the walk via a truncated no-park offset (positional
    restart, floor kept), and the next batch recovers the unwalked subtree."""
    responses.get(f"{SERVICE_URL}$metadata", body=R43_CI_COLLATION_METADATA, status=200)
    parents = {"n": 0}

    def _parents_cb(_req):
        parents["n"] += 1
        if parents["n"] == 1:  # batch 1: both parents, server CI order
            return (200, {}, json.dumps({"value": [{"Id": "a1"}, {"Id": "B2"}]}))
        return (200, {}, json.dumps({"value": [{"Id": "B2"}]}))  # a1 deleted

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents_cb)
    responses.get(
        f"{SERVICE_URL}Parents('a1')/Children",
        json={
            "value": [
                {"Cid": 1, "ModifiedAt": "2024-06-01T00:00:00Z"},
                {"Cid": 2, "ModifiedAt": "2024-06-02T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Parents('B2')/Children",
        json={"value": [{"Cid": 3, "ModifiedAt": "2024-03-01T00:00:00Z"}]},
        match_querystring=False,
    )
    opts = {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "1",
        "pagination": "nextlink",
        "cursor_probe": "false",
        # Pin the N+1 walk (no expand preflight — its probe request would
        # advance the parents callback's batch counter).
        "expand_contained": "false",
    }
    # Batch 1: cap parks parent 'a1' (boundary-trimmed, key chain parked).
    recs, off1 = _make().read_table("Parents__Children", {}, opts)
    assert [r["Cid"] for r in recs] == [1]
    assert off1.get("parent_keys") == [{"Id": "a1"}]
    # Batch 2: 'a1' vanished. The seek exhausts and RESETS (no cursor fold,
    # no park) instead of completing past the unwalked 'B2'.
    with caplog.at_level(logging.WARNING):
        recs, off2 = _make().read_table("Parents__Children", off1, opts)
    assert list(recs) == []
    assert "was not re-found" in caplog.text
    assert "parent_keys" not in off2
    assert "cursor" not in off2  # floor (None) kept — running_max NOT folded
    # Round 44: the reset offset must not stamp an inert explicit
    # ``truncated_chain_cursor: None`` (cosmetic wart; resume reads .get()).
    assert "truncated_chain_cursor" not in off2
    # Batch 3: full re-walk recovers B2's sub-max child.
    recs, off3 = _make().read_table("Parents__Children", off2, opts)
    assert [r["Cid"] for r in recs] == [3]
    # Completion folds the accumulated running_max (>= batch 1's rows).
    assert _drop_lb(off3)["cursor"] == "2024-06-01T00:00:00Z"


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


def test_cursor_lookback_factor_rejects_nan():
    """NaN passes every ``<=`` comparison (all False) and used to sail
    through the validator, then kill the read with an uncurated
    ``cannot convert float NaN to integer`` deep in the lookback resolve."""
    c = _make()
    with pytest.raises(ValueError, match="cursor_lookback_factor"):
        c._parse_cursor_lookback_factor({"cursor_lookback_factor": "nan"})


R43_ALIAS_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="com.example.model" Alias="m" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Orders" EntityType="m.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_list_tables_in_namespace_accepts_alias():
    """The ``namespace`` table option resolves schema aliases; the namespace
    LISTING used to compare raw and silently return ``[]`` for the same
    alias string — the connector's own alias contract, applied to both."""
    responses.get(f"{SERVICE_URL}$metadata", body=R43_ALIAS_METADATA, status=200)
    c = _make({"token": "t"})
    assert c.list_tables_in_namespace(["com.example.model"]) == ["Orders"]
    assert c.list_tables_in_namespace(["m"]) == ["Orders"]


@responses.activate
def test_batch_probe_duplicate_ids_last_wins():
    """Duplicate ``id`` echoes resolve LAST-wins in ``_post_batch``'s by-id
    dict; the probe must judge the same sub-response the hydrate would
    consume, not pass on the first one."""
    responses.post(
        f"{SERVICE_URL}$batch",
        json={
            "responses": [
                {"id": "0", "status": 200, "body": {"value": []}},
                {"id": "0", "status": 404},
            ]
        },
    )
    assert _make()._verify_batch_support(["Roots"], {}) is False


@responses.activate
def test_enumerate_contained_paths_warns_on_unresolvable_root(caplog):
    """An entity set whose EntityType reference resolves to nothing still
    lists (its read fails loudly), but its contained children silently
    didn't enumerate — now it says so."""
    broken = R43_ALIAS_METADATA.replace('EntityType="m.Order"', 'EntityType="m.Missing"')
    responses.get(f"{SERVICE_URL}$metadata", body=broken, status=200)
    c = _make({"token": "t"})
    with caplog.at_level(logging.WARNING):
        tables = c.list_tables()
    assert "Orders" in tables
    assert "Cannot enumerate contained paths" in caplog.text


# ---------------------------------------------------------------------------
# Round 44 — numeric-string cursor ordering (Decimal sort key), basic-format
# ISO guard, streaming-snapshot cap warning, reset-offset hygiene
# ---------------------------------------------------------------------------


def test_cursor_numeric_string_pairs_order_numerically():
    """Two numeric STRINGS used to compare ordinally ("1000" < "999") — the
    round-38 Decimal bridge lived in the except-TypeError path, which
    str/str never reaches. The sort key now Decimal-keys numeric strings,
    aligning cursor_newer with cursor_same_instant's existing numeric
    treatment of the same pair class."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_le,
        cursor_max,
        cursor_newer,
    )

    assert cursor_newer("1000", "999") is True
    assert cursor_newer("999", "1000") is False
    assert cursor_le("1000", "999") is False
    assert cursor_newer("100000", "99999") is True
    # Watermark folds must be order-independent at the digit boundary.
    assert cursor_max(["999", "1000"]) == "1000"
    assert cursor_max(["1000", "999"]) == "1000"
    # Cross-rendering (int vs numeric string) still orders in the primary path.
    assert cursor_newer("5000", 4000) is True
    assert cursor_newer(4000, "5000") is False


@responses.activate
def test_flat_stream_numeric_string_cursor_crosses_digit_boundary():
    """E2E pin of the silent-stall shape: watermark "999", the server
    correctly answers `Seq gt 999` with Seq="1000" (IEEE754Compatible
    string rendering) — the client re-filter used to drop it every batch,
    freezing the offset with data pending forever."""
    responses.get(f"{SERVICE_URL}$metadata", body=R41_INT64_METADATA, status=200)
    responses.get(
        f"{SERVICE_URL}Events",
        json={"value": [{"Id": 3, "Seq": "1000"}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Events", {"cursor": "999"}, {"cursor_field": "Seq", "pagination": "nextlink"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [3]
    assert _drop_lb(offset) == {"cursor": "1000"}


def test_basic_format_iso_strings_stay_numeric():
    """`fromisoformat` on Python >= 3.11 parses BASIC format ("20240101"),
    which the 3.10 floor rejects — without the structural guard the
    comparison keys were version-divergent, and "20240101" aliased
    "2024-01-01" as the same instant. 8-digit yyyymmdd string cursors now
    key as Decimals uniformly (numeric order == chronological order)."""
    from datetime import datetime

    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_newer,
        cursor_same_instant,
        cursor_sort_key,
    )

    assert not isinstance(cursor_sort_key("20240101"), datetime)
    assert cursor_same_instant("20240101", "2024-01-01") is False
    assert cursor_newer("20240102", "20240101") is True
    # Extended format still datetime-keys.
    assert isinstance(cursor_sort_key("2024-01-01"), datetime)


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
# Round 45 — same-rendering (not same-instant) chain-element conflation,
# fence desc self-check, delta stored-link 4xx curation, keyless-delta gate
# ---------------------------------------------------------------------------

R45_DIGIT_PK_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="q" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Mids" Type="Collection(q.Mid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Key><PropertyRef Name="MId"/></Key>
        <Property Name="MId" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Leaves" Type="Collection(q.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="LId"/></Key>
        <Property Name="LId" Type="Edm.Int32" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Roots" EntityType="q.Root"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


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


@responses.activate
def test_fence_probe_self_checks_orderby_desc():
    """A server that silently ignores `$orderby ... desc` hands the fence
    probe a stale first row — the fence pins, get_partitions returns []
    every trigger, and the stream stalls silently with data pending. The
    probe now self-checks (`cursor gt <probed max>` must be empty) and
    raises actionably instead."""
    _mock_nested_metadata()

    # Orderby-ignoring server: always default order — probe gets the OLD
    # row; the self-check (gt) finds the newer one.
    def _parents(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        rows = [
            {"Id": 1, "Name": "2024-01-01T00:00:00Z"},
            {"Id": 2, "Name": "2024-06-01T00:00:00Z"},
        ]
        m = re.search(r"Name gt (\S+)", flt)
        if m:
            rows = [r for r in rows if r["Name"] > m.group(1).strip("'")]
        return (200, {}, json.dumps({"value": rows[:1]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    with pytest.raises(ValueError, match="ignoring \\$orderby"):
        c.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)


@responses.activate
def test_delta_stored_link_non_410_4xx_curated():
    """Real gateways answer 404/400 (not the spec's 410) for an expired
    delta token; the checkpoint pins the link, so a bare HTTPError re-raised
    forever was an undiagnosable dead-end. Now a curated error names the
    cause and the full-refresh remedy — and does NOT auto-rebootstrap."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"error": {"message": "token not found"}},
        status=404,
        match_querystring=False,
    )
    c = _make()
    with pytest.raises(RuntimeError, match="full refresh"):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": f"{SERVICE_URL}Customers?$deltatoken=expired"},
            {"delta_tracking": "enabled"},
        )
        list(records)


R45_KEYLESS_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="k" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Log">
        <Property Name="At" Type="Edm.DateTimeOffset"/>
        <Property Name="Msg" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Logs" EntityType="k.Log"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


@responses.activate
def test_keyless_delta_enabled_raises_auto_falls_back():
    """Delta rows MERGE on the primary key; a keyless tombstone MERGEs
    against nothing and the deletion is silently lost — and the
    per-tombstone raise is gated on a non-empty key list, so it can never
    fire. Enabled → eager curated error; auto → snapshot fallback with no
    probe (no Prefer request, no synthetics)."""
    responses.get(f"{SERVICE_URL}$metadata", body=R45_KEYLESS_METADATA, status=200)
    prefer_seen = {"n": 0}

    def _logs(request):
        if request.headers.get("Prefer"):
            prefer_seen["n"] += 1
        return (200, {}, '{"value": [{"At": "2024-01-01T00:00:00Z", "Msg": "m"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Logs", callback=_logs)
    c = _make()
    with pytest.raises(ValueError, match="primary key"):
        c.read_table("Logs", {}, {"delta_tracking": "enabled"})
    records, offset = _make().read_table("Logs", {}, {"delta_tracking": "auto"})
    rows = list(records)
    assert prefer_seen["n"] == 0  # never probed
    assert rows and all("_deleted" not in r for r in rows)
    assert offset.get("snapshot_done") is True


# ---------------------------------------------------------------------------
# Round 46 — metadata-cache eviction race, fence self-check insert-race
# re-probe, delta fresh-link attribution, lb_cycle_started leak, lb_history
# sanitization
# ---------------------------------------------------------------------------


def test_metadata_cache_eviction_survives_concurrent_puts():
    """The lock-free eviction used to iterate the live dict while sibling
    threads insert/pop (> cap distinct service_urls on one driver) —
    "dictionary changed size during iteration" / KeyError escaped into the
    caller's metadata fetch. Candidates are now snapshotted first."""
    import sys
    import threading

    from databricks.labs.community_connector.sources.odata.odata import (
        _METADATA_CACHE,
        _metadata_cache_put,
    )

    _METADATA_CACHE.clear()
    errors = []
    # Shrink the GIL switch interval so the eviction loop's iterate-vs-pop
    # interleavings actually occur within the test's budget — at the 5ms
    # default the pre-fix race fires only sporadically.
    prev_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:

        def _hammer(tid):
            try:
                for i in range(600):
                    _metadata_cache_put(f"https://svc-{tid}-{i}.x/", ("x", None, None, float(i)))
            except Exception as exc:  # pragma: no cover - the pre-fix failure
                errors.append(repr(exc))

        threads = [threading.Thread(target=_hammer, args=(t,)) for t in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(prev_interval)
        _METADATA_CACHE.clear()
    assert errors == []


@responses.activate
def test_fence_self_check_survives_probe_race_insert():
    """A row inserted in the one-RTT window between the fence probe and the
    gt self-check made the check accuse a COMPLIANT server of ignoring
    $orderby — a spurious hard trigger failure scaling with write rate. On
    contradiction the check now re-probes with desc: at-or-above the
    check-found row proves desc works (PASS cached); the fence stays at the
    original probed max."""
    _mock_nested_metadata()
    state = {"rows": [{"Id": 1, "Name": "2024-01-01T00:00:00Z"}], "probes": 0}

    def _parents(request):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(request.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        rows = list(state["rows"])
        m = re.search(r"Name gt (\S+)", flt)
        if m:
            # The gt self-check: a fresh row landed after the first probe.
            state["rows"].append({"Id": 2, "Name": "2024-06-01T00:00:00Z"})
            rows = [r for r in state["rows"] if r["Name"] > m.group(1).strip("'")]
        elif "desc" in unquote(q.get("$orderby", [""])[0]):
            state["probes"] += 1
            rows = sorted(rows, key=lambda r: r["Name"], reverse=True)
        return (200, {}, json.dumps({"value": rows[:1]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    offset = c.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)
    # No spurious raise; fence stays at the original probed max.
    assert offset == {"cursor": "2024-01-01T00:00:00Z"}
    assert state["probes"] == 2  # initial probe + disambiguating re-probe
    # PASS verdict cached — the next trigger skips the self-check entirely.
    assert c._cached_capability("fence_desc_ok", table_name="Parents") is True
    n_before = len(responses.calls)
    c2 = _make()
    c2.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)
    assert len(responses.calls) == n_before + 1  # one probe, no gt check


@responses.activate
def test_delta_fresh_midwalk_link_404_not_blamed_on_stored_token():
    """A fresh @odata.nextLink minted by THIS walk's response that 404s must
    keep the bare HTTPError — the round-45 curation claimed the STORED
    token expired and prescribed a full refresh, the wrong remedy for a
    link that isn't persisted anywhere."""
    _mock_metadata()

    def _customers(request):
        if "$deltatoken=stored" in request.url:
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [{"Id": 1, "Name": "A", "ModifiedAt": "x"}],
                        "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=fresh2",
                    }
                ),
            )
        return (404, {}, '{"error": {"message": "page gone"}}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make()
    with pytest.raises(requests.HTTPError):
        records, _ = c.read_table(
            "Customers",
            {"delta_link": f"{SERVICE_URL}Customers?$deltatoken=stored"},
            {"delta_tracking": "enabled"},
        )
        list(records)


@responses.activate
def test_empty_completion_strips_lb_cycle_started():
    """The leaf-cursor walk's vanished-checkpoint empty completion bypasses
    _attach_lookback_state; the anchor used to leak, so the NEXT progressing
    walk recorded the whole idle gap as a "cycle span" — lb_history then
    carried a bogus multi-hour entry and the auto window pinned at the
    ceiling for 5 walks."""
    _mock_nested_metadata()
    # The parked ANCHOR is still enumerable (so the round-43 reset does NOT
    # fire) but its checkpointed rows vanished — the resume completes empty
    # through the `if not emitted:` early return.
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]}, match_querystring=False)
    responses.get(f"{SERVICE_URL}Parents(1)/Children", json={"value": []}, match_querystring=False)
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {
            "parent_idx": 0,
            "parent_keys": [{"Id": 1}],
            "truncated_chain_cursor": "2024-01-01T00:00:00Z",
            "running_max": "2024-01-01T00:00:00Z",
            "lb_cycle_started": 1000.0,
        },
        {"cursor_field": "ModifiedAt", "pagination": "nextlink", "cursor_probe": "false"},
    )
    assert list(records) == []
    assert "lb_cycle_started" not in offset
    assert offset.get("cursor") == "2024-01-01T00:00:00Z"


def test_lb_history_garbage_sanitized():
    """lb_history rides the user-visible checkpoint: a hand-edited entry
    ("abc", NaN, a negative) used to crash the window resolve uncurated —
    or float the read filter ABOVE the watermark (negative window = silent
    exclusion band). Non-finite/non-positive/non-numeric entries are now
    filtered before sizing."""
    c = _make()
    c._cursor_lookback = "auto"
    assert c._resolve_active_lookback({"lb_history": ["abc", -5.0, float("nan"), True]}) == 0
    assert c._resolve_active_lookback({"lb_history": ["abc", 2.0]}) == 3.0  # 2.0 × 1.5


# ---------------------------------------------------------------------------
# Round 47 — capability cache LRU cap (memory + disk-inode bound)
# ---------------------------------------------------------------------------


def test_capability_cache_caps_entries_and_sweeps_disk():
    """`_CAPABILITY_CACHE` used to grow one dict entry + one mtime-memo entry
    + one tempdir file per distinct service_url for the driver's whole
    lifetime (its sibling _METADATA_CACHE was capped, this one wasn't). It's
    now bounded: creating > cap distinct services evicts oldest-created
    entries and deletes their on-disk mirrors."""
    import glob

    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_CACHE_MAX_SERVICES,
        _CAPABILITY_DISK_MTIME,
        _capability_cache_path,
        _capability_cache_store,
        _clear_capability_cache,
    )

    _clear_capability_cache()
    n = _CAPABILITY_CACHE_MAX_SERVICES + 50
    paths = []
    for i in range(n):
        svc = f"https://cap-r47-{i}.example.com/odata/"
        paths.append(_capability_cache_path(svc))
        _capability_cache_store(svc, "batch_ok", True)
    try:
        # In-memory dict and mtime memo are both bounded at the cap.
        assert len(_CAPABILITY_CACHE) == _CAPABILITY_CACHE_MAX_SERVICES
        assert len(_CAPABILITY_DISK_MTIME) <= _CAPABILITY_CACHE_MAX_SERVICES
        # The most-recent cap services survive; the oldest are evicted.
        assert f"https://cap-r47-{n - 1}.example.com/odata/" in _CAPABILITY_CACHE
        assert f"https://cap-r47-0.example.com/odata/" not in _CAPABILITY_CACHE
        # Evicted services' disk mirrors are deleted, not left as dead inodes.
        assert not os.path.exists(paths[0])
        assert os.path.exists(paths[-1])
        # Total on-disk mirror count is bounded at the cap too.
        assert sum(os.path.exists(p) for p in paths) == _CAPABILITY_CACHE_MAX_SERVICES
    finally:
        _clear_capability_cache()


def test_capability_cache_cap_survives_concurrent_first_touch():
    """Entry creation now happens under _CAPABILITY_LOCK (the eviction
    iterates the shared dict); concurrent first-touches of many services
    must not raise or blow past the cap."""
    import sys
    import threading

    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_CACHE_MAX_SERVICES,
        _capability_cache_store,
        _clear_capability_cache,
    )

    _clear_capability_cache()
    errors = []
    prev = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:

        def _hammer(tid):
            try:
                for i in range(200):
                    _capability_cache_store(
                        f"https://race-r47-{tid}-{i}.example.com/odata/", "batch_ok", True
                    )
            except Exception as exc:  # pragma: no cover - pre-fix would surface here
                errors.append(repr(exc))

        threads = [threading.Thread(target=_hammer, args=(t,)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(prev)
    assert errors == []
    assert len(_CAPABILITY_CACHE) == _CAPABILITY_CACHE_MAX_SERVICES
    _clear_capability_cache()


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


def test_capability_flush_skips_memo_for_evicted_service():
    """A store whose service was concurrently evicted must NOT re-plant an
    orphaned mtime memo: that would leak the memo AND make the next load
    short-circuit past the on-disk verdict (a redundant re-probe). The memo
    write is now gated on live cache membership."""
    from databricks.labs.community_connector.sources.odata.odata import (
        _CAPABILITY_CACHE,
        _CAPABILITY_DISK_MTIME,
        _capability_cache_flush,
        _capability_cache_path,
        _clear_capability_cache,
    )

    _clear_capability_cache()
    svc = "https://flush-r48.example.com/odata/"
    path = _capability_cache_path(svc)
    # Service NOT in the cache (simulating an eviction between load and flush).
    _capability_cache_flush(svc, json.dumps({"batch_ok": True}))
    try:
        assert os.path.exists(path)  # the file is still written (disk authoritative)
        assert path not in _CAPABILITY_DISK_MTIME  # but no orphaned memo planted
    finally:
        _clear_capability_cache()


# ---------------------------------------------------------------------------
# Round 49 — Edm.Binary base64url decode, delta reserved-column collision,
# capability_cache_load symmetric memo-race gate, mode-aware no-progress log
# ---------------------------------------------------------------------------


_BINARY_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="F"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
   <Property Name="Blob" Type="Edm.Binary"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Fs" EntityType="NS.F"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


@responses.activate
def test_edm_binary_base64url_decoded_to_bytes():
    """OData v4.01 JSON encodes Edm.Binary as base64url (- / _ alphabet).
    The framework row parser only understands standard base64, so without an
    in-connector decode the payload is silently corrupted. read_table must
    emit raw bytes (which the parser then passes through untouched)."""
    import base64 as _b64

    from databricks.labs.community_connector.libs.utils import parse_value
    from pyspark.sql.types import BinaryType

    raw = bytes([0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF])  # -> base64url contains - and _
    wire = _b64.urlsafe_b64encode(raw).decode()
    assert "-" in wire or "_" in wire  # this payload actually exercises base64url
    responses.get(f"{SERVICE_URL}$metadata", body=_BINARY_MD, status=200)
    responses.get(f"{SERVICE_URL}Fs", json={"value": [{"Id": 1, "Blob": wire}]})
    c = _make()
    schema = c.get_table_schema("Fs", {})
    rows, _ = c.read_table("Fs", None, {})
    emitted = list(rows)[0]["Blob"]
    assert emitted == raw, f"binary not decoded to bytes at emit: {emitted!r}"
    blob_type = [f.dataType for f in schema.fields if f.name == "Blob"][0]
    assert isinstance(blob_type, BinaryType)
    assert parse_value(emitted, blob_type) == raw  # survives the framework parser


@responses.activate
def test_edm_binary_standard_base64_still_decoded():
    """A server that (non-spec) sends standard base64 for Edm.Binary must
    still decode — urlsafe_b64decode leaves +/ untouched, so both alphabets
    round-trip through the same path."""
    import base64 as _b64

    raw = bytes([0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF])
    wire = _b64.b64encode(raw).decode()  # standard alphabet: contains + and /
    assert "+" in wire or "/" in wire
    responses.get(f"{SERVICE_URL}$metadata", body=_BINARY_MD, status=200)
    responses.get(f"{SERVICE_URL}Fs", json={"value": [{"Id": 1, "Blob": wire}]})
    c = _make()
    emitted = list(c.read_table("Fs", None, {})[0])[0]["Blob"]
    assert emitted == raw


@responses.activate
def test_edm_binary_null_and_undecodable_preserved():
    """A null binary stays None; a value that is not decodable keeps its
    original form (no crash — the framework fallback then runs)."""
    responses.get(f"{SERVICE_URL}$metadata", body=_BINARY_MD, status=200)
    responses.get(
        f"{SERVICE_URL}Fs",
        json={"value": [{"Id": 1, "Blob": None}, {"Id": 2, "Blob": "!!not base64!!"}]},
    )
    c = _make()
    rows = list(c.read_table("Fs", None, {})[0])
    assert rows[0]["Blob"] is None
    assert rows[1]["Blob"] == "!!not base64!!"  # undecodable — left as-is, no raise


_COLLISION_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="W"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
   <Property Name="_deleted" Type="Edm.String"/>
   <Property Name="_lc_sequence" Type="Edm.String"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Widgets" EntityType="NS.W"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


@responses.activate
def test_delta_synthetic_column_collision_raises():
    """A source column literally named _deleted / _lc_sequence (legal: the
    OData v4 ABNF allows a leading underscore) collides with the delta
    synthetics. Under delta_tracking the schema must NOT silently emit
    duplicate columns / overwrite the source value — it raises a curated
    reserved-name error instead."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="reserved delta synthetic"):
        c.get_table_schema("Widgets", {"delta_tracking": "enabled"})


@responses.activate
def test_delta_collision_table_still_readable_without_delta():
    """The same table without delta tracking must schema/read fine — the
    guard only fires when the synthetics would actually be stamped."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    schema = c.get_table_schema("Widgets", {})
    names = [f.name for f in schema.fields]
    assert names.count("_deleted") == 1 and names.count("_lc_sequence") == 1


def test_capability_load_skips_memo_for_evicted_service():
    """Symmetric to the round-48 flush gate: if a sibling load evicts this
    service during load()'s lock-free file read, the merge must NOT re-plant
    an orphaned mtime memo — otherwise the next load short-circuits past the
    on-disk verdicts and returns an empty entry (silent verdict loss)."""
    from databricks.labs.community_connector.sources.odata import odata as _od

    _od._clear_capability_cache()
    _od._CAPABILITY_CACHE.clear()
    _od._CAPABILITY_DISK_MTIME.clear()
    svc = "https://memo-load-r49.example.com/odata/"
    path = _od._capability_cache_path(svc)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"batch_ok": True, "expand_ok": {"Orders": True}}))

    real_json_load = json.load
    fired = {"done": False}

    def _racing_load(fh, *a, **k):
        # Runs inside load()'s lock-free read window: simulate a sibling
        # load(other) that trips cap-eviction and pops THIS service + its memo.
        if not fired["done"]:
            fired["done"] = True
            with _od._CAPABILITY_LOCK:
                _od._CAPABILITY_CACHE.pop(svc, None)
                _od._CAPABILITY_DISK_MTIME.pop(path, None)
        return real_json_load(fh, *a, **k)

    json.load = _racing_load
    try:
        _od._capability_cache_load(svc)
    finally:
        json.load = real_json_load
    # No orphaned memo left behind for the evicted service...
    assert path not in _od._CAPABILITY_DISK_MTIME
    # ...so a fresh load re-reads and merges the persisted verdicts.
    entry2 = _od._capability_cache_load(svc)
    assert entry2.get("batch_ok") is True
    assert entry2.get("expand_ok") == {"Orders": True}
    _od._clear_capability_cache()


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


# ---------------------------------------------------------------------------
# Round 50 — null ancestor FK fail-loud guard, reserved-column guard mirrored
# into read_table_metadata
# ---------------------------------------------------------------------------


_FK_NULL_MD = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices><Schema Namespace="Nested" xmlns="http://docs.oasis-open.org/odata/ns/edm">
    <EntityType Name="Parent"><Key><PropertyRef Name="Id"/></Key>
      <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      <Property Name="Name" Type="Edm.String"/>
      <NavigationProperty Name="Children" Type="Collection(Nested.Child)" ContainsTarget="true"/></EntityType>
    <EntityType Name="Child"><Key><PropertyRef Name="Id"/></Key>
      <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      <Property Name="Label" Type="Edm.String"/></EntityType>
    <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="Nested.Parent"/></EntityContainer>
  </Schema></edmx:DataServices></edmx:Edmx>"""


@responses.activate
def test_null_ancestor_fk_fails_loudly_expand_path():
    """A parent entity that omits/nulls its own primary key (non-conformant —
    OData keys are never null) would stamp a null onto the NON-nullable FK
    column, sending a null MERGE key into apply_changes. Mirrors the leaf-PK
    never_pad protection onto the ancestor-FK side: fail loudly, don't emit."""
    import re as _re

    responses.get(f"{SERVICE_URL}$metadata", body=_FK_NULL_MD, status=200)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": [{"Name": "P1", "Children": [{"Id": 11, "Label": "a"}]}]},
        match_querystring=False,
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    # Absorb any (malformed) continuation drain so the ONLY failure is the guard.
    responses.add_callback(
        responses.GET,
        _re.compile(rf"{_re.escape(SERVICE_URL)}Parents\(.*\)/Children.*"),
        callback=lambda req: (200, {}, '{"value": []}'),
    )
    c = _make()
    # Non-nullable FK column, by construction.
    schema = c.get_table_schema("Parents__Children", {"expand_contained": "true"})
    fk = [f for f in schema.fields if f.name == "Parents_Id"][0]
    assert fk.nullable is False
    with pytest.raises(ValueError, match="missing its key"):
        list(c.read_table("Parents__Children", None, {"expand_contained": "true"})[0])


@responses.activate
def test_valid_ancestor_fk_still_stamped():
    """A well-formed parent (key present) still stamps the FK — the guard
    only fires on a null ancestor key."""
    responses.get(f"{SERVICE_URL}$metadata", body=_FK_NULL_MD, status=200)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 7, "Name": "P1", "Children": [{"Id": 11, "Label": "a"}]}]},
        match_querystring=False,
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    responses.get(f"{SERVICE_URL}Parents(7)/Children", json={"value": []})
    c = _make()
    rows = list(c.read_table("Parents__Children", None, {"expand_contained": "true"})[0])
    assert rows == [{"Id": 11, "Label": "a", "Parents_Id": 7}]


@responses.activate
def test_reserved_column_guard_also_in_read_table_metadata():
    """The reserved delta-synthetic collision must fail as loudly from
    read_table_metadata as from read_table (consistent ordering), not report
    cdc success for a table the later read would reject."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="reserved delta synthetic"):
        c.read_table_metadata("Widgets", {"delta_tracking": "enabled"})


# ---------------------------------------------------------------------------
# Round 51 — metadata reserved-column guard mirrors the read's select filter,
# keyless entity set + cursor_field fails loudly (no keyless CDC contract)
# ---------------------------------------------------------------------------


@responses.activate
def test_reserved_column_metadata_respects_select_drop():
    """When `select` drops the colliding source column, the synthetic takes
    its place and the READ succeeds — so read_table_metadata must NOT raise
    (it now mirrors get_table_schema's select-filtered check, not raw
    _fields_for). Round-50's guard over-fired here."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    opts = {"delta_tracking": "enabled", "select": "Id"}
    # The read path is happy — source _deleted/_lc_sequence dropped by select,
    # synthetics appended.
    names = [f.name for f in c.get_table_schema("Widgets", opts).fields]
    assert names == ["Id", "_deleted", "_lc_sequence"]
    # Metadata must agree (no false-positive raise).
    meta = c.read_table_metadata("Widgets", opts)
    assert meta["ingestion_type"] == "cdc"
    assert meta["cursor_field"] == "_lc_sequence"


@responses.activate
def test_reserved_column_metadata_raises_when_collision_kept():
    """With no select (colliding source column survives), metadata still
    raises — the guard fires exactly when the read would."""
    responses.get(f"{SERVICE_URL}$metadata", body=_COLLISION_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="reserved delta synthetic"):
        c.read_table_metadata("Widgets", {"delta_tracking": "enabled"})


_KEYLESS_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="V">
   <Property Name="X" Type="Edm.Int32"/>
   <Property Name="M" Type="Edm.DateTimeOffset"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Views" EntityType="NS.V"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


@responses.activate
def test_keyless_cursor_field_fails_loudly():
    """cursor_field reports ingestion_type=cdc, which apply_changes MERGEs on
    the primary key; a keyless entity type declares none, so the MERGE has no
    key — silent loss by construction. Mirror the keyless delta gate: raise,
    don't emit a keyless CDC contract."""
    responses.get(f"{SERVICE_URL}$metadata", body=_KEYLESS_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="no key in .metadata"):
        c.read_table_metadata("Views", {"cursor_field": "M"})


@responses.activate
def test_keyless_snapshot_still_ok():
    """A keyless set WITHOUT cursor_field is a plain snapshot (no MERGE, no
    key needed) — must still succeed."""
    responses.get(f"{SERVICE_URL}$metadata", body=_KEYLESS_MD, status=200)
    c = _make()
    meta = c.read_table_metadata("Views", {})
    assert meta == {"primary_keys": [], "cursor_field": None, "ingestion_type": "snapshot"}


# ---------------------------------------------------------------------------
# Round 52 — read_table_metadata mirrors _validate_select_columns; fence
# self-check tolerates the insert-then-delete race (still raises genuine
# desc-ignore)
# ---------------------------------------------------------------------------


@responses.activate
def test_metadata_validates_select_drops_cursor():
    """read_table_metadata must fail in the same place as the read when a
    select drops the cursor_field (dispatch runs _validate_select_columns for
    every read; metadata used to skip it and report a valid CDC source)."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="omits cursor_field"):
        c.read_table_metadata("Customers", {"cursor_field": "ModifiedAt", "select": "Id"})


@responses.activate
def test_metadata_validates_select_drops_pk():
    """Same, for a select that drops a primary key."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="omits primary-key"):
        c.read_table_metadata(
            "Customers", {"cursor_field": "ModifiedAt", "select": "Name,ModifiedAt"}
        )


_FENCE_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="N" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="Parent"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
   <Property Name="Seq" Type="Edm.Int32"/>
   <NavigationProperty Name="Children" Type="Collection(N.Child)" ContainsTarget="true"/></EntityType>
  <EntityType Name="Child"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="N.Parent"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


@responses.activate
def test_fence_check_tolerates_insert_then_delete_race():
    """A desc-COMPLIANT server where a row inserted after the max-probe is
    deleted before the re-probe must NOT raise: the desc re-probe surfaces the
    true (post-delete) max and no row persists above it. Round-46 handled the
    plain-insert race; this covers insert-then-delete."""
    from urllib.parse import parse_qs, unquote, urlparse

    responses.get(f"{SERVICE_URL}$metadata", body=_FENCE_MD, status=200)
    gt_calls = {"n": 0}

    def _cb(req):
        q = parse_qs(urlparse(req.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        orderby = unquote(q.get("$orderby", [""])[0])
        if "Seq desc" in orderby:  # both desc probes return the compliant max
            return (200, {}, json.dumps({"value": [{"Seq": 10}]}))
        if "Seq gt" in flt:
            gt_calls["n"] += 1
            # self-check sees the raced insert; still_above sees it deleted.
            body = {"value": [{"Seq": 11}]} if gt_calls["n"] == 1 else {"value": []}
            return (200, {}, json.dumps(body))
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_cb)
    c = _make()
    # Must not raise — the compliant server is not accused of ignoring $orderby.
    off = c.latest_offset("Parents__Children", {"cursor_field": "Seq"}, None)
    assert off is not None


@responses.activate
def test_fence_check_still_raises_on_genuine_desc_ignore():
    """A server that genuinely ignores $orderby (desc probe returns a non-max
    while rows persist above it) must still raise — the fix must not mask the
    real failure."""
    from urllib.parse import parse_qs, unquote, urlparse

    responses.get(f"{SERVICE_URL}$metadata", body=_FENCE_MD, status=200)

    def _cb(req):
        q = parse_qs(urlparse(req.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        orderby = unquote(q.get("$orderby", [""])[0])
        if "Seq desc" in orderby:  # ignores desc: returns a stale non-max
            return (200, {}, json.dumps({"value": [{"Seq": 5}]}))
        if "Seq gt" in flt:  # a row genuinely persists above the probed value
            return (200, {}, json.dumps({"value": [{"Seq": 10}]}))
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_cb)
    c = _make()
    with pytest.raises(ValueError, match="ignoring .orderby"):
        c.latest_offset("Parents__Children", {"cursor_field": "Seq"}, None)
