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
import time

import pytest
import responses

from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal
from pyspark.sql.types import IntegerType, StringType, TimestampType
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestODataConnector(LakeflowConnectTests):
    """Contract test suite for the OData connector against the simulator.

    The simulator stands up a Northwind-shaped service at
    ``/odata/`` with a fixed ``$metadata`` document and Customers /
    Orders entity sets seeded from the JSON corpus. Connector reads
    flow through the simulator's custom OData handler (entity_set.py)
    which implements just enough ``$top``/``$skip``/``$filter``/
    ``$orderby``/``@odata.nextLink`` semantics to drive the suite.
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


# ---------------------------------------------------------------------------
# Static helpers
# ---------------------------------------------------------------------------


def test_odata_literal_quotes_strings_and_escapes():
    assert _odata_literal("O'Brien") == "'O''Brien'"
    assert _odata_literal(5) == "5"
    assert _odata_literal(True) == "true"


def test_odata_literal_passes_iso_timestamps_bare():
    assert _odata_literal("2024-05-01T00:00:00Z") == "2024-05-01T00:00:00Z"


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
    assert offset == {}
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
    captured = {}

    def _callback(request):
        captured["url"] = request.url
        return (200, {}, '{"value": [{"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    records, offset = c.read_table(
        "Customers",
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "10"},
    )
    rows = list(records)
    assert rows == [{"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"}]
    assert offset == {"cursor": "2024-03-01T00:00:00Z"}
    # Neither `le` nor `gt` should appear in the URL — no cursor filter
    # at all on the first call.
    normalised = captured["url"].replace("%20", " ")
    assert " le " not in normalised
    assert " gt " not in normalised


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
    new rows."""
    _mock_metadata()
    call_count = {"n": 0}

    def _callback(request):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # Initial drain — two records already in the source.
            return (
                200,
                {},
                '{"value": ['
                '{"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"},'
                '{"Id": 2, "ModifiedAt": "2024-03-02T00:00:00Z"}]}',
            )
        if call_count["n"] == 2:
            # No data since the offset; mirrors the "caught up" state
            # between bursts.
            return (200, {}, '{"value": []}')
        # New row arrived while the stream was idle.
        return (
            200,
            {},
            '{"value": [{"Id": 3, "ModifiedAt": "2024-03-05T00:00:00Z"}]}',
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    # Batch 1: no offset, two rows drain. Trim of trailing single-cohort
    # leaves [Id=1]; offset = 2024-03-01.
    rows1, offset1 = c.read_table("Customers", None, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows1] == [1]
    assert offset1 == {"cursor": "2024-03-01T00:00:00Z"}

    # Batch 2: feeding offset1 back. Source returned empty — stable
    # offset signals "no more data" to Spark; the same connector
    # instance handled the call without any cap-driven short-circuit.
    rows2, offset2 = c.read_table("Customers", offset1, {"cursor_field": "ModifiedAt"})
    assert list(rows2) == []
    assert offset2 == offset1

    # Batch 3: new row appeared in the source. The continuous-polling
    # connector picks it up using only the `gt` filter — no frozen
    # snapshot ceiling getting in the way.
    rows3, offset3 = c.read_table("Customers", offset2, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows3] == [3]
    assert offset3 == {"cursor": "2024-03-05T00:00:00Z"}


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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "5"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert offset == {"cursor": "2024-05-02T00:00:00Z"}


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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert offset == {"cursor": "2024-05-01T00:00:00Z"}


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
            None,
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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2, 3]
    assert offset == {"cursor": "2024-05-01T00:00:00Z"}


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
            {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
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
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]


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
    c.read_table("Customers", None, {"cursor_field": "ModifiedAt"})
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
    assert offset == {"cursor": "2024-05-02T00:00:00Z"}


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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert offset == {"cursor": "2024-04-01T00:00:00Z"}


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


@responses.activate
def test_oauth2_fetches_token():
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "minted", "token_type": "Bearer"},
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    c.list_tables()
    assert c._get_session().headers["Authorization"] == "Bearer minted"


@responses.activate
def test_oauth2_client_credentials_uses_client_credentials_grant():
    """No refresh_token on the connection → client_credentials grant."""
    captured = {}

    def _token_callback(request):
        captured["body"] = request.body
        return (200, {}, '{"access_token": "cc-minted", "token_type": "Bearer"}')

    responses.add_callback(
        responses.POST, "https://idp.example.com/token", callback=_token_callback
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    c.list_tables()
    assert "grant_type=client_credentials" in captured["body"]
    assert c._get_session().headers["Authorization"] == "Bearer cc-minted"


@responses.activate
def test_oauth2_user_flow_uses_pre_supplied_access_token():
    """When `oauth2_access_token` is provided, the connector uses it
    directly and does NOT hit the token endpoint at startup."""
    # Register the token URL but don't expect it to be called.
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "should-not-be-used", "token_type": "Bearer"},
    )
    _mock_metadata()
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_access_token": "user-flow-access",
            "oauth2_refresh_token": "user-flow-refresh",
        }
    )
    c.list_tables()
    assert c._get_session().headers["Authorization"] == "Bearer user-flow-access"
    # The token endpoint must not have been called during list_tables.
    token_calls = [c for c in responses.calls if c.request.url == "https://idp.example.com/token"]
    assert token_calls == []


@responses.activate
def test_oauth2_user_flow_refreshes_on_401_and_retries():
    """An expired access token surfaces as 401. The connector refreshes
    via `grant_type=refresh_token`, swaps the header, and retries the
    request once before raising."""
    _mock_metadata()
    captured_token_bodies = []

    def _token_callback(request):
        captured_token_bodies.append(request.body)
        return (200, {}, '{"access_token": "refreshed-access", "token_type": "Bearer"}')

    responses.add_callback(
        responses.POST, "https://idp.example.com/token", callback=_token_callback
    )

    call_count = {"n": 0}

    def _customers_callback(request):
        call_count["n"] += 1
        auth = request.headers.get("Authorization", "")
        if call_count["n"] == 1:
            # First call: stale token → 401.
            assert auth == "Bearer stale-access"
            return (401, {}, '{"error": "expired"}')
        # Second call: must carry the refreshed token.
        assert auth == "Bearer refreshed-access"
        return (
            200,
            {},
            '{"value": [{"Id": 1, "Name": "A", "ModifiedAt": "2024-01-01T00:00:00Z"}]}',
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers_callback)

    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_access_token": "stale-access",
            "oauth2_refresh_token": "user-flow-refresh",
        }
    )
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in rows] == [1]
    assert call_count["n"] == 2
    assert len(captured_token_bodies) == 1
    body = captured_token_bodies[0]
    assert "grant_type=refresh_token" in body
    assert "refresh_token=user-flow-refresh" in body
    # Session's Authorization header must now carry the refreshed token.
    assert c._get_session().headers["Authorization"] == "Bearer refreshed-access"


@responses.activate
def test_oauth2_user_flow_tracks_rotated_refresh_token():
    """Some providers rotate the refresh token on every refresh. The
    new value must be picked up so the next refresh doesn't use the
    already-invalidated one."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={
            "access_token": "rotated-access",
            "refresh_token": "rotated-refresh",
            "token_type": "Bearer",
        },
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "initial-refresh",
        }
    )
    c.list_tables()
    assert c.options["oauth2_refresh_token"] == "rotated-refresh"


@responses.activate
def test_oauth2_captures_expires_in_from_token_response():
    """`expires_in` from the token endpoint is stored as a monotonic
    deadline so the next request can pre-emptively refresh."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "minted", "expires_in": 3600, "token_type": "Bearer"},
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    before = time.monotonic()
    c.list_tables()  # triggers session creation which mints the token
    after = time.monotonic()
    # Expires_at should be ~ now + 3600 - 60s buffer, accounting for test time.
    assert c._access_token_expires_at is not None
    assert before + 3600 - 60 - 1 <= c._access_token_expires_at <= after + 3600 - 60


@responses.activate
def test_oauth2_preemptively_refreshes_when_token_near_expiry():
    """When the recorded deadline has passed, `_http_get` mints a fresh
    token BEFORE issuing the request — no 401 round-trip needed."""
    _mock_metadata()

    token_responses = iter(
        [
            '{"access_token": "first", "expires_in": 3600, "token_type": "Bearer"}',
            '{"access_token": "second", "expires_in": 3600, "token_type": "Bearer"}',
        ]
    )

    def _token_callback(request):
        return (200, {}, next(token_responses))

    responses.add_callback(
        responses.POST, "https://idp.example.com/token", callback=_token_callback
    )

    request_auths = []

    def _customers_callback(request):
        request_auths.append(request.headers.get("Authorization"))
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers_callback)

    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    # Force session creation (mints the first token), then yank the deadline
    # into the past to simulate post-expiry on the next request.
    session = c._get_session()
    assert session.headers["Authorization"] == "Bearer first"
    c._access_token_expires_at = time.monotonic() - 1.0

    list(c.read_table("Customers", None, {})[0])
    # No 401 in this scenario — pre-emptive refresh happened before send,
    # so the single Customers request carries the refreshed token.
    assert request_auths == ["Bearer second"]


@responses.activate
def test_oauth2_handles_token_endpoint_without_expires_in():
    """Some token endpoints omit `expires_in`. Treat that as 'unknown
    expiry' and fall back to the 401-retry path — no exception, just no
    pre-emptive refresh."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "minted", "token_type": "Bearer"},  # no expires_in
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    c.list_tables()
    assert c._access_token_expires_at is None


@responses.activate
def test_oauth2_refresh_failure_raises_actionable_error():
    """A 401 from the token endpoint during a refresh-token grant
    surfaces the OAuth2 error code + description, and names the
    `oauth2_refresh_token` / `oauth2_client_id` fields the user
    should check."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"error": "invalid_grant", "error_description": "refresh_token expired"},
        status=401,
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "stale",
        }
    )
    with pytest.raises(ValueError) as ei:
        c.list_tables()
    msg = str(ei.value)
    assert "refreshing the access token" in msg
    assert "oauth2_refresh_token" in msg
    assert "oauth2_client_id" in msg
    assert "invalid_grant" in msg
    assert "refresh_token expired" in msg


@responses.activate
def test_oauth2_client_credentials_failure_raises_actionable_error():
    """A 401 from the token endpoint during client_credentials names
    the client_id / client_secret / token_url / scope fields."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"error": "invalid_client"},
        status=401,
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "wrong-secret",
        }
    )
    with pytest.raises(ValueError) as ei:
        c.list_tables()
    msg = str(ei.value)
    assert "client_credentials grant" in msg
    assert "oauth2_client_secret" in msg
    assert "invalid_client" in msg


@responses.activate
def test_oauth2_persistent_401_after_refresh_raises_permission_error():
    """If the source keeps returning 401 even after a fresh token
    arrives, the access token isn't the problem. Surface a
    PermissionError that points at scope / principal / tenant rather
    than the token itself."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "fresh", "token_type": "Bearer"},
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=401,
        json={"error": "AccessDenied", "message": "principal lacks read on Customers"},
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=401,
        json={"error": "AccessDenied", "message": "principal lacks read on Customers"},
    )
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
            "oauth2_refresh_token": "valid",
        }
    )
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "even after refreshing" in msg
    assert "oauth2_scope" in msg
    assert "service_url" in msg


def test_missing_service_url_raises():
    with pytest.raises(ValueError, match="service_url"):
        ODataLakeflowConnect({})


# ---------------------------------------------------------------------------
# 401 / 403 UX when there's no OAuth refresh path
# ---------------------------------------------------------------------------

# When the source returns 401/403 and the connector can't auto-refresh
# the token (bearer, basic, api_key, or OAuth without client creds /
# refresh token), the raw HTTPError gives the operator nothing
# actionable. The connector raises PermissionError with auth-mode-
# specific remediation hints instead.


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
    assert "auth_type=oauth2" in msg  # suggested upgrade path
    assert "oauth2_client_id" in msg
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
def test_oauth2_without_refresh_path_401_raises_actionable_permission_error():
    """auth_type=oauth2 + pre-supplied access_token + no refresh_token +
    no client_id/secret is a legitimate config — but means there's no
    refresh path. A 401 here can't be auto-fixed; surface the auth
    options that need attention."""
    _mock_metadata()
    responses.add(responses.GET, f"{SERVICE_URL}Customers", status=401, body="expired")
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_access_token": "stale-access",
            # No client_id / client_secret → no refresh path.
        }
    )
    with pytest.raises(PermissionError) as ei:
        list(c.read_table("Customers", None, {})[0])
    msg = str(ei.value)
    assert "auth_type=oauth2" in msg
    assert "oauth2_refresh_token" in msg
    assert "oauth2_access_token" in msg
    assert "oauth2_scope" in msg


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
    assert "bearer, basic, api_key, oauth2" in msg


@responses.activate
def test_403_on_bearer_raises_permission_error():
    """403 means authenticated-but-not-authorized — different from 401
    but equally a permission issue. Same error UX."""
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
    assert "auth_type=bearer" in msg
    assert "403" in msg


@responses.activate
def test_oauth2_with_refresh_path_still_uses_existing_flow():
    """A 401 with an OAuth refresh path goes through the existing
    refresh-and-retry logic, NOT the new no-refresh-path error. This
    is the regression-guard for the existing OAuth UX."""
    _mock_metadata()
    responses.post(
        "https://idp.example.com/token",
        json={"access_token": "fresh", "token_type": "Bearer"},
    )
    call = {"n": 0}

    def _customers(request):
        call["n"] += 1
        if call["n"] == 1:
            return (401, {}, '{"error": "expired"}')
        return (200, {}, '{"value": [{"Id": 1, "Name": "x", "ModifiedAt": "x"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_customers)
    c = _make(
        {
            "auth_type": "oauth2",
            "oauth2_token_url": "https://idp.example.com/token",
            "oauth2_client_id": "id",
            "oauth2_client_secret": "secret",
        }
    )
    # Refreshable 401 → resolves cleanly via the existing path. New
    # PermissionError code path is bypassed.
    rows, _ = c.read_table("Customers", None, {})
    assert [r["Id"] for r in list(rows)] == [1]
    assert call["n"] == 2  # 401 then 200 after refresh


# ---------------------------------------------------------------------------
# Multi-schema (SupportsNamespaces)
# ---------------------------------------------------------------------------


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
    assert offset == {"delta_link": DELTA_LINK_V1}


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
    assert offset == {}


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
    assert offset == {"delta_link": DELTA_LINK_V1}


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
    assert tombstone == {"Id": 2, "_deleted": True, "_lc_sequence": tombstone["_lc_sequence"]}
    assert offset == {"delta_link": DELTA_LINK_V2}


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
    assert offset == {"delta_link": DELTA_LINK_V2}


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
    assert offset == {"delta_link": DELTA_LINK_V1}


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
    assert offset == {"delta_link": DELTA_LINK_V2}


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
def test_delta_max_records_caps_and_stashes_next_link():
    """A long catch-up after a paused pipeline can return more rows than
    ``max_records_per_batch``. The connector caps mid-pagination and
    stashes the unfollowed ``@odata.nextLink`` as the resume point."""
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
    assert [r["Id"] for r in rows] == [1, 2]
    # Offset carries both prior delta_link (fallback) AND next_link
    # (preferred resume point).
    assert offset == {"delta_link": DELTA_LINK_V1, "next_link": next_link}


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
    assert offset == {"delta_link": DELTA_LINK_V2}
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
    assert offset == {"delta_link": DELTA_LINK_V1}


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
    url = c._build_contained_url(["Parents", "Children"], [{"Id": 7}], {})
    assert url.startswith(f"{SERVICE_URL}Parents(7)/Children?")
    assert "$top=1000" in url


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


@responses.activate
def test_build_expand_url_three_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children", "Notes"], {})
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
    url = c._build_expand_url(["A", "B", "C", "D"], {})
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
    url = c._build_expand_url(["Parents", "Children"], {})
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
    assert offset == {}
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
    records, _ = c.read_table("Parents__Children", None, {"expand_contained": "true"})
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
    """When the cap happens to fire exactly at a page's last top_row,
    the current page item is NOT re-queued (it's fully drained); only
    the server's next-page URL stays in ``pending_fetches``."""
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
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {},
        {"expand_contained": "true", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert len(rows) == 2
    pending = offset.get("pending_fetches")
    assert pending == [{"url": next_link, "level": 0, "chain": [], "cur_val": None, "skip": 0}]
    assert "cursor" not in offset


def test_read_table_disables_cap_when_start_offset_none_and_cap_unset():
    """Spark's batch reader (``LakeflowBatchReader``) calls
    ``read_table`` with ``start_offset=None`` and discards the
    returned end-offset. ``read_table`` detects that signal and
    raises ``max_records_per_batch`` to a near-infinite sentinel so
    the cap can't fire and the chain drains fully in one call —
    parked ``pending_fetches`` would otherwise be silently dropped.

    Streaming readers always pass a dict (``{}`` initial or parked
    offset), so this override does not touch the streaming path.

    If the user passes ``max_records_per_batch`` themselves, the
    override is skipped — same-cursor-cohort overflow detection and
    any other cap-driven behaviour stays intact."""
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

    # start_offset=None BUT cap explicitly set → user's value wins.
    captured.clear()
    ODataLakeflowConnect._read_contained_expand = _spy  # type: ignore[assignment]
    try:
        c.read_table(
            "Parents__Children",
            None,
            {"expand_contained": "true", "max_records_per_batch": "50"},
        )
    finally:
        ODataLakeflowConnect._read_contained_expand = original  # type: ignore[assignment]
    assert captured[0]["max_records_per_batch"] == "50"

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
    assert offset == {}


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
    assert offset == {}


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
    assert offset == {"cursor": "2024-07-10T00:00:00Z"}


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
    assert offset == {}
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
    assert offset2 == {}


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
    records, _ = c.read_table("Parents__Children__Notes", None, {})
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
    responses.get(
        f"{SERVICE_URL}Parents(5)/Children(10)/Notes",
        json={"value": [{"Id": 100, "Text": "x"}]},
        match=[
            responses.matchers.query_param_matcher(
                {"$top": "1000", "$filter": "Id eq 100", "$orderby": "Id asc"}
            )
        ],
    )
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
    # Dynamic tops for N=2 page_size=1000: [100, 10].
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
                    "$top": "1000",
                    "$filter": "(ModifiedAt gt 2024-01-01T00:00:00Z) and (Label eq 'a')",
                    "$orderby": "ModifiedAt asc,Id asc",
                }
            )
        ],
    )
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
    assert offset == {"cursor": "2024-01-02T00:00:00Z"}


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
    records, offset = c.read_table("Parents__Children", None, {"cursor_field": "ModifiedAt"})
    rows = list(records)
    assert len(rows) == 2
    assert offset == {"cursor": "2024-01-02T00:00:00Z"}
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
    assert offset == {"cursor": "2024-01-03T00:00:00Z"}
    # Cursor filter present on the leaf call. Call order:
    # 0: $metadata, 1: Parents (PK walk), 2: Parents(1)/Children (leaf).
    leaf_call = responses.calls[2].request.url
    assert "ModifiedAt%20gt%20" in leaf_call or "ModifiedAt+gt+" in leaf_call


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
    assert offset == {"cursor": "2024-01-02T00:00:00Z"}


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
            {"cursor_field": "ModifiedAt"},
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
            {"cursor_field": "ModifiedAt"},
        )
        list(records)


@responses.activate
def test_contained_incremental_truncation_trims_boundary_cohort():
    """When the per-parent walk truncates, the trailing same-cursor cohort
    of the truncated chain is trimmed and the offset carries a
    ``truncated_chain_cursor`` so the resumed call re-picks up exactly
    that cohort without skipping it (Option A boundary trim, scoped to
    the truncated chain only)."""
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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # Trim drops the c2 boundary cohort; only c1 is emitted.
    assert len(rows) == 1
    assert rows[0]["ModifiedAt"] == "2024-01-01T00:00:00Z"
    # Resume re-fetches parent 0 from cursor gt c1, picking up c2 + beyond.
    assert offset == {
        "parent_idx": 0,
        "truncated_chain_cursor": "2024-01-01T00:00:00Z",
    }


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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # All three same-cursor rows come through despite the cap of 2 ...
    assert [r["Id"] for r in rows] == [11, 12, 13]
    # ... and the watermark advances to that value with the terminal
    # offset shape — no parent_idx / truncated_chain_cursor parked.
    assert offset == {"cursor": "2024-01-01T00:00:00Z"}


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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # Parent 1's full cohort + parent 2's trimmed prefix (22's cohort dropped).
    assert [r["Id"] for r in rows] == [11, 12, 21]
    # Checkpoint lands on parent 2 (index 1) at its last distinct cursor.
    assert offset == {
        "parent_idx": 1,
        "truncated_chain_cursor": "2024-02-01T00:00:00Z",
    }


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
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    # Both chains' rows come through; offset is back to natural-completion shape.
    assert {r["ModifiedAt"] for r in rows} == {
        "2024-01-02T00:00:00Z",
        "2024-01-05T00:00:00Z",
    }
    assert offset == {"cursor": "2024-01-05T00:00:00Z"}
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
        None,
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    # Whole page emitted (page-boundary truncation; no Option A trim).
    assert len(rows) == 2
    assert offset == {
        "parent_idx": 0,
        "chain_next_link": next_link,
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
    assert offset == {"cursor": "2024-01-05T00:00:00Z"}
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
        None,
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
    records, offset = c.read_table("Parents__Children__Notes", None, {"cursor_field": "ModifiedAt"})
    rows = list(records)
    # All 3 leaf rows emitted; cursor value propagated from ancestor.
    assert len(rows) == 3
    assert all(r["ModifiedAt"] for r in rows)
    # Children with Id=10 stamps its ModifiedAt onto its two notes.
    notes_under_10 = [r for r in rows if r["Children_Id"] == 10]
    assert all(r["ModifiedAt"] == "2024-01-01T00:00:00Z" for r in notes_under_10)
    # Offset advances to max ancestor cursor.
    assert offset == {"cursor": "2024-01-02T00:00:00Z"}
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
    assert offset == {"cursor": "2024-01-02T00:00:00Z"}
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
    assert rows == [{"Id": 1, "Name": "x"}]


@responses.activate
def test_partition_latest_offset_probes_top_level_max_cursor():
    """In streaming mode the fence comes from a single
    ``?$top=1&$orderby=<cursor> desc`` probe against the top set."""
    _mock_nested_metadata()
    # Probe response: the max cursor row.
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": [{"Id": 9, "Name": "z"}]},
        match_querystring=False,
    )
    # Add a Name property to the metadata-mocked Parent so the probe
    # finds a column. The nested metadata declares Parent.Name already.
    c = _make()
    offset = c.latest_offset(
        "Parents__Children",
        {"cursor_field": "Name"},
        None,
    )
    assert offset == {"cursor": "z"}


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


# ---------------------------------------------------------------------------
# 429 / 503 retry with backoff
# ---------------------------------------------------------------------------


def _patch_sleep(monkeypatch):
    """Capture every ``time.sleep`` call from the connector retry loop.

    Returns the list the sleeps are appended into — tests assert on
    durations directly. The lambda short-circuits the real sleep so the
    suite stays sub-second.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.time.sleep",
        lambda s: sleeps.append(s),
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
    rows, _ = c.read_table("Customers", None, {})
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
    rows, _ = c.read_table("Customers", None, {})
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
    rows, _ = c.read_table("Customers", None, {})
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
