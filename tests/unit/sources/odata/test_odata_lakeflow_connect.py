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
