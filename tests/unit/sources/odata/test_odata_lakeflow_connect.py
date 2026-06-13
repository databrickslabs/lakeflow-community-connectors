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


@responses.activate
def test_build_expand_url_three_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children", "Notes"], {})
    assert "$expand=Children($expand=Notes)" in url
    assert url.startswith(f"{SERVICE_URL}Parents?")


@responses.activate
def test_build_expand_url_four_level_nests_correctly():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["A", "B", "C", "D"], {})
    assert "$expand=B($expand=C($expand=D))" in url


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
def test_contained_expand_invalid_value_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid expand_contained"):
        c.read_table("Parents__Children", None, {"expand_contained": "yes"})


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
def test_contained_incremental_truncation_raises_when_cohort_exceeds_cap():
    """If max_records_per_batch is smaller than a single same-cursor
    cohort within one parent, trimming leaves zero rows and we surface
    an actionable RuntimeError (same shape as the flat path)."""
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
    with pytest.raises(RuntimeError, match="largest same-cursor cohort"):
        records, _ = c.read_table(
            "Parents__Children",
            None,
            {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
        )
        list(records)


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
