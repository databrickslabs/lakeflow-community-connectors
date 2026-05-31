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
def test_incremental_first_call_uses_le_init_filter():
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
    # First call should filter cursor <= init_ts (no `gt` clause).
    assert "%20le%20" in captured["url"] or " le " in captured["url"]
    assert "%20gt%20" not in captured["url"]


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
def test_incremental_skips_call_when_caught_up_to_init_ts():
    _mock_metadata()
    # Don't register Customers URL — if a request is made we'll fail.
    c = _make()
    # Far-future cursor > init_ts → short-circuit.
    start = {"cursor": "9999-01-01T00:00:00Z"}
    records, offset = c.read_table("Customers", start, {"cursor_field": "ModifiedAt"})
    assert list(records) == []
    assert offset == start


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
