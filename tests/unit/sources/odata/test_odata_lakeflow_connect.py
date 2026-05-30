"""Unit tests for the OData LakeflowConnect connector.

Uses `responses` to mock HTTP. Drop this file under
tests/unit/ in the lakeflow-community-connectors repo to run it
inside the standard test harness.
"""

import pytest
import responses

from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal
from pyspark.sql.types import IntegerType, StringType, TimestampType


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
def test_incremental_emits_full_batch_advances_to_last_cursor():
    """Pattern 2: no boundary trim. Every record returned by the server
    flows through, and the offset advances to the last record's cursor.

    Trade-off: a same-cursor row added between this batch and the next
    won't be picked up (the next call uses strict `>`). The cursor field
    is expected to be high-cardinality enough to make that race
    negligible. In return, no row is ever emitted twice — important when
    the destination doesn't dedupe (empty primary_keys)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-03T00:00:00Z"},
                {"Id": 4, "ModifiedAt": "2024-05-03T00:00:00Z"},
                {"Id": 5, "ModifiedAt": "2024-05-03T00:00:00Z"},
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
    assert [r["Id"] for r in rows] == [1, 2, 3, 4, 5]
    assert offset == {"cursor": "2024-05-03T00:00:00Z"}


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
    boundary record never appears twice in the destination."""
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
    assert [r["Id"] for r in rows] == [2, 3]
    assert offset == {"cursor": "2024-05-03T00:00:00Z"}


@responses.activate
def test_incremental_max_records_caps_batch_and_advances_offset():
    """Pattern 2: when the cap is hit, emit exactly `max_records` records
    and advance the offset to the last emitted record's cursor. The
    next call resumes from there with a strict `>` filter."""
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
    assert [r["Id"] for r in rows] == [1, 2]
    assert offset == {"cursor": "2024-04-02T00:00:00Z"}


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
