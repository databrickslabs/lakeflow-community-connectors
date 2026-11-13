"""Custom table aliases must resolve to objectTypeId on every API read path."""

import runpy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest import mock

import pytest

from databricks.labs.community_connector.sources.hubspot import hubspot as hubspot_module
from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect


class _Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def _empty_associations(url, json, **_kwargs):
    assert "/crm/associations/2026-09/" in url
    return _Response({"status": "COMPLETE", "results": [
        {"from": {"id": item["id"]}, "to": []} for item in json["inputs"]
    ]})


def _connector_class(generated):
    if not generated:
        return HubspotLakeflowConnect
    path = Path(hubspot_module.__file__).with_name("_generated_hubspot_python_source.py")
    registry = mock.Mock()
    runpy.run_path(str(path))["register_lakeflow_source"](
        SimpleNamespace(dataSource=registry)
    )
    registry.register.assert_called_once()
    source_class = registry.register.call_args.args[0]
    return lambda options: source_class(options).lakeflow_connect


@pytest.mark.parametrize("generated", [False, True])
def test_custom_object_id_used_for_properties_snapshot_incremental_and_history(generated):
    paths = []
    stamp = "2024-03-01T00:00:00.000Z"
    parent = {"id": "1", "updatedAt": stamp, "properties": {"vin": "example-vin"}}

    def get(url, **_kwargs):
        path = url.split("api.hubapi.com", 1)[1].split("?", 1)[0]
        paths.append(path)
        if path == "/crm-object-schemas/2026-09/schemas":
            return _Response({"results": [{"name": "Cars", "objectTypeId": "2-12345"}]})
        if path == "/crm/properties/2026-09/2-12345":
            return _Response({"results": [{"name": "vin", "type": "string"}]})
        assert path == "/crm/objects/2026-09/2-12345"
        return _Response({"results": [parent]})

    def post(url, **_kwargs):
        path = url.split("api.hubapi.com", 1)[1].split("?", 1)[0]
        paths.append(path)
        if path == "/crm/objects/2026-09/2-12345/search":
            return _Response({"results": [parent]})
        assert path == "/crm/objects/2026-09/2-12345/batch/read"
        return _Response({"status": "COMPLETE", "results": [{
            "id": "1",
            "propertiesWithHistory": {"vin": [{"value": "example-vin", "timestamp": stamp}]},
        }]})

    with mock.patch("requests.get", side_effect=get), mock.patch("requests.post", side_effect=post):
        conn = _connector_class(generated)({"access_token": "test-only-token"})
        conn._history._sleep = lambda _: None
        assert "cars" in conn.list_tables()
        assert "cars_property_history" in conn.list_tables()
        schema = conn.get_table_schema("cars", {})
        assert schema["properties"].dataType.fieldNames() == ["vin", "hs_lastmodifieddate"]
        rows, _ = conn.read_table("cars", {}, {})
        assert list(rows)[0]["properties"]["vin"] == "example-vin"
        rows, _ = conn.read_table("cars", {"updatedAt": "2023-01-01T00:00:00.000Z"}, {})
        assert list(rows)[0]["id"] == "1"
        rows, offset = conn.read_table("cars_property_history", {}, {})
        assert [(r["object_id"], r["property"], r["value"]) for r in rows] == [
            ("1", "vin", "example-vin"),
        ]
        assert offset["updatedAt"] == conn._history._init_ts
        assert offset["history_scan"]["complete"] is True
    assert paths.count("/crm-object-schemas/2026-09/schemas") == 1
    assert "/crm/properties/2026-09/2-12345" in paths
    assert "/crm/objects/2026-09/2-12345" in paths
    assert "/crm/objects/2026-09/2-12345/search" in paths
    assert "/crm/objects/2026-09/2-12345/batch/read" in paths


@pytest.mark.parametrize("generated", [False, True])
def test_properties_discovery_uses_versioned_wrapper_for_standard_objects(generated):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    properties = [{"name": "dealstage", "type": "enumeration"}]
    with mock.patch("requests.get", return_value=_Response({"results": properties})) as get:
        assert conn._get_object_properties("deals") == properties
    assert get.call_args.args[0] == "https://api.hubapi.com/crm/properties/2026-09/deals"


@pytest.mark.parametrize("generated", [False, True])
def test_wide_object_filters_properties_before_building_snapshot_request(generated):
    """A 1,200-property schema must not leak into the filtered request URL."""
    names = [f"wide_custom_property_{i:04d}" for i in range(1200)]
    properties = [{"name": name, "type": "string"} for name in names]
    selected = names[1]
    expected = {selected, "hs_lastmodifieddate"}
    object_requests = []

    def get(url, **_kwargs):
        path = urlparse(url).path
        if path == "/crm-object-schemas/2026-09/schemas":
            return _Response({"results": []})
        if path == "/crm/properties/2026-09/deals":
            return _Response({"results": properties})
        assert path == "/crm/objects/2026-09/deals"
        object_requests.append(url)
        assert len(url.encode("utf-8")) < 8192
        requested = set(parse_qs(urlparse(url).query)["properties"][0].split(","))
        assert requested == expected
        return _Response({"results": [{
            "id": "1",
            "updatedAt": "2024-03-01T00:00:00.000Z",
            "properties": {selected: "example-value"},
        }]})

    conn = _connector_class(generated)({"access_token": "test-only-token"})
    opts = {"include_properties": f"{selected},{names[2]}", "exclude_properties": names[2]}
    with mock.patch("requests.get", side_effect=get), mock.patch("requests.post", side_effect=_empty_associations):
        schema = conn.get_table_schema("deals", opts)
        assert set(schema["properties"].dataType.fieldNames()) == expected
        rows, _ = conn.read_table("deals", {}, opts)
        assert list(rows)[0]["properties"] == {selected: "example-value"}
    assert len(object_requests) == 1
    assert len(",".join(names).encode("utf-8")) > 8192


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("boundary_page", [False, True])
def test_base_incremental_paging_preserves_all_tied_parents(generated, boundary_page):
    checkpoint = "2023-01-01T00:00:00.000Z"
    stamp = "2024-03-01T00:00:00.000Z"
    parents = [{"id": str(i), "updatedAt": stamp, "properties": {}} for i in range(150)]
    if boundary_page:
        parents = [{"id": f"old-{i}", "updatedAt": checkpoint} for i in range(40)] + parents

    def post(_url, json, **_kwargs):
        if "/crm/associations/" in _url:
            return _empty_associations(_url, json)
        lower = int(json["filterGroups"][0]["filters"][0]["value"])
        matching = [p for p in parents if int(
            datetime.fromisoformat(p["updatedAt"].replace("Z", "+00:00")).timestamp() * 1000
        ) >= lower]
        start = int(json.get("after") or 0)
        page = matching[start:start + 40]
        end = start + len(page)
        paging = {"next": {"after": str(end)}} if end < len(matching) else {}
        return _Response({"results": page, "paging": paging})

    conn = _connector_class(generated)({"access_token": "test-only-token"})
    opts = {"max_records_per_batch": "60"}
    with mock.patch("requests.get", return_value=_Response({"results": []})), \
            mock.patch("requests.post", side_effect=post), mock.patch("time.sleep"):
        rows, offset = conn.read_table("deals", {"updatedAt": checkpoint}, opts)
        assert {r["id"] for r in rows} == {str(i) for i in range(150)}
        assert offset == {"updatedAt": stamp}
        remaining, final_offset = conn.read_table("deals", offset, opts)
        assert list(remaining) == []
        assert final_offset == offset


@pytest.mark.parametrize("generated", [False, True])
def test_custom_association_definitions_are_order_independent(generated):
    schemas = [
        {"name": "Garages", "objectTypeId": "2-200", "associations": [
            {"fromObjectTypeId": "2-200", "toObjectTypeId": "2-100"},
        ]},
        {"name": "Cars", "objectTypeId": "2-100", "associations": [
            {"fromObjectTypeId": "2-100", "toObjectTypeId": "2-200"},
            {"fromObjectTypeId": "2-100", "toObjectTypeId": "2-200"},
            {"fromObjectTypeId": "2-100", "toObjectTypeId": "0-1"},
            {"fromObjectTypeId": "0-1", "toObjectTypeId": "2-100"},
            {"fromObjectTypeId": "2-100", "toObjectTypeId": "2-100"},
        ]},
    ]

    def get(url, **_kwargs):
        return _Response({"results": schemas if url.endswith("/schemas") else []})

    conn = _connector_class(generated)({"access_token": "test-only-token"})
    with mock.patch("requests.get", side_effect=get):
        assert "cars" in conn.list_tables()
        assert conn.read_table_metadata("cars", {})["associations"] == ["cars", "contacts", "garages"]
        assert conn.read_table_metadata("garages", {})["associations"] == ["cars"]
        assert conn.read_table_metadata("contacts", {})["associations"] == ["companies", "cars"]
        schema = conn.get_table_schema("cars", {})
        assert all(name in schema.fieldNames() for name in ("cars", "contacts", "garages"))


@pytest.mark.parametrize("schemas", [
    [{"name": "Cars"}],
    [{"name": "Cars", "objectTypeId": "2-100"}, {"name": "cars", "objectTypeId": "2-200"}],
    [{"name": "contacts", "objectTypeId": "2-100"}],
    [{"name": "Cars", "objectTypeId": "2-100", "associations": [
        {"fromObjectTypeId": "2-100", "toObjectTypeId": "2-missing"},
    ]}],
])
def test_invalid_custom_schema_fails_instead_of_dropping_associations(schemas):
    conn = HubspotLakeflowConnect({"access_token": "test-only-token"})
    with mock.patch("requests.get", return_value=_Response({"results": schemas})):
        with pytest.raises(RuntimeError):
            conn.list_tables()
    assert conn._custom_objects is None


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("incremental", [False, True])
@pytest.mark.parametrize("table", ["cars", "garages"])
def test_custom_associations_are_complete_in_snapshot_and_incremental(generated, incremental, table):
    schemas = [
        {"name": "Cars", "objectTypeId": "2-100", "associations": [
            {"fromObjectTypeId": "2-100", "toObjectTypeId": "2-200"},
            {"fromObjectTypeId": "2-200", "toObjectTypeId": "2-100"},
        ]},
        {"name": "Garages", "objectTypeId": "2-200", "associations": []},
    ]
    stamp = "2024-03-01T00:00:00.000Z"
    types = {"cars": "2-100", "garages": "2-200"}
    parents = {
        "2-100": [{"id": i, "updatedAt": stamp, "properties": {}} for i in ("1", "2")],
        "2-200": [{"id": i, "updatedAt": stamp, "properties": {}} for i in ("10", "11")],
    }
    association_calls = []

    def get(url, **_kwargs):
        path = urlparse(url).path
        if path == "/crm-object-schemas/2026-09/schemas":
            return _Response({"results": schemas})
        if path.startswith("/crm/properties/2026-09/"):
            return _Response({"results": []})
        assert path == f"/crm/objects/2026-09/{types[table]}"
        assert "associations" not in parse_qs(urlparse(url).query)
        return _Response({"results": parents[types[table]]})

    def post(url, json, **_kwargs):
        if url.endswith("/search"):
            assert url.endswith(f"/{types[table]}/search")
            return _Response({"results": parents[types[table]]})
        association_calls.append((url, json))
        assert url.endswith(f"/{types[table]}/{types['garages' if table == 'cars' else 'cars']}/batch/read")
        results = []
        for item in json["inputs"]:
            if table == "cars" and item["id"] == "1":
                result = {"from": {"id": "1"}, "to": [{"toObjectId": "10"}]}
                if "after" not in item:
                    result["paging"] = {"next": {"after": "next-parent-1"}}
                else:
                    assert item["after"] == "next-parent-1"
                    result["to"].append({"toObjectId": "11"})
            else:
                result = {"from": {"id": item["id"]}, "to": (
                    [{"toObjectId": "1"}] if table == "garages" else []
                )}
            results.append(result)
        return _Response({"status": "COMPLETE", "results": results[::-1]})

    conn = _connector_class(generated)({"access_token": "test-only-token"})
    conn._history._sleep = lambda _: None
    start = {"updatedAt": "2023-01-01T00:00:00.000Z"} if incremental else {}
    with mock.patch("requests.get", side_effect=get), mock.patch("requests.post", side_effect=post):
        schema = conn.get_table_schema(table, {})
        target = "garages" if table == "cars" else "cars"
        assert target in schema.fieldNames()
        rows, offset = conn.read_table(table, start, {})
        assert {r["id"]: r[target] for r in rows} == (
            {"1": ["10", "11"], "2": []} if table == "cars" else {"10": ["1"], "11": ["1"]}
        )
        assert offset == {"updatedAt": stamp}
    assert len(association_calls) == (2 if table == "cars" else 1)
    if table == "cars":
        assert association_calls[1][1]["inputs"] == [{"id": "1", "after": "next-parent-1"}]


@pytest.mark.parametrize("generated", [False, True])
def test_incomplete_associations_never_return_a_new_checkpoint(generated):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    conn._history._sleep = lambda _: None
    conn._associations._sleep = lambda _: None
    stamp = "2024-03-01T00:00:00.000Z"
    start = {"updatedAt": "2023-01-01T00:00:00.000Z"}

    def post(url, json, **_kwargs):
        if url.endswith("/search"):
            return _Response({"results": [{"id": "1", "updatedAt": stamp}]})
        return _Response({"status": "COMPLETE", "results": []})

    with mock.patch("requests.get", return_value=_Response({"results": []})), \
            mock.patch("requests.post", side_effect=post):
        with pytest.raises(RuntimeError, match="Incomplete HubSpot associations"):
            conn.read_table("deals", start, {})
    assert start == {"updatedAt": "2023-01-01T00:00:00.000Z"}


@pytest.mark.parametrize("generated", [False, True])
def test_full_refresh_mode_reconciles_association_removal_without_parent_update(generated):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    schemas = [
        {"name": "Cars", "objectTypeId": "2-100", "associations": [
            {"fromObjectTypeId": "2-100", "toObjectTypeId": "2-200"},
        ]},
        {"name": "Garages", "objectTypeId": "2-200", "associations": []},
    ]
    stamp = "2024-03-01T00:00:00.000Z"

    def get(url, **_kwargs):
        if url.endswith("/schemas"):
            return _Response({"results": schemas})
        if "/properties/" in url:
            return _Response({"results": []})
        assert urlparse(url).path == "/crm/objects/2026-09/2-100"
        return _Response({"results": [{"id": "1", "updatedAt": stamp, "properties": {}}]})

    opts = {"association_sync_mode": "full_refresh"}
    with mock.patch("requests.get", side_effect=get), mock.patch.object(
        conn._associations, "read", side_effect=[{"1": ["10"]}, {"1": []}]
    ) as associations:
        assert conn.read_table_metadata("cars", opts)["ingestion_type"] == "snapshot"
        first, offset = conn.read_table("cars", {}, opts)
        second, next_offset = conn.read_table("cars", offset, opts)
        assert list(first)[0]["garages"] == ["10"]
        assert list(second)[0]["garages"] == []
        assert offset == next_offset == {"updatedAt": stamp}
    assert associations.call_args_list == [mock.call("2-100", "2-200", ["1"])] * 2


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("first_snapshot", [False, True])
def test_metadata_cache_respects_association_sync_mode(generated, first_snapshot):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    snapshot = {"association_sync_mode": "full_refresh"}
    order = [snapshot, {}] if first_snapshot else [{}, snapshot]
    with mock.patch("requests.get", return_value=_Response({"results": []})):
        for options in order + order:
            expected = "snapshot" if options else "cdc_with_deletes"
            assert conn.read_table_metadata("deals", options)["ingestion_type"] == expected


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("status", ["PENDING", "PROCESSING", "CANCELED"])
def test_noncomplete_history_batch_never_returns_a_checkpoint(generated, status):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    history = conn._history
    history._sleep = lambda _: None
    history._get_property_names = lambda _: ["email"]
    stamp = "2024-03-01T00:00:00.000Z"
    start = {"updatedAt": "2023-01-01T00:00:00.000Z"}
    with mock.patch.object(history, "_search_changed", return_value=([
        {"id": "1", "updatedAt": stamp},
    ], None)), mock.patch.object(history, "_post", return_value={
        "status": status, "results": [{"id": "1", "propertiesWithHistory": {
            "email": [{"timestamp": stamp, "value": "example"}],
        }}],
    }):
        with pytest.raises(RuntimeError, match="incomplete"):
            history.read_table("contacts_property_history", start, {})
    assert start == {"updatedAt": "2023-01-01T00:00:00.000Z"}


@pytest.mark.parametrize("generated", [False, True])
def test_pipeline_snapshot_requires_complete_partition_discovery(generated):
    conn = _connector_class(generated)({"access_token": "offline"})
    conn._non_crm._sleep = lambda _: None

    def get(url, **_kwargs):
        if url.endswith("/schemas"):
            return mock.Mock(status_code=403)
        assert "/crm/pipelines/2026-09/" in url
        return _Response({"results": [{"id": "p", "stages": []}]})

    with mock.patch("requests.get", side_effect=get):
        with pytest.raises(RuntimeError, match="schema discovery.*403"):
            conn.read_table("pipelines", None, {})
        rows, offset = conn.read_table(
            "pipelines", None, {"pipeline_object_types": "0-3"}
        )
        assert [r["objectTypeId"] for r in rows] == ["0-3"]
        assert offset == {}


@pytest.mark.parametrize("generated", [False, True])
def test_unknown_association_object_types_are_skipped_not_fatal(generated):
    """HubSpot auto-associates a new custom object with internal object types the
    connector does not model (0-18 communication, 0-4 engagement, ...). Those
    edges have no destination table, so discovery must skip them rather than
    fail every table in the pipeline.
    """
    schemas = {"results": [{
        "name": "Cars",
        "objectTypeId": "2-12345",
        "associations": [
            {"fromObjectTypeId": "2-12345", "toObjectTypeId": "0-1"},
            {"fromObjectTypeId": "0-1", "toObjectTypeId": "2-12345"},
            {"fromObjectTypeId": "2-12345", "toObjectTypeId": "0-2"},
            {"fromObjectTypeId": "2-12345", "toObjectTypeId": "0-18"},
            {"fromObjectTypeId": "0-4", "toObjectTypeId": "2-12345"},
            {"fromObjectTypeId": "2-12345", "toObjectTypeId": "0-116"},
        ],
    }]}

    def get(url, **_kwargs):
        assert urlparse(url).path == "/crm-object-schemas/2026-09/schemas"
        return _Response(schemas)

    conn = _connector_class(generated)({"access_token": "test-only-token"})
    with mock.patch("requests.get", side_effect=get):
        tables = conn.list_tables()
    assert "cars" in tables
    associations = conn._custom_object_configs["cars"]["associations"]
    assert "contacts" in associations
    assert "companies" in associations
    # None of the unmapped internal types leaked an association column in.
    assert not any(name.startswith(("0-", "2-")) for name in associations)


@pytest.mark.parametrize("generated", [False, True])
def test_custom_object_archived_records_are_emitted_as_deletes(generated):
    """Custom objects do support archived listing.

    Without a delete flow an archived custom record never leaves the
    destination and keeps looking active, so a discovered custom object must
    opt into deletes and surface archived records through read_table_deletes.
    """
    archived_at = "2024-03-02T00:00:00Z"
    seen_queries = []

    def get(url, **_kwargs):
        path = urlparse(url).path
        query = parse_qs(urlparse(url).query)
        if path == "/crm-object-schemas/2026-09/schemas":
            return _Response({"results": [{"name": "Cars", "objectTypeId": "2-12345"}]})
        if path == "/crm/properties/2026-09/2-12345":
            return _Response({"results": [{"name": "vin", "type": "string"}]})
        assert path == "/crm/objects/2026-09/2-12345"
        seen_queries.append(query)
        if query.get("archived") == ["true"]:
            return _Response({"results": [{
                "id": "9",
                "archived": True,
                "archivedAt": archived_at,
                "properties": {"vin": "archived-vin"},
            }]})
        return _Response({"results": []})

    conn = _connector_class(generated)({"access_token": "test-only-token"})
    conn._init_ts = "2024-03-04T00:00:00.000Z"
    with mock.patch("requests.get", side_effect=get):
        assert conn.read_table_metadata("cars", {})["ingestion_type"] == "cdc_with_deletes"
        rows, offset = conn.read_table_deletes(
            "cars", {"updatedAt": "2024-02-01T00:00:00.000Z"}, {}
        )
    rows = list(rows)
    assert [row["id"] for row in rows] == ["9"]
    assert rows[0]["archived"] is True
    assert rows[0]["updatedAt"] == "2024-03-02T00:00:00.000Z"
    assert offset == {"updatedAt": conn._init_ts}
    assert any(query.get("archived") == ["true"] for query in seen_queries)
