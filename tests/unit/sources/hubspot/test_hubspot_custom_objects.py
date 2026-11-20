"""Custom table aliases must resolve to objectTypeId on every API read path."""

import runpy
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest import mock

import pytest
from pyspark.sql.types import ArrayType, StringType, StructType, _make_type_verifier

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
def test_custom_object_id_used_for_properties_snapshot_and_incremental(generated):
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
        raise AssertionError(path)

    with mock.patch("requests.get", side_effect=get), mock.patch("requests.post", side_effect=post):
        conn = _connector_class(generated)({"access_token": "test-only-token"})
        assert "cars" in conn.list_tables()
        schema = conn.get_table_schema("cars", {})
        assert schema["properties"].dataType.fieldNames() == ["vin"]
        rows, _ = conn.read_table("cars", {}, {})
        assert list(rows)[0]["properties"]["vin"] == "example-vin"
        rows, _ = conn.read_table("cars", {"updatedAt": "2023-01-01T00:00:00.000Z"}, {})
        assert list(rows)[0]["id"] == "1"
    assert paths.count("/crm-object-schemas/2026-09/schemas") == 1
    assert "/crm/properties/2026-09/2-12345" in paths
    assert "/crm/objects/2026-09/2-12345" in paths
    assert "/crm/objects/2026-09/2-12345/search" in paths


@pytest.mark.parametrize("generated", [False, True])
def test_properties_discovery_uses_versioned_wrapper_for_standard_objects(generated):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    properties = [{"name": "dealstage", "type": "enumeration"}]
    with mock.patch("requests.get", return_value=_Response({"results": properties})) as get:
        assert conn._get_object_properties("deals") == properties
    assert get.call_args.args[0] == "https://api.hubapi.com/crm/properties/2026-09/deals"


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
def test_association_options_select_columns_and_typed_values(generated):
    conn = _connector_class(generated)({"access_token": "test-only-token"})
    with mock.patch.object(conn, "list_tables", return_value=["deals"]), mock.patch.object(
        conn, "_get_object_properties", return_value=[]
    ):
        assert not any(
            name in conn.get_table_schema("deals", {"associations": ""}).fieldNames()
            for name in ("contacts", "companies", "tickets")
        )
        ids = conn.get_table_schema(
            "deals", {"associations": "contacts"}
        )["contacts"].dataType
        typed_options = {
            "associations": "contacts",
            "association_value_mode": "typed",
        }
        typed = conn.get_table_schema("deals", typed_options)["contacts"].dataType
        assert isinstance(ids, ArrayType) and isinstance(ids.elementType, StringType)
        assert isinstance(typed, ArrayType) and isinstance(typed.elementType, StructType)
        assert typed.elementType.fieldNames() == ["id", "typeId", "category", "label"]
        assert conn.read_table_metadata("deals", typed_options)[
            "association_value_mode"
        ] == "typed"

        record = {"id": "1", "properties": {}}
        edge = {
            "id": "2",
            "typeId": 7,
            "category": "USER_DEFINED",
            "label": "Primary",
        }
        with mock.patch.object(
            conn._associations, "read", return_value={"1": [edge]}
        ) as read:
            conn._hydrate_associations("deals", [record], ["contacts"], "typed")
        read.assert_called_once_with("deals", "contacts", ["1"], with_types=True)
        row = conn._transform_single_record(
            record, "deals", ["contacts"], "typed"
        )
        _make_type_verifier(conn.get_table_schema("deals", typed_options))(row)
        assert row["contacts"] == [edge]

        with pytest.raises(ValueError, match="Unknown associations"):
            conn.get_table_schema("deals", {"associations": "unknown"})
        with pytest.raises(ValueError, match="association_value_mode"):
            conn.get_table_schema("deals", {"association_value_mode": "unknown"})
