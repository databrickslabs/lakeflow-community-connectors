from pathlib import Path

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType,
)

from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.elasticsearch.elasticsearch import LakeflowConnect


def test_elasticsearch_connector():
    """Test the Elasticsearch connector using the generic test suite and dev configs."""
    # Inject the LakeflowConnect class into test_suite module's namespace
    test_suite.LakeflowConnect = LakeflowConnect

    # Load connection-level configuration (e.g. endpoint, index, authentication, api_key, verify_ssl)
    base_dir = Path(__file__).parent.parent
    config_path = base_dir / "configs" / "dev_config.json"
    table_config_path = base_dir / "configs" / "dev_table_config.json"

    init_options = load_config(config_path)
    table_configs = load_config(table_config_path)

    # Create tester with the config and per-table options
    tester = LakeflowConnectTester(init_options, table_configs)

    # Run all standard LakeflowConnect tests for this connector
    report = tester.run_all_tests()
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )


class DummyClient:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def get(self, path, params=None):
        self.calls.append(("GET", path))
        return self.responses.get(("GET", path), {})

    def post(self, path, json, params=None):
        self.calls.append(("POST", path))
        return self.responses.get(("POST", path), {})

    def delete(self, path, json=None, params=None):
        self.calls.append(("DELETE", path))
        return self.responses.get(("DELETE", path), {})


class TestConnector(LakeflowConnect):
    """A lightweight test double that bypasses network calls."""

    def __init__(self, client: DummyClient):
        # Bypass base __init__
        self._client = client
        self._default_cursor_fields = ["timestamp", "updated_at"]
        self._indices_cache = ["idx"]
        self._index_properties_cache = {}
        self._metadata_cache = {}
        self._schema_cache = {}
        self._verify_ssl = True

    def _schema_for_index(self, index: str) -> StructType:
        return StructType([StructField("name", StringType(), True)])


class DiscoveryConnector(TestConnector):
    """Test double that forces index discovery instead of using a preset cache."""

    def __init__(self, client: DummyClient):
        super().__init__(client)
        self._indices_cache = []  # Force discovery path


def test_map_elasticsearch_field_to_spark_types():
    conn = TestConnector(DummyClient())
    type_cases = [
        ("ts", {"type": "date_nanos"}, "timestamp"),
        ("ip", {"type": "ip"}, "string"),
        ("flt", {"type": "flattened"}, "map<"),
        ("ver", {"type": "version"}, "string"),
        ("comp", {"type": "completion"}, "string"),
    ]
    for name, field, expected in type_cases:
        sf = conn._map_elasticsearch_field_to_spark(name, field)
        assert sf.name == name
        assert expected in sf.dataType.simpleString()

    # object -> struct
    obj_field = conn._map_elasticsearch_field_to_spark(
        "obj", {"properties": {"a": {"type": "keyword"}}}
    )
    assert isinstance(obj_field.dataType, StructType)

    # nested -> array<struct>
    nested_field = conn._map_elasticsearch_field_to_spark(
        "nested", {"type": "nested", "properties": {"a": {"type": "keyword"}}}
    )
    assert isinstance(nested_field.dataType, ArrayType)
    assert isinstance(nested_field.dataType.elementType, StructType)


def test_properties_to_struct_and_schema_meta_fields():
    conn = TestConnector(DummyClient())
    schema = conn.get_table_schema("idx", {"meta_fields": "true"})
    field_names = [f.name for f in schema.fields]
    assert field_names == ["name", "_id"]


def test_validate_index_rejects_wildcards():
    conn = TestConnector(DummyClient())
    with pytest.raises(ValueError):
        conn.get_table_schema("logs-*", {})


def test_fetch_index_properties_multi_index_same_mapping():
    props = {"mappings": {"properties": {"a": {"type": "keyword"}}}}
    responses = {
        ("GET", "/alias/_mapping"): {"idx-a": props, "idx-b": props},
    }
    conn = TestConnector(DummyClient(responses=responses))
    result = conn._fetch_index_properties("alias")
    assert result == {"a": {"type": "keyword"}}


def test_fetch_index_properties_multi_index_incompatible_mappings():
    props_a = {"mappings": {"properties": {"a": {"type": "keyword"}}}}
    props_b = {"mappings": {"properties": {"a": {"type": "long"}}}}
    responses = {
        ("GET", "/alias/_mapping"): {"idx-a": props_a, "idx-b": props_b},
    }
    conn = TestConnector(DummyClient(responses=responses))
    with pytest.raises(ValueError):
        conn._fetch_index_properties("alias")


def test_build_search_request_meta_flags():
    responses = {("POST", "/idx/_pit"): {"id": "pit-1"}}
    conn = TestConnector(DummyClient(responses=responses))
    pit_id, body = conn._build_search_request(
        index="idx",
        cursor_field="timestamp",
        start_offset=None,
        size=10,
        keep_alive="1m",
    )
    assert "pit" in body and pit_id == body["pit"]["id"]


def test_index_properties_cached_single_fetch():
    responses = {
        ("GET", "/idx/_mapping"): {"idx": {"mappings": {"properties": {"a": {"type": "keyword"}}}}},
    }
    client = DummyClient(responses=responses)
    conn = TestConnector(client)
    first = conn._fetch_index_properties("idx")
    second = conn._fetch_index_properties("idx")
    assert first == {"a": {"type": "keyword"}}
    assert second == first
    assert client.calls.count(("GET", "/idx/_mapping")) == 1


def test_is_field_available_handles_nested_and_multifields():
    conn = TestConnector(DummyClient())
    properties = {
        "name": {"type": "keyword"},
        "obj": {"properties": {"child": {"type": "date"}}},
        "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
    }

    assert conn._is_field_available(properties, "name")
    assert conn._is_field_available(properties, "obj.child")
    assert conn._is_field_available(properties, "title.raw")
    assert not conn._is_field_available(properties, "missing.field")


def test_metadata_for_index_cursor_selection_and_overrides():
    conn = TestConnector(DummyClient())
    conn._index_properties_cache["idx"] = {
        "timestamp": {"type": "date"},
        "name": {"type": "keyword"},
    }

    meta = conn._metadata_for_index("idx", {})
    assert meta["cursor_field"] == "timestamp"
    assert meta["ingestion_type"] == "cdc"

    overridden = conn._metadata_for_index("idx", {"cursor_field": "name"})
    assert overridden["cursor_field"] == "name"

    with pytest.raises(ValueError):
        conn._metadata_for_index("idx", {"cursor_field": "_private"})


def test_metadata_for_index_without_cursor_ignores_append_override():
    conn = TestConnector(DummyClient())
    conn._index_properties_cache["idx"] = {"name": {"type": "keyword"}}

    meta = conn._metadata_for_index("idx", {"ingestion_type": "append"})
    assert meta["ingestion_type"] == "snapshot"
    assert "cursor_field" not in meta


def test_read_index_empty_closes_pit_and_returns_offset():
    responses = {
        ("POST", "/idx/_pit"): {"id": "pit-123"},
        ("POST", "/_search"): {"hits": {"hits": []}},
        ("DELETE", "/_pit"): {},
    }
    client = DummyClient(responses=responses)
    conn = TestConnector(client)
    conn._metadata_cache["idx"] = {"primary_keys": ["_id"], "ingestion_type": "cdc", "cursor_field": "timestamp"}

    iterator, offset = conn.read_table("idx", {}, {})
    assert list(iterator) == []
    assert offset == {}
    assert ("DELETE", "/_pit") in client.calls


def test_read_index_returns_next_offset_with_cursor():
    responses = {
        ("POST", "/idx/_pit"): {"id": "pit-xyz"},
        ("POST", "/_search"): {
            "hits": {"hits": [{"_id": "1", "_source": {"name": "alice"}, "sort": ["2024-01-01T00:00:00Z", 5]}]}
        },
    }
    client = DummyClient(responses=responses)
    conn = TestConnector(client)
    conn._metadata_cache["idx"] = {"primary_keys": ["_id"], "ingestion_type": "cdc", "cursor_field": "timestamp"}

    iterator, offset = conn.read_table("idx", {}, {"cursor_field": "timestamp"})
    assert list(iterator) == [{"name": "alice", "_id": "1"}]
    assert offset["pit_id"] == "pit-xyz"
    assert offset["search_after"] == ["2024-01-01T00:00:00Z", 5]
    assert offset["cursor"] == "2024-01-01T00:00:00Z"
    assert offset["cursor_field"] == "timestamp"


def test_list_tables_discovers_indices_and_caches():
    responses = {
        ("GET", "/_cat/indices"): [{"index": "idx-a"}],
        ("GET", "/_aliases"): {"idx-a": {"aliases": {"alias1": {}}}},
        ("POST", "/idx-a/_search"): {},
        ("POST", "/alias1/_search"): {},
    }
    client = DummyClient(responses=responses)
    conn = DiscoveryConnector(client)

    tables_first = conn.list_tables()
    calls_after_first = list(client.calls)
    tables_second = conn.list_tables()

    assert tables_first == ["alias1", "idx-a"]
    assert tables_second == tables_first
    assert calls_after_first == client.calls  # cache prevents additional network calls


def test_read_table_normalizes_list_struct_fields():
    responses = {
        ("POST", "/idx/_pit"): {"id": "pit-1"},
        ("POST", "/_search"): {
            "hits": {"hits": [{"_id": "1", "_source": {"geo": [{"lat": 1.0, "lon": 2.0}]}}]}
        },
    }
    client = DummyClient(responses=responses)
    conn = TestConnector(client)
    conn._metadata_cache["idx"] = {"primary_keys": ["_id"], "ingestion_type": "snapshot"}
    conn._schema_cache["idx"] = StructType(
        [
            StructField("geo", StructType([StructField("lat", DoubleType()), StructField("lon", DoubleType())])),
            StructField("_id", StringType(), False),
        ]
    )

    iterator, _ = conn.read_table("idx", {}, {})
    rows = list(iterator)
    assert rows == [{"geo": {"lat": 1.0, "lon": 2.0}, "_id": "1"}]


def test_read_table_paginates_and_closes_pit_after_last_page():
    class SequencedClient(DummyClient):
        def __init__(self, responses=None):
            super().__init__(responses=responses)
            self.seq_index = {}

        def post(self, path, json, params=None):
            key = ("POST", path)
            value = self.responses.get(key, {})
            if isinstance(value, list):
                idx = self.seq_index.get(key, 0)
                self.seq_index[key] = idx + 1
                return value[min(idx, len(value) - 1)]
            return super().post(path, json, params)

    responses = {
        ("POST", "/idx/_pit"): {"id": "pit-1"},
        ("POST", "/_search"): [
            {"hits": {"hits": [{"_id": "1", "_source": {"name": "a"}, "sort": ["c1", 1]}]}},
            {"hits": {"hits": []}},
        ],
        ("DELETE", "/_pit"): {},
    }
    client = SequencedClient(responses=responses)
    conn = TestConnector(client)
    conn._metadata_cache["idx"] = {"primary_keys": ["_id"], "ingestion_type": "cdc", "cursor_field": "timestamp"}

    # First page
    iterator1, offset1 = conn.read_table("idx", {}, {"cursor_field": "timestamp"})
    assert list(iterator1) == [{"name": "a", "_id": "1"}]
    assert offset1["pit_id"] == "pit-1"
    assert offset1["search_after"] == ["c1", 1]

    # Second (final) page should close PIT
    iterator2, offset2 = conn.read_table("idx", offset1, {"cursor_field": "timestamp"})
    assert list(iterator2) == []
    assert offset2 == offset1
    assert ("DELETE", "/_pit") in client.calls


def test_discover_indices_skips_inaccessible():
    class ProbeClient(DummyClient):
        def post(self, path, json, params=None):
            if path == "/bad/_search":
                raise RuntimeError("forbidden")
            return super().post(path, json, params)

    responses = {
        ("GET", "/_cat/indices"): [{"index": "good"}, {"index": "bad"}],
        ("GET", "/_aliases"): {},
        ("POST", "/good/_search"): {},
    }
    client = ProbeClient(responses=responses)
    conn = DiscoveryConnector(client)

    tables = conn.list_tables()
    assert tables == ["good"]


def test_cursor_field_override_missing_in_mapping_raises():
    conn = TestConnector(DummyClient())
    conn._index_properties_cache["idx"] = {"name": {"type": "keyword"}}
    with pytest.raises(ValueError):
        conn.read_table_metadata("idx", {"cursor_field": "missing"})


def test_pit_keep_alive_override_used():
    class KeepAliveConnector(TestConnector):
        def __init__(self, client):
            super().__init__(client)
            self.seen_keep_alive = None

        def _open_point_in_time(self, index: str, keep_alive: str) -> str:
            self.seen_keep_alive = keep_alive
            return "pit-keep"

    responses = {
        ("POST", "/_search"): {"hits": {"hits": []}},
        ("DELETE", "/_pit"): {},
    }
    client = DummyClient(responses=responses)
    conn = KeepAliveConnector(client)
    conn._metadata_cache["idx"] = {"primary_keys": ["_id"], "ingestion_type": "cdc"}

    conn.read_table("idx", {}, {"pit_keep_alive": "2m"})
    assert conn.seen_keep_alive == "2m"
