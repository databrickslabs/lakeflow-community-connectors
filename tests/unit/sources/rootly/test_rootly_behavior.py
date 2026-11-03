"""Pagination, failure, and security regressions independent of live credentials."""

import json
import pickle
from email.utils import formatdate
from pathlib import Path

import pytest
import requests
from pyspark.sql.types import VariantVal, _make_type_verifier

from databricks.labs.community_connector.libs.utils import parse_value
from databricks.labs.community_connector.sources.rootly import rootly
from databricks.labs.community_connector.sources.rootly.rootly import RootlyLakeflowConnect


def resource(identifier="1"):
    return {"id": identifier, "type": "incidents", "attributes": {"title": "Synthetic"}}


def page(rows=None, next_page=None, **meta):
    return {
        "data": rows if rows is not None else [resource()],
        "meta": {"next_page": next_page, **meta},
    }


def response(body=None, status=200, headers=None):
    result = requests.Response()
    result.status_code = status
    result.headers.update(headers or {})
    result._content = json.dumps(body).encode()  # pylint: disable=protected-access
    result._content_consumed = True  # pylint: disable=protected-access
    return result


@pytest.fixture
def connector():
    return RootlyLakeflowConnect({"api_token": "synthetic-secret"})


@pytest.fixture
def transport(monkeypatch):
    calls = []
    replies = []
    sleeps = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        assert replies, "Unexpected additional HTTP call"
        reply = replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(rootly.requests, "get", get)
    monkeypatch.setattr(rootly.time, "sleep", sleeps.append)
    return calls, replies, sleeps


def test_numbered_pages_stream_without_losing_rows(connector, transport):
    calls, replies, _ = transport
    replies.extend(
        [
            response(page([resource("1"), resource("2")], next_page=2)),
            response(page([resource("3")], next_page=3)),
            response(page([resource("4")])),
        ]
    )
    records, offset = connector.read_table("services", {"old": "checkpoint"}, {"page_size": "2"})
    assert calls == []  # Lazy, bounded to one response page in memory.
    assert next(records)["id"] == "1"
    assert len(calls) == 1
    assert [row["id"] for row in records] == ["2", "3", "4"]
    assert offset is None
    assert [call[1]["params"]["page[number]"] for call in calls] == ["1", "2", "3"]
    for url, options in calls:
        assert url == "https://api.rootly.com/v1/services"
        assert options["timeout"] == (10, 60)
        assert options["allow_redirects"] is False
        assert options["headers"]["Authorization"] == "Bearer synthetic-secret"


def test_cursor_pages_use_opaque_values_and_terminate(connector, transport):
    calls, replies, _ = transport
    replies.extend(
        [
            response(page([resource("1")], next_page=2, next_cursor="opaque+/=one")),
            response(page([resource("2")], next_cursor="opaque-two")),
            response(page([resource("3")], next_cursor=None)),
        ]
    )
    rows, offset = connector.read_table("incidents", None, {})
    assert [row["id"] for row in rows] == ["1", "2", "3"]
    assert offset is None
    assert calls[1][1]["params"] == {"page[size]": "100", "page[after]": "opaque+/=one"}
    assert "page[number]" not in calls[2][1]["params"]


def test_empty_snapshot(connector, transport):
    _, replies, _ = transport
    replies.append(response(page([])))
    rows, offset = connector.read_table("incidents", None, {})
    assert list(rows) == []
    assert offset is None


@pytest.mark.parametrize(
    "body",
    [
        [],
        {},
        {"data": []},
        {"data": [], "meta": {}},
        {"data": {}, "meta": {"next_page": None}},
        page([{"id": "1", "type": "incidents"}]),
        page([{"id": 1, "type": "incidents", "attributes": {}}]),
        page([{"id": "", "type": "incidents", "attributes": {}}]),
        page([None]),
        page([], next_page=2),
        page(next_page=1),
        page(next_page=True),
        page(next_page="2"),
        page(next_cursor=""),
    ],
)
def test_invalid_response_fails_instead_of_succeeding_partially(connector, transport, body):
    calls, replies, _ = transport
    replies.append(response(body))
    rows, _ = connector.read_table("incidents", None, {})
    with pytest.raises(RuntimeError):
        list(rows)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "second_page",
    [
        page(next_cursor="same"),
        page(next_page=2),
        page(),
    ],
)
def test_cursor_cycles_and_missing_metadata_fail(connector, transport, second_page):
    _, replies, _ = transport
    replies.extend([response(page(next_cursor="same")), response(second_page)])
    rows, _ = connector.read_table("incidents", None, {})
    with pytest.raises(RuntimeError):
        list(rows)


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_status_retry(connector, transport, status):
    calls, replies, sleeps = transport
    replies.extend([response(status=status, headers={"Retry-After": "3"}), response(page())])
    rows, _ = connector.read_table("incidents", None, {})
    assert len(list(rows)) == 1
    assert len(calls) == 2
    assert sleeps == [3]


@pytest.mark.parametrize("status", [301, 401, 403, 404, 422])
def test_other_statuses_are_not_retried_or_leaked(connector, transport, status):
    calls, replies, sleeps = transport
    replies.append(response({"detail": "synthetic-secret private incident"}, status=status))
    rows, _ = connector.read_table("incidents", None, {})
    with pytest.raises(RuntimeError, match=f"HTTP {status}") as error:
        list(rows)
    assert "synthetic-secret" not in str(error.value)
    assert "private incident" not in str(error.value)
    assert len(calls) == 1
    assert sleeps == []


def test_retries_are_bounded(connector, transport):
    calls, replies, sleeps = transport
    replies.extend(response(status=503) for _ in range(5))
    rows, _ = connector.read_table("incidents", None, {})
    with pytest.raises(RuntimeError, match="HTTP 503"):
        list(rows)
    assert len(calls) == 5
    assert sleeps == [1, 2, 4, 8]


@pytest.mark.parametrize("exception", [requests.Timeout, requests.ConnectionError])
def test_network_retries_are_bounded_and_sanitized(connector, transport, exception):
    calls, replies, _ = transport
    replies.extend(exception("synthetic-secret") for _ in range(5))
    rows, _ = connector.read_table("incidents", None, {})
    with pytest.raises(RuntimeError) as error:
        list(rows)
    assert "synthetic-secret" not in str(error.value)
    assert error.value.__suppress_context__
    assert len(calls) == 5


def test_invalid_json_not_retried(connector, transport):
    calls, replies, _ = transport
    bad = response()
    bad._content = b"not JSON"  # pylint: disable=protected-access
    replies.append(bad)
    rows, _ = connector.read_table("incidents", None, {})
    with pytest.raises(RuntimeError, match="invalid JSON"):
        list(rows)
    assert len(calls) == 1


def test_retry_hints(monkeypatch):
    monkeypatch.setattr(rootly.time, "time", lambda: 1000)
    assert rootly._retry_delay({"Retry-After": formatdate(1030, usegmt=True)}, 0) == 30
    assert rootly._retry_delay({"X-RateLimit-Reset": "1040"}, 0) == 40
    assert rootly._retry_delay({"Retry-After": "invalid", "X-RateLimit-Reset": "1005"}, 0) == 5
    assert rootly._retry_delay({"Retry-After": "NaN"}, 2) == 4
    assert rootly._retry_delay({"Retry-After": "-1"}, 0) == 0
    assert rootly._retry_delay({}, 3) == 8
    with pytest.raises(RuntimeError, match="long retry delay"):
        rootly._retry_delay({"Retry-After": "121"}, 0)


@pytest.mark.parametrize("value", ["0", "101", "-1", "bad", "1.5", None])
def test_invalid_page_size(connector, value):
    with pytest.raises(ValueError, match="page_size"):
        connector.read_table("incidents", None, {"page_size": value})


@pytest.mark.parametrize("token", [None, "", " ", "fake\nkey"])
def test_invalid_token(token):
    with pytest.raises(ValueError, match="api_token"):
        RootlyLakeflowConnect({"api_token": token})


def test_discovery_requires_no_requests_and_rejects_paths(connector, transport):
    tables = connector.list_tables()
    assert len(tables) == 7
    assert len(set(tables)) == 7
    for table in tables:
        assert {"id", "type", "attributes"}.issubset(
            connector.get_table_schema(table, {}).fieldNames()
        )
        assert connector.read_table_metadata(table, {}) == {
            "primary_keys": ["id"],
            "ingestion_type": "snapshot",
        }
    for method in (connector.get_table_schema, connector.read_table_metadata):
        with pytest.raises(ValueError, match="Unsupported"):
            method("../users", {})
    with pytest.raises(ValueError, match="Unsupported"):
        connector.read_table("https://other.invalid", None, {})
    assert transport[0] == []


def test_connector_can_be_serialized_for_spark(connector):
    restored = pickle.loads(pickle.dumps(connector))
    assert restored.list_tables() == connector.list_tables()


def test_all_corpus_rows_convert_to_spark_internal_values(connector):
    corpus_dir = (
        Path(__file__).resolve().parents[4]
        / "src/databricks/labs/community_connector/source_simulator/specs/rootly/corpus"
    )
    for table in connector.list_tables():
        schema = connector.get_table_schema(table, {})
        for record in json.loads((corpus_dir / f"{table}.json").read_text()):
            prepared = rootly._prepare_row(record, schema)
            converted = parse_value(prepared, schema)
            _make_type_verifier(schema)(converted)
            assert schema.toInternal(converted) is not None


def test_open_objects_and_empty_structs_remain_spark_compatible(connector, transport):
    _, replies, _ = transport
    labels = {"platform": "osx", "nested": {"attempts": 2}, "list": [1, True]}
    record = resource()
    record["attributes"].update(labels=labels, severity={})
    replies.append(response(page([record])))
    rows, _ = connector.read_table("incidents", None, {})
    prepared = next(rows)
    variant = prepared["attributes"]["labels"]
    assert isinstance(variant, VariantVal)
    assert variant.toPython() == labels
    assert prepared["attributes"]["severity"]
    assert all(value is None for value in prepared["attributes"]["severity"].values())
    schema = connector.get_table_schema("incidents", {})
    converted = parse_value(prepared, schema)
    _make_type_verifier(schema)(converted)
    assert schema.toInternal(converted) is not None
