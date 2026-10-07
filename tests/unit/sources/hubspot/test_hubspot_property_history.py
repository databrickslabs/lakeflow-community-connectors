"""Targeted tests for the HubSpot ``{object}_property_history`` tables.

The shared ``LakeflowConnectTests`` suite (test_hubspot_lakeflow_connect.py)
already runs schema / metadata / read / termination / column-coverage checks
on every table, including the history tables. These tests cover behaviour
the generic suite can't see: history flattening, lookback-once-per-trigger,
``max_records_per_batch``, the 10k search cap, and 429 retry.
"""

import json
from pathlib import Path
from unittest import mock

import pytest

from databricks.labs.community_connector import source_simulator as sim_pkg
from databricks.labs.community_connector.source_simulator import MODE_SIMULATE, Simulator
from databricks.labs.community_connector.sources.hubspot import hubspot_property_history as hph
from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect

SPEC_DIR = Path(sim_pkg.__file__).parent / "specs" / "hubspot"
HISTORY_TABLES = [
    "contacts_property_history",
    "companies_property_history",
    "deals_property_history",
    "tickets_property_history",
]


# ----------------------------------------------------------------------
# Simulator-backed tests
# ----------------------------------------------------------------------


@pytest.fixture(scope="module")
def connector():
    with Simulator(
        mode=MODE_SIMULATE,
        spec_path=SPEC_DIR / "endpoints.yaml",
        corpus_dir=SPEC_DIR / "corpus",
    ):
        conn = HubspotLakeflowConnect({"access_token": "simulator-fake-access-token"})
        conn._history._sleep = lambda _s: None  # pylint: disable=protected-access
        yield conn


def test_history_tables_listed(connector):
    tables = connector.list_tables()
    for t in HISTORY_TABLES:
        assert t in tables
    # Existing tables are unchanged.
    assert "contacts" in tables and "notes" in tables


def test_history_metadata(connector):
    for t in HISTORY_TABLES:
        assert connector.read_table_metadata(t, {}) == {
            "primary_keys": ["object_id", "property", "timestamp"],
            "cursor_field": "timestamp",
            "ingestion_type": "cdc",
        }


def test_history_initial_read_flattens_entries(connector):
    rows, offset = connector.read_table("contacts_property_history", {}, {})
    rows = list(rows)
    corpus = json.loads((SPEC_DIR / "corpus" / "contacts_history.json").read_text())
    expected = sum(len(r["propertiesWithHistory"]["name"]) for r in corpus)
    assert len(rows) == expected
    assert offset == {"updatedAt": "2024-02-04T00:00:00Z"}
    first = next(r for r in rows if r["object_id"] == "1000" and r["source_type"] == "CRM_UI")
    assert first == {
        "object_id": "1000",
        "property": "name",
        "value": "Sample 0 v2",
        "timestamp": "2024-02-01T00:00:00Z",
        "source_type": "CRM_UI",
        "source_id": "userId:5001",
        "source_label": "Jane Admin",
        "updated_by_user_id": 5001,
    }
    # Keys are unique on the declared primary key.
    keys = {(r["object_id"], r["property"], r["timestamp"]) for r in rows}
    assert len(keys) == len(rows)


def test_history_since_filters_old_entries(connector):
    rows, _ = connector.read_table(
        "deals_property_history", {}, {"history_since": "2024-01-15T00:00:00Z"}
    )
    rows = list(rows)
    assert rows and all(r["timestamp"] >= "2024-01-15" for r in rows)


def test_history_properties_option_limits_request(connector):
    rows, _ = connector.read_table(
        "tickets_property_history", {}, {"history_properties": "not_a_property"}
    )
    assert list(rows) == []


# ----------------------------------------------------------------------
# Unit tests with a fake HTTP layer
# ----------------------------------------------------------------------


class _Resp:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


def _parent(i, ts):
    return {"id": str(i), "updatedAt": ts}


def _history_result(i, ts):
    return {
        "id": str(i),
        "propertiesWithHistory": {
            "email": [{"value": f"v{i}", "timestamp": ts, "sourceType": "API"}]
        },
    }


def _reader(sleeps=None):
    return hph.HubspotPropertyHistory(
        base_url="https://api.hubapi.com",
        auth_header={},
        init_ts="2030-01-01T00:00:00.000Z",
        get_property_names=lambda _obj: ["email"],
        get_cursor_property=lambda _obj: "lastmodifieddate",
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)),
    )


class _FakeHubspot:
    """Serves search pages (in ascending updatedAt order) and batch reads."""

    def __init__(self, parents, page_size=hph.SEARCH_PAGE_SIZE):
        self.parents = parents
        self.page_size = page_size
        self.search_bodies = []
        self.batch_bodies = []

    def post(self, url, headers=None, json=None, timeout=None):  # pylint: disable=redefined-outer-name
        assert timeout
        if url.endswith("/search"):
            self.search_bodies.append(json)
            lo = None
            for f in (json.get("filterGroups") or [{}])[0].get("filters", []):
                if f["operator"] == "GTE":
                    lo = int(f["value"])
            matching = [
                p for p in self.parents
                if lo is None or hph._to_ms(hph._parse_ts(p["updatedAt"])) >= lo  # pylint: disable=protected-access
            ]
            start = int(json.get("after") or 0)
            page = matching[start : start + self.page_size]
            nxt = start + len(page)
            paging = {"next": {"after": str(nxt)}} if nxt < len(matching) else {}
            return _Resp(200, {"results": page, "paging": paging})
        self.batch_bodies.append(json)
        by_id = {p["id"]: p for p in self.parents}
        results = [
            _history_result(i["id"], by_id[i["id"]]["updatedAt"]) for i in json["inputs"]
        ]
        return _Resp(200, {"results": results})


def test_429_retries_with_retry_after():
    sleeps = []
    reader = _reader(sleeps)
    responses = [
        _Resp(429, {"message": "rate limited"}, {"Retry-After": "2"}),
        _Resp(429, {"message": "rate limited"}),
        _Resp(200, {"results": []}),
    ]
    with mock.patch.object(hph.requests, "post", side_effect=responses) as post:
        rows, offset = reader.read_table("contacts_property_history", {}, {})
    assert list(rows) == [] and offset == {}
    assert post.call_count == 3
    assert 2.0 in sleeps  # honoured Retry-After
    assert 2.0 ** 1 in sleeps  # exponential fallback on 2nd attempt


def test_429_gives_up_after_max_retries():
    reader = _reader()
    with mock.patch.object(hph.requests, "post", return_value=_Resp(429)):
        with pytest.raises(RuntimeError, match="429"):
            reader.read_table("contacts_property_history", {}, {})


def test_non_429_error_is_raised_immediately():
    reader = _reader()
    with mock.patch.object(hph.requests, "post", return_value=_Resp(500)) as post:
        with pytest.raises(RuntimeError, match="500"):
            reader.read_table("contacts_property_history", {}, {})
    assert post.call_count == 1


def test_lookback_applied_once_per_trigger():
    fake = _FakeHubspot([_parent(1, "2024-03-01T00:00:00.000Z")])
    reader = _reader()
    start = {"updatedAt": "2024-03-01T00:00:00.000Z"}
    opts = {"history_lookback_minutes": "60"}
    with mock.patch.object(hph.requests, "post", side_effect=fake.post):
        reader.read_table("contacts_property_history", start, opts)
        reader.read_table("contacts_property_history", start, opts)

    def gte(body):
        filters = body["filterGroups"][0]["filters"]
        return next(f for f in filters if f["operator"] == "GTE")["value"]

    watermark_ms = hph._to_ms(hph._parse_ts(start["updatedAt"]))  # pylint: disable=protected-access
    assert int(gte(fake.search_bodies[0])) == watermark_ms - 60 * 60 * 1000
    assert int(gte(fake.search_bodies[1])) == watermark_ms
    # Upper bound is always the connector's init time.
    lte = [f for f in fake.search_bodies[0]["filterGroups"][0]["filters"] if f["operator"] == "LTE"]
    assert lte and int(lte[0]["value"]) == hph._to_ms(hph._parse_ts("2030-01-01T00:00:00.000Z"))  # pylint: disable=protected-access


def test_max_records_per_batch_and_convergence():
    parents = [_parent(i, f"2024-03-{1 + i // 50:02d}T00:00:00.000Z") for i in range(200)]
    fake = _FakeHubspot(parents)
    reader = _reader()
    opts = {"max_records_per_batch": "60", "history_lookback_minutes": "0"}
    seen = set()
    offset = {}
    calls = 0
    with mock.patch.object(hph.requests, "post", side_effect=fake.post):
        while True:
            rows, new_offset = reader.read_table("contacts_property_history", offset, opts)
            rows = list(rows)
            calls += 1
            # Cap is enforced at batch-read boundaries (50 IDs per call).
            assert len(rows) <= 100
            seen.update(r["object_id"] for r in rows)
            if new_offset == offset:
                break
            offset = new_offset
            assert calls < 20
    assert seen == {str(i) for i in range(200)}
    assert offset == {"updatedAt": "2024-03-04T00:00:00.000Z"}
    assert all(len(b["inputs"]) <= hph.BATCH_READ_SIZE for b in fake.batch_bodies)


def test_search_result_cap_reanchors(monkeypatch):
    monkeypatch.setattr(hph, "SEARCH_RESULT_CAP", 10)
    parents = [_parent(i, f"2024-03-01T00:00:{i:02d}.000Z") for i in range(25)]
    fake = _FakeHubspot(parents, page_size=4)
    monkeypatch.setattr(hph, "SEARCH_PAGE_SIZE", 4)
    reader = _reader()
    with mock.patch.object(hph.requests, "post", side_effect=fake.post):
        rows, offset = reader.read_table("contacts_property_history", {}, {})
    rows = list(rows)
    # Stopped before paging past the cap and checkpointed the last updatedAt.
    assert max(int(b.get("after") or 0) for b in fake.search_bodies) + 4 <= 10
    assert offset == {"updatedAt": parents[len(rows) - 1]["updatedAt"]}


def test_short_circuit_at_init_time():
    reader = _reader()
    start = {"updatedAt": "2030-01-01T00:00:00.000Z"}
    with mock.patch.object(hph.requests, "post") as post:
        rows, offset = reader.read_table("contacts_property_history", start, {})
    assert list(rows) == [] and offset == start
    post.assert_not_called()
