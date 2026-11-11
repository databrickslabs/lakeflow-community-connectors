"""Incident checkpoints must survive page boundaries, retries and later runs."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from databricks.labs.community_connector.source_simulator.specs.rootly.handlers import (
    list_resources,
)
from databricks.labs.community_connector.sources.rootly.rootly import RootlyLakeflowConnect
from tests.unit.sources.rootly.test_rootly_behavior import (  # noqa: F401
    connector as connector_fixture,
)
from tests.unit.sources.rootly.test_rootly_behavior import (
    page,
    response,
)
from tests.unit.sources.rootly.test_rootly_behavior import (
    transport as transport_fixture,
)

connector = connector_fixture
transport = transport_fixture

CUTOFF = "2026-11-11T12:00:00+00:00"
EARLIER = "2026-11-11T11:00:00Z"


def incident(identifier, updated=EARLIER):
    return {"id": identifier, "type": "incidents", "attributes": {"updated_at": updated}}


def read(connector, offset=None, size="2"):
    rows, end = connector.read_table("incidents", offset, {"page_size": size})
    return list(rows), end


def test_equal_timestamps_cross_pages_and_resume_on_new_instance(connector, transport):
    calls, replies, _ = transport
    connector._init_time = CUTOFF
    replies.extend(
        [
            response(page([incident("a"), incident("b")], next_cursor="opaque+/=one")),
            response(page([incident("c"), incident("d")], next_cursor="two")),
            response(page([incident("e")], next_cursor=None)),
            response(page([], next_cursor=None)),
        ]
    )
    first, offset = read(connector)
    assert len(calls) == 1  # One page per bounded, checkpointable batch.
    saved = json.loads(json.dumps(offset))
    resumed = RootlyLakeflowConnect({"api_token": "synthetic-secret"})
    resumed._init_time = "2026-11-11T13:00:00+00:00"
    second, offset = read(resumed, saved, "1000")
    third, offset = read(resumed, offset, "1000")
    assert [row["id"] for row in first + second + third] == list("abcde")
    assert offset == {"updated_through": CUTOFF}
    assert saved == {
        "updated_through": None,
        "window_end": CUTOFF,
        "after": "opaque+/=one",
        "page_size": 2,
    }
    for _, options in calls[:3]:
        assert options["params"]["page[size]"] == "2"
        assert options["params"]["sort"] == "updated_at"
        assert options["params"]["filter[updated_at][lt]"] == CUTOFF
        assert "page[number]" not in options["params"]
    assert calls[1][1]["params"]["page[after]"] == "opaque+/=one"
    # Finishing an interrupted old window also catches up to this run's cutoff.
    rows, end = read(resumed, offset, "1000")
    assert rows == []
    assert calls[-1][1]["params"]["filter[updated_at][gte]"] == CUTOFF
    assert calls[-1][1]["params"]["page[size]"] == "1000"
    assert read(resumed, end) == ([], end)
    assert len(calls) == 4


def test_half_open_windows_include_exact_boundary_and_updates(connector, monkeypatch):
    """Exercise real query construction and the simulator across two runs."""
    import requests

    from databricks.labs.community_connector.sources.rootly import rootly

    records = [incident("old"), incident("boundary", CUTOFF)]
    corpus = SimpleNamespace(get=lambda _: deepcopy(records))
    calls = []

    def get(url, **kwargs):
        prepared = requests.Request("GET", url, params=kwargs["params"]).prepare()
        calls.append(prepared)
        return list_resources(prepared, SimpleNamespace(corpus="incidents"), corpus)

    monkeypatch.setattr(rootly.requests, "get", get)
    connector._init_time = CUTOFF
    rows, offset = read(connector, size="1")
    assert [r["id"] for r in rows] == ["old"]
    assert read(connector, offset) == ([], offset)
    records[0]["attributes"]["updated_at"] = "2026-11-11T12:00:30Z"
    newer = RootlyLakeflowConnect({"api_token": "synthetic-secret"})
    newer._init_time = "2026-11-11T12:01:00+00:00"
    rows, offset = read(newer, offset, "1")
    more, offset = read(newer, offset, "1")
    assert [r["id"] for r in rows + more] == ["boundary", "old"]
    assert read(newer, offset) == ([], offset)
    assert len(calls) == 3


def test_empty_initial_run_checkpoints_and_stops(connector, transport):
    calls, replies, _ = transport
    connector._init_time = CUTOFF
    replies.append(response(page([], next_cursor=None)))
    assert read(connector) == ([], {"updated_through": CUTOFF})
    assert read(connector, {"updated_through": CUTOFF}) == ([], {"updated_through": CUTOFF})
    assert len(calls) == 1


def test_failed_page_retries_same_window_without_mutating_checkpoint(connector, transport):
    calls, replies, _ = transport
    offset = {"updated_through": EARLIER, "window_end": CUTOFF, "after": "token", "page_size": 2}
    original = deepcopy(offset)
    replies.extend([response(status=503)] * 5)
    with pytest.raises(RuntimeError, match="HTTP 503"):
        read(connector, offset)
    assert offset == original
    assert all(call[1]["params"] == calls[0][1]["params"] for call in calls)
    replies.append(response(page([incident("a")], next_cursor=None)))
    rows, end = read(connector, offset)
    assert rows[0]["id"] == "a"
    assert end == {"updated_through": CUTOFF}
    assert calls[-1][1]["params"] == calls[0][1]["params"]


@pytest.mark.parametrize(
    "body",
    [
        page([incident("a")]),  # Missing cursor metadata, never silently finish.
        page([incident("a")], next_cursor=""),
        page([incident("a")], next_cursor=2),
        page([], next_cursor="more"),
        page([incident("a")], next_page=2, next_cursor=None),
        page([incident("a", CUTOFF)], next_cursor=None),
        page([incident("a", "2026-11-11T11:30:00Z"), incident("b")], next_cursor=None),
    ],
)
def test_bad_page_never_advances_checkpoint(connector, transport, body):
    connector._init_time = CUTOFF
    transport[1].append(response(body))
    with pytest.raises(RuntimeError):
        read(connector)


@pytest.mark.parametrize("value", [None, "bad", "2026-11-11T10:00:00"])
def test_missing_or_ambiguous_updated_at_fails(connector, transport, value):
    transport[1].append(response(page([incident("a", value)], next_cursor=None)))
    with pytest.raises(ValueError, match="timezone-aware"):
        read(connector)


def test_repeated_cursor_fails(connector, transport):
    transport[1].append(response(page([incident("a")], next_cursor="same")))
    offset = {"window_end": CUTOFF, "after": "same", "page_size": 2}
    with pytest.raises(RuntimeError, match="repeated"):
        read(connector, offset)


def test_future_records_do_not_keep_available_now_running(connector, monkeypatch):
    import requests

    from databricks.labs.community_connector.sources.rootly import rootly

    cutoff = datetime.now(timezone.utc).replace(microsecond=0)
    connector._init_time = cutoff.isoformat()
    records = [incident("past", (cutoff - timedelta(seconds=1)).isoformat())]
    records += [incident(str(i), (cutoff + timedelta(seconds=i)).isoformat()) for i in range(100)]
    corpus = SimpleNamespace(get=lambda _: records)
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs["params"])
        return list_resources(
            requests.Request("GET", url, params=kwargs["params"]).prepare(),
            SimpleNamespace(corpus="incidents"),
            corpus,
        )

    monkeypatch.setattr(rootly.requests, "get", get)
    rows, offset = read(connector, size="1")
    assert [r["id"] for r in rows] == ["past"]
    assert read(connector, offset, "1") == ([], offset)
    assert len(calls) == 1
