"""Multi-read offset tests for the GitHub cdc and append readers.

A non-empty batch must advance the offset; a drained source must return an
empty batch at a stable offset (SPARK-55416).
"""

from types import SimpleNamespace

import pytest

from databricks.labs.community_connector.sources.github.github import (
    GithubLakeflowConnect,
)

OPTS = {"owner": "o", "repo": "r"}


def _connector(routes):
    connector = GithubLakeflowConnect({"token": "fake"})

    def fake_get(url, params=None, timeout=None):  # noqa: ARG001
        matches = [k for k in routes if url.endswith(k)]
        if not matches:
            raise AssertionError(f"unexpected URL: {url}")
        payload = routes[max(matches, key=len)]
        return SimpleNamespace(status_code=200, headers={}, text="", json=lambda: payload)

    connector._session = SimpleNamespace(get=fake_get)  # noqa: SLF001
    return connector


def _drain(connector, table, opts):
    rows, offset = [], {}
    for _ in range(20):
        batch, next_offset = connector.read_table(table, offset, opts)
        batch = list(batch)
        if next_offset == offset:
            assert batch == [], "non-empty batch must advance the offset"
            return rows, offset
        assert batch, "an advancing offset must come with rows"
        rows.extend(batch)
        offset = next_offset
    raise AssertionError("offset never stabilised")


@pytest.mark.parametrize(
    "table,path",
    [
        ("issues", "/repos/o/r/issues"),
        ("pull_requests", "/repos/o/r/pulls"),
        ("comments", "/repos/o/r/issues/comments"),
    ],
)
def test_cdc_rows_sharing_the_boundary_second_are_not_dropped(table, path):
    # Five rows in one second, batch size 3 (the reviewer's example).
    same_second = [{"id": i, "updated_at": "2024-05-01T00:00:05Z"} for i in (5, 1, 4, 2, 3)]
    connector = _connector({path: same_second})
    rows, offset = _drain(connector, table, {**OPTS, "max_records_per_batch": "3"})

    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]
    assert offset == {"cursor": "2024-05-01T00:00:05Z", "cursor_id": 5}


def test_cdc_picks_up_new_rows_on_the_next_run():
    routes = {"/repos/o/r/issues": [{"id": 1, "updated_at": "2024-05-01T00:00:05Z"}]}
    _, offset = _drain(_connector(routes), "issues", OPTS)

    # Next run: a new issue in the same second plus a later update.
    routes["/repos/o/r/issues"] += [
        {"id": 2, "updated_at": "2024-05-01T00:00:05Z"},
        {"id": 1, "updated_at": "2024-05-02T00:00:00Z"},
    ]
    batch, next_offset = _connector(routes).read_table("issues", offset, OPTS)
    assert [(r["id"], r["updated_at"]) for r in batch] == [
        (2, "2024-05-01T00:00:05Z"),
        (1, "2024-05-02T00:00:00Z"),
    ]
    assert next_offset == {"cursor": "2024-05-02T00:00:00Z", "cursor_id": 1}


def test_cdc_holds_back_rows_updated_during_the_run():
    connector = _connector({"/repos/o/r/issues": [{"id": 9, "updated_at": "2024-05-01T00:00:05Z"}]})
    connector._init_time = "2024-05-01T00:00:05Z"  # noqa: SLF001
    batch, offset = connector.read_table("issues", {}, OPTS)
    assert list(batch) == [] and offset == {}


def test_reviews_resume_across_runs_and_batches():
    routes = {
        "/repos/o/r/pulls": [{"number": 1}, {"number": 2}],
        "/repos/o/r/pulls/1/reviews": [{"id": 30}, {"id": 10}],
        "/repos/o/r/pulls/2/reviews": [{"id": 20}],
    }
    rows, offset = _drain(_connector(routes), "reviews", {**OPTS, "max_records_per_batch": "2"})
    assert [r["id"] for r in rows] == [10, 20, 30]
    assert offset == {"max_review_id": 30}

    # A review submitted after the first run is ingested by the next one.
    routes["/repos/o/r/pulls/2/reviews"] = [{"id": 20}, {"id": 40}]
    batch, next_offset = _connector(routes).read_table("reviews", offset, OPTS)
    assert [r["id"] for r in batch] == [40] and next_offset == {"max_review_id": 40}
