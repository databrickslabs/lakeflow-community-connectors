"""Mock-based unit tests for AzureDevopsLakeflowConnect.

These tests stub ``requests.Session`` responses to cover specific bug
scenarios and edge cases — complementing the live-API integration
suite in ``test_azure_devops_lakeflow_connect.py``. They run without
credentials and are therefore safe for CI.

Each test targets a specific past bug or contract so a future regression
surfaces here instead of being reported by a user.
"""

from unittest.mock import MagicMock

import pytest

from databricks.labs.community_connector.sources.azure_devops.azure_devops import (
    AzureDevopsLakeflowConnect,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _response(status_code: int = 200, json_body=None, headers=None):
    """Build a mock ``requests.Response``-like object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else {}
    resp.headers = headers if headers is not None else {}
    resp.text = str(json_body) if json_body is not None else ""
    return resp


@pytest.fixture
def conn():
    """AzureDevopsLakeflowConnect with a stubbed ``requests.Session``.

    ``conn.project`` defaults to "proj1" so tests that don't exercise
    multi-project discovery skip the ``_read_projects`` API round-trip.
    """
    c = AzureDevopsLakeflowConnect(
        {
            "organization": "testorg",
            "project": "proj1",
            "personal_access_token": "fake-pat",
        }
    )
    c._session = MagicMock()
    return c


# ---------------------------------------------------------------------------
# _discover_workitem_ids — WIQL escaping and project scoping
# ---------------------------------------------------------------------------


def test_wiql_escapes_single_quote_in_project_name(conn):
    """Review comment #3: project names with apostrophes must be escaped."""
    conn._session.post.return_value = _response(json_body={"workItems": []})

    conn._discover_workitem_ids("Bob's Team")

    posted = conn._session.post.call_args
    query = posted.kwargs["json"]["query"]
    # Escaped form present
    assert "'Bob''s Team'" in query
    # Unescaped form (which would break WIQL parsing) absent
    assert "= 'Bob's Team'" not in query


def test_wiql_filters_by_team_project(conn):
    """Issue #8: WIQL must scope to a project via System.TeamProject."""
    conn._session.post.return_value = _response(json_body={"workItems": []})

    conn._discover_workitem_ids("myproject")

    query = conn._session.post.call_args.kwargs["json"]["query"]
    assert "[System.TeamProject] = 'myproject'" in query


# ---------------------------------------------------------------------------
# _read_users — drained-offset termination contract
# ---------------------------------------------------------------------------


def test_read_users_drained_returns_start_offset_unchanged(conn):
    """Review comment #4: no continuation token ⇒ return start_offset so
    end_offset == start_offset and pagination terminates cleanly."""
    conn._session.get.return_value = _response(
        json_body={"value": [{"descriptor": "abc", "displayName": "X"}]},
        headers={},  # no X-MS-ContinuationToken
    )

    start = {"prior": "state"}
    records, end = conn._read_users(start, {})
    records = list(records)

    assert len(records) == 1
    assert records[0]["descriptor"] == "abc"
    assert end is start or end == start  # unchanged offset


def test_read_users_passes_continuation_token_when_present(conn):
    """With a continuation token, the offset carries it forward."""
    conn._session.get.return_value = _response(
        json_body={"value": [{"descriptor": "a"}]},
        headers={"X-MS-ContinuationToken": "token-abc"},
    )

    _, offset = conn._read_users({}, {})
    assert offset == {"continuationToken": "token-abc"}


# ---------------------------------------------------------------------------
# _read_workitem_revisions — derived columns + isLastBatch semantics
# ---------------------------------------------------------------------------


def test_workitem_revisions_populates_derived_columns(conn):
    """Issue #9: workItemId, revisedDate, url must be populated."""
    conn._session.get.return_value = _response(
        json_body={
            "values": [
                {
                    "id": 42,
                    "rev": 3,
                    "fields": {
                        "System.Id": 42,
                        "System.RevisedDate": "2026-04-20T10:00:00Z",
                        "System.ChangedDate": "2026-04-20T10:00:00Z",
                    },
                }
            ],
            "isLastBatch": True,
            "continuationToken": "tok",
        }
    )
    # Skip the projects-list API call by passing project explicitly
    records, _ = conn._read_workitem_revisions({}, {"project": "proj1"})
    records = list(records)

    assert len(records) == 1
    rec = records[0]
    assert rec["workItemId"] == 42  # mirror of id
    assert rec["revisedDate"] == "2026-04-20T10:00:00Z"  # hoisted
    assert rec["url"] == (
        "https://dev.azure.com/testorg/proj1/_apis/wit/workItems/42/revisions/3"
    )


def test_workitem_revisions_isLastBatch_false_returns_resume(conn):
    """Phase 1 fix: isLastBatch=false ⇒ mid-pagination; offset carries resume."""
    conn._session.get.return_value = _response(
        json_body={
            "values": [{"id": 1, "rev": 1, "fields": {}}],
            "isLastBatch": False,
            "continuationToken": "mid-token",
        }
    )

    _, offset = conn._read_workitem_revisions({}, {"project": "proj1"})
    assert offset["resume"]["project"] == "proj1"
    assert offset["resume"]["continuationToken"] == "mid-token"


def test_workitem_revisions_isLastBatch_true_finalizes_watermark(conn):
    """Phase 1 fix: isLastBatch=true ⇒ watermark advances, no resume key."""
    conn._session.get.return_value = _response(
        json_body={
            "values": [{"id": 1, "rev": 1, "fields": {}}],
            "isLastBatch": True,
            "continuationToken": "final-token",
        }
    )

    _, offset = conn._read_workitem_revisions({}, {"project": "proj1"})
    assert offset["watermarks"]["proj1"] == "final-token"
    assert "resume" not in offset


# ---------------------------------------------------------------------------
# _read_workitems — cross-project scoping
# ---------------------------------------------------------------------------


def test_workitems_wiql_scoped_to_project(conn):
    """Issue #8: WIQL must include TeamProject filter; otherwise items from
    other projects leak in and get labeled with the current loop's project."""
    conn._session.post.return_value = _response(json_body={"workItems": []})

    conn._read_workitems({}, {"project": "projA"})

    query = conn._session.post.call_args.kwargs["json"]["query"]
    assert "[System.TeamProject] = 'projA'" in query


# ---------------------------------------------------------------------------
# _read_pullrequests — incremental watermarks
# ---------------------------------------------------------------------------


def _make_pullrequests_mock(conn, prs: list):
    """Prime the session.get queue for one project / one repo pullrequests fetch."""
    conn._session.get.side_effect = [
        # _resolve_repo_pairs -> _read_repositories for proj1
        _response(
            json_body={
                "value": [
                    {"id": "r1", "name": "r1", "project": {"name": "proj1"}}
                ]
            }
        ),
        # _fetch_pullrequests
        _response(json_body={"value": prs}),
    ]


def test_pullrequests_initial_fetch_sets_watermark(conn):
    """First run: watermark = max(closedDate ∨ creationDate)."""
    _make_pullrequests_mock(
        conn,
        [
            {
                "pullRequestId": 1,
                "status": "completed",
                "closedDate": "2026-04-20T10:00:00Z",
                "creationDate": "2026-04-19T09:00:00Z",
            },
            {
                "pullRequestId": 2,
                "status": "active",
                "creationDate": "2026-04-21T11:00:00Z",
            },
        ],
    )

    records, offset = conn._read_pullrequests({}, {})
    records = list(records)
    assert len(records) == 2
    assert offset["watermarks"]["proj1/r1"] == "2026-04-21T11:00:00Z"


def test_pullrequests_incremental_uses_updated_filter(conn):
    """With watermark present: searchCriteria.queryTimeRangeType=updated."""
    _make_pullrequests_mock(conn, [])

    prior = {"watermarks": {"proj1/r1": "2026-04-19T00:00:00Z"}}
    conn._read_pullrequests(prior, {})

    fetch_params = conn._session.get.call_args_list[-1].kwargs["params"]
    assert fetch_params["searchCriteria.queryTimeRangeType"] == "updated"
    assert fetch_params["searchCriteria.minTime"] == "2026-04-19T00:00:00Z"


def test_pullrequests_stable_offset_when_no_changes(conn):
    """No new PRs ⇒ end_offset == start_offset (termination)."""
    _make_pullrequests_mock(conn, [])

    prior = {"watermarks": {"proj1/r1": "2026-04-19T00:00:00Z"}}
    records, offset = conn._read_pullrequests(prior, {})
    assert list(records) == []
    assert offset == prior


# ---------------------------------------------------------------------------
# _read_pullrequest_threads — Option D hybrid
# ---------------------------------------------------------------------------


def _make_threads_mock(conn, pr_ids: list[int], threads_by_pr: dict):
    """Prime the session.get queue for threads auto-discovery path."""
    calls = [
        # _resolve_repo_pairs -> _read_repositories
        _response(
            json_body={
                "value": [
                    {"id": "r1", "name": "r1", "project": {"name": "proj1"}}
                ]
            }
        ),
        # _list_updated_pr_ids -> pullrequests
        _response(
            json_body={
                "value": [{"pullRequestId": pid} for pid in pr_ids]
            }
        ),
    ]
    # One /threads response per PR
    for pid in pr_ids:
        calls.append(_response(json_body={"value": threads_by_pr[pid]}))
    conn._session.get.side_effect = calls


def test_threads_uses_pullrequests_updated_filter_for_discovery(conn):
    """Option D: PR discovery via queryTimeRangeType=updated."""
    _make_threads_mock(
        conn,
        [42],
        {42: [{"id": 99, "lastUpdatedDate": "2026-04-21T12:00:00Z"}]},
    )

    prior = {"watermarks": {"proj1/r1": "2026-04-20T00:00:00Z"}}
    records, offset = conn._read_pullrequest_threads(prior, {})
    records = list(records)

    assert len(records) == 1
    assert records[0]["pullrequest_id"] == 42
    assert offset["watermarks"]["proj1/r1"] == "2026-04-21T12:00:00Z"

    # The 2nd call was the PR-discovery via pullrequests endpoint
    discovery_params = conn._session.get.call_args_list[1].kwargs["params"]
    assert discovery_params["searchCriteria.queryTimeRangeType"] == "updated"
    assert discovery_params["searchCriteria.minTime"] == "2026-04-20T00:00:00Z"


def test_threads_client_side_filter_drops_old_threads(conn):
    """Threads with lastUpdatedDate <= watermark are dropped client-side."""
    _make_threads_mock(
        conn,
        [1],
        {
            1: [
                # Before watermark — should be filtered out
                {"id": 1, "lastUpdatedDate": "2026-04-19T00:00:00Z"},
                # After watermark — should pass through
                {"id": 2, "lastUpdatedDate": "2026-04-21T00:00:00Z"},
            ]
        },
    )

    prior = {"watermarks": {"proj1/r1": "2026-04-20T00:00:00Z"}}
    records, _ = conn._read_pullrequest_threads(prior, {})
    records = list(records)

    assert len(records) == 1
    assert records[0]["id"] == 2


def test_threads_explicit_pr_bypasses_watermarks(conn):
    """repo_id + pr_id ⇒ fetch single PR's threads without touching offsets."""
    conn._session.get.return_value = _response(
        json_body={
            "value": [{"id": 1, "lastUpdatedDate": "2026-04-21T00:00:00Z"}]
        }
    )

    opts = {"repository_id": "r1", "pullrequest_id": "42"}
    prior = {"watermarks": {"something": "else"}}
    records, offset = conn._read_pullrequest_threads(prior, opts)
    records = list(records)

    assert len(records) == 1
    assert records[0]["pullrequest_id"] == 42
    assert offset == prior  # unchanged


# ---------------------------------------------------------------------------
# _read_commits — per-repo watermark + strict-dedup
# ---------------------------------------------------------------------------


def _make_commits_mock(conn, repos: list, commits_by_repo: dict):
    """Prime session.get for commits auto-discovery."""
    calls = [
        _response(json_body={"value": repos}),  # _read_repositories
    ]
    for repo in repos:
        calls.append(
            _response(json_body={"value": commits_by_repo[repo["id"]]})
        )
    conn._session.get.side_effect = calls


def test_commits_per_repo_watermarks(conn):
    """Each repo gets its own watermark, keyed by project/repo_id."""
    repos = [
        {"id": "r1", "name": "r1", "project": {"name": "proj1"}},
        {"id": "r2", "name": "r2", "project": {"name": "proj1"}},
    ]
    _make_commits_mock(
        conn,
        repos,
        {
            "r1": [
                {
                    "commitId": "abc",
                    "committer": {"date": "2026-04-20T10:00:00Z"},
                }
            ],
            "r2": [
                {
                    "commitId": "def",
                    "committer": {"date": "2026-04-21T11:00:00Z"},
                }
            ],
        },
    )

    records, offset = conn._read_commits({}, {})
    records = list(records)
    assert len(records) == 2
    assert offset["watermarks"]["proj1/r1"] == "2026-04-20T10:00:00Z"
    assert offset["watermarks"]["proj1/r2"] == "2026-04-21T11:00:00Z"


def test_commits_incremental_uses_fromDate(conn):
    """Subsequent runs must pass searchCriteria.fromDate."""
    _make_commits_mock(
        conn,
        [{"id": "r1", "name": "r1", "project": {"name": "proj1"}}],
        {"r1": []},
    )

    prior = {"watermarks": {"proj1/r1": "2026-04-20T00:00:00Z"}}
    conn._read_commits(prior, {})

    fetch_params = conn._session.get.call_args_list[-1].kwargs["params"]
    assert fetch_params["searchCriteria.fromDate"] == "2026-04-20T00:00:00Z"


def test_commits_client_side_strict_gt_dedups_boundary(conn):
    """ADO's inclusive >= filter + client-side > = strict overall.
    Records at exactly the watermark must be dropped, not duplicated."""
    _make_commits_mock(
        conn,
        [{"id": "r1", "name": "r1", "project": {"name": "proj1"}}],
        {
            "r1": [
                # At watermark ⇒ drop
                {
                    "commitId": "a",
                    "committer": {"date": "2026-04-20T00:00:00Z"},
                },
                # After watermark ⇒ keep
                {
                    "commitId": "b",
                    "committer": {"date": "2026-04-21T00:00:00Z"},
                },
            ]
        },
    )

    prior = {"watermarks": {"proj1/r1": "2026-04-20T00:00:00Z"}}
    records, _ = conn._read_commits(prior, {})
    records = list(records)
    assert len(records) == 1
    assert records[0]["commitId"] == "b"


# ---------------------------------------------------------------------------
# _read_pushes — same pattern as commits
# ---------------------------------------------------------------------------


def test_pushes_per_repo_watermarks(conn):
    """Pushes follow the commits pattern: per-repo watermarks."""
    repos = [{"id": "r1", "name": "r1", "project": {"name": "proj1"}}]
    conn._session.get.side_effect = [
        _response(json_body={"value": repos}),
        _response(
            json_body={
                "value": [
                    {"pushId": 1, "date": "2026-04-20T10:00:00.123Z"}
                ]
            }
        ),
    ]

    records, offset = conn._read_pushes({}, {})
    records = list(records)
    assert len(records) == 1
    assert offset["watermarks"]["proj1/r1"] == "2026-04-20T10:00:00.123Z"


# ---------------------------------------------------------------------------
# _for_all_repos — narrow exception handling
# ---------------------------------------------------------------------------


def test_for_all_repos_skips_runtime_error(conn):
    """Review comment #5: RuntimeError (403/404 etc.) is skipped gracefully."""
    conn._session.get.return_value = _response(
        json_body={
            "value": [
                {"id": "r1", "name": "r1", "project": {"name": "proj1"}},
                {"id": "r2", "name": "r2", "project": {"name": "proj1"}},
            ]
        }
    )

    calls = []

    def reader(start_offset, opts):
        calls.append(opts["repository_id"])
        if opts["repository_id"] == "r1":
            raise RuntimeError("simulated 403")
        return iter([{"commitId": "ok"}]), {}

    records, _ = conn._for_all_repos({}, {}, reader)
    records = list(records)
    assert len(records) == 1  # only r2 succeeded
    assert calls == ["r1", "r2"]  # both attempted


def test_for_all_repos_propagates_non_runtime_error(conn):
    """Review comment #5: non-RuntimeError (genuine bug) must propagate."""
    conn._session.get.return_value = _response(
        json_body={
            "value": [
                {"id": "r1", "name": "r1", "project": {"name": "proj1"}}
            ]
        }
    )

    def buggy_reader(start_offset, opts):
        raise AttributeError("genuine programming bug")

    with pytest.raises(AttributeError):
        conn._for_all_repos({}, {}, buggy_reader)
