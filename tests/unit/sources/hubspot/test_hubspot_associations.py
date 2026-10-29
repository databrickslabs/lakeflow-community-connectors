"""Association payload reconciliation, independently of the HTTP transport."""

from unittest.mock import Mock, patch

import pytest

from databricks.labs.community_connector.sources.hubspot.hubspot_associations import (
    ASSOCIATION_MAX_RETRIES,
    HubspotAssociations,
)


def result(parent, targets=(), after=None):
    value = {"from": {"id": parent}, "to": [{"toObjectId": i} for i in targets]}
    if after is not None:
        value["paging"] = {"next": {"after": after}}
    return value


def batch(*results, status="COMPLETE", **extra):
    return {"status": status, "results": list(results), **extra}


def reader(*responses):
    post = Mock(side_effect=responses)
    sleep = Mock()
    return HubspotAssociations("https://example.invalid/", post, sleep), post, sleep


def inputs(post):
    return [call.args[2]["inputs"] for call in post.call_args_list]


def test_independent_cursors_reordered_parents_and_duplicate_targets():
    helper, post, sleep = reader(
        batch(result("b", [9], "b1"), result("a", [2, "2", "10"], "a1")),
        batch(result("a", ["3", "2"]), result("b", ["8", "9"], "b2")),
        batch(result("b", ["7"])),
    )
    assert helper.read("contacts", "2-123", ["a", "b"]) == {
        "a": ["10", "2", "3"], "b": ["7", "8", "9"]
    }
    assert inputs(post) == [
        [{"id": "a"}, {"id": "b"}],
        [{"id": "a", "after": "a1"}, {"id": "b", "after": "b1"}],
        [{"id": "b", "after": "b2"}],
    ]
    for call in post.call_args_list:
        assert call.args[0] == "associations"
        assert call.args[1] == (
            "https://example.invalid/crm/associations/2026-09/contacts/2-123/batch/read"
        )
        assert call.args[3] == 0.11
    sleep.assert_not_called()


def test_partial_207_like_payload_retries_only_missing_at_current_cursor():
    helper, post, sleep = reader(
        batch(result("a", [], "a1"), result("b")),
        batch(errors=[{"status": "error", "context": {"id": "a"}}]),
        batch(result("a", [5])),
    )
    assert helper.read("contacts", "companies", ["a", "b"]) == {"a": ["5"], "b": []}
    assert inputs(post)[1:] == [[{"id": "a", "after": "a1"}]] * 2
    sleep.assert_called_once_with(1.0)


def test_missing_parent_recovers_on_last_retry():
    helper, post, sleep = reader(*([batch()] * ASSOCIATION_MAX_RETRIES), batch(result("a")))
    assert helper.read("contacts", "companies", ["a"]) == {"a": []}
    assert post.call_count == ASSOCIATION_MAX_RETRIES + 1
    assert [c.args[0] for c in sleep.call_args_list] == [1, 2, 4, 8, 16]


def test_missing_parent_exhausts_without_returning_partial_data():
    helper, post, sleep = reader(batch(result("a")), *([batch()] * ASSOCIATION_MAX_RETRIES))
    with pytest.raises(RuntimeError, match="exhausted retries"):
        helper.read("contacts", "companies", ["a", "b"])
    assert post.call_count == ASSOCIATION_MAX_RETRIES + 1
    assert inputs(post)[1:] == [[{"id": "b"}]] * ASSOCIATION_MAX_RETRIES
    assert sleep.call_count == ASSOCIATION_MAX_RETRIES


@pytest.mark.parametrize("status", ["PENDING", "PROCESSING", "CANCELED", None, "UNKNOWN"])
def test_noncomplete_batch_does_not_commit_results_or_cursor(status):
    helper, post, _ = reader(
        batch(result("a", ["discard"], "unused"), status=status),
        batch(result("a", ["keep"])),
    )
    assert helper.read("contacts", "companies", ["a"]) == {"a": ["keep"]}
    assert inputs(post) == [[{"id": "a"}]] * 2


def test_noncomplete_status_exhausts():
    helper, post, _ = reader(*([batch(result("a"), status="PENDING")] * 6))
    with pytest.raises(RuntimeError, match="exhausted retries"):
        helper.read("contacts", "companies", ["a"])
    assert post.call_count == 6


@pytest.mark.parametrize("bad", [None, True, False, 1.5, -1, "", " ", [], {}])
@pytest.mark.parametrize("field", ["parent", "target"])
def test_rejects_bad_ids(bad, field):
    value = result(bad if field == "parent" else "a", [bad] if field == "target" else [])
    helper, post, _ = reader(batch(value))
    with pytest.raises(RuntimeError, match="Malformed"):
        helper.read("contacts", "companies", ["a"])
    assert post.call_count == 1


@pytest.mark.parametrize("payload", [
    batch(result("other")), batch(result("a"), result("a")),
    batch({"from": {"id": "a"}}), batch({"from": {"id": "a"}, "to": None}),
    batch({"from": {"id": "a"}, "to": [None]}), batch({"from": None, "to": []}),
    batch(None), {"status": "COMPLETE", "results": None}, [],
])
def test_rejects_malformed_and_unrequested_results(payload):
    helper, _, _ = reader(payload)
    with pytest.raises(RuntimeError):
        helper.read("contacts", "companies", ["a"])


@pytest.mark.parametrize("paging", [
    None, [], {"next": None}, {"next": {}}, {"next": {"link": "ignored"}},
    {"next": {"after": None}}, {"next": {"after": True}},
    {"next": {"after": ""}}, {"next": {"after": []}},
])
def test_rejects_malformed_paging(paging):
    value = result("a")
    value["paging"] = paging
    helper, _, _ = reader(batch(value))
    with pytest.raises(RuntimeError, match="Malformed"):
        helper.read("contacts", "companies", ["a"])


@pytest.mark.parametrize("cursors", [["x", "x"], ["x", "y", "x"], [0, "0"]])
def test_rejects_repeated_and_cyclic_cursors(cursors):
    helper, post, _ = reader(*(batch(result("a", [], c)) for c in cursors))
    with pytest.raises(RuntimeError, match="Repeated or cyclic"):
        helper.read("contacts", "companies", ["a"])
    assert post.call_count == len(cursors)


def test_retry_budget_resets_for_each_legitimate_page():
    responses = []
    for page in range(8):
        responses.extend([batch()] * ASSOCIATION_MAX_RETRIES)
        responses.append(batch(result("a", [page], str(page) if page < 7 else None)))
    helper, post, _ = reader(*responses)
    assert helper.read("contacts", "companies", ["a"]) == {"a": list("01234567")}
    assert post.call_count == 48


def test_more_than_1000_parents_are_chunked():
    parents = [str(i) for i in range(2001)]
    post = Mock(side_effect=lambda _kind, _url, body, _interval: batch(
        *(result(i["id"], [i["id"]]) for i in reversed(body["inputs"]))
    ))
    helper = HubspotAssociations("https://example.invalid", post)
    assert helper.read("contacts", "companies", parents) == {p: [p] for p in parents}
    assert [len(i) for i in inputs(post)] == [1000, 1000, 1]


def test_empty_input_does_not_call_transport():
    helper, post, _ = reader()
    assert helper.read("contacts", "companies", []) == {}
    post.assert_not_called()


@pytest.mark.parametrize("parents", [["a", "a"], [None], [True]])
def test_bad_requested_parents_fail_before_transport(parents):
    helper, post, _ = reader()
    with pytest.raises(RuntimeError):
        helper.read("contacts", "companies", parents)
    post.assert_not_called()


@pytest.mark.parametrize("http_status", [200, 207])
def test_history_transport_handles_http_status_separately(http_status):
    # Exercise the existing callback rather than duplicating HTTP handling here.
    from databricks.labs.community_connector.sources.hubspot.hubspot_property_history import (
        HubspotPropertyHistory,
    )

    transport = HubspotPropertyHistory(
        "https://example.invalid", {}, "2030-01-01T00:00:00Z",
        lambda _: [], lambda _: "",
        sleep=lambda _: None,
    )
    limited = Mock(status_code=429, headers={"Retry-After": "0"})
    success = Mock(status_code=http_status)
    success.json.return_value = batch(result("a", ["2"]))
    helper = HubspotAssociations("https://example.invalid", transport._post)
    with patch(
        "databricks.labs.community_connector.sources.hubspot.hubspot_property_history.requests.post",
        side_effect=[limited, success],
    ) as post:
        assert helper.read("contacts", "companies", ["a"]) == {"a": ["2"]}
    assert post.call_count == 2
    assert post.call_args.kwargs["json"] == {"inputs": [{"id": "a"}]}
