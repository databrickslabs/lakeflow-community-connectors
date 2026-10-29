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


def reader(*responses, generated=False):
    post = Mock(side_effect=responses)
    sleep = Mock()
    return association_class(generated)("https://example.invalid/", post, sleep), post, sleep


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


def association_class(generated):
    import runpy
    from pathlib import Path
    import databricks.labs.community_connector.sources.hubspot.hubspot as source

    helper_class = HubspotAssociations
    if generated:
        path = Path(source.__file__).with_name("_generated_hubspot_python_source.py")
        from types import SimpleNamespace
        registry = Mock()
        runpy.run_path(str(path))["register_lakeflow_source"](SimpleNamespace(dataSource=registry))
        connector = registry.register.call_args.args[0]({"access_token": "offline"}).lakeflow_connect
        helper_class = type(connector._associations)
    return helper_class


@pytest.mark.parametrize("generated", [False, True])
def test_exhaustion_diagnostics_are_bounded_and_do_not_expose_payload(generated):
    helper_class = association_class(generated)
    payload = batch(errors=[{"message": "SECRET_PAYLOAD", "context": {"id": "PRIVATE_ID"}}])
    post = Mock(return_value=payload)
    helper = helper_class("https://example.invalid", post, Mock())
    with pytest.raises(RuntimeError) as raised:
        helper.read("deals", "tickets", ["PRIVATE_ID"])
    message = str(raised.value)
    assert "status=COMPLETE" in message
    assert "requested=1" in message and "returned=0" in message
    assert "errors=1" in message
    assert "SECRET_PAYLOAD" not in message and "PRIVATE_ID" not in message


@pytest.mark.parametrize("extra", [
    {"errors": [{"message": "attributable context not guaranteed"}]},
    {"numErrors": 1},
])
@pytest.mark.parametrize("generated", [False, True])
def test_complete_with_errors_never_commits_explicit_empty(extra, generated):
    helper, post, _ = reader(*([batch(result("a"), **extra)] * 6), generated=generated)
    with pytest.raises(RuntimeError, match="exhausted retries"):
        helper.read("deals", "tickets", ["a"])
    assert post.call_count == 6


@pytest.mark.parametrize("extra", [{"errors": "bad"}, {"numErrors": -1},
                                   {"numErrors": True}, {"numErrors": "1"}])
@pytest.mark.parametrize("generated", [False, True])
def test_malformed_error_envelope_fails_before_returning_empty(extra, generated):
    helper, _, _ = reader(batch(result("a"), **extra), generated=generated)
    with pytest.raises(RuntimeError, match="error"):
        helper.read("deals", "tickets", ["a"])


@pytest.mark.parametrize("generated", [False, True])
def test_exhaustion_reports_only_finite_error_labels(generated):
    payload = batch(errors=[{
        "category": "VALIDATION_ERROR",
        "message": "PRIVATE_MESSAGE",
        "errors": [{"code": "OBJECT_NOT_FOUND", "context": {"id": "PRIVATE_ID"}}],
    }, {
        "category": "PRIVATE_CATEGORY",
        "errors": [{"code": "PRIVATE_CODE"}, {"code": ["PRIVATE_NESTED"]}],
    }, {"category": {"value": "PRIVATE_CATEGORY"}}, "PRIVATE_ERROR"], numErrors=4)
    helper, post, _ = reader(*([payload] * 6), generated=generated)
    with pytest.raises(RuntimeError) as raised:
        helper.read("deals", "contacts", ["PRIVATE_ID"])
    message = str(raised.value)
    assert "error_categories=UNKNOWN|VALIDATION_ERROR" in message
    assert "error_codes=OBJECT_NOT_FOUND|UNKNOWN" in message
    assert "PRIVATE" not in message
    assert post.call_count == 6
    assert len(message) < 500


def missing_objects():
    return batch(errors=[{"category": "OBJECT_NOT_FOUND"}], numErrors=1)


def get_page(*ids, after=None):
    page = {"results": [{"toObjectId": i, "associationTypes": []} for i in ids]}
    if after is not None:
        page["paging"] = {"next": {"after": after, "link": "https://untrusted.invalid"}}
    return page


@pytest.mark.parametrize("generated", [False, True])
def test_not_found_reconciles_all_pending_parents_with_independent_gets(generated):
    post = Mock(return_value=missing_objects())
    get = Mock(side_effect=[get_page("2", after="a+cursor"), get_page("2", "3"),
                           get_page(after="b_cursor"), get_page(), {"id": "b", "archived": False}])
    helper = association_class(generated)("https://example.invalid", post, Mock(), get=get)
    assert helper.read("deals", "contacts", ["a", "b"]) == {"a": ["2", "3"], "b": []}
    assert post.call_count == 6
    assert get.call_args_list[0].args[2] == {"limit": 500}
    assert get.call_args_list[1].args[2] == {"limit": 500, "after": "a+cursor"}
    assert get.call_args_list[2].args[2] == {"limit": 500}
    assert get.call_args_list[-1].args[1].endswith("/deals/b")


@pytest.mark.parametrize("generated", [False, True])
def test_get_restarts_at_page_one_and_replaces_previous_batch_targets(generated):
    post = Mock(side_effect=[batch(result("a", ["stale"], "batch_cursor"))] + [missing_objects()] * 6)
    get = Mock(return_value=get_page("fresh"))
    helper = association_class(generated)("https://example.invalid", post, Mock(), get=get)
    assert helper.read("deals", "contacts", ["a"]) == {"a": ["fresh"]}
    assert get.call_args.args[2] == {"limit": 500}


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("payload", [batch(), batch(errors=[{"category": "MISSING_SCOPES"}], numErrors=1),
    batch(errors=[{"category": "OBJECT_NOT_FOUND"}], numErrors=2),
    batch(errors=[{"category": "OBJECT_NOT_FOUND"}]),
    batch(errors=[{"category": "OBJECT_NOT_FOUND"}, {"category": "UNKNOWN"}], numErrors=2),
    {"status": "COMPLETE", "errors": [{"category": "OBJECT_NOT_FOUND"}], "numErrors": 1},
    batch(status="PROCESSING", errors=[{"category": "OBJECT_NOT_FOUND"}], numErrors=1)])
def test_get_fallback_rejects_nonqualifying_batches(payload, generated):
    get = Mock()
    helper = association_class(generated)("https://example.invalid", Mock(return_value=payload),
                                          Mock(), get=get)
    with pytest.raises(RuntimeError):
        helper.read("deals", "contacts", ["a"])
    get.assert_not_called()


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("pages", [[{}], [{"results": None}], [{"results": [None]}],
    [{"results": [{"toObjectId": "2"}]}],
    [get_page(after="cycle"), get_page(after="cycle")],
    [{"results": [], "paging": {"next": {}}}],
    [get_page(), {"id": "wrong", "archived": False}],
    [get_page(), {"id": "a", "archived": True}],
    [get_page(), {"id": "a"}]])
def test_get_reconciliation_rejects_unproven_results(pages, generated):
    helper = association_class(generated)("https://example.invalid", Mock(return_value=missing_objects()),
                                          Mock(), get=Mock(side_effect=pages))
    with pytest.raises(RuntimeError):
        helper.read("deals", "contacts", ["a"])


@pytest.mark.parametrize("generated", [False, True])
def test_get_later_parent_failure_does_not_return_partial_results(generated):
    helper = association_class(generated)("https://example.invalid", Mock(return_value=missing_objects()),
        Mock(), get=Mock(side_effect=[get_page("2"), RuntimeError("read failed")]))
    with pytest.raises(RuntimeError, match="read failed"):
        helper.read("deals", "contacts", ["a", "b"])


@pytest.mark.parametrize("generated", [False, True])
def test_get_path_segments_encoded_and_missing_parent_fails(generated):
    get = Mock(side_effect=[get_page(), RuntimeError("parent unavailable")])
    helper = association_class(generated)("https://example.invalid", Mock(return_value=missing_objects()),
                                          Mock(), get=get)
    with pytest.raises(RuntimeError, match="parent unavailable"):
        helper.read("2-1", "contacts", ["a/b?x=1"])
    assert "/a%2Fb%3Fx%3D1/associations/contacts" in get.call_args_list[0].args[1]


def transport_connector(generated):
    from pathlib import Path
    from types import SimpleNamespace
    import runpy
    import databricks.labs.community_connector.sources.hubspot.hubspot as source
    if not generated:
        return source.HubspotLakeflowConnect({"access_token": "offline"})
    registry = Mock()
    path = Path(source.__file__).with_name("_generated_hubspot_python_source.py")
    runpy.run_path(str(path))["register_lakeflow_source"](SimpleNamespace(dataSource=registry))
    return registry.register.call_args.args[0]({"access_token": "offline"}).lakeflow_connect


@pytest.mark.parametrize("generated", [False, True])
def test_get_transport_429_is_bounded_and_reuses_shared_association_throttle(generated):
    connector = transport_connector(generated)
    connector._history._throttle = Mock()
    connector._history._sleep = Mock()
    limited = Mock(status_code=429, headers={"Retry-After": "999"})
    success = Mock(status_code=200)
    success.json.return_value = get_page("2")
    with patch("requests.get", side_effect=[limited, success]) as get:
        assert connector._get_association_page("associations", "https://example.invalid", {}, .11) == get_page("2")
    assert get.call_count == 2
    connector._history._sleep.assert_called_once_with(30.0)
    assert connector._history._throttle.call_count == 2
    assert connector._associations._get == connector._get_association_page
    connector._history._sleep.reset_mock()
    with patch("requests.get", return_value=limited) as get:
        with pytest.raises(RuntimeError, match="status=429"):
            connector._get_association_page("associations", "https://example.invalid", {}, .11)
    assert get.call_count == 6
    assert connector._history._sleep.call_count == 5


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("failure", [404, 403, 207, "request", "json"])
def test_get_transport_never_exposes_payload_url_or_chained_exception(generated, failure):
    import requests
    import traceback
    connector = transport_connector(generated)
    connector._history._throttle = Mock()
    response = Mock(status_code=failure if isinstance(failure, int) else 200, text="PRIVATE_BODY")
    response.json.side_effect = ValueError("PRIVATE_JSON")
    with patch("requests.get", **({"side_effect": requests.RequestException("PRIVATE_EXCEPTION")}
                                if failure == "request" else {"return_value": response})):
        with pytest.raises(RuntimeError) as raised:
            connector._get_association_page("associations", "https://example.invalid/PRIVATE_ID", {}, .11)
    assert "PRIVATE" not in str(raised.value)
    assert raised.value.__suppress_context__ or raised.value.__context__ is None
    assert "PRIVATE_EXCEPTION" not in "".join(traceback.format_exception(raised.value))


@pytest.mark.parametrize("generated", [False, True])
def test_failed_get_reconciliation_returns_no_connector_rows_or_checkpoint(generated):
    connector = transport_connector(generated)
    connector.list_tables = Mock(return_value=["deals"])
    connector.read_table_metadata = Mock(return_value={"property_names": [],
        "cursor_property_field": "hs_lastmodifieddate", "associations": ["contacts"]})
    records = [{"id": parent, "updatedAt": "2020-01-01T00:00:00Z"} for parent in ["a", "b"]]
    connector._fetch_incremental_batch = Mock(return_value=(records, None, None))
    connector._transform_records = Mock()
    connector._associations._post = Mock(return_value=missing_objects())
    connector._associations._sleep = Mock()
    connector._associations._get = Mock(side_effect=[get_page("2"), RuntimeError("second parent failed")])
    start = {"updatedAt": "1970-01-01T00:00:00Z"}
    with pytest.raises(RuntimeError, match="second parent failed"):
        connector.read_table("deals", start, {})
    assert start == {"updatedAt": "1970-01-01T00:00:00Z"}
    connector._transform_records.assert_not_called()


@pytest.mark.parametrize("generated", [False, True])
def test_get_reconciliation_preserves_parents_already_completed_by_batch(generated):
    post = Mock(side_effect=[batch(result("done", ["existing"]), result("a", ["stale"], "batch"))]
                           + [missing_objects()] * 6)
    get = Mock(return_value=get_page("fresh"))
    helper = association_class(generated)("https://example.invalid", post, Mock(), get=get)
    assert helper.read("deals", "contacts", ["done", "a"]) == {"done": ["existing"], "a": ["fresh"]}
    assert get.call_count == 1
    assert get.call_args.args[1].endswith("/a/associations/contacts")


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("extra", [
    {"status": "error", "errors": [{"category": "MISSING_SCOPES"}]},
    {"errors": [{"category": "OBJECT_NOT_FOUND"}]}, {"numErrors": 0},
    {"category": "PRIVATE_CATEGORY"}, {"status": None},
    {"status": {"private": "value"}},
])
def test_empty_get_rejects_error_bearing_parent_confirmation(generated, extra):
    get = Mock(side_effect=[get_page(), {"id": "a", "archived": False, **extra}])
    helper = association_class(generated)("https://example.invalid", Mock(return_value=missing_objects()),
                                          Mock(), get=get)
    with pytest.raises(RuntimeError, match="parent identity"):
        helper.read("deals", "contacts", ["a"])
