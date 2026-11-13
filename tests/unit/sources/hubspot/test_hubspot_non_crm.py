"""Offline snapshot protocol and consumer-schema tests for non-CRM resources."""

import json
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import _make_type_verifier

from databricks.labs.community_connector.sources.hubspot.hubspot_non_crm import (
    NON_CRM_MAX_RETRIES,
    HubspotNonCrm,
)

NON_CRM_TEST_GET = (
    "databricks.labs.community_connector.sources.hubspot.hubspot_non_crm.requests.get"
)


def page(*rows, after=None):
    data = {"results": list(rows)}
    if after is not None:
        data["paging"] = {"next": {"after": after, "link": "do-not-follow"}}
    return data


def response(data=None, status=200, headers=None):
    value = Mock(status_code=status, headers=headers or {})
    value.json.return_value = data
    return value


def helper(*payloads, custom_types=()):
    post = Mock(side_effect=payloads)
    sleeps = Mock()
    return HubspotNonCrm("https://example.invalid/", {"Authorization": "fake"},
                         lambda: list(custom_types), post, sleeps), post, sleeps


def contact_list(identity="1", **extra):
    return {"listId": identity, "objectTypeId": "0-1", "processingType": "MANUAL",
            "processingStatus": "COMPLETE", **extra}


@pytest.mark.parametrize("table", ["owners", "forms"])
def test_partitions_pagination_and_identical_duplicate(table):
    source = {"id": "1", "archived": False, "unknown": {"nested": [1, True]}}
    archived = {"id": "2", "archived": True}
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, side_effect=[
        response(page(source, after="next")), response(page(source)), response(page(archived)),
    ]) as get:
        rows = read.read_table(table, {})
    assert [r["id"] for r in rows] == ["1", "2"]
    assert json.loads(rows[0]["payload"]) == source
    assert [c.kwargs["params"]["archived"] for c in get.call_args_list] == [
        "false", "false", "true",
    ]
    assert get.call_args_list[1].kwargs["params"]["after"] == "next"
    expected = "/crm/owners/2026-09" if table == "owners" else "/marketing/v3/forms"
    assert all(c.args[0] == "https://example.invalid" + expected for c in get.call_args_list)
    if table == "forms":
        assert get.call_args.kwargs["params"]["formTypes"] == "all"
    assert all(c.kwargs["timeout"] == 60 for c in get.call_args_list)


def test_lists_post_body_and_body_cursor():
    read, post, _ = helper(page(contact_list(), after="opaque"), page(contact_list("2")))
    rows = read.read_table("contact_lists", {})
    assert [r["listId"] for r in rows] == ["1", "2"]
    first, second = post.call_args_list
    assert first.args == (
        "non_crm", "https://example.invalid/crm/lists/2026-09/all",
        {"objectTypeId": "0-1", "processingTypes": ["MANUAL", "DYNAMIC", "SNAPSHOT"],
         "additionalProperties": [], "count": 500}, 0.11,
    )
    assert second.args[2] == {**first.args[2], "after": "opaque"}


def test_memberships_all_lists_composite_keys_and_full_rescan_removal():
    inventory = page(contact_list("1"), contact_list("2"))
    read, post, _ = helper(inventory, inventory)
    with patch(NON_CRM_TEST_GET, side_effect=[
        response(page({"recordId": 10, "membershipTimestamp": "now"}, after="x")),
        response(page({"recordId": "20"})), response(page({"recordId": "10"})),
        response(page()), response(page({"recordId": "10"})),
    ]) as get:
        first = read.read_table("contact_list_memberships", {})
        second = read.read_table("contact_list_memberships", {})
    assert {(r["listId"], r["recordId"]) for r in first} == {
        ("1", "10"), ("1", "20"), ("2", "10"),
    }
    assert [(r["listId"], r["recordId"]) for r in second] == [("2", "10")]
    assert get.call_args_list[1].kwargs["params"] == {"limit": 100, "after": "x"}
    assert get.call_args_list[2].args[0].endswith("/crm/lists/2026-09/2/memberships")
    assert post.call_count == 2


def test_submissions_all_forms_pages_and_composite_identity():
    submission = {"conversionId": "same", "submittedAt": 123,
                  "values": [{"name": "email", "value": "a@example.invalid", "unknown": 1}],
                  "unknown": {"keep": True}}
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, side_effect=[
        response(page({"id": "f1"})), response(page({"id": "f2"})),
        response(page(submission, after="opaque")),
        response(page({"conversionId": "other", "submittedAt": 124, "values": []})),
        response(page(submission)),
    ]) as get:
        rows = read.read_table("form_submissions", {})
    assert {(r["formId"], r["conversionId"]) for r in rows} == {
        ("f1", "same"), ("f1", "other"), ("f2", "same"),
    }
    assert json.loads(rows[0]["payload"])["unknown"] == {"keep": True}
    assert rows[0]["values"][0]["value"] == "a@example.invalid"
    for row in rows:
        _make_type_verifier(read.get_table_schema("form_submissions"))(row)
    assert get.call_args_list[2].args[0].endswith("/form-integrations/v1/submissions/forms/f1")
    assert get.call_args_list[3].kwargs["params"] == {"limit": 50, "after": "opaque"}


def test_pipeline_defaults_overrides_and_nested_lossless_payload():
    source = {"id": "same", "label": "Main", "displayOrder": 1,
              "stages": [{"id": 7, "label": "Open", "displayOrder": 2,
                          "metadata": {"probability": "0.5", "unknown": {"a": 1}},
                          "extra": [1]}], "extra": {"nested": True}}
    read, _, _ = helper(custom_types=["2-9"])
    with patch(NON_CRM_TEST_GET, return_value=response(page(source))) as get:
        rows = read.read_table("pipelines", {})
    assert [r["objectTypeId"] for r in rows] == ["0-3", "0-5", "0-27", "2-9"]
    assert [c.args[0].rsplit("/", 1)[1] for c in get.call_args_list] == [
        "0-3", "0-5", "0-27", "2-9",
    ]
    assert rows[0]["stages"][0]["id"] == "7"
    assert rows[0]["stages"][0]["metadata"]["unknown"] == '{"a":1}'
    assert json.loads(rows[0]["payload"]) == {**source, "objectTypeId": "0-3"}
    _make_type_verifier(read.get_table_schema("pipelines"))(rows[0])
    with patch(NON_CRM_TEST_GET, return_value=response(page())) as get:
        assert read.read_table("pipelines", {"pipeline_object_types": "0-136, 0-421"}) == []
    assert get.call_count == 2
    with pytest.raises(ValueError, match="Unsupported"):
        read.read_table("pipelines", {"pipeline_object_types": "0-999"})


@pytest.mark.parametrize("table,source", [
    ("owners", {"id": 1, "userId": 2, "userIdIncludingInactive": 3,
                "teams": [{"id": 4, "name": "Sales", "primary": True}]}),
    ("forms", {"id": "f", "formType": "future", "configuration": {"a": [1]},
               "fieldGroups": [{"new": True}]}),
    ("contact_lists", contact_list(listVersion=3, additionalProperties={"x": "y"})),
])
def test_consumer_schemas_and_lossless_fields(table, source):
    read, _, _ = helper(page(source))
    if table == "contact_lists":
        rows = read.read_table(table, {})
    else:
        with patch(NON_CRM_TEST_GET, side_effect=[response(page(source)), response(page())]):
            rows = read.read_table(table, {})
    _make_type_verifier(read.get_table_schema(table))(rows[0])
    payload = json.loads(rows[0]["payload"])
    assert all(payload[k] == v for k, v in source.items())
    assert set(rows[0]) == set(read.get_table_schema(table).fieldNames())


@pytest.mark.parametrize("table", HubspotNonCrm.list_tables())
def test_empty_valid_snapshots_and_metadata(table):
    read, _, _ = helper(page())
    with patch(NON_CRM_TEST_GET, return_value=response(page())):
        assert read.read_table(table, {}) == []
    metadata = read.read_table_metadata(table)
    assert metadata["ingestion_type"] == "snapshot"
    assert metadata["cursor_field"] is None
    assert set(metadata["primary_keys"]) <= set(read.get_table_schema(table).fieldNames())


@pytest.mark.parametrize("table,id_field", [
    ("owners", "id"), ("forms", "id"), ("pipelines", "id"),
    ("contact_lists", "listId"), ("contact_list_memberships", "recordId"),
    ("form_submissions", "conversionId"),
])
@pytest.mark.parametrize("bad", [None, True, "", {}, 1.5])
def test_required_id_validation(table, id_field, bad):
    read, _, _ = helper()
    source = {id_field: bad}
    if table == "contact_lists":
        source["objectTypeId"] = "0-1"
    if table == "contact_list_memberships":
        source["listId"] = "1"
    if table == "form_submissions":
        source["formId"] = "f"
    if table == "pipelines":
        source["objectTypeId"] = "0-3"
    with pytest.raises(RuntimeError, match="identity"):
        read._row(table, source)


@pytest.mark.parametrize("data", [{}, {"results": None}, {"results": {}}, page(None)])
def test_missing_or_malformed_results(data):
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, return_value=response(data)):
        with pytest.raises(RuntimeError, match="Malformed"):
            read.read_table("owners", {})


@pytest.mark.parametrize("paging", [None, [], {"next": {}}, {"next": {"after": True}}])
def test_malformed_paging(paging):
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, return_value=response({"results": [], "paging": paging})):
        with pytest.raises(RuntimeError, match="Malformed"):
            read.read_table("owners", {})


@pytest.mark.parametrize("cursors", [["a", "a"], ["a", "b", "a"]])
def test_cursor_cycles(cursors):
    read, _, _ = helper(*(page(after=c) for c in cursors))
    with pytest.raises(RuntimeError, match="cyclic"):
        read.read_table("contact_lists", {})


@pytest.mark.parametrize("status", [403, 404, 500])
def test_error_after_successful_page_aborts_snapshot_without_body_leak(status):
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, side_effect=[
        response(page({"id": "a"}, after="next")), response(status=status),
    ]) as get:
        with pytest.raises(RuntimeError, match=f"HTTP {status}") as error:
            read.read_table("owners", {})
    assert str(error.value) == f"HubSpot non-CRM HTTP {status}"
    assert get.call_count == 2


def test_failed_archived_partition_is_not_skipped():
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, side_effect=[response(page()), response(status=403)]):
        with pytest.raises(RuntimeError, match="403"):
            read.read_table("forms", {})


def test_429_retry_after_and_exhaustion():
    read, _, sleep = helper()
    with patch(NON_CRM_TEST_GET, side_effect=[
        response(status=429, headers={"Retry-After": "2"}),
        response(status=429, headers={"Retry-After": "bad"}),
        response(page()), response(page()),
    ]) as get:
        assert read.read_table("owners", {}) == []
    assert get.call_count == 4
    assert sum(c.args == (2.0,) for c in sleep.call_args_list) == 2
    with patch(NON_CRM_TEST_GET, return_value=response(status=429)) as get:
        with pytest.raises(RuntimeError, match="429"):
            read.read_table("forms", {})
    assert get.call_count == NON_CRM_MAX_RETRIES + 1


def test_processing_list_cannot_claim_complete_memberships():
    read, _, _ = helper(page(contact_list(processingStatus="PROCESSING")))
    with patch(NON_CRM_TEST_GET) as get:
        with pytest.raises(RuntimeError, match="PROCESSING"):
            read.read_table("contact_list_memberships", {})
    get.assert_not_called()


def test_post_failure_after_page_and_noncontact_inventory_fail():
    read, _, _ = helper(page(contact_list(), after="x"), RuntimeError("transport failed"))
    with pytest.raises(RuntimeError, match="transport failed"):
        read.read_table("contact_lists", {})
    read, _, _ = helper(page(contact_list(objectTypeId="0-2")))
    with pytest.raises(RuntimeError, match="Non-contact"):
        read.read_table("contact_lists", {})


def test_conflicting_duplicate_identity_fails():
    read, _, _ = helper(page(contact_list(name="one"), contact_list(name="two")))
    with pytest.raises(RuntimeError, match="Conflicting duplicate"):
        read.read_table("contact_lists", {})


def test_injected_partition_cannot_overwrite_conflicting_source_identity():
    read, _, _ = helper()
    with patch(NON_CRM_TEST_GET, return_value=response(page({
        "id": "p", "objectTypeId": "0-5",
    }))):
        with pytest.raises(RuntimeError, match="partition identity"):
            read.read_table("pipelines", {"pipeline_object_types": "0-3"})


def test_post_missing_results_is_not_empty_snapshot():
    read, _, _ = helper({})
    with pytest.raises(RuntimeError, match="Malformed"):
        read.read_table("contact_lists", {})


def test_membership_schema_retains_timestamp_and_unknown_payload():
    read, _, _ = helper(page(contact_list()))
    source = {"recordId": 42, "listId": 1,
              "membershipTimestamp": "2026-10-28T00:00:00Z",
              "unknown": [1, {"x": True}]}
    with patch(NON_CRM_TEST_GET, return_value=response(page(source))):
        rows = read.read_table("contact_list_memberships", {})
    _make_type_verifier(read.get_table_schema("contact_list_memberships"))(rows[0])
    assert rows[0]["recordId"] == "42"
    assert json.loads(rows[0]["payload"]) == source
    assert rows[0]["listId"] == "1"
