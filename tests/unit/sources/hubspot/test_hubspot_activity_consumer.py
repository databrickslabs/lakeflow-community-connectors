"""Nonempty engagement fields must survive the actual source/generated consumer."""

import copy
import json
import runpy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from pyspark.sql.types import _make_type_verifier

from databricks.labs.community_connector.sources.hubspot import hubspot as module
from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowStreamReader

STAMP = "2026-01-02T00:00:00.000Z"
FIELDS = {
    "calls": {"hs_call_body": "Call résumé ☎", "hs_call_duration": "1234",
              "hs_call_disposition": "disposition-guid", "hs_call_recording_url": "https://example.invalid/call"},
    "emails": {"hs_email_text": "Text résumé", "hs_email_html": "<p>HTML résumé</p>",
               "hs_email_headers": json.dumps({"from": {"email": "from@example.invalid"},
                                               "cc": [{"email": "cc@example.invalid"}]})},
    "meetings": {"hs_meeting_body": "Public description", "hs_internal_meeting_notes": "Private notes",
                 "hs_meeting_transcript": "transcript-id", "hs_meeting_summary": "Summary"},
    "tasks": {"hs_task_subject": "Follow up", "hs_task_body": "Task résumé",
              "hs_task_status": "WAITING", "hs_timestamp": "2030-01-01T00:00:00Z"},
    "notes": {"hs_note_body": "Résumé 📝 " * 4096, "hs_attachment_ids": "10;20"},
}


class ActivityAPI:
    def __init__(self, table, number="42.125"):
        self.table = table
        self.properties = {**FIELDS[table], "extra_decimal_metric": number,
                           "extra_boolean": "false", "extra_future_field": "preserve me",
                           "hs_lastmodifieddate": STAMP}
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append(("GET", url, kwargs))
        if url.endswith("/schemas"):
            payload = {"results": []}
        elif "/crm/properties/2026-09/" in url:
            payload = {"results": [{"name": name, "type": (
                "number" if name in {"extra_decimal_metric", "hs_call_duration"} else
                "bool" if name == "extra_boolean" else "string"
            )} for name in self.properties]}
        else:
            assert urlparse(url).path == f"/crm/objects/2026-09/{self.table}"
            payload = {"results": [self.parent()]}
        return Mock(status_code=200, json=lambda: copy.deepcopy(payload))

    def parent(self):
        return {"id": "1", "createdAt": STAMP, "updatedAt": STAMP,
                "archived": False, "properties": self.properties}

    def post(self, url, **kwargs):
        self.requests.append(("POST", url, kwargs))
        if url.endswith("/search"):
            payload = {"results": [self.parent()]}
        else:
            assert "/crm/associations/2026-09/" in url
            payload = {"status": "COMPLETE", "results": [
                {"from": {"id": item["id"]}, "to": []} for item in kwargs["json"]["inputs"]
            ]}
        return Mock(status_code=200, json=lambda: copy.deepcopy(payload))


def consumer(table, generated):
    options = {"access_token": "offline", "tableName": table}
    if generated:
        registry = Mock()
        path = Path(module.__file__).with_name("_generated_hubspot_python_source.py")
        runpy.run_path(str(path))["register_lakeflow_source"](SimpleNamespace(dataSource=registry))
        source = registry.register.call_args.args[0](options)
        conn = source.lakeflow_connect
        schema = conn.get_table_schema(table, options)
        stream = source.simpleStreamReader(schema)
    else:
        conn = HubspotLakeflowConnect(options)
        schema = conn.get_table_schema(table, options)
        stream = LakeflowStreamReader(options, schema, conn)
    conn._history._sleep = lambda _: None
    return conn, schema, stream


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("incremental", [False, True])
@pytest.mark.parametrize("table", list(FIELDS))
def test_nonempty_activity_fields_survive_reader(table, incremental, generated):
    api = ActivityAPI(table)
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, schema, stream = consumer(table, generated)
        start = {"updatedAt": "2026-01-01T00:00:00.000Z"} if incremental else {}
        rows, end = stream.read(start)
        rows = list(rows)
    assert len(rows) == 1 and end != start
    _make_type_verifier(schema)(rows[0])
    properties = rows[0].properties.asDict()
    assert properties["extra_decimal_metric"] == Decimal("42.125")
    assert properties["extra_boolean"] is False
    assert properties["extra_future_field"] == "preserve me"
    for field, value in FIELDS[table].items():
        assert properties[field] == (Decimal(value) if field == "hs_call_duration" else value)
    if incremental and table != "meetings":
        body = next(kwargs["json"] for method, url, kwargs in api.requests if url.endswith("/search"))
        assert set(api.properties) <= set(body["properties"])
        assert all(item["propertyName"] == "hs_lastmodifieddate"
                   for group in body["filterGroups"] for item in group["filters"])
    else:
        url = next(url for method, url, _ in api.requests
                   if urlparse(url).path == f"/crm/objects/2026-09/{table}")
        requested = parse_qs(urlparse(url).query)["properties"][0].split(",")
        assert set(api.properties) <= set(requested)


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("number", ["NaN", "Infinity", "bad", "0.0000000000000000001", "100000000000000000000"])
def test_unrepresentable_numeric_property_fails_before_checkpoint(generated, number):
    api = ActivityAPI("meetings", number=number)
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _, _ = consumer("meetings", generated)
        start = {"updatedAt": "2026-01-01T00:00:00.000Z"}
        with pytest.raises((ValueError, RuntimeError), match="numeric|decimal|number"):
            conn.read_table("meetings", start, {})
    assert start == {"updatedAt": "2026-01-01T00:00:00.000Z"}


@pytest.mark.parametrize("generated", [False, True])
def test_maximum_decimal_is_exact_at_consumer(generated):
    value = "99999999999999999999.999999999999999999"
    api = ActivityAPI("meetings", value)
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        _, schema, stream = consumer("meetings", generated)
        rows, _ = stream.read({})
        row = list(rows)[0]
    _make_type_verifier(schema)(row)
    assert row.properties.extra_decimal_metric == Decimal(value)


@pytest.mark.parametrize("generated", [False, True])
def test_excluded_invalid_number_does_not_fail_read(generated):
    api = ActivityAPI("meetings", "NaN")
    with patch("requests.get", side_effect=api.get), patch("requests.post", side_effect=api.post):
        conn, _, _ = consumer("meetings", generated)
        rows, end = conn.read_table("meetings", {}, {
            "exclude_properties": "extra_decimal_metric"})
        assert len(list(rows)) == 1 and end
