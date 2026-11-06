"""Exercise each snapshot through the batch reader using source and generated connectors."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import _make_type_verifier

from databricks.labs.community_connector.sources.hubspot import hubspot as source_module
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowBatchReader
from tests.unit.sources.hubspot.test_hubspot_custom_objects import _connector_class

TABLES = (
    "owners", "pipelines", "forms", "contact_lists",
    "contact_list_memberships", "form_submissions",
)


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("table", TABLES)
def test_snapshot_consumer_nested_rows_and_empty_rescan(generated, table):
    corpus = Path(source_module.__file__).parents[2] / "source_simulator/specs/hubspot/corpus"
    fixtures = {name: json.loads((corpus / f"{name}.json").read_text()) for name in TABLES}
    empty = False

    def response_for(url, **_kwargs):
        if "/memberships" in url:
            name = "contact_list_memberships"
        elif "/submissions/forms/" in url:
            name = "form_submissions"
        elif "/pipelines/" in url:
            name = "pipelines"
        elif "/owners/" in url:
            name = "owners"
        elif "/lists/" in url:
            name = "contact_lists"
        else:
            assert url.endswith("/marketing/v3/forms"), url
            name = "forms"
        return Mock(status_code=200, json=lambda: {"results": [] if empty else fixtures[name]})

    conn = _connector_class(generated)({"access_token": "offline"})
    conn._non_crm._sleep = lambda _: None
    conn._history._sleep = lambda _: None
    options = {"tableName": table, "pipeline_object_types": "0-3"}
    schema = conn.get_table_schema(table, options)
    metadata = conn.read_table_metadata(table, options)
    assert metadata["ingestion_type"] == "snapshot"
    assert metadata["cursor_field"] is None
    # The framework reader converts connector dictionaries to Spark tuples.
    reader = LakeflowBatchReader(options, schema, conn)
    with patch("requests.get", side_effect=response_for), patch("requests.post", side_effect=response_for):
        rows = list(reader.read(InputPartition(None)))
        assert len(rows) == 1
        _make_type_verifier(schema)(rows[0])
        payload = json.loads(rows[0][schema.fieldNames().index("payload")])
        assert all(payload[key] == value for key, value in fixtures[table][0].items())
        for key in metadata["primary_keys"]:
            assert rows[0][schema.fieldNames().index(key)] is not None
        empty = True
        assert list(reader.read(InputPartition(None))) == []


@pytest.mark.parametrize("generated", [False, True])
def test_membership_batch_reader_lazily_drains_every_page(generated):
    list_row = {
        "listId": "list-1", "objectTypeId": "0-1", "processingType": "MANUAL",
        "processingStatus": "COMPLETE",
    }
    membership_calls = 0

    def post(url, **_kwargs):
        assert url.endswith("/crm/lists/2026-09/all")
        return Mock(status_code=200, json=lambda: {"results": [list_row]})

    def get(url, **kwargs):
        nonlocal membership_calls
        assert url.endswith("/crm/lists/2026-09/list-1/memberships")
        membership_calls += 1
        start = (membership_calls - 1) * 100
        count = 100 if membership_calls < 3 else 5
        payload = {"results": [
            {"recordId": str(index)} for index in range(start, start + count)
        ]}
        if membership_calls < 3:
            payload["paging"] = {"next": {"after": f"page-{membership_calls}"}}
        if membership_calls > 1:
            assert kwargs["params"]["after"] == f"page-{membership_calls - 1}"
        return Mock(status_code=200, json=lambda: payload)

    options = {
        "tableName": "contact_list_memberships", "max_records_per_batch": "1",
    }
    conn = _connector_class(generated)({"access_token": "offline"})
    conn._non_crm._sleep = lambda _: None
    schema = conn.get_table_schema("contact_list_memberships", options)
    reader = LakeflowBatchReader(options, schema, conn)
    with patch("requests.get", side_effect=get), patch("requests.post", side_effect=post):
        rows = reader.read(InputPartition(None))
        assert membership_calls == 0
        first = next(rows)
        assert first.listId == "list-1" and first.recordId == "0"
        assert membership_calls == 1
        assert 1 + sum(1 for _ in rows) == 205
        assert membership_calls == 3
