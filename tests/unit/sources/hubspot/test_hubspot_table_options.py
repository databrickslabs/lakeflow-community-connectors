"""Per-table envelopes must survive a different object's flat managed options."""

import json
from unittest import mock

import pytest

from tests.unit.sources.hubspot.test_hubspot_custom_objects import _connector_class


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("entry", ["schema", "metadata", "read", "deletes", "replay"])
def test_spi_resolves_before_dispatch(generated, entry):
    conn = _connector_class(generated)({"access_token": "constructor-only"})
    selected = {"history_properties": "hs_call_body", "include_properties": "hs_call_body"}
    options = {"include_properties": "hs_note_body", "tableConfigs": json.dumps({
        "calls_property_history": selected, "notes": {"include_properties": "hs_note_body"},
    })}
    name = "calls_property_history"
    with mock.patch.object(conn, "list_tables", return_value=[name]), mock.patch.object(
        conn._history, "get_table_schema", return_value="schema"
    ), mock.patch.object(conn._history, "get_table_metadata", return_value={}), mock.patch.object(
        conn._history, "read_table", return_value=([], {})
    ) as read, mock.patch.object(conn._history, "read_between_offsets", return_value=[]) as replay:
        if entry == "schema":
            conn.get_table_schema(name, options)
        elif entry == "metadata":
            conn.read_table_metadata(name, options)
        elif entry == "read":
            conn.read_table(name, {}, options)
            assert read.call_args.args[2] == selected
        elif entry == "deletes":
            assert list(conn.read_table_deletes(name, {}, options)[0]) == []
        else:
            conn.read_table_between_offsets(name, {}, {}, options)
            assert replay.call_args.args[3] == selected
        # Structural rejection must occur before even early history dispatch.
        with pytest.raises(ValueError, match="HubSpot tableConfigs"):
            bad = {"tableConfigs": '{"notes":{}}'}
            if entry == "schema":
                conn.get_table_schema(name, bad)
            elif entry == "metadata":
                conn.read_table_metadata(name, bad)
            elif entry == "read":
                conn.read_table(name, {}, bad)
            elif entry == "deletes":
                conn.read_table_deletes(name, {}, bad)
            else:
                conn.read_table_between_offsets(name, {}, {}, bad)


@pytest.mark.parametrize("generated", [False, True])
def test_authoritative_empty_copy_and_idempotency(generated):
    conn = _connector_class(generated)({"access_token": "constructor-only"})
    options = {"include_properties": "wrong", "association_sync_mode": "full_refresh",
               "tableConfigs": '{"calls":{},"notes":{"include_properties":"hs_note_body"}}'}
    selected = conn._resolve_table_options("calls", options)
    assert selected == {}
    assert conn._resolve_table_options("calls", selected) == {}
    assert options["include_properties"] == "wrong"
    flat = {"include_properties": "hs_call_body"}
    assert conn._resolve_table_options("calls", flat) == flat
    assert conn._resolve_table_options("calls", flat) is not flat


@pytest.mark.parametrize("raw", [
    '', 'SECRET', 'null', '[]', '{}', '{"calls":null}', '{"calls":[]}',
    '{"calls":{"include_properties":true}}', '{"calls":{"include_properties":1}}',
    '{"calls":{},"calls":{}}',
    '{"calls":{"include_properties":"a","include_properties":"b"}}',
    '{"":{}}', '{"calls":{},"notes":{"access_token":"SECRET"}}',
    '{"calls":{"tableName":"notes"}}', '{"calls":{"tableConfigs":"{}"}}',
    '{"calls":{"base_url":"SECRET"}}', '{"calls":{"operation":"read_table"}}',
    '{"calls":{"unknown":"SECRET"}}', 1, {}, None,
])
@pytest.mark.parametrize("generated", [False, True])
def test_invalid_envelope_is_sanitized_and_fail_closed(raw, generated):
    conn = _connector_class(generated)({"access_token": "constructor-only"})
    with pytest.raises(ValueError, match="HubSpot tableConfigs") as error:
        conn._resolve_table_options("calls", {"tableConfigs": raw})
    assert "SECRET" not in str(error.value)
    assert error.value.__cause__ is None


@pytest.mark.parametrize("generated", [False, True])
def test_crm_schema_metadata_and_reads_use_selected_filters(generated):
    conn = _connector_class(generated)({"access_token": "constructor-only"})
    properties = [{"name": name, "type": "string"} for name in (
        "hs_call_body", "hs_note_body", "hs_lastmodifieddate"
    )]
    selected = {"include_properties": "hs_call_body"}
    options = {"include_properties": "hs_note_body", "tableConfigs": json.dumps({
        "calls": selected, "notes": {"include_properties": "hs_note_body"},
    })}
    with mock.patch.object(conn, "list_tables", return_value=["calls", "notes"]), mock.patch.object(
        conn, "_get_object_properties", return_value=properties
    ), mock.patch.object(conn, "_read_data", return_value=([], {})) as read:
        schema = conn.get_table_schema("calls", options)
        assert schema["properties"].dataType.fieldNames() == ["hs_call_body", "hs_lastmodifieddate"]
        assert conn.read_table_metadata("calls", options)["property_names"] == [
            "hs_call_body", "hs_lastmodifieddate"
        ]
        conn.read_table("calls", {"updatedAt": "2024-01-01"}, options)
        assert read.call_args.kwargs["table_options"] == selected
        assert read.call_args.kwargs["incremental"] is True
        conn.read_table_between_offsets("calls", {}, {}, options)
        assert read.call_args.kwargs["table_options"] == selected
        other = {"tableConfigs": '{"calls":{"include_properties":"hs_note_body"}}'}
        assert "hs_note_body" in conn.get_table_schema("calls", other)["properties"].dataType.fieldNames()


@pytest.mark.parametrize("generated", [False, True])
def test_spark_lowercased_envelope_and_conflicting_case_variants(generated):
    from pyspark.sql.datasource import CaseInsensitiveDict

    conn = _connector_class(generated)({"access_token": "constructor-only"})
    options = CaseInsensitiveDict({"tableConfigs": '{"calls":{}}', "include_properties": "wrong"})
    assert conn._resolve_table_options("calls", options) == {}
    assert conn._resolve_table_options("calls", dict(options)) == {}
    with pytest.raises(ValueError, match="HubSpot tableConfigs"):
        conn._resolve_table_options("calls", {"tableConfigs": '{"calls":{}}',
                                               "tableconfigs": '{"calls":{}}'})


@pytest.mark.parametrize("generated", [False, True])
def test_five_tables_and_consumer_paths_stay_isolated(generated):
    from pyspark.sql.datasource import InputPartition
    from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
        ReadTableOp, _default_get_object_metadata,
    )
    from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
        LakeflowBatchReader, LakeflowStreamReader,
    )

    conn = _connector_class(generated)({"access_token": "constructor-only"})
    bodies = {"calls": "hs_call_body", "emails": "hs_email_text", "meetings": "hs_meeting_body",
              "tasks": "hs_task_body", "notes": "hs_note_body"}
    configs = {name: {"include_properties": prop} for name, prop in bodies.items()}
    properties = [{"name": prop, "type": "string"} for prop in [*bodies.values(), "hs_lastmodifieddate"]]
    opts = {"include_properties": "hs_note_body", "association_sync_mode": "full_refresh",
            "tableConfigs": json.dumps(configs)}
    with mock.patch.object(conn, "list_tables", return_value=list(bodies)), mock.patch.object(
        conn, "_get_object_properties", return_value=properties
    ), mock.patch.object(conn, "_read_data", return_value=([], {})) as read:
        for name in [*bodies, *reversed(bodies)]:
            options = {**opts, "tableName": name}
            schema = conn.get_table_schema(name, options)
            assert schema["properties"].dataType.fieldNames() == [bodies[name], "hs_lastmodifieddate"]
            metadata = conn.read_table_metadata(name, options)
            assert metadata["ingestion_type"] == ("snapshot" if name == "meetings" else "cdc_with_deletes")
            assert list(LakeflowBatchReader(options, schema, conn).read(InputPartition(None))) == []
            list(LakeflowStreamReader(options, schema, conn).read({})[0])
            list(LakeflowStreamReader(options, schema, conn).readBetweenOffsets({}, {}))
            op = ReadTableOp()
            assert op.resolve_schema(conn, options) == schema
            assert list(op.pull(conn, options)) == []
            agent_metadata = dict((row["key"], row["value"]) for row in
                                  _default_get_object_metadata(conn, table_name=name, table_options=options))
            assert agent_metadata["ingestion_type"] == metadata["ingestion_type"]
            assert read.call_args.kwargs["table_options"] == configs[name]
        # Delete reads must select the calls entry too, not only ignore wrong values.
        with mock.patch.object(conn, "_fetch_full_refresh_batch", return_value=([], None)) as fetch:
            conn.read_table_deletes("calls", {}, opts)
            assert fetch.call_args.args[1] == ["hs_call_body", "hs_lastmodifieddate"]


def test_whitelist_matches_declared_connector_options():
    from pathlib import Path
    import yaml
    from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect

    spec = yaml.safe_load((Path(__file__).parents[4] / "src/databricks/labs/community_connector/sources/hubspot/connector_spec.yaml").read_text())
    assert HubspotLakeflowConnect._TABLE_OPTION_KEYS == set(spec["external_options_allowlist"].split(","))


def _source_class(generated):
    if generated:
        import runpy
        from pathlib import Path
        path = Path(__file__).parents[4] / "src/databricks/labs/community_connector/sources/hubspot/_generated_hubspot_python_source.py"
        from types import SimpleNamespace
        registry = mock.Mock()
        runpy.run_path(str(path))["register_lakeflow_source"](SimpleNamespace(dataSource=registry))
        return registry.register.call_args.args[0]
    from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect
    from databricks.labs.community_connector.sparkpds.lakeflow_datasource import LakeflowSource

    class HubspotSource(LakeflowSource):
        _lakeflow_connect_cls = HubspotLakeflowConnect

    return HubspotSource


@pytest.mark.parametrize("generated", [False, True])
@pytest.mark.parametrize("raw", ['SECRET', '{"calls":{},"calls":{}}',
                                '{"calls":{"access_token":"SECRET"}}'])
def test_virtual_metadata_validates_before_shared_decoder(generated, raw):
    from pyspark.sql.datasource import CaseInsensitiveDict

    options = CaseInsensitiveDict({"access_token": "constructor-only",
                                   "tableName": "_community_table_metadata",
                                   "tableNameList": '["calls"]', "tableConfigs": raw})
    with pytest.raises(ValueError, match="HubSpot tableConfigs") as error:
        _source_class(generated)(options)
    assert "SECRET" not in str(error.value)
    assert error.value.__cause__ is None


@pytest.mark.parametrize("generated", [False, True])
def test_virtual_metadata_missing_entry_is_not_default_options(generated):
    from pyspark.sql.datasource import CaseInsensitiveDict, InputPartition

    source = _source_class(generated)(CaseInsensitiveDict({"access_token": "constructor-only",
        "tableName": "_community_table_metadata", "tableNameList": '["calls"]',
        "tableConfigs": '{"notes":{}}'}))
    with mock.patch.object(source.lakeflow_connect, "list_tables", return_value=["calls"]), mock.patch.object(
        source.lakeflow_connect, "_get_object_properties", return_value=[]
    ), pytest.raises(ValueError, match="HubSpot tableConfigs"):
        list(source.reader(source.schema()).read(InputPartition(None)))


@pytest.mark.parametrize("generated", [False, True])
def test_constructor_map_survives_adapter_extraction_and_rejects_replacement(generated):
    conn = _connector_class(generated)({"access_token": "constructor-only",
        "tableconfigs": '{"calls":{},"notes":{"include_properties":"hs_note_body"}}'})
    assert conn._resolve_table_options("calls", {"include_properties": "wrong"}) == {}
    assert conn._resolve_table_options("notes", {}) == {"include_properties": "hs_note_body"}
    with pytest.raises(ValueError, match="HubSpot tableConfigs"):
        conn._resolve_table_options("calls", {"tableConfigs": '{"calls":{"include_properties":"other"}}'})
    with pytest.raises(ValueError, match="HubSpot tableConfigs"):
        conn._resolve_table_options("tasks", {})


@pytest.mark.parametrize("generated", [False, True])
def test_virtual_metadata_empty_and_filtered_entries(generated):
    from pyspark.sql.datasource import CaseInsensitiveDict, InputPartition

    source = _source_class(generated)(CaseInsensitiveDict({"access_token": "constructor-only",
        "tableName": "_community_table_metadata", "tableNameList": '["calls","notes"]',
        "include_properties": "wrong", "tableConfigs": '{"calls":{},"notes":{"include_properties":"hs_note_body"}}'}))
    props = [{"name": name, "type": "string"} for name in
             ["hs_call_body", "hs_note_body", "hs_lastmodifieddate"]]
    with mock.patch.object(source.lakeflow_connect, "list_tables", return_value=["calls", "notes"]), mock.patch.object(
        source.lakeflow_connect, "_get_object_properties", return_value=props
    ):
        schema = source.schema()
        reader = source.reader(schema)
        # The public virtual schema exposes only routing metadata, not the
        # connector's extra property_names field. Inspect its raw producer too.
        raw = reader._read_table_metadata()
        assert raw[0]["property_names"] == ["hs_call_body", "hs_note_body", "hs_lastmodifieddate"]
        assert raw[1]["property_names"] == ["hs_note_body", "hs_lastmodifieddate"]
        rows = list(reader.read(InputPartition(None)))
        index = schema.fieldNames().index("ingestion_type")
        assert [row[index] for row in rows] == ["cdc_with_deletes", "cdc_with_deletes"]


@pytest.mark.parametrize("generated", [False, True])
def test_alternating_maps_flat_and_empty_on_same_legacy_instance(generated):
    conn = _connector_class(generated)({"access_token": "constructor-only"})
    props = [{"name": name, "type": "string"} for name in
             ["hs_call_body", "hs_note_body", "hs_lastmodifieddate"]]
    with mock.patch.object(conn, "list_tables", return_value=["calls"]), mock.patch.object(
        conn, "_get_object_properties", return_value=props
    ):
        cases = [({"tableConfigs": '{"calls":{"include_properties":"hs_call_body"}}'}, ["hs_call_body", "hs_lastmodifieddate"]),
                 ({"include_properties": "hs_note_body"}, ["hs_note_body", "hs_lastmodifieddate"]),
                 ({"tableconfigs": '{"calls":{}}', "include_properties": "wrong"}, [p["name"] for p in props]),
                 ({}, [p["name"] for p in props])]
        for options, expected in cases + list(reversed(cases)):
            assert conn.read_table_metadata("calls", options)["property_names"] == expected
            assert conn.get_table_schema("calls", options)["properties"].dataType.fieldNames() == expected


@pytest.mark.parametrize("generated", [False, True])
def test_ingestion_agent_supported_routing_with_lowercase_envelope(generated):
    from pyspark.sql.datasource import CaseInsensitiveDict
    from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import (
        IngestionAgentDispatcher, _connector_options,
    )

    base = CaseInsensitiveDict({"access_token": "constructor-only", "tableName": "calls",
        "name": "calls", "include_properties": "wrong",
        "tableConfigs": '{"calls":{"include_properties":"hs_call_body"},"notes":{}}'})
    conn = _connector_class(generated)(_connector_options(base))
    props = [{"name": name, "type": "string"} for name in ["hs_call_body", "hs_note_body", "hs_lastmodifieddate"]]
    with mock.patch.object(conn, "list_tables", return_value=["calls"]), mock.patch.object(
        conn, "_get_object_properties", return_value=props
    ), mock.patch.object(conn, "_read_data", return_value=([], {})) as read:
        for operation in ["read_table", "get_object_metadata"]:
            # The dispatcher currently requires canonical tableName after its
            # plain-dict copy. The separate regression below records that limit.
            options = {**base, "tableName": "calls", "operation": operation}
            dispatcher = IngestionAgentDispatcher(options=options, connector=conn, init_error=None)
            schema = dispatcher.schema()
            reader = dispatcher.reader(schema)
            rows = list(reader.read(reader.partitions()[0]))
            if operation == "read_table":
                assert schema["properties"].dataType.fieldNames() == ["hs_call_body", "hs_lastmodifieddate"]
                assert rows == []
                assert read.call_args.kwargs["table_options"] == {"include_properties": "hs_call_body"}
            else:
                assert any(row["key"] == "ingestion_type" and row["value"] == "cdc_with_deletes" for row in rows)


def test_ingestion_agent_preexisting_lowercase_routing_rejected():
    from pyspark.sql.datasource import CaseInsensitiveDict
    from databricks.labs.community_connector.interface import AgentError
    from databricks.labs.community_connector.sparkpds.ingestion_agent_datasource import IngestionAgentDispatcher

    conn = _connector_class(False)({"access_token": "constructor-only"})
    options = CaseInsensitiveDict({"operation": "read_table", "tableName": "calls",
                                  "tableConfigs": '{"calls":{}}'})
    dispatcher = IngestionAgentDispatcher(options=options, connector=conn, init_error=None)
    with pytest.raises(AgentError, match="requires option 'tableName'"):
        dispatcher.schema()
