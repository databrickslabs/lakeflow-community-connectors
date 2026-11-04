"""HubSpot-only managed CLI envelope normalization."""

import copy
import json

import pytest

from databricks.labs.community_connector_cli import managed_pipeline as managed


def body(options=None):
    return {"name": "p", "environment": {"dependencies": [
        "/Volumes/c/s/v/lakeflow_community_connectors_hubspot-0.1.0-py3-none-any.whl"
    ]}, "ingestion_definition": {"source_type": "COMMUNITY", "objects": [
        {"table": {"source_table": name, "destination_table": "dest_" + name,
                   "connector_options": {"community_connector_options": {"options": opts}}}}
        for name, opts in (options or [("calls", {"include_properties": "hs_call_body"}),
                                      ("notes", {})])
    ]}}


def normalize(value, source_name=None):
    return managed.normalize_hubspot_table_configs(value, source_name)


def test_shared_map_and_idempotency_preserve_original():
    original = body()
    before = copy.deepcopy(original)
    result = normalize(original)
    objects = result["ingestion_definition"]["objects"]
    maps = [obj["table"]["connector_options"]["community_connector_options"]["options"]["tableConfigs"]
            for obj in objects]
    assert maps[0] == maps[1]
    assert json.loads(maps[0]) == {"calls": {"include_properties": "hs_call_body"}, "notes": {}}
    assert original == before
    assert result["environment"] == original["environment"]
    assert normalize(result) == result
    assert [o["table"]["destination_table"] for o in objects] == ["dest_calls", "dest_notes"]


def test_identity_is_explicit_or_exact_wheel_not_table_names():
    value = body()
    value["environment"]["dependencies"] = ["renamed.whl"]
    assert normalize(value) == value
    assert "tableConfigs" in normalize(value, "hubspot")["ingestion_definition"]["objects"][0]["table"]["connector_options"]["community_connector_options"]["options"]
    assert normalize(body(), "github") == body()


@pytest.mark.parametrize("objects", [
    [{"schema": {"source_schema": "default"}}],
    [{"report": {"source_report": "x"}}],
    [{"table": {"source_table": "calls", "source_schema": "other"}}],
    [{"table": {"source_table": ""}}],
])
def test_hubspot_rejects_ambiguous_selectors(objects):
    value = body()
    value["ingestion_definition"]["objects"] = objects
    with pytest.raises(managed.ManagedPipelineSpecError, match="HubSpot"):
        normalize(value)


@pytest.mark.parametrize("options", [
    [("calls", {"access_token": "SECRET"})],
    [("calls", {"tableName": "notes"})],
    [("calls", {"include_properties": True})],
    [("calls", {"tableConfigs": "SECRET"})],
    [("calls", {"tableConfigs": '{"calls":{},"calls":{}}'})],
    [("calls", {"include_properties": "a"}), ("calls", {"include_properties": "b"})],
    [("calls", {"tableConfigs": '{"calls":{},"notes":{}}'})],
])
def test_cli_rejects_invalid_or_conflicting_envelope(options):
    with pytest.raises(managed.ManagedPipelineSpecError, match="HubSpot") as error:
        normalize(body(options))
    assert "SECRET" not in str(error.value)


@pytest.mark.parametrize("full", [False, True])
@pytest.mark.parametrize("update", [False, True])
def test_cli_common_builder_normalizes_full_bare_create_update(full, update):
    from unittest.mock import patch
    from databricks.labs.community_connector_cli.cli import _build_managed_pipeline_body

    original = body()
    spec = original if full else original["ingestion_definition"]
    before = copy.deepcopy(spec)
    with patch("databricks.labs.community_connector_cli.cli._managed_dependencies",
               return_value=original["environment"]["dependencies"]):
        result = _build_managed_pipeline_body(
            None, spec, "p", None, None, None, None, None, (), False,
            build_wheels=False, base_spec=original if update else None,
        )
    assert spec == before
    assert result == normalize(result)
    for obj in result["ingestion_definition"]["objects"]:
        assert "tableConfigs" in obj["table"]["connector_options"]["community_connector_options"]["options"]


def test_order_empty_options_and_same_duplicate_destinations():
    original = body([("calls", {}), ("notes", {"include_properties": "hs_note_body"}), ("calls", {})])
    first = normalize(original)
    original["ingestion_definition"]["objects"].reverse()
    second = normalize(original)
    get_map = lambda value: value["ingestion_definition"]["objects"][0]["table"]["connector_options"]["community_connector_options"]["options"]["tableConfigs"]
    assert get_map(first) == get_map(second)
    assert json.loads(get_map(first))["calls"] == {}


@pytest.mark.parametrize("shape", [None, [], "SECRET"])
def test_cli_rejects_malformed_nested_options(shape):
    value = body()
    value["ingestion_definition"]["objects"][0]["table"]["connector_options"]["community_connector_options"]["options"] = shape
    with pytest.raises(managed.ManagedPipelineSpecError, match="HubSpot") as error:
        normalize(value)
    assert "SECRET" not in str(error.value)


def test_cli_whitelist_and_merged_connection_allowlist():
    from pathlib import Path
    import yaml
    from databricks.labs.community_connector_cli.cli import (
        _get_constant_external_options_allowlist, _merge_external_options_allowlist,
    )

    spec = yaml.safe_load((Path(__file__).parents[3] / "src/databricks/labs/community_connector/sources/hubspot/connector_spec.yaml").read_text())
    declared = set(spec["external_options_allowlist"].split(","))
    assert managed._HUBSPOT_TABLE_OPTION_KEYS == declared
    merged = _merge_external_options_allowlist(spec["external_options_allowlist"],
                                              _get_constant_external_options_allowlist())
    assert "tableConfigs" in merged.split(",")
    assert "tableConfigs" not in declared
