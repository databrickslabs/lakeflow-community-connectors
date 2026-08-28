"""Unit tests for the managed ingestion pipeline body builders."""

import pytest

from databricks.labs.community_connector_cli.managed_pipeline import (
    ManagedPipelineSpecError,
    REGISTER_PYTHON_DATA_SOURCE_KEY,
    SOURCE_TYPE_COMMUNITY,
    augment_full_pipeline_body,
    build_bare_pipeline_body,
    build_ingestion_definition,
    is_full_pipeline_spec,
    validate_ingestion_definition,
)


class TestIsFullPipelineSpec:
    def test_bare_spec_is_not_full(self):
        assert not is_full_pipeline_spec(
            {"connection_name": "c", "objects": [{"table": {"source_table": "t"}}]}
        )

    def test_full_spec_detected(self):
        assert is_full_pipeline_spec({"name": "p", "ingestion_definition": {"objects": []}})


class TestValidateIngestionDefinition:
    def test_valid_table(self):
        validate_ingestion_definition({"objects": [{"table": {"source_table": "t"}}]})

    def test_valid_schema_object(self):
        validate_ingestion_definition({"objects": [{"schema": {"source_schema": "s"}}]})

    def test_missing_objects_allowed(self):
        # An empty pipeline (connection only, no tables) is valid.
        validate_ingestion_definition({"connection_name": "c"})

    def test_empty_objects_allowed(self):
        validate_ingestion_definition({"objects": []})

    def test_objects_wrong_type(self):
        with pytest.raises(ManagedPipelineSpecError):
            validate_ingestion_definition({"objects": "not-a-list"})

    def test_object_without_selector(self):
        with pytest.raises(ManagedPipelineSpecError):
            validate_ingestion_definition({"objects": [{"foo": "bar"}]})


class TestBuildIngestionDefinition:
    def test_injects_source_type_and_connection(self):
        result = build_ingestion_definition(
            {"objects": [{"table": {"source_table": "commits"}}]},
            connection_name="my_conn", catalog="cat", schema="sch",
        )
        assert result["source_type"] == SOURCE_TYPE_COMMUNITY
        assert result["connection_name"] == "my_conn"

    def test_spec_connection_name_wins(self):
        result = build_ingestion_definition(
            {"connection_name": "spec_conn", "objects": [{"table": {"source_table": "t"}}]},
            connection_name="cli_conn", catalog=None, schema=None,
        )
        assert result["connection_name"] == "spec_conn"

    def test_backfills_destinations_and_source_schema(self):
        result = build_ingestion_definition(
            {"objects": [{"table": {"source_table": "commits"}}]},
            connection_name="c", catalog="cat", schema="sch",
        )
        table = result["objects"][0]["table"]
        assert table["destination_catalog"] == "cat"
        assert table["destination_schema"] == "sch"
        assert table["source_schema"] == "default"

    def test_per_table_destination_wins(self):
        result = build_ingestion_definition(
            {"objects": [{"table": {
                "source_table": "commits",
                "destination_catalog": "override_cat",
                "source_schema": "custom",
            }}]},
            connection_name="c", catalog="cat", schema="sch",
        )
        table = result["objects"][0]["table"]
        assert table["destination_catalog"] == "override_cat"
        assert table["destination_schema"] == "sch"
        assert table["source_schema"] == "custom"

    def test_does_not_mutate_input(self):
        spec = {"objects": [{"table": {"source_table": "t"}}]}
        build_ingestion_definition(spec, "c", "cat", "sch")
        assert spec == {"objects": [{"table": {"source_table": "t"}}]}

    def test_preserves_connector_options(self):
        opts = {"community_connector_options": {"options": {"owner": "o", "repo": "r"}}}
        result = build_ingestion_definition(
            {"objects": [{"table": {"source_table": "commits", "connector_options": opts}}]},
            connection_name="c", catalog="cat", schema="sch",
        )
        assert result["objects"][0]["table"]["connector_options"] == opts


class TestBuildBarePipelineBody:
    def test_full_body_shape(self):
        body = build_bare_pipeline_body(
            {"connection_name": "c", "objects": [{"table": {"source_table": "t"}}]},
            pipeline_name="my_pipeline", connection_name=None,
            catalog="cat", schema="sch",
            dependencies=["/Volumes/a/b/v/w.whl"],
            channel="PREVIEW", serverless=True,
        )
        assert body["name"] == "my_pipeline"
        assert body["catalog"] == "cat"
        assert body["schema"] == "sch"
        assert body["channel"] == "PREVIEW"
        assert body["serverless"] is True
        assert body["configuration"][REGISTER_PYTHON_DATA_SOURCE_KEY] == "true"
        assert body["environment"]["dependencies"] == ["/Volumes/a/b/v/w.whl"]
        assert body["ingestion_definition"]["source_type"] == SOURCE_TYPE_COMMUNITY

    def test_no_dependencies_omits_environment(self):
        body = build_bare_pipeline_body(
            {"objects": [{"table": {"source_table": "t"}}]},
            pipeline_name="p", connection_name="c",
            catalog=None, schema=None, dependencies=None,
        )
        assert "environment" not in body
        assert "catalog" not in body

    def test_merge_onto_base_preserves_unmanaged_fields(self):
        base = {
            "name": "p",
            "catalog": "cat",
            "schema": "sch",
            "channel": "CURRENT",
            "serverless": False,
            "tags": {"team": "data"},
            "environment": {"dependencies": ["/old.whl"]},
        }
        body = build_bare_pipeline_body(
            {"objects": [{"table": {"source_table": "t"}}]},
            pipeline_name="p", connection_name="c",
            catalog=None, schema=None, dependencies=["/new.whl"],
            channel=None, serverless=None, base=base,
        )
        # Unmanaged fields survive; channel/serverless not overridden.
        assert body["tags"] == {"team": "data"}
        assert body["channel"] == "CURRENT"
        assert body["serverless"] is False
        # Managed additions applied; deps replaced.
        assert body["configuration"][REGISTER_PYTHON_DATA_SOURCE_KEY] == "true"
        assert body["environment"]["dependencies"] == ["/new.whl"]
        assert body["ingestion_definition"]["source_type"] == SOURCE_TYPE_COMMUNITY

    def test_merge_does_not_mutate_base(self):
        base = {"name": "p", "tags": {"t": "v"}, "configuration": {}}
        build_bare_pipeline_body(
            {"objects": []}, pipeline_name="p", connection_name="c",
            catalog=None, schema=None, dependencies=None, base=base,
        )
        assert base == {"name": "p", "tags": {"t": "v"}, "configuration": {}}


class TestAugmentFullPipelineBody:
    def _spec(self, **extra):
        base = {
            "name": "p",
            "catalog": "cat",
            "schema": "sch",
            "ingestion_definition": {
                "connection_name": "c",
                "objects": [{"table": {"source_table": "t"}}],
            },
        }
        base.update(extra)
        return base

    def test_adds_config_flag_and_source_type(self):
        body = augment_full_pipeline_body(self._spec(), dependencies=["/v/w.whl"])
        assert body["configuration"][REGISTER_PYTHON_DATA_SOURCE_KEY] == "true"
        assert body["ingestion_definition"]["source_type"] == SOURCE_TYPE_COMMUNITY
        assert body["environment"]["dependencies"] == ["/v/w.whl"]

    def test_existing_environment_preserved_no_deps_added(self):
        spec = self._spec(environment={"dependencies": ["/existing/w.whl"]})
        body = augment_full_pipeline_body(spec, dependencies=["/new/w.whl"])
        assert body["environment"]["dependencies"] == ["/existing/w.whl"]

    def test_existing_config_flag_preserved(self):
        spec = self._spec(configuration={REGISTER_PYTHON_DATA_SOURCE_KEY: "false", "x": "y"})
        body = augment_full_pipeline_body(spec, dependencies=None)
        assert body["configuration"][REGISTER_PYTHON_DATA_SOURCE_KEY] == "false"
        assert body["configuration"]["x"] == "y"

    def test_cli_options_applied_when_provided(self):
        body = augment_full_pipeline_body(
            self._spec(), pipeline_name="override", catalog="newcat",
            connection_name="newconn", dependencies=None,
        )
        assert body["name"] == "override"
        assert body["catalog"] == "newcat"
        assert body["ingestion_definition"]["connection_name"] == "newconn"

    def test_cli_options_omitted_leaves_spec(self):
        body = augment_full_pipeline_body(self._spec(), dependencies=None)
        assert body["name"] == "p"
        assert body["catalog"] == "cat"
        assert body["ingestion_definition"]["connection_name"] == "c"

    def test_does_not_mutate_input(self):
        spec = self._spec()
        augment_full_pipeline_body(spec, dependencies=["/v/w.whl"])
        assert "environment" not in spec
        assert REGISTER_PYTHON_DATA_SOURCE_KEY not in spec.get("configuration", {})
