import json
from importlib.resources import files
from pathlib import Path

import yaml

from databricks.labs.community_connector.libs.spec_parser import PipelineSpec
from databricks.labs.community_connector.sources.bluesky.options import TABLE_OPTIONS


def test_packaged_spec_matches_options():
    spec = yaml.safe_load(
        files("databricks.labs.community_connector.sources.bluesky")
        .joinpath("connector_spec.yaml")
        .read_text()
    )
    assert spec["display_name"] == "Bluesky"
    assert set(spec["external_options_allowlist"].split(",")) == TABLE_OPTIONS
    key = next(p for p in spec["connection"]["parameters"] if p["name"] == "api_key")
    assert key["secret"] is True


def test_pipeline_spec_uses_real_framework_schema():
    spec_path = Path(__file__).parent / "fixtures" / "pipeline_spec.json"
    spec = PipelineSpec.model_validate_json(spec_path.read_text())
    assert spec.objects[0].table.source_table == "events"
    assert spec.objects[0].table.table_configuration["scd_type"] == "APPEND_ONLY"
    assert json.loads(spec.model_dump_json())["connection_name"] == "bluesky_jetstream"
