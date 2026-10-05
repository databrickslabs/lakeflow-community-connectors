from importlib.resources import files

import yaml

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
