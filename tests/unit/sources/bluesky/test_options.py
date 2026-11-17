"""Compatibility contracts for Lakeflow option parsing and checkpoint scope."""

import pytest
from pydantic import ValidationError

from databricks.labs.community_connector.sources.bluesky.options import Options


def test_defaults_keep_existing_checkpoint_scope():
    options = Options.parse({}, {})

    assert options.kinds == ("account", "commit", "identity", "sync")
    assert (
        options.fingerprint() == "b2e0d905e95086f324e417310dc5093daf0b3b89eceed3005696a2f536065dfd"
    )
    assert Options.parse(options.to_options_dict(), {}).fingerprint() == options.fingerprint()


def test_selection_normalization_and_table_override():
    options = Options.parse(
        {"endpoint": "https://example.com///", "api_key": "token", "kinds": "sync,commit"},
        {"kinds": "identity,commit,commit", "collections": "app.bsky.feed.*, app.bsky.feed.*"},
    )

    assert options.endpoint == "https://example.com"
    assert options.kinds == ("commit", "identity")
    assert options.collections == ("app.bsky.feed.*",)
    assert options.with_table_options({"kinds": "commit"}).collections == options.collections


@pytest.mark.parametrize(
    "value, expected", [(True, True), (False, False), (" TRUE ", True), ("False", False)]
)
def test_boolean_payload_option_accepts_yaml_and_text_forms(value, expected):
    assert Options.parse({"include_raw_payload": value}, {}).include_raw_payload is expected


def test_forwarded_endpoint_is_compared_after_normalization():
    options = Options.parse(
        {"endpoint": "https://example.com/", "api_key": "token"},
        {"endpoint": "https://example.com///", "api_key": "token", "tableName": "events"},
    )

    assert options.endpoint == "https://example.com"


@pytest.mark.parametrize(
    "table",
    [{"endpoint": "https://other.example"}, {"api_key": "different-token"}],
)
def test_connection_settings_cannot_be_overridden(table):
    with pytest.raises(ValueError, match="cannot be overridden"):
        Options.parse({"endpoint": "https://example.com", "api_key": "token"}, table)


def test_pydantic_enforces_constraints_without_exposing_secret():
    with pytest.raises(ValidationError) as error:
        Options.parse({"api_key": "private token", "max_retries": "9"}, {})

    assert {item["loc"][0] for item in error.value.errors()} == {"api_key", "max_retries"}
    assert "private token" not in str(error.value)

    options = Options.parse({"api_key": "private-token"}, {})
    assert "private-token" not in repr(options)
    assert "api_key" not in options.model_dump()
    assert options.to_options_dict()["api_key"] == "private-token"
