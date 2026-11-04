"""Tests for the ``include_properties`` / ``exclude_properties`` table options.

The shared ``LakeflowConnectTests`` suite runs the default (unfiltered) path.
These tests cover object-table property filtering: schema, metadata, the
options-aware caches, cursor-property retention, and read pass-through.
"""

import json
from pathlib import Path

import pytest

from databricks.labs.community_connector import source_simulator as sim_pkg
from databricks.labs.community_connector.source_simulator import MODE_SIMULATE, Simulator
from databricks.labs.community_connector.sources.hubspot.hubspot import HubspotLakeflowConnect

SPEC_DIR = Path(sim_pkg.__file__).parent / "specs" / "hubspot"


@pytest.fixture(scope="module")
def connector():
    with Simulator(
        mode=MODE_SIMULATE,
        spec_path=SPEC_DIR / "endpoints.yaml",
        corpus_dir=SPEC_DIR / "corpus",
    ):
        conn = HubspotLakeflowConnect({"access_token": "simulator-fake-access-token"})
        yield conn


def _property_fields(schema):
    props = [f for f in schema.fields if f.name == "properties"]
    return [f.name for f in props[0].dataType.fields] if props else []


def _metadata_property_names(conn, table, opts):
    return conn.read_table_metadata(table, opts)["property_names"]


def test_include_properties_limits_schema_and_metadata(connector):
    opts = {"include_properties": "name"}
    schema = connector.get_table_schema("contacts", opts)
    meta_names = _metadata_property_names(connector, "contacts", opts)

    # The cursor property is always retained; unknown names ("nope") are ignored.
    assert set(meta_names) == {"name", "lastmodifieddate"}
    assert set(_property_fields(schema)) == set(meta_names)

    rows, _ = connector.read_table("contacts", {}, opts)
    rows = list(rows)
    assert rows
    for r in rows:
        if r.get("properties"):
            assert set(r["properties"]) <= set(meta_names)


def test_exclude_properties_drops_selected_and_keeps_rest(connector):
    opts = {"exclude_properties": "hs_lastmodifieddate"}
    meta_names = _metadata_property_names(connector, "deals", opts)
    # hs_lastmodifieddate is the deals cursor property -> forced back in.
    assert "hs_lastmodifieddate" in meta_names
    assert "amount" in meta_names and "dealstage" in meta_names


def test_include_then_exclude_precedence(connector):
    opts = {
        "include_properties": "amount,dealstage,name",
        "exclude_properties": "name",
    }
    meta_names = _metadata_property_names(connector, "deals", opts)
    assert set(meta_names) == {"amount", "dealstage", "hs_lastmodifieddate"}


def test_options_aware_caches_do_not_leak(connector):
    all_names = _metadata_property_names(connector, "companies", {})
    filtered = _metadata_property_names(connector, "companies", {"include_properties": "domain"})

    assert len(all_names) > len(filtered)
    # Second call with each option set returns the cached, unchanged values.
    assert _metadata_property_names(connector, "companies", {}) == all_names
    assert (
        _metadata_property_names(connector, "companies", {"include_properties": "domain"})
        == filtered
    )
    schema_all = connector.get_table_schema("companies", {})
    schema_filtered = connector.get_table_schema("companies", {"include_properties": "domain"})
    assert len(_property_fields(schema_all)) > len(_property_fields(schema_filtered))


def test_history_table_honours_include_properties(connector):
    """include_properties also bounds the history tables' propertiesWithHistory."""
    corpus = json.loads((SPEC_DIR / "corpus" / "contacts_history.json").read_text())
    all_props = set()
    for r in corpus:
        all_props.update(r["propertiesWithHistory"].keys())

    opts = {"include_properties": sorted(all_props)[0]}
    rows, _ = connector.read_table("contacts_property_history", {}, opts)
    rows = list(rows)
    assert rows
    assert {r["property"] for r in rows} == {opts["include_properties"]}
