"""OData connector unit tests — schema group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import logging
import re

import pytest
import responses

from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal
from pyspark.sql.types import DecimalType, IntegerType
from tests.unit.sources.odata._odata_test_helpers import *  # noqa: F401,F403


def test_parse_iso8601_normalizes_fraction_digit_count():
    """Version-uniform parsing: Python 3.10 (the declared floor, DBR 13.3
    LTS) accepts only 3- or 6-digit fractional seconds, while servers render
    value-dependent digit counts (Olingo/SAP trim trailing zeros) and
    nanosecond servers emit 7+. The helper pads/truncates to 6 so parsing —
    and everything built on it: the ISO sniff, the chronological
    comparisons, the lookback floor — behaves identically everywhere."""
    from datetime import datetime, timezone

    from databricks.labs.community_connector.sources.odata._helpers import parse_iso8601

    base = datetime(2024, 1, 1, 23, 0, 0, 500000, tzinfo=timezone.utc)
    assert parse_iso8601("2024-01-01T23:00:00.5Z") == base  # 1 digit → padded
    assert parse_iso8601("2024-01-01T23:00:00.50000Z") == base  # 5 digits → padded
    assert parse_iso8601("2024-01-01T23:00:00.5000000Z") == base  # 7 digits → truncated
    # Sub-microsecond digits truncate (ordering-tie territory, duplicate-safe).
    assert parse_iso8601("2024-01-01T23:00:00.1234567Z") == parse_iso8601(
        "2024-01-01T23:00:00.123456Z"
    )
    # Non-fractional and offset forms pass through untouched.
    assert parse_iso8601("2024-01-01T23:00:00+10:00").utcoffset().total_seconds() == 36000
    with pytest.raises(ValueError):
        parse_iso8601("not-a-timestamp")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


@responses.activate
def test_list_tables_returns_all_entity_sets():
    _mock_metadata()
    c = _make()
    assert sorted(c.list_tables()) == ["Customers", "Orders"]


@responses.activate
def test_get_table_schema_maps_edm_types():
    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {})
    names = [f.name for f in schema.fields]
    types = [type(f.dataType).__name__ for f in schema.fields]
    assert names == ["Id", "Name", "ModifiedAt"]
    assert types == ["IntegerType", "StringType", "TimestampType"]
    assert schema.fields[0].nullable is False


@responses.activate
def test_get_table_schema_respects_select():
    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {"select": "Id,ModifiedAt"})
    assert [f.name for f in schema.fields] == ["Id", "ModifiedAt"]


def test_missing_service_url_raises():
    with pytest.raises(ValueError, match="service_url"):
        ODataLakeflowConnect({})


def test_sequence_counter_is_picklable_and_monotonic():
    """The ``_lc_sequence`` tie-breaker must survive pickling: in the merged
    bundle cloudpickle serializes the connector class BY VALUE and walks the
    closure cell holding this counter — a bare ``itertools.count`` is a
    TypeError on Python >= 3.14 (see the bundle round-trip test). A clone
    restarts at zero (benign: the ns timestamp dominates the sequence)."""
    import pickle

    from databricks.labs.community_connector.sources.odata.odata import (
        _SEQUENCE_COUNTER,
        _next_sequence,
    )

    first, second = _next_sequence(), _next_sequence()
    assert first < second  # still strictly increasing
    clone = pickle.loads(pickle.dumps(_SEQUENCE_COUNTER))
    assert isinstance(next(clone), int)


@responses.activate
def test_list_namespaces_returns_all_schemas():
    _mock_multi_metadata()
    c = _make()
    assert sorted(c.list_namespaces()) == [["HR"], ["Sales"]]


@responses.activate
def test_list_namespaces_with_prefix_is_empty():
    """OData has a single flat level — anything under a namespace returns []."""
    _mock_multi_metadata()
    c = _make()
    assert c.list_namespaces(["Sales"]) == []


@responses.activate
def test_list_tables_in_namespace_filters_by_schema():
    _mock_multi_metadata()
    c = _make()
    assert sorted(c.list_tables_in_namespace(["Sales"])) == ["Customers", "Orders"]
    assert c.list_tables_in_namespace(["HR"]) == ["Customers"]


@responses.activate
def test_list_tables_in_root_namespace_is_empty():
    _mock_multi_metadata()
    c = _make()
    # OData entity sets always live inside a Schema — never at the root.
    assert c.list_tables_in_namespace([]) == []


@responses.activate
def test_list_tables_dedupes_across_namespaces():
    _mock_multi_metadata()
    c = _make()
    # 'Customers' appears in both Sales and HR — should appear once.
    assert sorted(c.list_tables()) == ["Customers", "Orders"]


@responses.activate
def test_ambiguous_table_name_raises_without_namespace():
    _mock_multi_metadata()
    c = _make()
    with pytest.raises(ValueError, match="multiple namespaces"):
        c.get_table_schema("Customers", {})


@responses.activate
def test_namespace_disambiguates_schema_lookup():
    _mock_multi_metadata()
    c = _make()
    sales_schema = c.get_table_schema("Customers", {"namespace": "Sales"})
    hr_schema = c.get_table_schema("Customers", {"namespace": "HR"})
    assert [f.name for f in sales_schema.fields] == ["Id", "Account"]
    assert [f.name for f in hr_schema.fields] == ["EmployeeId", "Department"]


@responses.activate
def test_unique_name_does_not_require_namespace():
    """When a name appears in only one schema, namespace is optional."""
    _mock_multi_metadata()
    c = _make()
    schema = c.get_table_schema("Orders", {})  # only in Sales
    assert [f.name for f in schema.fields] == ["OrderId"]


@responses.activate
def test_inheritance_primary_key_walks_base_chain():
    """``user`` has no <Key> of its own — Key is on ``entity`` two
    levels up. Without chain walking the connector returns no PK; with
    it, MERGE-on-PK at the destination works correctly."""
    _mock_inherited_metadata()
    c = _make()
    meta = c.read_table_metadata("users", {})
    assert meta["primary_keys"] == ["id"]


@responses.activate
def test_inheritance_schema_aggregates_properties_root_to_leaf():
    """Inherited properties (``id``, ``deletedDateTime``) appear before
    the leaf's own additions. Reflects the order a developer reading
    the CSDL would expect: base type first, derived overlays after."""
    _mock_inherited_metadata()
    c = _make()
    schema = c.get_table_schema("users", {})
    names = [f.name for f in schema.fields]
    assert names == ["id", "deletedDateTime", "displayName", "mail"]


@responses.activate
def test_inheritance_alias_resolution():
    """A BaseType referenced via the schema's ``Alias`` (e.g.
    ``graph.entity`` when the schema declares ``Alias="graph"``) must
    resolve to the same EntityType as the full namespace
    (``microsoft.graph.entity``). Graph relies on this for every
    derived type."""
    _mock_inherited_metadata()
    c = _make()
    # directoryObject's BaseType uses the alias; user's uses the full
    # namespace. If alias resolution were broken, one would resolve and
    # the other wouldn't.
    et = c._entity_type_for("users")
    chain = c._resolve_base_chain(et)
    type_names = [t.get("Name") for t in chain]
    assert type_names == ["user", "directoryObject", "entity"]


@responses.activate
def test_inheritance_id_in_schema_when_only_declared_on_base():
    """Concrete regression for the Graph-compatibility bug: ``id`` is
    only declared on ``graph.entity``, but every Graph entity set needs
    it as a column."""
    _mock_inherited_metadata()
    c = _make()
    schema = c.get_table_schema("users", {})
    id_field = next(f for f in schema.fields if f.name == "id")
    assert type(id_field.dataType).__name__ == "StringType"
    assert id_field.nullable is False


@responses.activate
def test_inheritance_cycle_guard_terminates():
    """Malformed CSDL with a BaseType cycle must not loop. The walker
    halts at the first repeat, returning whatever Key/Properties it
    found along the way."""
    responses.get(f"{SERVICE_URL}$metadata", body=CYCLE_METADATA_XML, status=200)
    c = _make()
    # Should terminate (no infinite loop) and surface SOME schema /
    # PK info from whatever chain was walked before the cycle.
    schema = c.get_table_schema("things", {})
    pks = c.read_table_metadata("things", {})["primary_keys"]
    assert {f.name for f in schema.fields} == {"a_field", "b_field"}
    assert pks == ["b_field"]


@responses.activate
def test_inheritance_unresolvable_base_returns_what_can_be_resolved():
    """BaseType references that point at a non-existent type
    (e.g. an external schema we didn't fetch) just truncate the
    chain — they're not a hard error."""
    xml = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="x" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Item" BaseType="external.Missing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Items" EntityType="x.Item"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    responses.get(f"{SERVICE_URL}$metadata", body=xml, status=200)
    c = _make()
    # External BaseType reference can't be resolved — connector still
    # produces the local Key + Property data.
    meta = c.read_table_metadata("Items", {})
    schema = c.get_table_schema("Items", {})
    assert meta["primary_keys"] == ["Id"]
    assert [f.name for f in schema.fields] == ["Id"]


@responses.activate
def test_recursive_containment_fk_columns_stay_distinct_per_level():
    """A hand-written recursive containment path repeats the same nav-prop
    name at two non-leaf levels. The FK mapping is keyed by level INDEX —
    a name-keyed map would collapse both levels into one entry, duplicating
    the surviving column in the schema and dropping a composite-key
    component (silent MERGE collisions between leaves under different
    level-1 parents)."""
    _mock_recursive_metadata()
    c = _make()
    table = "Nodes__Children__Children__Children"
    schema = c.get_table_schema(table, {})
    names = [f.name for f in schema.fields]
    assert len(names) == len(set(names)), f"duplicate columns in schema: {names}"
    # One distinct FK column per non-leaf level, collision-suffixed.
    assert names[:3] == ["Nodes_Id", "Children_Id", "_Children_Id"]
    # The composite key carries every level's component plus the leaf PK.
    assert c._primary_keys_for(table) == ["Nodes_Id", "Children_Id", "_Children_Id", "Id"]


@responses.activate
def test_recursive_containment_rows_tagged_with_each_levels_fk():
    """The N+1 walk stamps each ancestor level's PK into its OWN column —
    the deeper repeated level must not overwrite the shallower one."""
    _mock_recursive_metadata()
    responses.get(f"{SERVICE_URL}Nodes", json={"value": [{"Id": 1, "Label": "root"}]})
    responses.get(f"{SERVICE_URL}Nodes(1)/Children", json={"value": [{"Id": 10, "Label": "l1"}]})
    responses.get(
        f"{SERVICE_URL}Nodes(1)/Children(10)/Children",
        json={"value": [{"Id": 100, "Label": "l2"}]},
    )
    responses.get(
        f"{SERVICE_URL}Nodes(1)/Children(10)/Children(100)/Children",
        json={"value": [{"Id": 1000, "Label": "leaf"}]},
    )
    c = _make()
    recs, _ = c.read_table(
        "Nodes__Children__Children__Children",
        {},
        {"expand_contained": "false", "contained_fetch": "single", "pagination": "nextlink"},
    )
    rows = list(recs)
    assert rows == [
        {"Nodes_Id": 1, "Children_Id": 10, "_Children_Id": 100, "Id": 1000, "Label": "leaf"}
    ]


@responses.activate
def test_decimal_precision_scale_facets_honoured():
    """``Edm.Decimal`` honours declared CSDL ``Precision``/``Scale`` facets.
    A hardcoded ``DecimalType(38, 18)`` leaves only 20 digits left of the
    point — it can't hold a ``Decimal(38, 0)`` ID column's large values.
    Absent facets (and ``Scale="variable"``) keep the historical wide
    default so existing destinations don't shift types; ``Scale`` absent
    with ``Precision`` declared is scale 0 (the CSDL default)."""
    responses.get(f"{SERVICE_URL}$metadata", body=DECIMAL_METADATA_XML, status=200)
    c = _make()
    types = {f.name: f.dataType for f in c.get_table_schema("Moneys", {}).fields}
    assert types["Exact"] == DecimalType(10, 2)
    assert types["Wide"] == DecimalType(38, 18)
    assert types["Varying"] == DecimalType(38, 18)
    assert types["BigId"] == DecimalType(38, 0)


# --- Path parsing / discovery ---


def test_parse_contained_path_flat_returns_none():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    assert _parse_contained_path("Customers") is None


def test_parse_contained_path_multi_segment():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    assert _parse_contained_path("A__B__C") == ["A", "B", "C"]


def test_parse_contained_path_rejects_empty_segment():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    with pytest.raises(ValueError, match="Empty path segment"):
        _parse_contained_path("A____B")


def test_parse_contained_path_rejects_slash_with_actionable_message():
    """Old-form slash paths are common when the user copied the table
    name from OData URL syntax or from a pre-fix version of
    ``list_tables``. The error must spell out the rename so the user
    isn't left staring at a "not found" with a 200-entry available list.
    """
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    with pytest.raises(
        ValueError, match="Rename 'Instances/AssetPacks' to 'Instances__AssetPacks'"
    ):
        _parse_contained_path("Instances/AssetPacks")


def test_parse_contained_path_rejects_over_depth():
    from databricks.labs.community_connector.sources.odata.odata import (
        _parse_contained_path,
    )

    # 11 segments exceeds the depth-10 cap.
    with pytest.raises(ValueError, match="exceeds max depth"):
        _parse_contained_path("A__B__C__D__E__F__G__H__I__J__K")


@responses.activate
def test_list_tables_includes_nested_paths():
    _mock_nested_metadata()
    c = _make()
    flat = c.list_tables()
    # Top-level + every reachable contained path.
    assert "Parents" in flat
    assert "Parents__Children" in flat
    assert "Parents__Tags" in flat
    assert "Parents__Children__Notes" in flat


@responses.activate
def test_list_tables_in_namespace_includes_nested_paths():
    _mock_nested_metadata()
    c = _make()
    tables = c.list_tables_in_namespace(["Nested"])
    assert tables == [
        "Parents",
        "Parents__Children",
        "Parents__Children__Notes",
        "Parents__Tags",
    ]


# --- Entity type resolution / schema / PK ---


@responses.activate
def test_get_table_schema_for_two_level_contained():
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children", {})
    names = [f.name for f in schema.fields]
    # Parent FK prepended, then child's own fields in CSDL order.
    assert names == ["Parents_Id", "Id", "Label", "ModifiedAt"]
    fk_field = schema["Parents_Id"]
    assert isinstance(fk_field.dataType, IntegerType)
    assert fk_field.nullable is False


@responses.activate
def test_get_table_schema_for_three_level_contained_emits_full_ancestor_chain():
    """For ``A__B__C`` every non-leaf ancestor contributes FK columns
    (OData v4 §13.4.3 — contained-entity keys are unique within parent
    only, so the full chain is required for global uniqueness)."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Children__Notes", {})
    names = [f.name for f in schema.fields]
    assert names == ["Parents_Id", "Children_Id", "Id", "Text"]


@responses.activate
def test_get_table_schema_for_contained_with_composite_parent_pk():
    """Parents__Tags has a composite-key leaf; FK prepend on a single-PK
    parent yields exactly one ancestor column. Inverse test (composite
    parent) requires a different fixture — covered indirectly via the
    Tag leaf's own composite key showing up in primary_keys_for."""
    _mock_nested_metadata()
    c = _make()
    schema = c.get_table_schema("Parents__Tags", {})
    names = [f.name for f in schema.fields]
    assert names == ["Parents_Id", "Category", "Value"]


@responses.activate
def test_primary_keys_for_two_level_contained():
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children", {})
    assert meta["primary_keys"] == ["Parents_Id", "Id"]
    assert meta["ingestion_type"] == "snapshot"


@responses.activate
def test_primary_keys_for_three_level_contained_full_ancestor_chain():
    """Composite PK is every ancestor's FK + leaf PK — required for
    global uniqueness when leaf IDs only repeat within a parent."""
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Children__Notes", {})
    assert meta["primary_keys"] == ["Parents_Id", "Children_Id", "Id"]


@responses.activate
def test_primary_keys_for_composite_leaf_in_contained():
    _mock_nested_metadata()
    c = _make()
    meta = c.read_table_metadata("Parents__Tags", {})
    # Composite PK on the leaf — both columns surface alongside parent FK.
    assert meta["primary_keys"] == ["Parents_Id", "Category", "Value"]


@responses.activate
def test_entity_type_for_invalid_nav_prop_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not a contained-collection"):
        c.read_table_metadata("Parents__NotAThing", {})


# --- URL construction ---


@responses.activate
def test_key_predicate_single_key():
    _mock_nested_metadata()
    c = _make()
    assert c._format_key_predicate({"Id": 42}) == "(42)"


@responses.activate
def test_key_predicate_composite():
    _mock_nested_metadata()
    c = _make()
    pred = c._format_key_predicate({"Category": "fruit", "Value": "apple"})
    assert pred == "(Category='fruit',Value='apple')"


@responses.activate
def test_build_contained_url_two_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_contained_url(["Parents", "Children"], [{"Id": 7}], {"page_size": "1000"})
    assert url.startswith(f"{SERVICE_URL}Parents(7)/Children?")
    assert "$top=1000" in url


def test_bin_pack_hits_requested_partition_count():
    """Balanced ``divmod`` sizing yields every partition the user asked for
    (uniform ``ceil(n/p)`` slicing collapsed n=9,p=4 to 3 bins), with
    exactly-once contiguous coverage and no empty bins."""
    from databricks.labs.community_connector.sources.odata._partition import _bin_pack

    rows = [{"Id": i} for i in range(9)]
    parts = _bin_pack(rows, 4, None)
    assert [len(p["top_parent_rows"]) for p in parts] == [3, 2, 2, 2]
    assert [r["Id"] for p in parts for r in p["top_parent_rows"]] == list(range(9))
    assert len(_bin_pack([{"Id": i} for i in range(10)], 6, None)) == 6
    assert len(_bin_pack([{"Id": 1}, {"Id": 2}], 5, None)) == 2  # never empty bins
    assert _bin_pack([], 4, None) == []


@responses.activate
def test_build_contained_url_three_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_contained_url(
        ["Parents", "Children", "Notes"],
        [{"Id": 7}, {"Id": 9}],
        {},
    )
    assert url.startswith(f"{SERVICE_URL}Parents(7)/Children(9)/Notes?")


@responses.activate
def test_build_expand_url_three_level():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children", "Notes"], {"page_size": "1000"})
    # Dynamic distribution for N=3, page_size=1000: [34, 5, 5] (product 850).
    # PK-only $orderby is injected at every (non-cursor) level for
    # skiptoken stability.
    assert "Parents?$top=34" in url
    assert "$orderby=Id asc" in url
    assert "$expand=Children($top=5;$orderby=Id asc;$expand=Notes($top=5;$orderby=Id asc))" in url


@responses.activate
def test_build_expand_url_four_level_nests_correctly():
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["A", "B", "C", "D"], {"page_size": "1000"})
    # Dynamic distribution for N=4, page_size=1000: [8, 5, 5, 5] (product 1000).
    # A/B/C/D aren't declared in the fixture metadata, so the per-level
    # PK $orderby degrades to none — this test pins the $top nesting
    # structure only (real-entity $orderby is covered above).
    assert "A?$top=8" in url
    assert "$expand=B($top=5;$expand=C($top=5;$expand=D($top=5)))" in url


@responses.activate
def test_build_expand_url_dynamic_tops_for_two_level():
    """User's stated rule: for a 2-segment expand with page_size=1000,
    the top URL gets ``$top=100`` and the single inner expand gets
    ``$top=10`` — product equals the budget exactly."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children"], {"page_size": "1000"})
    assert "Parents?$top=100" in url
    assert "$expand=Children($top=10;$orderby=Id asc)" in url


@responses.activate
def test_build_expand_url_page_size_scales_dynamic_tops():
    """Reducing ``page_size`` scales every level proportionally."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(["Parents", "Children"], {"page_size": "100"})
    # For N=2 page_size=100: inner = 100^(1/3) ≈ 4.6 → clamped to 5,
    # then upper level absorbs remaining budget = 100 // 5 = 20.
    # Product 20 × 5 = 100 (exact).
    assert "Parents?$top=20" in url
    assert "$expand=Children($top=5;$orderby=Id asc)" in url


@responses.activate
def test_build_expand_url_inner_top_with_cursor_clause():
    """Inner ``$top`` composes with ``$filter``/``$orderby`` when a
    cursor is injected at that level."""
    _mock_nested_metadata()
    c = _make()
    url = c._build_expand_url(
        ["Parents", "Children"],
        {"page_size": "500"},
        cursor_level=1,
        cursor_filter="ModifiedAt gt 2024-01-01T00:00:00Z",
        cursor_order="ModifiedAt asc,Id asc",
    )
    # Dynamic distribution for N=2, page_size=500: [62, 7]. $filter and
    # $orderby compose with the inner $top at the cursor's level.
    assert "Parents?$top=62" in url
    assert "$expand=Children($top=7" in url
    assert "$filter=ModifiedAt gt 2024-01-01T00:00:00Z" in url
    assert "$orderby=ModifiedAt asc,Id asc" in url


# --- Per-segment filters (filter_at_<segment>, filter_at_<idx>) ---


def test_resolve_segment_filters_name_form():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {
            "filter_at_Parents": "Id eq 5",
            "filter_at_Children": "Status eq 'active'",
            "filter_at_Notes": "Text ne null",
            "filter": "ignored — different key",
        },
        ["Parents", "Children", "Notes"],
    )
    assert out == {0: "Id eq 5", 1: "Status eq 'active'", 2: "Text ne null"}


def test_resolve_segment_filters_index_form():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {"filter_at_0": "Id eq 5", "filter_at_2": "Text ne null"},
        ["Parents", "Children", "Notes"],
    )
    assert out == {0: "Id eq 5", 2: "Text ne null"}


def test_resolve_segment_filters_case_insensitive_segment_name():
    """Lakeflow Connect lowercases option keys before forwarding them
    to ``read_table``, so a pipeline-config ``filter_at_Instances``
    arrives as ``filter_at_instances``. The segment-name match must
    be case-insensitive."""
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {
            "filter_at_instances": "Id eq 1",  # lowercased by framework
            "filter_at_PROJECTS": "Id eq 2",  # any casing accepted
        },
        ["Instances", "Projects", "WorkPackageDetails"],
    )
    assert out == {0: "Id eq 1", 1: "Id eq 2"}


def test_resolve_segment_filters_index_overrides_name_on_conflict():
    """Index form is the more explicit of the two — wins when both
    target the same level."""
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    out = resolve_segment_filters(
        {"filter_at_Children": "by name", "filter_at_1": "by index"},
        ["Parents", "Children", "Notes"],
    )
    assert out[1] == "by index"


def test_resolve_segment_filters_unknown_segment_raises():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    with pytest.raises(ValueError, match="Bogus"):
        resolve_segment_filters(
            {"filter_at_Bogus": "Id eq 5"},
            ["Parents", "Children", "Notes"],
        )


def test_resolve_segment_filters_out_of_range_index_raises():
    from databricks.labs.community_connector.sources.odata._contained import (
        resolve_segment_filters,
    )

    with pytest.raises(ValueError, match="out of range"):
        resolve_segment_filters(
            {"filter_at_5": "Id eq 5"},
            ["Parents", "Children", "Notes"],
        )


@responses.activate
def test_structured_values_emitted_as_json_not_python_repr():
    """Complex-typed / collection values map to string columns, and the
    framework stringifies via ``str()`` — a Python repr downstream
    ``from_json`` can't parse. The connector renders structured values as
    JSON at the emit boundary instead."""
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {
                    "Id": 1,
                    "Name": "x",
                    "Address": {"City": "Y", "Zip": 10001},
                    "Tags": ["a", "b"],
                }
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, _ = c.read_table("Customers", None, {})
    row = next(iter(records))
    assert row["Address"] == '{"City":"Y","Zip":10001}'
    assert row["Tags"] == '["a","b"]'
    assert json.loads(row["Address"]) == {"City": "Y", "Zip": 10001}
    assert row["Id"] == 1  # scalars untouched


@responses.activate
def test_metadata_id_keyed_memos_dropped_at_pickle_boundary():
    """Spark pickles the reader (and the parsed-CSDL bundle) to executor
    tasks, where the unpickled tree's elements have NEW addresses: the
    ``id(et)``-keyed memos' driver-address keys are dead weight at best and
    a silently-wrong-schema false hit at worst. ``__getstate__`` must drop
    them; the executor re-derives per element, yielding the same schema."""
    import pickle

    _mock_metadata()
    c = _make()
    schema = c.get_table_schema("Customers", {})
    pks = c._primary_keys_for("Customers")
    state = c._metadata_state()
    assert state.own_fields and state.own_pks  # id()-keyed memos populated

    c2 = pickle.loads(pickle.dumps(c))
    state2 = c2._metadata_state()
    assert state2.own_fields == {}
    assert state2.own_pks == {}
    assert state2.base_chain == {}
    # Name-keyed memos are process-portable and survive.
    assert state2.fields
    # Executor-side re-derivation produces identical results.
    assert c2.get_table_schema("Customers", {}) == schema
    assert c2._primary_keys_for("Customers") == pks


@responses.activate
def test_guid_key_predicate_renders_bare():
    """An ``Edm.Guid`` key arrives as a JSON string, but its key predicate
    must be UNQUOTED per the OData v4 ABNF — strict servers (Olingo, SAP)
    400 on ``Accounts('<guid>')``. The value sniff can't know this; the
    declared type must win."""
    from urllib.parse import unquote

    _mock_guid_metadata()
    responses.get(
        f"{SERVICE_URL}Accounts", json={"value": [{"AccountId": _GUID}]}, match_querystring=False
    )
    # ONLY the bare-predicate URL is registered — a quoted predicate would
    # hit an unregistered URL and fail the read outright.
    responses.get(
        f"{SERVICE_URL}Accounts({_GUID})/Contacts",
        json={"value": [{"ContactId": _GUID, "ModifiedAt": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )
    c = _make()
    recs, _ = c.read_table(
        "Accounts__Contacts", {}, {"contained_fetch": "single", "pagination": "nextlink"}
    )
    assert [r["ContactId"] for r in recs] == [_GUID]
    urls = [unquote(call.request.url) for call in responses.calls]
    assert any(f"Accounts({_GUID})/Contacts" in u for u in urls)
    assert not any("Accounts('" in u for u in urls)


@responses.activate
def test_edm_types_for_level_memoizes_failure():
    """Round-28: an unresolvable path must not re-run entity-type resolution
    (and re-format its "Available: ..." error) on every URL build — the
    failure is memoized per (path, namespace)."""
    _mock_metadata()
    c = _make()
    calls = {"n": 0}
    orig = c._entity_type_for

    def _counting(name, namespace=None):
        calls["n"] += 1
        return orig(name, namespace)

    c._entity_type_for = _counting
    assert c._edm_types_for_level(["NoSuchSet"], 0, None) == {}
    assert calls["n"] == 1
    assert c._edm_types_for_level(["NoSuchSet"], 0, None) == {}
    assert calls["n"] == 1  # second call short-circuits on the failure memo
    del c._entity_type_for


@responses.activate
def test_edm_types_root_wins_matching_schema_resolution():
    """On (spec-forbidden) redeclaring metadata the literal-typing map must
    agree with the SCHEMA resolver (closest-to-root wins) — a seek boundary
    quoted for the leaf declaration while the schema parses the root type
    would desync the wire filter from the declared column."""
    responses.get(f"{SERVICE_URL}$metadata", body=REDECLARE_METADATA_XML, status=200)
    c = _make()
    assert c._edm_types_for_table("Deriveds", None)["V"] == "Edm.Int32"
    schema = c.get_table_schema("Deriveds", {})
    (v_field,) = [f for f in schema.fields if f.name == "V"]
    assert v_field.dataType == IntegerType()


@responses.activate
def test_metadata_process_cache_honors_ttl(monkeypatch):
    """The process-wide _METADATA_CACHE previously had NO TTL check — in a
    long-running driver $metadata never refreshed regardless of the setting.
    Entries are now stamped and expire after metadata_cache_ttl_seconds."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    responses.get(f"{SERVICE_URL}$metadata", body=METADATA_XML, status=200)
    a = _make({"metadata_cache_ttl_seconds": "60"})
    assert "Customers" in a.list_tables()

    # Serve a changed document and jump past the TTL (both the process
    # stamp and the on-disk mtime check read the same clock).
    responses.replace(responses.GET, f"{SERVICE_URL}$metadata", body=METADATA_XML_V2, status=200)
    real_time = odata_mod.time.time
    monkeypatch.setattr(odata_mod.time, "time", lambda: real_time() + 61)
    b = _make({"metadata_cache_ttl_seconds": "60"})
    tables = b.list_tables()
    assert "CustomersV2" in tables and "Customers" not in tables


@responses.activate
def test_metadata_cache_ttl_zero_disables_process_cache():
    """metadata_cache_ttl_seconds=0 is documented as 'disable' but previously
    only skipped the on-disk pickle — the process dict still served (and was
    fed) stale documents. TTL 0 now bypasses the process layer entirely."""
    responses.get(f"{SERVICE_URL}$metadata", body=METADATA_XML, status=200)
    a = _make({"metadata_cache_ttl_seconds": "0"})
    assert "Customers" in a.list_tables()

    responses.replace(responses.GET, f"{SERVICE_URL}$metadata", body=METADATA_XML_V2, status=200)
    b = _make({"metadata_cache_ttl_seconds": "0"})
    tables = b.list_tables()
    assert "CustomersV2" in tables and "Customers" not in tables


@responses.activate
def test_typedef_properties_resolve_to_underlying_edm_type():
    """A property typed via <TypeDefinition> previously recorded the
    definition name verbatim, falling out of typed literal rendering — an
    ISO-looking string on an Edm.String-backed definition rendered BARE
    (invalid predicate), the exact misfire typed rendering exists to stop.
    The index now resolves definitions to their underlying primitive,
    accepting both namespace- and alias-qualified references."""
    from databricks.labs.community_connector.sources.odata._contained import (
        odata_literal_typed,
    )

    responses.get(f"{SERVICE_URL}$metadata", body=TYPEDEF_METADATA_XML, status=200)
    c = _make()
    types = c._edm_types_for_table("Items", None)
    assert types["Code"] == "Edm.String"
    assert types["Qty"] == "Edm.Int64"
    assert odata_literal_typed("2024-01-01", types["Code"]) == "'2024-01-01'"
    assert odata_literal_typed("42", types["Qty"]) == "42"


@responses.activate
def test_namespace_option_accepts_schema_alias():
    """CSDL lets type references use the schema Alias interchangeably with
    its Namespace; the `namespace` table option now does too (previously an
    alias failed with the type-only-schema error)."""
    responses.get(f"{SERVICE_URL}$metadata", body=TYPEDEF_METADATA_XML, status=200)
    c = _make()
    schema = c.get_table_schema("Items", {"namespace": "ta"})
    assert {f.name for f in schema.fields} >= {"Id", "Code", "Qty"}


@responses.activate
def test_list_tables_in_namespace_rejects_multi_segment():
    """OData namespaces are single-level; a multi-segment path names nothing.
    Previously segment[0]'s tables were returned, fabricating rows under a
    nonexistent namespace path."""
    _mock_metadata()
    c = _make()
    assert c.list_tables_in_namespace(["Demo"]) == ["Customers", "Orders"]
    assert c.list_tables_in_namespace(["Demo", "bogus"]) == []
    assert c.list_tables_in_namespace([]) == []


def test_decimal_nonfinite_renders_odata_wire_literals():
    """Decimal('Infinity')/NaN previously fell through the float-only guard
    and rendered Python's spellings (invalid on the wire)."""
    from decimal import Decimal as _D

    assert _odata_literal(_D("Infinity")) == "INF"
    assert _odata_literal(_D("-Infinity")) == "-INF"
    assert _odata_literal(_D("NaN")) == "NaN"
    assert _odata_literal(_D("1.5")) == "1.5"


@responses.activate
def test_duplicate_set_in_single_namespace_gets_precise_error():
    """Same set name twice in ONE namespace (malformed CSDL): the old message
    suggested the `namespace` option, which cannot disambiguate here."""
    responses.get(f"{SERVICE_URL}$metadata", body=DUPSET_METADATA_XML, status=200)
    c = _make()
    with pytest.raises(ValueError, match="more than once in namespace"):
        c.get_table_schema("Things", {})


@responses.activate
def test_fields_and_pk_memos_are_exclusion_aware():
    """The `_fields_for`/`_primary_keys_for` memos embedded the
    exclusion-FILTERED FK columns under a (table, namespace)-only key, so
    a shared instance froze schema AND composite PK at the first call's
    `exclude_ancestor_columns` while row stamping followed the current
    one — hard parse failures one way, silent MERGE collisions the other."""
    _mock_nested_metadata()
    c = _make()
    with_fk = c.get_table_schema("Parents__Children", {})
    assert "Parents_Id" in {f.name for f in with_fk.fields}
    without_fk = c.get_table_schema("Parents__Children", {"exclude_ancestor_columns": "Parents_Id"})
    assert "Parents_Id" not in {f.name for f in without_fk.fields}
    # And back again on the SAME instance — pre-fix this returned the
    # excluded shape from the memo.
    again = c.get_table_schema("Parents__Children", {})
    assert "Parents_Id" in {f.name for f in again.fields}

    # Composite PK follows the same rule.
    pks = c.read_table_metadata("Parents__Children", {})["primary_keys"]
    assert pks == ["Parents_Id", "Id"]
    pks_excl = c.read_table_metadata(
        "Parents__Children", {"exclude_ancestor_columns": "Parents_Id"}
    )["primary_keys"]
    assert pks_excl == ["Id"]
    pks_back = c.read_table_metadata("Parents__Children", {})["primary_keys"]
    assert pks_back == ["Parents_Id", "Id"]


@responses.activate
def test_dunder_set_contained_children_are_readable():
    """Round 31 made a flat `My__Set` readable, but its contained children
    (`My__Set__Kids`) still split into a nonexistent containment path —
    the same listed-but-unreadable class one level deeper. The longest
    declared flat prefix now becomes the root segment."""
    responses.get(f"{SERVICE_URL}$metadata", body=DUNDER_KIDS_METADATA_XML, status=200)
    responses.get(f"{SERVICE_URL}My__Set", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}My__Set(1)/Kids",
        json={"value": [{"Id": 7, "Note": "n"}]},
    )
    c = _make()
    assert c.list_tables() == ["My__Set", "My__Set__Kids"]
    assert c._table_segments("My__Set__Kids") == ["My__Set", "Kids"]
    schema = c.get_table_schema("My__Set__Kids", {})
    assert {f.name for f in schema.fields} == {"My__Set_Id", "Id", "Note"}
    records, _ = c.read_table("My__Set__Kids", None, {})
    (row,) = list(records)
    assert row["Id"] == 7 and row["My__Set_Id"] == 1


@responses.activate
def test_value_null_tolerated_as_empty_page():
    """A spec-invalid `"value": null` previously crashed iterating None;
    it now reads as an empty page."""
    _mock_metadata()
    responses.get(f"{SERVICE_URL}Customers", json={"value": None}, match_querystring=False)
    c = _make({"token": "t"})
    records, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert list(records) == []


@responses.activate
def test_alias_error_message_names_requested_alias():
    """A user passing the schema Alias should see the alias they typed in
    the not-found error (with the canonical resolution alongside), not a
    namespace name they never wrote."""
    responses.get(f"{SERVICE_URL}$metadata", body=TYPEDEF_METADATA_XML, status=200)
    c = _make()
    with pytest.raises(ValueError, match=r"'ta' \(alias of 'T'\)"):
        c.get_table_schema("Nope", {"namespace": "ta"})


@responses.activate
def test_missing_primary_key_not_padded_stays_loud():
    """The emit-boundary padding must NOT pad primary-key columns: a server
    never legally omits a KEY (the omit-null rationale can't apply), so a
    missing one is a broken response that must keep failing loudly in the
    framework parser — padding it would send a silent null-key row into the
    destination MERGE."""
    responses.get(f"{SERVICE_URL}$metadata", body=NONNULL_FLAT_METADATA_XML, status=200)
    responses.get(f"{SERVICE_URL}Items", json={"value": [{"Opt": "y"}]})  # Id AND Req omitted
    c = _make({"token": "t"})
    rows, _ = c.read_table("Items", None, {"pagination": "nextlink"})
    (row,) = list(rows)
    # Non-key column padded (omit-null tolerance)…
    assert row["Req"] is None
    # …but the PK stays ABSENT so the framework parser still raises.
    assert "Id" not in row


def test_metadata_cache_capped_eviction():
    """The process-wide metadata cache evicts oldest-first beyond its cap,
    so a long-lived driver serving many distinct services doesn't retain one
    multi-MB parsed tree per service forever."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    saved = dict(odata_mod._METADATA_CACHE)
    odata_mod._METADATA_CACHE.clear()
    try:
        cap = odata_mod._METADATA_CACHE_MAX_SERVICES
        for i in range(cap + 3):
            odata_mod._metadata_cache_put(f"https://svc{i}/", ("x", None, None, float(i)))
        assert len(odata_mod._METADATA_CACHE) == cap
        # The three oldest (0, 1, 2) were evicted; the newest survive.
        assert f"https://svc{cap + 2}/" in odata_mod._METADATA_CACHE
        assert "https://svc0/" not in odata_mod._METADATA_CACHE
        assert "https://svc2/" not in odata_mod._METADATA_CACHE
    finally:
        odata_mod._METADATA_CACHE.clear()
        odata_mod._METADATA_CACHE.update(saved)


def test_metadata_cache_put_never_evicts_new_entry():
    """Inserting an entry whose fetched_at is OLDER than everything cached
    (the file-cache-hit path stamps entries with the file's mtime) must not
    evict the just-inserted entry itself — that service would re-parse its
    pickle on every fresh instance while idle services stay cached."""
    from databricks.labs.community_connector.sources.odata import odata as odata_mod

    saved = dict(odata_mod._METADATA_CACHE)
    odata_mod._METADATA_CACHE.clear()
    try:
        cap = odata_mod._METADATA_CACHE_MAX_SERVICES
        for i in range(cap):
            odata_mod._metadata_cache_put(f"https://svc{i}/", ("x", None, None, 1000.0 + i))
        # Newcomer with the OLDEST stamp: must survive; an existing oldest
        # entry is evicted instead.
        odata_mod._metadata_cache_put("https://old-file/", ("x", None, None, 1.0))
        assert "https://old-file/" in odata_mod._METADATA_CACHE
        assert len(odata_mod._METADATA_CACHE) == cap
        assert "https://svc0/" not in odata_mod._METADATA_CACHE  # oldest other
    finally:
        odata_mod._METADATA_CACHE.clear()
        odata_mod._METADATA_CACHE.update(saved)


def test_decimal_literal_never_carries_exponent():
    """OData's decimalValue ABNF has no exponent form — Decimal literals
    render in plain positional notation (Edm.Double floats keep their
    spec-valid exponent)."""
    from decimal import Decimal

    from databricks.labs.community_connector.sources.odata._contained import odata_literal

    assert odata_literal(Decimal("1.5E+7")) == "15000000"
    assert odata_literal(Decimal("1E-6")) == "0.000001"
    assert odata_literal(Decimal("-2.5")) == "-2.5"
    # Floats (Edm.Double) legitimately keep exponents; '+' stays escaped.
    assert "e" in odata_literal(1e20).lower()


@responses.activate
def test_typedef_property_gets_underlying_spark_type():
    """A TypeDefinition-typed property maps to its underlying primitive's
    Spark type — it used to fall to StringType while the literal-rendering
    map correctly resolved Edm.Int64, silently degrading every
    TypeDefinition-backed column (SAP production shape) to a string."""
    from pyspark.sql.types import LongType, TimestampType

    responses.get(f"{SERVICE_URL}$metadata", body=R40_TYPEDEF_METADATA, status=200)
    c = _make({"token": "t"})
    schema = {f.name: f.dataType for f in c.get_table_schema("Items", {}).fields}
    assert isinstance(schema["Qty"], LongType)
    assert isinstance(schema["At"], TimestampType)


@responses.activate
def test_complex_path_key_raises_loudly():
    """A complex-path key (<PropertyRef Name="Info/Code" Alias="IC"/>) can't
    be addressed or MERGEd on (neither the path nor the alias is an emitted
    column) — it used to silently report primary_keys=['Info/Code'], a MERGE
    key matching no schema column. It now fails loudly and honestly."""
    responses.get(f"{SERVICE_URL}$metadata", body=R40_PATHKEY_METADATA, status=200)
    c = _make({"token": "t"})
    with pytest.raises(ValueError, match="complex type"):
        c.read_table_metadata("Things", {})


@responses.activate
def test_dunder_nav_property_skipped_in_discovery():
    """A nav property named with '__' would list as a table the read path
    can never resolve back (discovery→read round-trip failure). Discovery
    now skips it with a warning; normal siblings still list."""
    responses.get(f"{SERVICE_URL}$metadata", body=R40_DUNDER_NAV_METADATA, status=200)
    c = _make({"token": "t"})
    tables = c.list_tables()
    assert "Parents" in tables and "Parents__Pets" in tables
    assert not any("My__Kids" in t for t in tables)


@responses.activate
def test_list_tables_in_namespace_accepts_alias():
    """The ``namespace`` table option resolves schema aliases; the namespace
    LISTING used to compare raw and silently return ``[]`` for the same
    alias string — the connector's own alias contract, applied to both."""
    responses.get(f"{SERVICE_URL}$metadata", body=R43_ALIAS_METADATA, status=200)
    c = _make({"token": "t"})
    assert c.list_tables_in_namespace(["com.example.model"]) == ["Orders"]
    assert c.list_tables_in_namespace(["m"]) == ["Orders"]


@responses.activate
def test_enumerate_contained_paths_warns_on_unresolvable_root(caplog):
    """An entity set whose EntityType reference resolves to nothing still
    lists (its read fails loudly), but its contained children silently
    didn't enumerate — now it says so."""
    broken = R43_ALIAS_METADATA.replace('EntityType="m.Order"', 'EntityType="m.Missing"')
    responses.get(f"{SERVICE_URL}$metadata", body=broken, status=200)
    c = _make({"token": "t"})
    with caplog.at_level(logging.WARNING):
        tables = c.list_tables()
    assert "Orders" in tables
    assert "Cannot enumerate contained paths" in caplog.text


@responses.activate
def test_fence_probe_self_checks_orderby_desc():
    """A server that silently ignores `$orderby ... desc` hands the fence
    probe a stale first row — the fence pins, get_partitions returns []
    every trigger, and the stream stalls silently with data pending. The
    probe now self-checks (`cursor gt <probed max>` must be empty) and
    raises actionably instead."""
    _mock_nested_metadata()

    # Orderby-ignoring server: always default order — probe gets the OLD
    # row; the self-check (gt) finds the newer one.
    def _parents(request):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
        rows = [
            {"Id": 1, "Name": "2024-01-01T00:00:00Z"},
            {"Id": 2, "Name": "2024-06-01T00:00:00Z"},
        ]
        m = re.search(r"Name gt (\S+)", flt)
        if m:
            rows = [r for r in rows if r["Name"] > m.group(1).strip("'")]
        return (200, {}, json.dumps({"value": rows[:1]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    with pytest.raises(ValueError, match="ignoring \\$orderby"):
        c.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)


@responses.activate
def test_keyless_delta_enabled_raises_auto_falls_back():
    """Delta rows MERGE on the primary key; a keyless tombstone MERGEs
    against nothing and the deletion is silently lost — and the
    per-tombstone raise is gated on a non-empty key list, so it can never
    fire. Enabled → eager curated error; auto → snapshot fallback with no
    probe (no Prefer request, no synthetics)."""
    responses.get(f"{SERVICE_URL}$metadata", body=R45_KEYLESS_METADATA, status=200)
    prefer_seen = {"n": 0}

    def _logs(request):
        if request.headers.get("Prefer"):
            prefer_seen["n"] += 1
        return (200, {}, '{"value": [{"At": "2024-01-01T00:00:00Z", "Msg": "m"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Logs", callback=_logs)
    c = _make()
    with pytest.raises(ValueError, match="primary key"):
        c.read_table("Logs", {}, {"delta_tracking": "enabled"})
    records, offset = _make().read_table("Logs", {}, {"delta_tracking": "auto"})
    rows = list(records)
    assert prefer_seen["n"] == 0  # never probed
    assert rows and all("_deleted" not in r for r in rows)
    assert offset.get("snapshot_done") is True


# ---------------------------------------------------------------------------
# Round 46 — metadata-cache eviction race, fence self-check insert-race
# re-probe, delta fresh-link attribution, lb_cycle_started leak, lb_history
# sanitization
# ---------------------------------------------------------------------------


def test_metadata_cache_eviction_survives_concurrent_puts():
    """The lock-free eviction used to iterate the live dict while sibling
    threads insert/pop (> cap distinct service_urls on one driver) —
    "dictionary changed size during iteration" / KeyError escaped into the
    caller's metadata fetch. Candidates are now snapshotted first."""
    import sys
    import threading

    from databricks.labs.community_connector.sources.odata.odata import (
        _METADATA_CACHE,
        _metadata_cache_put,
    )

    _METADATA_CACHE.clear()
    errors = []
    # Shrink the GIL switch interval so the eviction loop's iterate-vs-pop
    # interleavings actually occur within the test's budget — at the 5ms
    # default the pre-fix race fires only sporadically.
    prev_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:

        def _hammer(tid):
            try:
                for i in range(600):
                    _metadata_cache_put(f"https://svc-{tid}-{i}.x/", ("x", None, None, float(i)))
            except Exception as exc:  # pragma: no cover - the pre-fix failure
                errors.append(repr(exc))

        threads = [threading.Thread(target=_hammer, args=(t,)) for t in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        sys.setswitchinterval(prev_interval)
        _METADATA_CACHE.clear()
    assert errors == []


@responses.activate
def test_fence_self_check_survives_probe_race_insert():
    """A row inserted in the one-RTT window between the fence probe and the
    gt self-check made the check accuse a COMPLIANT server of ignoring
    $orderby — a spurious hard trigger failure scaling with write rate. On
    contradiction the check now re-probes with desc: at-or-above the
    check-found row proves desc works (PASS cached); the fence stays at the
    original probed max."""
    _mock_nested_metadata()
    state = {"rows": [{"Id": 1, "Name": "2024-01-01T00:00:00Z"}], "probes": 0}

    def _parents(request):
        from urllib.parse import parse_qs, unquote, urlparse

        q = parse_qs(urlparse(request.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        rows = list(state["rows"])
        m = re.search(r"Name gt (\S+)", flt)
        if m:
            # The gt self-check: a fresh row landed after the first probe.
            state["rows"].append({"Id": 2, "Name": "2024-06-01T00:00:00Z"})
            rows = [r for r in state["rows"] if r["Name"] > m.group(1).strip("'")]
        elif "desc" in unquote(q.get("$orderby", [""])[0]):
            state["probes"] += 1
            rows = sorted(rows, key=lambda r: r["Name"], reverse=True)
        return (200, {}, json.dumps({"value": rows[:1]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    c = _make()
    offset = c.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)
    # No spurious raise; fence stays at the original probed max.
    assert offset == {"cursor": "2024-01-01T00:00:00Z"}
    assert state["probes"] == 2  # initial probe + disambiguating re-probe
    # PASS verdict cached — the next trigger skips the self-check entirely.
    assert c._cached_capability("fence_desc_ok", table_name="Parents") is True
    n_before = len(responses.calls)
    c2 = _make()
    c2.latest_offset("Parents__Children", {"cursor_field": "Name"}, None)
    assert len(responses.calls) == n_before + 1  # one probe, no gt check


@responses.activate
def test_edm_binary_base64url_decoded_to_bytes():
    """OData v4.01 JSON encodes Edm.Binary as base64url (- / _ alphabet).
    The framework row parser only understands standard base64, so without an
    in-connector decode the payload is silently corrupted. read_table must
    emit raw bytes (which the parser then passes through untouched)."""
    import base64 as _b64

    from databricks.labs.community_connector.libs.utils import parse_value
    from pyspark.sql.types import BinaryType

    raw = bytes([0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF])  # -> base64url contains - and _
    wire = _b64.urlsafe_b64encode(raw).decode()
    assert "-" in wire or "_" in wire  # this payload actually exercises base64url
    responses.get(f"{SERVICE_URL}$metadata", body=_BINARY_MD, status=200)
    responses.get(f"{SERVICE_URL}Fs", json={"value": [{"Id": 1, "Blob": wire}]})
    c = _make()
    schema = c.get_table_schema("Fs", {})
    rows, _ = c.read_table("Fs", None, {})
    emitted = list(rows)[0]["Blob"]
    assert emitted == raw, f"binary not decoded to bytes at emit: {emitted!r}"
    blob_type = [f.dataType for f in schema.fields if f.name == "Blob"][0]
    assert isinstance(blob_type, BinaryType)
    assert parse_value(emitted, blob_type) == raw  # survives the framework parser


@responses.activate
def test_edm_binary_standard_base64_still_decoded():
    """A server that (non-spec) sends standard base64 for Edm.Binary must
    still decode — urlsafe_b64decode leaves +/ untouched, so both alphabets
    round-trip through the same path."""
    import base64 as _b64

    raw = bytes([0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF])
    wire = _b64.b64encode(raw).decode()  # standard alphabet: contains + and /
    assert "+" in wire or "/" in wire
    responses.get(f"{SERVICE_URL}$metadata", body=_BINARY_MD, status=200)
    responses.get(f"{SERVICE_URL}Fs", json={"value": [{"Id": 1, "Blob": wire}]})
    c = _make()
    emitted = list(c.read_table("Fs", None, {})[0])[0]["Blob"]
    assert emitted == raw


@responses.activate
def test_edm_binary_null_and_undecodable_preserved():
    """A null binary stays None; a value that is not decodable keeps its
    original form (no crash — the framework fallback then runs)."""
    responses.get(f"{SERVICE_URL}$metadata", body=_BINARY_MD, status=200)
    responses.get(
        f"{SERVICE_URL}Fs",
        json={"value": [{"Id": 1, "Blob": None}, {"Id": 2, "Blob": "!!not base64!!"}]},
    )
    c = _make()
    rows = list(c.read_table("Fs", None, {})[0])
    assert rows[0]["Blob"] is None
    assert rows[1]["Blob"] == "!!not base64!!"  # undecodable — left as-is, no raise


@responses.activate
def test_keyless_cursor_field_fails_loudly():
    """cursor_field reports ingestion_type=cdc, which apply_changes MERGEs on
    the primary key; a keyless entity type declares none, so the MERGE has no
    key — silent loss by construction. Mirror the keyless delta gate: raise,
    don't emit a keyless CDC contract."""
    responses.get(f"{SERVICE_URL}$metadata", body=_KEYLESS_MD, status=200)
    c = _make()
    with pytest.raises(ValueError, match="no key in .metadata"):
        c.read_table_metadata("Views", {"cursor_field": "M"})


@responses.activate
def test_keyless_snapshot_still_ok():
    """A keyless set WITHOUT cursor_field is a plain snapshot (no MERGE, no
    key needed) — must still succeed."""
    responses.get(f"{SERVICE_URL}$metadata", body=_KEYLESS_MD, status=200)
    c = _make()
    meta = c.read_table_metadata("Views", {})
    assert meta == {"primary_keys": [], "cursor_field": None, "ingestion_type": "snapshot"}


# ---------------------------------------------------------------------------
# Round 52 — read_table_metadata mirrors _validate_select_columns; fence
# self-check tolerates the insert-then-delete race (still raises genuine
# desc-ignore)
# ---------------------------------------------------------------------------


@responses.activate
def test_metadata_validates_select_drops_cursor():
    """read_table_metadata must fail in the same place as the read when a
    select drops the cursor_field (dispatch runs _validate_select_columns for
    every read; metadata used to skip it and report a valid CDC source)."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="omits cursor_field"):
        c.read_table_metadata("Customers", {"cursor_field": "ModifiedAt", "select": "Id"})


@responses.activate
def test_metadata_validates_select_drops_pk():
    """Same, for a select that drops a primary key."""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="omits primary-key"):
        c.read_table_metadata(
            "Customers", {"cursor_field": "ModifiedAt", "select": "Name,ModifiedAt"}
        )


@responses.activate
def test_fence_check_tolerates_insert_then_delete_race():
    """A desc-COMPLIANT server where a row inserted after the max-probe is
    deleted before the re-probe must NOT raise: the desc re-probe surfaces the
    true (post-delete) max and no row persists above it. Round-46 handled the
    plain-insert race; this covers insert-then-delete."""
    from urllib.parse import parse_qs, unquote, urlparse

    responses.get(f"{SERVICE_URL}$metadata", body=_FENCE_MD, status=200)
    gt_calls = {"n": 0}

    def _cb(req):
        q = parse_qs(urlparse(req.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        orderby = unquote(q.get("$orderby", [""])[0])
        if "Seq desc" in orderby:  # both desc probes return the compliant max
            return (200, {}, json.dumps({"value": [{"Seq": 10}]}))
        if "Seq gt" in flt:
            gt_calls["n"] += 1
            # self-check sees the raced insert; still_above sees it deleted.
            body = {"value": [{"Seq": 11}]} if gt_calls["n"] == 1 else {"value": []}
            return (200, {}, json.dumps(body))
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_cb)
    c = _make()
    # Must not raise — the compliant server is not accused of ignoring $orderby.
    off = c.latest_offset("Parents__Children", {"cursor_field": "Seq"}, None)
    assert off is not None


@responses.activate
def test_fence_check_still_raises_on_genuine_desc_ignore():
    """A server that genuinely ignores $orderby (desc probe returns a non-max
    while rows persist above it) must still raise — the fix must not mask the
    real failure."""
    from urllib.parse import parse_qs, unquote, urlparse

    responses.get(f"{SERVICE_URL}$metadata", body=_FENCE_MD, status=200)

    def _cb(req):
        q = parse_qs(urlparse(req.url).query)
        flt = unquote(q.get("$filter", [""])[0])
        orderby = unquote(q.get("$orderby", [""])[0])
        if "Seq desc" in orderby:  # ignores desc: returns a stale non-max
            return (200, {}, json.dumps({"value": [{"Seq": 5}]}))
        if "Seq gt" in flt:  # a row genuinely persists above the probed value
            return (200, {}, json.dumps({"value": [{"Seq": 10}]}))
        return (200, {}, json.dumps({"value": []}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_cb)
    c = _make()
    with pytest.raises(ValueError, match="ignoring .orderby"):
        c.latest_offset("Parents__Children", {"cursor_field": "Seq"}, None)
