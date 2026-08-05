"""Shared fixtures, metadata documents, and HTTP helpers for the OData
connector unit tests.

Extracted from the former monolithic ``test_odata_lakeflow_connect.py`` so the
per-feature test modules (``test_odata_contained.py`` etc.) can share one copy
of the ~40 ``*_METADATA_XML`` CSDL documents and the ``responses`` callback
builders. ``import *`` re-exports every constant and helper via ``__all__``.
"""


import json
import logging
import os
import re
import time

import pytest
import requests
import responses

from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect
from databricks.labs.community_connector.sources.odata.odata import _odata_literal
from pyspark.sql.types import DecimalType, IntegerType, StringType, TimestampType
from tests.unit.sources.test_suite import LakeflowConnectTests
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests

__all__ = [
    "SERVICE_URL",
    "METADATA_XML",
    "MULTI_SCHEMA_METADATA",
    "INHERITED_METADATA_XML",
    "CYCLE_METADATA_XML",
    "DELTA_LINK_V1",
    "DELTA_LINK_V2",
    "NESTED_METADATA_XML",
    "RECURSIVE_METADATA_XML",
    "DECIMAL_METADATA_XML",
    "PROBE_METADATA_XML",
    "PROBE_TABLE",
    "_EXPAND_AUTO_OPTS",
    "GUID_METADATA_XML",
    "_GUID",
    "GUID_CURSOR_METADATA_XML",
    "_GUID2",
    "REDECLARE_METADATA_XML",
    "STREAM_METADATA_XML",
    "METADATA_XML_V2",
    "TYPEDEF_METADATA_XML",
    "DUNDER_SET_METADATA_XML",
    "DUPSET_METADATA_XML",
    "NONNULL_METADATA_XML",
    "NONNULL_STREAM_METADATA_XML",
    "DUNDER_KIDS_METADATA_XML",
    "COLLIDE_METADATA_XML",
    "NONNULL_FLAT_METADATA_XML",
    "R39_FLIP_METADATA",
    "R40_TYPEDEF_METADATA",
    "R40_PATHKEY_METADATA",
    "R40_DUNDER_NAV_METADATA",
    "R41_INT64_METADATA",
    "R42_KEYLESS_MID_METADATA",
    "R43_CI_COLLATION_METADATA",
    "R43_ALIAS_METADATA",
    "R45_DIGIT_PK_METADATA",
    "R45_KEYLESS_METADATA",
    "_BINARY_MD",
    "_COLLISION_MD",
    "_FK_NULL_MD",
    "_KEYLESS_MD",
    "_FENCE_MD",
    "_mock_metadata",
    "_make",
    "_drop_lb",
    "_mock_multi_metadata",
    "_mock_inherited_metadata",
    "_delta_bootstrap_body",
    "_mock_nested_metadata",
    "_mock_recursive_metadata",
    "_pagination_dataset",
    "_churn_walk_opts",
    "_churn_children_cb",
    "_expand_inner_park_batch1",
    "_expand_l0_park_batch1",
    "_expand_l0_page1",
    "_patch_sleep",
    "_mock_probe_metadata",
    "_skip_probe_preflight",
    "_probe_filter_floor",
    "_probe_mids_callback",
    "_mids_reject_expand_callback",
    "_batch_responder",
    "_too_many_parts_responder",
    "_expand_auto_roots_callback",
    "_switch_opts",
    "_switch_tree",
    "_expand_urls",
    "_leaves_or_probe_callback",
    "_mock_guid_metadata",
    "_run_flip_preflight",
]



SERVICE_URL = "https://example.com/odata/"

METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Demo" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="Order">
        <Key><PropertyRef Name="OrderId"/></Key>
        <Property Name="OrderId" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Total" Type="Edm.Decimal"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Customers" EntityType="Demo.Customer"/>
        <EntitySet Name="Orders" EntityType="Demo.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


MULTI_SCHEMA_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Sales" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Account" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Order">
        <Key><PropertyRef Name="OrderId"/></Key>
        <Property Name="OrderId" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="SalesContainer">
        <EntitySet Name="Customers" EntityType="Sales.Customer"/>
        <EntitySet Name="Orders" EntityType="Sales.Order"/>
      </EntityContainer>
    </Schema>
    <Schema Namespace="HR" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="EmployeeId"/></Key>
        <Property Name="EmployeeId" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Department" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="HRContainer">
        <EntitySet Name="Customers" EntityType="HR.Customer"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# CSDL BaseType inheritance (OData v4 §8.4)
# ---------------------------------------------------------------------------

# Microsoft Graph and most real OData v4 services declare keys and
# properties on abstract base types and inherit them through a chain of
# derived types. The connector must walk that chain on metadata lookups
# or it returns empty PKs and incomplete schemas.

INHERITED_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="microsoft.graph" Alias="graph" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <!-- Abstract root: declares the Key + id property everything inherits. -->
      <EntityType Name="entity" Abstract="true">
        <Key><PropertyRef Name="id"/></Key>
        <Property Name="id" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <!-- Mid-level: adds deletedDateTime; alias-qualified BaseType. -->
      <EntityType Name="directoryObject" BaseType="graph.entity">
        <Property Name="deletedDateTime" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <!-- Leaf: adds user-specific fields, FQN BaseType. -->
      <EntityType Name="user" BaseType="microsoft.graph.directoryObject">
        <Property Name="displayName" Type="Edm.String"/>
        <Property Name="mail" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="GraphService">
        <EntitySet Name="users" EntityType="microsoft.graph.user"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


CYCLE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="bad" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="A" BaseType="bad.B">
        <Property Name="a_field" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="B" BaseType="bad.A">
        <Key><PropertyRef Name="b_field"/></Key>
        <Property Name="b_field" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="things" EntityType="bad.A"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Delta tracking (Prefer: odata.track-changes)
# ---------------------------------------------------------------------------


# Realistic delta link shape — server-minted opaque token. The connector
# treats this URL as the offset payload to resume from.
DELTA_LINK_V1 = f"{SERVICE_URL}Customers?$deltatoken=tok-1"
DELTA_LINK_V2 = f"{SERVICE_URL}Customers?$deltatoken=tok-2"


# ---------------------------------------------------------------------------
# Contained navigation properties (ContainsTarget="true")
# ---------------------------------------------------------------------------


NESTED_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Nested" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Parent">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <NavigationProperty Name="Children" Type="Collection(Nested.Child)" ContainsTarget="true"/>
        <NavigationProperty Name="Tags" Type="Collection(Nested.Tag)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Child">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Label" Type="Edm.String"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Notes" Type="Collection(Nested.Note)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Note">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Text" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Tag">
        <Key>
          <PropertyRef Name="Category"/>
          <PropertyRef Name="Value"/>
        </Key>
        <Property Name="Category" Type="Edm.String" Nullable="false"/>
        <Property Name="Value" Type="Edm.String" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Parents" EntityType="Nested.Parent"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


RECURSIVE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Rec" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Node">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Label" Type="Edm.String"/>
        <NavigationProperty Name="Children" Type="Collection(Rec.Node)" ContainsTarget="true"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Nodes" EntityType="Rec.Node"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


DECIMAL_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Dec" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Money">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Exact" Type="Edm.Decimal" Precision="10" Scale="2"/>
        <Property Name="Wide" Type="Edm.Decimal"/>
        <Property Name="Varying" Type="Edm.Decimal" Precision="20" Scale="variable"/>
        <Property Name="BigId" Type="Edm.Decimal" Precision="38"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Moneys" EntityType="Dec.Money"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# cursor_probe — sparse-change optimization for deep leaf-cursor reads
# ---------------------------------------------------------------------------

PROBE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Probe" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Mids" Type="Collection(Probe.Mid)" ContainsTarget="true"/>
        <NavigationProperty Name="Plains" Type="Collection(Probe.Plain)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
        <Property Name="MidOnly" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Leaves" Type="Collection(Probe.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="Plain">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Items" Type="Collection(Probe.Item)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Roots" EntityType="Probe.Root"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

PROBE_TABLE = "Roots__Mids__Leaves"


# ---------------------------------------------------------------------------
# expand_contained=auto — preflighted nested-$expand with N+1 fallback
# ---------------------------------------------------------------------------

_EXPAND_AUTO_OPTS = {
    "cursor_field": "RecordLastModified",
    "expand_contained": "auto",
    "cursor_probe": "false",  # keep the N+1 fallback a plain walk (no $batch)
    "pagination": "nextlink",
}


# ---------------------------------------------------------------------------
# Round-27 fixes: typed literals, queue-park preservation, watermark floors,
# %24filter folding, $batch envelope retry, curated option validation
# ---------------------------------------------------------------------------

GUID_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="G" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Account">
        <Key><PropertyRef Name="AccountId"/></Key>
        <Property Name="AccountId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <NavigationProperty Name="Contacts" Type="Collection(G.Contact)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Contact">
        <Key><PropertyRef Name="ContactId"/></Key>
        <Property Name="ContactId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityType Name="DayBatch">
        <Key><PropertyRef Name="Day"/></Key>
        <Property Name="Day" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Items" Type="Collection(G.DayItem)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="DayItem">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Accounts" EntityType="G.Account"/>
        <EntitySet Name="DayBatches" EntityType="G.DayBatch"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

_GUID = "550e8400-e29b-41d4-a716-446655440000"


GUID_CURSOR_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="G" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Account">
        <Key><PropertyRef Name="AccountId"/></Key>
        <Property Name="AccountId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <NavigationProperty Name="Contacts" Type="Collection(G.Contact)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Contact">
        <Key><PropertyRef Name="ContactId"/></Key>
        <Property Name="ContactId" Type="Edm.Guid" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Accounts" EntityType="G.Account"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

_GUID2 = "0a1b2c3d-4e5f-6789-abcd-ef0123456789"


REDECLARE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="R" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Base">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="V" Type="Edm.Int32"/>
      </EntityType>
      <EntityType Name="Derived" BaseType="R.Base">
        <Property Name="V" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Deriveds" EntityType="R.Derived"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


STREAM_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="S" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Doc">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
        <Property Name="Content" Type="Edm.Stream"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Docs" EntityType="S.Doc"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round-31 fixes: metadata process-cache TTL, symlink-safe cache writes,
# TypeDefinition typing, __-named flat sets, 408 retry, auth_type-gated
# refresh, wall-clock token deadline, jittered backoff, Decimal literals
# ---------------------------------------------------------------------------


METADATA_XML_V2 = METADATA_XML.replace('Name="Customers"', 'Name="CustomersV2"')


TYPEDEF_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="T" Alias="ta" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <TypeDefinition Name="Code" UnderlyingType="Edm.String"/>
      <TypeDefinition Name="Qty" UnderlyingType="Edm.Int64"/>
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Code" Type="T.Code"/>
        <Property Name="Qty" Type="ta.Qty"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Items" EntityType="T.Item"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


DUNDER_SET_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="My__Set" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


DUPSET_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C1">
        <EntitySet Name="Things" EntityType="D.Thing"/>
      </EntityContainer>
      <EntityContainer Name="C2">
        <EntitySet Name="Things" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round-32 fixes: tombstone NULL padding, exclusion-aware memos, stream
# nullability, shared delta verdict, dunder-prefix children, rotation stash
# ---------------------------------------------------------------------------


NONNULL_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="N" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Customer">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Name" Type="Edm.String" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Customers" EntityType="N.Customer"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


NONNULL_STREAM_METADATA_XML = STREAM_METADATA_XML.replace(
    '<Property Name="Content" Type="Edm.Stream"/>',
    '<Property Name="Content" Type="Edm.Stream" Nullable="false"/>',
)


DUNDER_KIDS_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Kid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Note" Type="Edm.String"/>
      </EntityType>
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Kids" Type="Collection(D.Kid)" ContainsTarget="true"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="My__Set" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


COLLIDE_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="D" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Sub">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Set" Type="Collection(D.Sub)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="My" EntityType="D.Root"/>
        <EntitySet Name="My__Set" EntityType="D.Thing"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


NONNULL_FLAT_METADATA_XML = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="N" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Req" Type="Edm.String" Nullable="false"/>
        <Property Name="Opt" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Items" EntityType="N.Item"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round 39 — incomparable cursor pairs, numeric same-instant, preflight
# verdict hygiene (rendering flip / annotation deferral / race taint),
# absent expanded property, streaming-snapshot quiesce marker
# ---------------------------------------------------------------------------

R39_FLIP_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="P" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Mids" Type="Collection(P.Mid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
        <NavigationProperty Name="Leaves" Type="Collection(P.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Roots" EntityType="P.Root"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round 40 — mixed-type cursor ordering bridge, delta wire-shape hardening,
# CSDL TypeDefinition/complex-key/__-nav resolution
# ---------------------------------------------------------------------------

R40_TYPEDEF_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="ta" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <TypeDefinition Name="Qty64" UnderlyingType="Edm.Int64"/>
      <TypeDefinition Name="When" UnderlyingType="Edm.DateTimeOffset"/>
      <EntityType Name="Item">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Qty" Type="ta.Qty64"/>
        <Property Name="At" Type="ta.When"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Items" EntityType="ta.Item"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

R40_PATHKEY_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="pk" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <ComplexType Name="Info"><Property Name="Code" Type="Edm.String"/></ComplexType>
      <EntityType Name="Thing">
        <Key><PropertyRef Name="Info/Code" Alias="IC"/></Key>
        <Property Name="Info" Type="pk.Info"/>
        <Property Name="Label" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Things" EntityType="pk.Thing"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""

R40_DUNDER_NAV_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="dn" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Parent">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="My__Kids" Type="Collection(dn.Kid)" ContainsTarget="true"/>
        <NavigationProperty Name="Pets" Type="Collection(dn.Kid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Kid">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="dn.Parent"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round 41 — same-instant boundary trim, typed cursor-filter rendering,
# probe-fetch transient classification, +-encoded orderby
# ---------------------------------------------------------------------------

R41_INT64_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="sq" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Event">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="Seq" Type="Edm.Int64"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Events" EntityType="sq.Event"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


R42_KEYLESS_MID_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="kq" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Mids" Type="Collection(kq.Mid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Property Name="Code" Type="Edm.String"/>
        <NavigationProperty Name="Leaves" Type="Collection(kq.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
        <Property Name="RecordLastModified" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Roots" EntityType="kq.Root"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round 43 — collation-honest park-resume seeks (identity anchors, three-way
# order, vanished-anchor reset), NaN lookback factor, alias-aware namespace
# listing, $batch probe duplicate-id consistency, discovery warning
# ---------------------------------------------------------------------------

R43_CI_COLLATION_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="cq" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Parent">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Children" Type="Collection(cq.Child)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Child">
        <Key><PropertyRef Name="Cid"/></Key>
        <Property Name="Cid" Type="Edm.Int32" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="cq.Parent"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


R43_ALIAS_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="com.example.model" Alias="m" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      </EntityType>
      <EntityContainer Name="C">
        <EntitySet Name="Orders" EntityType="m.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round 45 — same-rendering (not same-instant) chain-element conflation,
# fence desc self-check, delta stored-link 4xx curation, keyless-delta gate
# ---------------------------------------------------------------------------

R45_DIGIT_PK_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="q" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Root">
        <Key><PropertyRef Name="Id"/></Key>
        <Property Name="Id" Type="Edm.String" Nullable="false"/>
        <NavigationProperty Name="Mids" Type="Collection(q.Mid)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Mid">
        <Key><PropertyRef Name="MId"/></Key>
        <Property Name="MId" Type="Edm.Int32" Nullable="false"/>
        <NavigationProperty Name="Leaves" Type="Collection(q.Leaf)" ContainsTarget="true"/>
      </EntityType>
      <EntityType Name="Leaf">
        <Key><PropertyRef Name="LId"/></Key>
        <Property Name="LId" Type="Edm.Int32" Nullable="false"/>
        <Property Name="ModifiedAt" Type="Edm.DateTimeOffset"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Roots" EntityType="q.Root"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


R45_KEYLESS_METADATA = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="k" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Log">
        <Property Name="At" Type="Edm.DateTimeOffset"/>
        <Property Name="Msg" Type="Edm.String"/>
      </EntityType>
      <EntityContainer Name="C"><EntitySet Name="Logs" EntityType="k.Log"/></EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# Round 49 — Edm.Binary base64url decode, delta reserved-column collision,
# capability_cache_load symmetric memo-race gate, mode-aware no-progress log
# ---------------------------------------------------------------------------


_BINARY_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="F"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
   <Property Name="Blob" Type="Edm.Binary"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Fs" EntityType="NS.F"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


_COLLISION_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="W"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
   <Property Name="_deleted" Type="Edm.String"/>
   <Property Name="_lc_sequence" Type="Edm.String"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Widgets" EntityType="NS.W"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


# ---------------------------------------------------------------------------
# Round 50 — null ancestor FK fail-loud guard, reserved-column guard mirrored
# into read_table_metadata
# ---------------------------------------------------------------------------


_FK_NULL_MD = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices><Schema Namespace="Nested" xmlns="http://docs.oasis-open.org/odata/ns/edm">
    <EntityType Name="Parent"><Key><PropertyRef Name="Id"/></Key>
      <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      <Property Name="Name" Type="Edm.String"/>
      <NavigationProperty Name="Children" Type="Collection(Nested.Child)" ContainsTarget="true"/></EntityType>
    <EntityType Name="Child"><Key><PropertyRef Name="Id"/></Key>
      <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
      <Property Name="Label" Type="Edm.String"/></EntityType>
    <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="Nested.Parent"/></EntityContainer>
  </Schema></edmx:DataServices></edmx:Edmx>"""


_KEYLESS_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="NS" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="V">
   <Property Name="X" Type="Edm.Int32"/>
   <Property Name="M" Type="Edm.DateTimeOffset"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Views" EntityType="NS.V"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


_FENCE_MD = """<?xml version="1.0"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices><Schema Namespace="N" xmlns="http://docs.oasis-open.org/odata/ns/edm">
  <EntityType Name="Parent"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
   <Property Name="Seq" Type="Edm.Int32"/>
   <NavigationProperty Name="Children" Type="Collection(N.Child)" ContainsTarget="true"/></EntityType>
  <EntityType Name="Child"><Key><PropertyRef Name="Id"/></Key>
   <Property Name="Id" Type="Edm.Int32" Nullable="false"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="Parents" EntityType="N.Parent"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


def _mock_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=METADATA_XML, status=200)


def _make(options=None):
    base = {"service_url": SERVICE_URL}
    if options:
        base.update(options)
    return ODataLakeflowConnect(base)


def _drop_lb(offset):
    """Strip non-logical bookkeeping from an offset for stable equality asserts:
    the ``auto`` cursor_lookback state (every ``lb_*`` key — the duration
    history and the in-flight cycle anchor, both non-deterministic
    wall-clock) and the persisted capability verdicts (``cursor_probe_ok`` /
    ``batch_ok`` / ``batch_size_ok`` / ``or_filter_ok``, one-time-set markers
    threaded across microbatches). Tests assert the cursor/resume state, not this
    bookkeeping — mirrors the no-progress comparison in ``_finalize_cursor_read``."""
    _bookkeeping = {
        "cursor_probe_ok",
        "batch_ok",
        "batch_size_ok",
        "or_filter_ok",
        "expand_ok",
        "delta_ok",
    }
    return {
        k: v for k, v in (offset or {}).items() if k not in _bookkeeping and not k.startswith("lb_")
    }


def _mock_multi_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=MULTI_SCHEMA_METADATA, status=200)


def _mock_inherited_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=INHERITED_METADATA_XML, status=200)


def _delta_bootstrap_body(value, delta_link=DELTA_LINK_V1, next_link=None):
    """Construct a delta-bootstrap response body. Defaults match the
    OData v4 spec: full snapshot + terminal ``@odata.deltaLink``."""
    body = {"@odata.context": f"{SERVICE_URL}$metadata#Customers", "value": value}
    if delta_link is not None:
        body["@odata.deltaLink"] = delta_link
    if next_link is not None:
        body["@odata.nextLink"] = next_link
    return body


def _mock_nested_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=NESTED_METADATA_XML, status=200)


def _mock_recursive_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=RECURSIVE_METADATA_XML, status=200)


def _pagination_dataset():
    return [{"Id": i, "ModifiedAt": f"2024-01-{i:02d}T00:00:00Z"} for i in range(1, 6)]


def _churn_walk_opts():
    return {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "3",
        "pagination": "nextlink",
    }


def _churn_children_cb(rows):
    """Children endpoint callback honoring the walk's ``cursor gt`` filter."""

    def cb(req):
        from urllib.parse import parse_qs, unquote, urlparse

        flt = unquote(parse_qs(urlparse(req.url).query).get("$filter", [""])[0])
        out = rows
        m = re.search(r"ModifiedAt gt (\S+)", flt)
        if m:
            out = [r for r in rows if r["ModifiedAt"] > m.group(1)]
        return (200, {}, json.dumps({"value": out}))

    return cb


def _expand_inner_park_batch1():
    """Shared setup: a parked LEVEL-1 inner-collection continuation under
    parent 1 (the server paged Children with ``Children@odata.nextLink``),
    constructed DIRECTLY.

    The depth-first drainer drains an inner continuation in the same batch it
    discovers it, so a real batch-1 seldom parks one mid-collection — but a
    parked inner continuation is still a valid resume state (the cap can fire
    with one pending, and old offsets carry them). The recovery path
    (:meth:`_recover_expand_item`) is unchanged; resuming from this hand-built
    offset exercises it in isolation, exactly as a real park would."""
    inner_link = f"{SERVICE_URL}Parents(1)/Children?$skiptoken=t1"
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "3",
        "pagination": "nextlink",
    }
    offset1 = {
        "pending_fetches": [
            {
                "url": inner_link,
                "level": 1,
                "chain": [{"Id": 1}],
                "cur_val": "2024-01-01T00:00:00Z",
                "skip": 0,
            }
        ],
        "cursor": "2024-01-01T00:00:00Z",
        "running_max_cursor": "2024-01-01T00:00:00Z",
    }
    return opts, offset1


def _expand_l0_park_batch1(c, parents_cb):
    """Batch 1 of an expand read that parks a LEVEL-0 top continuation
    (the server's top-level $skiptoken link) in ``pending_fetches``."""
    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=parents_cb)
    opts = {
        "expand_contained": "true",
        "cursor_field": "Name",
        "max_records_per_batch": "1",
        "pagination": "nextlink",
    }
    recs1, offset1 = c.read_table("Parents__Children", {}, opts)
    assert [r["Id"] for r in recs1] == [11]
    pending = offset1["pending_fetches"]
    assert len(pending) == 1 and pending[0]["level"] == 0
    assert "skiptoken=top1" in pending[0]["url"]
    return opts, offset1


def _expand_l0_page1():
    return {
        "value": [
            {
                "Id": 1,
                "Name": "2024-01-01T00:00:00Z",
                "Children": [{"Id": 11, "Label": "a"}],
            }
        ],
        "@odata.nextLink": f"{SERVICE_URL}Parents?$skiptoken=top1",
    }


# ---------------------------------------------------------------------------
# 429 / 503 retry with backoff
# ---------------------------------------------------------------------------


def _patch_sleep(monkeypatch):
    """Capture every ``time.sleep`` call from the connector retry loop.

    Returns the list the sleeps are appended into — tests assert on
    durations directly. The lambda short-circuits the real sleep so the
    suite stays sub-second. Backoff jitter is pinned to its upper bound
    (``random.uniform → 1.0``) so the captured durations are the
    deterministic exponential sequence (1, 2, 4 …); jitter itself is
    covered by ``test_backoff_delay_is_jittered``.
    """
    sleeps: list[float] = []
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.time.sleep",
        lambda s: sleeps.append(s),
    )
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.odata.odata.random.uniform",
        lambda a, b: b,
    )
    return sleeps


def _mock_probe_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=PROBE_METADATA_XML, status=200)


def _skip_probe_preflight(c, table=PROBE_TABLE):
    """Pre-seed the cursor_probe capability cache as verified, so a test can
    exercise probe READ behaviour without also mocking the preflight requests.
    The preflight itself is covered by dedicated tests."""
    segs = tuple(table.split("__"))
    # Cache value is ``(problem, conclusive, race_tainted)``: no problem,
    # conclusively verified, no race contamination.
    c.__dict__.setdefault("_cursor_probe_verified", {})[(segs, None)] = (None, True, False)


def _probe_filter_floor(request):
    """Parse the ``RecordLastModified gt <iso>`` floor from a request's
    ``$filter`` (ISO timestamps go on the wire bare). ``None`` when no
    cursor floor is present (first batch)."""
    from urllib.parse import parse_qs, unquote, urlparse

    flt = unquote(parse_qs(urlparse(request.url).query).get("$filter", [""])[0])
    m = re.search(r"RecordLastModified gt ([0-9T:\-.Z]+)", flt)
    return m.group(1) if m else None


def _probe_mids_callback(inner_expand_newest):
    """Callback for ``Roots(1)/Mids``: returns Mid 10's key for the preflight's
    leaf-parent enumeration, and Mid 10 with a probe-shaped ``Leaves`` whose
    newest cursor is ``inner_expand_newest`` for the inner-$expand check."""

    def _cb(request):
        from urllib.parse import unquote

        if "$expand=Leaves" in unquote(request.url):
            return (
                200,
                {},
                json.dumps(
                    {"value": [{"Id": 10, "Leaves": [{"RecordLastModified": inner_expand_newest}]}]}
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 10}]}))

    return _cb


def _mids_reject_expand_callback(request):
    """Callback for ``Roots(1)/Mids``: 400 on the nested-``$expand`` probe (a
    server that rejects inner ``$orderby``/``$top``/``$select``, e.g. Hexagon
    Smart API), and a plain Id list for the N+1 enumeration / fallback."""
    from urllib.parse import unquote

    if "$expand=Leaves" in unquote(request.url):
        return (400, {}, json.dumps({"error": {"message": "inner $expand not supported"}}))
    return (200, {}, json.dumps({"value": [{"Id": 10}]}))


# ---------------------------------------------------------------------------
# cursor_probe=batch — $batch hydrate fallback + auto cascade
# ---------------------------------------------------------------------------


def _batch_responder(route_map):
    """Build a ``responses`` POST callback for the OData ``$batch`` endpoint.

    ``route_map`` is a list of ``(url_substring, body_dict)`` pairs; for each
    posted sub-request the first substring that occurs in its ``url`` wins and
    its ``body`` is returned with sub-status 200 (404 + empty when none match).
    Records every posted sub-request URL on ``.seen`` for assertions."""
    seen: list[str] = []

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        out = []
        for r in reqs:
            url = r["url"]
            seen.append(url)
            body = next((b for sub, b in route_map if sub in url), None)
            status = 200 if body is not None else 404
            out.append({"id": r["id"], "status": status, "body": body or {}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    _cb.seen = seen
    return _cb


def _too_many_parts_responder(route_map, max_parts, message="contains too many parts"):
    """``$batch`` callback that rejects any POST carrying more than ``max_parts``
    sub-requests with a 400 carrying ``message`` (the adaptive-shrink trigger),
    and otherwise behaves like :func:`_batch_responder`. Records the sub-request
    count of each *accepted* POST on ``.accepted`` and the number of rejections
    on ``.rejections``."""
    seen: list[str] = []
    accepted: list[int] = []
    rejections = [0]

    def _cb(request):
        reqs = json.loads(request.body)["requests"]
        if len(reqs) > max_parts:
            rejections[0] += 1
            return (
                400,
                {"Content-Type": "application/json"},
                json.dumps({"error": {"message": message}}),
            )
        accepted.append(len(reqs))
        out = []
        for r in reqs:
            url = r["url"]
            seen.append(url)
            body = next((b for sub, b in route_map if sub in url), None)
            status = 200 if body is not None else 404
            out.append({"id": r["id"], "status": status, "body": body or {}})
        return (200, {"Content-Type": "application/json"}, json.dumps({"responses": out}))

    _cb.seen = seen
    _cb.accepted = accepted
    _cb.rejections = rejections
    return _cb


def _expand_auto_roots_callback(expand_body=None, expand_status=200):
    """GET Roots callback: requests carrying ``$expand`` get ``expand_body`` /
    ``expand_status``; plain requests (N+1 ancestor enumeration) get bare Ids."""
    from urllib.parse import unquote

    def _cb(request):
        if "$expand" in unquote(request.url):
            body = expand_body if expand_body is not None else {"value": [{"Id": 1}]}
            return (expand_status, {}, json.dumps(body))
        return (200, {}, json.dumps({"value": [{"Id": 1}]}))

    return _cb


# ---------------------------------------------------------------------------
# expand_contained mode switches — streaming resume across false/true/auto
# ---------------------------------------------------------------------------


def _switch_opts(mode):
    """Table options for the mode-switch tests: leaf cursor on PROBE_TABLE,
    N+1 fallback kept a plain walk (no $batch), server-driven paging. The
    ``auto`` cursor-lookback is disabled so the read filter equals the
    committed watermark exactly — these tests assert the ``gt <watermark>``
    literal to prove the switched mode resumed from the shared cursor key."""
    return {
        "cursor_field": "RecordLastModified",
        "expand_contained": mode,
        "cursor_probe": "false",
        "pagination": "nextlink",
        "cursor_lookback_seconds": "off",
    }


def _switch_tree(leaf_id, ts):
    """One-root/one-mid $expand response whose single leaf is ``leaf_id``."""
    return {
        "value": [
            {"Id": 1, "Mids": [{"Id": 10, "Leaves": [{"Id": leaf_id, "RecordLastModified": ts}]}]}
        ]
    }


def _expand_urls():
    from urllib.parse import unquote

    return [unquote(c.request.url) for c in responses.calls if "$expand" in unquote(c.request.url)]


# ---------------------------------------------------------------------------
# OR-across-columns keyset-seek preflight → fall back to $skip (mode B)
# ---------------------------------------------------------------------------


def _leaves_or_probe_callback(seen, reject_or):
    """Callback for the leaf collection under `auto` pagination. Page 1 returns
    one row with no `@odata.nextLink` (forces the client-driven seek). The
    composite `(cursor,pk)` seek builds an OR-across-columns `$filter`; the
    `$top=1` probe carrying that OR is answered 400 when `reject_or`, and the
    subsequent `$skip` drain returns empty. Records what shapes were seen."""
    from urllib.parse import parse_qs, unquote, urlparse

    def _cb(request):
        qs = parse_qs(urlparse(request.url).query)
        flt = unquote(qs.get("$filter", [""])[0])
        top = qs.get("$top", [""])[0]
        has_skip = "$skip" in qs
        if " or " in flt and top == "1":
            seen["or_probe"] += 1
            if reject_or:
                return (
                    400,
                    {},
                    json.dumps({"error": {"message": "on different columns, only AND"}}),
                )
            return (200, {}, json.dumps({"value": []}))
        if " or " in flt:
            seen["keyset_seek"] += 1
            return (200, {}, json.dumps({"value": []}))  # keyset drain → empty
        if has_skip:
            seen["skip_seek"] += 1
            return (200, {}, json.dumps({"value": []}))  # $skip drain → empty
        return (
            200,
            {},
            json.dumps({"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]}),
        )

    return _cb


def _mock_guid_metadata():
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_METADATA_XML, status=200)


def _run_flip_preflight(direct_suffix, expand_suffix):
    """Run the cursor_probe preflight against a server whose direct-nav and
    probe-shaped fetches render the same instant with different suffixes."""
    responses.get(f"{SERVICE_URL}$metadata", body=R39_FLIP_METADATA, status=200)
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]}, match_querystring=False)
    newest, older = "2024-05-02T00:00:00", "2024-05-01T00:00:00"

    def _mids_cb(req):
        from urllib.parse import unquote

        if "$expand=" in unquote(req.url):
            return (
                200,
                {},
                json.dumps(
                    {
                        "value": [
                            {
                                "Id": 7,
                                "Leaves": [
                                    {"Id": 71, "RecordLastModified": newest + expand_suffix}
                                ],
                            }
                        ]
                    }
                ),
            )
        return (200, {}, json.dumps({"value": [{"Id": 7}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_cb)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids(7)/Leaves",
        callback=lambda _r: (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {"RecordLastModified": newest + direct_suffix},
                        {"RecordLastModified": older + direct_suffix},
                    ]
                }
            ),
        ),
    )
    c = _make()
    return c._run_cursor_probe_preflight(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified"
    )
