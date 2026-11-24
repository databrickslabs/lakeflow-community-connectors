"""Tests for ShopifyLakeflowConnect.

Default mode runs against the in-process source simulator (no creds,
no network) — pulled from
``src/databricks/labs/community_connector/source_simulator/specs/shopify/``.

Live-mode runs against a real Shopify store via env vars:
``CONNECTOR_TEST_MODE=record CONNECTOR_TEST_CONFIG_JSON='{"shop":...,
"access_token":...,"api_version":...}' pytest tests/unit/sources/shopify/``
"""

from databricks.labs.community_connector.sources.shopify.shopify import (
    ShopifyLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestShopifyConnector(LakeflowConnectTests):
    connector_class = ShopifyLakeflowConnect
    simulator_source = "shopify"
    # Known: returns records without advancing the offset, which would abort a
    # managed incremental pipeline (SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE).
    # Pending owner confirmation/fix; tracked in #307. Remove when fixed.
    offset_advance_exempt_tables = frozenset({"inventory_levels"})
    # Stand-in credentials for simulate / replay mode. The simulator
    # never validates these — it just needs the connector to be
    # constructible without requiring a real shop.
    replay_config = {
        "shop": "simulator-shop",
        "access_token": "simulator-fake-token",
        "api_version": "2026-04",
    }
