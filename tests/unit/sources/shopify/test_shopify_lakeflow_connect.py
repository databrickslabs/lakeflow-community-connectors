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
    # Stand-in credentials for simulate / replay mode. The simulator
    # never validates these — it just needs the connector to be
    # constructible without requiring a real shop.
    replay_config = {
        "shop": "simulator-shop",
        "access_token": "simulator-fake-token",
        "api_version": "2026-04",
    }
