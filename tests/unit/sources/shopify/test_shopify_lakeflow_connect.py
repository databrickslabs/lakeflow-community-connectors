"""Live-API integration tests for ShopifyLakeflowConnect.

Excluded from CI via .github/workflows/test_exclude.txt — requires
real shop credentials in configs/dev_config.json.
"""

from databricks.labs.community_connector.sources.shopify.shopify import (
    ShopifyLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestShopifyConnector(LakeflowConnectTests):
    connector_class = ShopifyLakeflowConnect
