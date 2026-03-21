"""Tests for the Denodo Lakeflow Community Connector.

Uses the standard LakeflowConnectTests harness. Requires a running
Denodo VDP instance with PostgreSQL interface enabled.

Credentials are loaded from configs/dev_config.json.
"""

from databricks.labs.community_connector.sources.denodo.denodo import DenodoLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestDenodoConnector(LakeflowConnectTests):
    connector_class = DenodoLakeflowConnect
    sample_records = 50
