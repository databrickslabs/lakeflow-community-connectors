"""Tests for the HL7 v2 community connector (Delta table source mode).

Runs the standard LakeflowConnectTests suite against the simulator by
default (offline, deterministic, no creds). The simulator implements the
Databricks SQL Statement Execution API endpoints used in delta mode and
serves rows projected from the same ``messages`` corpus the GCP handler
uses. To exercise a real Delta table, set ``CONNECTOR_TEST_MODE=live``.
"""

import json

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestHL7V2DeltaConnector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    simulator_source = "hl7_v2"
    sample_records = 5

    # Stand-in credentials for delta mode. The connector's
    # ``_init_delta`` only requires presence; the simulator intercepts the
    # SQL Statement Execution API calls before any auth check fires.
    replay_config = {
        "source_type": "delta",
        "delta_table_name": "sim_catalog.sim_schema.hl7_raw",
        "databricks_host": "https://sim-workspace.cloud.databricks.com",
        "databricks_token": "dapi-sim-token",
        "sql_warehouse_id": "sim-warehouse-id",
        "delta_query_mode": "per_window",
    }

    @classmethod
    def _load_table_configs(cls):
        path = cls._config_dir() / "dev_table_config_delta.json"
        if not path.exists():
            return {}
        with open(path, "r") as f:
            return json.load(f)
