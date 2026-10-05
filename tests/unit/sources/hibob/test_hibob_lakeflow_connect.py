from typing import Any, Dict

from databricks.labs.community_connector.source_simulator import MODE_SIMULATE
from databricks.labs.community_connector.sources.hibob.hibob import HibobLakeflowConnect
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import (
    LakeflowConnectTests,
    _resolve_env_mode_for_simulator,
)

# The synthesized corpus stamps ``createdOn`` from 2024-01-01 onwards, which
# falls outside the connector's default 180-day lookback floor (the live API
# rejects older ``since`` values). In simulate mode only, widen the lookback
# and pin the start so the fixed-date corpus stays reachable regardless of
# when the tests run. Live / record runs keep the real API limits. Simulate
# overrides win over configs/dev_table_config.json (which targets live data).
_SIMULATE_TABLE_CONFIGS: Dict[str, Dict[str, Any]] = {
    "time_off_request_changes": {
        "start_date": "2023-12-31T00:00:00Z",
        "max_lookback_days": "36500",
        "window_days": "90",
    },
    # The simulator corpus serves synthetic rows under the same report ID as
    # configs/dev_table_config.json, so live drift validation can replay it.
    "company_report": {
        "report_id": "31115110",
        "primary_keys": "employee_id_bob,effective_date",
    },
}


class TestHibobConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = HibobLakeflowConnect
    simulator_source = "hibob"
    replay_config = {
        "service_user_id": "simulator-service-user",
        "service_user_token": "simulator-fake-token",
    }

    @classmethod
    def _load_table_configs(cls) -> Dict[str, Dict[str, Any]]:
        configs = super()._load_table_configs()
        if _resolve_env_mode_for_simulator(cls.simulator_source) == MODE_SIMULATE:
            for table, opts in _SIMULATE_TABLE_CONFIGS.items():
                configs[table] = {**configs.get(table, {}), **opts}
        return configs
