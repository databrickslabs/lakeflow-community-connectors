from databricks.labs.community_connector.sources.appsflyer.appsflyer import AppsflyerLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestAppsflyerConnector(LakeflowConnectTests):
    connector_class = AppsflyerLakeflowConnect
    simulator_source = "appsflyer"
    replay_config = {
        "api_token": "simulator-fake-token",
        "base_url": "https://simulator.example.com",
    }
    # AppsFlyer connector queries fixed-date windows (e.g. start_date in
    # 2026-01); the simulator corpus does not contain records dated within
    # the connector's first-call window. Fixture limitation, not a bug.
    allow_empty_first_read = frozenset({
        "installs_report",
        "in_app_events_report",
        "uninstall_events_report",
        "organic_installs_report",
        "organic_in_app_events_report",
        "organic_uninstall_events_report",
    })
