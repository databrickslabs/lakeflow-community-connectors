from databricks.labs.community_connector.sources.lancedb.lancedb import (
    LancedbLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestLancedbConnector(LakeflowConnectTests):
    connector_class = LancedbLakeflowConnect
    simulator_source = "lancedb"
    sample_records = 5
    # LanceDB Cloud auth is an ``x-api-key`` header; the project/database name
    # and region are interpolated into the host. The simulator never validates
    # these, so any strings of the right shape work. ``project_name`` and
    # ``region`` must pass ``_sanitize_identifier`` (alphanumeric / hyphen /
    # underscore only). __init__ reads: api_key, project_name (or database),
    # region.
    replay_config = {
        "api_key": "simulator-fake-api-key",
        "project_name": "simulator-project",
        "region": "us-east-1",
    }
