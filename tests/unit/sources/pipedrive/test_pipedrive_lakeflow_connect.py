from databricks.labs.community_connector.sources.pipedrive.pipedrive import (
    PipedriveLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestPipedriveConnector(LakeflowConnectTests):
    """Simulate-mode suite for the Pipedrive (API v1 + v2) connector.

    Runs offline against ``source_simulator/specs/pipedrive/`` (endpoints.yaml,
    custom handlers and corpus). Pipedrive is read sequentially through one
    throttled client, so the connector is not partitioned.
    """

    connector_class = PipedriveLakeflowConnect
    simulator_source = "pipedrive"
    sample_records = 5

    # The simulator never validates credentials; any well-shaped token works.
    replay_config = {
        "api_token": "simulator-fake-api-token",
        "start_date": "2010-01-01T00:00:00Z",
        "company_domain": "simulator",
        "burst_limit": "20",
    }
