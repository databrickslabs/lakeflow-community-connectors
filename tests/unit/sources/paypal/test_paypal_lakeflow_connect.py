from databricks.labs.community_connector.sources.paypal.paypal import PaypalLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestPaypalConnector(LakeflowConnectTests):
    connector_class = PaypalLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/paypal/``. The OAuth token exchange is stubbed
    # by a simulator handler, so the stand-in client credentials are never
    # validated.
    simulator_source = "paypal"
    replay_config = {
        "client_id": "simulator-client-id",
        "client_secret": "simulator-client-secret",
        "environment": "sandbox",
    }
    sample_records = 5
