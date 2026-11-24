from databricks.labs.community_connector.sources.collibra.collibra import (
    CollibraLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestCollibraConnector(LakeflowConnectTests):
    connector_class = CollibraLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/collibra/``. No live credentials required.
    simulator_source = "collibra"
    sample_records = 5
    # ``__init__`` requires ``org`` (Collibra subdomain) and a bearer token.
    # ``access_token`` is the UC-injected OAuth m2m token; ``_configure_auth``
    # falls back to ``token``. The simulator never validates these values.
    replay_config = {
        "org": "simulator",
        "access_token": "simulator-fake-token",
    }
