from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview import (  # noqa: E501
    MicrosoftPurviewLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestMicrosoftPurviewConnector(LakeflowConnectTests):
    connector_class = MicrosoftPurviewLakeflowConnect
    # Simulate mode by default: spec + corpus live at
    # ``source_simulator/specs/microsoft_purview/``. No live credentials needed.
    simulator_source = "microsoft_purview"
    sample_records = 5
    # ``__init__`` requires ``tenant_id`` and a bearer token. ``access_token``
    # is the UC-injected OAuth m2m token; ``_configure_auth`` falls back to
    # ``token``. The simulator never validates these values.
    replay_config = {
        "tenant_id": "00000000-0000-0000-0000-000000000000",
        "access_token": "simulator-fake-token",
    }

