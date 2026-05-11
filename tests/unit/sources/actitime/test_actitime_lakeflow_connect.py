from databricks.labs.community_connector.sources.actitime.actitime import (
    ActitimeLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestActitimeConnector(LakeflowConnectTests):
    connector_class = ActitimeLakeflowConnect
    simulator_source = "actitime"
    replay_config = {
        # A real actiTIME Online deployment uses
        # ``https://online.actitime.com/<tenant>`` — the tenant slug is part
        # of the URL PATH, not the subdomain. We mirror that here with a
        # ``/sim`` segment so the simulator's path patterns
        # (``/{tenant}/api/v1/<table>``) match in both simulate and record
        # mode without needing a separate spec per environment.
        "base_url": "https://simulator.actitime.example/sim",
        "username": "simulator",
        "password": "simulator-fake-password",
    }
    # Empty-first-read allowances:
    #
    # * ``settings``, ``userGroups``, ``holidays``, ``approvalStatus`` —
    #   these are feature-gated on the actiTIME tenant. On the test
    #   tenant they return HTTP 404 ("api.error.unknown_error"). The
    #   connector swallows 404 on these endpoints (treat as absent
    #   table) and emits zero records; we allow the empty first read so
    #   that fact alone doesn't fail the suite. Note: ``settings`` is a
    #   ``snapshot`` and is therefore already exempt from the empty-read
    #   check, but listing it keeps the intent obvious.
    allow_empty_first_read = frozenset({
        "settings",
        "userGroups",
        "holidays",
        "approvalStatus",
    })
