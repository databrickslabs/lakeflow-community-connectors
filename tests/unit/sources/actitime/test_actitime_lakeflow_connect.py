from databricks.labs.community_connector.sources.actitime.actitime import (
    ActitimeLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestActitimeConnector(LakeflowConnectTests):
    connector_class = ActitimeLakeflowConnect
    simulator_source = "actitime"
    replay_config = {
        # The simulator matches on URL path only (e.g. ``/api/v1/customers``).
        # A real actiTIME deployment uses ``https://online.actitime.com/<tenant>``
        # which would prepend a tenant segment to every path, so we use a
        # bare host here to keep the spec-side paths simple.
        "base_url": "https://simulator.actitime.example",
        "username": "simulator",
        "password": "simulator-fake-password",
    }
    # Known fixture limitations of simulate mode (not connector bugs):
    #
    # * ``timetrack`` and ``leavetime`` — the actiTIME API returns a NESTED
    #   envelope of the form
    #       [{"userId": 1, "date": "2026-05-08", "records": [{...}, ...]}]
    #   that the connector flattens into one row per ``records[]`` entry.
    #   The auto-generated corpus produced from ``TABLE_SCHEMAS`` is flat,
    #   so the connector's envelope parser sees no nested ``records`` and
    #   yields zero rows. Phase 2 record mode replaces this corpus with
    #   real data, eliminating the discrepancy.
    #
    # * ``approvalStatus`` — the simulator's ``dateFrom`` / ``dateTo``
    #   filter targets the API-side ``weekStartDate`` field (camelCase),
    #   but the auto-generated corpus carries the connector-side
    #   ``week_start_date`` (snake_case, matching the Spark schema). The
    #   filter sees ``None`` for every record and drops them all on the
    #   first windowed read. A hand-curated camelCase corpus or a record-
    #   mode re-seed fixes this; until then, tolerate the empty first read.
    #
    # The pattern mirrors how the zendesk test class tolerates
    # ``ticket_comments`` returning empty on the first call.
    allow_empty_first_read = frozenset({"timetrack", "leavetime", "approvalStatus"})
