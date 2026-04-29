"""HTTP record/replay framework for connector tests.

Three modes controlled by the ``CONNECTOR_TEST_MODE`` env var:

    live    (default) — tests hit the real source API; no cassettes involved.
    record  — tests hit the real source API and all HTTP interactions are
              written to a cassette file next to the test file.
    replay  — no live calls; HTTP responses are served from the cassette.

Cassettes live at ``tests/unit/sources/{source}/cassettes/{TestClassName}.json``.
Integration with the test harness is automatic via an autouse fixture on
``LakeflowConnectTests`` (see ``tests/unit/sources/test_suite.py``).
"""

from tests.unit.sources.mock_framework.cassette import (
    Cassette,
    Interaction,
    NoMatchingInteraction,
    RequestRecord,
    ResponseRecord,
)
from tests.unit.sources.mock_framework.recorder import (
    MODE_LIVE,
    MODE_RECORD,
    MODE_REPLAY,
    RecordReplayPatch,
    get_mode,
)

__all__ = [
    "Cassette",
    "Interaction",
    "MODE_LIVE",
    "MODE_RECORD",
    "MODE_REPLAY",
    "NoMatchingInteraction",
    "RecordReplayPatch",
    "RequestRecord",
    "ResponseRecord",
    "get_mode",
]
