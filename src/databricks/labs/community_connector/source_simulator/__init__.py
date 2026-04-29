"""Source simulator: stand in for a live source API in tests and pipelines.

See ``DESIGN.md`` for the architecture and operating modes (live / record /
replay / planned simulate). For the test-harness use case, also see
``README.md``.
"""

from databricks.labs.community_connector.source_simulator.cassette import (
    Cassette,
    Interaction,
    NoMatchingInteraction,
    RequestRecord,
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.modes import (
    MODE_ENV,
    MODE_LIVE,
    MODE_RECORD,
    MODE_REPLAY,
    get_mode,
)
from databricks.labs.community_connector.source_simulator.simulator import (
    RecordReplayPatch,  # deprecated alias for Simulator
    Simulator,
    record_replay,  # deprecated alias for simulator()
    simulator,
)

__all__ = [
    "Cassette",
    "Interaction",
    "MODE_ENV",
    "MODE_LIVE",
    "MODE_RECORD",
    "MODE_REPLAY",
    "NoMatchingInteraction",
    "RecordReplayPatch",
    "RequestRecord",
    "ResponseRecord",
    "Simulator",
    "get_mode",
    "record_replay",
    "simulator",
]
