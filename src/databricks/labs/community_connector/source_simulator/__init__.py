"""Source simulator: stand in for a live source API in tests and pipelines.

See ``DESIGN.md`` for the architecture and operating modes (live / replay /
simulate). For the test-harness use case, see ``README.md``.
"""

from databricks.labs.community_connector.source_simulator.cassette import (
    Cassette,
    Interaction,
    NoMatchingInteraction,
    RequestRecord,
    ResponseRecord,
)
from databricks.labs.community_connector.source_simulator.corpus import CorpusStore
from databricks.labs.community_connector.source_simulator.coverage import (
    CoverageTracker,
    EndpointHit,
)
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
    FilterOp,
    ParamRole,
    ResponseShape,
    ResponseWrapper,
    load_specs,
    match_endpoint,
)
from databricks.labs.community_connector.source_simulator.handler import (
    SimulateHandler,
    UnknownEndpoint,
)
from databricks.labs.community_connector.source_simulator.modes import (
    MODE_ENV,
    MODE_LIVE,
    MODE_RECORD,
    MODE_REPLAY,
    MODE_SIMULATE,
    get_mode,
)
from databricks.labs.community_connector.source_simulator.pagination import (
    PaginationStyle,
    get_style,
    register_style,
)
from databricks.labs.community_connector.source_simulator.simulator import (
    RecordReplayPatch,  # deprecated alias for Simulator
    Simulator,
    record_replay,  # deprecated alias for simulator()
    simulator,
)

__all__ = [
    "Cassette",
    "CorpusStore",
    "CoverageTracker",
    "EndpointHit",
    "EndpointSpec",
    "FilterOp",
    "Interaction",
    "MODE_ENV",
    "MODE_LIVE",
    "MODE_RECORD",
    "MODE_REPLAY",
    "MODE_SIMULATE",
    "NoMatchingInteraction",
    "PaginationStyle",
    "ParamRole",
    "RecordReplayPatch",
    "ResponseShape",
    "ResponseWrapper",
    "RequestRecord",
    "ResponseRecord",
    "SimulateHandler",
    "Simulator",
    "UnknownEndpoint",
    "get_mode",
    "get_style",
    "load_specs",
    "match_endpoint",
    "record_replay",
    "register_style",
    "simulator",
]
