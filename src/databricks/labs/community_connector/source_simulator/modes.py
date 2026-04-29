"""Operating modes for the source simulator.

Three modes today:

    live    — no-op patch; calls hit the real API
    record  — pass-through to live, persist responses to a cassette
    replay  — verbatim playback of recorded responses

A future ``simulate`` mode will read an endpoint spec + record corpus and
serve responses with real query semantics (filter, sort, paginate). See
DESIGN.md for the bigger picture.
"""

from __future__ import annotations

import os

MODE_ENV = "CONNECTOR_TEST_MODE"

MODE_LIVE = "live"
MODE_RECORD = "record"
MODE_REPLAY = "replay"
# Reserved for phase 2; not yet wired into Simulator dispatch.
MODE_SIMULATE = "simulate"

_VALID_MODES = {MODE_LIVE, MODE_RECORD, MODE_REPLAY}


def get_mode() -> str:
    """Read ``CONNECTOR_TEST_MODE`` env var; default = ``live``."""
    mode = os.environ.get(MODE_ENV, MODE_LIVE).strip().lower()
    if mode not in _VALID_MODES:
        raise ValueError(
            f"Invalid {MODE_ENV}={mode!r}. Expected one of: {sorted(_VALID_MODES)}"
        )
    return mode
