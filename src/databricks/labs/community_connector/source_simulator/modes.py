"""Operating modes for the source simulator.

Two modes today:

    live    — proxy posture; calls go to the real source. By default also
              appends responses into the cassette and tracks per-endpoint
              coverage (free side-effects of any live run). Pass
              ``record=False`` to keep coverage-only behavior.
    replay  — stand-in posture; verbatim playback of recorded responses.

A future ``simulate`` mode (stand-in posture) will read an endpoint spec +
record corpus and serve responses with real query semantics (filter, sort,
paginate). See DESIGN.md.

``record`` is accepted as a deprecated alias of ``live`` — kept for one
release while callers migrate. The earlier separate "record" mode collapses
into ``live`` because the only difference between them was "persist or
don't", and persisting during a live run is essentially free.
"""

from __future__ import annotations

import os

MODE_ENV = "CONNECTOR_TEST_MODE"

MODE_LIVE = "live"
MODE_RECORD = "record"
MODE_REPLAY = "replay"
MODE_SIMULATE = "simulate"

_VALID_MODES = {MODE_LIVE, MODE_RECORD, MODE_REPLAY, MODE_SIMULATE}


def get_mode() -> str:
    """Read ``CONNECTOR_TEST_MODE`` env var; default = ``live``."""
    mode = os.environ.get(MODE_ENV, MODE_LIVE).strip().lower()
    if mode not in _VALID_MODES:
        raise ValueError(
            f"Invalid {MODE_ENV}={mode!r}. Expected one of: {sorted(_VALID_MODES)}"
        )
    return mode
