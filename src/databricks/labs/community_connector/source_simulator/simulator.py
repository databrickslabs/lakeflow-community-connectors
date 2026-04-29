"""Source simulator orchestrator — public entry point.

Wraps cassette I/O + the requests-library patch into a context manager that
connector code (or a test harness) can enter. Modes today: ``live`` (no-op),
``record`` (live + persist), ``replay`` (cassette → response).

A future ``simulate`` mode will serve responses from a corpus + endpoint
spec; see DESIGN.md.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

import requests
from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import Cassette
from databricks.labs.community_connector.source_simulator.interceptor import (
    Interceptor,
)
from databricks.labs.community_connector.source_simulator.modes import (
    MODE_ENV,
    MODE_LIVE,
    MODE_RECORD,
    MODE_REPLAY,
    _VALID_MODES,
)
from databricks.labs.community_connector.source_simulator.recorder import Recorder
from databricks.labs.community_connector.source_simulator.replayer import Replayer


class Simulator:
    """Install a record/replay simulator on ``requests.sessions.Session.send``.

    Usage::

        with Simulator(mode="replay", cassette_path=Path("x.json")):
            connector.read_table(...)

    In ``live`` mode this is a no-op.

    Args:
        mode: ``live``, ``record``, or ``replay``.
        cassette_path: JSON file to read from (replay) or write to (record).
        source: free-form tag stored in the cassette.
        allow_missing_cassette: in replay mode, don't raise if absent.
        ignore_query_params: query-param names dropped before match — useful
            for now()-based timestamps and pagination params that should
            collapse to a single cassette entry.
        sample_size: in record mode, trim each response's records array down
            to this many. Connectors stop paginating once recorded responses
            have no "next" hint.
        synthesize_count: in replay mode, expand each response's records up
            to this many via type-aware variation. ``0`` disables synthesis
            (response is returned as recorded).
    """

    def __init__(
        self,
        mode: str,
        cassette_path: Path,
        source: str = "",
        allow_missing_cassette: bool = False,
        ignore_query_params: frozenset = frozenset(),
        sample_size: int = 5,
        synthesize_count: int = 0,
    ) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"Invalid mode: {mode!r}")
        self.mode = mode
        self.cassette_path = Path(cassette_path)
        self.source = source
        self.allow_missing_cassette = allow_missing_cassette
        self.ignore_query_params = frozenset(ignore_query_params)
        self.sample_size = sample_size
        self.synthesize_count = synthesize_count

        self.cassette: Optional[Cassette] = None
        self._interceptor: Optional[Interceptor] = None
        self._recorder: Optional[Recorder] = None
        self._replayer: Optional[Replayer] = None

    def __enter__(self) -> "Simulator":
        if self.mode == MODE_LIVE:
            return self

        if self.mode == MODE_REPLAY:
            if not self.cassette_path.exists():
                if self.allow_missing_cassette:
                    self.cassette = Cassette.empty(self.cassette_path, self.source)
                else:
                    raise FileNotFoundError(
                        f"Cassette not found: {self.cassette_path}\n"
                        f"  Fix: run with {MODE_ENV}=record first to create it."
                    )
            else:
                self.cassette = Cassette.load(self.cassette_path)
        else:  # MODE_RECORD
            self.cassette = Cassette.empty(self.cassette_path, self.source)

        self.cassette.ignore_query_params = self.ignore_query_params

        if self.mode == MODE_RECORD:
            self._recorder = Recorder(self.cassette, sample_size=self.sample_size)
        else:
            self._replayer = Replayer(
                self.cassette, synthesize_count=self.synthesize_count
            )

        self._interceptor = Interceptor(handler=self._handle_send)
        self._interceptor.install()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.mode == MODE_LIVE:
            return
        if self._interceptor is not None:
            self._interceptor.uninstall()
            self._interceptor = None
        if self.mode == MODE_RECORD and self.cassette is not None:
            # Save even on test failure so partial recordings are inspectable.
            self.cassette.save()

    def _handle_send(
        self,
        session: requests.sessions.Session,
        prep: PreparedRequest,
        original_send,
        **kwargs: Any,
    ) -> Response:
        if self.mode == MODE_RECORD:
            resp = original_send(session, prep, **kwargs)
            assert self._recorder is not None
            self._recorder.record(prep, resp)
            return resp

        # MODE_REPLAY
        assert self._replayer is not None
        return self._replayer.replay(prep)


# Backward-compat alias for the pre-rename class name.
RecordReplayPatch = Simulator


@contextmanager
def simulator(
    mode: str,
    cassette_path: Path,
    **kwargs: Any,
) -> Iterator[Simulator]:
    """Function-style wrapper around ``Simulator``."""
    sim = Simulator(mode=mode, cassette_path=cassette_path, **kwargs)
    with sim:
        yield sim


# Backward-compat alias.
record_replay = simulator
