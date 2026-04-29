"""Source simulator orchestrator — public entry point.

Wraps cassette I/O + the requests-library patch into a context manager that
connector code (or a test harness) can enter. Two postures:

- **Proxy** (``live`` mode) — forwards to the real source. By default also
  appends responses to the cassette and tracks per-endpoint coverage; both
  are free side-effects of any live run.
- **Stand-in** (``replay`` mode) — serves responses from the cassette without
  touching the live source. A future ``simulate`` mode will serve from a
  spec + corpus instead.

See ``DESIGN.md`` for the bigger picture.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

import requests
from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import Cassette
from databricks.labs.community_connector.source_simulator.coverage import (
    CoverageTracker,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    Interceptor,
    request_record_from_prepared,
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

    In ``live`` mode (the proxy posture) the simulator forwards every
    request to the real source. By default it **also** appends responses
    to the cassette (sample + scrub + dedup) and tracks which endpoints
    were hit; both are free outputs of any live run, used to keep the
    cassette current and to spot coverage gaps. Pass ``record=False`` to
    skip the cassette write while keeping coverage tracking.

    Args:
        mode: ``live`` or ``replay``. ``record`` is accepted as a deprecated
            alias for ``live``.
        cassette_path: JSON file to read from (replay) or write to (live).
        source: free-form tag stored in the cassette.
        allow_missing_cassette: in replay mode, don't raise if absent.
        ignore_query_params: query-param names dropped before match — useful
            for now()-based timestamps and pagination params that should
            collapse to a single cassette entry.
        sample_size: when recording, trim each response's records array
            down to this many. Connectors stop paginating once recorded
            responses have no "next" hint.
        synthesize_count: in replay mode, expand each response's records up
            to this many via type-aware variation. ``0`` disables synthesis
            (response is returned as recorded).
        record: in live mode, append intercepted responses to the cassette.
            Default ``True`` so live runs keep the cassette current. Set
            ``False`` when you want the proxy posture without persistence
            (e.g. ad-hoc smoke tests).
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
        record: bool = True,
    ) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"Invalid mode: {mode!r}")
        # ``record`` is treated identically to ``live`` (deprecated alias).
        if mode == MODE_RECORD:
            mode = MODE_LIVE
        self.mode = mode
        self.cassette_path = Path(cassette_path)
        self.source = source
        self.allow_missing_cassette = allow_missing_cassette
        self.ignore_query_params = frozenset(ignore_query_params)
        self.sample_size = sample_size
        self.synthesize_count = synthesize_count
        self.record = record

        self.cassette: Optional[Cassette] = None
        self.coverage = CoverageTracker()
        self._interceptor: Optional[Interceptor] = None
        self._recorder: Optional[Recorder] = None
        self._replayer: Optional[Replayer] = None

    @property
    def coverage_path(self) -> Path:
        """Where the per-endpoint coverage report is written, next to the cassette."""
        return self.cassette_path.with_suffix(self.cassette_path.suffix + ".coverage.json")

    def __enter__(self) -> "Simulator":
        # Live (proxy posture): load existing cassette so we append, not
        # overwrite. If recording is disabled or no path is meaningful, the
        # cassette stays in memory only.
        if self.mode == MODE_LIVE:
            if self.record:
                if self.cassette_path.exists():
                    self.cassette = Cassette.load(self.cassette_path)
                else:
                    self.cassette = Cassette.empty(self.cassette_path, self.source)
                self.cassette.ignore_query_params = self.ignore_query_params
                self._recorder = Recorder(self.cassette, sample_size=self.sample_size)
            self._interceptor = Interceptor(handler=self._handle_send)
            self._interceptor.install()
            return self

        # Replay (stand-in posture).
        if self.mode == MODE_REPLAY:
            if not self.cassette_path.exists():
                if self.allow_missing_cassette:
                    self.cassette = Cassette.empty(self.cassette_path, self.source)
                else:
                    raise FileNotFoundError(
                        f"Cassette not found: {self.cassette_path}\n"
                        f"  Fix: run with {MODE_ENV}=live first to create it."
                    )
            else:
                self.cassette = Cassette.load(self.cassette_path)
            self.cassette.ignore_query_params = self.ignore_query_params
            self._replayer = Replayer(
                self.cassette, synthesize_count=self.synthesize_count
            )
            self._interceptor = Interceptor(handler=self._handle_send)
            self._interceptor.install()

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._interceptor is not None:
            self._interceptor.uninstall()
            self._interceptor = None
        if self.mode == MODE_LIVE and self.record and self.cassette is not None:
            # Save even on test failure so partial recordings are inspectable.
            self.cassette.save()
            self.coverage.save(self.coverage_path)

    def _handle_send(
        self,
        session: requests.sessions.Session,
        prep: PreparedRequest,
        original_send,
        **kwargs: Any,
    ) -> Response:
        if self.mode == MODE_LIVE:
            resp = original_send(session, prep, **kwargs)
            self.coverage.observe(request_record_from_prepared(prep))
            if self._recorder is not None:
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
