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
from typing import Any, Iterator, List, Optional

import requests
from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import Cassette
from databricks.labs.community_connector.source_simulator.corpus import CorpusStore
from databricks.labs.community_connector.source_simulator.coverage import (
    CoverageTracker,
)
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
    load_specs,
)
from databricks.labs.community_connector.source_simulator.handler import (
    SimulateHandler,
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
    MODE_SIMULATE,
    _VALID_MODES,
)
from databricks.labs.community_connector.source_simulator.recorder import Recorder
from databricks.labs.community_connector.source_simulator.replayer import Replayer
from databricks.labs.community_connector.source_simulator.validator import (
    LiveValidator,
)


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
        cassette_path: Optional[Path] = None,
        source: str = "",
        allow_missing_cassette: bool = False,
        ignore_query_params: frozenset = frozenset(),
        sample_size: int = 5,
        synthesize_count: int = 0,
        record: bool = True,
        spec_path: Optional[Path] = None,
        corpus_dir: Optional[Path] = None,
    ) -> None:
        if mode not in _VALID_MODES:
            raise ValueError(f"Invalid mode: {mode!r}")
        # ``record`` is treated identically to ``live`` (deprecated alias).
        if mode == MODE_RECORD:
            mode = MODE_LIVE

        if mode == MODE_SIMULATE:
            if spec_path is None or corpus_dir is None:
                raise ValueError(
                    f"{mode!r} mode requires spec_path and corpus_dir"
                )
        else:
            if cassette_path is None:
                raise ValueError(f"{mode!r} mode requires cassette_path")
            # In live mode, spec_path + corpus_dir are optional. When both
            # are provided, the simulator also runs the spec validator: each
            # live response is diffed against what the spec would produce,
            # and the per-endpoint validation report is written next to the
            # cassette at the end of the run.

        self.mode = mode
        self.cassette_path = Path(cassette_path) if cassette_path else None
        self.spec_path = Path(spec_path) if spec_path else None
        self.corpus_dir = Path(corpus_dir) if corpus_dir else None
        self.source = source
        self.allow_missing_cassette = allow_missing_cassette
        self.ignore_query_params = frozenset(ignore_query_params)
        self.sample_size = sample_size
        self.synthesize_count = synthesize_count
        self.record = record

        self.cassette: Optional[Cassette] = None
        self.specs: Optional[List[EndpointSpec]] = None
        self.corpus: Optional[CorpusStore] = None
        self.coverage = CoverageTracker()
        self._interceptor: Optional[Interceptor] = None
        self._recorder: Optional[Recorder] = None
        self._replayer: Optional[Replayer] = None
        self._handler: Optional[SimulateHandler] = None
        # In live mode, when spec_path + corpus_dir are also provided, this
        # validates each live response against what the spec would produce.
        self._validator: Optional[LiveValidator] = None

    @property
    def coverage_path(self) -> Path:
        """Where the per-endpoint coverage report is written, next to the cassette."""
        return self.cassette_path.with_suffix(self.cassette_path.suffix + ".coverage.json")

    @property
    def validation_path(self) -> Path:
        """Where the per-endpoint validation report is written, next to the cassette."""
        return self.cassette_path.with_suffix(self.cassette_path.suffix + ".validation.json")

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

            # When spec_path + corpus_dir are also given in live mode, set
            # up the spec validator. Each live request gets a free
            # spec-vs-reality diff; results are written next to the cassette
            # on __exit__.
            if self.spec_path is not None and self.corpus_dir is not None:
                self.specs = load_specs(self.spec_path)
                self.corpus = CorpusStore.load(self.corpus_dir)
                self._validator = LiveValidator(specs=self.specs, corpus=self.corpus)

            self._interceptor = Interceptor(handler=self._handle_send)
            self._interceptor.install()
            return self

        # Replay (stand-in posture, cassette source).
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

        # Simulate (stand-in posture, spec+corpus source).
        if self.mode == MODE_SIMULATE:
            self.specs = load_specs(self.spec_path)
            self.corpus = CorpusStore.load(self.corpus_dir)
            # Apply ``synthesize_future_records`` directives from the spec.
            # No-op when no spec declares them. When present, the directive
            # strengthens ``test_read_terminates`` into an explicit cap
            # check: future records leak through if the connector's
            # ``until=`` filter is missing or wrong.
            from databricks.labs.community_connector.source_simulator.corpus import (
                inject_future_records as _inject_future_records,
            )
            _inject_future_records(self.corpus, self.specs)
            self._handler = SimulateHandler(specs=self.specs, corpus=self.corpus)
            self._interceptor = Interceptor(handler=self._handle_send)
            self._interceptor.install()
            return self

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._interceptor is not None:
            self._interceptor.uninstall()
            self._interceptor = None
        if self.mode == MODE_LIVE and self.record and self.cassette is not None:
            # Save even on test failure so partial recordings are inspectable.
            # Skip writing entirely when no traffic was intercepted — connectors
            # that don't use ``requests`` (e.g. in-memory simulated sources)
            # otherwise leave empty cassettes lying around.
            if self.cassette.interactions:
                self.cassette.save()
                self.coverage.save(self.coverage_path)
                if self._validator is not None:
                    self._validator.save(self.validation_path)

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
            if self._validator is not None:
                self._validator.observe(prep, resp)
            return resp

        if self.mode == MODE_SIMULATE:
            assert self._handler is not None
            self.coverage.observe(request_record_from_prepared(prep))
            return self._handler.handle(prep)

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
