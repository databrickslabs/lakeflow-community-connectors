"""Monkey-patch ``requests.sessions.Session.send`` for record/replay.

The single choke-point for every ``requests`` call — ``Session.get``,
``Session.post``, ``requests.get``, even the module-level ``requests.request`` —
is ``Session.send(PreparedRequest)``. Patching that one method catches all HTTP
traffic from every connector that uses the ``requests`` library.

The patch is installed once per test class via a context manager
(``RecordReplayPatch``). In ``live`` mode the patch is a no-op (we don't even
wrap ``send``) so real-API testing stays identical to today.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

import requests
from requests.models import PreparedRequest, Response

from tests.unit.sources.mock_framework.cassette import (
    Cassette,
    Interaction,
    NoMatchingInteraction,
    RequestRecord,
    ResponseRecord,
    body_sha256,
    encode_body,
    scrub_emails,
    scrub_headers,
    split_url,
)
from tests.unit.sources.mock_framework.sampler import (
    sample_body,
    strip_link_header_next,
)
from tests.unit.sources.mock_framework.synthesizer import (
    stable_seed,
    synthesize_body,
)

MODE_ENV = "CONNECTOR_TEST_MODE"
MODE_LIVE = "live"
MODE_RECORD = "record"
MODE_REPLAY = "replay"
_VALID_MODES = {MODE_LIVE, MODE_RECORD, MODE_REPLAY}


def get_mode() -> str:
    """Read ``CONNECTOR_TEST_MODE`` env var; default = ``live``."""
    mode = os.environ.get(MODE_ENV, MODE_LIVE).strip().lower()
    if mode not in _VALID_MODES:
        raise ValueError(
            f"Invalid {MODE_ENV}={mode!r}. Expected one of: {sorted(_VALID_MODES)}"
        )
    return mode


def _request_record_from_prepared(prep: PreparedRequest) -> RequestRecord:
    base, query = split_url(prep.url or "")
    return RequestRecord(
        method=(prep.method or "GET").upper(),
        url=base,
        query=query,
        body_sha256=body_sha256(prep.body),
    )


def _response_record_from_response(resp: Response) -> ResponseRecord:
    body_text, body_b64 = encode_body(resp.content, resp.encoding)
    return ResponseRecord(
        status_code=resp.status_code,
        headers=dict(resp.headers),
        body_text=body_text,
        body_b64=body_b64,
        encoding=resp.encoding,
        url=resp.url,
    )


def _response_from_record(rec: ResponseRecord, prep: PreparedRequest) -> Response:
    """Reconstruct a ``requests.Response`` from a recorded ``ResponseRecord``."""
    resp = Response()
    resp.status_code = rec.status_code
    resp.headers.update(rec.headers or {})
    resp._content = rec.content_bytes()
    resp.encoding = rec.encoding
    resp.url = rec.url or (prep.url or "")
    resp.request = prep
    resp.reason = _status_reason(rec.status_code)
    # requests uses resp.raw for streaming; we don't support stream=True in
    # replay (no connector in this repo uses it at time of writing).
    return resp


def _status_reason(status: int) -> str:
    # Minimal mapping; only for nicer error messages / logs.
    return {
        200: "OK",
        201: "Created",
        204: "No Content",
        301: "Moved Permanently",
        302: "Found",
        304: "Not Modified",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        429: "Too Many Requests",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
    }.get(status, "")


class RecordReplayPatch:
    """Install a record/replay patch on ``requests.sessions.Session.send``.

    Usage::

        with RecordReplayPatch(mode=MODE_RECORD, cassette_path=Path("x.json"), source="foo"):
            # ... tests run here ...
            pass

    In ``MODE_LIVE`` this is a no-op.
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
        """Create a record/replay patch.

        Args:
            mode: ``live``, ``record``, or ``replay``.
            cassette_path: JSON file to read from (replay) or write to (record).
            source: free-form tag stored in the cassette.
            allow_missing_cassette: in replay mode, don't raise if absent.
            ignore_query_params: query-param names dropped before match.
            sample_size: in record mode, trim each response's records array
                down to this many. Connectors stop paginating once recorded
                responses have no "next" hint.
            synthesize_count: in replay mode, expand each response's records
                up to this many via type-aware variation. ``0`` disables
                synthesis (response is returned as recorded).
        """
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
        self._original_send = None
        self._installed = False

    # --- public API ---

    def __enter__(self) -> "RecordReplayPatch":
        if self.mode == MODE_LIVE:
            return self

        if self.mode == MODE_REPLAY:
            if not self.cassette_path.exists():
                if self.allow_missing_cassette:
                    self.cassette = Cassette.empty(self.cassette_path, self.source)
                else:
                    raise FileNotFoundError(
                        f"Cassette not found: {self.cassette_path}\n"
                        f"  Fix: run tests with {MODE_ENV}=record first to create it."
                    )
            else:
                self.cassette = Cassette.load(self.cassette_path)
        else:  # MODE_RECORD
            self.cassette = Cassette.empty(self.cassette_path, self.source)

        self.cassette.ignore_query_params = self.ignore_query_params

        self._install()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.mode == MODE_LIVE:
            return
        self._uninstall()
        if self.mode == MODE_RECORD and self.cassette is not None:
            # Save even on test failure so partial recordings are inspectable.
            self.cassette.save()

    # --- patch plumbing ---

    def _install(self) -> None:
        if self._installed:
            return
        patch = self  # capture for closure

        original_send = requests.sessions.Session.send

        def _patched_send(
            session: requests.sessions.Session,
            request: PreparedRequest,
            **kwargs: Any,
        ) -> Response:
            return patch._handle_send(session, request, original_send, **kwargs)

        self._original_send = original_send
        requests.sessions.Session.send = _patched_send  # type: ignore[assignment]
        self._installed = True

    def _uninstall(self) -> None:
        if not self._installed:
            return
        requests.sessions.Session.send = self._original_send  # type: ignore[assignment]
        self._original_send = None
        self._installed = False

    # --- per-request dispatch ---

    def _handle_send(
        self,
        session: requests.sessions.Session,
        prep: PreparedRequest,
        original_send,
        **kwargs: Any,
    ) -> Response:
        assert self.cassette is not None  # enforced by __enter__
        req_rec = _request_record_from_prepared(prep)

        if self.mode == MODE_RECORD:
            resp = original_send(session, prep, **kwargs)

            # Dedup: if we've already recorded this match-key, don't append a
            # second copy. (We still hand the live response to the connector
            # so its pagination loop can keep going during recording.)
            if self.cassette.has_key(req_rec):
                return resp

            resp_rec = _response_record_from_response(resp)
            # Trim records array and strip in-body pagination pointers.
            resp_rec.body_text = sample_body(
                resp_rec.body_text, max_records=self.sample_size
            )
            # Scrub PII (email-shape strings) from the response body before
            # persisting.
            resp_rec.body_text = scrub_emails(resp_rec.body_text)
            # Strip Link-header "next" pagination, drop sensitive headers.
            resp_rec.headers = scrub_headers(strip_link_header_next(resp_rec.headers))

            recorded_request = RequestRecord(
                method=req_rec.method,
                url=req_rec.url,
                query=req_rec.query,
                body_sha256=req_rec.body_sha256,
            )
            self.cassette.append(
                Interaction(request=recorded_request, response=resp_rec)
            )
            return resp

        # MODE_REPLAY
        resp_rec = self.cassette.match(req_rec)
        if self.synthesize_count > 0:
            seed = stable_seed(
                req_rec.method, req_rec.url, repr(sorted(req_rec.query.items()))
            )
            expanded = synthesize_body(
                resp_rec.body_text,
                target_count=self.synthesize_count,
                seed=seed,
            )
            if expanded is not resp_rec.body_text:
                # Return a shallow-modified copy so the cached cassette record
                # isn't mutated across repeated replays.
                resp_rec = ResponseRecord(
                    status_code=resp_rec.status_code,
                    headers=dict(resp_rec.headers),
                    body_text=expanded,
                    body_b64=resp_rec.body_b64,
                    encoding=resp_rec.encoding,
                    url=resp_rec.url,
                )
        return _response_from_record(resp_rec, prep)


@contextmanager
def record_replay(
    mode: str,
    cassette_path: Path,
    source: str = "",
    allow_missing_cassette: bool = False,
) -> Iterator[RecordReplayPatch]:
    """Function-style wrapper around ``RecordReplayPatch``."""
    patch = RecordReplayPatch(
        mode=mode,
        cassette_path=cassette_path,
        source=source,
        allow_missing_cassette=allow_missing_cassette,
    )
    with patch:
        yield patch
