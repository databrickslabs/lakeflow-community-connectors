"""Low-level monkey-patch on ``requests.sessions.Session.send``.

Every ``Session.get`` / ``Session.post`` / module-level ``requests.request``
call ultimately invokes ``Session.send(PreparedRequest)``. Patching it once
catches every connector that uses the ``requests`` library — connectors
require zero changes.

The interceptor doesn't decide what to do with each request. The caller
supplies a handler (recorder, replayer, future corpus handler) and the
interceptor manages the patch lifecycle.

Also exposes a few small adapters between ``requests`` types and our
cassette types, used by both recorder and replayer.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

import requests
from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import (
    RequestRecord,
    ResponseRecord,
    body_sha256,
    encode_body,
    split_url,
)

# Handler signature: (session, prepared_request, original_send, **kwargs) -> Response
Handler = Callable[..., Response]


class Interceptor:
    """Owns the install/uninstall of the ``Session.send`` monkey-patch."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self._original_send: Optional[Callable] = None
        self._installed = False

    def install(self) -> None:
        if self._installed:
            return
        original_send = requests.sessions.Session.send
        handler = self._handler

        def _patched_send(
            session: requests.sessions.Session,
            request: PreparedRequest,
            **kwargs: Any,
        ) -> Response:
            return handler(session, request, original_send, **kwargs)

        self._original_send = original_send
        requests.sessions.Session.send = _patched_send  # type: ignore[assignment]
        self._installed = True

    def uninstall(self) -> None:
        if not self._installed:
            return
        requests.sessions.Session.send = self._original_send  # type: ignore[assignment]
        self._original_send = None
        self._installed = False


# ----- adapters: requests <-> cassette ------------------------------------


def request_record_from_prepared(prep: PreparedRequest) -> RequestRecord:
    base, query = split_url(prep.url or "")
    return RequestRecord(
        method=(prep.method or "GET").upper(),
        url=base,
        query=query,
        body_sha256=body_sha256(prep.body),
    )


def response_record_from_response(resp: Response) -> ResponseRecord:
    body_text, body_b64 = encode_body(resp.content, resp.encoding)
    return ResponseRecord(
        status_code=resp.status_code,
        headers=dict(resp.headers),
        body_text=body_text,
        body_b64=body_b64,
        encoding=resp.encoding,
        url=resp.url,
    )


def response_from_record(rec: ResponseRecord, prep: PreparedRequest) -> Response:
    """Build a ``requests.Response`` from a recorded ``ResponseRecord``."""
    resp = Response()
    resp.status_code = rec.status_code
    resp.headers.update(rec.headers or {})
    resp._content = rec.content_bytes()
    resp.encoding = rec.encoding
    resp.url = rec.url or (prep.url or "")
    resp.request = prep
    resp.reason = _status_reason(rec.status_code)
    # stream=True isn't supported in replay (no connector in the repo uses it).
    return resp


def _status_reason(status: int) -> str:
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
