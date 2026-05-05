"""Low-level HTTP intercepts.

Every connector in this repo issues HTTP through one of two stdlib-or-near-stdlib
choke points:

- ``requests.sessions.Session.send`` — for connectors using the ``requests``
  library (most of them).
- ``urllib.request.urlopen`` — for the few that stick to stdlib (``dicomweb``).

``Interceptor`` patches both so the simulator catches all of it. The handler
itself is library-agnostic: requests-shaped types (``PreparedRequest`` /
``Response``) are the lingua franca, and urllib calls are bridged in and out
via small adapters in this file.
"""

from __future__ import annotations

import email.message
import urllib.request
from io import BytesIO
from typing import Any, Callable, Mapping, Optional, Union
from urllib.parse import urlencode

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
    """Install/uninstall HTTP intercepts on both ``requests`` and ``urllib``."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self._original_send: Optional[Callable] = None
        self._original_urlopen: Optional[Callable] = None
        self._installed = False

    def install(self) -> None:
        if self._installed:
            return
        handler = self._handler

        # ----- requests -----
        original_send = requests.sessions.Session.send

        def _patched_send(
            session: requests.sessions.Session,
            request: PreparedRequest,
            **kwargs: Any,
        ) -> Response:
            return handler(session, request, original_send, **kwargs)

        self._original_send = original_send
        requests.sessions.Session.send = _patched_send  # type: ignore[assignment]

        # ----- urllib -----
        # Re-routes ``urllib.request.urlopen(req_or_url, ...)`` through the
        # same handler. We adapt the urllib request to a ``PreparedRequest``
        # on the way in, then wrap the handler's ``Response`` as an
        # http.client.HTTPResponse-shaped duck object on the way out.
        original_urlopen = urllib.request.urlopen

        def _patched_urlopen(
            req_or_url: Union[str, urllib.request.Request],
            data: Any = None,
            timeout: Any = None,
            **kwargs: Any,
        ):
            prep = prepared_from_urllib(req_or_url, data=data)
            resp = handler(
                _StdlibSession(original_urlopen, timeout=timeout),
                prep,
                _StdlibOriginalSend(original_urlopen, timeout=timeout),
            )
            return urllib_response_from_requests(resp)

        self._original_urlopen = original_urlopen
        urllib.request.urlopen = _patched_urlopen  # type: ignore[assignment]

        self._installed = True

    def uninstall(self) -> None:
        if not self._installed:
            return
        requests.sessions.Session.send = self._original_send  # type: ignore[assignment]
        urllib.request.urlopen = self._original_urlopen  # type: ignore[assignment]
        self._original_send = None
        self._original_urlopen = None
        self._installed = False


# ----- adapters: requests <-> cassette ----------------------------------


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
    return resp


# ----- adapters: urllib <-> requests ------------------------------------


def prepared_from_urllib(
    req_or_url: Union[str, urllib.request.Request],
    data: Any = None,
) -> PreparedRequest:
    """Convert a urllib request (Request or URL string) to a ``PreparedRequest``.

    Lets the same handler chain (recorder, replayer, simulator, validator)
    process urllib calls without knowing the difference.
    """
    if isinstance(req_or_url, urllib.request.Request):
        url = req_or_url.full_url
        method = req_or_url.get_method()
        # urllib stores headers as a dict-like via ``.headers``, but also
        # via ``.unredirected_hdrs``. ``header_items()`` returns both.
        headers = dict(req_or_url.header_items())
        body = req_or_url.data if req_or_url.data is not None else data
    else:
        url = str(req_or_url)
        method = "POST" if data is not None else "GET"
        headers = {}
        body = data

    if isinstance(body, dict):
        body = urlencode(body).encode("utf-8")
    elif isinstance(body, str):
        body = body.encode("utf-8")

    prep = PreparedRequest()
    prep.method = method.upper()
    prep.url = url
    prep.headers = requests.structures.CaseInsensitiveDict(headers)
    prep.body = body
    return prep


class _UrllibFakeResponse:
    """Minimal duck of ``http.client.HTTPResponse`` for replay/simulate.

    The ``dicomweb`` connector reads ``.status``, ``.read()``, and
    ``.headers.get(...)``. That's all we provide. Also implements the
    context-manager protocol since ``with urllib.request.urlopen(...)``
    is a common pattern.
    """

    def __init__(self, status: int, headers: Mapping[str, str], body: bytes, url: str) -> None:
        self.status = status
        self._body = body
        self._body_io = BytesIO(body)
        self.url = url
        # ``http.client.HTTPResponse.headers`` is an ``email.message.Message``;
        # the connector calls ``.get()`` so a Message exposes the right API.
        msg = email.message.Message()
        for k, v in (headers or {}).items():
            msg[k] = v
        self.headers = msg

    def read(self, amt: Optional[int] = None) -> bytes:
        if amt is None:
            return self._body_io.read()
        return self._body_io.read(amt)

    def __enter__(self) -> "_UrllibFakeResponse":
        return self

    def __exit__(self, *exc_info) -> None:
        self._body_io.close()

    def close(self) -> None:
        self._body_io.close()


def urllib_response_from_requests(resp: Response) -> _UrllibFakeResponse:
    """Wrap a ``requests.Response`` as a ``urllib`` HTTPResponse-shaped duck."""
    return _UrllibFakeResponse(
        status=resp.status_code,
        headers=dict(resp.headers),
        body=resp.content,
        url=resp.url,
    )


# ----- live-mode helpers for urllib -------------------------------------


class _StdlibSession:
    """Stand-in for the requests.Session arg passed to the handler.

    Connectors don't poke at the session; the handler chain only forwards
    it through to ``original_send``. We pass an opaque object so the type
    is consistent with the requests path.
    """

    def __init__(self, original_urlopen: Callable, timeout: Any = None) -> None:
        self._original_urlopen = original_urlopen
        self._timeout = timeout


class _StdlibOriginalSend:
    """Callable that replays a ``PreparedRequest`` through the original urllib.

    The handler treats ``original_send(session, prep, **kw)`` as "make the
    real network call and give me a ``requests.Response``." For urllib we
    rebuild a urllib.Request, call original urlopen, and convert the
    resulting HTTPResponse back to a requests.Response so downstream
    recorder/coverage/validator code reads it the same way.
    """

    def __init__(self, original_urlopen: Callable, timeout: Any = None) -> None:
        self._original_urlopen = original_urlopen
        self._timeout = timeout

    def __call__(
        self, session: Any, prep: PreparedRequest, **kwargs: Any
    ) -> Response:
        req = urllib.request.Request(
            url=prep.url or "",
            data=prep.body if isinstance(prep.body, (bytes, bytearray)) else None,
            headers=dict(prep.headers or {}),
            method=(prep.method or "GET").upper(),
        )
        urllib_resp = (
            self._original_urlopen(req, timeout=self._timeout)
            if self._timeout is not None
            else self._original_urlopen(req)
        )
        return _requests_response_from_urllib(urllib_resp, prep)


def _requests_response_from_urllib(urllib_resp: Any, prep: PreparedRequest) -> Response:
    """Wrap an ``http.client.HTTPResponse`` as a ``requests.Response``.

    Used in live mode so the recorder/coverage/validator path stays
    requests-typed regardless of which library issued the call.
    """
    body = urllib_resp.read()
    headers = {}
    if hasattr(urllib_resp, "headers"):
        try:
            headers = dict(urllib_resp.headers.items())
        except Exception:  # email.message.Message edge cases
            headers = dict(urllib_resp.headers or {})
    resp = Response()
    resp.status_code = getattr(urllib_resp, "status", 200)
    resp._content = body
    resp.headers.update(headers)
    resp.url = getattr(urllib_resp, "url", prep.url or "")
    resp.request = prep
    resp.encoding = "utf-8"
    resp.reason = _status_reason(resp.status_code)
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
