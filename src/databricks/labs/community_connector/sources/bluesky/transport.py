"""Bounded HTTP requests and resumable, generation-pinned byte ranges."""

import json
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Any, overload

import requests

from databricks.labs.community_connector.sources.bluesky.errors import (
    ArchiveChanged,
    CursorTooOld,
    JetstreamError,
    ProtocolError,
    RefreshTimeout,
)
from databricks.labs.community_connector.sources.bluesky.options import Options

XRPC = "/xrpc/network.bsky.jetstream."
MAX_RESPONSE = 32 * 1024 * 1024


class Transport:
    """Perform bounded and authenticated HTTP requests to Jetstream.

    Parameters
    ----------
    options : Options
        Validated connector settings controlling authentication and request limits.

    Attributes
    ----------
    options : Options
        Settings shared with the planner and archive reader.
    session : requests.Session
        HTTP session with ambient credential discovery disabled.
    deadline : float
        Monotonic deadline shared by all requests in this refresh.
    """

    def __init__(self, options: Options) -> None:
        self.options = options
        self.session = requests.Session()
        # Avoid implicit netrc credentials and environment-level credential surprises.
        self.session.trust_env = False
        self.session.headers.update({"Accept-Encoding": "identity"})
        if options.api_key:
            self.session.headers["Authorization"] = f"Bearer {options.api_key}"
        self.deadline = time.monotonic() + options.refresh_timeout_seconds

    def __enter__(self) -> "Transport":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.session.close()

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RefreshTimeout("Refresh time budget exhausted; retain the previous checkpoint")
        return remaining

    def _pause(self, attempt: int, retry_after: str | None = None) -> None:
        delay = min(2**attempt, 20) + random.uniform(0, 0.25)
        if retry_after:
            try:
                delay = max(delay, float(retry_after))
            except ValueError:
                try:
                    date = parsedate_to_datetime(retry_after)
                    delay = max(delay, (date - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    pass
        if delay >= self.remaining():
            raise RefreshTimeout("Retry-After/backoff exceeds refresh budget; retry later")
        time.sleep(delay)

    @overload
    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        byte_range: None = None,
        checksum: str | None = None,
    ) -> dict[str, Any]: ...

    @overload
    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        byte_range: tuple[int, int],
        checksum: str,
    ) -> tuple[bytes, int]: ...

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        byte_range: tuple[int, int] | None = None,
        checksum: str | None = None,
    ) -> dict[str, Any] | tuple[bytes, int]:
        result = bytearray()
        total = None
        for attempt in range(self.options.max_retries + 1):
            self.remaining()
            headers = {}
            if byte_range:
                first, last = byte_range
                headers["Range"] = f"bytes={first + len(result)}-{last}"
                headers["If-Match"] = f'"{checksum}"'
            retry_after = None
            try:
                with self.session.request(
                    method,
                    self.options.endpoint + XRPC + endpoint,
                    json=body,
                    params=params,
                    headers=headers,
                    allow_redirects=False,
                    stream=True,
                    timeout=min(self.options.request_timeout_seconds, self.remaining()),
                ) as response:
                    status = response.status_code
                    if status in (401, 403):
                        raise JetstreamError("Jetstream denied archive access; check api_key")
                    if status in (404, 410, 412) and byte_range:
                        raise ArchiveChanged(
                            f"Archive changed or unavailable (HTTP {status}); do not replan"
                        )
                    if status == 400:
                        # Read at most a small error body; never put its contents in exceptions.
                        chunk = next(response.iter_content(4096), b"")
                        try:
                            error = json.loads(chunk).get("error")
                        except (ValueError, AttributeError):
                            error = None
                        if error in ("CursorTooOld", "OutdatedCursor"):
                            raise CursorTooOld(
                                "Jetstream rejected an expired cursor; no data skipped"
                            )
                        raise ProtocolError("Jetstream rejected the request (HTTP 400)")
                    if status == 429 or status in (408, 500, 502, 503, 504):
                        retry_after = response.headers.get("Retry-After")
                    elif 300 <= status < 400:
                        raise ProtocolError(
                            "Jetstream redirect refused; configure the final origin"
                        )
                    elif byte_range:
                        if status != 206:
                            raise ProtocolError("Jetstream must support exact HTTP byte ranges")
                        if response.headers.get("ETag") != f'"{checksum}"':
                            raise ArchiveChanged("Archive generation changed; refusing mixed bytes")
                        match = re.fullmatch(
                            r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", "")
                        )
                        if not match:
                            raise ProtocolError("Missing or invalid Content-Range")
                        begin, end, size = map(int, match.groups())
                        if (
                            begin != first + len(result)
                            or end != last
                            or end >= size
                            or (total is not None and total != size)
                        ):
                            raise ProtocolError("Unexpected Content-Range")
                        total = size
                        for chunk in response.iter_content(16384):
                            self.remaining()
                            result.extend(chunk)
                            if len(result) > last - first + 1:
                                raise ProtocolError("Oversized range response")
                        if len(result) == last - first + 1:
                            return bytes(result), total
                        # A quota-limited transfer can end cleanly; continue at exact received byte.
                    elif status == 200:
                        result.clear()
                        for chunk in response.iter_content(16384):
                            self.remaining()
                            result.extend(chunk)
                            if len(result) > MAX_RESPONSE:
                                raise ProtocolError("Response exceeds safety limit")
                        try:
                            decoded = json.loads(result)
                        except ValueError:
                            raise ProtocolError("Invalid JSON response") from None
                        if not isinstance(decoded, dict):
                            raise ProtocolError("Expected a JSON object")
                        return decoded
                    else:
                        raise JetstreamError(f"Jetstream request failed (HTTP {status})")
            except requests.RequestException:
                # requests exceptions may contain URLs/credentials; suppress their chaining.
                pass
            if attempt == self.options.max_retries:
                break
            if not byte_range:
                result.clear()
            self._pause(attempt, retry_after)
        raise JetstreamError(
            "Jetstream transient request failures exhausted retry budget"
        ) from None

    def plan(self, after: int, before: int | None = None) -> dict[str, Any]:
        body = {
            "afterSeq": after,
            "collections": list(self.options.collections),
            "dids": list(self.options.dids),
            "kinds": list(self.options.kinds),
        }
        if before is not None:
            body["beforeSeq"] = before
        return self._request("POST", "planSnapshot", body=body)

    def range(self, segment: str, checksum: str, first: int, last: int) -> tuple[bytes, int]:
        if not 0 <= first <= last or last - first + 1 > MAX_RESPONSE:
            raise ProtocolError("Archive range exceeds safety limit")
        return self._request(
            "GET",
            "getSegment",
            params={"name": segment},
            byte_range=(first, last),
            checksum=checksum,
        )
