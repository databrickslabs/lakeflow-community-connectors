"""Bounded HTTP transport for idempotent HubSpot reads."""

import math
import time
from typing import Callable, Optional

import requests

HUBSPOT_MAX_RETRIES = 5
HUBSPOT_MAX_BACKOFF_S = 30.0
HUBSPOT_REQUEST_TIMEOUT_S = 60
HUBSPOT_RETRIABLE_STATUSES = frozenset({429, 500, 502, 503})


class HubspotHttpClient:
    """Replay only safe GETs and read-only POSTs after transient failures."""

    def __init__(
        self,
        auth_header: dict,
        sleep: Optional[Callable[[float], None]] = None,
        throttle: Optional[Callable[[str, float], None]] = None,
    ) -> None:
        self._auth_header = auth_header
        self._sleep = sleep or time.sleep
        self._external_throttle = throttle
        self._last_call = {}

    @staticmethod
    def retry_delay(response, attempt: int) -> float:
        """Return a finite, bounded delay from Retry-After or exponential backoff."""
        headers = getattr(response, "headers", None) or {}
        retry_after = headers.get("Retry-After")
        if retry_after is not None:
            try:
                seconds = float(retry_after)
                if math.isfinite(seconds) and seconds >= 0:
                    return min(seconds, HUBSPOT_MAX_BACKOFF_S)
            except (TypeError, ValueError):
                pass
        return min(2.0 ** attempt, HUBSPOT_MAX_BACKOFF_S)

    def _throttle(self, kind: Optional[str], min_interval: float) -> None:
        if not kind or min_interval <= 0:
            return
        if self._external_throttle is not None:
            self._external_throttle(kind, min_interval)
            return
        now = time.monotonic()
        last = self._last_call.get(kind)
        if last is not None and now - last < min_interval:
            self._sleep(min_interval - (now - last))
        self._last_call[kind] = time.monotonic()

    def _request(
        self,
        method: str,
        url: str,
        *,
        operation: str,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
        kind: Optional[str] = None,
        min_interval: float = 0,
        timeout: int = HUBSPOT_REQUEST_TIMEOUT_S,
    ):
        """Issue an idempotent read and retry only bounded transient failures."""
        send = getattr(requests, method)
        request_args = {"headers": self._auth_header, "timeout": timeout}
        if params is not None:
            request_args["params"] = params
        if json_body is not None:
            request_args["json"] = json_body

        for attempt in range(HUBSPOT_MAX_RETRIES + 1):
            self._throttle(kind, min_interval)
            try:
                response = send(url, **request_args)
            except requests.RequestException:
                if attempt < HUBSPOT_MAX_RETRIES:
                    self._sleep(min(2.0 ** attempt, HUBSPOT_MAX_BACKOFF_S))
                    continue
                raise RuntimeError(
                    f"HubSpot {operation} failed after retries (request error)"
                ) from None

            if response.status_code in HUBSPOT_RETRIABLE_STATUSES:
                if attempt < HUBSPOT_MAX_RETRIES:
                    self._sleep(self.retry_delay(response, attempt))
                    continue
                raise RuntimeError(
                    f"HubSpot {operation} failed after retries "
                    f"(status={response.status_code})"
                ) from None
            return response

        raise RuntimeError(f"HubSpot {operation} exhausted retries")  # pragma: no cover

    def get(self, url: str, *, operation: str, params=None, kind=None, min_interval=0):
        return self._request(
            "get", url, operation=operation, params=params,
            kind=kind, min_interval=min_interval,
        )

    def post(self, url: str, body: dict, *, operation: str, kind=None, min_interval=0):
        """POST only to HubSpot Search, batch-read, and list-read endpoints."""
        return self._request(
            "post", url, operation=operation, json_body=body,
            kind=kind, min_interval=min_interval,
        )
