"""HTTP client and authentication for the OSI PI connector.

This module contains the PI Web API HTTP client that handles authentication,
request execution, and response processing.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

from databricks.labs.community_connector.sources.osipi.osipi_utils import (
    as_bool,
    batch_request_dict,
    batch_response_items,
    isoformat_z,
    parse_pi_time,
    parse_ts,
    utcnow,
)


class PiWebApiClient:
    """HTTP client for PI Web API with authentication support.

    Handles:
    - Bearer token authentication
    - OIDC client credentials flow
    - Basic authentication
    - HTTP GET/POST with retry on 401
    - Batch request execution
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """Initialize the PI Web API client.

        Args:
            options: Configuration dictionary with connection parameters.
        """
        self.options = options

        # Resolve base URL from various option keys
        self.base_url = (
            options.get("pi_base_url") or options.get("pi_web_api_url") or ""
        ).rstrip("/")

        # UC ConnectionType.HTTP (bearer) exposes standard option keys
        if not self.base_url:
            host = (options.get("host") or "").strip()
            base_path = (options.get("base_path") or "").strip()
            port = (options.get("port") or "").strip()

            if host:
                # Normalize scheme
                if host.startswith("http://") or host.startswith("https://"):
                    scheme_host = host
                else:
                    scheme_host = "https://" + host

                # Optional port
                if port and ":" not in scheme_host.split("//", 1)[-1]:
                    scheme_host = scheme_host.rstrip("/") + f":{port}"

                # Optional base_path
                if base_path:
                    if not base_path.startswith("/"):
                        base_path = "/" + base_path
                    scheme_host = scheme_host.rstrip("/") + base_path.rstrip("/")

                self.base_url = scheme_host.rstrip("/")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.verify_ssl = as_bool(options.get("verify_ssl"), default=True)
        self._auth_resolved = False

        # OIDC token cache
        self._oidc_access_token: Optional[str] = None
        self._oidc_token_expires_at: Optional[datetime] = None

    def ensure_auth(self) -> None:  # pylint: disable=too-many-return-statements,too-many-branches
        """Authenticate using UC Connection-injected options."""
        # If already resolved, check OIDC token expiry
        if self._auth_resolved:
            if self._oidc_access_token and self._oidc_token_expires_at:
                if utcnow() < self._oidc_token_expires_at - timedelta(minutes=5):
                    return
                self._auth_resolved = False
            else:
                return

        connection_name = self.options.get("databricks.connection")
        if connection_name:
            print(f"🔍 Using UC Connection: {connection_name}")

        # Extract auth parameters
        access_token = (
            self.options.get("access_token")
            or self.options.get("bearer_token")
            or self.options.get("bearer_value")
        )
        workspace_host = self.options.get("workspace_host")
        client_id = self.options.get("client_id")
        client_secret = self.options.get("client_secret")
        username = self.options.get("username")
        password = self.options.get("password")

        # Opt-in: allow unauthenticated access
        if as_bool(self.options.get("allow_anonymous"), default=False):
            print("⚠️  AUTH: allow_anonymous=true (no Authorization header will be sent)")
            self.session.headers.pop("Authorization", None)
            self.session.auth = None
            self._oidc_access_token = None
            self._oidc_token_expires_at = None
            self._auth_resolved = True
            return

        # Method 1: Bearer token
        if access_token:
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            self._auth_resolved = True
            return

        # Method 2: OIDC with client credentials
        if workspace_host and client_id and client_secret:
            if not workspace_host.startswith("http://") and not workspace_host.startswith(
                "https://"
            ):
                workspace_host = "https://" + workspace_host

            if self._oidc_access_token and self._oidc_token_expires_at:
                if utcnow() < self._oidc_token_expires_at - timedelta(minutes=5):
                    self.session.headers.update(
                        {"Authorization": f"Bearer {self._oidc_access_token}"}
                    )
                    self._auth_resolved = True
                    return

            token_url = f"{workspace_host}/oidc/v1/token"
            resp = requests.post(
                token_url,
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                auth=(client_id, client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json() or {}
            token = payload.get("access_token")
            if not token:
                raise RuntimeError("OIDC endpoint did not return access_token")
            expires_in = int(payload.get("expires_in") or 3600)
            self._oidc_access_token = token
            self._oidc_token_expires_at = utcnow() + timedelta(seconds=expires_in)
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            self._auth_resolved = True
            return

        # Method 3: Basic auth
        if username and password:
            self.session.auth = (username, password)
            self._auth_resolved = True
            return

        raise RuntimeError(
            "No valid authentication credentials found in options. "
            "Expected one of: access_token, OR (workspace_host + client_id + client_secret), "
            "OR (username + password)."
        )

    def _reset_auth(self) -> None:
        """Reset authentication state for retry."""
        self._auth_resolved = False
        self._oidc_access_token = None
        self._oidc_token_expires_at = None
        self.session.headers.pop("Authorization", None)
        self.session.auth = None

    def get_json(self, path: str, params: Optional[Any] = None) -> dict:
        """Make a GET request and return JSON response.

        Args:
            path: API path (e.g., "/piwebapi/dataservers").
            params: Query parameters.

        Returns:
            JSON response as dictionary.
        """
        url = f"{self.base_url}{path}"
        debug = as_bool(self.options.get("debug_http"), default=False)

        if debug:
            print("🔍 DEBUG get_json:")
            print(f"   URL: {url}")
            print(f"   Headers: {dict(self.session.headers)}")
            print(f"   Auth: {self.session.auth}")
            print(f"   Params: {params}")
            print(f"   verify_ssl: {self.verify_ssl}")

        for attempt in range(2):
            self.ensure_auth()
            r = self.session.get(url, params=params, timeout=60, verify=self.verify_ssl)

            # Some proxies/apps behave differently with a trailing slash
            if r.status_code == 404 and not url.endswith("/"):
                r_slash = self.session.get(
                    url + "/", params=params, timeout=60, verify=self.verify_ssl
                )
                if r_slash.status_code < 400:
                    r = r_slash

            if debug:
                print(f"   Response status: {r.status_code}")
                print(f"   Response headers: {dict(r.headers)}")

            if r.status_code == 401 and attempt == 0:
                if debug:
                    print("   Got 401, retrying auth...")
                self._reset_auth()
                continue

            try:
                r.raise_for_status()
            except requests.HTTPError as e:
                body = (getattr(r, "text", None) or "")[:2000]
                hdrs = {
                    k: (v[:200] if isinstance(v, str) else str(v)[:200])
                    for k, v in (getattr(r, "headers", {}) or {}).items()
                }
                raise requests.HTTPError(
                    f"{e}. Response headers: {hdrs}. Response body (truncated): {body}",
                    response=r,
                ) from e

            return r.json()

        raise RuntimeError("Authentication failed after retry")

    def post_json(self, path: str, payload: Any) -> dict:
        """Make a POST request with JSON payload and return JSON response.

        Args:
            path: API path.
            payload: JSON payload to send.

        Returns:
            JSON response as dictionary.
        """
        url = f"{self.base_url}{path}"
        self.session.headers.setdefault("Content-Type", "application/json")

        for attempt in range(2):
            self.ensure_auth()
            r = self.session.post(url, json=payload, timeout=120, verify=self.verify_ssl)

            if r.status_code == 401 and attempt == 0:
                self._reset_auth()
                continue

            try:
                r.raise_for_status()
            except requests.HTTPError as e:
                body = (getattr(r, "text", None) or "")[:2000]
                hdrs = {
                    k: (v[:200] if isinstance(v, str) else str(v)[:200])
                    for k, v in (getattr(r, "headers", {}) or {}).items()
                }
                raise requests.HTTPError(
                    f"{e}. Response headers: {hdrs}. Response body (truncated): {body}",
                    response=r,
                ) from e

            return r.json()

        raise RuntimeError("Authentication failed after retry")

    def batch_execute(self, requests_list: List[dict]) -> List[Tuple[str, dict]]:
        """Execute a batch request to PI Web API.

        Args:
            requests_list: List of request dictionaries.

        Returns:
            List of (request_id, response_dict) tuples.
        """
        payload = batch_request_dict(requests_list)
        resp_json = self.post_json("/piwebapi/batch", payload)
        return batch_response_items(resp_json)


# =============================================================================
# Time range and pagination helpers
# =============================================================================


def compute_time_range(
    start_offset: dict,
    table_options: Dict[str, str],
    *,
    apply_window_seconds: bool = False,
) -> Tuple[str, str]:
    """Compute start/end time range for time-series reads.

    Args:
        start_offset: Offset dictionary with optional "offset" key.
        table_options: Table options with time configuration.
        apply_window_seconds: Whether to apply window_seconds cap.

    Returns:
        Tuple of (start_time_str, end_time_str) in ISO format.
    """
    now = utcnow()

    end_opt = table_options.get("endTime") or table_options.get("end_time") or "*"
    end_dt = parse_pi_time(end_opt, now=now)

    start_dt = _start_dt_from_offset(start_offset)
    if start_dt is None:
        start_opt = table_options.get("startTime") or table_options.get("start_time")
        if start_opt:
            start_dt = parse_pi_time(str(start_opt), now=end_dt)
        else:
            lookback_minutes = int(table_options.get("lookback_minutes", 60))
            start_dt = end_dt - timedelta(minutes=lookback_minutes)

    if apply_window_seconds:
        window_seconds = int(table_options.get("window_seconds", 0) or 0)
        if window_seconds > 0:
            end_dt = min(end_dt, start_dt + timedelta(seconds=window_seconds))

    return isoformat_z(start_dt), isoformat_z(end_dt)


def _start_dt_from_offset(start_offset: dict) -> Optional[datetime]:
    """Extract start datetime from offset dictionary."""
    if start_offset and isinstance(start_offset, dict):
        off = start_offset.get("offset")
        if isinstance(off, str) and off:
            try:
                return parse_ts(off)
            except Exception:
                return None
    return None


def build_streamset_params(  # pylint: disable=too-many-arguments
    webids: List[str],
    *,
    start_str: str,
    end_str: str,
    max_count: Optional[int] = None,
    interval: Optional[str] = None,
    intervals: Optional[int] = None,
    selected_fields: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """Build query parameters for StreamSet endpoints.

    Args:
        webids: List of tag WebIDs.
        start_str: Start time string.
        end_str: End time string.
        max_count: Maximum count per request.
        interval: Interval string (e.g., "1m").
        intervals: Number of intervals.
        selected_fields: Fields to select.

    Returns:
        List of (key, value) tuples for query parameters.
    """
    params: List[Tuple[str, str]] = [("webId", w) for w in webids]
    params += [("startTime", start_str), ("endTime", end_str)]
    if interval:
        params.append(("interval", str(interval)))
    if intervals is not None:
        params.append(("intervals", str(intervals)))
    if max_count is not None:
        params.append(("maxCount", str(max_count)))
    if selected_fields:
        params.append(("selectedFields", str(selected_fields)))
    return params


def paginate_time_series(  # pylint: disable=too-many-branches,too-many-statements,too-many-nested-blocks
    get_data_func,
    start_str: str,
    end_str: str,
    max_count: int,
) -> Iterator[dict]:
    """Generic pagination for time-series endpoints using time-based cursors.

    Args:
        get_data_func: Function that takes (start, end) and returns API response.
        start_str: Start time string.
        end_str: End time string.
        max_count: Maximum records per page.

    Yields:
        Stream or item dictionaries from paginated responses.
    """
    current_start = start_str

    while True:
        data = get_data_func(current_start, end_str)

        items_container = data.get("Items", []) or []
        if not items_container:
            break

        # Check if this is a streamset response
        is_streamset = (
            isinstance(items_container, list)
            and len(items_container) > 0
            and isinstance(items_container[0], dict)
            and "WebId" in items_container[0]
        )

        page_record_count = 0
        if is_streamset:
            for stream in items_container:
                stream_items = stream.get("Items", []) or []
                page_record_count += len(stream_items)
                yield stream
        else:
            page_record_count = len(items_container)
            yield from items_container

        if page_record_count < max_count:
            break

        # Track timestamps for pagination
        last_timestamps: List[datetime] = []
        if is_streamset:
            for stream in items_container:
                stream_items = stream.get("Items", []) or []
                stream_last_ts = None
                for item in stream_items:
                    ts = item.get("Timestamp")
                    if ts:
                        try:
                            ts_dt = parse_ts(ts)
                            if stream_last_ts is None or ts_dt > stream_last_ts:
                                stream_last_ts = ts_dt
                        except Exception:
                            pass
                if stream_last_ts:
                    last_timestamps.append(stream_last_ts)
        else:
            for item in items_container:
                ts = item.get("Timestamp")
                if ts:
                    try:
                        ts_dt = parse_ts(ts)
                        last_timestamps.append(ts_dt)
                    except Exception:
                        pass

        if not last_timestamps:
            break

        if is_streamset:
            last_timestamp = min(last_timestamps)
        else:
            last_timestamp = max(last_timestamps)

        current_start = isoformat_z(last_timestamp + timedelta(microseconds=1))

        try:
            if parse_ts(current_start) >= parse_ts(end_str):
                break
        except Exception:
            break
