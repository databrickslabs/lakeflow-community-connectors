"""HTTP/auth plumbing and small helpers for the Power BI connector.

The client is deliberately built out of module-level ``requests`` calls and
plain attributes (strings / datetimes) rather than a ``requests.Session`` so
that the owning connector instance stays picklable — Spark ships the
connector to executors for partitioned reads.
"""

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from databricks.labs.community_connector.sources.power_bi_semantic_model.power_bi_semantic_model_schemas import (  # noqa: E501  pylint: disable=line-too-long
    ADMIN_DENIED_STATUS_CODES,
    DAX_COLUMN_TYPES,
    DAX_RESERVED_COLUMN_NAMES,
    DEFAULT_TIMEOUT_SECONDS,
    ENTRA_LOGIN_BASE,
    INITIAL_BACKOFF_SECONDS,
    MAX_RETRIES,
    POWER_BI_SCOPE,
    RETRIABLE_STATUS_CODES,
)


class PowerBiAdminAccessDenied(RuntimeError):
    """Raised when an ``/admin/*`` call is rejected with 401/403.

    Signals that the service principal has not been enabled for the Power BI
    Admin APIs, which is the connector's cue to fall back to the
    membership-scoped (non-admin) endpoints.
    """


class PowerBiApiError(RuntimeError):
    """Raised for any non-retriable, non-admin-denial API failure."""


class PowerBiClient:
    """Minimal Power BI REST client with Entra ID auth.

    Supports the two auth methods Power BI's own docs describe:

    - ``service_principal`` — OAuth 2.0 client-credentials (app-only), via
      ``client_secret``. Preferred: no user account required, works with
      MFA-enforced tenants.
    - ``user`` — the classic "master user" pattern, via Entra ID's Resource
      Owner Password Credentials (ROPC) grant (``username``/``password``).
      Microsoft treats ROPC as legacy and it does not support interactive
      challenges, so the account must not have MFA enabled.

    ``client_secret`` takes precedence when both are present.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        login_base: str = ENTRA_LOGIN_BASE,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.timeout = timeout
        self.login_base = login_base.rstrip("/")
        self._access_token: str | None = None
        self._token_expiry: datetime | None = None

    # -- auth ------------------------------------------------------------

    def get_access_token(self) -> str:
        """Return a cached bearer token, minting a new one when near expiry."""
        if not (self.tenant_id and self.client_id):
            raise ValueError(
                "Power BI connector requires connection options 'tenant_id' "
                "and 'client_id', plus either 'client_secret' "
                "(service_principal) or 'username'+'password' (user)."
            )
        if not (self.client_secret or (self.username and self.password)):
            raise ValueError(
                "Power BI connector requires either 'client_secret' "
                "(service_principal, Entra ID app-only auth) or "
                "'username'+'password' (user, Entra ID ROPC auth)."
            )

        if (
            self._access_token
            and self._token_expiry
            and datetime.now(timezone.utc) < self._token_expiry
        ):
            return self._access_token

        token_url = f"{self.login_base}/{self.tenant_id}/oauth2/v2.0/token"
        if self.client_secret:
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                # ``.default`` mints a token for the Power BI resource using
                # the application permissions granted in the Power BI Admin
                # Portal.
                "scope": POWER_BI_SCOPE,
            }
        else:
            payload = {
                "grant_type": "password",
                "client_id": self.client_id,
                "username": self.username,
                "password": self.password,
                "scope": POWER_BI_SCOPE,
            }

        try:
            resp = requests.post(token_url, data=payload, timeout=self.timeout)
        except requests.RequestException as exc:
            raise PowerBiApiError(f"Entra ID token request failed: {exc}") from exc

        if resp.status_code != 200:
            tenant_preview = (
                f"{self.tenant_id[:8]}..." if len(self.tenant_id or "") > 8 else "unset"
            )
            raise PowerBiApiError(
                f"Entra ID token request failed with status {resp.status_code}. "
                f"tenant_id (first 8 chars): {tenant_preview}. "
                f"Response: {resp.text[:500]}"
            )

        body = resp.json()
        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 3600))
        # Refresh 5 minutes early so long partitions never run out mid-read.
        self._token_expiry = datetime.now(timezone.utc) + timedelta(
            seconds=max(60, expires_in - 300)
        )
        return self._access_token

    # -- requests --------------------------------------------------------

    def get(self, url: str, params: dict | None = None) -> dict:
        return self._request("GET", url, params=params)

    def post(self, url: str, params: dict | None = None, json_body: dict | None = None) -> dict:
        return self._request("POST", url, params=params, json_body=json_body)

    def _request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> dict:
        """Issue a request, honouring ``Retry-After`` and backing off on 5xx."""
        backoff = INITIAL_BACKOFF_SECONDS
        last_error = ""

        for attempt in range(MAX_RETRIES):
            headers = {
                "Authorization": f"Bearer {self.get_access_token()}",
                "Content-Type": "application/json",
            }
            try:
                resp = requests.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt == MAX_RETRIES - 1:
                    raise PowerBiApiError(
                        f"{method} {url} failed after {MAX_RETRIES} attempts: {exc}"
                    ) from exc
                time.sleep(backoff)
                backoff *= 2
                continue

            if 200 <= resp.status_code < 300:
                return _decode_json(resp)

            if resp.status_code in ADMIN_DENIED_STATUS_CODES:
                # Drop the cached token: a 401 may simply mean it expired.
                if resp.status_code == 401:
                    self._access_token = None
                    self._token_expiry = None
                raise PowerBiAdminAccessDenied(
                    f"{method} {url} was rejected with {resp.status_code}. "
                    f"Verify that 'Allow service principals to use Power BI APIs' "
                    f"is enabled for this app and, for /admin endpoints, that the "
                    f"Admin API tenant settings are on. Response: {resp.text[:300]}"
                )

            if resp.status_code in RETRIABLE_STATUS_CODES and attempt < MAX_RETRIES - 1:
                # Power BI returns Retry-After (seconds) on 429.
                retry_after = resp.headers.get("Retry-After")
                try:
                    delay = int(retry_after) if retry_after else backoff
                except (TypeError, ValueError):
                    delay = backoff
                time.sleep(min(delay, 300))
                backoff *= 2
                last_error = f"status {resp.status_code}: {resp.text[:300]}"
                continue

            raise PowerBiApiError(
                f"{method} {url} failed with status {resp.status_code}: {resp.text[:500]}"
            )

        raise PowerBiApiError(f"{method} {url} failed after {MAX_RETRIES} attempts: {last_error}")


def _decode_json(resp: Any) -> dict:
    """Return the response body as a dict; ``{}`` for empty bodies."""
    if not resp.content:
        return {}
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {"value": body}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    """``2026-08-16T12:34:56.789Z`` — the canonical cursor representation."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (with or without a trailing ``Z``).

    Returns ``None`` when the value is missing or unparseable so callers can
    decide how to treat records with no usable cursor instead of blowing up.
    """
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def parse_int_option(
    table_options: dict, key: str, default: int, minimum: int = 1, maximum: int | None = None
) -> int:
    """Read an int table option, clamping it into ``[minimum, maximum]``."""
    raw = table_options.get(key)
    try:
        value = int(raw) if raw is not None and raw != "" else default
    except (TypeError, ValueError):
        value = default
    value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def parse_bool_option(table_options: dict, key: str, default: bool) -> bool:
    raw = table_options.get(key)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def parse_csv_option(table_options: dict, key: str) -> list[str]:
    """Split a comma-separated table option into a list of trimmed values."""
    raw = table_options.get(key)
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def chunked(items: list, size: int) -> list[list]:
    """Split ``items`` into consecutive chunks of at most ``size`` elements."""
    if size <= 0:
        size = 1
    return [items[i : i + size] for i in range(0, len(items), size)]


def json_encode(value: Any) -> str | None:
    """JSON-encode dict/list values; pass strings through untouched."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)


def parse_dax_columns_option(table_options: dict) -> list[dict]:
    """Parse and validate the ``dax_columns`` table option.

    The option is a JSON array (google_analytics_aggregated passes its
    ``dimensions``/``metrics`` the same way) of entries describing the columns
    the configured DAX query returns::

        [{"dax": "Sales[Region]",  "name": "region",       "type": "string"},
         {"dax": "[Total Amount]", "name": "total_amount", "type": "double"}]

    ``dax`` is the key as it appears in the API response; ``name`` is the Spark
    column to land it in.  They are kept separate because DAX column names carry
    brackets and spaces that make poor Delta identifiers.  ``name`` defaults to
    ``dax`` when omitted and ``type`` defaults to ``string``.

    Returns ``[]`` when the option is absent, which selects the map-based
    fallback schema.  Raises ``ValueError`` on a malformed entry — a typo here
    would otherwise surface as a silently all-null column.
    """
    raw = table_options.get("dax_columns")
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return []

    if isinstance(raw, (list, tuple)):
        entries: Any = list(raw)
    else:
        try:
            entries = json.loads(str(raw))
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Table option 'dax_columns' is not valid JSON: {exc}. Expected a "
                'JSON array like [{"dax": "Sales[Region]", "name": "region", '
                '"type": "string"}].'
            ) from exc

    if not isinstance(entries, list):
        raise ValueError(
            "Table option 'dax_columns' must be a JSON array of column objects, "
            f"got {type(entries).__name__}."
        )

    specs: list[dict] = []
    seen: set[str] = set()
    for index, entry in enumerate(entries):
        # A bare string is accepted as shorthand for a string-typed column
        # whose Spark name matches its DAX name.
        if isinstance(entry, str):
            entry = {"dax": entry}
        if not isinstance(entry, dict):
            raise ValueError(
                f"Entry {index} of 'dax_columns' must be an object or a string, "
                f"got {type(entry).__name__}."
            )

        dax_name = str(entry.get("dax") or entry.get("dax_name") or "").strip()
        if not dax_name:
            raise ValueError(
                f"Entry {index} of 'dax_columns' is missing the required 'dax' "
                "key (the column name as returned by the API)."
            )

        column_name = str(entry.get("name") or dax_name).strip()
        if column_name in DAX_RESERVED_COLUMN_NAMES:
            raise ValueError(
                f"'dax_columns' entry {index} uses reserved column name "
                f"'{column_name}'. Reserved names: "
                f"{sorted(DAX_RESERVED_COLUMN_NAMES)}."
            )
        if column_name in seen:
            raise ValueError(f"'dax_columns' declares column '{column_name}' more than once.")
        seen.add(column_name)

        type_name = str(entry.get("type") or "string").strip().lower()
        if type_name not in DAX_COLUMN_TYPES:
            raise ValueError(
                f"'dax_columns' entry {index} has unsupported type "
                f"'{type_name}'. Supported: {sorted(DAX_COLUMN_TYPES)}."
            )

        specs.append({"dax": dax_name, "name": column_name, "type": type_name})
    return specs


def query_fingerprint(query: str) -> str:
    """Stable short identity for a configured DAX query string.

    Whitespace-insensitive so reformatting a query does not orphan the rows it
    already produced, but any change to the query's actual text does.
    """
    normalised = " ".join(str(query).split())
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()[:16]


def stringify(value: Any) -> str | None:
    """Render a DAX cell value for the string-typed ``columns`` map."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        # str(True) is "True"; JSON's "true" round-trips better.
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json_encode(value)


def none_if_empty(value: Any) -> Any:
    """Normalise ``{}`` to ``None``.

    The framework's ``parse_value`` rejects an empty dict for a StructType
    field, so any struct-shaped column has to be nulled out explicitly.
    """
    if isinstance(value, dict) and not value:
        return None
    return value
