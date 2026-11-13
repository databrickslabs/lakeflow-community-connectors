"""HTTP client, authentication, bundle pagination, and record extraction for FHIR R4.

Key design decisions (from fhir_api_doc.md):
- SmartAuthClient handles jwt_assertion, client_secret, and none auth types.
- Token is cached and refreshed 30s before expiry (tokens are short-lived: ~300s).
- iter_bundle_pages follows Bundle.link[relation=next] per FHIR R4 spec.
- extract_record maps a resource to one row of RESOURCE_SCHEMA, which every
  resource-type table shares; the resource itself is a VARIANT, so no
  per-type schema code exists.
- Every HTTP call has a timeout (see fhir_constants) to prevent hangs on slow FHIR servers.
"""

import json
import logging
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional
from urllib.parse import urljoin

import jwt  # pylint: disable=import-error
import requests
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, VariantType

from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    HTTP_TIMEOUT,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    MAX_RETRY_AFTER,
    PAGE_DELAY,
    RETRIABLE_STATUS_CODES,
    TOKEN_TIMEOUT,
)

logger = logging.getLogger(__name__)

# Shared by every resource-type table. `resource` holds the whole FHIR
# resource; the scalar columns exist because VARIANT has no min/max statistics,
# so filters on them are what let Delta skip files. resource_type is constant
# within a table but makes UNION ALL across tables self-describing.
RESOURCE_SCHEMA = StructType(
    [
        StructField("resource_type", StringType(), nullable=False),
        StructField("resource_id", StringType(), nullable=False),
        StructField("last_updated", TimestampType(), nullable=True),
        StructField("resource", VariantType(), nullable=True),
        StructField("fhir_version", StringType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=True),
    ]
)

_SUPPORTED_ALGORITHMS = {"RS384", "ES384"}
_TOKEN_AUTH_TYPES = {"jwt_assertion", "client_secret"}


def retry_after(resp: requests.Response, default: float, ceiling: Optional[float] = None) -> float:
    """Extract Retry-After (seconds) from a response, clamped to a safe range.

    Shared by the REST and bulk paths. A server may send a value that is
    negative (time.sleep raises), non-finite (also raises, or hangs), or an
    HTTP-date rather than a number; none of those may take the connector down.

    `ceiling` bounds the wait for callers that have their own deadline to
    respect. It defaults to None — honouring the server's request in full —
    because sleeping *less* than a 429 asked for is what provokes a request
    storm. Waits beyond MAX_RETRY_AFTER are logged so a stalled run is
    diagnosable from the driver log.
    """
    seconds = default
    header = resp.headers.get("Retry-After")
    if header:
        try:
            parsed = float(header)
        except (TypeError, ValueError):
            parsed = None  # HTTP-date form; fall back to the caller's backoff
        if parsed is not None and math.isfinite(parsed):
            seconds = parsed
    if ceiling is not None:
        seconds = min(seconds, ceiling)
    seconds = max(0.0, seconds)
    if seconds > MAX_RETRY_AFTER:
        logger.warning(
            "Server asked to retry after %.0fs (>%ds); honouring it, so this "
            "request will block until then.",
            seconds,
            MAX_RETRY_AFTER,
        )
    return seconds


class SmartAuthClient:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Fetches and caches a SMART on FHIR Bearer token.

    auth_type values:
    - "jwt_assertion": SMART Backend Services with RSA/EC private key (RFC 7523)
    - "client_secret": OAuth2 client credentials with symmetric secret
    - "none": No authentication (open servers, dev/test only)
    """

    def __init__(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        token_url: str,
        client_id: str,
        auth_type: str,
        private_key_pem: str = "",
        client_secret: str = "",
        scope: str = "",
        kid: str = "",
        private_key_algorithm: str = "RS384",
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._auth_type = auth_type
        self._private_key_pem = private_key_pem
        self._client_secret = client_secret
        self._scope = scope
        self._kid = kid
        self._private_key_algorithm = private_key_algorithm
        self._access_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

        # Fail at construction, not on the first HTTP call mid-pipeline.
        if auth_type not in _TOKEN_AUTH_TYPES | {"none"}:
            raise ValueError(
                f"Unsupported auth_type: {auth_type!r}. "
                f"Use 'jwt_assertion', 'client_secret', or 'none'."
            )
        if auth_type == "jwt_assertion":
            if private_key_algorithm not in _SUPPORTED_ALGORITHMS:
                raise ValueError(
                    f"private_key_algorithm {private_key_algorithm!r} "
                    f"is not supported. "
                    f"Use one of: {sorted(_SUPPORTED_ALGORITHMS)}."
                )

    def get_token(self) -> str:
        """Return a cached or freshly-fetched Bearer token."""
        if self._auth_type == "none":
            return ""
        if self._access_token and self._expires_at:
            if datetime.now(timezone.utc) < self._expires_at:
                return self._access_token
        self._refresh_token()
        return self._access_token

    def _refresh_token(self) -> None:
        if self._auth_type == "jwt_assertion":
            data = self._jwt_assertion_data()
            post_kwargs: dict = {}
        else:  # client_secret; "none" returns before reaching here
            data = self._client_secret_data()
            post_kwargs = {"auth": (self._client_id, self._client_secret)}
        resp = requests.post(
            self._token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=TOKEN_TIMEOUT,
            **post_kwargs,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"SMART token request failed (HTTP {resp.status_code}): {resp.text}")
        body = resp.json()
        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 300))
        self._expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 30)

    def _jwt_assertion_data(self) -> dict:
        now = datetime.now(timezone.utc)
        payload = {
            "iss": self._client_id,
            "sub": self._client_id,
            "aud": self._token_url,
            "jti": str(uuid.uuid4()),
            "iat": now,
            "nbf": now,
            "exp": now + timedelta(minutes=5),
        }
        if not self._kid:
            raise ValueError(
                "kid is required for jwt_assertion auth. "
                "Provide the key ID (kid) that identifies the private key "
                "registered with the FHIR server's JWK Set."
            )
        jwt_headers = {"kid": self._kid, "typ": "JWT"}
        assertion = jwt.encode(
            payload,
            self._private_key_pem,
            algorithm=self._private_key_algorithm,
            headers=jwt_headers,
        )
        data = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": assertion,
        }
        if self._scope:
            data["scope"] = self._scope
        return data

    def _client_secret_data(self) -> dict:
        # client_id and client_secret are NOT included in the POST body.
        # Per SMART client-confidential-symmetric spec, they are passed via
        # HTTP Basic auth (Authorization: Basic B64(client_id:client_secret)).
        data = {"grant_type": "client_credentials"}
        if self._scope:
            data["scope"] = self._scope
        return data


def discover_token_url(base_url: str) -> str:
    """Fetch the OAuth2 token endpoint from the FHIR server's SMART configuration.

    Per SMART on FHIR Backend Services spec, servers publish their OAuth endpoints at:
    {base_url}/.well-known/smart-configuration

    Returns the token_endpoint URL.
    Raises RuntimeError if discovery fails or token_endpoint is absent.
    """
    discovery_url = base_url.rstrip("/") + "/.well-known/smart-configuration"
    resp = requests.get(
        discovery_url,
        headers={"Accept": "application/json"},
        timeout=TOKEN_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"SMART configuration discovery failed (HTTP {resp.status_code}) at {discovery_url}: "
            f"{resp.text}"
        )
    config = resp.json()
    token_endpoint = config.get("token_endpoint")
    if not token_endpoint:
        raise RuntimeError(
            f"SMART configuration at {discovery_url} does not contain 'token_endpoint'. "
            f"Keys present: {list(config.keys())}"
        )
    return token_endpoint


def build_auth_client(options: dict) -> SmartAuthClient:
    """Build the SMART auth client from connection options.

    Discovers the token endpoint from .well-known/smart-configuration when a
    token-based auth_type is configured without an explicit token_url.
    """
    auth_type = options.get("auth_type", "none")
    token_url = options.get("token_url", "").strip()
    if not token_url and auth_type in _TOKEN_AUTH_TYPES:
        token_url = discover_token_url(options["base_url"])
    return SmartAuthClient(
        token_url=token_url,
        client_id=options.get("client_id", ""),
        auth_type=auth_type,
        private_key_pem=options.get("private_key_pem", ""),
        client_secret=options.get("client_secret", ""),
        scope=options.get("scope", ""),
        kid=options.get("kid", ""),
        private_key_algorithm=options.get("private_key_algorithm", "RS384"),
    )


def auth_headers(auth_client: SmartAuthClient, accept: str = "application/fhir+json") -> dict:
    """Request headers with a Bearer token when the auth client has one."""
    headers = {"Accept": accept}
    token = auth_client.get_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


class FhirHttpClient:
    """HTTP client for FHIR R4 REST API with auth injection and retry logic.

    Retries on 429/500/503 with exponential backoff. Honors Retry-After on 429.
    Every request has a timeout (HTTP_TIMEOUT) to prevent hangs.
    """

    def __init__(self, base_url: str, auth_client: SmartAuthClient) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._auth_client = auth_client
        self._session = requests.Session()

    def get(self, resource_type: str, params: Optional[dict] = None) -> requests.Response:
        """Fetch a FHIR resource type with optional query parameters."""
        url = urljoin(self._base_url, resource_type)
        return self.get_url(url, params=params)

    def get_url(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """Fetch a URL with retry logic and auth injection."""
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(
                url,
                params=params,
                headers=auth_headers(self._auth_client),
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(retry_after(resp, backoff))
                backoff *= 2
        return resp


def _next_link(bundle: dict) -> Optional[str]:
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None


def iter_bundle_pages(
    client: FhirHttpClient,
    resource_type: str,
    params: Optional[dict] = None,
    page_delay: float = PAGE_DELAY,
) -> Iterator[dict]:
    """Yield FHIR resources from a paginated search, following Bundle next links."""
    for entry in iter_bundle_entries(client, resource_type, params, page_delay):
        resource = entry.get("resource")
        if resource:
            yield resource


def iter_bundle_entries(
    client: FhirHttpClient,
    path: str,
    params: Optional[dict] = None,
    page_delay: float = PAGE_DELAY,
) -> Iterator[dict]:
    """Yield Bundle entries from a paginated search or history, following next links.

    History entries matter beyond their resource: a DELETE entry has no
    resource at all, only `request.method` and `request.url`.
    """
    resp = client.get(path, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"FHIR search failed for {path} (HTTP {resp.status_code}): {resp.text}")

    while True:
        bundle = resp.json()
        if bundle.get("resourceType") != "Bundle":
            raise RuntimeError(f"Expected Bundle for {path}, got: {bundle.get('resourceType')}")
        yield from bundle.get("entry", [])

        next_url = _next_link(bundle)
        if not next_url:
            return

        if page_delay > 0:
            time.sleep(page_delay)
        resp = client.get_url(next_url)
        if resp.status_code != 200:
            raise RuntimeError(
                f"FHIR pagination failed for {path} (HTTP {resp.status_code}): {resp.text}"
            )


def extract_record(
    resource: dict,
    raw: Optional[str] = None,
    fhir_version: Optional[str] = None,
    ingested_at: Optional[str] = None,
) -> dict:
    """Map a FHIR resource to a row of RESOURCE_SCHEMA.

    `raw` is the resource's JSON text as the server sent it (bulk NDJSON). When
    given it becomes the VARIANT value unchanged, which keeps FHIR decimal
    precision exactly; otherwise the parsed resource is re-serialised, which
    rounds decimals to float64. Values are returned raw: the framework's
    parse_value() converts JSON text via VariantVal.parseJson() and ISO strings
    to timestamps.
    """
    return {
        "resource_type": resource.get("resourceType"),
        "resource_id": resource.get("id"),
        "last_updated": (resource.get("meta") or {}).get("lastUpdated"),
        "resource": raw if raw is not None else json.dumps(resource),
        "fhir_version": fhir_version,
        "ingested_at": ingested_at,
    }
