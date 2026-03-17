"""HTTP client, authentication, bundle pagination, and record extraction for FHIR R4.

Key design decisions (from fhir_api_doc.md):
- SmartAuthClient handles jwt_assertion, client_secret, and none auth types.
- Token is cached and refreshed 30s before expiry (tokens are short-lived: ~300s).
- iter_bundle_pages follows Bundle.link[relation=next] per FHIR R4 spec.
- extract_record returns raw field values (no type conversion) -- framework handles coercion.
- Absent struct fields return None, not {}.
- All HTTP calls have timeout=60 to prevent hangs on slow FHIR servers.
"""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional
from urllib.parse import urljoin

import jwt
import requests

from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    HTTP_TIMEOUT, INITIAL_BACKOFF, MAX_RETRIES, PAGE_DELAY, RETRIABLE_STATUS_CODES, TOKEN_TIMEOUT,
)
from databricks.labs.community_connector.sources.fhir.fhir_profile_registry import extract


class SmartAuthClient:
    """Fetches and caches a SMART on FHIR Bearer token.

    auth_type values:
    - "jwt_assertion": SMART Backend Services with RSA/EC private key (RFC 7523)
    - "client_secret": OAuth2 client credentials with symmetric secret
    - "none": No authentication (open servers, dev/test only)
    """

    def __init__(self, token_url: str, client_id: str, auth_type: str,
                 private_key_pem: str = "", client_secret: str = "", scope: str = "",
                 kid: str = "", private_key_algorithm: str = "RS384") -> None:
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

        if auth_type == "jwt_assertion":
            _SUPPORTED_ALGORITHMS = {"RS384", "ES384"}
            if private_key_algorithm not in _SUPPORTED_ALGORITHMS:
                raise ValueError(
                    f"private_key_algorithm {private_key_algorithm!r} is not supported. "
                    f"Use one of: {sorted(_SUPPORTED_ALGORITHMS)}. "
                    f"Per the SMART on FHIR Backend Services spec, clients SHALL support RS384 and ES384."
                )

    def get_token(self) -> str:
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
        elif self._auth_type == "client_secret":
            data = self._client_secret_data()
            post_kwargs = {"auth": (self._client_id, self._client_secret)}
        else:
            raise ValueError(f"Unsupported auth_type: {self._auth_type!r}. "
                             f"Use 'jwt_assertion', 'client_secret', or 'none'.")
        resp = requests.post(
            self._token_url, data=data,
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
            "iss": self._client_id, "sub": self._client_id, "aud": self._token_url,
            "jti": str(uuid.uuid4()), "iat": now, "nbf": now,
            "exp": now + timedelta(minutes=5),
        }
        if not self._kid:
            raise ValueError(
                "kid is required for jwt_assertion auth. "
                "Provide the key ID (kid) that identifies the private key "
                "registered with the FHIR server's JWK Set."
            )
        jwt_headers = {"kid": self._kid, "typ": "JWT"}
        assertion = jwt.encode(payload, self._private_key_pem, algorithm=self._private_key_algorithm, headers=jwt_headers)
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


class FhirHttpClient:
    """HTTP client for FHIR R4 REST API with auth injection and retry logic.

    Retries on 429/500/503 with exponential backoff. Honors Retry-After on 429.
    All requests have timeout=60 to prevent hangs.
    """

    def __init__(self, base_url: str, auth_client: SmartAuthClient) -> None:
        self._base_url = base_url.rstrip("/") + "/"
        self._auth_client = auth_client
        self._session = requests.Session()

    def _headers(self) -> dict:
        token = self._auth_client.get_token()
        h = {"Accept": "application/fhir+json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def get(self, resource_type: str, params: Optional[dict] = None) -> requests.Response:
        url = urljoin(self._base_url, resource_type)
        return self.get_url(url, params=params)

    def get_url(self, url: str, params: Optional[dict] = None) -> requests.Response:
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(url, params=params, headers=self._headers(), timeout=HTTP_TIMEOUT)
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_seconds = float(retry_after)
                    except ValueError:
                        sleep_seconds = backoff  # Retry-After is an HTTP-date; fall back to backoff
                else:
                    sleep_seconds = backoff
                time.sleep(sleep_seconds)
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
    max_records: Optional[int] = None,
    page_delay: float = PAGE_DELAY,
) -> Iterator[dict]:
    """Yield FHIR resources from a paginated search, following Bundle next links."""
    count = 0
    resp = client.get(resource_type, params=params)
    if resp.status_code != 200:
        raise RuntimeError(
            f"FHIR search failed for {resource_type} (HTTP {resp.status_code}): {resp.text}"
        )

    while True:
        bundle = resp.json()
        if bundle.get("resourceType") != "Bundle":
            raise RuntimeError(
                f"Expected Bundle for {resource_type}, got: {bundle.get('resourceType')}"
            )
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                yield resource
                count += 1
                if max_records is not None and count >= max_records:
                    return

        next_url = _next_link(bundle)
        if not next_url:
            return

        if page_delay > 0:
            time.sleep(page_delay)
        resp = client.get_url(next_url)
        if resp.status_code != 200:
            raise RuntimeError(
                f"FHIR pagination failed for {resource_type} (HTTP {resp.status_code}): {resp.text}"
            )


def _safe(d, *keys, default=None):
    """Safely navigate nested dicts. Returns default if any key is missing."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d


def extract_record(resource: dict, resource_type: str, profile: str = "uk_core") -> dict:
    """Map a raw FHIR resource to hybrid schema format (typed fields + raw_json).

    Delegates to the profile registry for resource-specific extraction.
    Returns raw field values — do NOT pre-convert types; framework handles coercion.
    profile defaults to 'uk_core' (falls back to base_r4 for resources without UK Core override).
    """
    meta = resource.get("meta") or {}
    record = {
        "id": resource.get("id"),
        "resourceType": resource.get("resourceType", resource_type),
        "lastUpdated": meta.get("lastUpdated"),
        # raw_json and extension are stored as StringType (JSON strings).
        # Downstream queries on DBR 15.3+ can use parse_json() to get VARIANT.
        "raw_json": json.dumps(resource),
        "extension": json.dumps(resource.get("extension")),
    }
    record.update(extract(resource, resource_type, profile))
    return record
