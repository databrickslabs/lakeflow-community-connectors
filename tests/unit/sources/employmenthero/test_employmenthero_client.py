# Fixtures (employees_data, _patch_time_sleep) and constants (CONFIGS_DIR, DATA_PATH)
# are defined in conftest.py and are available to all tests in this directory.

import json
import re
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import urlencode
from typing import Optional

import pytest
import requests

from databricks.labs.community_connector.sources.employmenthero.employmenthero_client import EmploymentHeroAPIClient


def _merged_post_data(post_call: tuple[str, dict]) -> dict:
    """Extract merged params/data/json from a mock session _post_calls entry."""
    _, kwargs = post_call
    out = dict(kwargs.get("params") or {})
    for key in ("data", "json"):
        if isinstance(kwargs.get(key), dict):
            out.update(kwargs[key])
    return out


# =============================================================================
# Mocks
# =============================================================================

class MockEmploymentHeroRequestsSession(MagicMock, requests.Session):
    """Mock requests session for the Employment Hero client.

    Recognises Employment Hero API URLs and returns relevant mock responses:
    - OAuth token (POST oauth.employmenthero.com/oauth2/token): token response
    - Get Employees (GET api.employmenthero.com/.../organisations/.../employees): paginated employees

    Returns 401 if required params or Authorization header are missing.
    Returns 404 if the URL does not match a known endpoint.
    Returns 429 if rate_limit_retries is set > 0.

    Records all get/post calls in _get_calls and _post_calls for assertion.
    """
    _oauth_token_url_pattern = re.compile(
        r"^https?://oauth\.employmenthero\.com/oauth2/token",
        re.IGNORECASE,
    )
    _employees_url_pattern = re.compile(
        r"^https?://api\.employmenthero\.com/api/v1/organisations/[^/]+/employees(?:\?|$)",
        re.IGNORECASE,
    )

    def __init__(self, employees_data: Optional[dict] = None, rate_limit_retries: int = 0, **kwargs) -> None:
        super().__init__(**kwargs)
        self._employees_data = employees_data or {}
        self._rate_limit_retries = rate_limit_retries
        self._get_calls: list[tuple[str, dict]] = []
        self._post_calls: list[tuple[str, dict]] = []

    def _make_json_response(self, status_code: int, data: dict) -> requests.Response:
        """Build a requests.Response with JSON body."""
        resp = requests.Response()
        resp.status_code = status_code
        resp.headers["Content-Type"] = "application/json"
        resp._content = json.dumps(data).encode("utf-8")
        return resp

    def _make_error_response(self, status_code: int, message: str) -> requests.Response:
        """Build a requests.Response for API errors (401, 404)."""
        return self._make_json_response(
            status_code,
            {"code": str(status_code), "error": {"message": message}, "errors": []},
        )

    def _make_redirect_response(self, location: str, status_code: int = 302) -> requests.Response:
        """Build a requests.Response for redirects (e.g. OAuth authorize)."""
        resp = requests.Response()
        resp.status_code = status_code
        resp.headers["Location"] = location
        resp._content = b""
        return resp

    def get(self, url: str, **kwargs) -> requests.Response:
        """Mock GET request. Handles authorize URL (redirect) and employees payload."""
        self._get_calls.append((url, kwargs))
        if self._rate_limit_retries > 0:
            self._rate_limit_retries -= 1
            return self._make_error_response(429, "Too Many Requests")

        if not self._employees_url_pattern.search(url):
            return self._make_error_response(404, "The specified resource could not be found.")

        headers = kwargs.get("headers") or {}
        auth = headers.get("Authorization") or ""
        if not (auth.startswith("Bearer ") and len(auth.strip()) > 7):
            return self._make_error_response(
                401, "Invalid or missing authentication token."
            )

        data = self._employees_data
        params = kwargs.get("params") or {}
        page_index = int(params.get("page_index", 1))
        item_per_page = int(params.get("item_per_page", data.get("item_per_page", 20)))
        pages = data.get("pages", [])
        page = next((p for p in pages if p.get("page_index") == page_index), None)
        if page is not None:
            items = page["items"]
            total_items = page["total_items"]
            total_pages = page["total_pages"]
        else:
            items = []
            total_items = data.get("total_items", 0)
            total_pages = data.get("total_pages", 1)
        payload = {
            "data": {
                "items": items,
                "page_index": page_index,
                "item_per_page": item_per_page,
                "total_items": total_items,
                "total_pages": total_pages,
            }
        }
        return self._make_json_response(200, payload)

    def post(self, url: str, **kwargs) -> requests.Response:
        """Mock POST request. Returns token payload for OAuth token URL."""
        self._post_calls.append((url, kwargs))
        if not self._oauth_token_url_pattern.search(url):
            return self._make_error_response(404, "The specified resource could not be found.")

        # Params may be query string, form body, or JSON body
        params = dict(kwargs.get("params") or {})
        for key in ("data", "json"):
            val = kwargs.get(key)
            if isinstance(val, dict):
                params.update(val)

        client_id = params.get("client_id")
        client_secret = params.get("client_secret")
        grant_type = params.get("grant_type")

        if not client_id or not client_secret or not grant_type:
            return self._make_error_response(
                401, "Invalid or missing authentication token."
            )

        if grant_type == "authorization_code":
            if not params.get("code") or not params.get("redirect_uri"):
                return self._make_error_response(
                    401, "Invalid or missing authentication token."
                )
        elif grant_type == "refresh_token":
            if not params.get("refresh_token"):
                return self._make_error_response(
                    401, "Invalid or missing authentication token."
                )
        else:
            return self._make_error_response(
                401, "Invalid or missing authentication token."
            )

        payload = {
            "access_token": "mock_access_token_abc123",
            "refresh_token": "mock_refresh_token_xyz789",
            "token_type": "bearer",
            "expires_in": 0,
            "scope": "urn:mainapp:organisations:read urn:mainapp:employees:read",
        }
        return self._make_json_response(200, payload)


# =============================================================================
# Offline Unit Tests
# =============================================================================

def test_employmenthero_client_authenticates_on_request(employees_data: dict):
    """Test that the EmploymentHeroAPIClient performs OAuth 2.0 flow then returns first page of employees.

    No refresh token is provided initially. The client is expected to obtain an access token
    via grant_type=authorization_code (exchanging the authorization code from the OAuth callback),
    which returns both access_token and refresh_token per Employment Hero API.
    """
    mock_session = MockEmploymentHeroRequestsSession(employees_data=employees_data)
    redirect_uri = "https://example.databricks.com/oauth/callback"
    client = EmploymentHeroAPIClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri=redirect_uri,
        authorization_code="test_authorization_code",
        session=mock_session,
    )

    # ---- Run test ----
    response = client.request(
        "GET",
        "/api/v1/organisations/test-org-id/employees",
        params={"page_index": 1, "item_per_page": 2},
    )

    # ---- Check OAuth flow: token via authorization_code, then authenticated GET ----
    assert len(mock_session._post_calls) >= 1, "Expected POST to OAuth token endpoint"
    post_url, post_kwargs = mock_session._post_calls[0]
    assert "oauth.employmenthero.com" in post_url and "/oauth2/token" in post_url
    post_data = dict(post_kwargs.get("params") or {})
    for key in ("data", "json"):
        if isinstance(post_kwargs.get(key), dict):
            post_data.update(post_kwargs[key])
    assert post_data.get("client_id") == "test_client_id"
    assert post_data.get("client_secret") == "test_client_secret"
    assert post_data.get("grant_type") == "authorization_code"
    assert post_data.get("code") == "test_authorization_code"
    assert post_data.get("redirect_uri") == redirect_uri

    assert len(mock_session._get_calls) >= 1, "Expected GET to employees endpoint"
    get_url, get_kwargs = mock_session._get_calls[0]
    assert "api.employmenthero.com" in get_url and "/employees" in get_url
    assert get_kwargs.get("headers", {}).get("Authorization") == "Bearer mock_access_token_abc123"
    assert get_kwargs.get("params", {}).get("page_index") == 1
    assert get_kwargs.get("params", {}).get("item_per_page") == 2

    # ---- Check response is the exact first page from the JSON fixture ----
    assert response is not None, "request() must return the API response dict"
    assert "data" in response
    expected_first_page = employees_data["pages"][0]
    assert response["data"] == expected_first_page


def test_employmenthero_client_retries_on_retriable_error(employees_data: dict):
    """Test that request() retries on 429 (Too Many Requests) and eventually succeeds.

    The mock is configured with rate_limit_retries=3, so the first 3 GETs return 429
    and the 4th succeeds. We assert that the client retried 3 times (4 GET calls total)
    and returned the successful response.
    """
    mock_session = MockEmploymentHeroRequestsSession(
        employees_data=employees_data,
        rate_limit_retries=3,
    )
    redirect_uri = "https://example.databricks.com/oauth/callback"
    client = EmploymentHeroAPIClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri=redirect_uri,
        authorization_code="test_authorization_code",
        session=mock_session,
    )

    # ---- Run test ----
    response = client.request(
        "GET",
        "/api/v1/organisations/test-org-id/employees",
        params={"page_index": 1, "item_per_page": 2},
        max_retries=3,
    )

    # Client should have retried 3 times after 429s: 4 GETs total (3 rate-limited + 1 success)
    assert len(mock_session._get_calls) == 4, (
        "Expected 4 GET calls (3 retries after 429 then success)"
    )
    # Response should be the first page of data
    assert response is not None
    assert "data" in response
    expected_first_page = employees_data["pages"][0]
    assert response["data"] == expected_first_page


def test_employmenthero_client_fails_after_exceeding_max_retries(employees_data: dict):
    """Test that request() raises 429 after exhausting max_retries.

    Mock returns 429 for the first 4 GETs (rate_limit_retries=4). Client uses max_retries=3,
    so it makes 4 attempts (initial + 3 retries), all get 429, then raises HTTPError.
    """
    mock_session = MockEmploymentHeroRequestsSession(
        employees_data=employees_data,
        rate_limit_retries=4,
    )
    redirect_uri = "https://example.databricks.com/oauth/callback"
    client = EmploymentHeroAPIClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri=redirect_uri,
        authorization_code="test_authorization_code",
        session=mock_session,
    )

    # ---- Run test ----
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        client.request(
            "GET",
            "/api/v1/organisations/test-org-id/employees",
            params={"page_index": 1, "item_per_page": 2},
            max_retries=3,
        )

    # Client should have retried 3 times after initial 429 error: 4 GETs total
    assert exc_info.value.response.status_code == 429
    assert len(mock_session._get_calls) == 4, (
        "Expected 4 GET calls before giving up (initial + 3 retries)"
    )


def test_employmenthero_client_paginates_and_refreshes_token(employees_data: dict):
    """Test that EmploymentHeroAPIClient.paginate() fetches all pages and refreshes the
    access token when it expires.

    The mock returns token responses with expires_in=0, so the first token is immediately
    expired. When the client requests the second page, it must request a new access token
    using the refresh_token grant (since the client received a refresh_token from the
    first token response).
    """
    mock_session = MockEmploymentHeroRequestsSession(employees_data=employees_data)
    redirect_uri = "https://example.databricks.com/oauth/callback"
    client = EmploymentHeroAPIClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri=redirect_uri,
        authorization_code="test_authorization_code",
        session=mock_session,
    )

    # ---- Run test ----
    records = list(
        client.paginate(
            "/api/v1/organisations/test-org-id/employees",
            params={},
            data_key="data",
            per_page=2,
        )
    )

    # All 3 employees across 2 pages
    assert len(records) == 3

    # Two token requests: first authorization_code, second refresh_token (token expired after first use)
    assert len(mock_session._post_calls) == 2, "Expected two token requests (initial + refresh)"

    first_post_data = _merged_post_data(mock_session._post_calls[0])
    assert first_post_data.get("grant_type") == "authorization_code"
    assert first_post_data.get("code") == "test_authorization_code"
    assert first_post_data.get("redirect_uri") == redirect_uri

    second_post_data = _merged_post_data(mock_session._post_calls[1])
    assert second_post_data.get("grant_type") == "refresh_token"
    assert second_post_data.get("refresh_token") == "mock_refresh_token_xyz789"

    # Two GETs: one per page
    assert len(mock_session._get_calls) == 2
    assert mock_session._get_calls[0][1].get("params", {}).get("page_index") == 1
    assert mock_session._get_calls[1][1].get("params", {}).get("page_index") == 2
