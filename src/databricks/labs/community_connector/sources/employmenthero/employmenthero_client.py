"""
Employment Hero API Client.

Handles authentication, HTTP requests, and pagination for the Employment Hero API.
This module separates API concerns from business logic.
"""

import re
import time
from datetime import datetime, timedelta
from typing import Iterator, Optional

import requests


class EmploymentHeroAPIClient:  # pylint: disable=too-many-instance-attributes
    """
    HTTP client for Employment Hero API with OAuth2 authentication.

    Handles:
    - OAuth2 token refresh
    - Rate limiting with exponential backoff
    - Paginated API responses
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        authorization_code: str,
        refresh_token: Optional[str] = None,
        base_url: str = "https://api.employmenthero.com",
        oauth_url: str = "https://oauth.employmenthero.com",
        session: Optional[requests.Session] = None,
    ) -> None:
        """
        Initialize the API client.

        Args:
            client_id: OAuth Client ID from Employment Hero Developer Portal.
            client_secret: OAuth Client Secret from Employment Hero Developer Portal.
            redirect_uri: Redirect URI registered with the OAuth application. Required for authorization code exchange.
            authorization_code: Authorization code from the OAuth callback, used for the initial token exchange.
            refresh_token: Optional OAuth refresh token. If not provided, the client will use the authorization code grant; otherwise, it will use the refresh token grant.
            base_url: Base URL for the Employment Hero API. Defaults to "https://api.employmenthero.com".
            oauth_url: Base URL for the Employment Hero OAuth2 endpoints. Defaults to "https://oauth.employmenthero.com".
            session: Optional requests.Session or compatible HTTP session for making requests. Used for connection pooling and easier testing.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.authorization_code = authorization_code
        self.refresh_token = refresh_token
        self.base_url = base_url
        self.oauth_url = oauth_url

        # Token management
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None

        # HTTP session for connection pooling
        self._session = session or requests.Session()

    def _get_access_token(self) -> str:
        """
        Return a valid access token, obtaining or refreshing via the OAuth token endpoint.
        Access tokens expire after 15 minutes (900 seconds). Uses authorization_code
        when no refresh_token is available, otherwise refresh_token grant.
        """
        if self._access_token and self._token_expires_at:
            if datetime.now() < self._token_expires_at - timedelta(minutes=5):
                return self._access_token

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.refresh_token:
            data["grant_type"] = "refresh_token"
            data["refresh_token"] = self.refresh_token
        else:
            data["grant_type"] = "authorization_code"
            data["code"] = self.authorization_code
            data["redirect_uri"] = self.redirect_uri

        response = self._session.post(f"{self.oauth_url.rstrip('/')}/oauth2/token", data=data, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        if "access_token" not in token_data:
            raise ValueError("Token response missing access_token")

        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 900)
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        if "refresh_token" in token_data:
            self.refresh_token = token_data["refresh_token"]
        return self._access_token

    def _make_http_request(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        method: str,
        url: str,
        headers: dict,
        params: Optional[dict],
        data: Optional[dict],
    ) -> requests.Response:
        """
        Execute the HTTP request using the session.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            url: Full URL to request
            headers: Request headers including Authorization
            params: Query parameters
            data: Request body for POST/PUT

        Returns:
            requests.Response object
        """
        method = method.upper()
        if method == "GET":
            return self._session.get(url, headers=headers, params=params, timeout=30)
        if method == "POST":
            return self._session.post(url, headers=headers, json=data, params=params, timeout=30)
        if method == "PUT":
            return self._session.put(url, headers=headers, json=data, params=params, timeout=30)
        if method == "DELETE":
            return self._session.delete(url, headers=headers, params=params, timeout=30)
        raise ValueError(f"Unsupported HTTP method: {method}")

    def request(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        max_retries: int = 3,
    ) -> dict:
        """
        Make an authenticated API request to Employment Hero.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path (e.g., "/api/v1/organisations/my-org-id/employees")
            params: Query parameters
            data: Request body for POST/PUT
            max_retries: Maximum retry attempts for rate limiting

        Returns:
            Parsed JSON response as dictionary
        """
        access_token = self._get_access_token()
        url = f"{self.base_url.rstrip('/')}{endpoint}"
        headers = {"Authorization": f"Bearer {access_token}"}

        for attempt in range(max_retries + 1):
            response = self._make_http_request(method, url, headers, params, data)
            if response.status_code >= 429 and attempt < max_retries:
                time.sleep(2**attempt)
                continue
            response.raise_for_status()
            if not response.text or not response.text.strip():
                return {}
            return response.json()

    def paginate(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        data_key: str = "data",
        per_page: int = 200,
    ) -> Iterator[dict]:
        """
        Iterate through paginated API responses.

        Uses Employment Hero pagination: page_index (1-based) and item_per_page.
        Calls request() for each page and yields individual records from response[data_key]["items"].

        Args:
            endpoint: API endpoint path
            params: Base query parameters (page_index and item_per_page will be added)
            data_key: Key in response containing the page data (default "data")
            per_page: Number of records per page (sent as item_per_page; max 200 for Employment Hero)

        Yields:
            Individual records from each page
        """
        base_params = dict(params) if params else {}
        page_index = 1

        while True:
            request_params = {**base_params, "page_index": page_index, "item_per_page": per_page}
            response = self.request("GET", endpoint, params=request_params)
            page_data = response.get(data_key, {})
            items = page_data.get("items", [])
            total_pages = page_data.get("total_pages", 1)

            yield from items

            if page_index >= total_pages or not items:
                break
            page_index += 1
