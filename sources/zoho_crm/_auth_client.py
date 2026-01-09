import re
import requests
import time
from datetime import datetime, timedelta
from typing import Optional


class AuthClient:
    """
    Handles OAuth 2.0 authentication and API requests for Zoho CRM.

    This class manages the OAuth token lifecycle, including:
    - Storing and refreshing access tokens before expiry
    - Deriving the correct API endpoint based on regional OAuth URL
    - Making authenticated HTTP requests with retry logic
    - Handling rate limiting (HTTP 429) with exponential backoff

    Attributes:
        client_id: OAuth Client ID from Zoho API Console.
        client_secret: OAuth Client Secret from Zoho API Console.
        refresh_token: Long-lived refresh token for obtaining access tokens.
        accounts_url: Zoho accounts URL for OAuth (region-specific).
        api_url: Zoho CRM API URL (derived from accounts_url).
        access_token: Current valid access token (or None if not yet obtained).
        token_expires_at: Expiration datetime of current access token.

    Note:
        The class maintains a requests.Session for connection pooling,
        which improves performance for multiple sequential API calls.
    """

    # Token refresh buffer - refresh 5 minutes before actual expiry
    TOKEN_REFRESH_BUFFER_MINUTES = 5

    # Maximum retry attempts for transient errors
    MAX_RETRIES = 3

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the authentication client with OAuth credentials.

        Args:
            options: Dictionary containing connection configuration:
                - client_id (required): OAuth Client ID from Zoho API Console.
                - client_value_tmp (required): OAuth Client Secret.
                - refresh_value_tmp (required): Long-lived refresh token.
                - base_url (optional): Zoho accounts URL for OAuth.
                  Defaults to "https://accounts.zoho.com" (US region).
                  Other regions:
                    - EU: https://accounts.zoho.eu
                    - IN: https://accounts.zoho.in
                    - AU: https://accounts.zoho.com.au
                    - CN: https://accounts.zoho.com.cn

        Note:
            The API URL is automatically derived from the accounts URL.
            For example, https://accounts.zoho.eu -> https://www.zohoapis.eu
        """
        # Extract OAuth credentials from options
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_value_tmp")
        self.refresh_token = options.get("refresh_value_tmp")

        # Determine the Zoho Accounts URL (for OAuth) and derive the API URL
        self.accounts_url = options.get("base_url", "https://accounts.zoho.com").rstrip("/")

        # Extract domain suffix (e.g., "eu", "in", "com.au") from accounts URL
        # Pattern: accounts.zoho.{suffix} -> www.zohoapis.{suffix}
        match = re.search(r"accounts\.zoho\.(.+)$", self.accounts_url)
        domain_suffix = match.group(1) if match else "com"
        self.api_url = f"https://www.zohoapis.{domain_suffix}"

        # Token management - initially no token is available
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

        # Use a persistent session for connection pooling and keep-alive
        self._session = requests.Session()

    def get_access_token(self) -> str:
        """
        Retrieve a valid OAuth access token, refreshing if necessary.

        This method implements lazy token refresh - it only requests a new
        token when the current one is expired or about to expire (within
        5 minutes of expiry).

        Returns:
            A valid access token string.

        Raises:
            Exception: If token refresh fails or response is invalid.

        Note:
            Access tokens typically expire after 1 hour (3600 seconds).
            This method automatically handles the refresh before expiry.
        """
        # Check if current token is still valid (with buffer time)
        if self.access_token and self.token_expires_at:
            buffer = timedelta(minutes=self.TOKEN_REFRESH_BUFFER_MINUTES)
            if datetime.now() < self.token_expires_at - buffer:
                return self.access_token

        # Token is expired or doesn't exist - refresh it
        token_refresh_url = f"{self.accounts_url}/oauth/v2/token"
        refresh_payload = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }

        response = requests.post(token_refresh_url, data=refresh_payload)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error_detail = response.text
            raise Exception(f"Failed to refresh access token: {e}. Response: {error_detail}")

        token_data = response.json()

        # Validate response contains access_token
        if "access_token" not in token_data:
            raise Exception(
                f"Token refresh response missing 'access_token'. "
                f"Response: {token_data}. "
                f"Please check your client_id, client_value_tmp, and refresh_value_tmp are valid."
            )

        # Store the new token and calculate expiration time
        self.access_token = token_data["access_token"]
        expires_in_seconds = token_data.get("expires_in", 3600)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in_seconds)

        return self.access_token

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
    ) -> dict:
        """
        Make an authenticated API request to Zoho CRM.

        This method handles:
        - Automatic token refresh if needed
        - Rate limiting with exponential backoff
        - Token re-acquisition on 401 errors
        - Empty response handling

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            endpoint: API endpoint path (e.g., "/crm/v8/Leads").
            params: Optional query parameters as a dictionary.
            data: Optional request body for POST/PUT requests.

        Returns:
            Parsed JSON response as a dictionary.
            Returns empty dict for empty responses.

        Raises:
            ValueError: If an unsupported HTTP method is specified.
            Exception: If request fails after max retries or rate limit exceeded.
            requests.exceptions.HTTPError: For non-retryable HTTP errors.

        Example:
            >>> # GET request with query parameters
            >>> response = client.make_request(
            ...     "GET",
            ...     "/crm/v8/Leads",
            ...     params={"page": 1, "per_page": 200}
            ... )

            >>> # POST request with JSON body
            >>> response = client.make_request(
            ...     "POST",
            ...     "/crm/v8/Leads",
            ...     data={"data": [{"Last_Name": "Smith"}]}
            ... )
        """
        access_token = self.get_access_token()

        # Construct full URL and authorization header
        request_url = f"{self.api_url}{endpoint}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
        }

        if data:
            headers["Content-Type"] = "application/json"

        # Retry loop with exponential backoff for transient errors
        for attempt in range(self.MAX_RETRIES):
            try:
                # Execute the appropriate HTTP method
                if method.upper() == "GET":
                    response = self._session.get(request_url, headers=headers, params=params)
                elif method.upper() == "POST":
                    response = self._session.post(request_url, headers=headers, json=data, params=params)
                elif method.upper() == "PUT":
                    response = self._session.put(request_url, headers=headers, json=data, params=params)
                elif method.upper() == "DELETE":
                    response = self._session.delete(request_url, headers=headers, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                # Handle rate limiting (HTTP 429) with exponential backoff
                if response.status_code == 429:
                    if attempt < self.MAX_RETRIES - 1:
                        wait_time = 2 ** attempt  # 1s, 2s, 4s
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception("Rate limit exceeded after multiple retries. Please wait and try again later.")

                response.raise_for_status()

                # Handle empty responses (some Zoho endpoints return no body)
                if not response.text or response.text.strip() == "":
                    return {}

                return response.json()

            except requests.exceptions.HTTPError as e:
                # On 401 Unauthorized, try refreshing the token once
                if e.response.status_code == 401 and attempt == 0:
                    self.access_token = None  # Force token refresh
                    access_token = self.get_access_token()
                    headers["Authorization"] = f"Zoho-oauthtoken {access_token}"
                    continue
                raise

        raise Exception(f"Failed to make API request after {self.MAX_RETRIES} attempts")
