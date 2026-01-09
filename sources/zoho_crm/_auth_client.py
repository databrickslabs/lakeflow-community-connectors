import re
import requests
import time
from datetime import datetime, timedelta
from typing import Optional


class AuthClient:
    """
    Handles OAuth authentication and token management for Zoho CRM API interactions.
    This includes obtaining and refreshing access tokens, and making authenticated API requests.
    """

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initializes the AuthClient with OAuth credentials and base URLs.

        Args:
            options: A dictionary containing connection-level options, including:
                     - client_id: OAuth Client ID from Zoho API Console.
                     - client_secret: OAuth Client Secret from Zoho API Console.
                     - refresh_token: Long-lived refresh token obtained from OAuth flow.
                     - base_url (optional): Zoho accounts URL for OAuth. Defaults to https://accounts.zoho.com.
        """
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_value_tmp")
        self.refresh_token = options.get("refresh_value_tmp")

        # Determine Zoho Accounts URL for OAuth and derive the Zoho CRM API URL.
        self.accounts_url = options.get("base_url", "https://accounts.zoho.com").rstrip("/")
        # The API URL is derived from the accounts URL's domain suffix (e.g., .eu, .com).
        match = re.search(r"accounts\.zoho\.(.+)$", self.accounts_url)
        domain_suffix = match.group(1) if match else "com"
        self.api_url = f"https://www.zohoapis.{domain_suffix}"

        # Initialize attributes for managing OAuth access tokens and their expiration.
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

        # Configure a session for making persistent API requests to Zoho CRM.
        self._session = requests.Session()

    def get_access_token(self) -> str:
        """
        Retrieves a valid OAuth access token, refreshing it if necessary.
        Access tokens typically expire after 1 hour (3600 seconds).
        """
        # Return current token if it's still valid (with a 5-minute buffer).
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at - timedelta(minutes=5):
            return self.access_token

        # Construct the token refresh URL and payload.
        token_refresh_url = f"{self.accounts_url}/oauth/v2/token"
        refresh_payload = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }

        # Send the refresh token request.
        response = requests.post(token_refresh_url, data=refresh_payload)

        try:
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx).
        except requests.exceptions.HTTPError as e:
            error_detail = response.text
            raise Exception(f"Failed to refresh access token: {e}. Response: {error_detail}")

        token_data = response.json()

        # Validate the presence of 'access_token' in the response.
        if "access_token" not in token_data:
            raise Exception(
                f"Token refresh response missing 'access_token'. "
                f"Response: {token_data}. "
                f"Requested with data: {refresh_payload}. "
                f"Please check your client_id, client_secret, and refresh_token are valid."
            )

        # Update access token and expiration time.
        self.access_token = token_data["access_token"]
        expires_in_seconds = token_data.get("expires_in", 3600)  # Default to 3600 seconds (1 hour).
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
        Makes an authenticated API request to Zoho CRM, handling token refresh and rate limiting.
        Includes retry logic for transient errors like rate limits or expired tokens.
        """
        access_token = self.get_access_token()

        # Construct the full API URL and headers, including authorization.
        request_url = f"{self.api_url}{endpoint}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
        }

        if data:
            headers["Content-Type"] = "application/json"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Execute the HTTP request based on the specified method.
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

                # Handle API rate limiting (HTTP 429 Too Many Requests) with exponential backoff.
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)  # Wait for 2^attempt seconds.
                        continue
                    else:
                        raise Exception("Rate limit exceeded after multiple retries. Please wait and try again later.")

                response.raise_for_status()  # Raise HTTPError for other bad responses (e.g., 400, 500).

                # Return an empty dictionary for empty responses to prevent JSON parsing errors.
                if not response.text or response.text.strip() == "":
                    return {}

                return response.json()

            except requests.exceptions.HTTPError as e:
                # If 401 Unauthorized, invalidate token, refresh, and re-attempt the request once.
                if e.response.status_code == 401 and attempt == 0:
                    self.access_token = None  # Clear the expired token.
                    access_token = self.get_access_token()  # Fetch a new access token.
                    headers["Authorization"] = f"Zoho-oauthtoken {access_token}"  # Update authorization header.
                    continue
                raise  # Re-raise all other HTTP errors immediately.

        raise Exception(f"Failed to make API request after {max_retries} attempts")
