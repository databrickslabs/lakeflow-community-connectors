# Gmail API Client Utilities
# Handles OAuth authentication, HTTP requests, batch operations, and parallel fetching.

import json
import time
from typing import Dict, List, Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


# Batch API settings
BATCH_SIZE = 50  # Gmail batch API supports up to 100, using 50 for safety
MAX_WORKERS = 5  # Concurrent workers for parallel fetching


class GmailApiClient:
    """Handles Gmail API communication: auth, requests, batch, and parallel fetching."""

    BASE_URL = "https://gmail.googleapis.com/gmail/v1"
    BATCH_URL = "https://gmail.googleapis.com/batch/gmail/v1"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        user_id: str = "me",
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.user_id = user_id

        self._access_token = None
        self._token_expires_at = 0
        self._session = requests.Session()

    def get_access_token(self) -> str:
        """Exchange refresh token for access token with caching."""
        # Return cached token if still valid (with 60s buffer)
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token

        response = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        response.raise_for_status()
        data = response.json()

        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 3600)

        return self._access_token

    def get_headers(self) -> Dict[str, str]:
        """Get headers with valid access token."""
        return {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Accept": "application/json",
        }

    def make_request(
        self, method: str, endpoint: str, params: Optional[Dict] = None, retry_count: int = 3
    ) -> Optional[Dict]:
        """Make API request with retry and rate limit handling."""
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(retry_count):
            response = self._session.request(
                method, url, headers=self.get_headers(), params=params
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - exponential backoff
                wait_time = (2**attempt) + 1
                time.sleep(wait_time)
                continue
            elif response.status_code == 404:
                # History ID expired or resource not found
                return None
            elif response.status_code == 403:
                # Forbidden - missing OAuth scope or permission
                return None
            else:
                response.raise_for_status()

        raise Exception(f"Failed after {retry_count} retries")

    def make_batch_request(
        self, endpoints: List[str], params_list: Optional[List[Dict]] = None
    ) -> List[Dict]:
        """
        Make batch API request for efficient bulk data retrieval.

        Gmail batch API allows up to 100 requests in a single HTTP call,
        reducing network overhead significantly.
        """
        if not endpoints:
            return []

        if params_list is None:
            params_list = [{}] * len(endpoints)

        # Build multipart batch request body
        boundary = "batch_gmail_connector"
        body_parts: List[str] = []

        for i, (endpoint, params) in enumerate(zip(endpoints, params_list)):
            url = f"{self.BASE_URL}{endpoint}"
            if params:
                query_string = "&".join(f"{k}={v}" for k, v in params.items())
                url = f"{url}?{query_string}"

            part = f"--{boundary}\r\n"
            part += "Content-Type: application/http\r\n"
            part += f"Content-ID: <item{i}>\r\n\r\n"
            part += f"GET {url}\r\n"
            body_parts.append(part)

        body = "\r\n".join(body_parts) + f"\r\n--{boundary}--"

        headers = self.get_headers()
        headers["Content-Type"] = f"multipart/mixed; boundary={boundary}"

        response = self._session.post(self.BATCH_URL, headers=headers, data=body)

        if response.status_code != 200:
            # Fall back to sequential requests on batch failure
            return self._fetch_sequential(endpoints, params_list)

        return self._parse_batch_response(response.text, boundary)

    def _parse_batch_response(self, response_text: str, boundary: str) -> List[Dict]:
        """Parse multipart batch response."""
        results = []
        parts = response_text.split(f"--{boundary}")

        for part in parts:
            if "Content-Type: application/json" in part or '{"' in part:
                # Extract JSON from the response part
                try:
                    json_start = part.find("{")
                    json_end = part.rfind("}") + 1
                    if 0 <= json_start < json_end:
                        json_str = part[json_start:json_end]
                        results.append(json.loads(json_str))
                except (json.JSONDecodeError, ValueError):
                    continue

        return results

    def _fetch_sequential(
        self, endpoints: List[str], params_list: List[Dict]
    ) -> List[Dict]:
        """Fallback sequential fetch when batch fails."""
        results = []
        for endpoint, params in zip(endpoints, params_list):
            result = self.make_request("GET", endpoint, params)
            if result:
                results.append(result)
        return results

    def fetch_details_parallel(
        self, ids: List[str], fetch_func, max_workers: int = MAX_WORKERS
    ) -> Generator[Dict, None, None]:
        """
        Fetch details in parallel using thread pool.
        Yields results as they complete for true streaming.
        """
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_func, id_): id_ for id_ in ids
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        yield result
                except Exception:
                    # Skip failed fetches, continue with others
                    continue
