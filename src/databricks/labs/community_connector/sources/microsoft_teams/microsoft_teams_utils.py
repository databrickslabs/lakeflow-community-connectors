"""Utility functions for the Microsoft Teams connector.

This module contains helper functions for OAuth token management, HTTP requests
with retry logic, auto-discovery of teams/channels/messages, and common
record serialization used across the connector.
"""

import json
import time
from datetime import datetime, timedelta
from typing import Any

import requests


class MicrosoftGraphClient:
    """HTTP client for Microsoft Graph API with OAuth 2.0 and retry logic."""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://graph.microsoft.com/v1.0"
        self._access_token = None
        self._token_expiry = None

    def get_access_token(self) -> str:
        """
        Acquire OAuth 2.0 access token using client credentials flow.
        Tokens are cached and reused until 5 minutes before expiration.
        """
        if not self.tenant_id or not self.client_id or not self.client_secret:
            raise ValueError(
                "Missing required options: tenant_id, client_id, and client_secret are required. "
                "Pass them in the connection properties or in table_configuration for each table."
            )

        if (
            self._access_token
            and self._token_expiry
            and datetime.utcnow() < self._token_expiry
        ):
            return self._access_token

        token_url = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        try:
            response = requests.post(token_url, data=data, timeout=30)
            if response.status_code != 200:
                if self.tenant_id and len(self.tenant_id) > 8:
                    tenant_preview = f"{self.tenant_id[:8]}..."
                else:
                    tenant_preview = "INVALID"
                raise RuntimeError(
                    f"Token acquisition failed: {response.status_code}\n"
                    f"URL: {token_url}\n"
                    f"Tenant ID (first 8 chars): {tenant_preview}\n"
                    f"Response: {response.text[:500]}"
                )

            token_data = response.json()
            self._access_token = token_data["access_token"]

            expires_in = token_data.get("expires_in", 3600)
            self._token_expiry = datetime.utcnow() + timedelta(
                seconds=expires_in - 300
            )

            return self._access_token

        except requests.RequestException as e:
            raise RuntimeError(f"Token acquisition request failed: {str(e)}")

    def make_request(  # pylint: disable=too-many-branches
        self, url: str, params: dict = None, max_retries: int = 3
    ) -> dict:
        """
        Make HTTP GET request to Microsoft Graph API with exponential backoff retry.
        Handles rate limiting (429), server errors, and token refresh on 401.
        """
        for attempt in range(max_retries):
            try:
                headers = {
                    "Authorization": f"Bearer {self.get_access_token()}",
                    "Content-Type": "application/json",
                }

                response = requests.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    return response.json()

                elif response.status_code == 401:
                    self._access_token = None
                    self._token_expiry = None
                    if attempt < max_retries - 1:
                        continue
                    raise RuntimeError(
                        f"Authentication failed (401). Please verify credentials and permissions."
                    )

                elif response.status_code == 403:
                    raise RuntimeError(
                        f"Forbidden (403). Please verify the app has required permissions: {response.text}"
                    )

                elif response.status_code == 404:
                    raise RuntimeError(
                        f"Resource not found (404). Please verify team_id/channel_id: {response.text}"
                    )

                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    time.sleep(retry_after)
                    continue

                elif response.status_code in [500, 502, 503]:
                    if attempt < max_retries - 1:
                        time.sleep(2**attempt)
                        continue
                    raise RuntimeError(
                        f"Server error ({response.status_code}) after {max_retries} retries: {response.text}"
                    )

                else:
                    raise RuntimeError(
                        f"Request failed with status {response.status_code}: {response.text}"
                    )

            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                if isinstance(e, requests.Timeout):
                    raise RuntimeError(
                        f"Request timeout after {max_retries} attempts: {url}"
                    ) from e
                raise RuntimeError(f"Request exception: {str(e)}") from e

        raise RuntimeError(f"Max retries ({max_retries}) exceeded for: {url}")


def fetch_all_team_ids(client: MicrosoftGraphClient, max_pages: int) -> list[str]:
    """Fetch all team IDs from the organization."""
    teams_url = f"{client.base_url}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
    teams_params = {"$select": "id"}
    team_ids = []
    pages_fetched = 0
    next_url: str | None = teams_url

    while next_url and pages_fetched < max_pages:
        if pages_fetched == 0:
            data = client.make_request(teams_url, params=teams_params)
        else:
            data = client.make_request(next_url)

        teams = data.get("value", [])
        for team in teams:
            team_id = team.get("id")
            if team_id:
                team_ids.append(team_id)

        next_url = data.get("@odata.nextLink")
        pages_fetched += 1

        if next_url:
            time.sleep(0.1)

    return team_ids


def fetch_all_channel_ids(
    client: MicrosoftGraphClient, team_id: str, max_pages: int
) -> list[str]:
    """Fetch all channel IDs for a specific team. Returns empty list if team is inaccessible."""
    channels_url = f"{client.base_url}/teams/{team_id}/channels"
    channels_params = {"$select": "id"}
    channel_ids = []
    pages_fetched = 0
    next_url: str | None = channels_url

    try:
        while next_url and pages_fetched < max_pages:
            if pages_fetched == 0:
                data = client.make_request(channels_url, params=channels_params)
            else:
                data = client.make_request(next_url)

            channels = data.get("value", [])
            for channel in channels:
                channel_id = channel.get("id")
                if channel_id:
                    channel_ids.append(channel_id)

            next_url = data.get("@odata.nextLink")
            pages_fetched += 1

            if next_url:
                time.sleep(0.1)
    except Exception as e:
        if "404" in str(e) or "403" in str(e):
            return []
        raise

    return channel_ids


def fetch_all_message_ids(
    client: MicrosoftGraphClient,
    team_id: str,
    channel_id: str,
    max_pages: int,
) -> list[str]:
    """Fetch all message IDs for a specific channel. Returns empty list if channel is inaccessible."""
    messages_url = f"{client.base_url}/teams/{team_id}/channels/{channel_id}/messages"
    messages_params = {"$top": 50}
    message_ids = []
    pages_fetched = 0
    next_url: str | None = messages_url

    try:
        while next_url and pages_fetched < max_pages:
            if pages_fetched == 0:
                data = client.make_request(messages_url, params=messages_params)
            else:
                data = client.make_request(next_url)

            messages = data.get("value", [])
            for message in messages:
                message_id = message.get("id")
                if message_id:
                    message_ids.append(message_id)

            next_url = data.get("@odata.nextLink")
            pages_fetched += 1

            if next_url:
                time.sleep(0.1)
    except Exception as e:
        if "404" in str(e) or "403" in str(e):
            return []
        raise

    return message_ids


def serialize_complex_fields(record: dict[str, Any], fields: list[str]) -> None:
    """Serialize complex objects (dict/list) to JSON strings in-place."""
    for field in fields:
        if field in record:
            val = record[field]
            if isinstance(val, (dict, list)):
                record[field] = json.dumps(val)


def parse_int_option(
    table_options: dict[str, str], key: str, default: int
) -> int:
    """Parse an integer option from table_options with a default value."""
    try:
        return int(table_options.get(key, default))
    except (TypeError, ValueError):
        return default


def compute_next_cursor(
    max_modified: str | None,
    current_cursor: str | None,
    lookback_seconds: int,
) -> str | None:
    """Compute the next cursor value with a lookback window."""
    if not max_modified:
        return current_cursor

    try:
        dt = datetime.fromisoformat(max_modified.replace("Z", "+00:00"))
        dt_with_lookback = dt - timedelta(seconds=lookback_seconds)
        return dt_with_lookback.isoformat().replace("+00:00", "Z")
    except Exception:
        return max_modified


def get_cursor_from_offset(
    start_offset: dict | None, table_options: dict[str, str]
) -> str | None:
    """Extract the cursor value from start_offset or fall back to table_options."""
    cursor = None
    if start_offset and isinstance(start_offset, dict):
        cursor = start_offset.get("cursor")
    if not cursor:
        cursor = table_options.get("start_date")
    return cursor


def resolve_team_ids(
    client: MicrosoftGraphClient,
    table_options: dict[str, str],
    table_name: str,
    max_pages: int,
) -> list[str]:
    """Resolve team IDs from table_options or auto-discover all teams."""
    team_id = table_options.get("team_id")
    fetch_all_teams = table_options.get("fetch_all_teams", "").lower() == "true"

    if not team_id and not fetch_all_teams:
        raise ValueError(
            f"table_options for '{table_name}' must include either 'team_id' "
            f"or 'fetch_all_teams=true'"
        )

    if fetch_all_teams:
        return fetch_all_team_ids(client, max_pages)
    return [team_id]


def resolve_team_channel_pairs(
    client: MicrosoftGraphClient,
    table_options: dict[str, str],
    table_name: str,
    max_pages: int,
) -> list[tuple[str, str]]:
    """Resolve team-channel pairs from table_options or auto-discover."""
    team_ids = resolve_team_ids(client, table_options, table_name, max_pages)
    channel_id = table_options.get("channel_id")
    fetch_all_channels = table_options.get("fetch_all_channels", "").lower() == "true"

    if not channel_id and not fetch_all_channels:
        raise ValueError(
            f"table_options for '{table_name}' must include either 'channel_id' "
            f"or 'fetch_all_channels=true'"
        )

    pairs = []
    for tid in team_ids:
        if fetch_all_channels:
            ch_ids = fetch_all_channel_ids(client, tid, max_pages)
            if not ch_ids:
                continue
        else:
            ch_ids = [channel_id]
        for ch_id in ch_ids:
            pairs.append((tid, ch_id))
    return pairs
