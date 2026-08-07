import requests
from datetime import datetime, timedelta
from typing import Iterator, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Outlook connector with connection-level options.

        Expected options:
            - client_id: Application (client) ID from Azure AD app registration
            - client_secret: Client secret from Azure AD app registration
            - refresh_token: OAuth refresh token obtained during initial user authorization
            - tenant (optional): Tenant ID or 'common' for multi-tenant auth. Defaults to 'common'.
            - base_url (optional): Override for Microsoft Graph API base URL. Defaults to https://graph.microsoft.com/v1.0.
        """
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        refresh_token = options.get("refresh_token")

        if not client_id:
            raise ValueError("Outlook connector requires 'client_id' in options")
        if not client_secret:
            raise ValueError("Outlook connector requires 'client_secret' in options")
        if not refresh_token:
            raise ValueError("Outlook connector requires 'refresh_token' in options")

        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.tenant = options.get("tenant", "common")
        self.base_url = options.get(
            "base_url", "https://graph.microsoft.com/v1.0"
        ).rstrip("/")

        # OAuth token endpoint
        self.token_endpoint = f"https://login.microsoftonline.com/{self.tenant}/oauth2/v2.0/token"

        # Initialize session
        self._session = requests.Session()
        self._access_token = None
        self._token_expiry = None
        
        # Obtain access token on first API call instead of during initialization
        # This allows the connector to be instantiated for testing without making network calls
        # The token will be refreshed automatically when needed via _ensure_valid_token()

    def _refresh_access_token(self) -> None:
        """
        Exchange the refresh token for a new access token.
        Updates the session headers with the new bearer token.
        """
        token_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
            "scope": "User.Read",
        }

        response = requests.post(self.token_endpoint, data=token_data, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to refresh access token: {response.status_code} {response.text}"
            )

        token_response = response.json()
        self._access_token = token_response.get("access_token")
        expires_in = token_response.get("expires_in", 3600)

        if not self._access_token:
            raise RuntimeError("No access_token in token refresh response")

        # Set token expiry (with 5-minute buffer)
        self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in - 300)

        # Update session headers
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def _ensure_valid_token(self) -> None:
        """
        Check if the access token is still valid and refresh if needed.
        """
        if (
            self._token_expiry is None
            or datetime.utcnow() >= self._token_expiry
        ):
            self._refresh_access_token()

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        """
        return ["messages", "mailfolders"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the Microsoft Graph API documentation
        and connector design for Outlook Mail objects.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        # Nested recipient struct (used in from, sender, toRecipients, etc.)
        email_address_struct = StructType(
            [
                StructField("name", StringType(), True),
                StructField("address", StringType(), True),
            ]
        )

        recipient_struct = StructType(
            [
                StructField("emailAddress", email_address_struct, True),
            ]
        )

        # Nested body struct
        body_struct = StructType(
            [
                StructField("contentType", StringType(), True),
                StructField("content", StringType(), True),
            ]
        )

        # Nested dateTimeTimeZone struct (used in flag)
        date_time_time_zone_struct = StructType(
            [
                StructField("dateTime", StringType(), True),
                StructField("timeZone", StringType(), True),
            ]
        )

        # Nested flag struct
        flag_struct = StructType(
            [
                StructField("flagStatus", StringType(), True),
                StructField("startDateTime", date_time_time_zone_struct, True),
                StructField("dueDateTime", date_time_time_zone_struct, True),
                StructField("completedDateTime", date_time_time_zone_struct, True),
            ]
        )

        if table_name == "messages":
            messages_schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("createdDateTime", StringType(), True),
                    StructField("lastModifiedDateTime", StringType(), True),
                    StructField("receivedDateTime", StringType(), True),
                    StructField("sentDateTime", StringType(), True),
                    StructField("hasAttachments", BooleanType(), True),
                    StructField("internetMessageId", StringType(), True),
                    StructField("subject", StringType(), True),
                    StructField("bodyPreview", StringType(), True),
                    StructField("importance", StringType(), True),
                    StructField("parentFolderId", StringType(), True),
                    StructField("conversationId", StringType(), True),
                    StructField("conversationIndex", StringType(), True),
                    StructField("isDeliveryReceiptRequested", BooleanType(), True),
                    StructField("isReadReceiptRequested", BooleanType(), True),
                    StructField("isRead", BooleanType(), True),
                    StructField("isDraft", BooleanType(), True),
                    StructField("webLink", StringType(), True),
                    StructField("inferenceClassification", StringType(), True),
                    StructField("body", body_struct, True),
                    StructField("uniqueBody", body_struct, True),
                    StructField("from", recipient_struct, True),
                    StructField("sender", recipient_struct, True),
                    StructField("toRecipients", ArrayType(recipient_struct, True), True),
                    StructField("ccRecipients", ArrayType(recipient_struct, True), True),
                    StructField(
                        "bccRecipients", ArrayType(recipient_struct, True), True
                    ),
                    StructField("replyTo", ArrayType(recipient_struct, True), True),
                    StructField("flag", flag_struct, True),
                    StructField("categories", ArrayType(StringType(), True), True),
                ]
            )
            return messages_schema

        if table_name == "mailfolders":
            mailfolders_schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("displayName", StringType(), True),
                    StructField("parentFolderId", StringType(), True),
                    StructField("childFolderCount", LongType(), True),
                    StructField("unreadItemCount", LongType(), True),
                    StructField("totalItemCount", LongType(), True),
                    StructField("wellKnownName", StringType(), True),
                ]
            )
            return mailfolders_schema

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.

        For `messages`:
            - ingestion_type: cdc
            - primary_keys: ["id"]
            - cursor_field: lastModifiedDateTime

        For `mailfolders`:
            - ingestion_type: snapshot
            - primary_keys: ["id"]
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        if table_name == "messages":
            return {
                "primary_keys": ["id"],
                "cursor_field": "lastModifiedDateTime",
                "ingestion_type": "cdc",
            }

        if table_name == "mailfolders":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.

        For the `messages` table this method:
            - Uses `/me/messages` or `/me/mailfolders/{folderId}/messages` endpoint.
            - Supports incremental reads via the `$filter` query parameter with lastModifiedDateTime.
            - Paginates using Microsoft Graph's `@odata.nextLink` until all pages are read or
              a batch limit (if provided via table_options) is reached.

        Optional table_options for `messages`:
            - folder_id or folder_name: Target folder (default: all folders using /me/messages).
            - top: Page size (max 999, default 999).
            - start_date: Initial ISO 8601 timestamp for first run if no start_offset is provided.
            - lookback_seconds: Lookback window applied when computing next cursor (default: 120).
            - max_pages_per_batch: Optional safety limit on pages per read_table call (default: 50).
            - include_drafts: Whether to include draft messages (default: true, no filtering).

        For the `mailfolders` table this method:
            - Uses `/me/mailfolders` endpoint.
            - Performs a full snapshot read with pagination.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        if table_name == "messages":
            return self._read_messages(start_offset, table_options)

        if table_name == "mailfolders":
            return self._read_mailfolders(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_messages(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """Internal implementation for reading the `messages` table."""
        self._ensure_valid_token()

        # Determine target folder
        folder_id = table_options.get("folder_id")
        folder_name = table_options.get("folder_name")

        if folder_id and folder_name:
            raise ValueError(
                "table_options for 'messages' must not include both 'folder_id' and 'folder_name'; "
                "specify only one or neither for all folders."
            )

        # Build endpoint URL
        if folder_id:
            url = f"{self.base_url}/me/mailfolders/{folder_id}/messages"
        elif folder_name:
            url = f"{self.base_url}/me/mailfolders/{folder_name}/messages"
        else:
            # Default: read from all folders
            url = f"{self.base_url}/me/messages"

        # Page size and safety limits
        try:
            top = int(table_options.get("top", 999))
        except (TypeError, ValueError):
            top = 999
        top = max(1, min(top, 999))

        try:
            max_pages_per_batch = int(table_options.get("max_pages_per_batch", 50))
        except (TypeError, ValueError):
            max_pages_per_batch = 50

        try:
            lookback_seconds = int(table_options.get("lookback_seconds", 120))
        except (TypeError, ValueError):
            lookback_seconds = 120

        # Determine the starting cursor (ISO 8601 string)
        cursor = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
        if not cursor:
            cursor = table_options.get("start_date")

        # Build query parameters
        params = {
            "$top": top,
            "$orderby": "lastModifiedDateTime asc",
        }

        # Apply incremental filter if cursor exists
        if cursor:
            params["$filter"] = f"lastModifiedDateTime ge {cursor}"

        # Optional: exclude drafts if specified
        include_drafts = table_options.get("include_drafts", "true").lower() == "true"
        if not include_drafts:
            # Add isDraft eq false to filter
            draft_filter = "isDraft eq false"
            if "$filter" in params:
                params["$filter"] = f"({params['$filter']}) and ({draft_filter})"
            else:
                params["$filter"] = draft_filter

        records: list[dict[str, Any]] = []
        max_last_modified: str | None = None

        pages_fetched = 0
        next_url: str | None = url

        while next_url and pages_fetched < max_pages_per_batch:
            # For the first request, use params; for subsequent requests, the full URL is in @odata.nextLink
            if pages_fetched == 0:
                response = self._session.get(next_url, params=params, timeout=60)
            else:
                response = self._session.get(next_url, timeout=60)

            if response.status_code != 200:
                raise RuntimeError(
                    f"Microsoft Graph API error for messages: {response.status_code} {response.text}"
                )

            response_json = response.json()
            messages = response_json.get("value", [])

            if not isinstance(messages, list):
                raise ValueError(
                    f"Unexpected response format for messages: expected list, got {type(messages).__name__}"
                )

            for message in messages:
                # Return raw JSON as-is (no conversion based on schema)
                records.append(message)

                last_modified = message.get("lastModifiedDateTime")
                if isinstance(last_modified, str):
                    if max_last_modified is None or last_modified > max_last_modified:
                        max_last_modified = last_modified

            # Handle pagination via @odata.nextLink
            next_url = response_json.get("@odata.nextLink")
            if next_url:
                pages_fetched += 1
            else:
                break

        # Compute the next cursor with a lookback window to avoid missing records
        next_cursor = cursor
        if max_last_modified:
            try:
                # Parse ISO 8601 datetime (handles both 'Z' and timezone offsets)
                # Microsoft Graph returns ISO 8601 with 'Z' suffix
                if max_last_modified.endswith("Z"):
                    dt = datetime.strptime(max_last_modified, "%Y-%m-%dT%H:%M:%SZ")
                else:
                    # Handle other ISO 8601 formats if needed
                    dt = datetime.fromisoformat(
                        max_last_modified.replace("Z", "+00:00")
                    )

                dt_with_lookback = dt - timedelta(seconds=lookback_seconds)
                next_cursor = dt_with_lookback.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                # Fallback: if parsing fails, just reuse the raw max_last_modified
                next_cursor = max_last_modified

        # If no new records and we had a start_offset, return the same offset to indicate end of stream
        if not records and start_offset:
            next_offset = start_offset
        else:
            next_offset = {"cursor": next_cursor} if next_cursor else {}

        return iter(records), next_offset

    def _read_mailfolders(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the `mailfolders` snapshot table.

        This implementation lists mailfolders for the authenticated user using:
            GET /me/mailfolders

        Optional table_options:
            - top: Page size (max 999, default 999).
            - max_pages_per_batch: Optional safety limit on pages per read_table call (default: 50).
        """
        self._ensure_valid_token()

        url = f"{self.base_url}/me/mailfolders"

        try:
            top = int(table_options.get("top", 999))
        except (TypeError, ValueError):
            top = 999
        top = max(1, min(top, 999))

        try:
            max_pages_per_batch = int(table_options.get("max_pages_per_batch", 50))
        except (TypeError, ValueError):
            max_pages_per_batch = 50

        params = {"$top": top}

        records: list[dict[str, Any]] = []
        pages_fetched = 0
        next_url: str | None = url

        while next_url and pages_fetched < max_pages_per_batch:
            if pages_fetched == 0:
                response = self._session.get(next_url, params=params, timeout=60)
            else:
                response = self._session.get(next_url, timeout=60)

            if response.status_code != 200:
                raise RuntimeError(
                    f"Microsoft Graph API error for mailfolders: {response.status_code} {response.text}"
                )

            response_json = response.json()
            folders = response_json.get("value", [])

            if not isinstance(folders, list):
                raise ValueError(
                    f"Unexpected response format for mailfolders: expected list, got {type(folders).__name__}"
                )

            for folder in folders:
                # Return raw JSON as-is
                records.append(folder)

            # Handle pagination via @odata.nextLink
            next_url = response_json.get("@odata.nextLink")
            if next_url:
                pages_fetched += 1
            else:
                break

        # Snapshot table: no cursor, return empty offset
        return iter(records), {}

