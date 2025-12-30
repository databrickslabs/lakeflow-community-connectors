import requests
import json
from datetime import datetime, timedelta
from typing import Iterator, Any, Dict, List, Tuple
import time

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
        Initialize Microsoft Teams connector with OAuth 2.0 credentials.

        Required options (pass via table_configuration):
          - tenant_id: Azure AD tenant ID
          - client_id: Application (client) ID
          - client_secret: Client secret value

        Note: Credentials should be passed via table_configuration in the pipeline spec,
        not in the connection properties (which aren't accessible to the connector).

        Authentication uses the Client Credentials Flow with Application Permissions.
        Requires admin consent for all permissions.
        """
        self.tenant_id = options.get("tenant_id")
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")

        # NOTE: We do NOT validate credentials here anymore.
        # This allows the connector to initialize for metadata discovery (_lakeflow_metadata)
        # where credentials might not be passed yet.
        # Strict validation happens in _get_access_token() when we actually need to connect to Teams API.

        self.base_url = "https://graph.microsoft.com/v1.0"
        self._access_token = None
        self._token_expiry = None

        # Centralized object metadata configuration (following Stripe pattern)
        self._object_config = {
            "teams": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
                "endpoint": "groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')",
                # Required permission: Team.ReadBasic.All (Application)
                # Note: Using /groups with filter for Application Permissions (not /me/joinedTeams)
            },
            "channels": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
                "endpoint": "teams/{team_id}/channels",
                "requires_parent": [],  # team_id optional with fetch_all_teams
                # Required permission: Channel.ReadBasic.All (Application)
                # Supports fetch_all_teams=true to auto-discover all teams
            },
            "messages": {
                "primary_keys": ["id"],
                "cursor_field": "lastModifiedDateTime",
                "ingestion_type": "cdc",
                "endpoint": "teams/{team_id}/channels/{channel_id}/messages",
                "requires_parent": ["team_id"],  # channel_id optional with fetch_all_channels
                # Required permission: ChannelMessage.Read.All (Application)
                # Supports fetch_all_channels=true to auto-discover all channels in team
            },
            "members": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
                "endpoint": "teams/{team_id}/members",
                "requires_parent": [],  # team_id optional with fetch_all_teams
                # Required permission: TeamMember.Read.All (Application)
                # Supports fetch_all_teams=true to auto-discover all teams
            },
            "message_replies": {
                "primary_keys": ["id"],
                "cursor_field": "lastModifiedDateTime",
                "ingestion_type": "cdc",
                "endpoint": "teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
                "requires_parent": ["team_id", "channel_id"],  # message_id optional with fetch_all_messages
                # Required permission: ChannelMessage.Read.All (Application) - same as messages
                # Supports fetch_all_messages=true to auto-discover all messages in channel
            },
        }

        # Reusable nested schemas (following Stripe pattern)
        self._identity_set_schema = StructType(
            [
                StructField("application", StringType(), True),
                StructField("device", StringType(), True),
                StructField(
                    "user",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("displayName", StringType(), True),
                            StructField("userIdentityType", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

        self._body_schema = StructType(
            [
                StructField("contentType", StringType(), True),
                StructField("content", StringType(), True),
            ]
        )

        self._attachment_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("contentType", StringType(), True),
                StructField("contentUrl", StringType(), True),
                StructField("content", StringType(), True),
                StructField("name", StringType(), True),
                StructField("thumbnailUrl", StringType(), True),
            ]
        )

        self._mention_schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("mentionText", StringType(), True),
                StructField("mentioned", self._identity_set_schema, True),
            ]
        )

        self._reaction_schema = StructType(
            [
                StructField("reactionType", StringType(), True),
                StructField("createdDateTime", StringType(), True),
                StructField("user", self._identity_set_schema, True),
            ]
        )

        self._channel_identity_schema = StructType(
            [
                StructField("teamId", StringType(), True),
                StructField("channelId", StringType(), True),
            ]
        )

    def _get_access_token(self) -> str:
        """
        Acquire OAuth 2.0 access token using client credentials flow.

        The connector automatically refreshes tokens before expiry.
        Tokens are cached and reused until 5 minutes before expiration.

        Returns:
            str: Access token for Microsoft Graph API

        Raises:
            ValueError: If required credentials are missing
            RuntimeError: If token acquisition fails
        """
        # Validate credentials just-in-time before we need them
        if not self.tenant_id or not self.client_id or not self.client_secret:
            raise ValueError(
                "Missing required options: tenant_id, client_id, and client_secret are required. "
                "Pass them in the connection properties or in table_configuration for each table."
            )

        # Return cached token if still valid
        if (
            self._access_token
            and self._token_expiry
            and datetime.utcnow() < self._token_expiry
        ):
            return self._access_token

        # Request new token using client credentials flow
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
                raise RuntimeError(
                    f"Token acquisition failed: {response.status_code} {response.text}"
                )

            token_data = response.json()
            self._access_token = token_data["access_token"]

            # Set expiry 5 minutes before actual expiry for safety
            expires_in = token_data.get("expires_in", 3600)
            self._token_expiry = datetime.utcnow() + timedelta(
                seconds=expires_in - 300
            )

            return self._access_token

        except requests.RequestException as e:
            raise RuntimeError(f"Token acquisition request failed: {str(e)}")

    def _make_request_with_retry(
        self, url: str, params: dict = None, max_retries: int = 3
    ) -> dict:
        """
        Make HTTP GET request to Microsoft Graph API with exponential backoff retry.

        Implements production-grade error handling:
        - Automatic retry on rate limiting (429) with Retry-After header
        - Exponential backoff on server errors (500, 502, 503)
        - Timeout handling

        Args:
            url: Full URL to request
            params: Query parameters (optional)
            max_retries: Maximum number of retry attempts (default: 3)

        Returns:
            dict: JSON response from API

        Raises:
            RuntimeError: If request fails after all retries
        """
        for attempt in range(max_retries):
            try:
                headers = {
                    "Authorization": f"Bearer {self._get_access_token()}",
                    "Content-Type": "application/json",
                }

                response = requests.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    return response.json()

                elif response.status_code == 401:
                    # Token may have expired, clear cache and retry once
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
                    # Rate limiting - respect Retry-After header
                    retry_after = int(response.headers.get("Retry-After", 60))
                    time.sleep(retry_after)
                    continue

                elif response.status_code in [500, 502, 503]:
                    # Server errors - exponential backoff
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

            except requests.Timeout:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError(
                    f"Request timeout after {max_retries} attempts: {url}"
                )

            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise RuntimeError(f"Request exception: {str(e)}")

        raise RuntimeError(f"Max retries ({max_retries}) exceeded for: {url}")

    def list_tables(self) -> list[str]:
        """
        List all supported Microsoft Teams tables.

        Note: Chats table is not supported because the Microsoft Graph API /chats endpoint
        does not support Application Permissions (only Delegated Permissions with user context).

        Returns:
            list[str]: Static list of table names
        """
        return [
            "teams",
            "channels",
            "messages",
            "members",
            "message_replies",
        ]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Get the PySpark schema for a specific table.

        Args:
            table_name: Name of the table
            table_options: Table-specific options (not used for schema)

        Returns:
            StructType: PySpark schema definition

        Raises:
            ValueError: If table_name is not supported
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {self.list_tables()}"
            )

        if table_name == "teams":
            return StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("displayName", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("classification", StringType(), True),
                    StructField("visibility", StringType(), True),
                    StructField("webUrl", StringType(), True),
                    StructField("isArchived", BooleanType(), True),
                    StructField("createdDateTime", StringType(), True),
                    StructField("internalId", StringType(), True),
                    StructField("tenantId", StringType(), True),
                    StructField("specialization", StringType(), True),
                    # Store complex settings objects as JSON strings for flexibility
                    StructField("memberSettings", StringType(), True),
                    StructField("guestSettings", StringType(), True),
                    StructField("messagingSettings", StringType(), True),
                    StructField("funSettings", StringType(), True),
                ]
            )

        elif table_name == "channels":
            return StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("team_id", StringType(), False),  # Connector-derived
                    StructField("displayName", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("webUrl", StringType(), True),
                    StructField("membershipType", StringType(), True),
                    StructField("createdDateTime", StringType(), True),
                    StructField("isFavoriteByDefault", BooleanType(), True),
                    StructField("isArchived", BooleanType(), True),
                    StructField("tenantId", StringType(), True),
                ]
            )

        elif table_name == "messages":
            return StructType(
                [
                    StructField("id", StringType(), False),
                    StructField(
                        "team_id", StringType(), False
                    ),  # Connector-derived
                    StructField(
                        "channel_id", StringType(), False
                    ),  # Connector-derived
                    StructField("replyToId", StringType(), True),
                    StructField("etag", StringType(), True),
                    StructField("messageType", StringType(), True),
                    StructField("createdDateTime", StringType(), True),
                    StructField("lastModifiedDateTime", StringType(), True),
                    StructField("lastEditedDateTime", StringType(), True),
                    StructField("deletedDateTime", StringType(), True),
                    StructField("subject", StringType(), True),
                    StructField("summary", StringType(), True),
                    StructField("importance", StringType(), True),
                    StructField("locale", StringType(), True),
                    StructField("webUrl", StringType(), True),
                    StructField("from", self._identity_set_schema, True),
                    StructField("body", self._body_schema, True),
                    StructField(
                        "attachments", ArrayType(self._attachment_schema), True
                    ),
                    StructField("mentions", ArrayType(self._mention_schema), True),
                    StructField("reactions", ArrayType(self._reaction_schema), True),
                    StructField("channelIdentity", self._channel_identity_schema, True),
                    # Store complex/polymorphic objects as JSON strings
                    StructField("policyViolation", StringType(), True),
                    StructField("eventDetail", StringType(), True),
                    StructField("messageHistory", StringType(), True),
                ]
            )

        elif table_name == "members":
            return StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("team_id", StringType(), False),  # Connector-derived
                    StructField("roles", ArrayType(StringType()), True),
                    StructField("displayName", StringType(), True),
                    StructField("userId", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("visibleHistoryStartDateTime", StringType(), True),
                    StructField("tenantId", StringType(), True),
                ]
            )

        elif table_name == "message_replies":
            # Same schema as messages, plus parent_message_id
            return StructType(
                [
                    StructField("id", StringType(), False),
                    StructField(
                        "parent_message_id", StringType(), False
                    ),  # Connector-derived (NEW!)
                    StructField(
                        "team_id", StringType(), False
                    ),  # Connector-derived
                    StructField(
                        "channel_id", StringType(), False
                    ),  # Connector-derived
                    StructField("replyToId", StringType(), True),
                    StructField("etag", StringType(), True),
                    StructField("messageType", StringType(), True),
                    StructField("createdDateTime", StringType(), True),
                    StructField("lastModifiedDateTime", StringType(), True),
                    StructField("lastEditedDateTime", StringType(), True),
                    StructField("deletedDateTime", StringType(), True),
                    StructField("subject", StringType(), True),
                    StructField("summary", StringType(), True),
                    StructField("importance", StringType(), True),
                    StructField("locale", StringType(), True),
                    StructField("webUrl", StringType(), True),
                    StructField("from", self._identity_set_schema, True),
                    StructField("body", self._body_schema, True),
                    StructField(
                        "attachments", ArrayType(self._attachment_schema), True
                    ),
                    StructField("mentions", ArrayType(self._mention_schema), True),
                    StructField("reactions", ArrayType(self._reaction_schema), True),
                    StructField("channelIdentity", self._channel_identity_schema, True),
                    # Store complex/polymorphic objects as JSON strings
                    StructField("policyViolation", StringType(), True),
                    StructField("eventDetail", StringType(), True),
                    StructField("messageHistory", StringType(), True),
                ]
            )

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Get metadata for a table (primary keys, cursor field, ingestion type).

        Args:
            table_name: Name of the table
            table_options: Table-specific options (not used for metadata)

        Returns:
            dict: Metadata with keys: primary_keys, ingestion_type, cursor_field (if CDC)

        Raises:
            ValueError: If table_name is not supported
        """
        if table_name not in self._object_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {self.list_tables()}"
            )

        config = self._object_config[table_name]
        metadata = {
            "primary_keys": config["primary_keys"],
            "ingestion_type": config["ingestion_type"],
        }

        # Add cursor field for CDC tables
        if "cursor_field" in config:
            metadata["cursor_field"] = config["cursor_field"]

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read data from a Microsoft Teams table.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary with cursor for incremental reads (e.g., {"cursor": "2025-01-15T10:30:00.000Z"})
            table_options: Table-specific options (team_id, channel_id, etc.)

        Returns:
            Tuple of (iterator of records, next_offset dict)

        Raises:
            ValueError: If table_name is not supported or required options are missing
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {self.list_tables()}"
            )

        # Route to table-specific implementation
        if table_name == "teams":
            return self._read_teams(start_offset, table_options)
        elif table_name == "channels":
            return self._read_channels(start_offset, table_options)
        elif table_name == "messages":
            return self._read_messages(start_offset, table_options)
        elif table_name == "members":
            return self._read_members(start_offset, table_options)
        elif table_name == "message_replies":
            return self._read_message_replies(start_offset, table_options)

    def _read_teams(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read teams table (snapshot mode).

        Args:
            start_offset: Not used for snapshot tables
            table_options: Optional table options (top, max_pages_per_batch)

        Returns:
            Tuple of (iterator of team records, empty offset dict)
        """
        # Parse options
        try:
            top = int(table_options.get("top", 50))
        except (TypeError, ValueError):
            top = 50
        top = max(1, min(top, 999))  # Max 999 per Graph API

        try:
            max_pages = int(table_options.get("max_pages_per_batch", 100))
        except (TypeError, ValueError):
            max_pages = 100

        # Build initial request
        endpoint = self._object_config["teams"]["endpoint"]
        url = f"{self.base_url}/{endpoint}"
        params = {"$top": top}

        records: List[dict[str, Any]] = []
        pages_fetched = 0
        next_url: str | None = url

        while next_url and pages_fetched < max_pages:
            # Use URL directly for subsequent pages (contains pagination token)
            if pages_fetched == 0:
                data = self._make_request_with_retry(url, params=params)
            else:
                data = self._make_request_with_retry(next_url)

            teams = data.get("value", [])

            for team in teams:
                # Convert complex nested objects to JSON strings
                record: dict[str, Any] = dict(team)

                # Serialize complex settings objects
                if "memberSettings" in record and isinstance(
                    record["memberSettings"], dict
                ):
                    record["memberSettings"] = json.dumps(record["memberSettings"])

                if "guestSettings" in record and isinstance(
                    record["guestSettings"], dict
                ):
                    record["guestSettings"] = json.dumps(record["guestSettings"])

                if "messagingSettings" in record and isinstance(
                    record["messagingSettings"], dict
                ):
                    record["messagingSettings"] = json.dumps(
                        record["messagingSettings"]
                    )

                if "funSettings" in record and isinstance(record["funSettings"], dict):
                    record["funSettings"] = json.dumps(record["funSettings"])

                records.append(record)

            # Handle pagination
            next_url = data.get("@odata.nextLink")
            pages_fetched += 1

            # Rate limiting - sleep between requests
            if next_url:
                time.sleep(0.1)  # 100ms delay

        # Snapshot tables return empty offset
        return iter(records), {}

    def _read_channels(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read channels table (snapshot mode).

        Two modes:
        1. Specific team: Requires team_id
        2. Auto-discovery: Requires fetch_all_teams="true"

        Args:
            start_offset: Not used for snapshot tables
            table_options: Must include either team_id or fetch_all_teams

        Returns:
            Tuple of (iterator of channel records, empty offset dict)

        Raises:
            ValueError: If required options are missing
        """
        team_id = table_options.get("team_id")
        fetch_all_teams = table_options.get("fetch_all_teams", "").lower() == "true"

        # Validate inputs
        if not team_id and not fetch_all_teams:
            raise ValueError(
                "table_options for 'channels' must include either 'team_id' "
                "or 'fetch_all_teams=true'"
            )

        # Parse options
        try:
            max_pages = int(table_options.get("max_pages_per_batch", 100))
        except (TypeError, ValueError):
            max_pages = 100

        # If fetch_all_teams mode, first discover all team IDs
        team_ids_to_process = []
        if fetch_all_teams:
            # Fetch all teams to get team IDs
            teams_url = f"{self.base_url}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
            teams_params = {"$select": "id"}
            teams_next_url: str | None = teams_url
            tm_pages_fetched = 0

            while teams_next_url and tm_pages_fetched < max_pages:
                if tm_pages_fetched == 0:
                    tm_data = self._make_request_with_retry(teams_url, params=teams_params)
                else:
                    tm_data = self._make_request_with_retry(teams_next_url)

                teams = tm_data.get("value", [])
                for tm in teams:
                    tm_id = tm.get("id")
                    if tm_id:
                        team_ids_to_process.append(tm_id)

                teams_next_url = tm_data.get("@odata.nextLink")
                tm_pages_fetched += 1

                if teams_next_url:
                    time.sleep(0.1)
        else:
            # Single team mode
            team_ids_to_process = [team_id]

        # Now fetch channels for all discovered teams
        records: List[dict[str, Any]] = []

        for current_team_id in team_ids_to_process:
            # Build request for this team's channels
            url = f"{self.base_url}/teams/{current_team_id}/channels"
            params = {}  # No query parameters needed - API returns all channels with pagination

            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._make_request_with_retry(url, params=params)
                    else:
                        data = self._make_request_with_retry(next_url)

                    channels = data.get("value", [])

                    for channel in channels:
                        # Add connector-derived field
                        record: dict[str, Any] = dict(channel)
                        record["team_id"] = current_team_id
                        records.append(record)

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1

                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    # If a team is inaccessible, log and continue
                    if "404" not in str(e) and "403" not in str(e):
                        # Only raise if it's not a 404/403 (inaccessible team)
                        raise
                    # Continue to next team on 404/403
                    break

        return iter(records), {}

    def _read_members(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read members table (snapshot mode).

        Two modes:
        1. Specific team: Requires team_id
        2. Auto-discovery: Requires fetch_all_teams="true"

        Args:
            start_offset: Not used for snapshot tables
            table_options: Must include either team_id or fetch_all_teams

        Returns:
            Tuple of (iterator of member records, empty offset dict)

        Raises:
            ValueError: If required options are missing
        """
        team_id = table_options.get("team_id")
        fetch_all_teams = table_options.get("fetch_all_teams", "").lower() == "true"

        # Validate inputs
        if not team_id and not fetch_all_teams:
            raise ValueError(
                "table_options for 'members' must include either 'team_id' "
                "or 'fetch_all_teams=true'"
            )

        try:
            max_pages = int(table_options.get("max_pages_per_batch", 100))
        except (TypeError, ValueError):
            max_pages = 100

        # If fetch_all_teams mode, first discover all team IDs
        team_ids_to_process = []
        if fetch_all_teams:
            # Fetch all teams to get team IDs
            teams_url = f"{self.base_url}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
            teams_params = {"$select": "id"}
            teams_next_url: str | None = teams_url
            tm_pages_fetched = 0

            while teams_next_url and tm_pages_fetched < max_pages:
                if tm_pages_fetched == 0:
                    tm_data = self._make_request_with_retry(teams_url, params=teams_params)
                else:
                    tm_data = self._make_request_with_retry(teams_next_url)

                teams = tm_data.get("value", [])
                for tm in teams:
                    tm_id = tm.get("id")
                    if tm_id:
                        team_ids_to_process.append(tm_id)

                teams_next_url = tm_data.get("@odata.nextLink")
                tm_pages_fetched += 1

                if teams_next_url:
                    time.sleep(0.1)
        else:
            # Single team mode
            team_ids_to_process = [team_id]

        # Now fetch members for all discovered teams
        records: List[dict[str, Any]] = []

        for current_team_id in team_ids_to_process:
            # Note: /teams/{id}/members endpoint does NOT support $top parameter
            url = f"{self.base_url}/teams/{current_team_id}/members"
            params = {}  # No query parameters needed - API returns all members with pagination

            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._make_request_with_retry(url, params=params)
                    else:
                        data = self._make_request_with_retry(next_url)

                    members = data.get("value", [])

                    for member in members:
                        record: dict[str, Any] = dict(member)
                        record["team_id"] = current_team_id
                        records.append(record)

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1

                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    # If a team is inaccessible, log and continue
                    if "404" not in str(e) and "403" not in str(e):
                        # Only raise if it's not a 404/403 (inaccessible team)
                        raise
                    # Continue to next team on 404/403
                    break

        return iter(records), {}

    def _read_messages(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read messages table (CDC mode with timestamp-based filtering).

        Three modes:
        1. Specific channel: Requires team_id and channel_id
        2. Auto-discover channels: Requires team_id and fetch_all_channels="true"
        3. Auto-discover everything: Requires fetch_all_teams="true" and fetch_all_channels="true"

        Args:
            start_offset: Dictionary with cursor (e.g., {"cursor": "2025-01-15T10:30:00.000Z"})
            table_options: Must include team_id OR fetch_all_teams, plus either channel_id or fetch_all_channels

        Returns:
            Tuple of (iterator of message records, next_offset dict with updated cursor)

        Raises:
            ValueError: If required options are missing
        """
        team_id = table_options.get("team_id")
        channel_id = table_options.get("channel_id")
        fetch_all_channels = table_options.get("fetch_all_channels", "").lower() == "true"
        fetch_all_teams = table_options.get("fetch_all_teams", "").lower() == "true"

        # Validate inputs
        if not team_id and not fetch_all_teams:
            raise ValueError(
                "table_options for 'messages' must include either 'team_id' "
                "or 'fetch_all_teams=true'"
            )

        if not channel_id and not fetch_all_channels:
            raise ValueError(
                "table_options for 'messages' must include either 'channel_id' "
                "or 'fetch_all_channels=true'"
            )

        # Parse options
        try:
            top = int(table_options.get("top", 50))
        except (TypeError, ValueError):
            top = 50
        top = max(1, min(top, 50))  # Max 50 for messages endpoint

        try:
            max_pages = int(table_options.get("max_pages_per_batch", 100))
        except (TypeError, ValueError):
            max_pages = 100

        try:
            lookback_seconds = int(table_options.get("lookback_seconds", 300))
        except (TypeError, ValueError):
            lookback_seconds = 300

        # Determine starting cursor
        cursor = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
        if not cursor:
            cursor = table_options.get("start_date")

        # If fetch_all_teams mode, first discover all team IDs
        team_ids_to_process = []
        if fetch_all_teams:
            # Fetch all teams to get team IDs
            teams_url = f"{self.base_url}/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')"
            teams_params = {"$select": "id"}
            teams_next_url: str | None = teams_url
            tm_pages_fetched = 0

            while teams_next_url and tm_pages_fetched < max_pages:
                if tm_pages_fetched == 0:
                    tm_data = self._make_request_with_retry(teams_url, params=teams_params)
                else:
                    tm_data = self._make_request_with_retry(teams_next_url)

                teams = tm_data.get("value", [])
                for tm in teams:
                    tm_id = tm.get("id")
                    if tm_id:
                        team_ids_to_process.append(tm_id)

                teams_next_url = tm_data.get("@odata.nextLink")
                tm_pages_fetched += 1

                if teams_next_url:
                    time.sleep(0.1)
        else:
            # Single team mode
            team_ids_to_process = [team_id]

        # Now process each team to get channels
        all_team_channel_pairs = []
        for current_team_id in team_ids_to_process:
            # If fetch_all_channels mode, discover all channel IDs in this team
            channel_ids_to_process = []
            if fetch_all_channels:
                # Fetch all channels from the team to get channel IDs
                channels_url = f"{self.base_url}/teams/{current_team_id}/channels"
                channels_params = {"$select": "id"}
                channels_next_url: str | None = channels_url
                ch_pages_fetched = 0

                try:
                    while channels_next_url and ch_pages_fetched < max_pages:
                        if ch_pages_fetched == 0:
                            ch_data = self._make_request_with_retry(channels_url, params=channels_params)
                        else:
                            ch_data = self._make_request_with_retry(channels_next_url)

                        channels = ch_data.get("value", [])
                        for ch in channels:
                            ch_id = ch.get("id")
                            if ch_id:
                                channel_ids_to_process.append(ch_id)

                        channels_next_url = ch_data.get("@odata.nextLink")
                        ch_pages_fetched += 1

                        if channels_next_url:
                            time.sleep(0.1)
                except Exception as e:
                    # If a team is inaccessible, log and continue
                    if "404" not in str(e) and "403" not in str(e):
                        raise
                    # Skip this team on 404/403
                    continue
            else:
                # Single channel mode
                channel_ids_to_process = [channel_id]

            # Add all team-channel pairs
            for ch_id in channel_ids_to_process:
                all_team_channel_pairs.append((current_team_id, ch_id))

        # Now fetch messages for all discovered team-channel pairs
        records: List[dict[str, Any]] = []
        max_modified: str | None = None

        for current_team_id, current_channel_id in all_team_channel_pairs:
            # Build request URL for this channel's messages
            url = f"{self.base_url}/teams/{current_team_id}/channels/{current_channel_id}/messages"
            params = {"$top": top}

            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._make_request_with_retry(url, params=params)
                    else:
                        data = self._make_request_with_retry(next_url)

                    messages = data.get("value", [])

                    for msg in messages:
                        # Filter by cursor (client-side since API doesn't support $filter on /messages)
                        last_modified = msg.get("lastModifiedDateTime")
                        if cursor and last_modified and last_modified < cursor:
                            continue  # Skip messages before cursor

                        # Add connector-derived fields
                        record: dict[str, Any] = dict(msg)
                        record["team_id"] = current_team_id
                        record["channel_id"] = current_channel_id

                        # Serialize complex objects to JSON strings
                        if "policyViolation" in record and isinstance(
                            record["policyViolation"], dict
                        ):
                            record["policyViolation"] = json.dumps(record["policyViolation"])

                        if "eventDetail" in record and isinstance(record["eventDetail"], dict):
                            record["eventDetail"] = json.dumps(record["eventDetail"])

                        if "messageHistory" in record and isinstance(
                            record["messageHistory"], list
                        ):
                            record["messageHistory"] = json.dumps(record["messageHistory"])

                        records.append(record)

                        # Track max timestamp
                        if last_modified:
                            if max_modified is None or last_modified > max_modified:
                                max_modified = last_modified

                    # Handle pagination
                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1

                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    # If a channel is inaccessible, log and continue
                    if "404" not in str(e) and "403" not in str(e):
                        # Only raise if it's not a 404/403 (inaccessible channel)
                        raise
                    # Continue to next channel on 404/403
                    break

        # Compute next cursor with lookback window
        next_cursor = cursor
        if max_modified:
            try:
                # Parse ISO 8601 timestamp
                dt = datetime.fromisoformat(max_modified.replace("Z", "+00:00"))
                dt_with_lookback = dt - timedelta(seconds=lookback_seconds)
                next_cursor = dt_with_lookback.isoformat().replace("+00:00", "Z")
            except Exception:
                # Fallback: use max_modified as-is
                next_cursor = max_modified

        next_offset = {"cursor": next_cursor} if next_cursor else {}
        return iter(records), next_offset

    def _read_message_replies(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """
        Read message replies (threads) for a specific message or all messages in a channel (CDC mode).

        Two modes:
        1. Specific message: Requires team_id, channel_id, and message_id
        2. Auto-discovery: Requires team_id, channel_id, and fetch_all_messages="true"

        Args:
            start_offset: Dictionary with cursor (e.g., {"cursor": "2025-01-15T10:30:00.000Z"})
            table_options: Must include team_id and channel_id, plus either message_id or fetch_all_messages

        Returns:
            Tuple of (iterator of reply records, next_offset dict with updated cursor)

        Raises:
            ValueError: If required options are missing
        """
        team_id = table_options.get("team_id")
        channel_id = table_options.get("channel_id")
        message_id = table_options.get("message_id")
        fetch_all_messages = table_options.get("fetch_all_messages", "").lower() == "true"

        # Validate inputs
        if not team_id or not channel_id:
            raise ValueError(
                "table_options for 'message_replies' must include 'team_id' and 'channel_id'"
            )

        if not message_id and not fetch_all_messages:
            raise ValueError(
                "table_options for 'message_replies' must include either 'message_id' "
                "or 'fetch_all_messages=true'"
            )

        # Parse options
        try:
            top = int(table_options.get("top", 50))
        except (TypeError, ValueError):
            top = 50
        top = max(1, min(top, 50))  # Max 50 for replies endpoint

        try:
            max_pages = int(table_options.get("max_pages_per_batch", 100))
        except (TypeError, ValueError):
            max_pages = 100

        try:
            lookback_seconds = int(table_options.get("lookback_seconds", 300))
        except (TypeError, ValueError):
            lookback_seconds = 300

        # Determine starting cursor
        cursor = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
        if not cursor:
            cursor = table_options.get("start_date")

        # If fetch_all_messages mode, first discover all message IDs in the channel
        message_ids_to_process = []
        if fetch_all_messages:
            # Fetch all messages from the channel to get message IDs
            messages_url = f"{self.base_url}/teams/{team_id}/channels/{channel_id}/messages"
            messages_params = {"$top": 50, "$select": "id"}
            messages_next_url: str | None = messages_url
            msg_pages_fetched = 0

            while messages_next_url and msg_pages_fetched < max_pages:
                if msg_pages_fetched == 0:
                    msg_data = self._make_request_with_retry(messages_url, params=messages_params)
                else:
                    msg_data = self._make_request_with_retry(messages_next_url)

                messages = msg_data.get("value", [])
                for msg in messages:
                    msg_id = msg.get("id")
                    if msg_id:
                        message_ids_to_process.append(msg_id)

                messages_next_url = msg_data.get("@odata.nextLink")
                msg_pages_fetched += 1

                if messages_next_url:
                    time.sleep(0.1)
        else:
            # Single message mode
            message_ids_to_process = [message_id]

        # Now fetch replies for all discovered messages
        records: List[dict[str, Any]] = []
        max_modified: str | None = None

        for current_message_id in message_ids_to_process:
            # Build request URL for this message's replies
            url = f"{self.base_url}/teams/{team_id}/channels/{channel_id}/messages/{current_message_id}/replies"
            params = {"$top": top}

            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._make_request_with_retry(url, params=params)
                    else:
                        data = self._make_request_with_retry(next_url)

                    replies = data.get("value", [])

                    for reply in replies:
                        # Filter by cursor (client-side since API doesn't support $filter on /replies)
                        last_modified = reply.get("lastModifiedDateTime")
                        if cursor and last_modified and last_modified < cursor:
                            continue  # Skip replies before cursor

                        # Add connector-derived fields
                        record: dict[str, Any] = dict(reply)
                        record["parent_message_id"] = current_message_id
                        record["team_id"] = team_id
                        record["channel_id"] = channel_id

                        # Serialize complex objects to JSON strings
                        if "policyViolation" in record and isinstance(
                            record["policyViolation"], dict
                        ):
                            record["policyViolation"] = json.dumps(record["policyViolation"])

                        if "eventDetail" in record and isinstance(record["eventDetail"], dict):
                            record["eventDetail"] = json.dumps(record["eventDetail"])

                        if "messageHistory" in record and isinstance(
                            record["messageHistory"], list
                        ):
                            record["messageHistory"] = json.dumps(record["messageHistory"])

                        records.append(record)

                        # Track max timestamp
                        if last_modified:
                            if max_modified is None or last_modified > max_modified:
                                max_modified = last_modified

                    # Handle pagination
                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1

                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    # If a message has no replies or is inaccessible, log and continue
                    # (some messages might be deleted or inaccessible)
                    if "404" not in str(e):
                        # Only raise if it's not a 404 (no replies)
                        raise
                    # Continue to next message on 404
                    break

        # Compute next cursor with lookback window
        next_cursor = cursor
        if max_modified:
            try:
                # Parse ISO 8601 timestamp
                dt = datetime.fromisoformat(max_modified.replace("Z", "+00:00"))
                dt_with_lookback = dt - timedelta(seconds=lookback_seconds)
                next_cursor = dt_with_lookback.isoformat().replace("+00:00", "Z")
            except Exception:
                # Fallback: use max_modified as-is
                next_cursor = max_modified

        next_offset = {"cursor": next_cursor} if next_cursor else {}
        return iter(records), next_offset


def register_lakeflow_source(spark):
    """
    Register the Microsoft Teams connector with the Lakeflow framework.

    Note: This is a placeholder function for compatibility with the source_loader.
    The actual registration happens through the ingestion pipeline.

    Args:
        spark: The Spark session
    """
    # No-op: The connector is used directly by the ingestion pipeline
    # which doesn't require explicit registration
    pass
