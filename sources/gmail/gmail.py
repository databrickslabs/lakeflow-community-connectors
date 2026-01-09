from typing import Iterator, Dict
import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Gmail connector with OAuth 2.0 credentials.

        Expected options:
            - client_id: OAuth 2.0 client ID
            - client_secret: OAuth 2.0 client secret
            - refresh_token: OAuth 2.0 refresh token
            - user_id (optional): Gmail user ID. Defaults to 'me' (authenticated user).
        """
        client_id = options.get("client_id")
        client_secret = options.get("client_secret")
        refresh_token = options.get("refresh_token")

        if not client_id or not client_secret or not refresh_token:
            raise ValueError(
                "Gmail connector requires 'client_id', 'client_secret', "
                "and 'refresh_token' in options"
            )

        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.user_id = options.get("user_id", "me")
        self.base_url = "https://gmail.googleapis.com/gmail/v1"

        # Initialize session and obtain access token
        self._session = requests.Session()
        self._access_token = None
        self._refresh_access_token()

    def _refresh_access_token(self) -> None:
        """
        Exchange refresh token for an access token using OAuth 2.0.
        """
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }

        response = requests.post(token_url, data=data)
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]

        # Update session headers with new access token
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._access_token}",
                "Accept": "application/json",
            }
        )

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make an HTTP request with automatic token refresh on 401 errors.
        """
        response = self._session.request(method, url, **kwargs)

        # If unauthorized, refresh token and retry once
        if response.status_code == 401:
            self._refresh_access_token()
            response = self._session.request(method, url, **kwargs)

        response.raise_for_status()
        return response

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        """
        return ["messages", "labels", "threads", "drafts", "profile"]

    def _get_header_struct(self) -> StructType:
        """Return the nested header struct schema."""
        return StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

    def _get_message_part_body_struct(self) -> StructType:
        """Return the message part body struct schema."""
        return StructType(
            [
                StructField("attachmentId", StringType(), True),
                StructField("size", LongType(), True),
                StructField("data", StringType(), True),
            ]
        )

    def _get_message_part_struct(self) -> StructType:
        """Return the nested message part struct schema (recursive structure)."""
        header_struct = self._get_header_struct()
        body_struct = self._get_message_part_body_struct()

        # Define the structure without the recursive 'parts' field first
        base_fields = [
            StructField("partId", StringType(), True),
            StructField("mimeType", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("headers", ArrayType(header_struct, True), True),
            StructField("body", body_struct, True),
        ]

        # For recursive parts, we'll use StringType to avoid infinite recursion
        # The actual nested parts will be preserved in the JSON
        base_fields.append(StructField("parts", StringType(), True))

        return StructType(base_fields)

    def _get_messages_schema(self) -> StructType:
        """Return the messages table schema."""
        message_part_struct = self._get_message_part_struct()

        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("threadId", StringType(), True),
                StructField("labelIds", ArrayType(StringType(), True), True),
                StructField("snippet", StringType(), True),
                StructField("historyId", StringType(), True),
                StructField("internalDate", StringType(), True),
                StructField("sizeEstimate", LongType(), True),
                StructField("raw", StringType(), True),
                StructField("payload", message_part_struct, True),
            ]
        )

    def _get_color_struct(self) -> StructType:
        """Return the color struct schema for labels."""
        return StructType(
            [
                StructField("textColor", StringType(), True),
                StructField("backgroundColor", StringType(), True),
            ]
        )

    def _get_labels_schema(self) -> StructType:
        """Return the labels table schema."""
        color_struct = self._get_color_struct()

        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("type", StringType(), True),
                StructField("messageListVisibility", StringType(), True),
                StructField("labelListVisibility", StringType(), True),
                StructField("messagesTotal", LongType(), True),
                StructField("messagesUnread", LongType(), True),
                StructField("threadsTotal", LongType(), True),
                StructField("threadsUnread", LongType(), True),
                StructField("color", color_struct, True),
            ]
        )

    def _get_threads_schema(self) -> StructType:
        """Return the threads table schema."""
        # For messages array in threads, we reference the full message schema
        message_struct = self._get_messages_schema()

        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("snippet", StringType(), True),
                StructField("historyId", StringType(), True),
                StructField("messages", ArrayType(message_struct, True), True),
            ]
        )

    def _get_drafts_schema(self) -> StructType:
        """Return the drafts table schema."""
        message_struct = self._get_messages_schema()

        return StructType(
            [
                StructField("id", StringType(), False),
                StructField("message", message_struct, True),
            ]
        )

    def _get_profile_schema(self) -> StructType:
        """Return the profile table schema."""
        return StructType(
            [
                StructField("emailAddress", StringType(), False),
                StructField("messagesTotal", LongType(), True),
                StructField("threadsTotal", LongType(), True),
                StructField("historyId", StringType(), True),
            ]
        )

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        if table_name == "messages":
            return self._get_messages_schema()
        elif table_name == "labels":
            return self._get_labels_schema()
        elif table_name == "threads":
            return self._get_threads_schema()
        elif table_name == "drafts":
            return self._get_drafts_schema()
        elif table_name == "profile":
            return self._get_profile_schema()
        else:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(self.list_tables())}"
            )

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(self.list_tables())}"
            )

        if table_name == "messages":
            return {
                "primary_keys": ["id"],
                "cursor_field": "historyId",
                "ingestion_type": "cdc",
            }
        elif table_name == "threads":
            return {
                "primary_keys": ["id"],
                "cursor_field": "historyId",
                "ingestion_type": "cdc",
            }
        elif table_name == "drafts":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }
        elif table_name == "labels":
            return {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            }
        elif table_name == "profile":
            return {
                "primary_keys": ["emailAddress"],
                "ingestion_type": "snapshot",
            }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the records of a table and return an iterator of records and an offset.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {', '.join(self.list_tables())}"
            )

        if table_name == "messages":
            return self._read_messages(start_offset, table_options)
        elif table_name == "labels":
            return self._read_labels(start_offset, table_options)
        elif table_name == "threads":
            return self._read_threads(start_offset, table_options)
        elif table_name == "drafts":
            return self._read_drafts(start_offset, table_options)
        elif table_name == "profile":
            return self._read_profile(start_offset, table_options)

    def _read_messages(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read messages using full sync or incremental sync via history API.
        """
        # Check if this is an incremental sync
        if start_offset and "historyId" in start_offset:
            return self._read_messages_incremental(start_offset, table_options)
        else:
            return self._read_messages_full(start_offset, table_options)

    def _read_messages_full(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Full sync: List all message IDs and fetch full message details.
        """
        max_results = 500  # Maximum allowed by Gmail API
        page_token = start_offset.get("pageToken") if start_offset else None

        # Optional filters from table_options
        query = table_options.get("q")  # Gmail search query
        label_ids = table_options.get("labelIds")  # Comma-separated label IDs
        include_spam_trash = table_options.get("includeSpamTrash", "false").lower() == "true"

        # Build query parameters
        params = {"maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token
        if query:
            params["q"] = query
        if label_ids:
            # Split comma-separated label IDs
            for label_id in label_ids.split(","):
                params.setdefault("labelIds", []).append(label_id.strip())
        if include_spam_trash:
            params["includeSpamTrash"] = "true"

        # List message IDs
        url = f"{self.base_url}/users/{self.user_id}/messages"
        response = self._make_request("GET", url, params=params)
        data = response.json()

        messages = data.get("messages", [])
        next_page_token = data.get("nextPageToken")

        # Track the maximum historyId seen for incremental sync
        max_history_id = None

        def message_generator():
            nonlocal max_history_id
            for msg_ref in messages:
                msg_id = msg_ref["id"]

                # Fetch full message details
                msg_url = f"{self.base_url}/users/{self.user_id}/messages/{msg_id}"
                msg_params = {"format": "full"}
                msg_response = self._make_request("GET", msg_url, params=msg_params)
                message = msg_response.json()

                # Track max historyId
                history_id = message.get("historyId")
                if history_id:
                    if max_history_id is None or history_id > max_history_id:
                        max_history_id = history_id

                yield message

        # Determine next offset
        if next_page_token:
            # More pages to fetch in full sync
            next_offset = {"pageToken": next_page_token}
        elif max_history_id:
            # Full sync complete, transition to incremental sync
            next_offset = {"historyId": max_history_id}
        else:
            # No messages or no history tracking
            next_offset = {}

        return message_generator(), next_offset

    def _read_messages_incremental(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Incremental sync: Use history API to fetch changes since last sync.
        """
        start_history_id = start_offset["historyId"]
        max_results = 500
        page_token = start_offset.get("pageToken")

        # Optional filters from table_options
        label_id = table_options.get("labelId")  # Single label ID filter
        history_types = table_options.get("historyTypes")  # Comma-separated types

        # Build query parameters
        params = {
            "startHistoryId": start_history_id,
            "maxResults": max_results,
        }
        if page_token:
            params["pageToken"] = page_token
        if label_id:
            params["labelId"] = label_id
        if history_types:
            for hist_type in history_types.split(","):
                params.setdefault("historyTypes", []).append(hist_type.strip())

        # Fetch history changes
        url = f"{self.base_url}/users/{self.user_id}/history"

        try:
            response = self._make_request("GET", url, params=params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                # History ID too old, fall back to full sync
                return self._read_messages_full({}, table_options)
            raise

        data = response.json()
        history = data.get("history", [])
        next_page_token = data.get("nextPageToken")
        current_history_id = data.get("historyId", start_history_id)

        # Collect unique message IDs that were added or modified
        message_ids = set()
        for record in history:
            # messagesAdded
            for msg_added in record.get("messagesAdded", []):
                message_ids.add(msg_added["message"]["id"])

            # labelsAdded or labelsRemoved (message was modified)
            for label_change in record.get("labelsAdded", []) + record.get("labelsRemoved", []):
                message_ids.add(label_change["message"]["id"])

        def message_generator():
            for msg_id in message_ids:
                # Fetch full message details
                msg_url = f"{self.base_url}/users/{self.user_id}/messages/{msg_id}"
                msg_params = {"format": "full"}
                msg_response = self._make_request("GET", msg_url, params=msg_params)
                message = msg_response.json()
                yield message

        # Determine next offset
        if next_page_token:
            # More history pages to fetch
            next_offset = {
                "historyId": start_history_id,
                "pageToken": next_page_token,
            }
        else:
            # History sync complete, update historyId
            next_offset = {"historyId": current_history_id}

        return message_generator(), next_offset

    def _read_labels(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read all labels (snapshot mode, no pagination).
        """
        url = f"{self.base_url}/users/{self.user_id}/labels"
        response = self._make_request("GET", url)
        data = response.json()

        labels = data.get("labels", [])

        def label_generator():
            for label in labels:
                yield label

        # Snapshot mode - no offset tracking
        return label_generator(), {}

    def _read_threads(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read threads using full sync or incremental sync via history API.
        """
        # Check if this is an incremental sync
        if start_offset and "historyId" in start_offset:
            return self._read_threads_incremental(start_offset, table_options)
        else:
            return self._read_threads_full(start_offset, table_options)

    def _read_threads_full(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Full sync: List all thread IDs and fetch full thread details.
        """
        max_results = 500
        page_token = start_offset.get("pageToken") if start_offset else None

        # Optional filters from table_options
        query = table_options.get("q")
        label_ids = table_options.get("labelIds")
        include_spam_trash = table_options.get("includeSpamTrash", "false").lower() == "true"

        # Build query parameters
        params = {"maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token
        if query:
            params["q"] = query
        if label_ids:
            for label_id in label_ids.split(","):
                params.setdefault("labelIds", []).append(label_id.strip())
        if include_spam_trash:
            params["includeSpamTrash"] = "true"

        # List thread IDs
        url = f"{self.base_url}/users/{self.user_id}/threads"
        response = self._make_request("GET", url, params=params)
        data = response.json()

        threads = data.get("threads", [])
        next_page_token = data.get("nextPageToken")

        max_history_id = None

        def thread_generator():
            nonlocal max_history_id
            for thread_ref in threads:
                thread_id = thread_ref["id"]

                # Fetch full thread details
                thread_url = f"{self.base_url}/users/{self.user_id}/threads/{thread_id}"
                thread_params = {"format": "full"}
                thread_response = self._make_request("GET", thread_url, params=thread_params)
                thread = thread_response.json()

                # Track max historyId
                history_id = thread.get("historyId")
                if history_id:
                    if max_history_id is None or history_id > max_history_id:
                        max_history_id = history_id

                yield thread

        # Determine next offset
        if next_page_token:
            next_offset = {"pageToken": next_page_token}
        elif max_history_id:
            next_offset = {"historyId": max_history_id}
        else:
            next_offset = {}

        return thread_generator(), next_offset

    def _read_threads_incremental(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Incremental sync: Use history API to identify changed threads.
        """
        start_history_id = start_offset["historyId"]
        max_results = 500
        page_token = start_offset.get("pageToken")

        # Optional filters from table_options
        label_id = table_options.get("labelId")
        history_types = table_options.get("historyTypes")

        # Build query parameters
        params = {
            "startHistoryId": start_history_id,
            "maxResults": max_results,
        }
        if page_token:
            params["pageToken"] = page_token
        if label_id:
            params["labelId"] = label_id
        if history_types:
            for hist_type in history_types.split(","):
                params.setdefault("historyTypes", []).append(hist_type.strip())

        # Fetch history changes
        url = f"{self.base_url}/users/{self.user_id}/history"

        try:
            response = self._make_request("GET", url, params=params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return self._read_threads_full({}, table_options)
            raise

        data = response.json()
        history = data.get("history", [])
        next_page_token = data.get("nextPageToken")
        current_history_id = data.get("historyId", start_history_id)

        # Collect unique thread IDs from history
        thread_ids = set()
        for record in history:
            # Extract thread IDs from all history event types
            for msg_added in record.get("messagesAdded", []):
                thread_ids.add(msg_added["message"]["threadId"])

            for label_change in record.get("labelsAdded", []) + record.get("labelsRemoved", []):
                thread_ids.add(label_change["message"]["threadId"])

            for msg_deleted in record.get("messagesDeleted", []):
                thread_ids.add(msg_deleted["message"]["threadId"])

        def thread_generator():
            for thread_id in thread_ids:
                # Fetch full thread details
                thread_url = f"{self.base_url}/users/{self.user_id}/threads/{thread_id}"
                thread_params = {"format": "full"}
                thread_response = self._make_request("GET", thread_url, params=thread_params)
                thread = thread_response.json()
                yield thread

        # Determine next offset
        if next_page_token:
            next_offset = {
                "historyId": start_history_id,
                "pageToken": next_page_token,
            }
        else:
            next_offset = {"historyId": current_history_id}

        return thread_generator(), next_offset

    def _read_drafts(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read drafts using snapshot mode (full sync only).
        """
        return self._read_drafts_full(start_offset, table_options)

    def _read_drafts_full(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Full sync: List all draft IDs and fetch full draft details.
        """
        max_results = 500
        page_token = start_offset.get("pageToken") if start_offset else None

        # Build query parameters
        params = {"maxResults": max_results}
        if page_token:
            params["pageToken"] = page_token

        # List draft IDs
        url = f"{self.base_url}/users/{self.user_id}/drafts"
        response = self._make_request("GET", url, params=params)
        data = response.json()

        drafts = data.get("drafts", [])
        next_page_token = data.get("nextPageToken")

        def draft_generator():
            for draft_ref in drafts:
                draft_id = draft_ref["id"]

                # Fetch full draft details
                draft_url = f"{self.base_url}/users/{self.user_id}/drafts/{draft_id}"
                draft_params = {"format": "full"}
                draft_response = self._make_request("GET", draft_url, params=draft_params)
                draft = draft_response.json()

                yield draft

        # Determine next offset (snapshot mode - only pageToken for pagination)
        if next_page_token:
            next_offset = {"pageToken": next_page_token}
        else:
            next_offset = {}

        return draft_generator(), next_offset

    def _read_drafts_incremental(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Incremental sync: Use history API filtered by DRAFT label.
        """
        start_history_id = start_offset["historyId"]
        max_results = 500
        page_token = start_offset.get("pageToken")

        # Build query parameters - filter by DRAFT label
        params = {
            "startHistoryId": start_history_id,
            "maxResults": max_results,
            "labelId": "DRAFT",
        }
        if page_token:
            params["pageToken"] = page_token

        # Fetch history changes
        url = f"{self.base_url}/users/{self.user_id}/history"

        try:
            response = self._make_request("GET", url, params=params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return self._read_drafts_full({}, table_options)
            raise

        data = response.json()
        history = data.get("history", [])
        next_page_token = data.get("nextPageToken")
        current_history_id = data.get("historyId", start_history_id)

        # Collect draft message IDs from history
        draft_message_ids = set()
        for record in history:
            for msg_added in record.get("messagesAdded", []):
                msg = msg_added["message"]
                # Check if message has DRAFT label
                if "DRAFT" in msg.get("labelIds", []):
                    draft_message_ids.add(msg["id"])

            for label_change in record.get("labelsAdded", []) + record.get("labelsRemoved", []):
                msg = label_change["message"]
                if "DRAFT" in msg.get("labelIds", []):
                    draft_message_ids.add(msg["id"])

        def draft_generator():
            # Get current drafts list to find draft IDs
            drafts_url = f"{self.base_url}/users/{self.user_id}/drafts"
            drafts_response = self._make_request("GET", drafts_url)
            drafts_data = drafts_response.json()
            all_drafts = drafts_data.get("drafts", [])

            # Match draft message IDs with draft IDs
            for draft_ref in all_drafts:
                draft_id = draft_ref["id"]

                # Fetch full draft to get message ID
                draft_url = f"{self.base_url}/users/{self.user_id}/drafts/{draft_id}"
                draft_params = {"format": "full"}
                draft_response = self._make_request("GET", draft_url, params=draft_params)
                draft = draft_response.json()

                # Check if this draft's message ID is in our changed set
                if "message" in draft and draft["message"].get("id") in draft_message_ids:
                    yield draft

        # Determine next offset
        if next_page_token:
            next_offset = {
                "historyId": start_history_id,
                "pageToken": next_page_token,
            }
        else:
            next_offset = {"historyId": current_history_id}

        return draft_generator(), next_offset

    def _read_profile(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read user profile (snapshot mode, single record).
        """
        url = f"{self.base_url}/users/{self.user_id}/profile"
        response = self._make_request("GET", url)
        profile = response.json()

        def profile_generator():
            yield profile

        # Snapshot mode - no offset tracking
        return profile_generator(), {}
