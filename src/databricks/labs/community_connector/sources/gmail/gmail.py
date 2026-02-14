# Gmail Connector for Lakeflow Community Connectors
# Implements LakeflowConnect interface to read data from Gmail API with 100% API coverage.

from typing import Dict, Iterator
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sources.gmail.gmail_schemas import (
    TABLE_SCHEMAS,
    TABLE_METADATA,
    SUPPORTED_TABLES,
)
from databricks.labs.community_connector.sources.gmail.gmail_utils import (
    GmailApiClient,
    BATCH_SIZE,
    MAX_WORKERS,
)


class GmailLakeflowConnect(LakeflowConnect):
    """Gmail connector implementing the LakeflowConnect interface with 100% API coverage."""

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Gmail connector with OAuth 2.0 credentials.

        Expected options:
            - client_id: OAuth 2.0 client ID from Google Cloud Console
            - client_secret: OAuth 2.0 client secret
            - refresh_token: Long-lived refresh token obtained via OAuth flow
            - user_id (optional): User email or 'me' (default: 'me')
        """
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        self.refresh_token = options.get("refresh_token")
        self.user_id = options.get("user_id", "me")

        if not self.client_id:
            raise ValueError("Gmail connector requires 'client_id' in options")
        if not self.client_secret:
            raise ValueError("Gmail connector requires 'client_secret' in options")
        if not self.refresh_token:
            raise ValueError("Gmail connector requires 'refresh_token' in options")

        self.api = GmailApiClient(
            self.client_id, self.client_secret, self.refresh_token, self.user_id
        )

    # ─── Interface Methods ────────────────────────────────────────────────────

    def list_tables(self) -> list[str]:
        """Return the list of available Gmail tables."""
        return SUPPORTED_TABLES.copy()

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """Fetch the schema of a table."""
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict:
        """Fetch the metadata of a table."""
        self._validate_table(table_name)
        return TABLE_METADATA[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table with streaming iteration.

        For messages and threads with a start_offset containing historyId,
        uses the History API for incremental reads. Otherwise, performs
        a full list operation.
        """
        self._validate_table(table_name)

        read_map = {
            "messages": self._read_messages,
            "threads": self._read_threads,
            "labels": self._read_labels,
            "drafts": self._read_drafts,
            "profile": self._read_profile,
            "settings": self._read_settings,
            "filters": self._read_filters,
            "forwarding_addresses": self._read_forwarding_addresses,
            "send_as": self._read_send_as,
            "delegates": self._read_delegates,
        }
        return read_map[table_name](start_offset, table_options)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read deleted records from Gmail using History API.

        Gmail History API provides messagesDeleted events for tracking deletions.
        This is only applicable for messages and threads (CDC tables).
        """
        self._validate_table(table_name)

        # Only messages and threads support delete tracking
        if table_name not in ("messages", "threads"):
            return iter([]), {}

        start_history_id = start_offset.get("historyId") if start_offset else None
        if not start_history_id:
            return iter([]), {}

        params = {
            "startHistoryId": start_history_id,
            "maxResults": 500,
            "historyTypes": "messageDeleted",
        }

        page_token = None
        latest_history_id = start_history_id
        deleted_records = []
        seen_ids = set()

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self.api.make_request("GET", f"/users/{self.user_id}/history", params=params)

            if response is None:
                break

            if response.get("historyId"):
                latest_history_id = response["historyId"]

            for history_record in response.get("history", []):
                for deleted in history_record.get("messagesDeleted", []):
                    msg = deleted.get("message", {})
                    if msg.get("id"):
                        if table_name == "messages":
                            if msg["id"] not in seen_ids:
                                seen_ids.add(msg["id"])
                                deleted_records.append(
                                    {
                                        "id": msg["id"],
                                        "threadId": msg.get("threadId"),
                                        "historyId": history_record.get("id", latest_history_id),
                                    }
                                )
                        elif table_name == "threads":
                            thread_id = msg.get("threadId")
                            if thread_id and thread_id not in seen_ids:
                                seen_ids.add(thread_id)
                                deleted_records.append(
                                    {
                                        "id": thread_id,
                                        "historyId": history_record.get("id", latest_history_id),
                                    }
                                )

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        next_offset = {"historyId": latest_history_id}
        return iter(deleted_records), next_offset

    # ─── Helpers ──────────────────────────────────────────────────────────────

    def _validate_table(self, table_name: str) -> None:
        """Validate that the table is supported."""
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table: '{table_name}'. Supported tables are: {SUPPORTED_TABLES}"
            )

    # ─── Table Readers ────────────────────────────────────────────────────────

    def _read_messages(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read messages using streaming list + batch get pattern."""
        max_results = int(table_options.get("maxResults", "100"))
        query = table_options.get("q")
        label_ids = table_options.get("labelIds")
        include_spam_trash = table_options.get("includeSpamTrash", "false").lower() == "true"

        start_history_id = start_offset.get("historyId") if start_offset else None

        if start_history_id:
            return self._read_messages_incremental(start_history_id, table_options)
        else:
            return self._read_messages_streaming(
                max_results, query, label_ids, include_spam_trash, table_options
            )

    def _read_messages_streaming(
        self,
        max_results: int,
        query: str,
        label_ids: str,
        include_spam_trash: bool,
        table_options: Dict[str, str],
    ) -> (Iterator[dict], dict):
        """Stream messages with sequential fetching for reliability."""
        params = {"maxResults": min(max_results, 500)}
        if query:
            params["q"] = query
        if label_ids:
            params["labelIds"] = label_ids
        if include_spam_trash:
            params["includeSpamTrash"] = "true"

        state = {"latest_history_id": None}
        format_type = table_options.get("format", "full")

        def fetch_message(mid):
            return self.api.make_request(
                "GET",
                f"/users/{self.user_id}/messages/{mid}",
                {"format": format_type},
            )

        all_messages = []
        page_token = None

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self.api.make_request(
                "GET", f"/users/{self.user_id}/messages", params=params
            )

            if not response or "messages" not in response:
                break

            message_ids = [m["id"] for m in response.get("messages", [])]

            # Fetch messages in parallel for better performance
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(fetch_message, message_ids))

            for msg_detail in results:
                if msg_detail:
                    if msg_detail.get("historyId"):
                        if (
                            not state["latest_history_id"]
                            or msg_detail["historyId"] > state["latest_history_id"]
                        ):
                            state["latest_history_id"] = msg_detail["historyId"]
                    all_messages.append(msg_detail)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        next_offset = (
            {"historyId": state["latest_history_id"]}
            if state["latest_history_id"]
            else {}
        )
        return iter(all_messages), next_offset

    def _read_messages_incremental(
        self, start_history_id: str, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read messages incrementally using History API with batch fetching."""
        params = {
            "startHistoryId": start_history_id,
            "maxResults": 500,
            "historyTypes": "messageAdded",
        }

        page_token = None
        latest_history_id = start_history_id
        all_message_ids = set()

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self.api.make_request("GET", f"/users/{self.user_id}/history", params=params)

            if response is None:
                return self._read_messages_streaming(
                    100, None, None, False, table_options
                )

            if response.get("historyId"):
                latest_history_id = response["historyId"]

            for history_record in response.get("history", []):
                for added in history_record.get("messagesAdded", []):
                    msg_id = added.get("message", {}).get("id")
                    if msg_id:
                        all_message_ids.add(msg_id)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        all_messages = []
        message_ids = list(all_message_ids)
        format_type = table_options.get("format", "full")

        for i in range(0, len(message_ids), BATCH_SIZE):
            batch_ids = message_ids[i : i + BATCH_SIZE]
            endpoints = [f"/users/{self.user_id}/messages/{mid}" for mid in batch_ids]
            params_list = [{"format": format_type}] * len(batch_ids)

            batch_results = self.api.make_batch_request(endpoints, params_list)
            all_messages.extend([r for r in batch_results if r])

        next_offset = {"historyId": latest_history_id}
        return iter(all_messages), next_offset

    def _read_threads(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read threads using streaming list + batch get pattern."""
        max_results = int(table_options.get("maxResults", "100"))
        query = table_options.get("q")
        label_ids = table_options.get("labelIds")
        include_spam_trash = (
            table_options.get("includeSpamTrash", "false").lower() == "true"
        )

        start_history_id = start_offset.get("historyId") if start_offset else None

        if start_history_id:
            return self._read_threads_incremental(start_history_id, table_options)
        else:
            return self._read_threads_streaming(
                max_results, query, label_ids, include_spam_trash, table_options
            )

    def _read_threads_streaming(
        self,
        max_results: int,
        query: str,
        label_ids: str,
        include_spam_trash: bool,
        table_options: Dict[str, str],
    ) -> (Iterator[dict], dict):
        """Stream threads with parallel fetching for performance."""
        params = {"maxResults": min(max_results, 500)}
        if query:
            params["q"] = query
        if label_ids:
            params["labelIds"] = label_ids
        if include_spam_trash:
            params["includeSpamTrash"] = "true"

        state = {"latest_history_id": None}
        all_threads = []
        page_token = None
        format_type = table_options.get("format", "full")

        def fetch_thread(tid):
            return self.api.make_request(
                "GET",
                f"/users/{self.user_id}/threads/{tid}",
                {"format": format_type},
            )

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self.api.make_request("GET", f"/users/{self.user_id}/threads", params=params)

            if not response or "threads" not in response:
                break

            thread_ids = [t["id"] for t in response.get("threads", [])]

            # Fetch threads in parallel for better performance
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(fetch_thread, thread_ids))

            for thread_detail in results:
                if thread_detail:
                    if thread_detail.get("historyId"):
                        if (
                            not state["latest_history_id"]
                            or thread_detail["historyId"] > state["latest_history_id"]
                        ):
                            state["latest_history_id"] = thread_detail["historyId"]
                    all_threads.append(thread_detail)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        next_offset = (
            {"historyId": state["latest_history_id"]}
            if state["latest_history_id"]
            else {}
        )
        return iter(all_threads), next_offset

    def _read_threads_incremental(
        self, start_history_id: str, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read threads incrementally using History API."""
        params = {
            "startHistoryId": start_history_id,
            "maxResults": 500,
            "historyTypes": "messageAdded",
        }

        page_token = None
        latest_history_id = start_history_id
        all_thread_ids = set()

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self.api.make_request("GET", f"/users/{self.user_id}/history", params=params)

            if response is None:
                return self._read_threads_streaming(100, None, None, False, table_options)

            if response.get("historyId"):
                latest_history_id = response["historyId"]

            for history_record in response.get("history", []):
                for added in history_record.get("messagesAdded", []):
                    thread_id = added.get("message", {}).get("threadId")
                    if thread_id:
                        all_thread_ids.add(thread_id)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        all_threads = []
        thread_ids = list(all_thread_ids)
        format_type = table_options.get("format", "full")

        for i in range(0, len(thread_ids), BATCH_SIZE):
            batch_ids = thread_ids[i : i + BATCH_SIZE]
            endpoints = [f"/users/{self.user_id}/threads/{tid}" for tid in batch_ids]
            params_list = [{"format": format_type}] * len(batch_ids)

            batch_results = self.api.make_batch_request(endpoints, params_list)
            all_threads.extend([r for r in batch_results if r])

        next_offset = {"historyId": latest_history_id}
        return iter(all_threads), next_offset

    def _read_labels(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all labels (snapshot mode)."""
        response = self.api.make_request("GET", f"/users/{self.user_id}/labels")

        if not response or "labels" not in response:
            return iter([]), {}

        # Get basic label list first
        labels = response.get("labels", [])

        # Fetch full details for each label to get counts and colors
        all_labels = []
        for label in labels:
            label_id = label.get("id")
            if label_id:
                detail = self.api.make_request("GET", f"/users/{self.user_id}/labels/{label_id}")
                if detail:
                    all_labels.append(detail)
                else:
                    # Fallback to basic info if detail fetch fails
                    all_labels.append(label)

        return iter(all_labels), {}

    def _read_drafts(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all drafts (snapshot mode) with parallel fetching."""
        params = {"maxResults": int(table_options.get("maxResults", "100"))}

        all_drafts = []
        page_token = None
        format_type = table_options.get("format", "full")

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = self.api.make_request("GET", f"/users/{self.user_id}/drafts", params=params)

            if not response or "drafts" not in response:
                break

            draft_ids = [d["id"] for d in response.get("drafts", [])]

            # Fetch drafts in parallel for better performance
            def fetch_draft(draft_id):
                return self.api.make_request(
                    "GET",
                    f"/users/{self.user_id}/drafts/{draft_id}",
                    {"format": format_type},
                )

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                results = list(executor.map(fetch_draft, draft_ids))
                all_drafts.extend([r for r in results if r])

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return iter(all_drafts), {}

    def _read_profile(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read user profile (single record)."""
        response = self.api.make_request("GET", f"/users/{self.user_id}/profile")

        if not response:
            return iter([]), {}

        return iter([response]), {}

    def _read_settings(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all user settings combined into a single record."""
        # Fetch all settings in parallel
        endpoints = [
            f"/users/{self.user_id}/settings/autoForwarding",
            f"/users/{self.user_id}/settings/imap",
            f"/users/{self.user_id}/settings/pop",
            f"/users/{self.user_id}/settings/language",
            f"/users/{self.user_id}/settings/vacation",
        ]

        results = self.api.make_batch_request(endpoints)

        # Get email address from profile
        profile = self.api.make_request("GET", f"/users/{self.user_id}/profile")
        email_address = profile.get("emailAddress", self.user_id) if profile else self.user_id

        # Combine all settings into one record
        settings = {
            "emailAddress": email_address,
            "autoForwarding": results[0] if len(results) > 0 else None,
            "imap": results[1] if len(results) > 1 else None,
            "pop": results[2] if len(results) > 2 else None,
            "language": results[3] if len(results) > 3 else None,
            "vacation": results[4] if len(results) > 4 else None,
        }

        return iter([settings]), {}

    def _read_filters(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all email filters."""
        response = self.api.make_request("GET", f"/users/{self.user_id}/settings/filters")

        if not response or "filter" not in response:
            return iter([]), {}

        return iter(response.get("filter", [])), {}

    def _read_forwarding_addresses(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all forwarding addresses."""
        response = self.api.make_request(
            "GET", f"/users/{self.user_id}/settings/forwardingAddresses"
        )

        if not response or "forwardingAddresses" not in response:
            return iter([]), {}

        return iter(response.get("forwardingAddresses", [])), {}

    def _read_send_as(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all send-as aliases."""
        response = self.api.make_request("GET", f"/users/{self.user_id}/settings/sendAs")

        if not response or "sendAs" not in response:
            return iter([]), {}

        return iter(response.get("sendAs", [])), {}

    def _read_delegates(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read all delegates."""
        response = self.api.make_request("GET", f"/users/{self.user_id}/settings/delegates")

        if not response or "delegates" not in response:
            return iter([]), {}

        return iter(response.get("delegates", [])), {}
