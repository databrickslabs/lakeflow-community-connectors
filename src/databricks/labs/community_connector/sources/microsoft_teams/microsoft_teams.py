import json
import time
from datetime import datetime, timedelta
from typing import Iterator, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.microsoft_teams.microsoft_teams_schemas import (
    TABLE_SCHEMAS,
    TABLE_METADATA,
    SUPPORTED_TABLES,
)
from databricks.labs.community_connector.sources.microsoft_teams.microsoft_teams_utils import (
    MicrosoftGraphClient,
    fetch_all_team_ids,
    fetch_all_channel_ids,
    fetch_all_message_ids,
    serialize_complex_fields,
    parse_int_option,
    compute_next_cursor,
    get_cursor_from_offset,
    resolve_team_ids,
    resolve_team_channel_pairs,
)


# Fields that need JSON serialization in team records
_TEAM_SETTINGS_FIELDS = ["memberSettings", "guestSettings", "messagingSettings", "funSettings"]

# Fields that need JSON serialization in message/reply records
_MESSAGE_COMPLEX_FIELDS = ["policyViolation", "eventDetail", "messageHistory"]


class MicrosoftTeamsLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize Microsoft Teams connector with OAuth 2.0 credentials.

        Required options (stored in UC Connection properties):
          - tenant_id: Azure AD tenant ID
          - client_id: Application (client) ID
          - client_secret: Client secret value
        """
        self._client = MicrosoftGraphClient(
            tenant_id=options.get("tenant_id"),
            client_id=options.get("client_id"),
            client_secret=options.get("client_secret"),
        )

    def list_tables(self) -> list[str]:
        return SUPPORTED_TABLES

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {SUPPORTED_TABLES}"
            )
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {SUPPORTED_TABLES}"
            )

        config = TABLE_METADATA[table_name]
        metadata = {
            "primary_keys": config["primary_keys"],
            "ingestion_type": config["ingestion_type"],
        }
        if "cursor_field" in config:
            metadata["cursor_field"] = config["cursor_field"]
        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables: {SUPPORTED_TABLES}"
            )

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

    # =========================================================================
    # Table-Specific Read Methods
    # =========================================================================

    def _read_teams(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read teams table (snapshot mode)."""
        top = parse_int_option(table_options, "top", 50)
        top = max(1, min(top, 999))
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)

        endpoint = TABLE_METADATA["teams"]["endpoint"]
        url = f"{self._client.base_url}/{endpoint}"
        params = {"$top": top}

        records: list[dict[str, Any]] = []
        pages_fetched = 0
        next_url: str | None = url

        while next_url and pages_fetched < max_pages:
            if pages_fetched == 0:
                data = self._client.make_request(url, params=params)
            else:
                data = self._client.make_request(next_url)

            for team in data.get("value", []):
                record: dict[str, Any] = dict(team)
                serialize_complex_fields(record, _TEAM_SETTINGS_FIELDS)
                records.append(record)

            next_url = data.get("@odata.nextLink")
            pages_fetched += 1
            if next_url:
                time.sleep(0.1)

        return iter(records), {}

    def _read_channels(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read channels table (snapshot mode)."""
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)
        team_ids = resolve_team_ids(self._client, table_options, "channels", max_pages)

        records: list[dict[str, Any]] = []

        for current_team_id in team_ids:
            url = f"{self._client.base_url}/teams/{current_team_id}/channels"
            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._client.make_request(url, params={})
                    else:
                        data = self._client.make_request(next_url)

                    for channel in data.get("value", []):
                        record: dict[str, Any] = dict(channel)
                        record["team_id"] = current_team_id
                        records.append(record)

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1
                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    if "404" not in str(e) and "403" not in str(e):
                        raise
                    break

        return iter(records), {}

    def _read_members(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read members table (snapshot mode)."""
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)
        team_ids = resolve_team_ids(self._client, table_options, "members", max_pages)

        records: list[dict[str, Any]] = []

        for current_team_id in team_ids:
            url = f"{self._client.base_url}/teams/{current_team_id}/members"
            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._client.make_request(url, params={})
                    else:
                        data = self._client.make_request(next_url)

                    for member in data.get("value", []):
                        record: dict[str, Any] = dict(member)
                        record["team_id"] = current_team_id
                        records.append(record)

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1
                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    if "404" not in str(e) and "403" not in str(e):
                        raise
                    break

        return iter(records), {}

    def _read_messages(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages table - routes to Delta API or legacy implementation."""
        use_delta_api = table_options.get("use_delta_api", "true").lower() == "true"

        if use_delta_api:
            return self._read_messages_delta(start_offset, table_options)
        else:
            return self._read_messages_legacy(start_offset, table_options)

    def _read_messages_delta(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages using Microsoft Graph Delta API (server-side filtering)."""
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)
        pairs = resolve_team_channel_pairs(
            self._client, table_options, "messages", max_pages
        )

        records = []
        delta_links = {}

        for current_team_id, current_channel_id in pairs:
            channel_key = f"{current_team_id}/{current_channel_id}"

            delta_link = None
            if start_offset and isinstance(start_offset, dict):
                delta_link = start_offset.get("deltaLinks", {}).get(channel_key)

            if delta_link:
                url = delta_link
            else:
                url = f"{self._client.base_url}/teams/{current_team_id}/channels/{current_channel_id}/messages/delta"

            pages_fetched = 0
            next_url = url

            try:
                while next_url and pages_fetched < max_pages:
                    data = self._client.make_request(next_url)
                    messages = data.get("value", [])

                    for msg in messages:
                        if "@removed" in msg:
                            record = {
                                "id": msg["id"],
                                "team_id": current_team_id,
                                "channel_id": current_channel_id,
                                "_deleted": True,
                                "lastModifiedDateTime": datetime.now().isoformat().replace("+00:00", "Z"),
                            }
                        else:
                            record = dict(msg)
                            record["team_id"] = current_team_id
                            record["channel_id"] = current_channel_id
                            serialize_complex_fields(record, _MESSAGE_COMPLEX_FIELDS)

                        records.append(record)

                    delta_link_new = data.get("@odata.deltaLink")
                    next_link = data.get("@odata.nextLink")

                    if delta_link_new:
                        delta_links[channel_key] = delta_link_new
                        break
                    elif next_link:
                        next_url = next_link
                        pages_fetched += 1
                        time.sleep(0.1)
                    else:
                        break

            except Exception as e:
                if "404" not in str(e) and "403" not in str(e):
                    raise
                continue

        next_offset = {"deltaLinks": delta_links} if delta_links else {}
        return iter(records), next_offset

    def _read_messages_legacy(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages with client-side timestamp filtering (legacy mode)."""
        top = parse_int_option(table_options, "top", 50)
        top = max(1, min(top, 50))
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)
        lookback_seconds = parse_int_option(table_options, "lookback_seconds", 300)
        cursor = get_cursor_from_offset(start_offset, table_options)

        pairs = resolve_team_channel_pairs(
            self._client, table_options, "messages", max_pages
        )

        records: list[dict[str, Any]] = []
        max_modified: str | None = None

        for current_team_id, current_channel_id in pairs:
            url = f"{self._client.base_url}/teams/{current_team_id}/channels/{current_channel_id}/messages"
            params = {"$top": top}

            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._client.make_request(url, params=params)
                    else:
                        data = self._client.make_request(next_url)

                    for msg in data.get("value", []):
                        last_modified = msg.get("lastModifiedDateTime")
                        if cursor and last_modified and last_modified < cursor:
                            continue

                        record: dict[str, Any] = dict(msg)
                        record["team_id"] = current_team_id
                        record["channel_id"] = current_channel_id
                        serialize_complex_fields(record, _MESSAGE_COMPLEX_FIELDS)
                        records.append(record)

                        if last_modified:
                            if max_modified is None or last_modified > max_modified:
                                max_modified = last_modified

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1
                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    if "404" not in str(e) and "403" not in str(e):
                        raise
                    break

        next_cursor = compute_next_cursor(max_modified, cursor, lookback_seconds)
        next_offset = {"cursor": next_cursor} if next_cursor else {}
        return iter(records), next_offset

    def _read_message_replies(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read message replies - routes to Delta API or legacy implementation."""
        use_delta_api = table_options.get("use_delta_api", "false").lower() == "true"

        if use_delta_api:
            return self._read_message_replies_delta(start_offset, table_options)
        else:
            return self._read_message_replies_legacy(start_offset, table_options)

    def _read_message_replies_delta(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read message replies using Microsoft Graph Delta API."""
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)
        pairs = resolve_team_channel_pairs(
            self._client, table_options, "message_replies", max_pages
        )

        message_id = table_options.get("message_id")
        fetch_all_messages = table_options.get("fetch_all_messages", "").lower() == "true"

        if not message_id and not fetch_all_messages:
            raise ValueError(
                "table_options for 'message_replies' must include either 'message_id' "
                "or 'fetch_all_messages=true'"
            )

        all_triples = []
        for current_team_id, current_channel_id in pairs:
            if fetch_all_messages:
                msg_ids = fetch_all_message_ids(
                    self._client, current_team_id, current_channel_id, max_pages
                )
                if not msg_ids:
                    continue
            else:
                msg_ids = [message_id]
            for msg_id in msg_ids:
                all_triples.append((current_team_id, current_channel_id, msg_id))

        records: list[dict[str, Any]] = []
        delta_links = {}

        for current_team_id, current_channel_id, current_message_id in all_triples:
            message_key = f"{current_team_id}/{current_channel_id}/{current_message_id}"

            delta_link = start_offset.get("deltaLinks", {}).get(message_key) if start_offset else None

            if delta_link:
                url = delta_link
            else:
                url = (
                    f"{self._client.base_url}/teams/{current_team_id}/channels/"
                    f"{current_channel_id}/messages/{current_message_id}/replies/delta"
                )

            pages_fetched = 0

            try:
                while url and pages_fetched < max_pages:
                    data = self._client.make_request(url)
                    replies = data.get("value", [])

                    for reply in replies:
                        if "@removed" in reply:
                            record = {
                                "id": reply["id"],
                                "parent_message_id": current_message_id,
                                "team_id": current_team_id,
                                "channel_id": current_channel_id,
                                "_deleted": True,
                                "lastModifiedDateTime": datetime.now().isoformat().replace("+00:00", "Z"),
                            }
                        else:
                            record: dict[str, Any] = dict(reply)
                            record["parent_message_id"] = current_message_id
                            record["team_id"] = current_team_id
                            record["channel_id"] = current_channel_id
                            serialize_complex_fields(record, _MESSAGE_COMPLEX_FIELDS)

                        records.append(record)

                    delta_link_new = data.get("@odata.deltaLink")
                    if delta_link_new:
                        delta_links[message_key] = delta_link_new
                        break

                    url = data.get("@odata.nextLink")
                    pages_fetched += 1
                    if url:
                        time.sleep(0.1)

            except Exception as e:
                if "404" not in str(e) and "403" not in str(e):
                    raise

        next_offset = {"deltaLinks": delta_links} if delta_links else {}
        return iter(records), next_offset

    def _read_message_replies_legacy(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read message replies with client-side timestamp filtering (legacy mode)."""
        top = parse_int_option(table_options, "top", 50)
        top = max(1, min(top, 50))
        max_pages = parse_int_option(table_options, "max_pages_per_batch", 100)
        lookback_seconds = parse_int_option(table_options, "lookback_seconds", 300)
        cursor = get_cursor_from_offset(start_offset, table_options)

        pairs = resolve_team_channel_pairs(
            self._client, table_options, "message_replies", max_pages
        )

        message_id = table_options.get("message_id")
        fetch_all_messages = table_options.get("fetch_all_messages", "").lower() == "true"

        if not message_id and not fetch_all_messages:
            raise ValueError(
                "table_options for 'message_replies' must include either 'message_id' "
                "or 'fetch_all_messages=true'"
            )

        all_triples = []
        for current_team_id, current_channel_id in pairs:
            if fetch_all_messages:
                msg_ids = fetch_all_message_ids(
                    self._client, current_team_id, current_channel_id, max_pages
                )
                if not msg_ids:
                    continue
            else:
                msg_ids = [message_id]
            for msg_id in msg_ids:
                all_triples.append((current_team_id, current_channel_id, msg_id))

        max_workers = parse_int_option(table_options, "max_concurrent_threads", 10)

        records: list[dict[str, Any]] = []
        max_modified: str | None = None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_replies_for_message,
                    tid, cid, mid, cursor, top, max_pages
                ): (tid, cid, mid)
                for tid, cid, mid in all_triples
            }

            for future in as_completed(futures):
                try:
                    reply_records, reply_max_modified = future.result()
                    records.extend(reply_records)

                    if reply_max_modified:
                        if max_modified is None or reply_max_modified > max_modified:
                            max_modified = reply_max_modified

                except Exception as e:
                    if "404" not in str(e) and "403" not in str(e):
                        raise

        next_cursor = compute_next_cursor(max_modified, cursor, lookback_seconds)
        next_offset = {"cursor": next_cursor} if next_cursor else {}
        return iter(records), next_offset

    def _fetch_replies_for_message(
        self,
        team_id: str,
        channel_id: str,
        message_id: str,
        cursor: str | None,
        top: int,
        max_pages: int,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch replies for a single message (helper for parallel execution)."""
        records: list[dict[str, Any]] = []
        max_modified: str | None = None

        url = f"{self._client.base_url}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"
        params = {"$top": top}

        pages_fetched = 0
        next_url: str | None = url

        try:
            while next_url and pages_fetched < max_pages:
                if pages_fetched == 0:
                    data = self._client.make_request(url, params=params)
                else:
                    data = self._client.make_request(next_url)

                for reply in data.get("value", []):
                    last_modified = reply.get("lastModifiedDateTime")
                    if cursor and last_modified and last_modified < cursor:
                        continue

                    record: dict[str, Any] = dict(reply)
                    record["parent_message_id"] = message_id
                    record["team_id"] = team_id
                    record["channel_id"] = channel_id
                    serialize_complex_fields(record, _MESSAGE_COMPLEX_FIELDS)
                    records.append(record)

                    if last_modified:
                        if max_modified is None or last_modified > max_modified:
                            max_modified = last_modified

                next_url = data.get("@odata.nextLink")
                pages_fetched += 1
                if next_url:
                    time.sleep(0.1)

        except Exception as e:
            if "404" not in str(e):
                raise

        return records, max_modified
