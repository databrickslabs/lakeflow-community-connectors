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
_TEAM_SETTINGS_FIELDS = [
    "memberSettings", "guestSettings",
    "messagingSettings", "funSettings",
]

# Fields that need JSON serialization in message/reply records
_MESSAGE_COMPLEX_FIELDS = [
    "policyViolation", "eventDetail", "messageHistory",
]


def _build_deleted_record(
    record_id: str, team_id: str, channel_id: str,
) -> dict[str, Any]:
    """Build a record representing a deleted entity."""
    return {
        "id": record_id,
        "team_id": team_id,
        "channel_id": channel_id,
        "_deleted": True,
        "lastModifiedDateTime": (
            datetime.now().isoformat().replace("+00:00", "Z")
        ),
    }


def _resolve_message_triples(
    client: MicrosoftGraphClient,
    pairs: list[tuple[str, str]],
    table_options: dict[str, str],
    max_pages: int,
) -> list[tuple[str, str, str]]:
    """Resolve (team_id, channel_id, message_id) triples."""
    message_id = table_options.get("message_id")
    fetch_all = (
        table_options.get("fetch_all_messages", "").lower() == "true"
    )

    if not message_id and not fetch_all:
        raise ValueError(
            "table_options for 'message_replies' must include "
            "either 'message_id' or 'fetch_all_messages=true'"
        )

    triples = []
    for tid, cid in pairs:
        if fetch_all:
            msg_ids = fetch_all_message_ids(
                client, tid, cid, max_pages,
            )
            if not msg_ids:
                continue
        else:
            msg_ids = [message_id]
        for mid in msg_ids:
            triples.append((tid, cid, mid))
    return triples


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
                f"Unsupported table: {table_name}. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        if table_name not in TABLE_METADATA:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables: {SUPPORTED_TABLES}"
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
        self, table_name: str, start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table: {table_name}. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )

        table_readers = {
            "teams": self._read_teams,
            "channels": self._read_channels,
            "messages": self._read_messages,
            "members": self._read_members,
            "message_replies": self._read_message_replies,
        }

        reader = table_readers.get(table_name)
        if reader is None:
            raise ValueError(
                f"No reader for table: {table_name}"
            )
        return reader(start_offset, table_options)

    # ================================================================
    # Table-Specific Read Methods
    # ================================================================

    def _read_teams(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read teams table (snapshot mode)."""
        top = parse_int_option(table_options, "top", 50)
        top = max(1, min(top, 999))
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )

        endpoint = TABLE_METADATA["teams"]["endpoint"]
        url = f"{self._client.base_url}/{endpoint}"
        params = {"$top": top}

        records: list[dict[str, Any]] = []
        pages_fetched = 0
        next_url: str | None = url

        while next_url and pages_fetched < max_pages:
            if pages_fetched == 0:
                data = self._client.make_request(
                    url, params=params,
                )
            else:
                data = self._client.make_request(next_url)

            for team in data.get("value", []):
                record: dict[str, Any] = dict(team)
                serialize_complex_fields(
                    record, _TEAM_SETTINGS_FIELDS,
                )
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
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )
        team_ids = resolve_team_ids(
            self._client, table_options, "channels", max_pages,
        )

        records: list[dict[str, Any]] = []

        for current_team_id in team_ids:
            base = self._client.base_url
            url = f"{base}/teams/{current_team_id}/channels"
            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._client.make_request(
                            url, params={},
                        )
                    else:
                        data = self._client.make_request(
                            next_url,
                        )

                    for channel in data.get("value", []):
                        record: dict[str, Any] = dict(channel)
                        record["team_id"] = current_team_id
                        records.append(record)

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1
                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    if ("404" not in str(e)
                            and "403" not in str(e)):
                        raise
                    break

        return iter(records), {}

    def _read_members(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read members table (snapshot mode)."""
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )
        team_ids = resolve_team_ids(
            self._client, table_options, "members", max_pages,
        )

        records: list[dict[str, Any]] = []

        for current_team_id in team_ids:
            base = self._client.base_url
            url = f"{base}/teams/{current_team_id}/members"
            pages_fetched = 0
            next_url: str | None = url

            while next_url and pages_fetched < max_pages:
                try:
                    if pages_fetched == 0:
                        data = self._client.make_request(
                            url, params={},
                        )
                    else:
                        data = self._client.make_request(
                            next_url,
                        )

                    for member in data.get("value", []):
                        record: dict[str, Any] = dict(member)
                        record["team_id"] = current_team_id
                        records.append(record)

                    next_url = data.get("@odata.nextLink")
                    pages_fetched += 1
                    if next_url:
                        time.sleep(0.1)

                except Exception as e:
                    if ("404" not in str(e)
                            and "403" not in str(e)):
                        raise
                    break

        return iter(records), {}

    def _read_messages(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages - routes to Delta API or legacy."""
        use_delta = (
            table_options.get("use_delta_api", "true").lower()
            == "true"
        )
        if use_delta:
            return self._read_messages_delta(
                start_offset, table_options,
            )
        return self._read_messages_legacy(
            start_offset, table_options,
        )

    def _read_messages_delta(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages using Microsoft Graph Delta API."""
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )
        pairs = resolve_team_channel_pairs(
            self._client, table_options, "messages", max_pages,
        )

        records = []
        delta_links = {}

        for team_id, channel_id in pairs:
            try:
                ch_recs, ch_key, ch_delta = (
                    self._fetch_channel_delta(
                        team_id, channel_id,
                        start_offset, max_pages,
                    )
                )
                records.extend(ch_recs)
                if ch_delta:
                    delta_links[ch_key] = ch_delta
            except Exception as e:
                if ("404" not in str(e)
                        and "403" not in str(e)):
                    raise

        next_offset = (
            {"deltaLinks": delta_links} if delta_links else {}
        )
        return iter(records), next_offset

    def _fetch_channel_delta(
        self, team_id, channel_id, start_offset, max_pages,
    ):
        """Fetch delta messages for a single channel."""
        channel_key = f"{team_id}/{channel_id}"

        delta_link = None
        if start_offset and isinstance(start_offset, dict):
            delta_link = start_offset.get(
                "deltaLinks", {},
            ).get(channel_key)

        base = self._client.base_url
        url = delta_link or (
            f"{base}/teams/{team_id}"
            f"/channels/{channel_id}/messages/delta"
        )

        records = []
        new_delta_link = None
        pages_fetched = 0

        while url and pages_fetched < max_pages:
            data = self._client.make_request(url)

            for msg in data.get("value", []):
                if "@removed" in msg:
                    record = _build_deleted_record(
                        msg["id"], team_id, channel_id,
                    )
                else:
                    record = dict(msg)
                    record["team_id"] = team_id
                    record["channel_id"] = channel_id
                    serialize_complex_fields(
                        record, _MESSAGE_COMPLEX_FIELDS,
                    )
                records.append(record)

            new_link = data.get("@odata.deltaLink")
            if new_link:
                new_delta_link = new_link
                break

            url = data.get("@odata.nextLink")
            pages_fetched += 1
            if url:
                time.sleep(0.1)

        return records, channel_key, new_delta_link

    def _read_messages_legacy(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read messages with client-side timestamp filtering."""
        top = parse_int_option(table_options, "top", 50)
        top = max(1, min(top, 50))
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )
        lookback_seconds = parse_int_option(
            table_options, "lookback_seconds", 300,
        )
        cursor = get_cursor_from_offset(
            start_offset, table_options,
        )
        pairs = resolve_team_channel_pairs(
            self._client, table_options, "messages", max_pages,
        )

        records: list[dict[str, Any]] = []
        max_modified: str | None = None
        fetch_params = {
            "cursor": cursor, "top": top,
            "max_pages": max_pages,
        }

        for team_id, channel_id in pairs:
            ch_recs, ch_max = self._fetch_channel_messages(
                team_id, channel_id, fetch_params,
            )
            records.extend(ch_recs)
            if ch_max:
                if not max_modified or ch_max > max_modified:
                    max_modified = ch_max

        next_cursor = compute_next_cursor(
            max_modified, cursor, lookback_seconds,
        )
        next_offset = (
            {"cursor": next_cursor} if next_cursor else {}
        )
        return iter(records), next_offset

    def _fetch_channel_messages(
        self, team_id, channel_id, fetch_params,
    ):
        """Fetch messages for a channel with cursor filtering."""
        cursor = fetch_params["cursor"]
        top = fetch_params["top"]
        max_pages = fetch_params["max_pages"]

        base = self._client.base_url
        url = (
            f"{base}/teams/{team_id}"
            f"/channels/{channel_id}/messages"
        )
        params = {"$top": top}
        records = []
        max_modified = None
        pages_fetched = 0
        next_url = url

        try:
            while next_url and pages_fetched < max_pages:
                if pages_fetched == 0:
                    data = self._client.make_request(
                        url, params=params,
                    )
                else:
                    data = self._client.make_request(next_url)

                for msg in data.get("value", []):
                    modified = msg.get(
                        "lastModifiedDateTime",
                    )
                    if cursor and modified and modified < cursor:
                        continue

                    record = dict(msg)
                    record["team_id"] = team_id
                    record["channel_id"] = channel_id
                    serialize_complex_fields(
                        record, _MESSAGE_COMPLEX_FIELDS,
                    )
                    records.append(record)

                    if modified:
                        if (not max_modified
                                or modified > max_modified):
                            max_modified = modified

                next_url = data.get("@odata.nextLink")
                pages_fetched += 1
                if next_url:
                    time.sleep(0.1)

        except Exception as e:
            if "404" not in str(e) and "403" not in str(e):
                raise

        return records, max_modified

    def _read_message_replies(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read message replies - routes to Delta or legacy."""
        use_delta = (
            table_options.get("use_delta_api", "false").lower()
            == "true"
        )
        if use_delta:
            return self._read_message_replies_delta(
                start_offset, table_options,
            )
        return self._read_message_replies_legacy(
            start_offset, table_options,
        )

    def _read_message_replies_delta(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read message replies using Microsoft Graph Delta API."""
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )
        pairs = resolve_team_channel_pairs(
            self._client, table_options,
            "message_replies", max_pages,
        )
        triples = _resolve_message_triples(
            self._client, pairs, table_options, max_pages,
        )

        records: list[dict[str, Any]] = []
        delta_links = {}

        for tid, cid, mid in triples:
            try:
                r, key, delta = self._fetch_reply_delta(
                    (tid, cid, mid), start_offset, max_pages,
                )
                records.extend(r)
                if delta:
                    delta_links[key] = delta
            except Exception as e:
                if ("404" not in str(e)
                        and "403" not in str(e)):
                    raise

        next_offset = (
            {"deltaLinks": delta_links} if delta_links else {}
        )
        return iter(records), next_offset

    def _fetch_reply_delta(
        self, message_triple, start_offset, max_pages,
    ):
        """Fetch delta replies for a single message."""
        team_id, channel_id, message_id = message_triple
        message_key = (
            f"{team_id}/{channel_id}/{message_id}"
        )

        delta_link = (
            start_offset.get("deltaLinks", {}).get(message_key)
            if start_offset
            else None
        )

        base = self._client.base_url
        url = delta_link or (
            f"{base}/teams/{team_id}/channels/"
            f"{channel_id}/messages/{message_id}"
            f"/replies/delta"
        )

        records = []
        new_delta_link = None
        pages_fetched = 0

        while url and pages_fetched < max_pages:
            data = self._client.make_request(url)

            for reply in data.get("value", []):
                if "@removed" in reply:
                    record = _build_deleted_record(
                        reply["id"], team_id, channel_id,
                    )
                    record["parent_message_id"] = message_id
                else:
                    record = dict(reply)
                    record["parent_message_id"] = message_id
                    record["team_id"] = team_id
                    record["channel_id"] = channel_id
                    serialize_complex_fields(
                        record, _MESSAGE_COMPLEX_FIELDS,
                    )
                records.append(record)

            new_link = data.get("@odata.deltaLink")
            if new_link:
                new_delta_link = new_link
                break

            url = data.get("@odata.nextLink")
            pages_fetched += 1
            if url:
                time.sleep(0.1)

        return records, message_key, new_delta_link

    def _read_message_replies_legacy(  # pylint: disable=too-many-locals
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read message replies with timestamp filtering."""
        top = parse_int_option(table_options, "top", 50)
        top = max(1, min(top, 50))
        max_pages = parse_int_option(
            table_options, "max_pages_per_batch", 100,
        )
        lookback_seconds = parse_int_option(
            table_options, "lookback_seconds", 300,
        )
        cursor = get_cursor_from_offset(
            start_offset, table_options,
        )
        pairs = resolve_team_channel_pairs(
            self._client, table_options,
            "message_replies", max_pages,
        )
        triples = _resolve_message_triples(
            self._client, pairs, table_options, max_pages,
        )
        max_workers = parse_int_option(
            table_options, "max_concurrent_threads", 10,
        )
        fetch_params = {
            "cursor": cursor, "top": top,
            "max_pages": max_pages,
        }

        records: list[dict[str, Any]] = []
        max_modified: str | None = None

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(
                    self._fetch_replies_for_message,
                    tid, cid, mid, fetch_params,
                ): (tid, cid, mid)
                for tid, cid, mid in triples
            }

            for future in as_completed(futures):
                try:
                    reply_recs, reply_max = future.result()
                    records.extend(reply_recs)

                    if reply_max:
                        if (not max_modified
                                or reply_max > max_modified):
                            max_modified = reply_max

                except Exception as e:
                    if ("404" not in str(e)
                            and "403" not in str(e)):
                        raise

        next_cursor = compute_next_cursor(
            max_modified, cursor, lookback_seconds,
        )
        next_offset = (
            {"cursor": next_cursor} if next_cursor else {}
        )
        return iter(records), next_offset

    def _fetch_replies_for_message(
        self, team_id, channel_id, message_id, fetch_params,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch replies for a single message (parallel exec)."""
        cursor = fetch_params["cursor"]
        top = fetch_params["top"]
        max_pages = fetch_params["max_pages"]

        records: list[dict[str, Any]] = []
        max_modified: str | None = None

        base = self._client.base_url
        url = (
            f"{base}/teams/{team_id}/channels/{channel_id}"
            f"/messages/{message_id}/replies"
        )
        params = {"$top": top}
        pages_fetched = 0
        next_url: str | None = url

        try:
            while next_url and pages_fetched < max_pages:
                if pages_fetched == 0:
                    data = self._client.make_request(
                        url, params=params,
                    )
                else:
                    data = self._client.make_request(next_url)

                for reply in data.get("value", []):
                    modified = reply.get(
                        "lastModifiedDateTime",
                    )
                    if cursor and modified and modified < cursor:
                        continue

                    record: dict[str, Any] = dict(reply)
                    record["parent_message_id"] = message_id
                    record["team_id"] = team_id
                    record["channel_id"] = channel_id
                    serialize_complex_fields(
                        record, _MESSAGE_COMPLEX_FIELDS,
                    )
                    records.append(record)

                    if modified:
                        if (not max_modified
                                or modified > max_modified):
                            max_modified = modified

                next_url = data.get("@odata.nextLink")
                pages_fetched += 1
                if next_url:
                    time.sleep(0.1)

        except Exception as e:
            if "404" not in str(e):
                raise

        return records, max_modified
