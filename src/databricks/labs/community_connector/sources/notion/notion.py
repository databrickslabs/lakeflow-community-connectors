"""Notion connector for the Lakeflow Community Connector framework.

Implements ``LakeflowConnect`` for the Notion REST API, supporting five tables:

- **databases** — Notion databases (metadata), CDC via ``last_edited_time``
- **pages** — Pages and database items, CDC via ``last_edited_time``
- **blocks** — Content blocks within pages (recursive), CDC via ``last_edited_time``
- **users** — Workspace users, snapshot (full refresh)
- **comments** — Comments on pages/blocks, CDC via ``created_time``

Authentication uses an internal integration bearer token (``api_key`` in options).
"""

import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.notion.notion_schemas import (
    BASE_URL,
    INITIAL_BACKOFF,
    LOOKBACK_SECONDS,
    MAX_BLOCK_DEPTH,
    MAX_PAGE_SIZE,
    MAX_RETRIES,
    NOTION_VERSION,
    RETRIABLE_STATUS_CODES,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

logger = logging.getLogger(__name__)


class NotionLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Notion."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key", "")
        if not api_key:
            raise ValueError("Connection option 'api_key' is required")

        self._base_url = options.get("base_url", BASE_URL).rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Notion-Version": options.get("notion_version", NOTION_VERSION),
                "Content-Type": "application/json",
            }
        )

        # Cap cursors at init time so a trigger never chases new data.
        self._init_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs):
        """Issue an HTTP request with retry logic for 429 and server errors."""
        url = f"{self._base_url}{path}"
        kwargs.setdefault("timeout", 30)
        backoff = INITIAL_BACKOFF
        last_resp = None

        for attempt in range(MAX_RETRIES):
            resp = self._session.request(method, url, **kwargs)
            last_resp = resp

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 1))
                jitter = random.uniform(0, 0.5)
                time.sleep(retry_after + jitter)
                continue

            if resp.status_code in RETRIABLE_STATUS_CODES:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff + random.uniform(0, 0.5))
                    backoff *= 2
                    continue

            return resp

        return last_resp

    def _get(self, path: str, params: dict | None = None):
        return self._request("GET", path, params=params)

    def _post(self, path: str, payload: dict):
        return self._request("POST", path, json=payload)

    # ------------------------------------------------------------------
    # Interface: list_tables / get_table_schema / read_table_metadata
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    # ------------------------------------------------------------------
    # Interface: read_table
    # ------------------------------------------------------------------

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)

        if table_name == "users":
            return self._read_users(table_options)
        if table_name == "databases":
            return self._read_search(
                table_name, start_offset, table_options, "data_source", "last_edited_time"
            )
        if table_name == "pages":
            return self._read_search(
                table_name, start_offset, table_options, "page", "last_edited_time"
            )
        if table_name == "blocks":
            return self._read_blocks(start_offset, table_options)
        if table_name == "comments":
            return self._read_comments(start_offset, table_options)

        raise ValueError(f"Unknown table: {table_name}")

    # ------------------------------------------------------------------
    # Users (snapshot)
    # ------------------------------------------------------------------

    def _read_users(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Full-refresh read for users. Returns all users in one batch."""
        records = []
        cursor = None

        while True:
            params: dict = {"page_size": MAX_PAGE_SIZE}
            if cursor:
                params["start_cursor"] = cursor

            resp = self._get("/v1/users", params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to list users: {resp.status_code} {resp.text}")

            body = resp.json()
            for user in body.get("results", []):
                records.append(self._transform_user(user))

            if not body.get("has_more", False):
                break
            cursor = body.get("next_cursor")

        return iter(records), {}

    # ------------------------------------------------------------------
    # Databases / Pages (via Search API)
    # ------------------------------------------------------------------

    def _read_search(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        filter_value: str,
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Incremental read using POST /v1/search sorted by last_edited_time ascending.

        The Search API does not support a ``since`` filter, so we rely on the
        ascending sort and skip records whose ``last_edited_time`` is at or
        before the stored cursor. The cursor is the ``last_edited_time`` of
        the most recently seen record.
        """
        since = start_offset.get("cursor") if start_offset else None

        # Short-circuit when caught up to init time.
        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", "200"))

        # Apply lookback on the first call of a trigger to catch late-indexed items.
        filter_since = since
        if filter_since:
            lookback_dt = datetime.fromisoformat(
                filter_since.replace("Z", "+00:00")
            ) - timedelta(seconds=LOOKBACK_SECONDS)
            filter_since = lookback_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        records: list[dict] = []
        api_cursor: str | None = None
        seen_ids: set[str] = set()

        while len(records) < max_records:
            payload: dict = {
                "filter": {"property": "object", "value": filter_value},
                "sort": {"timestamp": "last_edited_time", "direction": "ascending"},
                "page_size": MAX_PAGE_SIZE,
            }
            if api_cursor:
                payload["start_cursor"] = api_cursor

            resp = self._post("/v1/search", payload)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Search failed for {table_name}: {resp.status_code} {resp.text}"
                )

            body = resp.json()
            batch = body.get("results", [])
            if not batch:
                break

            for item in batch:
                item_time = item.get("last_edited_time", "")
                item_id = item.get("id", "")

                # Skip items at or before the stored cursor (dedup from lookback).
                if since and item_time <= since and item_id not in seen_ids:
                    continue

                # Stop if we've reached the init time cap.
                if item_time >= self._init_ts:
                    break

                if item_id not in seen_ids:
                    seen_ids.add(item_id)
                    if table_name == "databases":
                        records.append(self._transform_database(item))
                    else:
                        records.append(self._transform_page(item))

                if len(records) >= max_records:
                    break

            if not body.get("has_more", False):
                break
            api_cursor = body.get("next_cursor")

        if not records:
            return iter([]), start_offset or {}

        last_cursor = records[-1][cursor_field]
        end_offset = {"cursor": last_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Blocks (recursive from modified pages)
    # ------------------------------------------------------------------

    def _read_blocks(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Incremental block read.

        Strategy: find pages modified since the cursor via Search, then
        recursively fetch all blocks for those pages and emit blocks with
        ``last_edited_time > cursor``.
        """
        since = start_offset.get("cursor") if start_offset else None

        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", "200"))
        max_pages = int(table_options.get("max_pages_per_batch", "5"))

        # Find recently modified pages.
        page_ids = self._find_modified_page_ids(since, max_pages)

        if not page_ids:
            # No modified pages found — advance cursor to init_ts to stop.
            end_offset = {"cursor": self._init_ts}
            if start_offset and start_offset == end_offset:
                return iter([]), start_offset
            return iter([]), end_offset

        records: list[dict] = []
        max_cursor = since or ""

        for page_id in page_ids:
            if len(records) >= max_records:
                break

            blocks = self._fetch_all_blocks(page_id, depth=0)
            for block in blocks:
                block_time = block.get("last_edited_time", "")

                # Only emit blocks newer than the cursor.
                if since and block_time <= since:
                    continue

                if block_time >= self._init_ts:
                    continue

                record = self._transform_block(block, page_id)
                records.append(record)

                if block_time > max_cursor:
                    max_cursor = block_time

                if len(records) >= max_records:
                    break

        if not records:
            # Pages were modified but no new blocks — advance cursor.
            end_offset = {"cursor": self._init_ts}
            if start_offset and start_offset == end_offset:
                return iter([]), start_offset
            return iter([]), end_offset

        end_offset = {"cursor": max_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _find_modified_page_ids(self, since: str | None, max_pages: int) -> list[str]:
        """Return IDs of pages modified after ``since`` via Search."""
        page_ids: list[str] = []
        api_cursor: str | None = None

        while len(page_ids) < max_pages:
            payload: dict = {
                "filter": {"property": "object", "value": "page"},
                "sort": {"timestamp": "last_edited_time", "direction": "ascending"},
                "page_size": MAX_PAGE_SIZE,
            }
            if api_cursor:
                payload["start_cursor"] = api_cursor

            resp = self._post("/v1/search", payload)
            if resp.status_code != 200:
                break

            body = resp.json()
            for item in body.get("results", []):
                item_time = item.get("last_edited_time", "")
                if since and item_time <= since:
                    continue
                if item_time >= self._init_ts:
                    break
                page_ids.append(item["id"])
                if len(page_ids) >= max_pages:
                    break

            if not body.get("has_more", False):
                break
            api_cursor = body.get("next_cursor")

        return page_ids

    def _fetch_all_blocks(self, block_id: str, depth: int) -> list[dict]:
        """Recursively fetch all child blocks of a block or page."""
        if depth >= MAX_BLOCK_DEPTH:
            return []

        blocks: list[dict] = []
        cursor: str | None = None

        while True:
            params: dict = {"page_size": MAX_PAGE_SIZE}
            if cursor:
                params["start_cursor"] = cursor

            resp = self._get(f"/v1/blocks/{block_id}/children", params=params)
            if resp.status_code != 200:
                logger.warning(
                    "Failed to fetch blocks for %s: %s %s",
                    block_id, resp.status_code, resp.text,
                )
                break

            body = resp.json()
            for block in body.get("results", []):
                blocks.append(block)
                block_type = block.get("type", "")
                if (
                    block.get("has_children")
                    and block_type not in ("child_page", "child_database")
                ):
                    blocks.extend(self._fetch_all_blocks(block["id"], depth + 1))

            if not body.get("has_more", False):
                break
            cursor = body.get("next_cursor")

        return blocks

    # ------------------------------------------------------------------
    # Comments (per-page polling)
    # ------------------------------------------------------------------

    def _read_comments(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Incremental comment read.

        Strategy: find pages modified since the cursor, then fetch comments
        for each page and emit those with ``created_time > cursor``.
        """
        since = start_offset.get("cursor") if start_offset else None

        if since and since >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", "200"))
        max_pages = int(table_options.get("max_pages_per_batch", "10"))

        # Find recently modified pages to poll for comments.
        page_ids = self._find_modified_page_ids(since, max_pages)

        if not page_ids:
            end_offset = {"cursor": self._init_ts}
            if start_offset and start_offset == end_offset:
                return iter([]), start_offset
            return iter([]), end_offset

        records: list[dict] = []
        max_cursor = since or ""

        for page_id in page_ids:
            if len(records) >= max_records:
                break

            comments = self._fetch_comments_for_block(page_id)
            for comment in comments:
                comment_time = comment.get("created_time", "")
                if since and comment_time <= since:
                    continue
                if comment_time >= self._init_ts:
                    continue

                record = self._transform_comment(comment, page_id)
                records.append(record)

                if comment_time > max_cursor:
                    max_cursor = comment_time

                if len(records) >= max_records:
                    break

        if not records:
            end_offset = {"cursor": self._init_ts}
            if start_offset and start_offset == end_offset:
                return iter([]), start_offset
            return iter([]), end_offset

        end_offset = {"cursor": max_cursor}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    def _fetch_comments_for_block(self, block_id: str) -> list[dict]:
        """Fetch all comments for a given page or block ID."""
        comments: list[dict] = []
        cursor: str | None = None

        while True:
            params: dict = {"block_id": block_id, "page_size": MAX_PAGE_SIZE}
            if cursor:
                params["start_cursor"] = cursor

            resp = self._get("/v1/comments", params=params)
            if resp.status_code != 200:
                logger.warning(
                    "Failed to fetch comments for %s: %s %s",
                    block_id, resp.status_code, resp.text,
                )
                break

            body = resp.json()
            comments.extend(body.get("results", []))

            if not body.get("has_more", False):
                break
            cursor = body.get("next_cursor")

        return comments

    # ------------------------------------------------------------------
    # Transform helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_plain_text(rich_text_array) -> str:
        """Extract concatenated plain_text from a rich text array."""
        if not rich_text_array or not isinstance(rich_text_array, list):
            return ""
        return "".join(seg.get("plain_text", "") for seg in rich_text_array)

    def _transform_database(self, raw: dict) -> dict:
        """Transform a raw database object for output."""
        return {
            "object": raw.get("object"),
            "id": raw.get("id"),
            "created_time": raw.get("created_time"),
            "created_by": raw.get("created_by"),
            "last_edited_time": raw.get("last_edited_time"),
            "last_edited_by": raw.get("last_edited_by"),
            "title": self._extract_plain_text(raw.get("title")),
            "description": self._extract_plain_text(raw.get("description")),
            "icon": json.dumps(raw["icon"]) if raw.get("icon") else None,
            "cover": json.dumps(raw["cover"]) if raw.get("cover") else None,
            "parent": raw.get("parent"),
            "url": raw.get("url"),
            "in_trash": raw.get("in_trash"),
            "is_inline": raw.get("is_inline"),
            "public_url": raw.get("public_url"),
            "properties": json.dumps(raw["properties"]) if raw.get("properties") else None,
        }

    def _transform_page(self, raw: dict) -> dict:
        """Transform a raw page object for output."""
        return {
            "object": raw.get("object"),
            "id": raw.get("id"),
            "created_time": raw.get("created_time"),
            "created_by": raw.get("created_by"),
            "last_edited_time": raw.get("last_edited_time"),
            "last_edited_by": raw.get("last_edited_by"),
            "in_trash": raw.get("in_trash"),
            "is_archived": raw.get("is_archived"),
            "is_locked": raw.get("is_locked"),
            "icon": json.dumps(raw["icon"]) if raw.get("icon") else None,
            "cover": json.dumps(raw["cover"]) if raw.get("cover") else None,
            "parent": raw.get("parent"),
            "url": raw.get("url"),
            "public_url": raw.get("public_url"),
            "properties": json.dumps(raw["properties"]) if raw.get("properties") else None,
        }

    def _transform_block(self, raw: dict, page_id: str) -> dict:
        """Transform a raw block object for output."""
        block_type = raw.get("type", "")
        content = raw.get(block_type)
        return {
            "object": raw.get("object"),
            "id": raw.get("id"),
            "parent": raw.get("parent"),
            "type": block_type,
            "created_time": raw.get("created_time"),
            "created_by": raw.get("created_by"),
            "last_edited_time": raw.get("last_edited_time"),
            "last_edited_by": raw.get("last_edited_by"),
            "has_children": raw.get("has_children"),
            "in_trash": raw.get("in_trash"),
            "content": json.dumps(content) if content else None,
            "page_id": page_id,
        }

    def _transform_comment(self, raw: dict, page_id: str) -> dict:
        """Transform a raw comment object for output."""
        return {
            "object": raw.get("object"),
            "id": raw.get("id"),
            "parent": raw.get("parent"),
            "discussion_id": raw.get("discussion_id"),
            "created_time": raw.get("created_time"),
            "last_edited_time": raw.get("last_edited_time"),
            "created_by": raw.get("created_by"),
            "rich_text": json.dumps(raw["rich_text"]) if raw.get("rich_text") else None,
            "page_id": page_id,
        }

    @staticmethod
    def _transform_user(raw: dict) -> dict:
        """Transform a raw user object for output."""
        bot = raw.get("bot")
        return {
            "object": raw.get("object"),
            "id": raw.get("id"),
            "type": raw.get("type"),
            "name": raw.get("name"),
            "avatar_url": raw.get("avatar_url"),
            "person": raw.get("person"),
            "bot": json.dumps(bot) if bot else None,
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_table(table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {SUPPORTED_TABLES}"
            )
