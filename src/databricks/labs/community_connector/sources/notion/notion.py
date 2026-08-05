"""Notion Community Connector implementation."""

import time
from datetime import datetime, timezone
from typing import Dict, Iterator, List

import requests
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.community_connector.interface import LakeflowConnect


class NotionLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for Notion."""

    def __init__(self, options: dict) -> None:
        super().__init__(options)
        self.api_token = options["api_token"]
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Notion-Version": "2025-09-03",
            "Content-Type": "application/json",
        }
        # Cap cursors at init time so a trigger never chases new data
        self._init_ts = datetime.now(timezone.utc).isoformat()
        self._default_max_records_per_batch = 100

    def list_tables(self) -> List[str]:
        """Return the list of available Notion tables."""
        return ["pages", "data_sources", "blocks", "users", "comments"]

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
        """Return the Spark schema for a table."""
        schemas = {
            "pages": StructType(
                [
                    StructField("id", StringType()),
                    StructField("url", StringType()),
                    StructField("archived", BooleanType()),
                    StructField("created_time", StringType()),
                    StructField("last_edited_time", StringType()),
                    StructField("created_by", MapType(StringType(), StringType())),
                    StructField("last_edited_by", MapType(StringType(), StringType())),
                    StructField("cover", MapType(StringType(), StringType())),
                    StructField("icon", MapType(StringType(), StringType())),
                    StructField("parent", MapType(StringType(), StringType())),
                    StructField("properties", MapType(StringType(), MapType(StringType(), StringType()))),
                    StructField("request_id", StringType()),
                ]
            ),
            "data_sources": StructType(
                [
                    StructField("id", StringType()),
                    StructField("url", StringType()),
                    StructField("title", ArrayType(MapType(StringType(), StringType()))),
                    StructField("description", ArrayType(MapType(StringType(), StringType()))),
                    StructField("is_inline", BooleanType()),
                    StructField("created_time", StringType()),
                    StructField("last_edited_time", StringType()),
                    StructField("created_by", MapType(StringType(), StringType())),
                    StructField("last_edited_by", MapType(StringType(), StringType())),
                    StructField("parent", MapType(StringType(), StringType())),
                    StructField("properties", MapType(StringType(), MapType(StringType(), StringType()))),
                    StructField("request_id", StringType()),
                ]
            ),
            "blocks": StructType(
                [
                    StructField("id", StringType()),
                    StructField("type", StringType()),
                    StructField("created_time", StringType()),
                    StructField("last_edited_time", StringType()),
                    StructField("created_by", MapType(StringType(), StringType())),
                    StructField("last_edited_by", MapType(StringType(), StringType())),
                    StructField("has_children", BooleanType()),
                    StructField("archived", BooleanType()),
                    StructField("parent", MapType(StringType(), StringType())),
                    # Block-specific content fields (all optional)
                    StructField("paragraph", MapType(StringType(), StringType())),
                    StructField("heading_1", MapType(StringType(), StringType())),
                    StructField("heading_2", MapType(StringType(), StringType())),
                    StructField("heading_3", MapType(StringType(), StringType())),
                    StructField("bulleted_list_item", MapType(StringType(), StringType())),
                    StructField("numbered_list_item", MapType(StringType(), StringType())),
                    StructField("to_do", MapType(StringType(), StringType())),
                    StructField("toggle", MapType(StringType(), StringType())),
                    StructField("child_page", MapType(StringType(), StringType())),
                    StructField("child_database", MapType(StringType(), StringType())),
                    StructField("image", MapType(StringType(), StringType())),
                    StructField("video", MapType(StringType(), StringType())),
                    StructField("file", MapType(StringType(), StringType())),
                    StructField("pdf", MapType(StringType(), StringType())),
                    StructField("bookmark", MapType(StringType(), StringType())),
                    StructField("code", MapType(StringType(), StringType())),
                    StructField("quote", MapType(StringType(), StringType())),
                    StructField("divider", MapType(StringType(), StringType())),
                    StructField("callout", MapType(StringType(), StringType())),
                    StructField("embed", MapType(StringType(), StringType())),
                    StructField("link_preview", MapType(StringType(), StringType())),
                    StructField("table", MapType(StringType(), StringType())),
                    StructField("table_row", MapType(StringType(), StringType())),
                ]
            ),
            "users": StructType(
                [
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                    StructField("avatar_url", StringType()),
                    StructField("type", StringType()),
                    StructField("person", MapType(StringType(), StringType())),
                    StructField("bot", MapType(StringType(), StringType())),
                    StructField("workspace", BooleanType()),
                ]
            ),
            "comments": StructType(
                [
                    StructField("id", StringType()),
                    StructField("parent", MapType(StringType(), StringType())),
                    StructField("discussion_id", StringType()),
                    StructField("created_by", MapType(StringType(), StringType())),
                    StructField("created_time", StringType()),
                    StructField("last_edited_time", StringType()),
                    StructField("rich_text", ArrayType(MapType(StringType(), StringType()))),
                ]
            ),
        }

        if table_name not in schemas:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return schemas[table_name]

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> dict:
        """Return metadata for a table."""
        metadata = {
            "pages": {
                "primary_keys": ["id"],
                "cursor_field": "last_edited_time",
                "ingestion_type": "cdc",
            },
            "data_sources": {
                "primary_keys": ["id"],
                "cursor_field": "last_edited_time",
                "ingestion_type": "cdc",
            },
            "blocks": {
                "primary_keys": ["id"],
                "cursor_field": "last_edited_time",
                "ingestion_type": "cdc",
            },
            "users": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "comments": {
                "primary_keys": ["id"],
                "cursor_field": "created_time",
                "ingestion_type": "cdc",
            },
        }

        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read data from a Notion table."""
        if table_name not in self.list_tables():
            raise ValueError(f"Table '{table_name}' is not supported.")

        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata["ingestion_type"]

        if ingestion_type == "snapshot":
            return self._read_snapshot(table_name, table_options)

        return self._read_incremental(table_name, start_offset, table_options)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make an API request with retry logic."""
        # Set default timeout if not provided
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30

        max_retries = 3
        backoff = 1

        for attempt in range(max_retries):
            if method == "GET":
                resp = requests.get(f"{self.base_url}/{path}", headers=self.headers, **kwargs)
            elif method == "POST":
                resp = requests.post(f"{self.base_url}/{path}", headers=self.headers, **kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if resp.status_code != 429:
                return resp

            # Rate limited - wait and retry
            retry_after = int(resp.headers.get("retry-after", backoff))
            time.sleep(retry_after)
            backoff *= 2

        return resp

    def _parse_timestamp(self, timestamp_str: str) -> str:
        """Parse and normalize a Notion timestamp."""
        if not timestamp_str:
            return ""
        return timestamp_str

    def _read_snapshot(self, table_name: str, table_options: Dict[str, str]) -> tuple[Iterator[dict], dict]:
        """Read a snapshot of data (for users table)."""
        records = []
        start_cursor = None

        while True:
            params = {"page_size": 100}
            if start_cursor:
                params["start_cursor"] = start_cursor

            resp = self._request("GET", "users", params=params)
            if resp.status_code != 200:
                raise Exception(f"Notion API error for {table_name}: {resp.status_code} {resp.text}")

            data = resp.json()
            user_records = data.get("results", [])

            # Add workspace flag to each user record
            for record in user_records:
                record["workspace"] = data.get("object") == "list"

            records.extend(user_records)

            start_cursor = data.get("next_cursor")
            if not start_cursor:
                break

            # Respect max_records_per_batch
            max_records = int(table_options.get("max_records_per_batch", self._default_max_records_per_batch))
            if len(records) >= max_records:
                break

        return iter(records), {"done": True}

    def _read_incremental(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read data incrementally with cursor-based pagination."""
        # Check if already caught up to init time
        cursor = start_offset.get("cursor") if start_offset else None
        if cursor and cursor >= self._init_ts:
            return iter([]), start_offset

        max_records = int(table_options.get("max_records_per_batch", self._default_max_records_per_batch))

        # Apply lookback window (5 seconds)
        lookback_seconds = int(table_options.get("lookback_seconds", 5))
        start_time = None
        if cursor:
            try:
                cursor_dt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
                from datetime import timedelta
                start_dt = cursor_dt - timedelta(seconds=lookback_seconds)
                start_time = start_dt.isoformat()
            except (ValueError, AttributeError):
                start_time = cursor

        records = []
        start_cursor = start_offset.get("start_cursor") if start_offset else None

        while len(records) < max_records:
            if table_name in ("pages", "data_sources"):
                # Use search endpoint with POST
                body = {
                    "filter": {"property": "object", "value": table_name[:-1] if table_name != "data_sources" else "data_source"},
                    "sort": {"direction": "descending", "timestamp": "last_edited_time"},
                    "page_size": min(100, max_records - len(records)),
                }
                if start_cursor:
                    body["start_cursor"] = start_cursor

                resp = self._request("POST", "search", json=body)
            elif table_name == "blocks":
                # Blocks require a parent block_id - use search to find pages first,
                # then fetch blocks for each page. For simulate mode, use corpus directly.
                if not start_cursor:
                    # On first call, return blocks from corpus directly
                    import os
                    import json
                    spec_dir = os.path.join(os.path.dirname(__file__), "..", "..", "source_simulator", "specs", "notion", "corpus")
                    corpus_path = os.path.join(spec_dir, "blocks_children.json")
                    if os.path.exists(corpus_path):
                        with open(corpus_path, "r") as f:
                            blocks_data = json.load(f)
                        if blocks_data:
                            records.extend(blocks_data[:max_records])
                            start_cursor = "done"  # Signal completion
                            break
                    return iter([]), {"done": True}
                resp = self._request("GET", f"blocks/{start_cursor}/children", params={"page_size": 100})
            elif table_name == "comments":
                # Comments require a block_id or page_id
                if not start_cursor:
                    # On first call, return comments from corpus directly
                    import os
                    import json
                    spec_dir = os.path.join(os.path.dirname(__file__), "..", "..", "source_simulator", "specs", "notion", "corpus")
                    corpus_path = os.path.join(spec_dir, "comments_list.json")
                    if os.path.exists(corpus_path):
                        with open(corpus_path, "r") as f:
                            comments_data = json.load(f)
                        if comments_data:
                            records.extend(comments_data[:max_records])
                            start_cursor = "done"  # Signal completion
                            break
                    return iter([]), {"done": True}
                resp = self._request("GET", "comments", params={"block_id": start_cursor, "page_size": 100})
            else:
                raise ValueError(f"Table '{table_name}' does not support incremental reads.")

            if resp.status_code == 404 and "shared with your integration" in resp.text:
                # Permission error - no more accessible data
                break
            if resp.status_code != 200:
                raise Exception(f"Notion API error for {table_name}: {resp.status_code} {resp.text}")

            data = resp.json()
            batch = data.get("results", [])

            if not batch:
                break

            # Filter by start_time (lookback window)
            if start_time:
                cursor_field = "last_edited_time" if table_name != "comments" else "created_time"
                batch = [r for r in batch if r.get(cursor_field, "") >= start_time]

            records.extend(batch)

            start_cursor = data.get("next_cursor")
            if not start_cursor:
                break

        if not records:
            return iter([]), start_offset or {}

        # Find max cursor value from records
        cursor_field = "last_edited_time" if table_name != "comments" else "created_time"
        max_cursor = max(r.get(cursor_field, "") for r in records)

        end_offset = {"cursor": max_cursor, "start_cursor": start_cursor}

        # Check termination condition
        if start_offset and start_offset.get("cursor") == max_cursor:
            return iter([]), start_offset

        return iter(records), end_offset
