"""Read-only Rootly JSON:API connector with paginated full refreshes."""

import json
import math
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Iterator

import requests
from pyspark.sql.types import ArrayType, StructType, VariantType, VariantVal

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.rootly.rootly_schemas import TABLE_SCHEMAS

API_BASE = "https://api.rootly.com/v1"
MAX_ATTEMPTS = 5
MAX_RETRY_DELAY = 120
RETRY_STATUSES = {429, 500, 502, 503, 504}


class RootlyLakeflowConnect(LakeflowConnect):
    """Retrieve visible Rootly resources without modifying the source."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        token = options.get("api_token")
        if not isinstance(token, str) or not token.strip():
            raise ValueError("api_token is required")
        if any(character.isspace() for character in token.strip()):
            raise ValueError("api_token must not contain whitespace")
        self._headers = {
            "Authorization": f"Bearer {token.strip()}",
            "Accept": "application/vnd.api+json",
        }

    def list_tables(self) -> list[str]:
        return list(TABLE_SCHEMAS)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return {"primary_keys": ["id"], "ingestion_type": "snapshot"}

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        self._validate_table(table_name)
        try:
            page_size = int(table_options.get("page_size", "100"))
        except (TypeError, ValueError):
            raise ValueError("page_size must be an integer between 1 and 100") from None
        if not 1 <= page_size <= 100:
            raise ValueError("page_size must be an integer between 1 and 100")
        # Full refreshes deliberately ignore any previous checkpoint. Returning
        # None marks a snapshot; the iterator streams pages without retaining rows.
        return self._read_pages(table_name, page_size), None

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(f"Unsupported Rootly table: {table_name}")

    def _request(self, table_name: str, params: dict) -> dict:
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = requests.get(
                    f"{API_BASE}/{table_name}",
                    headers=self._headers,
                    params=params,
                    timeout=(10, 60),
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt == MAX_ATTEMPTS - 1:
                    raise RuntimeError("Rootly request failed after bounded retries") from None
                time.sleep(2**attempt)
                continue
            except requests.RequestException:
                raise RuntimeError("Rootly request failed") from None

            if response.status_code in RETRY_STATUSES and attempt < MAX_ATTEMPTS - 1:
                try:
                    delay = _retry_delay(response.headers, attempt)
                finally:
                    response.close()
                time.sleep(delay)
                continue
            if response.status_code != 200:
                status = response.status_code
                response.close()
                # Never include headers, response bodies, or request objects:
                # these can contain credentials or private incident details.
                raise RuntimeError(f"Rootly GET /v1/{table_name} failed (HTTP {status})")
            try:
                body = response.json()
            except ValueError:
                raise RuntimeError("Rootly returned invalid JSON") from None
            finally:
                response.close()
            if not isinstance(body, dict):
                raise RuntimeError("Rootly returned an invalid JSON:API envelope")
            return body
        raise RuntimeError("Rootly request exhausted retries")

    def _read_pages(self, table_name: str, page_size: int) -> Iterator[dict]:
        params = {"page[size]": str(page_size), "page[number]": "1"}
        seen_cursors = set()
        while True:
            body = self._request(table_name, params)
            rows = body.get("data")
            meta = body.get("meta")
            if not isinstance(rows, list) or not isinstance(meta, dict) or "next_page" not in meta:
                raise RuntimeError("Rootly response requires data[] and meta.next_page")
            for row in rows:
                if (
                    not isinstance(row, dict)
                    or not isinstance(row.get("id"), str)
                    or not row["id"]
                    or not isinstance(row.get("type"), str)
                    or not isinstance(row.get("attributes"), dict)
                ):
                    raise RuntimeError("Rootly returned an invalid JSON:API resource")
            next_params = self._next_page(table_name, meta, params, seen_cursors)
            if next_params is not None and not rows:
                raise RuntimeError("Rootly returned an empty page with a continuation")
            for row in rows:
                yield _prepare_row(row, TABLE_SCHEMAS[table_name])
            if next_params is None:
                return
            params = next_params

    @staticmethod
    def _next_page(table_name: str, meta: dict, params: dict, seen_cursors: set) -> dict | None:
        next_cursor = meta.get("next_cursor") if table_name == "incidents" else None
        if next_cursor is not None:
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                raise RuntimeError("Rootly returned an invalid or repeated pagination cursor")
            seen_cursors.add(next_cursor)
            return {"page[size]": params["page[size]"], "page[after]": next_cursor}
        next_page = meta["next_page"]
        if "page[after]" in params:
            # Once keyset pagination starts, never fall back to page numbers.
            if "next_cursor" not in meta or next_page is not None:
                raise RuntimeError("Rootly cursor pagination metadata is inconsistent")
            return None
        if next_page is None:
            return None
        if (
            not isinstance(next_page, int)
            or isinstance(next_page, bool)
            or next_page <= int(params["page[number]"])
        ):
            raise RuntimeError("Rootly returned an invalid or non-advancing page number")
        return {"page[size]": params["page[size]"], "page[number]": str(next_page)}


def _prepare_row(value, data_type):
    """Preserve nesting while satisfying Spark's null and VARIANT representation."""
    if value is None:
        return None
    if isinstance(data_type, VariantType):
        return VariantVal.parseJson(json.dumps(value))
    if isinstance(data_type, StructType) and isinstance(value, dict):
        # The framework rejects empty dicts for structs. Populate missing fields
        # explicitly, including nested optional objects, without inventing values.
        return {
            **value,
            **{
                field.name: _prepare_row(value.get(field.name), field.dataType)
                for field in data_type.fields
            },
        }
    if isinstance(data_type, ArrayType) and isinstance(value, list):
        return [_prepare_row(item, data_type.elementType) for item in value]
    return value


def _retry_delay(headers: dict, attempt: int) -> float:
    """Honor retry hints; fail rather than retry before a long server deadline."""
    delay = None
    retry_after = headers.get("Retry-After")
    if retry_after is not None:
        try:
            delay = float(retry_after)
        except (TypeError, ValueError):
            try:
                deadline = parsedate_to_datetime(retry_after)
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                delay = deadline.timestamp() - time.time()
            except (TypeError, ValueError, OverflowError):
                pass
    if delay is None:
        try:
            delay = float(headers["X-RateLimit-Reset"]) - time.time()
        except (KeyError, TypeError, ValueError):
            pass
    if delay is None or not math.isfinite(delay):
        delay = 2**attempt
    if delay > MAX_RETRY_DELAY:
        raise RuntimeError("Rootly requested a long retry delay; retry the refresh later")
    return max(0.0, delay)
