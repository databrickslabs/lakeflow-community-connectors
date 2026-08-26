"""Xquik REST API source for Lakeflow Community Connectors."""

import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterator
from urllib.parse import quote

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.xquik.xquik_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

BASE_URL = "https://xquik.com/api/v1"
MAX_ATTEMPTS = 5
INITIAL_BACKOFF_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 60
RETRIABLE_STATUS_CODES = {408, 409, 424, 429, 500, 502, 503, 504}
USERNAME_PATTERN = re.compile(r"^(?:[A-Za-z0-9_]{1,15}|[0-9]+)$")


def _bounded_int(
    options: dict[str, str], name: str, default: int, minimum: int, maximum: int
) -> int:
    raw = (options.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, min(int(raw), maximum))
    except ValueError:
        return default


def _csv_values(options: dict[str, str], name: str) -> list[str]:
    values = list(dict.fromkeys(part.strip() for part in (options.get(name) or "").split(",")))
    return [value for value in values if value]


def _required_option(options: dict[str, str], name: str, table: str) -> str:
    value = (options.get(name) or "").strip()
    if not value:
        raise ValueError(f"{table} requires the '{name}' table option")
    return value


def _retry_delay(response: requests.Response, backoff: float) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            delay = float(retry_after)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return backoff
        return max(0.0, min(delay, 60.0))
    return backoff


def _response_error(response: requests.Response) -> tuple[str, str]:
    try:
        payload = response.json()
    except ValueError:
        return "unknown_error", response.reason or "Unknown API error"
    if not isinstance(payload, dict):
        return "unknown_error", response.reason or "Unknown API error"
    error = payload.get("error", "unknown_error")
    if isinstance(error, dict):
        code = str(error.get("code") or error.get("type") or "unknown_error")
        message = str(error.get("message") or code)
    else:
        code = str(error)
        message = str(payload.get("message") or code)
    return code[:100], message[:300]


def _normalize_profile(record: dict) -> dict:
    """Replace empty entity structs that Spark cannot coerce."""
    bio = record.get("profile_bio")
    if not isinstance(bio, dict):
        return record
    entities = bio.get("entities")
    if not isinstance(entities, dict):
        return record
    normalized_entities = {name: None if value == {} else value for name, value in entities.items()}
    return {**record, "profile_bio": {**bio, "entities": normalized_entities}}


class XquikLakeflowConnect(LakeflowConnect):
    """Read public X search, profiles, timelines, and regional trends."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._api_key = (options.get("api_key") or "").strip()
        if not self._api_key:
            raise ValueError("Xquik connector requires 'api_key' in connection options")
        self._session = requests.Session()

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict | None]:
        del start_offset
        self._validate_table(table_name)
        readers = {
            "tweets_search": self._read_tweets_search,
            "user_profiles": self._read_user_profiles,
            "user_tweets": self._read_user_tweets,
            "trends": self._read_trends,
        }
        return iter(readers[table_name](table_options)), None

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {SUPPORTED_TABLES}"
            )

    def _request(self, path: str, params: dict[str, Any] | None = None) -> dict:
        url = f"{BASE_URL}{path}"
        backoff = INITIAL_BACKOFF_SECONDS
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = self._session.get(
                    url,
                    headers={"x-api-key": self._api_key},
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    allow_redirects=False,
                )
            except requests.RequestException as error:
                if attempt == MAX_ATTEMPTS - 1:
                    raise RuntimeError("Xquik request failed after retries") from error
                time.sleep(backoff)
                backoff *= 2
                continue

            if response.status_code in RETRIABLE_STATUS_CODES and attempt < MAX_ATTEMPTS - 1:
                time.sleep(_retry_delay(response, backoff))
                backoff *= 2
                continue
            if 300 <= response.status_code < 400:
                raise ValueError(f"Xquik API {path} refused HTTP redirect")
            if not response.ok:
                code, message = _response_error(response)
                raise ValueError(
                    f"Xquik API {path} returned HTTP {response.status_code} ({code}). {message}"
                )
            try:
                payload = response.json()
            except ValueError as error:
                raise ValueError(f"Xquik API {path} returned invalid JSON") from error
            if not isinstance(payload, dict):
                raise ValueError(f"Xquik API {path} returned a non-object response")
            return payload
        raise RuntimeError("Xquik request retry loop exhausted")

    def _paginate_tweets(self, path: str, params: dict[str, Any], max_pages: int) -> list[dict]:
        records: list[dict] = []
        request_params = dict(params)
        for _ in range(max_pages):
            payload = self._request(path, request_params)
            page = payload.get("tweets") or []
            if not isinstance(page, list):
                raise ValueError(f"Xquik API {path} returned invalid 'tweets' data")
            records.extend(record for record in page if isinstance(record, dict))
            cursor = payload.get("next_cursor")
            if not payload.get("has_next_page") or not cursor:
                break
            request_params["cursor"] = cursor
        return records

    def _read_tweets_search(self, options: dict[str, str]) -> list[dict]:
        query = _required_option(options, "q", "tweets_search")
        query_type = (options.get("query_type") or "Latest").strip().title()
        if query_type not in {"Latest", "Top"}:
            raise ValueError("tweets_search query_type must be 'Latest' or 'Top'")
        params: dict[str, Any] = {
            "q": query,
            "queryType": query_type,
            "limit": _bounded_int(options, "limit", 100, 1, 200),
        }
        if options.get("since_time"):
            params["sinceTime"] = options["since_time"].strip()
        if options.get("until_time"):
            params["untilTime"] = options["until_time"].strip()
        records = self._paginate_tweets(
            "/x/tweets/search", params, _bounded_int(options, "max_pages", 100, 1, 1000)
        )
        for record in records:
            record["search_query"] = query
        return records

    def _read_user_profiles(self, options: dict[str, str]) -> list[dict]:
        usernames = self._usernames(options, "user_profiles")
        records = []
        for username in usernames:
            record = _normalize_profile(self._request(f"/x/users/{quote(username, safe='')}"))
            record["configured_username"] = username
            records.append(record)
        return records

    def _read_user_tweets(self, options: dict[str, str]) -> list[dict]:
        usernames = self._usernames(options, "user_tweets")
        max_pages = _bounded_int(options, "max_pages", 100, 1, 1000)
        records = []
        for username in usernames:
            params: dict[str, Any] = {
                "pageSize": _bounded_int(options, "page_size", 20, 1, 100),
                "includeReplies": (options.get("include_replies") or "false").lower(),
            }
            if params["includeReplies"] not in {"true", "false"}:
                raise ValueError("user_tweets include_replies must be 'true' or 'false'")
            if options.get("since_date"):
                params["sinceDate"] = options["since_date"].strip()
            if options.get("until_date"):
                params["untilDate"] = options["until_date"].strip()
            page = self._paginate_tweets(
                f"/x/users/{quote(username, safe='')}/tweets", params, max_pages
            )
            for record in page:
                record["source_username"] = username
            records.extend(page)
        return records

    def _read_trends(self, options: dict[str, str]) -> list[dict]:
        raw_woeids = _required_option(options, "woeids", "trends")
        try:
            woeids = list(dict.fromkeys(int(value.strip()) for value in raw_woeids.split(",")))
        except ValueError as error:
            raise ValueError("trends woeids must be comma-separated integers") from error
        records = []
        for woeid in woeids:
            payload = self._request(
                "/x/trends",
                {"woeid": woeid, "count": _bounded_int(options, "count", 30, 1, 50)},
            )
            trends = payload.get("trends") or []
            if not isinstance(trends, list):
                raise ValueError("Xquik API /x/trends returned invalid 'trends' data")
            for trend in trends:
                if isinstance(trend, dict):
                    trend["woeid"] = woeid
                    records.append(trend)
        return records

    @staticmethod
    def _usernames(options: dict[str, str], table: str) -> list[str]:
        usernames = _csv_values(options, "usernames")
        if not usernames:
            raise ValueError(f"{table} requires the 'usernames' table option")
        invalid = [username for username in usernames if not USERNAME_PATTERN.fullmatch(username)]
        if invalid:
            raise ValueError(
                f"{table} usernames must omit @ and use only letters, digits, or underscores"
            )
        return usernames
