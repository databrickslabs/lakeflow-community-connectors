"""Mailchimp Marketing API v3.0 connector.

Implements the ``LakeflowConnect`` interface for four tables — ``campaigns``,
``reports``, ``lists``, and ``members`` — all ingested as ``cdc``. See
``mailchimp_api_doc.md`` for the authoritative endpoint mapping and
``mailchimp_schemas.py`` for schemas and per-table configuration.

Design notes
------------
- **Auth**: a single stored ``api_key``. HTTP Basic auth with a throwaway
  username (Mailchimp ignores the username). The account data center is
  derived from the key suffix (``...-us21`` -> ``us21``) to build the base
  URL. Only picklable strings (api_key, base_url) live on the instance so the
  reader serializes cleanly to Spark executors — auth is rebuilt per request.
- **Pagination**: classic offset/count. ``count=1000``; advance ``offset`` by
  ``count`` until a short page (or ``offset >= total_items``).
- **Incremental**: Strategy A sliding time-window. Every table exposes both a
  ``since_*`` and a ``before_*`` filter, so the window's upper bound caps the
  read and the cursor advances to the window end regardless of server sort
  order. The upper bound is capped at ``self._init_ts`` (captured in
  ``__init__``) so a trigger never chases records written during the sync;
  once the cursor reaches ``_init_ts`` the read short-circuits, guaranteeing
  ``Trigger.AvailableNow`` termination.
- **members fan-out**: ``members`` is nested under a list. Each window read
  enumerates every audience via ``GET /lists`` then pages
  ``GET /lists/{list_id}/members`` with the same window applied per list, and
  unions the results. The offset is a single ``{"cursor": ...}`` window
  checkpoint shared across the fan-out.
"""

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Tuple

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.mailchimp.mailchimp_schemas import (
    DEFAULT_MAX_RECORDS_PER_BATCH,
    DEFAULT_PAGE_COUNT,
    DEFAULT_WINDOW_SECONDS,
    INITIAL_BACKOFF,
    JSON_STRING_FIELDS,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
    TABLE_CONFIG,
    TABLE_SCHEMAS,
)

_USERNAME = "anystring"  # Mailchimp ignores the Basic-auth username.

# Mailchimp's ``since_*`` filters are EXCLUSIVE (strictly greater than), so a
# query seeded with the oldest cursor drops that boundary row. Subtract a small
# lookback from the queried ``since`` so the boundary is included; CDC upsert
# dedupes any re-fetched overlap. Overridable per table via ``lookback_seconds``.
DEFAULT_LOOKBACK_SECONDS = 1


class MailchimpLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Mailchimp Marketing API."""

    def __init__(self, options: dict) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Mailchimp connector requires an 'api_key' option.")
        self._api_key = api_key

        # Derive the data center from the key suffix (e.g. "...-us21" -> "us21").
        # Fall back to the documented 'server_prefix' option for legacy keys
        # with no suffix.
        dc: Optional[str] = None
        if "-" in api_key:
            dc = api_key.rsplit("-", 1)[1] or None
        if not dc:
            dc = options.get("server_prefix")
        if not dc:
            raise ValueError(
                "Could not determine the Mailchimp data center. The API key has "
                "no '-<dc>' suffix; supply a 'server_prefix' option, e.g. 'us21'."
            )
        # Validate the data center so it cannot inject a different host/path
        # into the base URL (it is only ever a short region code like 'us21').
        if not re.fullmatch(r"[a-z]{2}\d+", dc):
            raise ValueError(
                f"Invalid Mailchimp data center '{dc}'; expected a region code "
                "like 'us21'."
            )
        self._base_url = f"https://{dc}.api.mailchimp.com/3.0"

        # Freeze the window upper bound at init time so a single
        # Trigger.AvailableNow run terminates instead of chasing new data.
        # Mailchimp timestamps are ISO 8601 with a "+00:00" offset.
        self._init_ts = datetime.now(timezone.utc).isoformat()

    # ---- picklable auth helpers -----------------------------------------

    def _auth(self) -> Tuple[str, str]:
        """Build the HTTP Basic auth tuple per request (kept off ``self``)."""
        return (_USERNAME, self._api_key)

    def _request_with_retry(self, path: str, params: Optional[dict] = None):
        """GET ``path`` with exponential backoff on 429/500/503.

        429 is Mailchimp's rate-limit signal (max 10 concurrent connections
        per key); 500/503 are transient server errors. Transient transport
        errors (``ConnectionError``/``Timeout``) are retried on the same
        backoff so an executor-side read survives a blip; the last exception
        is re-raised if every attempt fails.
        """
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF
        resp = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    url, params=params or {}, auth=self._auth(), timeout=60
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        return resp

    # ---- interface methods ----------------------------------------------

    def list_tables(self) -> list[str]:
        """The four static top-level tables this connector supports."""
        return ["campaigns", "reports", "lists", "members"]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        cfg = TABLE_CONFIG[table_name]
        return {
            "primary_keys": list(cfg["primary_keys"]),
            "cursor_field": cfg["cursor_field"],
            "ingestion_type": "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        table_options = table_options or {}
        cfg = TABLE_CONFIG[table_name]
        if cfg["nested"]:
            return self._read_members_window(start_offset, table_options)
        return self._read_window(table_name, start_offset, table_options)

    # ---- validation ------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_CONFIG:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {self.list_tables()}"
            )

    # ---- pagination ------------------------------------------------------

    def _fetch_page(
        self, path: str, resource_key: str, params: dict
    ) -> Tuple[List[dict], int]:
        """One list GET. Returns (records, total_items)."""
        resp = self._request_with_retry(path, params)
        if resp.status_code != 200:
            # Truncate the body so a verbose error can't dump large/sensitive
            # payloads into pipeline logs. Mailchimp errors are short RFC 7807
            # problem-details, so the head is enough to diagnose.
            detail = (resp.text or "")[:500]
            raise RuntimeError(
                f"Mailchimp API error for {path}: {resp.status_code} {detail}"
            )
        body = resp.json()
        records = body.get(resource_key) or []
        total = int(body.get("total_items", 0) or 0)
        return records, total

    def _paginate(
        self,
        path: str,
        resource_key: str,
        base_params: dict,
        max_records: Optional[int] = None,
    ) -> Tuple[List[dict], bool]:
        """Page through a list endpoint with offset/count.

        Stops on a short page (fewer than ``count`` rows), when
        ``offset >= total_items``, or when ``max_records`` is reached.
        Short-page detection is the primary terminator so the loop ends even
        when the source does not populate ``total_items``.

        Returns ``(records, exhausted)`` where ``exhausted`` is True when the
        endpoint was drained for the current window (short page / total
        reached) and False when the loop bailed on the ``max_records`` cap.
        The caller uses that flag to decide whether it is safe to advance the
        cursor to ``window_end`` (drained) or must resume mid-window.
        """
        count = int(base_params.get("count", DEFAULT_PAGE_COUNT))
        records: List[dict] = []
        offset = 0
        exhausted = True
        while True:
            params = dict(base_params)
            params["count"] = count
            params["offset"] = offset
            batch, total = self._fetch_page(path, resource_key, params)
            records.extend(batch)

            if len(batch) < count:
                break
            offset += count
            if 0 < total <= offset:
                break
            if max_records is not None and len(records) >= max_records:
                # Bailed on the cap with a full last page: more rows may remain
                # in this window.
                exhausted = False
                break
        return records, exhausted

    # ---- incremental window reads ---------------------------------------

    def _resolve_since(
        self, table_name: str, start_offset: dict, table_options: dict, cfg: dict
    ) -> Optional[str]:
        """Resolve the window lower bound.

        Order: prior offset cursor -> user-supplied ``start_timestamp`` option
        -> auto-discover the oldest cursor value from the source.
        """
        since = start_offset.get("cursor") if start_offset else None
        if not since:
            since = table_options.get("start_timestamp")
        if not since:
            since = self._peek_oldest_cursor(table_name, cfg)
        return since

    def _peek_oldest_cursor(self, table_name: str, cfg: dict) -> Optional[str]:
        """Discover the oldest cursor value with a lightweight probe.

        For endpoints that honour sort (campaigns, members) request a single
        ascending record. For the rest (reports, lists), which are small in
        cardinality, fetch the first full page and take the minimum cursor.
        """
        cursor_field = cfg["cursor_field"]
        if cfg["nested"]:
            # members: probe EVERY audience and take the global minimum cursor.
            # All members share one cursor window, so seeding ``since`` from the
            # first audience's oldest member would permanently skip any member
            # in another audience with an older ``last_changed`` (the exclusive
            # ``since_*`` filter excludes it on every subsequent window).
            lists, _ = self._paginate("/lists", "lists", {"count": DEFAULT_PAGE_COUNT})
            oldest: Optional[str] = None
            for lst in lists:
                list_id = lst.get("id")
                if not list_id:
                    continue
                cursor = self._peek_from_path(
                    cfg["path"].format(list_id=list_id),
                    cfg["resource_key"],
                    cursor_field,
                    supports_sort=cfg["supports_sort"],
                )
                if cursor and (oldest is None or self._to_dt(cursor) < self._to_dt(oldest)):
                    oldest = cursor
            return oldest
        return self._peek_from_path(
            cfg["path"], cfg["resource_key"], cursor_field, cfg["supports_sort"]
        )

    def _peek_from_path(
        self, path: str, resource_key: str, cursor_field: str, supports_sort: bool
    ) -> Optional[str]:
        if supports_sort:
            params = {
                "count": 1,
                "offset": 0,
                "sort_field": cursor_field,
                "sort_dir": "ASC",
            }
            batch, _ = self._fetch_page(path, resource_key, params)
            if batch:
                return batch[0].get(cursor_field)
            return None
        # No sort support: drain all pages and take the global min cursor.
        # ``_paginate`` enumerates every page when ``max_records`` is None, so a
        # global-oldest row on a later page is not missed. This probe runs only
        # on first-run discovery, so the full enumeration is a one-time cost.
        batch, _ = self._paginate(path, resource_key, {"count": DEFAULT_PAGE_COUNT})
        cursors = [r.get(cursor_field) for r in batch if r.get(cursor_field)]
        return min(cursors) if cursors else None

    @staticmethod
    def _to_dt(ts: str) -> datetime:
        """Parse an ISO 8601 cursor to an aware UTC datetime.

        Mailchimp emits ``+00:00`` offsets, but a user-supplied
        ``start_timestamp`` (or a re-fetched cursor) may use a ``Z`` suffix.
        Comparing those as raw strings is wrong (``'...Z'`` sorts after
        ``'...+00:00'`` lexically even for the same instant), so every cursor
        comparison and arithmetic goes through this parser instead.
        """
        normalized = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _window_end(self, since: str, window_seconds: int) -> str:
        """Compute the window upper bound, capped at ``_init_ts``."""
        end_dt = self._to_dt(since) + timedelta(seconds=window_seconds)
        return min(end_dt, self._to_dt(self._init_ts)).isoformat()

    def _apply_lookback(self, since: str, lookback_seconds: int) -> str:
        """Subtract a lookback from an ISO cursor for the (exclusive) API filter.

        Mailchimp's ``since_*`` filter is strictly greater-than, so querying at
        exactly the boundary cursor drops that row. Shifting ``since`` back by a
        few seconds includes it; CDC upsert dedupes the re-fetched overlap. The
        stored offset keeps the raw cursor — only the queried value is shifted.
        """
        if lookback_seconds <= 0:
            return since
        return (self._to_dt(since) - timedelta(seconds=lookback_seconds)).isoformat()

    def _read_window(
        self, table_name: str, start_offset: dict, table_options: dict
    ) -> Tuple[Iterator[dict], dict]:
        """Sliding-window incremental read for a single (non-nested) resource."""
        cfg = TABLE_CONFIG[table_name]
        cursor_field = cfg["cursor_field"]

        since = self._resolve_since(table_name, start_offset, table_options, cfg)
        if not since:
            raise ValueError(
                f"Cannot determine a starting cursor for '{table_name}'. "
                f"Provide 'start_timestamp' in table_options."
            )
        # Already caught up to init time — nothing new this run.
        if self._to_dt(since) >= self._to_dt(self._init_ts):
            return iter([]), start_offset if start_offset else {}

        window_seconds = int(
            table_options.get("window_seconds", DEFAULT_WINDOW_SECONDS)
        )
        max_records = int(
            table_options.get("max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH)
        )
        lookback_seconds = int(
            table_options.get("lookback_seconds", DEFAULT_LOOKBACK_SECONDS)
        )
        window_end = self._window_end(since, window_seconds)

        base_params = {
            "count": DEFAULT_PAGE_COUNT,
            # since_* is exclusive: shift back so the boundary row is included.
            cfg["since_param"]: self._apply_lookback(since, lookback_seconds),
            cfg["before_param"]: window_end,
        }
        # The ``max_records`` mid-window cap is only safe on SORTED endpoints,
        # where rows arrive in ascending cursor order and the max cursor seen is
        # a valid resume point. UNSORTED endpoints (reports, lists) return the
        # window in server-default order, so a partial fetch cannot resume
        # without risking dropped rows; drain the whole window instead (these
        # tables are low-cardinality and the window bounds the volume).
        if cfg["supports_sort"]:
            base_params["sort_field"] = cursor_field
            base_params["sort_dir"] = "ASC"
            page_cap: Optional[int] = max_records
        else:
            page_cap = None

        records, window_drained = self._paginate(
            cfg["path"], cfg["resource_key"], base_params, page_cap
        )

        # Same-second cursor cluster larger than the cap: every capped row shares
        # one cursor value, so the resume point (`_max_cursor`) would equal
        # `since` and the next call re-fetches the same first page forever —
        # dropping everything past it. When the cap trips but the resume cursor
        # can't advance past the window start, re-drain the whole window uncapped;
        # ``window_seconds`` bounds the volume so peak memory stays bounded.
        # (Only reachable on sorted endpoints; the unsorted path never sets a cap.)
        if not window_drained and page_cap is not None:
            resume = self._max_cursor(records, cursor_field, window_end)
            if self._to_dt(resume) <= self._to_dt(since):
                records, window_drained = self._paginate(
                    cfg["path"], cfg["resource_key"], base_params, None
                )

        if window_drained:
            # Endpoint fully drained for this window: advance cleanly to the end.
            end_offset = {"cursor": window_end}
        else:
            # Cap hit mid-window on a sorted endpoint: resume from the max cursor
            # seen (everything below it is already fetched). CDC upsert tolerates
            # the re-fetch of the window head on the next call.
            end_offset = {"cursor": self._max_cursor(records, cursor_field, window_end)}

        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(self._project(records, table_name)), end_offset

    def _read_members_window(
        self, start_offset: dict, table_options: dict
    ) -> Tuple[Iterator[dict], dict]:
        """Sliding-window incremental read for the nested ``members`` table.

        The whole members population is treated under one cursor window. We
        enumerate every audience, apply the ``since``/``before`` window per
        list, and yield the members **lazily, one audience at a time**, then
        advance the cursor to the window end. Yielding lazily bounds peak
        memory to a single audience's members instead of the union of every
        audience. The window is drained completely (no client-side truncation)
        so no record with a cursor below ``window_end`` is skipped on the next
        call; ``max_records_per_batch`` is therefore best-effort here (windows
        are the sizing knob). Keep ``window_seconds`` small on large accounts.
        """
        cfg = TABLE_CONFIG["members"]
        cursor_field = cfg["cursor_field"]

        since = self._resolve_since("members", start_offset, table_options, cfg)
        if not since:
            raise ValueError(
                "Cannot determine a starting cursor for 'members'. "
                "Provide 'start_timestamp' in table_options."
            )
        if self._to_dt(since) >= self._to_dt(self._init_ts):
            return iter([]), start_offset if start_offset else {}

        window_seconds = int(
            table_options.get("window_seconds", DEFAULT_WINDOW_SECONDS)
        )
        lookback_seconds = int(
            table_options.get("lookback_seconds", DEFAULT_LOOKBACK_SECONDS)
        )
        window_end = self._window_end(since, window_seconds)
        api_since = self._apply_lookback(since, lookback_seconds)

        # Enumerate every audience, then fan out members per list within the
        # same window.
        lists, _ = self._paginate("/lists", "lists", {"count": DEFAULT_PAGE_COUNT})

        # Window is drained by construction — advance the cursor to its end.
        end_offset = {"cursor": window_end}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        def _iter_members() -> Iterator[dict]:
            """Fan out per audience, yielding lazily to bound peak memory."""
            for lst in lists:
                list_id = lst.get("id")
                if not list_id:
                    continue
                base_params = {
                    "count": DEFAULT_PAGE_COUNT,
                    # since_* is exclusive: shift back to include the boundary.
                    cfg["since_param"]: api_since,
                    cfg["before_param"]: window_end,
                    "sort_field": cursor_field,
                    "sort_dir": "ASC",
                }
                path = cfg["path"].format(list_id=list_id)
                members, _ = self._paginate(path, cfg["resource_key"], base_params)
                for m in members:
                    # Preserve provenance even if the payload omits list_id.
                    if not m.get("list_id"):
                        m["list_id"] = list_id
                yield from self._project(members, "members")

        return _iter_members(), end_offset

    # ---- record shaping --------------------------------------------------

    @staticmethod
    def _max_cursor(records: List[dict], cursor_field: str, fallback: str) -> str:
        values = [r.get(cursor_field) for r in records if r.get(cursor_field)]
        return max(values) if values else fallback

    @staticmethod
    def _project(records: List[dict], table_name: str) -> List[dict]:
        """Serialize dynamic-key fields to JSON strings.

        This is the one place the connector reshapes a value: ``merge_fields``
        and ``interests`` have per-audience key sets that cannot map to a fixed
        StructType, so they are stored as raw-JSON string columns (see
        ``mailchimp_schemas``). All other fields are returned as parsed by the
        API for the framework to coerce.
        """
        json_fields = JSON_STRING_FIELDS.get(table_name, [])
        if not json_fields:
            return records
        for rec in records:
            for fld in json_fields:
                if fld in rec and rec[fld] is not None and not isinstance(rec[fld], str):
                    rec[fld] = json.dumps(rec[fld], ensure_ascii=False)
        return records
