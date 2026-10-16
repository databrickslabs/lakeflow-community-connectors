"""PayPal connector for Lakeflow Community Connectors.

Reads Transaction Search (transactions, balances), Customer Disputes,
Invoicing v2 and Catalog Products via the PayPal REST APIs, authenticating
with the OAuth 2.0 client-credentials grant.
"""

import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from urllib.parse import parse_qs, quote, urlsplit

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.paypal.paypal_schemas import (
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)

BASE_URLS = {
    "production": "https://api-m.paypal.com",
    "sandbox": "https://api-m.sandbox.paypal.com",
}

REQUEST_TIMEOUT = 60
MAX_RETRIES = 4
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
TOKEN_REFRESH_MARGIN_SECONDS = 60

# Transaction Search limits: 31-day max range, 500 records/page, 3 years history.
TRANSACTIONS_MAX_WINDOW_SECONDS = 31 * 86400
TRANSACTIONS_DEFAULT_WINDOW_SECONDS = 7 * 86400
TRANSACTIONS_MAX_PAGE_SIZE = 500
TRANSACTIONS_DEFAULT_LOOKBACK_SECONDS = 86400
TRANSACTIONS_MAX_HISTORY = timedelta(days=3 * 365)

DISPUTES_MAX_PAGE_SIZE = 50
DISPUTES_DEFAULT_LOOKBACK_SECONDS = 7 * 86400

INVOICES_MAX_PAGE_SIZE = 100
PRODUCTS_MAX_PAGE_SIZE = 20

DEFAULT_MAX_RECORDS_PER_BATCH = 1000

_COMPACT_TZ_RE = re.compile(r"([+-]\d{2})(\d{2})$")


class _ResultSetTooLarge(Exception):
    """Transaction Search rejected a window holding more than 10,000 records."""


class PaypalLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for PayPal."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._client_id = options.get("client_id")
        self._client_secret = options.get("client_secret")
        if not self._client_id or not self._client_secret:
            raise ValueError("PayPal connector requires 'client_id' and 'client_secret'.")

        environment = (options.get("environment") or "production").strip().lower()
        if options.get("base_url"):
            self._base_url = options["base_url"].rstrip("/")
        elif environment in BASE_URLS:
            self._base_url = BASE_URLS[environment]
        else:
            raise ValueError(
                f"Unsupported environment '{environment}'. Use one of {sorted(BASE_URLS)}."
            )

        self._session = requests.Session()
        self._access_token: str | None = None
        self._token_expires_at = 0.0

        # Cap reads at init time so a trigger never chases new data.
        # The next trigger creates a fresh instance and picks up from here.
        self._init_dt = datetime.now(timezone.utc).replace(microsecond=0)
        self._init_ts = _format_ts(self._init_dt)

        # Tables whose lookback has already been applied in this trigger.
        self._lookback_applied: set[str] = set()

    # ------------------------------------------------------------------
    # HTTP layer
    # ------------------------------------------------------------------

    def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        resp = None
        for attempt in range(MAX_RETRIES):
            resp = self._session.post(
                f"{self._base_url}/v1/oauth2/token",
                auth=(self._client_id, self._client_secret),
                data={"grant_type": "client_credentials"},
                headers={"Accept": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                break
            if attempt < MAX_RETRIES - 1:
                _sleep_backoff(resp, attempt)

        if resp.status_code != 200:
            raise RuntimeError(
                f"PayPal OAuth token request failed ({resp.status_code}): {resp.text[:500]}"
            )
        body = resp.json()
        self._access_token = body["access_token"]
        expires_in = int(body.get("expires_in", 32400))
        self._token_expires_at = time.time() + max(expires_in - TOKEN_REFRESH_MARGIN_SECONDS, 0)
        return self._access_token

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> requests.Response:
        """Issue an authenticated request with retry on 429/5xx and one token refresh on 401."""
        refreshed = False
        resp = None
        attempt = 0
        while attempt < MAX_RETRIES:
            headers = {
                "Authorization": f"Bearer {self._get_access_token()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            resp = self._session.request(
                method,
                f"{self._base_url}{path}",
                params=params,
                json=json_body,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 401 and not refreshed:
                self._access_token = None
                refreshed = True
                continue
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                return resp
            if attempt < MAX_RETRIES - 1:
                _sleep_backoff(resp, attempt)
            attempt += 1
        return resp

    # ------------------------------------------------------------------
    # Interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        table_options = table_options or {}
        if table_name == "transactions":
            return self._read_transactions(start_offset, table_options)
        if table_name == "disputes":
            return self._read_disputes(start_offset, table_options)
        if table_name == "balances":
            return self._read_balances(table_options), {}
        if table_name == "invoices":
            return self._read_invoices(table_options), {}
        if table_name == "products":
            return self._iter_products(), {}
        return self._read_product_details(), {}

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {SUPPORTED_TABLES}"
            )

    # ------------------------------------------------------------------
    # transactions — sliding time-window over start_date/end_date
    # ------------------------------------------------------------------

    def _read_transactions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding-window incremental read.

        The API does not document a sort order, so every window is drained
        completely and the cursor advances to the window end.  Consecutive
        windows are read within a single call until ``max_records_per_batch``
        is reached (best-effort) or the init-time cap is hit.
        """
        cursor = start_offset.get("cursor") if start_offset else None
        if cursor and _parse_ts(cursor) >= self._init_dt:
            return iter([]), start_offset

        if cursor:
            since_dt = _parse_ts(cursor)
        elif table_options.get("start_date"):
            since_dt = _parse_ts(table_options["start_date"])
        else:
            since_dt = self._init_dt - TRANSACTIONS_MAX_HISTORY

        window = timedelta(
            seconds=_int_option(
                table_options,
                "window_seconds",
                TRANSACTIONS_DEFAULT_WINDOW_SECONDS,
                maximum=TRANSACTIONS_MAX_WINDOW_SECONDS,
            )
        )
        page_size = _int_option(
            table_options,
            "page_size",
            TRANSACTIONS_MAX_PAGE_SIZE,
            maximum=TRANSACTIONS_MAX_PAGE_SIZE,
        )
        max_records = _int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH
        )

        window_start = since_dt
        if cursor and "transactions" not in self._lookback_applied:
            lookback = _int_option(
                table_options, "lookback_seconds", TRANSACTIONS_DEFAULT_LOOKBACK_SECONDS, minimum=0
            )
            window_start = since_dt - timedelta(seconds=lookback)
            self._lookback_applied.add("transactions")

        records: list[dict] = []
        while window_start < self._init_dt:
            window_end = min(window_start + window, self._init_dt)
            batch, window_end = self._fetch_transaction_window(window_start, window_end, page_size)
            records.extend(batch)
            window_start = window_end
            if len(records) >= max_records and window_start >= since_dt:
                break

        end_offset = {"cursor": _format_ts(max(window_start, since_dt))}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _fetch_transaction_window(
        self, start: datetime, end: datetime, page_size: int
    ) -> tuple[list[dict], datetime]:
        """Read one window, halving it while PayPal reports RESULTSET_TOO_LARGE.

        Returns the records and the (possibly shrunk) window end actually read.
        """
        while True:
            try:
                return self._fetch_transaction_pages(start, end, page_size), end
            except _ResultSetTooLarge:
                span = (end - start).total_seconds()
                if span <= 1:
                    raise RuntimeError(
                        "PayPal Transaction Search returned RESULTSET_TOO_LARGE for a "
                        f"1-second window starting at {_format_ts(start)}."
                    )
                end = start + timedelta(seconds=max(int(span // 2), 1))

    def _fetch_transaction_pages(
        self, start: datetime, end: datetime, page_size: int
    ) -> list[dict]:
        records: list[dict] = []
        page = 1
        while True:
            params = {
                "start_date": _format_ts(start),
                "end_date": _format_ts(end),
                "fields": "all",
                "page_size": str(page_size),
                "page": str(page),
            }
            resp = self._request("GET", "/v1/reporting/transactions", params=params)
            if resp.status_code == 400 and _error_name(resp) == "RESULTSET_TOO_LARGE":
                raise _ResultSetTooLarge()
            _raise_for_status(resp, "transactions")

            body = resp.json()
            details = body.get("transaction_details") or []
            records.extend(_normalize_transaction(d) for d in _clean_records(details))

            if len(details) < page_size:
                break
            total_pages = body.get("total_pages")
            if total_pages is not None and page >= int(total_pages):
                break
            page += 1
        return records

    # ------------------------------------------------------------------
    # disputes — start_time filter + next_page_token pagination
    # ------------------------------------------------------------------

    def _read_disputes(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Incremental read filtered server-side by ``start_time``.

        The list API has no upper time bound and no documented sort order,
        so all pages since the cursor are fetched, sorted client-side by
        ``update_time`` and truncated to ``max_records_per_batch``.  Records
        sharing the truncation timestamp are kept together so the cursor
        always advances.
        """
        cursor = start_offset.get("cursor") if start_offset else None
        if cursor and _parse_ts(cursor) >= self._init_dt:
            return iter([]), start_offset

        if cursor:
            since_dt: datetime | None = _parse_ts(cursor)
        elif table_options.get("start_date"):
            since_dt = _parse_ts(table_options["start_date"])
        else:
            since_dt = None

        query_dt = since_dt
        if cursor and "disputes" not in self._lookback_applied:
            lookback = _int_option(
                table_options, "lookback_seconds", DISPUTES_DEFAULT_LOOKBACK_SECONDS, minimum=0
            )
            query_dt = since_dt - timedelta(seconds=lookback)
            self._lookback_applied.add("disputes")

        page_size = _int_option(
            table_options, "page_size", DISPUTES_MAX_PAGE_SIZE, maximum=DISPUTES_MAX_PAGE_SIZE
        )
        max_records = _int_option(
            table_options, "max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH
        )

        records = sorted(self._fetch_all_disputes(query_dt, page_size), key=_dispute_ts)
        if not records:
            return iter([]), start_offset or {}

        if len(records) > max_records:
            cut = _dispute_ts(records[max_records - 1])
            if since_dt is not None and cut <= since_dt:
                newer = [_dispute_ts(r) for r in records if _dispute_ts(r) > since_dt]
                if newer:
                    cut = newer[0]
            records = [r for r in records if _dispute_ts(r) <= cut]

        end_dt = _dispute_ts(records[-1])
        if since_dt is not None:
            end_dt = max(end_dt, since_dt)
        end_offset = {"cursor": _format_ts_ms(end_dt)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _fetch_all_disputes(self, start_dt: datetime | None, page_size: int) -> list[dict]:
        records: list[dict] = []
        next_page_token = None
        while True:
            params = {"page_size": str(page_size)}
            if start_dt is not None:
                params["start_time"] = _format_ts_ms(start_dt)
            if next_page_token:
                params["next_page_token"] = next_page_token

            resp = self._request("GET", "/v1/customer/disputes", params=params)
            _raise_for_status(resp, "disputes")
            body = resp.json()
            items = body.get("items") or []
            records.extend(_clean_records(items))

            next_page_token = _next_page_token(body.get("links"))
            if not items or not next_page_token:
                break
        return records

    # ------------------------------------------------------------------
    # Snapshot tables
    # ------------------------------------------------------------------

    def _read_balances(self, table_options: dict[str, str]) -> Iterator[dict]:
        params = {}
        if table_options.get("currency_code"):
            params["currency_code"] = table_options["currency_code"]
        if table_options.get("as_of_time"):
            params["as_of_time"] = table_options["as_of_time"]

        resp = self._request("GET", "/v1/reporting/balances", params=params or None)
        _raise_for_status(resp, "balances")
        body = resp.json()
        account_fields = {
            "account_id": body.get("account_id"),
            "as_of_time": body.get("as_of_time"),
            "last_refresh_time": body.get("last_refresh_time"),
        }
        for balance in _clean_records(body.get("balances")):
            yield {**balance, **account_fields}

    def _read_invoices(self, table_options: dict[str, str]) -> Iterator[dict]:
        page_size = _int_option(
            table_options, "page_size", INVOICES_MAX_PAGE_SIZE, maximum=INVOICES_MAX_PAGE_SIZE
        )
        search_body: dict[str, Any] = {"fields": ["items", "payments", "refunds"]}
        if table_options.get("start_date"):
            search_body["creation_date_range"] = {
                "start": _format_ts(_parse_ts(table_options["start_date"])),
                "end": self._init_ts,
            }

        page = 1
        while True:
            params = {"page": str(page), "page_size": str(page_size), "total_required": "true"}
            resp = self._request(
                "POST", "/v2/invoicing/search-invoices", params=params, json_body=search_body
            )
            _raise_for_status(resp, "invoices")
            body = resp.json()
            items = body.get("items") or []
            yield from _clean_records(items)

            if len(items) < page_size:
                break
            total_pages = body.get("total_pages")
            if total_pages is not None and page >= int(total_pages):
                break
            page += 1

    def _iter_products(self) -> Iterator[dict]:
        page = 1
        while True:
            params = {
                "page": str(page),
                "page_size": str(PRODUCTS_MAX_PAGE_SIZE),
                "total_required": "true",
            }
            resp = self._request("GET", "/v1/catalogs/products", params=params)
            _raise_for_status(resp, "products")
            body = resp.json()
            products = body.get("products") or []
            yield from _clean_records(products)

            if len(products) < PRODUCTS_MAX_PAGE_SIZE:
                break
            total_pages = body.get("total_pages")
            if total_pages is not None and page >= int(total_pages):
                break
            page += 1

    def _read_product_details(self) -> Iterator[dict]:
        """List products, then fetch each product's full detail record."""
        product_ids = [p["id"] for p in self._iter_products() if p.get("id")]
        for product_id in product_ids:
            resp = self._request("GET", f"/v1/catalogs/products/{quote(product_id, safe='')}")
            if resp.status_code == 404:
                continue
            _raise_for_status(resp, "product_details")
            yield from _clean_records([resp.json()])


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _sleep_backoff(resp: requests.Response, attempt: int) -> None:
    retry_after = resp.headers.get("Retry-After")
    if retry_after and retry_after.isdigit():
        delay = float(retry_after)
    else:
        delay = (2**attempt) + random.uniform(0, 0.5)
    time.sleep(delay)


def _raise_for_status(resp: requests.Response, table_name: str) -> None:
    if resp.status_code != 200:
        raise RuntimeError(
            f"PayPal API error reading '{table_name}' ({resp.status_code}): {resp.text[:500]}"
        )


def _error_name(resp: requests.Response) -> str | None:
    try:
        body = resp.json()
    except ValueError:
        return None
    return body.get("name") if isinstance(body, dict) else None


def _next_page_token(links: list | None) -> str | None:
    for link in links or []:
        if link.get("rel") == "next" and link.get("href"):
            tokens = parse_qs(urlsplit(link["href"]).query).get("next_page_token")
            if tokens:
                return tokens[0]
    return None


def _strip_empty(value: Any) -> Any:
    """Recursively replace empty objects with None.

    PayPal returns ``{}`` for absent sub-objects (e.g. ``shipping_info``),
    which Spark cannot coerce into a StructType.
    """
    if isinstance(value, dict):
        cleaned = {k: _strip_empty(v) for k, v in value.items()}
        return cleaned or None
    if isinstance(value, list):
        return [_strip_empty(v) for v in value]
    return value


def _clean_records(items: list | None) -> Iterator[dict]:
    for item in items or []:
        cleaned = _strip_empty(item)
        if cleaned:
            yield cleaned


def _normalize_transaction(record: dict) -> dict:
    info = record.get("transaction_info") or {}
    record["transaction_id"] = info.get("transaction_id")
    record["transaction_updated_date"] = info.get("transaction_updated_date")
    return record


_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


def _dispute_ts(record: dict) -> datetime:
    value = record.get("update_time")
    return _parse_ts(value) if value else _EPOCH


def _parse_ts(value: str) -> datetime:
    """Parse RFC 3339 timestamps, including PayPal's compact ``+0000`` offsets."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    text = _COMPACT_TZ_RE.sub(r"\1:\2", text) if "T" in text else text
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_ts_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def _int_option(
    table_options: dict[str, str],
    key: str,
    default: int,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    raw = table_options.get(key)
    value = int(raw) if raw not in (None, "") else default
    value = max(value, minimum)
    return min(value, maximum) if maximum is not None else value
