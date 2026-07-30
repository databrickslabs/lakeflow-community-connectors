"""Lakeflow community connector for QuickBooks Online.

All six entities support a checkpointed snapshot-to-incremental handoff based
on ``MetaData.LastUpdatedTime``. Transaction deletes are read from the
QuickBooks CDC endpoint; list-entity inactivation remains an ordinary update.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.quickbooks.quickbooks_schemas import (
    TABLE_SCHEMAS,
)

TABLE_TO_ENTITY = {
    "customers": "Customer",
    "vendors": "Vendor",
    "accounts": "Account",
    "items": "Item",
    "invoices": "Invoice",
    "bills": "Bill",
}
LIST_TABLES = frozenset({"customers", "vendors", "accounts", "items"})
DELETABLE_TABLES = frozenset({"invoices", "bills"})

RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 5
DEFAULT_INCREMENTAL_OVERLAP_SECONDS = 60
DEFAULT_MAX_INCREMENTAL_WINDOW_SECONDS = 86400
DEFAULT_DELETE_OVERLAP_SECONDS = 60
DEFAULT_INITIAL_DELETE_LOOKBACK_SECONDS = 300
MAX_CDC_LOOKBACK_SECONDS = 30 * 86400
MAX_CDC_OBJECTS = 1000
OFFSET_VERSION = 2
OFFSET_VERSION_KEY = "version"
OFFSET_CURSOR_KEY = "updated_through"
OFFSET_REALM_KEY = "realm_id"
OFFSET_TABLE_KEY = "table_name"
OFFSET_FLOW_KEY = "flow"
CURSOR_FIELD = "last_updated_at"


class QuickBooksApiClient:
    """Small, stateless QuickBooks Online query client."""

    def __init__(
        self,
        *,
        access_token: str,
        realm_id: str,
        environment: str,
        minor_version: int,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        if environment not in {"sandbox", "production"}:
            raise ValueError("environment must be 'sandbox' or 'production'")
        self._access_token = access_token
        self._realm_id = realm_id
        self._environment = environment
        self._minor_version = minor_version
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries

    @property
    def _base_url(self) -> str:
        if self._environment == "sandbox":
            return "https://sandbox-quickbooks.api.intuit.com"
        return "https://quickbooks.api.intuit.com"

    def iter_entity(
        self,
        entity: str,
        *,
        page_size: int,
        where_clause: str | None = None,
    ) -> Iterator[dict]:
        """Yield a complete positional QuickBooks query one page at a time."""
        start_position = 1
        while True:
            query = f"SELECT * FROM {entity}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" STARTPOSITION {start_position} MAXRESULTS {page_size}"
            body = self._get_query(query)
            page = body.get("QueryResponse", {}).get(entity, [])
            if not isinstance(page, list):
                raise RuntimeError(f"QuickBooks {entity} query returned an invalid row collection")
            yield from (row for row in page if isinstance(row, dict))
            if len(page) < page_size:
                return
            start_position += page_size

    def get_entity_changes(self, entity: str, *, changed_since: str) -> tuple[list[dict], str]:
        """Return one entity's recent CDC objects and the server response time."""
        url = f"{self._base_url}/v3/company/{self._realm_id}/cdc"
        payload = self._get(
            url,
            params={
                "entities": entity,
                "changedSince": changed_since,
                "minorversion": str(self._minor_version),
            },
            operation="CDC request",
        )
        cdc_responses = payload.get("CDCResponse")
        if not isinstance(cdc_responses, list):
            raise RuntimeError("QuickBooks CDC returned an invalid CDCResponse")

        rows: list[dict] = []
        reported_count = 0
        for cdc_response in cdc_responses:
            if not isinstance(cdc_response, dict):
                raise RuntimeError("QuickBooks CDC returned an invalid response entry")
            query_responses = cdc_response.get("QueryResponse", [])
            if not isinstance(query_responses, list):
                raise RuntimeError("QuickBooks CDC returned an invalid QueryResponse")
            for query_response in query_responses:
                if not isinstance(query_response, dict):
                    raise RuntimeError("QuickBooks CDC returned an invalid query entry")
                entity_rows = query_response.get(entity, [])
                if not isinstance(entity_rows, list):
                    raise RuntimeError(
                        f"QuickBooks CDC returned an invalid {entity} row collection"
                    )
                rows.extend(row for row in entity_rows if isinstance(row, dict))
                for count_key in ("maxResults", "totalCount"):
                    count = query_response.get(count_key)
                    if isinstance(count, int):
                        reported_count = max(reported_count, count)

        if len(rows) >= MAX_CDC_OBJECTS or reported_count >= MAX_CDC_OBJECTS:
            raise RuntimeError(
                "QuickBooks CDC reached its 1,000-object response limit; "
                "the delete checkpoint was not advanced. Run ingestion more "
                "frequently or perform a full reconciliation."
            )

        response_time = payload.get("time")
        if not isinstance(response_time, str):
            raise RuntimeError("QuickBooks CDC response is missing its server time")
        return rows, _format_qbo_datetime(_parse_qbo_datetime(response_time))

    def _get_query(self, query: str) -> dict:
        url = f"{self._base_url}/v3/company/{self._realm_id}/query"
        return self._get(
            url,
            params={"query": query, "minorversion": str(self._minor_version)},
            operation="query",
        )

    def _get(self, url: str, *, params: dict[str, str], operation: str) -> dict:
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }

        for attempt in range(self._max_retries):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self._timeout_seconds,
                )
            except requests.RequestException as exc:
                if attempt == self._max_retries - 1:
                    raise RuntimeError("QuickBooks request failed after retry exhaustion") from exc
                self._sleep_before_retry(attempt, None)
                continue

            if response.status_code in {401, 403}:
                raise PermissionError(
                    "QuickBooks authentication failed; refresh or recreate the "
                    "Unity Catalog connection"
                )
            if response.status_code not in RETRIABLE_STATUS_CODES:
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    raise RuntimeError(
                        f"QuickBooks {operation} failed with HTTP {response.status_code}"
                    ) from exc
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise RuntimeError("QuickBooks returned an invalid JSON response") from exc
                if not isinstance(payload, dict):
                    raise RuntimeError("QuickBooks returned an invalid JSON object")
                return payload

            if attempt == self._max_retries - 1:
                raise RuntimeError(
                    "QuickBooks request failed after retry exhaustion "
                    f"(HTTP {response.status_code})"
                )
            self._sleep_before_retry(attempt, response.headers.get("Retry-After"))

        raise AssertionError("unreachable retry state")

    @staticmethod
    def _sleep_before_retry(attempt: int, retry_after: str | None) -> None:
        delay = _retry_after_seconds(retry_after)
        if delay is None:
            delay = min(2**attempt, 30) + random.uniform(0, 0.25)
        time.sleep(delay)


class QuickBooksLakeflowConnect(LakeflowConnect):
    """QuickBooks connector with checkpointed incremental-update support."""

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        access_token = options.get("access_token", "").strip()
        realm_id = options.get("realm_id", "").strip()
        if not access_token:
            raise ValueError(
                "QuickBooks requires an access_token injected by the Unity Catalog OAuth connection"
            )
        if not realm_id:
            raise ValueError("QuickBooks requires realm_id for the authorized company")
        self._realm_id = realm_id

        self._client = QuickBooksApiClient(
            access_token=access_token,
            realm_id=realm_id,
            environment=options.get("environment", "production").strip().lower(),
            minor_version=int(options.get("minor_version", "75")),
            timeout_seconds=int(options.get("timeout_seconds", str(DEFAULT_TIMEOUT_SECONDS))),
            max_retries=int(options.get("max_retries", str(DEFAULT_MAX_RETRIES))),
        )
        # Freeze the upper bound for this Data Source instance. AvailableNow
        # must converge instead of chasing records written while it is running.
        self._init_ts = _format_qbo_datetime(_utc_now())

    def list_tables(self) -> list[str]:
        return list(TABLE_TO_ENTITY)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        del table_options
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        del table_options
        self._validate_table(table_name)
        return {
            "primary_keys": ["realm_id", "id"],
            "cursor_field": CURSOR_FIELD,
            "ingestion_type": (
                "cdc_with_deletes" if table_name in DELETABLE_TABLES else "cdc"
            ),
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        page_size = int(table_options.get("page_size", str(DEFAULT_PAGE_SIZE)))
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")

        return self._read_incrementally(
            table_name,
            start_offset,
            table_options,
            page_size=page_size,
        )

    def _read_incrementally(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        *,
        page_size: int,
    ) -> tuple[Iterator[dict], dict]:
        cursor = _parse_offset(
            start_offset,
            expected_realm_id=self._realm_id,
            expected_table_name=table_name,
            expected_flow="updates",
        )
        init_dt = _parse_qbo_datetime(self._init_ts)
        entity = TABLE_TO_ENTITY[table_name]

        # First call: emit the complete snapshot, but checkpoint the time at
        # which this reader was initialized. Changes racing with the snapshot
        # are replayed by the overlap on the next trigger.
        if cursor is None:
            where_clause = "Active IN (true, false)" if table_name in LIST_TABLES else None
            records = (
                _normalize_cdc_entity(table_name, row, realm_id=self._realm_id)
                for row in self._client.iter_entity(
                    entity,
                    page_size=page_size,
                    where_clause=where_clause,
                )
            )
            return records, _offset(
                self._init_ts,
                realm_id=self._realm_id,
                table_name=table_name,
                flow="updates",
            )

        cursor_dt = _parse_qbo_datetime(cursor)
        if cursor_dt >= init_dt:
            return iter([]), start_offset

        overlap_seconds = _bounded_int_option(
            table_options,
            "incremental_overlap_seconds",
            default=DEFAULT_INCREMENTAL_OVERLAP_SECONDS,
            minimum=0,
            maximum=3600,
        )
        max_window_seconds = _bounded_int_option(
            table_options,
            "max_incremental_window_seconds",
            default=DEFAULT_MAX_INCREMENTAL_WINDOW_SECONDS,
            minimum=60,
            maximum=604800,
        )
        lower_dt = cursor_dt - timedelta(seconds=overlap_seconds)
        upper_dt = min(
            cursor_dt + timedelta(seconds=max_window_seconds),
            init_dt,
        )
        lower = _format_qbo_datetime(lower_dt)
        upper = _format_qbo_datetime(upper_dt)
        predicates = []
        if table_name in LIST_TABLES:
            predicates.append("Active IN (true, false)")
        predicates.extend(
            [
                f"MetaData.LastUpdatedTime >= '{lower}'",
                f"MetaData.LastUpdatedTime <= '{upper}'",
            ]
        )
        where_clause = " AND ".join(predicates)
        records = (
            _normalize_cdc_entity(table_name, row, realm_id=self._realm_id)
            for row in self._client.iter_entity(
                entity,
                page_size=page_size,
                where_clause=where_clause,
            )
        )
        return records, _offset(
            upper,
            realm_id=self._realm_id,
            table_name=table_name,
            flow="updates",
        )

    def read_table_deletes(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        """Read hard-delete tombstones for QuickBooks transaction entities."""
        self._validate_table(table_name)
        if table_name not in DELETABLE_TABLES:
            raise ValueError(
                f"QuickBooks {TABLE_TO_ENTITY[table_name]} is inactivated, not hard-deleted"
            )

        cursor = _parse_offset(
            start_offset,
            expected_realm_id=self._realm_id,
            expected_table_name=table_name,
            expected_flow="deletes",
        )
        init_dt = _parse_qbo_datetime(self._init_ts)
        if cursor is not None and _parse_qbo_datetime(cursor) >= init_dt:
            return iter([]), start_offset

        if cursor is None:
            lookback_seconds = _bounded_int_option(
                table_options,
                "initial_delete_lookback_seconds",
                default=DEFAULT_INITIAL_DELETE_LOOKBACK_SECONDS,
                minimum=0,
                maximum=86400,
            )
            lower_dt = init_dt - timedelta(seconds=lookback_seconds)
        else:
            cursor_dt = _parse_qbo_datetime(cursor)
            overlap_seconds = _bounded_int_option(
                table_options,
                "delete_overlap_seconds",
                default=DEFAULT_DELETE_OVERLAP_SECONDS,
                minimum=0,
                maximum=3600,
            )
            lower_dt = cursor_dt - timedelta(seconds=overlap_seconds)
            if init_dt - lower_dt > timedelta(seconds=MAX_CDC_LOOKBACK_SECONDS):
                raise RuntimeError(
                    "QuickBooks CDC can only recover deletes from the previous 30 days; "
                    "the delete checkpoint was not advanced. Perform a full reconciliation."
                )

        changed_since = _format_qbo_datetime(lower_dt)
        entity = TABLE_TO_ENTITY[table_name]
        changes, response_time = self._client.get_entity_changes(
            entity,
            changed_since=changed_since,
        )
        response_dt = _parse_qbo_datetime(response_time)
        if response_dt < lower_dt:
            raise RuntimeError("QuickBooks CDC server time precedes changedSince")

        tombstones = (
            _normalize_delete_entity(table_name, row, realm_id=self._realm_id)
            for row in changes
            if str(row.get("status", "")).lower() == "deleted"
        )
        return tombstones, _offset(
            response_time,
            realm_id=self._realm_id,
            table_name=table_name,
            flow="deletes",
        )

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_TO_ENTITY:
            raise ValueError(
                f"Unsupported QuickBooks table '{table_name}'. "
                f"Supported tables: {self.list_tables()}"
            )


def _normalize_entity(table_name: str, row: dict, *, realm_id: str) -> dict:
    entity_id = row.get("Id")
    if entity_id in {None, ""}:
        raise RuntimeError("QuickBooks entity is missing Id")
    metadata = row.get("MetaData") if isinstance(row.get("MetaData"), dict) else {}
    common = {
        "realm_id": realm_id,
        "id": str(entity_id),
        "sync_token": _optional_string(row.get("SyncToken")),
        "created_at": _optional_datetime(metadata.get("CreateTime")),
        "last_updated_at": _optional_datetime(metadata.get("LastUpdatedTime")),
        "raw_json": json.dumps(row, separators=(",", ":"), sort_keys=True),
    }
    normalizers = {
        "customers": _normalize_customer,
        "vendors": _normalize_vendor,
        "accounts": _normalize_account,
        "items": _normalize_item,
        "invoices": _normalize_invoice,
        "bills": _normalize_bill,
    }
    return common | normalizers[table_name](row)


def _normalize_cdc_entity(table_name: str, row: dict, *, realm_id: str) -> dict:
    record = _normalize_entity(table_name, row, realm_id=realm_id)
    if record[CURSOR_FIELD] is None:
        entity = TABLE_TO_ENTITY[table_name]
        raise RuntimeError(f"QuickBooks {entity} is missing MetaData.LastUpdatedTime")
    return record


def _normalize_delete_entity(table_name: str, row: dict, *, realm_id: str) -> dict:
    """Build a schema-complete tombstone from QuickBooks' minimal delete body."""
    entity_id = row.get("Id")
    if entity_id in {None, ""}:
        raise RuntimeError("QuickBooks deleted entity is missing Id")
    metadata = row.get("MetaData") if isinstance(row.get("MetaData"), dict) else {}
    last_updated_at = _optional_datetime(metadata.get("LastUpdatedTime"))
    if last_updated_at is None:
        raise RuntimeError("QuickBooks deleted entity is missing MetaData.LastUpdatedTime")

    tombstone = {field.name: None for field in TABLE_SCHEMAS[table_name].fields}
    tombstone.update(
        {
            "realm_id": realm_id,
            "id": str(entity_id),
            "sync_token": _optional_string(row.get("SyncToken")),
            CURSOR_FIELD: last_updated_at,
            "raw_json": json.dumps(row, separators=(",", ":"), sort_keys=True),
        }
    )
    return tombstone


def _normalize_customer(row: dict) -> dict:
    return _party_fields(row) | {
        "balance": _optional_decimal(row.get("Balance")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _normalize_vendor(row: dict) -> dict:
    return _party_fields(row) | {
        "balance": _optional_decimal(row.get("Balance")),
        "vendor_1099": _optional_bool(row.get("Vendor1099")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _party_fields(row: dict) -> dict:
    return {
        "display_name": _optional_string(row.get("DisplayName")),
        "company_name": _optional_string(row.get("CompanyName")),
        "given_name": _optional_string(row.get("GivenName")),
        "family_name": _optional_string(row.get("FamilyName")),
        "primary_email": _nested_string(row, "PrimaryEmailAddr", "Address"),
        "primary_phone": _nested_string(row, "PrimaryPhone", "FreeFormNumber"),
    }


def _normalize_account(row: dict) -> dict:
    return {
        "name": _optional_string(row.get("Name")),
        "fully_qualified_name": _optional_string(row.get("FullyQualifiedName")),
        "account_type": _optional_string(row.get("AccountType")),
        "account_sub_type": _optional_string(row.get("AccountSubType")),
        "classification": _optional_string(row.get("Classification")),
        "current_balance": _optional_decimal(row.get("CurrentBalance")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _normalize_item(row: dict) -> dict:
    return {
        "name": _optional_string(row.get("Name")),
        "fully_qualified_name": _optional_string(row.get("FullyQualifiedName")),
        "item_type": _optional_string(row.get("Type")),
        "description": _optional_string(row.get("Description")),
        "unit_price": _optional_decimal(row.get("UnitPrice")),
        "purchase_cost": _optional_decimal(row.get("PurchaseCost")),
        "quantity_on_hand": _optional_decimal(row.get("QtyOnHand")),
        "income_account_ref": _reference_value(row.get("IncomeAccountRef")),
        "expense_account_ref": _reference_value(row.get("ExpenseAccountRef")),
        "asset_account_ref": _reference_value(row.get("AssetAccountRef")),
        "active": _optional_bool(row.get("Active")),
    }


def _normalize_invoice(row: dict) -> dict:
    return _transaction_fields(row) | {
        "customer_ref": _reference_value(row.get("CustomerRef")),
        "email_status": _optional_string(row.get("EmailStatus")),
        "print_status": _optional_string(row.get("PrintStatus")),
    }


def _normalize_bill(row: dict) -> dict:
    return _transaction_fields(row) | {
        "vendor_ref": _reference_value(row.get("VendorRef")),
        "ap_account_ref": _reference_value(row.get("APAccountRef")),
    }


def _transaction_fields(row: dict) -> dict:
    lines = row.get("Line")
    return {
        "doc_number": _optional_string(row.get("DocNumber")),
        "txn_date": _optional_date(row.get("TxnDate")),
        "due_date": _optional_date(row.get("DueDate")),
        "total_amount": _optional_decimal(row.get("TotalAmt")),
        "balance": _optional_decimal(row.get("Balance")),
        "currency_ref": _reference_value(row.get("CurrencyRef")),
        "line_json": (
            json.dumps(lines, separators=(",", ":"), sort_keys=True)
            if isinstance(lines, list)
            else None
        ),
    }


def _optional_string(value: object) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _optional_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError) as exc:
        raise RuntimeError(f"Invalid QuickBooks decimal value: {value!r}") from exc


def _optional_datetime(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError(f"Invalid QuickBooks timestamp: {value!r}") from exc


def _optional_date(value: object) -> str | None:
    if value in {None, ""}:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise RuntimeError(f"Invalid QuickBooks date: {value!r}") from exc


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _nested_string(row: dict, parent: str, child: str) -> str | None:
    value = row.get(parent)
    if not isinstance(value, dict):
        return None
    return _optional_string(value.get(child))


def _reference_value(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    return _optional_string(value.get("value"))


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        now = datetime.now(target.tzinfo)
        return max(0.0, (target - now).total_seconds())


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_qbo_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("QuickBooks cursor datetime must include a timezone")
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_qbo_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"Invalid QuickBooks cursor timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"QuickBooks cursor timestamp must include a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _offset(
    updated_through: str,
    *,
    realm_id: str,
    table_name: str,
    flow: str,
) -> dict:
    return {
        OFFSET_VERSION_KEY: OFFSET_VERSION,
        OFFSET_REALM_KEY: realm_id,
        OFFSET_TABLE_KEY: table_name,
        OFFSET_FLOW_KEY: flow,
        OFFSET_CURSOR_KEY: updated_through,
    }


def _parse_offset(
    start_offset: dict,
    *,
    expected_realm_id: str,
    expected_table_name: str,
    expected_flow: str,
) -> str | None:
    if not start_offset:
        return None
    if start_offset.get(OFFSET_VERSION_KEY) != OFFSET_VERSION:
        raise ValueError(
            f"Unsupported QuickBooks offset version: {start_offset.get(OFFSET_VERSION_KEY)!r}"
        )
    realm_id = start_offset.get(OFFSET_REALM_KEY)
    if realm_id != expected_realm_id:
        raise ValueError(
            "QuickBooks offset realm_id does not match the configured Unity Catalog "
            "connection; reset the pipeline checkpoint only after validating the tenant"
        )
    if start_offset.get(OFFSET_TABLE_KEY) != expected_table_name:
        raise ValueError(
            "QuickBooks offset table_name does not match the requested table; "
            "check the pipeline's checkpoint isolation"
        )
    if start_offset.get(OFFSET_FLOW_KEY) != expected_flow:
        raise ValueError(
            "QuickBooks offset flow does not match the requested update/delete flow; "
            "check the pipeline's checkpoint isolation"
        )
    cursor = start_offset.get(OFFSET_CURSOR_KEY)
    if not isinstance(cursor, str) or not cursor:
        raise ValueError(f"QuickBooks offset requires non-empty '{OFFSET_CURSOR_KEY}'")
    _parse_qbo_datetime(cursor)
    return cursor


def _bounded_int_option(
    options: dict[str, str],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        value = int(options.get(name, str(default)))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value
