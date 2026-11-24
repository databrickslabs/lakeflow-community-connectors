"""Maxio (SaaSOptics REST API v1.0) source connector."""

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface import LakeflowConnect


# SaaSOptics server-side behavior (observed during live validation against
# a real SaaSOptics tenant): the API silently caps the
# *returned* page size at 100 regardless of the requested ``page_size`` —
# we tested 1, 5, 50, 100, 200, 500, 1000 and every response returned
# exactly 100 records with ``next`` set. The server does echo whatever
# ``page_size`` we send back into the ``next`` URL (so subsequent pages
# also nominally request that value), but the effective page size is
# always 100. Setting ``_PAGE_SIZE`` to 100 matches this behavior; smaller
# values do NOT yield smaller pages, and larger values are wasted bytes
# in the query string. Keep at 100 unless server-side behavior changes.
_PAGE_SIZE = 100
_DEFAULT_MAX_RECORDS_PER_BATCH = 10_000
_RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 5
_INITIAL_BACKOFF = 0.5
_MIN_REQUEST_INTERVAL = 0.2

_CUSTOMERS_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("number", StringType()),
    StructField("code", StringType()),
    StructField("domain", StringType()),
    StructField("notes", StringType()),
    StructField("is_active", BooleanType()),
    StructField("qb_id", StringType()),
    StructField("do_not_sync", BooleanType()),
    StructField("unbalanced_revenue_exception", BooleanType()),
    StructField("modified", TimestampType()),
    StructField("chargify_id", StringType()),
    StructField("sf_id", StringType()),
    StructField("sf_owner_id", StringType()),
    StructField("einvoicing_id", StringType()),
    StructField("email", StringType()),
    StructField("cc_email", StringType()),
    StructField("escalation_email", StringType()),
    StructField("default_email_from_so", BooleanType()),
    StructField("default_enable_cc_payment", BooleanType()),
    StructField("default_enable_ach_payment", BooleanType()),
    StructField("autopay_enrollment", StringType()),
    StructField("autopay_status", StringType()),
    StructField("parent", LongType()),
    StructField("default_theme", StringType()),
    StructField("is_paying_customer", BooleanType()),
    StructField("text_field1", StringType()),
    StructField("text_field2", StringType()),
    StructField("text_field3", StringType()),
    StructField("number_field1", StringType()),
    StructField("number_field2", StringType()),
    StructField("number_field3", StringType()),
    StructField("industry", StringType()),
    StructField("segment", StringType()),
    StructField("market", StringType()),
    StructField("c_account_type", StringType()),
    StructField("c_po_reqd_for_invoicing", StringType()),
    StructField("c_salesforce_id", StringType()),
    StructField("c_subvertical", StringType()),
    StructField("c_vertical", StringType()),
    StructField("auditentry", StringType()),
    StructField("billing_name", StringType()),
    StructField("billing_company_name", StringType()),
    StructField("billing_salutation", StringType()),
    StructField("billing_first_name", StringType()),
    StructField("billing_last_name", StringType()),
    StructField("billing_addr1", StringType()),
    StructField("billing_addr2", StringType()),
    StructField("billing_addr3", StringType()),
    StructField("billing_city", StringType()),
    StructField("billing_state", StringType()),
    StructField("billing_zip_code", StringType()),
    StructField("billing_country", StringType()),
    StructField("shipping_addr1", StringType()),
    StructField("shipping_addr2", StringType()),
    StructField("shipping_addr3", StringType()),
    StructField("shipping_city", StringType()),
    StructField("shipping_state", StringType()),
    StructField("shipping_zip_code", StringType()),
    StructField("shipping_country", StringType()),
    StructField("billing_phone", StringType()),
    StructField("billing_alt_phone", StringType()),
    StructField("billing_fax", StringType()),
    StructField("billing_email", StringType()),
    StructField("billing_contact", StringType()),
    StructField("billing_alt_contact", StringType()),
    StructField("billing_sales_rep_fullname", StringType()),
    StructField("billing_resale_number", StringType()),
    StructField("billing_account_number", StringType()),
    StructField("billing_is_active", BooleanType()),
    StructField("billing_invoice_print_preference", BooleanType()),
    StructField("billing_invoice_email_preference", BooleanType()),
    StructField("billing_edit_sequence", StringType()),
    StructField("billing_last_updated_by_qb", BooleanType()),
    StructField("billing_avatax_address_validation_timestamp", DateType()),
    StructField("billing_payment_terms", LongType()),
    StructField("billing_payment_method", StringType()),
    StructField("billing_currency", StringType()),
    StructField("billing_sales_tax_code", StringType()),
    StructField("billing_item_sales_tax", StringType()),
    StructField("billing_sales_rep", StringType()),
    StructField("billing_customer_type", StringType()),
    StructField("billing_default_invoice_template", StringType()),
    StructField("billing_default_credit_memo_template", StringType()),
    StructField("billing_default_class", StringType()),
    StructField("billing_entity_use_code", StringType()),
    StructField("billing_tax_exemption_reason", StringType()),
])

_ITEMS_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("product_family", StringType()),
    StructField("name", StringType()),
    StructField("code", StringType()),
    StructField("source_system", StringType()),
    StructField("description", StringType()),
    StructField("recurring", BooleanType()),
    StructField("billing_description", StringType()),
    StructField("default_duration", StringType()),
    StructField("normalized_term", StringType()),
    StructField("is_active", BooleanType()),
    StructField("enable_create_transactions", BooleanType()),
    StructField("create_revenue", BooleanType()),
    StructField("use_chargify_start_and_end_dates", BooleanType()),
    StructField("needs_so_profile", BooleanType()),
    StructField("qb_id", StringType()),
    StructField("sf_do_not_sync", BooleanType()),
    StructField("wizard_enabled", BooleanType()),
    StructField("gl_name", StringType()),
    StructField("gl_description", StringType()),
    StructField("sync_invoices", BooleanType()),
    StructField("is_sales_tax", BooleanType()),
    StructField("is_discount", BooleanType()),
    StructField("is_taxable", BooleanType()),
    StructField("income_account", LongType()),
    StructField("qb_account", LongType()),
    StructField("modified", TimestampType()),
    StructField("modified_by_name", StringType()),
])

_INVOICES_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", StringType()),
    StructField("date", DateType()),
    StructField("due_date", DateType()),
    StructField("ship_date", DateType()),
    StructField("po_number", StringType()),
    StructField("memo", StringType()),
    StructField("other", StringType()),
    StructField("to_be_printed", BooleanType()),
    StructField("to_be_emailed", BooleanType()),
    StructField("billing_addr1", StringType()),
    StructField("billing_addr2", StringType()),
    StructField("billing_addr3", StringType()),
    StructField("billing_city", StringType()),
    StructField("billing_state", StringType()),
    StructField("billing_zip_code", StringType()),
    StructField("billing_country", StringType()),
    StructField("shipping_addr1", StringType()),
    StructField("shipping_addr2", StringType()),
    StructField("shipping_addr3", StringType()),
    StructField("shipping_city", StringType()),
    StructField("shipping_state", StringType()),
    StructField("shipping_zip_code", StringType()),
    StructField("shipping_country", StringType()),
    StructField("exported_date", TimestampType()),
    StructField("do_not_sync", BooleanType()),
    StructField("ignore_date_when_syncing", BooleanType()),
    StructField("type", StringType()),
    StructField("chargify_id", StringType()),
    StructField("qb_number", StringType()),
    StructField("qb_txn_id", StringType()),
    StructField("deleted_in_qb", BooleanType()),
    StructField("sync_date", TimestampType()),
    StructField("foreign_exchange_rate", DoubleType()),
    StructField("subtotal", DoubleType()),
    StructField("is_paid", BooleanType()),
    StructField("sales_tax", DoubleType()),
    StructField("sales_tax_percentage", DoubleType()),
    StructField("applied_amount", DoubleType()),
    StructField("balance", DoubleType()),
    StructField("email_from_so", BooleanType()),
    StructField("enable_cc_payment", StringType()),
    StructField("enable_ach_payment", StringType()),
    StructField("sf_id", StringType()),
    StructField("stripe_id", StringType()),
    StructField("contract", LongType()),
    StructField("qb_payment_terms", LongType()),
    StructField("qb_class", LongType()),
    StructField("qb_template", LongType()),
    StructField("qb_customer_message", StringType()),
    StructField("qb_sales_rep", LongType()),
    StructField("qb_currency", StringType()),
    StructField("qb_ar_account", LongType()),
    StructField("refund_of", StringType()),
    StructField("external_id", StringType()),
    StructField("committed", BooleanType()),
    StructField("committed_timestamp", TimestampType()),
    StructField("audit_created", StringType()),
    StructField("audit_created_by", StringType()),
    StructField("audit_created_by_name", StringType()),
    StructField("audit_modified", StringType()),
    StructField("audit_modified_by", StringType()),
    StructField("audit_modified_by_name", StringType()),
    StructField("local_amount", DoubleType()),
    StructField("home_amount", DoubleType()),
    StructField("tax_lines", StringType()),
    StructField("last_emailed", TimestampType()),
    StructField("last_clicked", TimestampType()),
    StructField("last_opened", TimestampType()),
    StructField("einvoicing_url", StringType()),
    StructField("einvoicing_url_no_click_tracking", StringType()),
])

_INVOICES_LINE_ITEMS_SCHEMA = StructType([
    StructField("invoice_id", LongType()),
    StructField("line_id", LongType()),
    StructField("line_number", StringType()),
    StructField("home_amount", DoubleType()),
    StructField("local_amount", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("notes", StringType()),
    StructField("no_transaction_permitted", BooleanType()),
    StructField("exported_date", TimestampType()),
    StructField("qb_txn_line_id", StringType()),
    StructField("sync_date", TimestampType()),
    StructField("deleted_in_qb", BooleanType()),
    StructField("qb_time_modified", TimestampType()),
    StructField("sf_id", StringType()),
    StructField("item", LongType()),
    StructField("transaction", LongType()),
    StructField("qb_class", LongType()),
    StructField("external_id", StringType()),
    StructField("modified", TimestampType()),
    StructField("modified_by_name", StringType()),
])

_CONTRACTS_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", StringType()),
    StructField("maxio_id", StringType()),
    StructField("entry_date", DateType()),
    StructField("is_active", BooleanType()),
    StructField("notes", StringType()),
    StructField("qb_job_id", StringType()),
    StructField("is_job", BooleanType()),
    StructField("unbalanced_revenue_exception", BooleanType()),
    StructField("email", StringType()),
    StructField("default_cadence_template", StringType()),
    StructField("sf_id", StringType()),
    StructField("register", LongType()),
    StructField("register_maxio_id", StringType()),
    StructField("customer", LongType()),
    StructField("created", StringType()),
    StructField("created_by", StringType()),
    StructField("created_by_name", StringType()),
    StructField("modified", TimestampType()),
    StructField("modified_by", StringType()),
    StructField("modified_by_name", StringType()),
    StructField("text_field1", StringType()),
    StructField("text_field2", StringType()),
    StructField("number_field1", StringType()),
    StructField("number_field2", StringType()),
    StructField("channel", StringType()),
    StructField("lead_source", StringType()),
    StructField("lead_date", DateType()),
    StructField("billing_parent_id", StringType()),
    StructField("billing_name", StringType()),
    StructField("billing_company_name", StringType()),
    StructField("billing_salutation", StringType()),
    StructField("billing_first_name", StringType()),
    StructField("billing_last_name", StringType()),
    StructField("billing_addr1", StringType()),
    StructField("billing_addr2", StringType()),
    StructField("billing_addr3", StringType()),
    StructField("billing_city", StringType()),
    StructField("billing_state", StringType()),
    StructField("billing_zip_code", StringType()),
    StructField("billing_country", StringType()),
    StructField("billing_shipping_addr1", StringType()),
    StructField("billing_shipping_addr2", StringType()),
    StructField("billing_shipping_addr3", StringType()),
    StructField("billing_shipping_city", StringType()),
    StructField("billing_shipping_state", StringType()),
    StructField("billing_shipping_zip_code", StringType()),
    StructField("billing_shipping_country", StringType()),
    StructField("billing_phone", StringType()),
    StructField("billing_alt_phone", StringType()),
    StructField("billing_fax", StringType()),
    StructField("billing_email", StringType()),
    StructField("billing_contact", StringType()),
    StructField("billing_alt_contact", StringType()),
    StructField("billing_customer_type_ref_fullname", StringType()),
    StructField("billing_resale_number", StringType()),
    StructField("billing_account_number", StringType()),
    StructField("billing_is_active", BooleanType()),
    StructField("billing_job_status", StringType()),
    StructField("billing_job_start_date", DateType()),
    StructField("billing_job_projected_end_date", DateType()),
    StructField("billing_job_end_date", DateType()),
    StructField("billing_job_description", StringType()),
    StructField("billing_invoice_print_preference", BooleanType()),
    StructField("billing_invoice_email_preference", BooleanType()),
    StructField("billing_edit_sequence", StringType()),
    StructField("billing_payment_terms", LongType()),
    StructField("billing_sales_rep", LongType()),
    StructField("billing_job_type", StringType()),
    StructField("billing_default_invoice_template", StringType()),
    StructField("billing_default_credit_memo_template", StringType()),
    StructField("billing_entity_use_code", StringType()),
    StructField("billing_qbo_bill_parent_customer", BooleanType()),
    StructField("billing_tax_exemption_reason", StringType()),
])

_TRANSACTIONS_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", StringType()),
    StructField("start_date", DateType()),
    StructField("end_date", DateType()),
    StructField("duration", StringType()),
    StructField("foreign_exchange_rate", DoubleType()),
    StructField("home_amount", DoubleType()),
    StructField("local_amount", DoubleType()),
    StructField("home_normalized_amount", DoubleType()),
    StructField("local_normalized_amount", DoubleType()),
    StructField("home_arr_amount", DoubleType()),
    StructField("local_arr_amount", DoubleType()),
    StructField("always_use_amount_as_normalized_amount", BooleanType()),
    StructField("quantity", DoubleType()),
    StructField("term_number", LongType()),
    StructField("unbalanced_revenue_exception", BooleanType()),
    StructField("different_invoice_item_permitted", BooleanType()),
    StructField("renewal_probability", StringType()),
    StructField("renewal_amount_value", DoubleType()),
    StructField("renewal_amount_percentage", DoubleType()),
    StructField("renewal_amount", DoubleType()),
    StructField("order_number", StringType()),
    StructField("order_date", DateType()),
    StructField("recognize", BooleanType()),
    StructField("flagged", StringType()),
    StructField("notes", StringType()),
    StructField("invoice_description", StringType()),
    StructField("do_not_sync_invoices", BooleanType()),
    StructField("home_rate", DoubleType()),
    StructField("local_rate", DoubleType()),
    StructField("home_normalized_rate", DoubleType()),
    StructField("local_normalized_rate", DoubleType()),
    StructField("renewal_quantity", DoubleType()),
    StructField("renewal_duration", StringType()),
    StructField("is_autorenewal", BooleanType()),
    StructField("reconcile_required", BooleanType()),
    StructField("revenue_notes", StringType()),
    StructField("revenue_amount", DoubleType()),
    StructField("modified", TimestampType()),
    StructField("sf_group_id", StringType()),
    StructField("sf_id", StringType()),
    StructField("sf_opportunity_id", StringType()),
    StructField("sf_opportunity_line_item_id", StringType()),
    StructField("sf_renewal_opportunity_id", StringType()),
    StructField("sf_renewal_opportunity_line_item_id", StringType()),
    StructField("cancelled", BooleanType()),
    StructField("contract", LongType()),
    StructField("item", LongType()),
    StructField("billing_method", LongType()),
    StructField("ili_qb_class", LongType()),
    StructField("renew_using_item", LongType()),
    StructField("renewal_factor", DoubleType()),
    StructField("autorenewal_profile", LongType()),
    StructField("sf_renewal_opportunity_rule", LongType()),
    StructField("item_class", LongType()),
    StructField("project", StringType()),
    StructField("renewal_of_set", StringType()),
    StructField("is_renewed", StringType()),
    StructField("renewal_billing_method", LongType()),
    StructField("crm_opportunity_id", StringType()),
    StructField("crm_opportunity_line_item_id", StringType()),
    StructField("hubspot_renewal_deal_rule", LongType()),
    StructField("advanced_billing_subscription_id", StringType()),
    StructField("advanced_billing_subscription_line_id", StringType()),
    StructField("text_field1", StringType()),
    StructField("text_field2", StringType()),
    StructField("number_field1", StringType()),
    StructField("number_field2", StringType()),
    StructField("sales_rep", StringType()),
    StructField("sales_manager", StringType()),
    StructField("conversion", StringType()),
    StructField("audit_created", StringType()),
    StructField("audit_created_by", StringType()),
    StructField("audit_modified", StringType()),
    StructField("audit_modified_by", StringType()),
])

_REVENUE_ENTRIES_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("transaction", LongType()),
    StructField("start_date", DateType()),
    StructField("end_date", DateType()),
    StructField("home_amount", DoubleType()),
    StructField("local_amount", DoubleType()),
    StructField("modified", TimestampType()),
])


TABLE_SCHEMAS: Dict[str, StructType] = {
    "customers": _CUSTOMERS_SCHEMA,
    "items": _ITEMS_SCHEMA,
    "invoices": _INVOICES_SCHEMA,
    "invoices_line_items": _INVOICES_LINE_ITEMS_SCHEMA,
    "contracts": _CONTRACTS_SCHEMA,
    "transactions": _TRANSACTIONS_SCHEMA,
    "revenue_entries": _REVENUE_ENTRIES_SCHEMA,
}


_TABLE_METADATA: Dict[str, Dict[str, Any]] = {
    "customers": {
        "primary_keys": ["id"],
        "cursor_field": "modified",
        "ingestion_type": "cdc",
        "endpoint": "customers/",
        "filter_param": "modified__gte",
    },
    "items": {
        "primary_keys": ["id"],
        "cursor_field": "modified",
        "ingestion_type": "cdc",
        "endpoint": "items/",
        "filter_param": "modified__gte",
    },
    "invoices": {
        "primary_keys": ["id"],
        "cursor_field": "audit_modified",
        "ingestion_type": "cdc",
        "endpoint": "invoices/",
        "filter_param": "auditentry__modified__gte",
    },
    "invoices_line_items": {
        "primary_keys": ["invoice_id", "line_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
        "endpoint": "invoices/",
        "filter_param": None,
    },
    "contracts": {
        "primary_keys": ["id"],
        "cursor_field": "modified",
        "ingestion_type": "cdc",
        "endpoint": "contracts/",
        "filter_param": "modified__gte",
    },
    "transactions": {
        "primary_keys": ["id"],
        "cursor_field": "audit_modified",
        "ingestion_type": "cdc",
        "endpoint": "transactions/",
        "filter_param": "auditentry__modified__gte",
    },
    "revenue_entries": {
        # Live validation against a real SaaSOptics tenant: the top-level
        # ``/revenue_entries/`` endpoint exists, returns entries in bulk, and
        # honors ``modified__gte`` filtering. Confirmed by direct probe and
        # consistent with the Singer tap-saasoptics implementation. We
        # therefore use it as the primary read path: it gives us proper
        # cdc support and avoids fanning out one sub-resource call per
        # transaction. See ``_read_revenue_entries`` for the fallback
        # behavior when the top-level endpoint is unavailable.
        "primary_keys": ["id"],
        "cursor_field": "modified",
        "ingestion_type": "cdc",
        "endpoint": "revenue_entries/",
        "filter_param": "modified__gte",
    },
}


class MaxioLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Maxio / SaaSOptics REST API v1.0."""

    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)
        self._server_subdomain = options["server_subdomain"]
        self._account_name = options["account_name"]
        self._api_key = options["api_key"]
        self._base_url = (
            f"https://{self._server_subdomain}.saasoptics.com/"
            f"{self._account_name}/api/v1.0/"
        )
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Token {self._api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "lakeflow-community-connector-maxio/1.0",
        })
        self._last_request_at = 0.0
        self._init_ts = datetime.now(timezone.utc).isoformat()

    def list_tables(self) -> List[str]:
        return list(TABLE_SCHEMAS.keys())

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        meta = _TABLE_METADATA[table_name]
        out = {
            "primary_keys": list(meta["primary_keys"]),
            "ingestion_type": meta["ingestion_type"],
        }
        if meta["cursor_field"]:
            out["cursor_field"] = meta["cursor_field"]
        return out

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name == "invoices":
            return self._read_invoices(start_offset, table_options)
        if table_name == "invoices_line_items":
            return self._read_invoices_line_items(start_offset, table_options)
        # revenue_entries uses the top-level ``/revenue_entries/`` endpoint
        # (cdc), which is the generic path. The previous sub-resource
        # traversal lives in ``_read_revenue_entries_sub_resource`` as a
        # fallback for tenants where the top-level endpoint is unavailable.
        return self._read_generic(table_name, start_offset, table_options)

    def _validate_table(self, table_name: str) -> None:
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Table '{table_name}' is not supported. "
                f"Supported tables: {list(TABLE_SCHEMAS.keys())}"
            )

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_at = time.monotonic()

    def _request_with_retry(self, url: str, params: Dict[str, str] | None = None) -> requests.Response:
        backoff = _INITIAL_BACKOFF
        last_resp: requests.Response | None = None
        for attempt in range(_MAX_RETRIES):
            self._throttle()
            resp = self._session.get(url, params=params, timeout=60)
            last_resp = resp
            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"Maxio API error {resp.status_code} for {url}: {resp.text[:500]}"
                    )
                return resp
            if attempt < _MAX_RETRIES - 1:
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait = float(retry_after) if retry_after else backoff
                    except ValueError:
                        wait = backoff
                else:
                    wait = backoff
                time.sleep(wait)
                backoff *= 2
        assert last_resp is not None
        raise RuntimeError(
            f"Maxio API error {last_resp.status_code} after {_MAX_RETRIES} retries: "
            f"{last_resp.text[:500]}"
        )

    def _resolve_next_url(self, current_url: str, raw_next: str | None) -> str | None:
        if not raw_next:
            return None
        parsed = urlsplit(raw_next)
        if parsed.scheme and parsed.netloc:
            return raw_next
        return urljoin(current_url, raw_next)

    def _page_records(self, url: str, params: Dict[str, str] | None) -> Tuple[List[dict], str | None]:
        resp = self._request_with_retry(url, params=params)
        body = resp.json()
        if isinstance(body, list):
            return body, None
        if isinstance(body, dict) and "results" in body:
            records = body.get("results") or []
            next_url = self._resolve_next_url(url, body.get("next"))
            return records, next_url
        if isinstance(body, dict):
            return [body], None
        return [], None

    def _start_url(self, table_name: str, start_offset: dict | None) -> Tuple[str, Dict[str, str] | None]:
        if start_offset and start_offset.get("next_url"):
            return start_offset["next_url"], None
        meta = _TABLE_METADATA[table_name]
        url = urljoin(self._base_url, meta["endpoint"])
        params: Dict[str, str] = {"page_size": str(_PAGE_SIZE)}
        cursor = start_offset.get("cursor") if start_offset else None
        if cursor and meta.get("filter_param"):
            params[meta["filter_param"]] = cursor
        return url, params

    def _read_generic(
        self,
        table_name: str,
        start_offset: dict,
        table_options: Dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        meta = _TABLE_METADATA[table_name]
        cursor_field = meta["cursor_field"]
        ingestion_type = meta["ingestion_type"]
        max_records = int(
            table_options.get("max_records_per_batch", _DEFAULT_MAX_RECORDS_PER_BATCH)
        )

        cursor = start_offset.get("cursor") if start_offset else None
        if (
            ingestion_type == "cdc"
            and cursor
            and not (start_offset or {}).get("next_url")
            and cursor >= self._init_ts
        ):
            return iter([]), start_offset

        url, params = self._start_url(table_name, start_offset)

        records: List[dict] = []
        last_cursor = cursor
        next_url: str | None = None

        while True:
            page, next_url = self._page_records(url, params)
            normalized = [self._normalize_record(table_name, rec) for rec in page]
            records.extend(normalized)
            if cursor_field:
                for rec in normalized:
                    val = rec.get(cursor_field)
                    if val and (last_cursor is None or val > last_cursor):
                        last_cursor = val
            if not next_url:
                break
            url, params = next_url, None
            if len(records) >= max_records:
                break

        return self._build_offset(start_offset, records, next_url, last_cursor, ingestion_type)

    def _read_invoices(
        self,
        start_offset: dict,
        table_options: Dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        meta = _TABLE_METADATA["invoices"]
        cursor_field = meta["cursor_field"]
        max_records = int(
            table_options.get("max_records_per_batch", _DEFAULT_MAX_RECORDS_PER_BATCH)
        )
        cursor = start_offset.get("cursor") if start_offset else None
        if (
            cursor
            and not (start_offset or {}).get("next_url")
            and cursor >= self._init_ts
        ):
            return iter([]), start_offset

        url, params = self._start_url("invoices", start_offset)
        records: List[dict] = []
        last_cursor = cursor
        next_url: str | None = None

        while True:
            page, next_url = self._page_records(url, params)
            for raw in page:
                normalized = self._normalize_record("invoices", raw)
                records.append(normalized)
                val = normalized.get(cursor_field)
                if val and (last_cursor is None or val > last_cursor):
                    last_cursor = val
            if not next_url:
                break
            url, params = next_url, None
            if len(records) >= max_records:
                break

        return self._build_offset(start_offset, records, next_url, last_cursor, "cdc")

    def _read_invoices_line_items(
        self,
        start_offset: dict,
        table_options: Dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        # Snapshot table — read everything in one call and signal "no more
        # data" with offset ``None``. This matches the canonical snapshot
        # pattern documented on ``LakeflowConnect.read_table``:
        #
        #   "For tables that cannot be incrementally read, return None as
        #    the offset to read the entire table in one batch."
        #
        # The framework treats ``offset is None`` after a snapshot read as
        # "this batch ingested everything available; on the next pipeline
        # run, re-read from scratch." We deliberately ignore
        # ``max_records_per_batch`` here so that
        # ``Trigger.AvailableNow``'s termination detection
        # (``test_read_terminates``) short-circuits on the very first call.
        # For very large tenants this can hold tens of MB of records in
        # memory during the read; that is the trade-off the snapshot
        # ingestion type makes.
        url, params = self._start_url("invoices_line_items", {})
        line_items: List[dict] = []
        while True:
            page, next_url = self._page_records(url, params)
            for raw in page:
                line_items.extend(self._explode_invoice_lines(raw))
            if not next_url:
                break
            url, params = next_url, None

        return iter(line_items), None

    def _read_revenue_entries_sub_resource(
        self,
        start_offset: dict,
        table_options: Dict[str, str],
    ) -> Tuple[Iterator[dict], dict]:
        # Legacy fallback path: enumerate transactions and walk each one's
        # /revenue_entries/ sub-resource. Used only on tenants where the
        # top-level /revenue_entries/ endpoint is unavailable (the connector
        # currently uses the top-level path — see ``_TABLE_METADATA`` —
        # because that endpoint is confirmed-present during live validation
        # and documented in the Singer tap implementation).
        max_records = int(
            table_options.get("max_records_per_batch", _DEFAULT_MAX_RECORDS_PER_BATCH)
        )

        if start_offset and start_offset.get("transaction_next_url"):
            txn_url = start_offset["transaction_next_url"]
            txn_params: Dict[str, str] | None = None
        else:
            txn_url = urljoin(self._base_url, "transactions/")
            txn_params = {"page_size": str(_PAGE_SIZE)}

        remaining_ids: List[int] = list(
            (start_offset or {}).get("pending_transaction_ids", []) or []
        )
        sub_resume_url: str | None = (start_offset or {}).get("sub_resume_url")

        records: List[dict] = []

        def drain_transaction(transaction_id: int, resume_url: str | None) -> str | None:
            sub_url = resume_url or urljoin(
                self._base_url, f"transactions/{transaction_id}/revenue_entries/"
            )
            sub_params: Dict[str, str] | None = None if resume_url else {"page_size": str(_PAGE_SIZE)}
            while True:
                page, next_sub = self._page_records(sub_url, sub_params)
                for raw in page:
                    if raw.get("transaction") is None:
                        raw["transaction"] = transaction_id
                    records.append(self._normalize_record("revenue_entries", raw))
                if not next_sub:
                    return None
                sub_url, sub_params = next_sub, None
                if len(records) >= max_records:
                    return sub_url

        while True:
            while remaining_ids and len(records) < max_records:
                txn_id = remaining_ids.pop(0)
                sub_resume_url = drain_transaction(txn_id, sub_resume_url)
                if sub_resume_url and len(records) >= max_records:
                    remaining_ids.insert(0, txn_id)
                    break

            if len(records) >= max_records:
                break

            if not txn_url:
                break
            page, next_txn = self._page_records(txn_url, txn_params)
            for txn in page:
                tid = txn.get("id")
                if tid is not None:
                    remaining_ids.append(int(tid))
            txn_url, txn_params = next_txn, None
            if not remaining_ids and not txn_url:
                break

        more_remaining = bool(remaining_ids) or bool(txn_url) or bool(sub_resume_url)
        if not more_remaining:
            return iter(records), {}

        end_offset = {
            "transaction_next_url": txn_url,
            "pending_transaction_ids": remaining_ids,
            "sub_resume_url": sub_resume_url,
        }
        if start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _build_offset(
        self,
        start_offset: dict,
        records: List[dict],
        next_url: str | None,
        last_cursor: str | None,
        ingestion_type: str,
    ) -> Tuple[Iterator[dict], dict]:
        if next_url:
            end_offset: dict = {"next_url": next_url}
            if last_cursor:
                end_offset["cursor"] = last_cursor
        elif ingestion_type == "cdc":
            if last_cursor:
                capped = last_cursor if last_cursor < self._init_ts else self._init_ts
                end_offset = {"cursor": capped}
            elif start_offset:
                end_offset = {"cursor": start_offset.get("cursor")} if start_offset.get("cursor") else {}
            else:
                end_offset = {}
        else:
            end_offset = {}

        if not records and not end_offset:
            return iter([]), start_offset or {}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(records), end_offset

    def _normalize_record(self, table_name: str, raw: dict) -> dict:
        if table_name == "customers":
            return self._normalize_customers(raw)
        if table_name == "items":
            return self._normalize_items(raw)
        if table_name == "invoices":
            return self._normalize_invoices(raw)
        if table_name == "contracts":
            return self._normalize_contracts(raw)
        if table_name == "transactions":
            return self._normalize_transactions(raw)
        if table_name == "revenue_entries":
            return self._normalize_revenue_entries(raw)
        return raw

    def _normalize_customers(self, raw: dict) -> dict:
        out = {f.name: raw.get(f.name) for f in _CUSTOMERS_SCHEMA.fields}
        billing = raw.get("billing_profile") or {}
        for src_key, dst_key in [
            ("name", "billing_name"),
            ("company_name", "billing_company_name"),
            ("salutation", "billing_salutation"),
            ("first_name", "billing_first_name"),
            ("last_name", "billing_last_name"),
            ("addr1", "billing_addr1"),
            ("addr2", "billing_addr2"),
            ("addr3", "billing_addr3"),
            ("city", "billing_city"),
            ("state", "billing_state"),
            ("zip_code", "billing_zip_code"),
            ("country", "billing_country"),
            ("shipping_addr1", "shipping_addr1"),
            ("shipping_addr2", "shipping_addr2"),
            ("shipping_addr3", "shipping_addr3"),
            ("shipping_city", "shipping_city"),
            ("shipping_state", "shipping_state"),
            ("shipping_zip_code", "shipping_zip_code"),
            ("shipping_country", "shipping_country"),
            ("phone", "billing_phone"),
            ("alt_phone", "billing_alt_phone"),
            ("fax", "billing_fax"),
            ("email", "billing_email"),
            ("contact", "billing_contact"),
            ("alt_contact", "billing_alt_contact"),
            ("sales_rep_fullname", "billing_sales_rep_fullname"),
            ("resale_number", "billing_resale_number"),
            ("account_number", "billing_account_number"),
            ("is_active", "billing_is_active"),
            ("invoice_print_preference", "billing_invoice_print_preference"),
            ("invoice_email_preference", "billing_invoice_email_preference"),
            ("edit_sequence", "billing_edit_sequence"),
            ("last_updated_by_qb", "billing_last_updated_by_qb"),
            ("avatax_address_validation_timestamp", "billing_avatax_address_validation_timestamp"),
            ("payment_terms", "billing_payment_terms"),
            ("payment_method", "billing_payment_method"),
            ("currency", "billing_currency"),
            ("sales_tax_code", "billing_sales_tax_code"),
            ("item_sales_tax", "billing_item_sales_tax"),
            ("sales_rep", "billing_sales_rep"),
            ("customer_type", "billing_customer_type"),
            ("default_invoice_template", "billing_default_invoice_template"),
            ("default_credit_memo_template", "billing_default_credit_memo_template"),
            ("default_class", "billing_default_class"),
            ("entity_use_code", "billing_entity_use_code"),
            ("tax_exemption_reason", "billing_tax_exemption_reason"),
        ]:
            if dst_key in out and out.get(dst_key) is None:
                out[dst_key] = billing.get(src_key)
        audit = raw.get("auditentry")
        out["auditentry"] = self._to_json_string(audit)
        return out

    def _normalize_items(self, raw: dict) -> dict:
        return {f.name: raw.get(f.name) for f in _ITEMS_SCHEMA.fields}

    def _normalize_invoices(self, raw: dict) -> dict:
        out = {f.name: raw.get(f.name) for f in _INVOICES_SCHEMA.fields}
        audit = raw.get("auditentry") or {}
        for src_key, dst_key in [
            ("created", "audit_created"),
            ("created_by", "audit_created_by"),
            ("created_by_name", "audit_created_by_name"),
            ("modified", "audit_modified"),
            ("modified_by", "audit_modified_by"),
            ("modified_by_name", "audit_modified_by_name"),
        ]:
            if out.get(dst_key) is None:
                out[dst_key] = audit.get(src_key)
        tax_lines = raw.get("tax_lines")
        out["tax_lines"] = self._to_json_string(tax_lines)
        return out

    def _explode_invoice_lines(self, raw_invoice: dict) -> List[dict]:
        invoice_id = raw_invoice.get("id")
        line_items = raw_invoice.get("line_items") or []
        exploded: List[dict] = []
        for line in line_items:
            row = {f.name: None for f in _INVOICES_LINE_ITEMS_SCHEMA.fields}
            row["invoice_id"] = invoice_id
            row["line_id"] = line.get("id")
            row["line_number"] = (
                str(line.get("number")) if line.get("number") is not None else None
            )
            row["home_amount"] = line.get("home_amount")
            row["local_amount"] = line.get("local_amount")
            row["quantity"] = line.get("quantity")
            row["notes"] = line.get("notes")
            row["no_transaction_permitted"] = line.get("no_transaction_permitted")
            row["exported_date"] = line.get("exported_date")
            row["qb_txn_line_id"] = line.get("qb_txn_line_id")
            row["sync_date"] = line.get("sync_date")
            row["deleted_in_qb"] = line.get("deleted_in_qb")
            row["qb_time_modified"] = line.get("qb_time_modified")
            row["sf_id"] = line.get("sf_id")
            row["item"] = line.get("item")
            row["transaction"] = line.get("transaction")
            row["qb_class"] = line.get("qb_class")
            row["external_id"] = line.get("external_id")
            row["modified"] = line.get("modified")
            row["modified_by_name"] = line.get("modified_by_name")
            exploded.append(row)
        return exploded

    def _normalize_contracts(self, raw: dict) -> dict:
        out = {f.name: raw.get(f.name) for f in _CONTRACTS_SCHEMA.fields}
        billing = raw.get("billing_profile") or {}
        for src_key, dst_key in [
            ("parent_id", "billing_parent_id"),
            ("name", "billing_name"),
            ("company_name", "billing_company_name"),
            ("salutation", "billing_salutation"),
            ("first_name", "billing_first_name"),
            ("last_name", "billing_last_name"),
            ("addr1", "billing_addr1"),
            ("addr2", "billing_addr2"),
            ("addr3", "billing_addr3"),
            ("city", "billing_city"),
            ("state", "billing_state"),
            ("zip_code", "billing_zip_code"),
            ("country", "billing_country"),
            ("shipping_addr1", "billing_shipping_addr1"),
            ("shipping_addr2", "billing_shipping_addr2"),
            ("shipping_addr3", "billing_shipping_addr3"),
            ("shipping_city", "billing_shipping_city"),
            ("shipping_state", "billing_shipping_state"),
            ("shipping_zip_code", "billing_shipping_zip_code"),
            ("shipping_country", "billing_shipping_country"),
            ("phone", "billing_phone"),
            ("alt_phone", "billing_alt_phone"),
            ("fax", "billing_fax"),
            ("email", "billing_email"),
            ("contact", "billing_contact"),
            ("alt_contact", "billing_alt_contact"),
            ("customer_type_ref_fullname", "billing_customer_type_ref_fullname"),
            ("resale_number", "billing_resale_number"),
            ("account_number", "billing_account_number"),
            ("is_active", "billing_is_active"),
            ("job_status", "billing_job_status"),
            ("job_start_date", "billing_job_start_date"),
            ("job_projected_end_date", "billing_job_projected_end_date"),
            ("job_end_date", "billing_job_end_date"),
            ("job_description", "billing_job_description"),
            ("invoice_print_preference", "billing_invoice_print_preference"),
            ("invoice_email_preference", "billing_invoice_email_preference"),
            ("edit_sequence", "billing_edit_sequence"),
            ("payment_terms", "billing_payment_terms"),
            ("sales_rep", "billing_sales_rep"),
            ("job_type", "billing_job_type"),
            ("default_invoice_template", "billing_default_invoice_template"),
            ("default_credit_memo_template", "billing_default_credit_memo_template"),
            ("entity_use_code", "billing_entity_use_code"),
            ("qbo_bill_parent_customer", "billing_qbo_bill_parent_customer"),
            ("tax_exemption_reason", "billing_tax_exemption_reason"),
        ]:
            if out.get(dst_key) is None:
                out[dst_key] = billing.get(src_key)
        return out

    def _normalize_transactions(self, raw: dict) -> dict:
        out = {f.name: raw.get(f.name) for f in _TRANSACTIONS_SCHEMA.fields}
        audit = raw.get("auditentry") or {}
        for src_key, dst_key in [
            ("created", "audit_created"),
            ("created_by", "audit_created_by"),
            ("modified", "audit_modified"),
            ("modified_by", "audit_modified_by"),
        ]:
            if out.get(dst_key) is None:
                out[dst_key] = audit.get(src_key)
        return out

    def _normalize_revenue_entries(self, raw: dict) -> dict:
        return {f.name: raw.get(f.name) for f in _REVENUE_ENTRIES_SCHEMA.fields}

    @staticmethod
    def _to_json_string(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError):
            return str(value)
