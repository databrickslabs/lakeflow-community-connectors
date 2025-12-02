# Stripe connector implementation


import requests
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    ArrayType,
)
from datetime import datetime
import time
from typing import Dict, List, Tuple, Iterator, Any


class LakeflowConnect:
    def __init__(self, options: dict) -> None:
        """
        Initialize the Stripe connector with API credentials.

        Args:
            options: Dictionary containing:
                - api_key: Stripe secret API key (sk_test_* or sk_live_*)
        """
        self.api_key = options["api_key"]
        self.base_url = "https://api.stripe.com/v1"
        self.auth = (self.api_key, "")  # API key as username, empty password

        # Cache for schemas to avoid repeated computation
        self._schema_cache = {}

        # Centralized object metadata configuration
        self._object_config = {
            "customers": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "customers",
                "supports_deleted": True,
            },
            "charges": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "charges",
                "supports_deleted": False,
            },
            "payment_intents": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "payment_intents",
                "supports_deleted": False,
            },
            "subscriptions": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "subscriptions",
                "supports_deleted": False,
            },
            "invoices": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "invoices",
                "supports_deleted": False,
            },
            "products": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "products",
                "supports_deleted": True,
            },
            "prices": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "prices",
                "supports_deleted": False,
            },
            "refunds": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "refunds",
                "supports_deleted": False,
            },
            "disputes": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "disputes",
                "supports_deleted": False,
            },
            "payment_methods": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "payment_methods",
                "supports_deleted": False,
            },
            "balance_transactions": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "balance_transactions",
                "supports_deleted": False,
            },
            "payouts": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "payouts",
                "supports_deleted": False,
            },
            "invoice_items": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "invoiceitems",
                "supports_deleted": False,
            },
            "plans": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "plans",
                "supports_deleted": True,
            },
            "events": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "events",
                "supports_deleted": False,
            },
            "coupons": {
                "primary_key": "id",
                "cursor_field": "created",
                "ingestion_type": "cdc",
                "endpoint": "coupons",
                "supports_deleted": False,
            },
        }

        # Reusable nested schema for Stripe address objects
        self._address_schema = StructType(
            [
                StructField("line1", StringType(), True),
                StructField("line2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )

        # Reusable nested schema for shipping (includes address + name/phone)
        self._shipping_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("address", self._address_schema, True),
            ]
        )

        # Centralized schema configuration
        self._schema_config = {
            "customers": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("email", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("phone", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("address", self._address_schema, True),
                    StructField("shipping", self._shipping_schema, True),
                    StructField("balance", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("delinquent", BooleanType(), True),
                    StructField("preferred_locales", ArrayType(StringType()), True),
                    StructField("invoice_settings", StringType(), True),
                    StructField("tax_exempt", StringType(), True),
                    StructField("default_source", StringType(), True),
                    StructField("invoice_prefix", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("discount", StringType(), True),
                    StructField("deleted", BooleanType(), True),
                    StructField("test_clock", StringType(), True),
                    StructField("tax", StringType(), True),
                    StructField("sources", StringType(), True),
                    StructField("subscriptions", StringType(), True),
                    StructField("next_invoice_sequence", LongType(), True),
                ]
            ),
            "charges": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("amount_captured", LongType(), True),
                    StructField("amount_refunded", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("paid", BooleanType(), True),
                    StructField("refunded", BooleanType(), True),
                    StructField("captured", BooleanType(), True),
                    StructField("disputed", BooleanType(), True),
                    StructField("customer", StringType(), True),
                    StructField("invoice", StringType(), True),
                    StructField("payment_intent", StringType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("receipt_email", StringType(), True),
                    StructField("receipt_url", StringType(), True),
                    StructField("statement_descriptor", StringType(), True),
                    StructField("billing_details", StringType(), True),
                    StructField("payment_method_details", StringType(), True),
                    StructField("outcome", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("failure_code", StringType(), True),
                    StructField("failure_message", StringType(), True),
                ]
            ),
            "payment_intents": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("amount_capturable", LongType(), True),
                    StructField("amount_received", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("canceled_at", LongType(), True),
                    StructField("cancellation_reason", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("invoice", StringType(), True),
                    StructField("payment_method", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("receipt_email", StringType(), True),
                    StructField("statement_descriptor", StringType(), True),
                    StructField("capture_method", StringType(), True),
                    StructField("confirmation_method", StringType(), True),
                    StructField("charges", StringType(), True),
                    StructField("payment_method_options", StringType(), True),
                    StructField("shipping", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("latest_charge", StringType(), True),
                ]
            ),
            "subscriptions": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("status", StringType(), True),
                    StructField("current_period_start", LongType(), True),
                    StructField("current_period_end", LongType(), True),
                    StructField("cancel_at", LongType(), True),
                    StructField("canceled_at", LongType(), True),
                    StructField("ended_at", LongType(), True),
                    StructField("trial_start", LongType(), True),
                    StructField("trial_end", LongType(), True),
                    StructField("customer", StringType(), True),
                    StructField("default_payment_method", StringType(), True),
                    StructField("latest_invoice", StringType(), True),
                    StructField("billing_cycle_anchor", LongType(), True),
                    StructField("collection_method", StringType(), True),
                    StructField("days_until_due", LongType(), True),
                    StructField("items", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("discount", StringType(), True),
                    StructField("cancel_at_period_end", BooleanType(), True),
                ]
            ),
            "invoices": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("status", StringType(), True),
                    StructField("paid", BooleanType(), True),
                    StructField("amount_due", LongType(), True),
                    StructField("amount_paid", LongType(), True),
                    StructField("amount_remaining", LongType(), True),
                    StructField("total", LongType(), True),
                    StructField("subtotal", LongType(), True),
                    StructField("tax", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("subscription", StringType(), True),
                    StructField("charge", StringType(), True),
                    StructField("payment_intent", StringType(), True),
                    StructField("billing_reason", StringType(), True),
                    StructField("collection_method", StringType(), True),
                    StructField("customer_email", StringType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("due_date", LongType(), True),
                    StructField("period_start", LongType(), True),
                    StructField("period_end", LongType(), True),
                    StructField("number", StringType(), True),
                    StructField("hosted_invoice_url", StringType(), True),
                    StructField("invoice_pdf", StringType(), True),
                    StructField("lines", StringType(), True),
                    StructField("metadata", StringType(), True),
                ]
            ),
            "products": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("updated", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("active", BooleanType(), True),
                    StructField("type", StringType(), True),
                    StructField("unit_label", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("images", ArrayType(StringType()), True),
                    StructField("metadata", StringType(), True),
                    StructField("statement_descriptor", StringType(), True),
                    StructField("tax_code", StringType(), True),
                    StructField("shippable", BooleanType(), True),
                    StructField("deleted", BooleanType(), True),
                    StructField("default_price", StringType(), True),
                    StructField("features", StringType(), True),
                    StructField("package_dimensions", StringType(), True),
                ]
            ),
            "prices": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("active", BooleanType(), True),
                    StructField("currency", StringType(), True),
                    StructField("unit_amount", LongType(), True),
                    StructField("unit_amount_decimal", StringType(), True),
                    StructField("product", StringType(), True),
                    StructField("billing_scheme", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("recurring", StringType(), True),
                    StructField("lookup_key", StringType(), True),
                    StructField("nickname", StringType(), True),
                    StructField("tax_behavior", StringType(), True),
                    StructField("tiers", StringType(), True),
                    StructField("tiers_mode", StringType(), True),
                    StructField("metadata", StringType(), True),
                ]
            ),
            "refunds": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("charge", StringType(), True),
                    StructField("payment_intent", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("reason", StringType(), True),
                    StructField("receipt_number", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("failure_reason", StringType(), True),
                ]
            ),
            "disputes": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("charge", StringType(), True),
                    StructField("payment_intent", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("reason", StringType(), True),
                    StructField("evidence", StringType(), True),
                    StructField("evidence_details", StringType(), True),
                    StructField("is_charge_refundable", BooleanType(), True),
                    StructField("metadata", StringType(), True),
                ]
            ),
            "payment_methods": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("type", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("billing_details", StringType(), True),
                    StructField("card", StringType(), True),
                    StructField("us_bank_account", StringType(), True),
                    StructField("metadata", StringType(), True),
                ]
            ),
            "balance_transactions": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("net", LongType(), True),
                    StructField("fee", LongType(), True),
                    StructField("fee_details", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("available_on", LongType(), True),
                    StructField("exchange_rate", DoubleType(), True),
                    StructField("reporting_category", StringType(), True),
                ]
            ),
            "payouts": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("arrival_date", LongType(), True),
                    StructField("status", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("method", StringType(), True),
                    StructField("destination", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("balance_transaction", StringType(), True),
                    StructField("failure_code", StringType(), True),
                    StructField("failure_message", StringType(), True),
                    StructField("metadata", StringType(), True),
                ]
            ),
            "invoice_items": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("customer", StringType(), True),
                    StructField("invoice", StringType(), True),
                    StructField("subscription", StringType(), True),
                    StructField("price", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("quantity", LongType(), True),
                    StructField("unit_amount", LongType(), True),
                    StructField("unit_amount_decimal", StringType(), True),
                    StructField("period", StringType(), True),
                    StructField("discounts", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("proration", BooleanType(), True),
                    StructField("date", LongType(), True),
                    StructField("discountable", BooleanType(), True),
                    StructField("tax_rates", StringType(), True),
                ]
            ),
            "plans": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("active", BooleanType(), True),
                    StructField("amount", LongType(), True),
                    StructField("currency", StringType(), True),
                    StructField("interval", StringType(), True),
                    StructField("interval_count", LongType(), True),
                    StructField("product", StringType(), True),
                    StructField("nickname", StringType(), True),
                    StructField("usage_type", StringType(), True),
                    StructField("aggregate_usage", StringType(), True),
                    StructField("trial_period_days", LongType(), True),
                    StructField("tiers", StringType(), True),
                    StructField("tiers_mode", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("deleted", BooleanType(), True),
                ]
            ),
            "events": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("type", StringType(), True),
                    StructField("data", StringType(), True),
                    StructField("api_version", StringType(), True),
                    StructField("request", StringType(), True),
                    StructField("pending_webhooks", LongType(), True),
                ]
            ),
            "coupons": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("object", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("livemode", BooleanType(), True),
                    StructField("name", StringType(), True),
                    StructField("amount_off", LongType(), True),
                    StructField("percent_off", DoubleType(), True),
                    StructField("currency", StringType(), True),
                    StructField("duration", StringType(), True),
                    StructField("duration_in_months", LongType(), True),
                    StructField("max_redemptions", LongType(), True),
                    StructField("times_redeemed", LongType(), True),
                    StructField("redeem_by", LongType(), True),
                    StructField("valid", BooleanType(), True),
                    StructField("applies_to", StringType(), True),
                    StructField("metadata", StringType(), True),
                    StructField("currency_options", StringType(), True),
                ]
            ),
        }

    def list_tables(self) -> list[str]:
        """
        List available Stripe tables/objects.

        Returns:
            List of supported table names
        """
        return [
            "customers",
            "charges",
            "payment_intents",
            "subscriptions",
            "invoices",
            "products",
            "prices",
            "refunds",
            "disputes",
            "payment_methods",
            "balance_transactions",
            "payouts",
            "invoice_items",
            "plans",
            "events",
            "coupons",
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a Stripe table.

        Args:
            table_name: Name of the table

        Returns:
            StructType representing the table schema
        """
        schema = self._schema_config[table_name]
        return schema

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Get metadata for a Stripe table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with primary_key, cursor_field, and ingestion_type
        """
        config = self._object_config[table_name]
        return {
            "primary_key": config["primary_key"],
            "cursor_field": config["cursor_field"],
            "ingestion_type": config["ingestion_type"],
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[List[Dict], Dict]:
        """
        Read data from a Stripe table.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
                - For incremental: {"created": <unix_timestamp>}
                - For full refresh: None or {}

        Returns:
            Tuple of (records, new_offset)
        """
        if table_name not in self._object_config:
            raise ValueError(f"Unsupported table: {table_name}")

        config = self._object_config[table_name]

        # Determine if this is an incremental read
        is_incremental = (
            start_offset is not None
            and start_offset.get(config["cursor_field"]) is not None
        )

        if is_incremental:
            return self._read_data_incremental(table_name, start_offset)
        else:
            return self._read_data_full(table_name)

    def _read_data_full(self, table_name: str) -> Tuple[List[Dict], Dict]:
        """
        Read all data from a Stripe table (full refresh).

        Args:
            table_name: Name of the table

        Returns:
            Tuple of (all_records, offset)
        """
        config = self._object_config[table_name]
        endpoint = config["endpoint"]
        cursor_field = config["cursor_field"]

        all_records = []
        starting_after = None
        latest_cursor_value = 0

        while True:
            # Build request parameters
            params = {
                "limit": 100  # Max allowed by Stripe
            }

            if starting_after:
                params["starting_after"] = starting_after

            # Make API request
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, auth=self.auth, params=params)

            if response.status_code != 200:
                raise Exception(
                    f"Stripe API error for {table_name}: {response.status_code} {response.text}"
                )

            data = response.json()
            records = data.get("data", [])

            if not records:
                break

            # Transform records
            # transformed_records = self._transform_records(records, table_name)
            all_records.extend(records)

            # Track the latest cursor value for checkpointing
            for record in records:
                cursor_value = record.get(cursor_field, 0)
                if cursor_value > latest_cursor_value:
                    latest_cursor_value = cursor_value

            # Check if there are more pages
            has_more = data.get("has_more", False)
            if not has_more:
                break

            # Get the last object ID for pagination
            starting_after = records[-1]["id"]

            # Rate limiting - be nice to the API
            time.sleep(0.1)

        # Return records and offset for next incremental sync
        offset = {cursor_field: latest_cursor_value} if latest_cursor_value > 0 else {}
        return all_records, offset

    def _read_data_incremental(
        self, table_name: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read incremental data from a Stripe table using cursor.

        Args:
            table_name: Name of the table
            start_offset: Dictionary with cursor field value

        Returns:
            Tuple of (new_records, new_offset)
        """
        config = self._object_config[table_name]
        endpoint = config["endpoint"]
        cursor_field = config["cursor_field"]

        # Get the starting point from offset
        cursor_start = start_offset.get(cursor_field, 0)

        all_records = []
        starting_after = None
        latest_cursor_value = cursor_start

        while True:
            # Build request parameters for incremental fetch
            params = {
                "limit": 100,
                f"{cursor_field}[gte]": cursor_start,  # Greater than or equal to last cursor
            }

            if starting_after:
                params["starting_after"] = starting_after

            # Make API request
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, auth=self.auth, params=params)

            if response.status_code != 200:
                raise Exception(
                    f"Stripe API error for {table_name}: {response.status_code} {response.text}"
                )

            data = response.json()
            records = data.get("data", [])

            if not records:
                break

            # Transform records
            # transformed_records = self._transform_records(records, table_name)
            all_records.extend(records)

            # Track the latest cursor value
            for record in records:
                cursor_value = record.get(cursor_field, 0)
                if cursor_value > latest_cursor_value:
                    latest_cursor_value = cursor_value

            # Check if there are more pages
            has_more = data.get("has_more", False)
            if not has_more:
                break

            # Get the last object ID for pagination
            starting_after = records[-1]["id"]

            # Rate limiting
            time.sleep(0.1)

        # Return new offset for next sync
        offset = {cursor_field: latest_cursor_value}
        return all_records, offset

    def _transform_records(self, records: List[Dict], table_name: str) -> List[Dict]:
        """
        Transform Stripe API records based on the table's schema.

        This generic approach replaces individual per-table transform functions.
        - Fields defined as StringType: nested objects (dicts/lists) are serialized to JSON
        - Fields defined as StructType: nested objects are kept as dicts for Spark

        Args:
            records: Raw records from Stripe API
            table_name: Name of the table

        Returns:
            List of transformed records
        """
        schema = self._schema_config.get(table_name)
        return [self._serialize_record(record, schema) for record in records]

    def _serialize_record(self, record: Dict, schema: StructType) -> Dict:
        """
        Serialize nested objects based on schema field types.

        Args:
            record: Raw record from Stripe API
            schema: The StructType schema for this table

        Returns:
            Record with appropriate serialization based on schema
        """
        # Build a lookup of field name -> field type
        field_types = {field.name: field.dataType for field in schema.fields}

        serialized = {}
        for key, value in record.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, (dict, list)):
                field_type = field_types.get(key)
                # Only serialize to JSON if the schema expects StringType
                if isinstance(field_type, StringType):
                    serialized[key] = json.dumps(value)
                else:
                    # Keep as dict/list for StructType or ArrayType fields
                    serialized[key] = value
            else:
                serialized[key] = value
        return serialized

    def test_connection(self) -> dict:
        """
        Test the connection to Stripe API.

        Returns:
            Dictionary with status and message
        """
        try:
            url = f"{self.base_url}/customers?limit=1"
            response = requests.get(url, auth=self.auth)

            if response.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
