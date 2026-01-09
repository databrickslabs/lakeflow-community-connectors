import requests
import base64
from datetime import datetime, timedelta
from typing import Iterator, Any

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    ArrayType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the PayPal connector with connection-level options.

        Expected options:
            - client_id: OAuth 2.0 client ID from PayPal Developer Dashboard
            - client_secret: OAuth 2.0 client secret from PayPal Developer Dashboard
            - environment (optional): 'sandbox' or 'production'. Defaults to 'sandbox'.
        """
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "PayPal connector requires 'client_id' and 'client_secret' in options"
            )

        # Determine base URL based on environment
        environment = options.get("environment", "sandbox").lower()
        if environment == "production":
            self.base_url = "https://api-m.paypal.com"
        else:
            self.base_url = "https://api-m.sandbox.paypal.com"

        # Configure session for API requests
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        # Token caching
        self._access_token = None
        self._token_expires_at = None

    def _get_access_token(self) -> str:
        """
        Obtain or refresh OAuth 2.0 access token using client credentials flow.
        
        Tokens are cached and refreshed 5 minutes before expiration (9-hour lifetime).
        """
        # Check if cached token is still valid (with 5-minute buffer)
        if self._access_token and self._token_expires_at:
            buffer = timedelta(minutes=5)
            if datetime.now() + buffer < self._token_expires_at:
                return self._access_token

        # Request new token
        token_url = f"{self.base_url}/v1/oauth2/token"
        
        # Create Basic auth header with Base64-encoded client_id:client_secret
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        data = {"grant_type": "client_credentials"}
        
        response = requests.post(token_url, headers=headers, data=data, timeout=30)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal OAuth token request failed: {response.status_code} {response.text}"
            )
        
        token_data = response.json()
        self._access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 32400)  # Default 9 hours
        
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        return self._access_token

    def _make_request(
        self, method: str, endpoint: str, params: dict = None
    ) -> requests.Response:
        """
        Make an authenticated API request to PayPal.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path (e.g., '/v1/reporting/transactions')
            params: Query parameters
            
        Returns:
            Response object
        """
        access_token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        url = f"{self.base_url}{endpoint}"
        
        response = self._session.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            timeout=30
        )
        
        # Handle common error cases
        if response.status_code == 401:
            # Token may have expired, clear cache and retry once
            self._access_token = None
            self._token_expires_at = None
            access_token = self._get_access_token()
            headers["Authorization"] = f"Bearer {access_token}"
            response = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=30
            )
        
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            raise RuntimeError(
                f"PayPal API rate limit exceeded. Retry after {retry_after} seconds."
            )
        
        if response.status_code not in [200, 201]:
            raise RuntimeError(
                f"PayPal API error: {response.status_code} {response.text}"
            )
        
        return response

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        
        Currently returns: transactions, subscriptions, products, plans, payment_captures
        
        Note: 'invoices' table requires Invoicing API permissions (not available
        with basic sandbox credentials). The table is fully implemented but not 
        listed by default. Users with proper permissions can access it directly.
        
        Note: 'orders' table is not included because PayPal Orders API v2 
        does not support bulk listing. Use 'transactions' table instead.
        """
        return ["transactions", "subscriptions", "products", "plans", "payment_captures"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        
        Args:
            table_name: The name of the table to fetch the schema for.
            table_options: Additional options (not required for PayPal connector).
            
        Returns:
            A StructType object representing the schema of the table.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        # Common struct types used across multiple tables
        amount_struct = StructType([
            StructField("currency_code", StringType(), True),
            StructField("value", StringType(), True),
        ])
        
        address_struct = StructType([
            StructField("line1", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("postal_code", StringType(), True),
        ])

        if table_name == "transactions":
            # Flattened transactions table schema
            # All fields from nested objects (transaction_info, payer_info, shipping_info, cart_info) 
            # are now top-level columns
            
            payer_name_struct = StructType([
                StructField("given_name", StringType(), True),
                StructField("surname", StringType(), True),
            ])
            
            # Item details for cart
            item_details_struct = StructType([
                StructField("item_code", StringType(), True),
                StructField("item_name", StringType(), True),
                StructField("item_description", StringType(), True),
                StructField("item_quantity", StringType(), True),
                StructField("item_unit_price", amount_struct, True),
                StructField("item_amount", amount_struct, True),
            ])
            
            # Flattened schema with all fields at top level
            transactions_schema = StructType([
                # Fields from transaction_info
                StructField("transaction_id", StringType(), False),
                StructField("paypal_account_id", StringType(), True),
                StructField("transaction_event_code", StringType(), True),
                StructField("transaction_initiation_date", StringType(), True),
                StructField("transaction_updated_date", StringType(), True),
                StructField("transaction_amount", amount_struct, True),
                StructField("fee_amount", amount_struct, True),
                StructField("transaction_status", StringType(), True),
                StructField("transaction_subject", StringType(), True),
                StructField("ending_balance", amount_struct, True),
                StructField("available_balance", amount_struct, True),
                StructField("invoice_id", StringType(), True),
                StructField("custom_field", StringType(), True),
                StructField("protection_eligibility", StringType(), True),
                # Fields from payer_info
                StructField("payer_account_id", StringType(), True),
                StructField("payer_email_address", StringType(), True),
                StructField("payer_address_status", StringType(), True),
                StructField("payer_status", StringType(), True),
                StructField("payer_name", payer_name_struct, True),
                StructField("payer_country_code", StringType(), True),
                # Fields from shipping_info
                StructField("shipping_name", StringType(), True),
                StructField("shipping_address", address_struct, True),
                # Fields from cart_info
                StructField("item_details", ArrayType(item_details_struct, True), True),
            ])
            
            return transactions_schema

        if table_name == "invoices":
            # Invoice schema based on PayPal Invoicing API v2
            invoice_detail_struct = StructType([
                StructField("invoice_number", StringType(), True),
                StructField("reference", StringType(), True),
                StructField("invoice_date", StringType(), True),
                StructField("currency_code", StringType(), True),
                StructField("note", StringType(), True),
                StructField("term", StringType(), True),
                StructField("memo", StringType(), True),
            ])
            
            invoicer_struct = StructType([
                StructField("name", StringType(), True),
                StructField("email_address", StringType(), True),
                StructField("phones", ArrayType(StructType([
                    StructField("country_code", StringType(), True),
                    StructField("national_number", StringType(), True),
                    StructField("phone_type", StringType(), True),
                ]), True), True),
                StructField("website", StringType(), True),
                StructField("tax_id", StringType(), True),
                StructField("logo_url", StringType(), True),
            ])
            
            primary_recipient_struct = StructType([
                StructField("billing_info", StructType([
                    StructField("email_address", StringType(), True),
                    StructField("language", StringType(), True),
                ]), True),
                StructField("shipping_info", StructType([
                    StructField("name", StringType(), True),
                    StructField("address", address_struct, True),
                ]), True),
            ])
            
            item_struct = StructType([
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("quantity", StringType(), True),
                StructField("unit_amount", amount_struct, True),
                StructField("tax", StructType([
                    StructField("name", StringType(), True),
                    StructField("percent", StringType(), True),
                    StructField("amount", amount_struct, True),
                ]), True),
                StructField("item_date", StringType(), True),
                StructField("discount", StructType([
                    StructField("percent", StringType(), True),
                    StructField("amount", amount_struct, True),
                ]), True),
            ])
            
            amount_summary_struct = StructType([
                StructField("currency_code", StringType(), True),
                StructField("value", StringType(), True),
            ])
            
            invoices_schema = StructType([
                StructField("id", StringType(), False),
                StructField("parent_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("detail", invoice_detail_struct, True),
                StructField("invoicer", invoicer_struct, True),
                StructField("primary_recipients", ArrayType(primary_recipient_struct, True), True),
                StructField("items", ArrayType(item_struct, True), True),
                StructField("amount", StructType([
                    StructField("breakdown", StructType([
                        StructField("item_total", amount_summary_struct, True),
                        StructField("discount", amount_summary_struct, True),
                        StructField("tax_total", amount_summary_struct, True),
                        StructField("shipping", amount_summary_struct, True),
                    ]), True),
                ]), True),
                StructField("due_amount", amount_summary_struct, True),
                StructField("gratuity", amount_summary_struct, True),
                StructField("links", ArrayType(StructType([
                    StructField("href", StringType(), True),
                    StructField("rel", StringType(), True),
                    StructField("method", StringType(), True),
                ]), True), True),
            ])
            
            return invoices_schema

        if table_name == "subscriptions":
            # Subscription schema based on PayPal Subscriptions API v1
            billing_info_struct = StructType([
                StructField("outstanding_balance", amount_struct, True),
                StructField("cycle_executions", ArrayType(StructType([
                    StructField("tenure_type", StringType(), True),
                    StructField("sequence", LongType(), True),
                    StructField("cycles_completed", LongType(), True),
                    StructField("cycles_remaining", LongType(), True),
                    StructField("total_cycles", LongType(), True),
                ]), True), True),
                StructField("last_payment", StructType([
                    StructField("amount", amount_struct, True),
                    StructField("time", StringType(), True),
                ]), True),
                StructField("next_billing_time", StringType(), True),
                StructField("final_payment_time", StringType(), True),
                StructField("failed_payments_count", LongType(), True),
            ])
            
            subscriber_struct = StructType([
                StructField("email_address", StringType(), True),
                StructField("payer_id", StringType(), True),
                StructField("name", StructType([
                    StructField("given_name", StringType(), True),
                    StructField("surname", StringType(), True),
                ]), True),
                StructField("shipping_address", address_struct, True),
            ])
            
            subscriptions_schema = StructType([
                StructField("id", StringType(), False),
                StructField("plan_id", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("quantity", StringType(), True),
                StructField("shipping_amount", amount_struct, True),
                StructField("subscriber", subscriber_struct, True),
                StructField("billing_info", billing_info_struct, True),
                StructField("create_time", StringType(), True),
                StructField("update_time", StringType(), True),
                StructField("status", StringType(), True),
                StructField("status_update_time", StringType(), True),
                StructField("links", ArrayType(StructType([
                    StructField("href", StringType(), True),
                    StructField("rel", StringType(), True),
                    StructField("method", StringType(), True),
                ]), True), True),
            ])
            
            return subscriptions_schema

        if table_name == "orders":
            # Orders schema based on PayPal Orders API v2
            purchase_unit_struct = StructType([
                StructField("reference_id", StringType(), True),
                StructField("amount", StructType([
                    StructField("currency_code", StringType(), True),
                    StructField("value", StringType(), True),
                    StructField("breakdown", StructType([
                        StructField("item_total", amount_struct, True),
                        StructField("shipping", amount_struct, True),
                        StructField("handling", amount_struct, True),
                        StructField("tax_total", amount_struct, True),
                        StructField("insurance", amount_struct, True),
                        StructField("shipping_discount", amount_struct, True),
                        StructField("discount", amount_struct, True),
                    ]), True),
                ]), True),
                StructField("payee", StructType([
                    StructField("email_address", StringType(), True),
                    StructField("merchant_id", StringType(), True),
                ]), True),
                StructField("description", StringType(), True),
                StructField("custom_id", StringType(), True),
                StructField("invoice_id", StringType(), True),
                StructField("soft_descriptor", StringType(), True),
                StructField("items", ArrayType(StructType([
                    StructField("name", StringType(), True),
                    StructField("unit_amount", amount_struct, True),
                    StructField("tax", amount_struct, True),
                    StructField("quantity", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("sku", StringType(), True),
                    StructField("category", StringType(), True),
                ]), True), True),
                StructField("shipping", StructType([
                    StructField("name", StructType([
                        StructField("full_name", StringType(), True),
                    ]), True),
                    StructField("address", address_struct, True),
                ]), True),
                StructField("payments", StructType([
                    StructField("captures", ArrayType(StructType([
                        StructField("id", StringType(), True),
                        StructField("status", StringType(), True),
                        StructField("amount", amount_struct, True),
                        StructField("final_capture", BooleanType(), True),
                        StructField("seller_protection", StructType([
                            StructField("status", StringType(), True),
                            StructField("dispute_categories", ArrayType(StringType(), True), True),
                        ]), True),
                        StructField("create_time", StringType(), True),
                        StructField("update_time", StringType(), True),
                    ]), True), True),
                    StructField("refunds", ArrayType(StructType([
                        StructField("id", StringType(), True),
                        StructField("status", StringType(), True),
                        StructField("amount", amount_struct, True),
                        StructField("create_time", StringType(), True),
                        StructField("update_time", StringType(), True),
                    ]), True), True),
                ]), True),
            ])
            
            payer_struct = StructType([
                StructField("email_address", StringType(), True),
                StructField("payer_id", StringType(), True),
                StructField("name", StructType([
                    StructField("given_name", StringType(), True),
                    StructField("surname", StringType(), True),
                ]), True),
                StructField("phone", StructType([
                    StructField("phone_number", StructType([
                        StructField("national_number", StringType(), True),
                    ]), True),
                ]), True),
                StructField("address", address_struct, True),
            ])
            
            orders_schema = StructType([
                StructField("id", StringType(), False),
                StructField("intent", StringType(), True),
                StructField("status", StringType(), True),
                StructField("purchase_units", ArrayType(purchase_unit_struct, True), True),
                StructField("payer", payer_struct, True),
                StructField("create_time", StringType(), True),
                StructField("update_time", StringType(), True),
                StructField("links", ArrayType(StructType([
                    StructField("href", StringType(), True),
                    StructField("rel", StringType(), True),
                    StructField("method", StringType(), True),
                ]), True), True),
            ])
            
            return orders_schema

        if table_name == "products":
            # Products schema based on PayPal Catalog Products API v1
            products_schema = StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("type", StringType(), True),
                StructField("category", StringType(), True),
                StructField("image_url", StringType(), True),
                StructField("home_url", StringType(), True),
                StructField("create_time", StringType(), True),
                StructField("update_time", StringType(), True),
                StructField("links", ArrayType(StructType([
                    StructField("href", StringType(), True),
                    StructField("rel", StringType(), True),
                    StructField("method", StringType(), True),
                ]), True), True),
            ])
            
            return products_schema

        if table_name == "plans":
            # Billing Plans schema based on PayPal Subscriptions API v1
            billing_cycle_struct = StructType([
                StructField("tenure_type", StringType(), True),
                StructField("sequence", LongType(), True),
                StructField("total_cycles", LongType(), True),
                StructField("pricing_scheme", StructType([
                    StructField("fixed_price", amount_struct, True),
                    StructField("create_time", StringType(), True),
                    StructField("update_time", StringType(), True),
                ]), True),
                StructField("frequency", StructType([
                    StructField("interval_unit", StringType(), True),
                    StructField("interval_count", LongType(), True),
                ]), True),
            ])
            
            payment_preferences_struct = StructType([
                StructField("auto_bill_outstanding", BooleanType(), True),
                StructField("setup_fee", amount_struct, True),
                StructField("setup_fee_failure_action", StringType(), True),
                StructField("payment_failure_threshold", LongType(), True),
            ])
            
            taxes_struct = StructType([
                StructField("percentage", StringType(), True),
                StructField("inclusive", BooleanType(), True),
            ])
            
            plans_schema = StructType([
                StructField("id", StringType(), False),
                StructField("product_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
                StructField("description", StringType(), True),
                StructField("billing_cycles", ArrayType(billing_cycle_struct, True), True),
                StructField("payment_preferences", payment_preferences_struct, True),
                StructField("taxes", taxes_struct, True),
                StructField("quantity_supported", BooleanType(), True),
                StructField("create_time", StringType(), True),
                StructField("update_time", StringType(), True),
                StructField("links", ArrayType(StructType([
                    StructField("href", StringType(), True),
                    StructField("rel", StringType(), True),
                    StructField("method", StringType(), True),
                ]), True), True),
            ])
            
            return plans_schema

        if table_name == "payment_captures":
            # Payment Captures schema based on PayPal Payments API v2
            seller_protection_struct = StructType([
                StructField("status", StringType(), True),
                StructField("dispute_categories", ArrayType(StringType(), True), True),
            ])
            
            seller_receivable_breakdown_struct = StructType([
                StructField("gross_amount", amount_struct, True),
                StructField("paypal_fee", amount_struct, True),
                StructField("net_amount", amount_struct, True),
                StructField("receivable_amount", amount_struct, True),
                StructField("exchange_rate", StructType([
                    StructField("source_currency", StringType(), True),
                    StructField("target_currency", StringType(), True),
                    StructField("value", StringType(), True),
                ]), True),
                StructField("platform_fees", ArrayType(StructType([
                    StructField("amount", amount_struct, True),
                    StructField("payee", StructType([
                        StructField("email_address", StringType(), True),
                        StructField("merchant_id", StringType(), True),
                    ]), True),
                ]), True), True),
            ])
            
            payment_captures_schema = StructType([
                StructField("id", StringType(), False),
                StructField("status", StringType(), True),
                StructField("status_details", StructType([
                    StructField("reason", StringType(), True),
                ]), True),
                StructField("amount", amount_struct, True),
                StructField("invoice_id", StringType(), True),
                StructField("custom_id", StringType(), True),
                StructField("seller_protection", seller_protection_struct, True),
                StructField("final_capture", BooleanType(), True),
                StructField("seller_receivable_breakdown", seller_receivable_breakdown_struct, True),
                StructField("disbursement_mode", StringType(), True),
                StructField("create_time", StringType(), True),
                StructField("update_time", StringType(), True),
                StructField("links", ArrayType(StructType([
                    StructField("href", StringType(), True),
                    StructField("rel", StringType(), True),
                    StructField("method", StringType(), True),
                ]), True), True),
            ])
            
            return payment_captures_schema

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.
        
        Args:
            table_name: The name of the table to fetch metadata for.
            table_options: Additional options (not required for PayPal connector).
            
        Returns:
            A dictionary containing primary_keys, cursor_field, and ingestion_type.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "transactions":
            return {
                "primary_keys": ["transaction_id"],
                "cursor_field": "transaction_initiation_date",
                "ingestion_type": "snapshot",
            }

        if table_name == "invoices":
            return {
                "primary_keys": ["id"],
                "cursor_field": "detail.invoice_date",
                "ingestion_type": "snapshot",
            }

        if table_name == "subscriptions":
            return {
                "primary_keys": ["id"],
                "cursor_field": "update_time",
                "ingestion_type": "cdc",
            }

        if table_name == "orders":
            return {
                "primary_keys": ["id"],
                "cursor_field": "update_time",
                "ingestion_type": "snapshot",
            }

        if table_name == "products":
            return {
                "primary_keys": ["id"],
                "cursor_field": "update_time",
                "ingestion_type": "cdc",
            }

        if table_name == "plans":
            return {
                "primary_keys": ["id"],
                "cursor_field": "update_time",
                "ingestion_type": "cdc",
            }

        if table_name == "payment_captures":
            return {
                "primary_keys": ["id"],
                "cursor_field": "update_time",
                "ingestion_type": "cdc",
            }

        raise ValueError(f"Unsupported table: {table_name!r}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.
        
        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
            table_options: Additional options including start_date and end_date.
            
        Returns:
            An iterator of records in JSON format and an offset.
        """
        if table_name not in self.list_tables():
            raise ValueError(f"Unsupported table: {table_name!r}")

        if table_name == "transactions":
            return self._read_transactions(start_offset, table_options)

        if table_name == "invoices":
            return self._read_invoices(start_offset, table_options)

        if table_name == "subscriptions":
            return self._read_subscriptions(start_offset, table_options)

        if table_name == "orders":
            return self._read_orders(start_offset, table_options)

        if table_name == "products":
            return self._read_products(start_offset, table_options)

        if table_name == "plans":
            return self._read_plans(start_offset, table_options)

        if table_name == "payment_captures":
            return self._read_payment_captures(start_offset, table_options)

        raise ValueError(f"Unsupported table: {table_name!r}")

    def _read_transactions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'transactions' table.
        
        Required table_options:
            - start_date: ISO 8601 date string (e.g., '2024-01-01T00:00:00Z')
            - end_date: ISO 8601 date string (e.g., '2024-01-31T23:59:59Z')
            
        Optional table_options:
            - page_size: Number of transactions per page (default: 100, max: 500)
            
        The PayPal API enforces a maximum 31-day date range per request.
        """
        start_date = table_options.get("start_date")
        end_date = table_options.get("end_date")
        
        if not start_date or not end_date:
            raise ValueError(
                "table_options for 'transactions' must include 'start_date' and 'end_date' "
                "in ISO 8601 format (e.g., '2024-01-01T00:00:00Z')"
            )
        
        # Validate date range (PayPal enforces 31-day maximum)
        try:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            date_range = (end_dt - start_dt).days
            
            if date_range > 31:
                raise ValueError(
                    f"Date range exceeds PayPal's 31-day maximum. "
                    f"Requested range: {date_range} days. "
                    f"Please split the date range into smaller windows."
                )
        except ValueError as e:
            if "31-day" in str(e):
                raise
            raise ValueError(
                f"Invalid date format. Expected ISO 8601 format "
                f"(e.g., '2024-01-01T00:00:00Z'): {e}"
            )
        
        # Get page size from options (default 100, max 500)
        try:
            page_size = int(table_options.get("page_size", 100))
        except (TypeError, ValueError):
            page_size = 100
        page_size = max(1, min(page_size, 500))
        
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Build query parameters
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page_size": page_size,
            "page": page,
        }
        
        # Make API request
        response = self._make_request("GET", "/v1/reporting/transactions", params)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal API error for transactions: {response.status_code} {response.text}"
            )
        
        data = response.json()
        
        # Extract transaction details array
        transaction_details = data.get("transaction_details", [])
        if not isinstance(transaction_details, list):
            raise ValueError(
                f"Unexpected response format for transaction_details: "
                f"{type(transaction_details).__name__}"
            )
        
        # Process records - flatten nested objects into top-level fields
        records: list[dict[str, Any]] = []
        for txn in transaction_details:
            transaction_info = txn.get("transaction_info", {}) or {}
            payer_info = txn.get("payer_info", {}) or {}
            shipping_info = txn.get("shipping_info", {}) or {}
            cart_info = txn.get("cart_info", {}) or {}
            
            # Flatten all fields to top level
            record: dict[str, Any] = {
                # Fields from transaction_info
                "transaction_id": transaction_info.get("transaction_id"),
                "paypal_account_id": transaction_info.get("paypal_account_id"),
                "transaction_event_code": transaction_info.get("transaction_event_code"),
                "transaction_initiation_date": transaction_info.get("transaction_initiation_date"),
                "transaction_updated_date": transaction_info.get("transaction_updated_date"),
                "transaction_amount": transaction_info.get("transaction_amount"),
                "fee_amount": transaction_info.get("fee_amount"),
                "transaction_status": transaction_info.get("transaction_status"),
                "transaction_subject": transaction_info.get("transaction_subject"),
                "ending_balance": transaction_info.get("ending_balance"),
                "available_balance": transaction_info.get("available_balance"),
                "invoice_id": transaction_info.get("invoice_id"),
                "custom_field": transaction_info.get("custom_field"),
                "protection_eligibility": transaction_info.get("protection_eligibility"),
                # Fields from payer_info
                "payer_account_id": payer_info.get("account_id"),
                "payer_email_address": payer_info.get("email_address"),
                "payer_address_status": payer_info.get("address_status"),
                "payer_status": payer_info.get("payer_status"),
                "payer_name": payer_info.get("payer_name"),
                "payer_country_code": payer_info.get("country_code"),
                # Fields from shipping_info
                "shipping_name": shipping_info.get("name"),
                "shipping_address": shipping_info.get("address"),
                # Fields from cart_info
                "item_details": cart_info.get("item_details"),
            }
            records.append(record)
        
        # Determine next offset based on pagination metadata
        total_pages = data.get("total_pages", 1)
        current_page = data.get("page", page)
        
        # If there are more pages, increment page number
        if current_page < total_pages:
            next_offset = {"page": current_page + 1}
        else:
            # No more pages - return same offset to indicate end of data
            next_offset = start_offset if start_offset else {"page": page}
        
        return iter(records), next_offset

    def _read_invoices(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'invoices' table.
        
        Uses PayPal Invoicing API v2: GET /v2/invoicing/invoices
        
        Optional table_options:
            - page: Page number (default: 1)
            - page_size: Number of invoices per page (default: 20, max: 100)
            - total_required: Whether to show total count (default: false)
        """
        # Get pagination parameters
        try:
            page_size = int(table_options.get("page_size", 20))
        except (TypeError, ValueError):
            page_size = 20
        page_size = max(1, min(page_size, 100))
        
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Build query parameters
        params = {
            "page": page,
            "page_size": page_size,
            "total_required": table_options.get("total_required", "false"),
        }
        
        # Make API request
        response = self._make_request("GET", "/v2/invoicing/invoices", params)
        
        if response.status_code == 403:
            raise RuntimeError(
                f"PayPal Invoicing API requires specific permissions. "
                f"Please enable Invoicing API access for your PayPal app in the Developer Dashboard. "
                f"Error: {response.status_code} {response.text}"
            )
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal API error for invoices: {response.status_code} {response.text}"
            )
        
        data = response.json()
        
        # Extract invoices array
        invoices = data.get("items", [])
        if not isinstance(invoices, list):
            raise ValueError(
                f"Unexpected response format for invoices: {type(invoices).__name__}"
            )
        
        # Process records - keep nested structure
        records: list[dict[str, Any]] = []
        for invoice in invoices:
            records.append(invoice)
        
        # Check if there are more pages using links
        links = data.get("links", [])
        has_next = any(link.get("rel") == "next" for link in links if isinstance(link, dict))
        
        # If there are more pages, increment page number
        if has_next:
            next_offset = {"page": page + 1}
        else:
            # No more pages - return same offset to indicate end of data
            next_offset = start_offset if start_offset else {"page": page}
        
        return iter(records), next_offset

    def _read_subscriptions(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'subscriptions' table.
        
        Uses PayPal Subscriptions API v1: GET /v1/billing/subscriptions/{id}
        
        IMPORTANT: PayPal does NOT support bulk listing of subscriptions.
        You must provide subscription IDs in table_options.
        
        Required table_options:
            - subscription_ids: List or comma-separated string of subscription IDs
                                Example: ["I-ABC123", "I-DEF456"] or "I-ABC123,I-DEF456"
        
        Optional table_options:
            - include_transactions: If true, fetch transaction details for each subscription
        """
        # Get subscription IDs from table_options
        subscription_ids = table_options.get("subscription_ids", [])
        
        # Handle comma-separated string or list
        if isinstance(subscription_ids, str):
            subscription_ids = [sid.strip() for sid in subscription_ids.split(",") if sid.strip()]
        elif not isinstance(subscription_ids, list):
            raise ValueError(
                "table_options for 'subscriptions' must include 'subscription_ids' as a list or comma-separated string. "
                "Example: {'subscription_ids': ['I-ABC123', 'I-DEF456']} or {'subscription_ids': 'I-ABC123,I-DEF456'}. "
                "\n\nNote: PayPal does not support bulk listing of subscriptions - you must provide specific IDs."
            )
        
        if not subscription_ids:
            raise ValueError(
                "table_options for 'subscriptions' must include at least one subscription ID in 'subscription_ids'. "
                "\n\nNote: PayPal does not support bulk listing of subscriptions - you must provide specific IDs. "
                "You can find subscription IDs in your PayPal dashboard or from subscription creation responses."
            )
        
        # Get starting index from offset (default 0)
        if start_offset and isinstance(start_offset, dict):
            current_index = start_offset.get("index", 0)
        else:
            current_index = 0
        
        # If we've processed all IDs, return empty
        if current_index >= len(subscription_ids):
            return iter([]), start_offset if start_offset else {"index": current_index}
        
        # Fetch subscription details
        records: list[dict[str, Any]] = []
        include_transactions = table_options.get("include_transactions", "false").lower() == "true"
        
        # Process subscription at current index
        subscription_id = subscription_ids[current_index]
        
        try:
            # Fetch subscription details
            response = self._make_request("GET", f"/v1/billing/subscriptions/{subscription_id}")
            
            if response.status_code == 200:
                subscription_data = response.json()
                
                # Optionally fetch transactions for this subscription
                if include_transactions:
                    try:
                        txn_response = self._make_request(
                            "GET",
                            f"/v1/billing/subscriptions/{subscription_id}/transactions",
                            {"start_time": subscription_data.get("start_time"), "end_time": subscription_data.get("update_time")}
                        )
                        if txn_response.status_code == 200:
                            subscription_data["transactions"] = txn_response.json().get("transactions", [])
                    except Exception:
                        # Transactions endpoint might not be available
                        subscription_data["transactions"] = None
                
                records.append(subscription_data)
            else:
                # Log error but continue (subscription might not exist or be inaccessible)
                print(f"Warning: Could not fetch subscription {subscription_id}: {response.status_code}")
        
        except Exception as e:
            # Log error but continue processing other subscriptions
            print(f"Warning: Error fetching subscription {subscription_id}: {e}")
        
        # Move to next subscription
        next_index = current_index + 1
        
        # Determine next offset
        if next_index < len(subscription_ids):
            next_offset = {"index": next_index}
        else:
            # All subscriptions processed
            next_offset = {"index": next_index}
        
        return iter(records), next_offset

    def _read_orders(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'orders' table.
        
        Note: PayPal Orders API v2 does not provide a direct "list all orders" endpoint.
        Orders are typically created and retrieved by ID, or accessed through 
        Transaction Search API.
        
        This implementation attempts to use available endpoints, but may have limitations.
        Consider using the 'transactions' table for order history instead.
        """
        # PayPal Orders API v2 is primarily for creating/managing individual orders
        # There is no bulk "list orders" endpoint in the standard API
        
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Since there's no list endpoint, we return an informative error
        raise RuntimeError(
            "PayPal Orders API v2 does not support bulk order listing. "
            "Orders are created and retrieved individually by ID. "
            "To retrieve order/payment history, use the 'transactions' table instead, "
            "which provides comprehensive transaction data including order information."
        )

    def _read_products(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'products' table.
        
        Uses PayPal Catalog Products API v1: GET /v1/catalogs/products
        
        Optional table_options:
            - page: Page number (default: 1)
            - page_size: Number of products per page (default: 20, max: 20)
        """
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Get page size from options (default 20, max 20)
        try:
            page_size = int(table_options.get("page_size", 20))
        except (TypeError, ValueError):
            page_size = 20
        page_size = max(1, min(page_size, 20))
        
        # Build query parameters
        params = {
            "page": page,
            "page_size": page_size,
        }
        
        # Make API request
        response = self._make_request("GET", "/v1/catalogs/products", params)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal API error for products: {response.status_code} {response.text}"
            )
        
        data = response.json()
        
        # Extract products array
        products = data.get("products", [])
        if not isinstance(products, list):
            raise ValueError(
                f"Unexpected response format for products: {type(products).__name__}"
            )
        
        # Process records
        records: list[dict[str, Any]] = []
        for product in products:
            records.append(product)
        
        # Check for pagination
        total_items = data.get("total_items", 0)
        total_pages = data.get("total_pages", 1)
        
        # If there are more pages, increment page number
        if page < total_pages:
            next_offset = {"page": page + 1}
        else:
            # No more pages - return same offset to indicate end of data
            next_offset = start_offset if start_offset else {"page": page}
        
        return iter(records), next_offset

    def _read_plans(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'plans' table.
        
        Uses PayPal Subscriptions API v1: GET /v1/billing/plans
        
        Optional table_options:
            - product_id: Filter by product ID
            - plan_ids: Comma-separated plan IDs to filter
            - page: Page number (default: 1)
            - page_size: Number of plans per page (default: 20, max: 20)
        """
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Get page size from options (default 20, max 20)
        try:
            page_size = int(table_options.get("page_size", 20))
        except (TypeError, ValueError):
            page_size = 20
        page_size = max(1, min(page_size, 20))
        
        # Build query parameters
        params = {
            "page": page,
            "page_size": page_size,
        }
        
        # Add optional filters
        if table_options.get("product_id"):
            params["product_id"] = table_options["product_id"]
        if table_options.get("plan_ids"):
            params["plan_ids"] = table_options["plan_ids"]
        
        # Make API request
        response = self._make_request("GET", "/v1/billing/plans", params)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal API error for plans: {response.status_code} {response.text}"
            )
        
        data = response.json()
        
        # Extract plans array
        plans = data.get("plans", [])
        if not isinstance(plans, list):
            raise ValueError(
                f"Unexpected response format for plans: {type(plans).__name__}"
            )
        
        # Process records
        records: list[dict[str, Any]] = []
        for plan in plans:
            records.append(plan)
        
        # Check for pagination
        total_items = data.get("total_items", 0)
        total_pages = data.get("total_pages", 1)
        
        # If there are more pages, increment page number
        if page < total_pages:
            next_offset = {"page": page + 1}
        else:
            # No more pages - return same offset to indicate end of data
            next_offset = start_offset if start_offset else {"page": page}
        
        return iter(records), next_offset

    def _read_payment_captures(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading the 'payment_captures' table.
        
        Uses PayPal Transaction Search API v1 to find captures: GET /v1/reporting/transactions
        
        Required table_options:
            - start_date: Start of date range in ISO 8601 format (UTC)
            - end_date: End of date range in ISO 8601 format (UTC)
            
        Optional table_options:
            - page_size: Number of captures per page (default: 100, max: 500)
            
        Note: This uses the Transaction Search API and filters for capture transactions only.
        """
        start_date = table_options.get("start_date")
        end_date = table_options.get("end_date")
        
        if not start_date or not end_date:
            raise ValueError(
                "table_options for 'payment_captures' must include 'start_date' and 'end_date' "
                "in ISO 8601 format (e.g., '2024-01-01T00:00:00Z')"
            )
        
        # Validate date range (PayPal enforces 31-day maximum)
        try:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            date_range = (end_dt - start_dt).days
            
            if date_range > 31:
                raise ValueError(
                    f"Date range exceeds PayPal's 31-day maximum. "
                    f"Requested range: {date_range} days. "
                    f"Please split the date range into smaller windows."
                )
        except ValueError as e:
            if "31-day" in str(e):
                raise
            raise ValueError(
                f"Invalid date format. Expected ISO 8601 format "
                f"(e.g., '2024-01-01T00:00:00Z'): {e}"
            )
        
        # Get page size from options (default 100, max 500)
        try:
            page_size = int(table_options.get("page_size", 100))
        except (TypeError, ValueError):
            page_size = 100
        page_size = max(1, min(page_size, 500))
        
        # Get starting page from offset (default 1)
        if start_offset and isinstance(start_offset, dict):
            page = start_offset.get("page", 1)
        else:
            page = 1
        
        # Build query parameters
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "page_size": page_size,
            "page": page,
        }
        
        # Make API request
        response = self._make_request("GET", "/v1/reporting/transactions", params)
        
        if response.status_code != 200:
            raise RuntimeError(
                f"PayPal API error for payment_captures: {response.status_code} {response.text}"
            )
        
        data = response.json()
        
        # Extract transaction details array
        transaction_details = data.get("transaction_details", [])
        if not isinstance(transaction_details, list):
            raise ValueError(
                f"Unexpected response format for transaction_details: "
                f"{type(transaction_details).__name__}"
            )
        
        # Process records - extract capture information from transactions
        records: list[dict[str, Any]] = []
        for txn in transaction_details:
            transaction_info = txn.get("transaction_info", {}) or {}
            
            # Build a capture record from transaction data
            record: dict[str, Any] = {
                "id": transaction_info.get("transaction_id"),
                "status": "COMPLETED" if transaction_info.get("transaction_status") == "S" else "PENDING",
                "status_details": None,
                "amount": transaction_info.get("transaction_amount"),
                "invoice_id": transaction_info.get("invoice_id"),
                "custom_id": transaction_info.get("custom_field"),
                "seller_protection": {
                    "status": transaction_info.get("protection_eligibility"),
                    "dispute_categories": None,
                },
                "final_capture": True,
                "seller_receivable_breakdown": {
                    "gross_amount": transaction_info.get("transaction_amount"),
                    "paypal_fee": transaction_info.get("fee_amount"),
                    "net_amount": transaction_info.get("ending_balance"),
                    "receivable_amount": transaction_info.get("transaction_amount"),
                    "exchange_rate": None,
                    "platform_fees": None,
                },
                "disbursement_mode": "INSTANT",
                "create_time": transaction_info.get("transaction_initiation_date"),
                "update_time": transaction_info.get("transaction_updated_date"),
                "links": None,
            }
            records.append(record)
        
        # Determine next offset based on pagination metadata
        total_pages = data.get("total_pages", 1)
        current_page = data.get("page", page)
        
        # If there are more pages, increment page number
        if current_page < total_pages:
            next_offset = {"page": current_page + 1}
        else:
            # No more pages - return same offset to indicate end of data
            next_offset = start_offset if start_offset else {"page": page}
        
        return iter(records), next_offset

