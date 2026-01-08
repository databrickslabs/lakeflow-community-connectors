import requests
import time
from urllib.parse import urlparse, parse_qs
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DoubleType,
    ArrayType,
    MapType,
)
from typing import Dict, List, Tuple, Iterator


class LakeflowConnect:
    def __init__(self, options: dict) -> None:
        """
        Initialize the Klaviyo connector with API credentials.

        Args:
            options: Dictionary containing:
                - api_key: Klaviyo private API key (pk_*)
        """
        self.api_key = options["api_key"]
        self.base_url = "https://a.klaviyo.com/api"
        self.api_version = "2024-10-15"

        # Request headers for GET requests (no Content-Type needed)
        self.headers = {
            "Authorization": f"Klaviyo-API-Key {self.api_key}",
            "revision": self.api_version,
            "Accept": "application/vnd.api+json",
        }

        # Centralized object metadata configuration
        # supports_page_size: whether endpoint supports page[size] parameter
        self._object_config = {
            "profiles": {
                "primary_keys": ["id"],
                "cursor_field": "updated",
                "ingestion_type": "cdc",
                "endpoint": "profiles",
                "filter_field": "updated",
                "supports_page_size": True,
            },
            "events": {
                "primary_keys": ["id"],
                "cursor_field": "datetime",
                "ingestion_type": "append",
                "endpoint": "events",
                "filter_field": "datetime",
                "supports_page_size": True,
            },
            "lists": {
                "primary_keys": ["id"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "lists",
                "filter_field": None,
                "supports_page_size": False,
            },
            "campaigns": {
                "primary_keys": ["id"],
                "cursor_field": "updated_at",
                "ingestion_type": "cdc",
                "endpoint": "campaigns",
                "filter_field": "updated_at",
                "supports_page_size": False,
                "required_filter": "equals(messages.channel,'email')",
            },
            "metrics": {
                "primary_keys": ["id"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "metrics",
                "filter_field": None,
                "supports_page_size": False,
            },
            "flows": {
                "primary_keys": ["id"],
                "cursor_field": "updated",
                "ingestion_type": "cdc",
                "endpoint": "flows",
                "filter_field": "updated",
                "supports_page_size": True,
            },
            "segments": {
                "primary_keys": ["id"],
                "cursor_field": None,
                "ingestion_type": "snapshot",
                "endpoint": "segments",
                "filter_field": None,
                "supports_page_size": False,
            },
            "templates": {
                "primary_keys": ["id"],
                "cursor_field": "updated",
                "ingestion_type": "cdc",
                "endpoint": "templates",
                "filter_field": "updated",
                "supports_page_size": False,
            },
        }

        # Reusable nested schema for location
        self._location_schema = StructType(
            [
                StructField("address1", StringType(), True),
                StructField("address2", StringType(), True),
                StructField("city", StringType(), True),
                StructField("region", StringType(), True),
                StructField("zip", StringType(), True),
                StructField("country", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("timezone", StringType(), True),
                StructField("ip", StringType(), True),
            ]
        )

        # Nested schema for email marketing subscription
        self._email_marketing_schema = StructType(
            [
                StructField("can_receive_email_marketing", BooleanType(), True),
                StructField("consent", StringType(), True),
                StructField("consent_timestamp", StringType(), True),
                StructField("last_updated", StringType(), True),
                StructField("method", StringType(), True),
                StructField("method_detail", StringType(), True),
                StructField("custom_method_detail", StringType(), True),
                StructField("double_optin", BooleanType(), True),
                StructField("suppression", StringType(), True),
                StructField("list_suppressions", ArrayType(StringType()), True),
            ]
        )

        # Nested schema for SMS marketing subscription
        self._sms_marketing_schema = StructType(
            [
                StructField("can_receive_sms_marketing", BooleanType(), True),
                StructField("consent", StringType(), True),
                StructField("consent_timestamp", StringType(), True),
                StructField("last_updated", StringType(), True),
                StructField("method", StringType(), True),
                StructField("method_detail", StringType(), True),
            ]
        )

        # Nested schema for subscriptions
        self._subscriptions_schema = StructType(
            [
                StructField(
                    "email",
                    StructType(
                        [StructField("marketing", self._email_marketing_schema, True)]
                    ),
                    True,
                ),
                StructField(
                    "sms",
                    StructType(
                        [StructField("marketing", self._sms_marketing_schema, True)]
                    ),
                    True,
                ),
            ]
        )

        # Nested schema for predictive analytics
        self._predictive_analytics_schema = StructType(
            [
                StructField("historic_clv", DoubleType(), True),
                StructField("predicted_clv", DoubleType(), True),
                StructField("total_clv", DoubleType(), True),
                StructField("historic_number_of_orders", LongType(), True),
                StructField("predicted_number_of_orders", LongType(), True),
                StructField("average_days_between_orders", DoubleType(), True),
                StructField("average_order_value", DoubleType(), True),
                StructField("churn_probability", DoubleType(), True),
                StructField("expected_date_of_next_order", StringType(), True),
            ]
        )

        # Nested schema for integration (used in metrics)
        self._integration_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
            ]
        )

        # Nested schema for audiences (used in campaigns)
        self._audiences_schema = StructType(
            [
                StructField("included", ArrayType(StringType()), True),
                StructField("excluded", ArrayType(StringType()), True),
            ]
        )

        # Nested schema for send_options (used in campaigns)
        self._send_options_schema = StructType(
            [
                StructField("use_smart_sending", BooleanType(), True),
                StructField("is_transactional", BooleanType(), True),
            ]
        )

        # Nested schema for tracking_options (used in campaigns)
        self._tracking_options_schema = StructType(
            [
                StructField("is_add_utm", BooleanType(), True),
                StructField("is_tracking_clicks", BooleanType(), True),
                StructField("is_tracking_opens", BooleanType(), True),
                StructField("utm_params", ArrayType(StringType()), True),
            ]
        )

        # Nested schema for send_strategy (used in campaigns)
        self._send_strategy_schema = StructType(
            [
                StructField("method", StringType(), True),
                StructField("options_static", StringType(), True),
                StructField("options_throttled", StringType(), True),
                StructField("options_sto", StringType(), True),
            ]
        )

        # Centralized schema configuration
        self._schema_config = {
            "profiles": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("email", StringType(), True),
                    StructField("phone_number", StringType(), True),
                    StructField("external_id", StringType(), True),
                    StructField("first_name", StringType(), True),
                    StructField("last_name", StringType(), True),
                    StructField("organization", StringType(), True),
                    StructField("locale", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("image", StringType(), True),
                    StructField("created", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("last_event_date", StringType(), True),
                    StructField("location", self._location_schema, True),
                    StructField(
                        "properties", MapType(StringType(), StringType()), True
                    ),
                    StructField("subscriptions", self._subscriptions_schema, True),
                    StructField(
                        "predictive_analytics", self._predictive_analytics_schema, True
                    ),
                ]
            ),
            "events": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("metric_id", StringType(), True),
                    StructField("profile_id", StringType(), True),
                    StructField("timestamp", StringType(), True),
                    StructField("datetime", StringType(), True),
                    StructField(
                        "event_properties", MapType(StringType(), StringType()), True
                    ),
                    StructField("uuid", StringType(), True),
                ]
            ),
            "lists": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("created", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("opt_in_process", StringType(), True),
                ]
            ),
            "campaigns": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("archived", BooleanType(), True),
                    StructField("audiences", self._audiences_schema, True),
                    StructField("send_options", self._send_options_schema, True),
                    StructField("tracking_options", self._tracking_options_schema, True),
                    StructField("send_strategy", self._send_strategy_schema, True),
                    StructField("created_at", StringType(), True),
                    StructField("updated_at", StringType(), True),
                    StructField("scheduled_at", StringType(), True),
                    StructField("send_time", StringType(), True),
                ]
            ),
            "metrics": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("created", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("integration", self._integration_schema, True),
                ]
            ),
            "flows": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("archived", BooleanType(), True),
                    StructField("created", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("trigger_type", StringType(), True),
                ]
            ),
            "segments": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField(
                        "definition", MapType(StringType(), StringType()), True
                    ),
                    StructField("created", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("is_active", BooleanType(), True),
                    StructField("is_processing", BooleanType(), True),
                    StructField("is_starred", BooleanType(), True),
                ]
            ),
            "templates": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("editor_type", StringType(), True),
                    StructField("html", StringType(), True),
                    StructField("text", StringType(), True),
                    StructField("created", StringType(), True),
                    StructField("updated", StringType(), True),
                ]
            ),
        }

    def list_tables(self) -> list[str]:
        """
        List available Klaviyo tables/objects.

        Returns:
            List of supported table names
        """
        return [
            "profiles",
            "events",
            "lists",
            "campaigns",
            "metrics",
            "flows",
            "segments",
            "templates",
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the Spark schema for a Klaviyo table.

        Args:
            table_name: Name of the table
            table_options: Additional options (not used for Klaviyo)

        Returns:
            StructType representing the table schema
        """
        if table_name not in self._schema_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        return self._schema_config[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Get metadata for a Klaviyo table.

        Args:
            table_name: Name of the table
            table_options: Additional options (not used for Klaviyo)

        Returns:
            Dictionary with primary_keys, cursor_field, and ingestion_type
        """
        if table_name not in self._object_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )
        config = self._object_config[table_name]
        return {
            "primary_keys": config["primary_keys"],
            "cursor_field": config["cursor_field"],
            "ingestion_type": config["ingestion_type"],
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[Dict], Dict]:
        """
        Read data from a Klaviyo table.

        Args:
            table_name: Name of the table to read
            start_offset: Dictionary containing cursor information for incremental reads
                - For incremental: {<cursor_field>: <timestamp>}
                - For full refresh: None or {}
            table_options: Additional options (not used for Klaviyo)

        Returns:
            Tuple of (records iterator, new_offset)
        """
        if table_name not in self._object_config:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.list_tables()}"
            )

        config = self._object_config[table_name]
        cursor_field = config["cursor_field"]

        # Determine if this is an incremental read
        is_incremental = (
            start_offset is not None
            and cursor_field is not None
            and start_offset.get(cursor_field) is not None
        )

        if is_incremental:
            return self._read_data_incremental(table_name, start_offset)
        else:
            return self._read_data_full(table_name)

    def _read_data_full(self, table_name: str) -> Tuple[List[Dict], Dict]:
        """
        Read all data from a Klaviyo table (full refresh).

        Args:
            table_name: Name of the table

        Returns:
            Tuple of (all_records, offset)
        """
        config = self._object_config[table_name]
        endpoint = config["endpoint"]
        cursor_field = config["cursor_field"]

        all_records = []
        next_cursor = None
        latest_cursor_value = None

        while True:
            # Build request URL
            url = f"{self.base_url}/{endpoint}/"

            # Build params - only include page[size] if endpoint supports it
            params = {}
            if config.get("supports_page_size", False):
                params["page[size]"] = 20

            # Add required filter if endpoint needs it (e.g., campaigns requires channel filter)
            if config.get("required_filter"):
                params["filter"] = config["required_filter"]

            # Add sort for consistent ordering if cursor field exists
            if cursor_field:
                params["sort"] = cursor_field

            if next_cursor:
                params["page[cursor]"] = next_cursor

            # Make API request
            response = requests.get(url, headers=self.headers, params=params if params else None)

            if response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                raise Exception(
                    f"Klaviyo API error for {table_name}: {response.status_code} {response.text}"
                )

            data = response.json()
            records_data = data.get("data", [])

            if not records_data:
                break

            # Process records - extract attributes and add id
            for record in records_data:
                processed = self._process_record(record)
                all_records.append(processed)

                # Track the latest cursor value for checkpointing
                if cursor_field and processed.get(cursor_field):
                    if (
                        latest_cursor_value is None
                        or processed[cursor_field] > latest_cursor_value
                    ):
                        latest_cursor_value = processed[cursor_field]

            # Check for next page
            links = data.get("links", {})
            next_url = links.get("next")

            if not next_url:
                break

            # Extract cursor from next URL
            next_cursor = self._extract_cursor_from_url(next_url)

            # Rate limiting - be nice to the API
            time.sleep(0.15)

        # Return records and offset for next incremental sync
        offset = {}
        if cursor_field and latest_cursor_value:
            offset[cursor_field] = latest_cursor_value

        return all_records, offset

    def _read_data_incremental(
        self, table_name: str, start_offset: dict
    ) -> Tuple[List[Dict], Dict]:
        """
        Read incremental data from a Klaviyo table using cursor.

        Args:
            table_name: Name of the table
            start_offset: Dictionary with cursor field value

        Returns:
            Tuple of (new_records, new_offset)
        """
        config = self._object_config[table_name]
        endpoint = config["endpoint"]
        cursor_field = config["cursor_field"]
        filter_field = config["filter_field"]

        # Get the starting point from offset
        cursor_start = start_offset.get(cursor_field)

        all_records = []
        next_cursor = None
        latest_cursor_value = cursor_start

        while True:
            # Build request URL
            url = f"{self.base_url}/{endpoint}/"

            # Build params - only include page[size] if endpoint supports it
            params = {}
            if config.get("supports_page_size", False):
                params["page[size]"] = 20

            # Build filter - combine required filter with incremental filter
            filters = []
            if config.get("required_filter"):
                filters.append(config["required_filter"])
            if filter_field and cursor_start:
                filters.append(f"greater-than({filter_field},{cursor_start})")
            if filters:
                # Combine filters with AND
                if len(filters) == 1:
                    params["filter"] = filters[0]
                else:
                    params["filter"] = f"and({','.join(filters)})"

            # Add sort for consistent ordering
            if cursor_field:
                params["sort"] = cursor_field

            if next_cursor:
                params["page[cursor]"] = next_cursor

            # Make API request
            response = requests.get(url, headers=self.headers, params=params if params else None)

            if response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get("Retry-After", 60))
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                raise Exception(
                    f"Klaviyo API error for {table_name}: {response.status_code} {response.text}"
                )

            data = response.json()
            records_data = data.get("data", [])

            if not records_data:
                break

            # Process records
            for record in records_data:
                processed = self._process_record(record)
                all_records.append(processed)

                # Track the latest cursor value
                if cursor_field and processed.get(cursor_field):
                    if processed[cursor_field] > latest_cursor_value:
                        latest_cursor_value = processed[cursor_field]

            # Check for next page
            links = data.get("links", {})
            next_url = links.get("next")

            if not next_url:
                break

            # Extract cursor from next URL
            next_cursor = self._extract_cursor_from_url(next_url)

            # Rate limiting
            time.sleep(0.15)

        # Return new offset for next sync
        offset = {cursor_field: latest_cursor_value}
        return all_records, offset

    def _process_record(self, record: dict) -> dict:
        """
        Process a JSON:API record by extracting attributes and adding id.

        Args:
            record: Raw JSON:API record with type, id, attributes

        Returns:
            Flattened record with id at top level
        """
        result = {"id": record.get("id")}

        # Extract attributes
        attributes = record.get("attributes", {})
        result.update(attributes)

        # Handle relationships if present (extract IDs)
        relationships = record.get("relationships", {})
        for rel_name, rel_data in relationships.items():
            rel_data_inner = rel_data.get("data")
            if rel_data_inner:
                if isinstance(rel_data_inner, dict):
                    result[f"{rel_name}_id"] = rel_data_inner.get("id")
                elif isinstance(rel_data_inner, list):
                    result[f"{rel_name}_ids"] = [
                        item.get("id") for item in rel_data_inner
                    ]

        return result

    def _extract_cursor_from_url(self, url: str) -> str:
        """
        Extract the page cursor from a pagination URL.

        Args:
            url: Full URL with page[cursor] parameter

        Returns:
            Cursor value
        """
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        cursor = query_params.get("page[cursor]", [None])[0]
        return cursor

    def test_connection(self) -> dict:
        """
        Test the connection to Klaviyo API.

        Returns:
            Dictionary with status and message
        """
        try:
            url = f"{self.base_url}/profiles/?page[size]=1"
            response = requests.get(url, headers=self.headers)

            if response.status_code == 200:
                return {"status": "success", "message": "Connection successful"}
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}

