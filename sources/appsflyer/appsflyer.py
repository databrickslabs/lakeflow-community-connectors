from datetime import datetime, timedelta
from typing import Iterator, Any
import time
import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    DoubleType,
    ArrayType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the AppsFlyer connector with connection-level options.

        Expected options:
            - api_token: API token for AppsFlyer authentication (Bearer token).
            - base_url (optional): Override for AppsFlyer API base URL.
              Defaults to https://hq1.appsflyer.com (US region).
              Use https://eu-west1.appsflyer.com for EU region.
        """
        api_token = options.get("api_token")
        if not api_token:
            raise ValueError("AppsFlyer connector requires 'api_token' in options")

        self.base_url = options.get("base_url", "https://hq1.appsflyer.com").rstrip("/")
        self.api_token = api_token

        # Rate limiting: Fixed 60 second delay between requests to avoid WAF challenges
        # AppsFlyer API triggers WAF challenges if requests are too frequent
        self.rate_limit_delay = 60.0  # Fixed at 60 seconds between requests
        self._last_request_time = 0

        # Configure a session with proper headers for AppsFlyer API
        # Note: Management API (/api/mng/*) and Raw Data Export API (/export/*)
        # may require different headers
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
                "User-Agent": "lakeflow-appsflyer-connector/1.0",
                "Accept": "application/json",
            }
        )

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        """
        return [
            "apps",
            "installs_report",
            "in_app_events_report",
            "uninstall_events_report",
            "organic_installs_report",
            "organic_in_app_events_report",
            "daily_report",
            "retargeting_installs_report",
            "retargeting_in_app_events_report",
        ]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        The schema is static and derived from the AppsFlyer API documentation
        and connector design.
        """
        schema_map = {
            "apps": self._get_apps_schema,
            "installs_report": self._get_installs_report_schema,
            "in_app_events_report": self._get_in_app_events_report_schema,
            "uninstall_events_report": self._get_uninstall_events_report_schema,
            # Same as installs
            "organic_installs_report": self._get_installs_report_schema,
            # Same as in_app_events
            "organic_in_app_events_report": self._get_in_app_events_report_schema,
            "daily_report": self._get_daily_report_schema,
            # Same as installs
            "retargeting_installs_report": self._get_installs_report_schema,
            # Same as in_app_events
            "retargeting_in_app_events_report": self._get_in_app_events_report_schema,
        }

        if table_name not in schema_map:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return schema_map[table_name]()

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the given table.
        """
        metadata_map = {
            "apps": {
                "primary_keys": ["app_id"],
                "ingestion_type": "snapshot",
            },
            "installs_report": {
                "primary_keys": ["appsflyer_id", "event_time"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
            "in_app_events_report": {
                "primary_keys": ["appsflyer_id", "event_time", "event_name"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
            "uninstall_events_report": {
                "primary_keys": ["appsflyer_id", "event_time"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
            "organic_installs_report": {
                "primary_keys": ["appsflyer_id", "event_time"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
            "organic_in_app_events_report": {
                "primary_keys": ["appsflyer_id", "event_time", "event_name"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
            "daily_report": {
                "primary_keys": ["date", "media_source"],
                "cursor_field": "date",
                "ingestion_type": "cdc",
            },
            "retargeting_installs_report": {
                "primary_keys": ["appsflyer_id", "event_time"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
            "retargeting_in_app_events_report": {
                "primary_keys": ["appsflyer_id", "event_time", "event_name"],
                "cursor_field": "event_time",
                "ingestion_type": "cdc",
            },
        }

        if table_name not in metadata_map:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return metadata_map[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read records from a table and return raw JSON-like dictionaries.

        For event reports (installs, in_app_events, etc.), this method:
            - Uses `/export/<app_id>/<report_type>/v5` endpoint
            - Supports incremental reads via date range filtering
            - Returns records with event_time as the cursor

        Required table_options for event reports:
            - app_id: AppsFlyer application identifier

        Optional table_options:
            - start_date: Initial ISO date (YYYY-MM-DD) for first run if no start_offset
            - lookback_hours: Lookback window in hours (default: 6)
            - max_days_per_batch: Maximum days to fetch per batch (default: 7)
        """
        reader_map = {
            "apps": self._read_apps,
            "installs_report": self._read_installs_report,
            "in_app_events_report": self._read_in_app_events_report,
            "uninstall_events_report": self._read_uninstall_events_report,
            "organic_installs_report": self._read_organic_installs_report,
            "organic_in_app_events_report": self._read_organic_in_app_events_report,
            "daily_report": self._read_daily_report,
            "retargeting_installs_report": self._read_retargeting_installs_report,
            "retargeting_in_app_events_report": self._read_retargeting_in_app_events_report,
        }

        if table_name not in reader_map:
            raise ValueError(f"Unsupported table: {table_name!r}")
        return reader_map[table_name](start_offset, table_options)

    def _get_apps_schema(self) -> StructType:
        """Return the apps table schema."""
        return StructType(
            [
                StructField("app_id", StringType(), False),
                StructField("app_name", StringType(), True),
                StructField("platform", StringType(), True),
                StructField("bundle_id", StringType(), True),
                StructField("time_zone", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )

    def _get_installs_report_schema(self) -> StructType:
        """Return the installs_report table schema."""
        return StructType(
            [
                # Event identification
                StructField("attributed_touch_type", StringType(), True),
                StructField("attributed_touch_time", StringType(), True),
                StructField("install_time", StringType(), True),
                StructField("event_time", StringType(), False),
                StructField("event_name", StringType(), True),
                StructField("event_value", StringType(), True),
                StructField("event_revenue", DoubleType(), True),
                StructField("event_revenue_currency", StringType(), True),
                StructField("event_revenue_usd", DoubleType(), True),
                StructField("event_source", StringType(), True),
                StructField("is_receipt_validated", BooleanType(), True),
                # Attribution fields
                StructField("af_prt", StringType(), True),
                StructField("media_source", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("keywords", StringType(), True),
                StructField("campaign", StringType(), True),
                StructField("campaign_id", StringType(), True),
                StructField("adset", StringType(), True),
                StructField("adset_id", StringType(), True),
                StructField("ad", StringType(), True),
                StructField("ad_id", StringType(), True),
                StructField("ad_type", StringType(), True),
                StructField("site_id", StringType(), True),
                StructField("sub_site_id", StringType(), True),
                StructField("sub_param_1", StringType(), True),
                StructField("sub_param_2", StringType(), True),
                StructField("sub_param_3", StringType(), True),
                StructField("sub_param_4", StringType(), True),
                StructField("sub_param_5", StringType(), True),
                # Cost fields
                StructField("cost_model", StringType(), True),
                StructField("cost_value", DoubleType(), True),
                StructField("cost_currency", StringType(), True),
                # Multi-touch attribution contributors
                StructField("contributor_1_af_prt", StringType(), True),
                StructField("contributor_1_media_source", StringType(), True),
                StructField("contributor_1_campaign", StringType(), True),
                StructField("contributor_1_touch_type", StringType(), True),
                StructField("contributor_1_touch_time", StringType(), True),
                StructField("contributor_2_af_prt", StringType(), True),
                StructField("contributor_2_media_source", StringType(), True),
                StructField("contributor_2_campaign", StringType(), True),
                StructField("contributor_2_touch_type", StringType(), True),
                StructField("contributor_2_touch_time", StringType(), True),
                StructField("contributor_3_af_prt", StringType(), True),
                StructField("contributor_3_media_source", StringType(), True),
                StructField("contributor_3_campaign", StringType(), True),
                StructField("contributor_3_touch_type", StringType(), True),
                StructField("contributor_3_touch_time", StringType(), True),
                # Geographic fields
                StructField("region", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("state", StringType(), True),
                StructField("city", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("dma", StringType(), True),
                StructField("ip", StringType(), True),
                # Network fields
                StructField("wifi", BooleanType(), True),
                StructField("operator", StringType(), True),
                StructField("carrier", StringType(), True),
                StructField("language", StringType(), True),
                # Device identifiers
                StructField("appsflyer_id", StringType(), False),
                StructField("advertising_id", StringType(), True),
                StructField("idfa", StringType(), True),
                StructField("android_id", StringType(), True),
                StructField("customer_user_id", StringType(), True),
                StructField("imei", StringType(), True),
                StructField("idfv", StringType(), True),
                # Device info
                StructField("platform", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("device_model", StringType(), True),
                StructField("device_category", StringType(), True),
                StructField("os_version", StringType(), True),
                # App info
                StructField("app_version", StringType(), True),
                StructField("sdk_version", StringType(), True),
                StructField("app_id", StringType(), True),
                StructField("app_name", StringType(), True),
                StructField("bundle_id", StringType(), True),
                # Retargeting fields
                StructField("is_retargeting", BooleanType(), True),
                StructField("retargeting_conversion_type", StringType(), True),
                # Additional attribution fields
                StructField("af_siteid", StringType(), True),
                StructField("match_type", StringType(), True),
                StructField("attribution_lookback", StringType(), True),
                StructField("af_keywords", StringType(), True),
                StructField("http_referrer", StringType(), True),
                StructField("original_url", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("is_primary_attribution", BooleanType(), True),
            ]
        )

    def _get_in_app_events_report_schema(self) -> StructType:
        """Return the in_app_events_report table schema."""
        # Inherits most fields from installs schema, with additional event fields
        return StructType(
            [
                # Event-specific fields
                StructField("event_time", StringType(), False),
                StructField("event_name", StringType(), False),
                StructField("event_value", StringType(), True),
                StructField("event_revenue", DoubleType(), True),
                StructField("event_revenue_currency", StringType(), True),
                StructField("event_revenue_usd", DoubleType(), True),
                StructField("af_revenue", DoubleType(), True),
                StructField("af_currency", StringType(), True),
                StructField("af_quantity", LongType(), True),
                StructField("af_content_id", StringType(), True),
                StructField("af_content_type", StringType(), True),
                StructField("af_price", DoubleType(), True),
                # Identity fields
                StructField("appsflyer_id", StringType(), False),
                StructField("customer_user_id", StringType(), True),
                StructField("install_time", StringType(), True),
                # Attribution (inherited from install)
                StructField("media_source", StringType(), True),
                StructField("campaign", StringType(), True),
                StructField("adset", StringType(), True),
                StructField("ad", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("keywords", StringType(), True),
                # Device info
                StructField("platform", StringType(), True),
                StructField("device_model", StringType(), True),
                StructField("os_version", StringType(), True),
                # App info
                StructField("app_id", StringType(), True),
                StructField("app_version", StringType(), True),
                StructField("sdk_version", StringType(), True),
                # Geographic
                StructField("country_code", StringType(), True),
                StructField("city", StringType(), True),
                StructField("region", StringType(), True),
            ]
        )

    def _get_uninstall_events_report_schema(self) -> StructType:
        """Return the uninstall_events_report table schema."""
        return StructType(
            [
                StructField("event_time", StringType(), False),
                StructField("event_name", StringType(), True),
                StructField("appsflyer_id", StringType(), False),
                StructField("customer_user_id", StringType(), True),
                StructField("install_time", StringType(), True),
                StructField("media_source", StringType(), True),
                StructField("campaign", StringType(), True),
                StructField("platform", StringType(), True),
                StructField("app_id", StringType(), True),
                StructField("country_code", StringType(), True),
            ]
        )

    def _get_daily_report_schema(self) -> StructType:
        """Return the daily_report table schema for aggregated metrics."""
        return StructType(
            [
                # Date and grouping dimensions
                StructField("date", StringType(), False),
                StructField("media_source", StringType(), False),
                StructField("campaign", StringType(), True),
                StructField("adset", StringType(), True),
                StructField("ad", StringType(), True),
                StructField("site_id", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("keywords", StringType(), True),
                StructField("geo", StringType(), True),
                StructField("country_code", StringType(), True),
                # Metrics
                StructField("impressions", LongType(), True),
                StructField("clicks", LongType(), True),
                StructField("installs", LongType(), True),
                StructField("conversions", LongType(), True),
                StructField("sessions", LongType(), True),
                StructField("loyal_users", LongType(), True),
                StructField("total_revenue", DoubleType(), True),
                StructField("average_ecpi", DoubleType(), True),
                StructField("total_cost", DoubleType(), True),
                StructField("roi", DoubleType(), True),
                StructField("arpu", DoubleType(), True),
            ]
        )

    def _apply_rate_limit(self):
        """
        Apply rate limiting to avoid triggering WAF challenges.
        Waits if necessary to maintain the configured delay between requests.
        """
        if self.rate_limit_delay > 0:
            current_time = time.time()
            time_since_last_request = current_time - self._last_request_time

            if time_since_last_request < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - time_since_last_request
                time.sleep(sleep_time)

            self._last_request_time = time.time()

    def _read_apps(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the apps snapshot table.
        """
        # Apply rate limiting before making request
        self._apply_rate_limit()

        url = f"{self.base_url}/api/mng/apps"
        response = self._session.get(url, timeout=60)

        if response.status_code != 200:
            raise RuntimeError(
                f"AppsFlyer API error for apps: {response.status_code} {response.text}"
            )

        data = response.json()

        # Handle different response formats
        if isinstance(data, list):
            apps = data
        elif isinstance(data, dict):
            # Response might be wrapped in a dict with 'apps' or 'data' key
            apps = data.get('apps') or data.get('data') or [data]
        else:
            raise ValueError(
                f"Unexpected response format for apps: {type(data).__name__}"
            )

        return iter(apps), {}

    def _read_installs_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the installs_report table using the raw data export API.
        """
        return self._read_event_report(
            "installs_report", start_offset, table_options
        )

    def _read_in_app_events_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the in_app_events_report table.
        """
        return self._read_event_report(
            "in_app_events_report", start_offset, table_options
        )

    def _read_uninstall_events_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the uninstall_events_report table.
        """
        return self._read_event_report(
            "uninstall_events_report", start_offset, table_options
        )

    def _read_organic_installs_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the organic_installs_report table.
        """
        return self._read_event_report(
            "organic_installs_report", start_offset, table_options
        )

    def _read_organic_in_app_events_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the organic_in_app_events_report table.
        """
        return self._read_event_report(
            "organic_in_app_events_report", start_offset, table_options
        )

    def _read_daily_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the daily_report (aggregated) table.
        """
        return self._read_event_report(
            "partners_report", start_offset, table_options, is_aggregated=True
        )

    def _read_retargeting_installs_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the retargeting_installs_report table.
        """
        return self._read_event_report(
            "retargeting_installs_report", start_offset, table_options
        )

    def _read_retargeting_in_app_events_report(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the retargeting_in_app_events_report table.
        """
        return self._read_event_report(
            "retargeting_in_app_events_report", start_offset, table_options
        )

    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    def _read_event_report(
        self,
        report_type: str,
        start_offset: dict,
        table_options: dict[str, str],
        is_aggregated: bool = False,
    ) -> (Iterator[dict], dict):
        """
        Internal implementation for reading event-based reports.

        Args:
            report_type: The report endpoint name (e.g., 'installs_report')
            start_offset: Dictionary containing cursor information
            table_options: Additional options including app_id, start_date, etc.
            is_aggregated: Whether this is an aggregated report (uses 'date' cursor)
        """
        app_id = table_options.get("app_id")
        if not app_id:
            raise ValueError(
                f"table_options for '{report_type}' must include non-empty 'app_id'"
            )

        # Get lookback hours (default 6 hours for late-arriving events)
        try:
            lookback_hours = int(table_options.get("lookback_hours", 6))
        except (TypeError, ValueError):
            lookback_hours = 6

        # Get max days per batch (default 7 days)
        try:
            max_days_per_batch = int(table_options.get("max_days_per_batch", 7))
        except (TypeError, ValueError):
            max_days_per_batch = 7

        # Determine the starting cursor
        cursor = None
        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
        if not cursor:
            cursor = table_options.get("start_date")

        # Calculate date range
        if cursor:
            # Parse cursor and apply lookback
            try:
                cursor_dt = datetime.strptime(cursor, "%Y-%m-%d")
                from_dt = cursor_dt - timedelta(hours=lookback_hours)
            except (ValueError, TypeError):
                # If cursor is not a valid date, default to 30 days ago
                from_dt = datetime.utcnow() - timedelta(days=30)
        else:
            # No cursor, default to 30 days ago
            from_dt = datetime.utcnow() - timedelta(days=30)

        to_dt = datetime.utcnow()

        # Limit to max_days_per_batch
        if (to_dt - from_dt).days > max_days_per_batch:
            to_dt = from_dt + timedelta(days=max_days_per_batch)

        from_date = from_dt.strftime("%Y-%m-%d")
        to_date = to_dt.strftime("%Y-%m-%d")

        # Build request URL
        url = f"{self.base_url}/export/{app_id}/{report_type}/v5"
        params = {
            "from": from_date,
            "to": to_date,
            "timezone": "UTC",
        }

        # Apply rate limiting before making request
        self._apply_rate_limit()

        # Make API request with CSV Accept header
        # Note: Raw Data Export API expects CSV format
        headers = {"Accept": "text/csv"}
        response = self._session.get(url, params=params, headers=headers, timeout=120)

        if response.status_code != 200:
            raise RuntimeError(
                f"AppsFlyer API error for {report_type}: {response.status_code} {response.text}"
            )

        data = response.json() or []
        if not isinstance(data, list):
            raise ValueError(
                f"Unexpected response format for {report_type}: {type(data).__name__}"
            )

        # Find the maximum event_time or date for the next cursor
        max_cursor = cursor
        cursor_field = "date" if is_aggregated else "event_time"

        for record in data:
            record_time = record.get(cursor_field)
            if isinstance(record_time, str):
                if max_cursor is None or record_time > max_cursor:
                    max_cursor = record_time

        # Compute next offset
        if max_cursor:
            # For event_time (ISO datetime), extract just the date part
            if not is_aggregated and max_cursor:
                try:
                    max_cursor = max_cursor[:10]  # Extract YYYY-MM-DD
                except:
                    pass

        # Return records and next offset
        next_offset = {"cursor": max_cursor} if max_cursor else {}
        return iter(data), next_offset
