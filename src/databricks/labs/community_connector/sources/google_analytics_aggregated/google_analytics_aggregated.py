import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Any
import requests

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    DateType,
    ArrayType,
)

try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request
except ImportError:
    raise ImportError(
        "google-auth library is required for Google Analytics connector. "
        "Install it with: pip install google-auth"
    )

from databricks.labs.community_connector.interface.lakeflow_connect import (
    LakeflowConnect,
)


class GoogleAnalyticsAggregatedLakeflowConnect(LakeflowConnect):
    # Class-level cache for prebuilt reports (loaded once)
    _prebuilt_reports_cache = None

    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Google Analytics Aggregated Data connector.

        Expected options:
            - property_ids: JSON array of GA4 property IDs (numeric strings)
            - credentials_json: Service account JSON credentials
        """
        self.property_ids = self._parse_property_ids(options)
        self.base_url = "https://analyticsdata.googleapis.com/v1beta"
        self.credentials = self._parse_credentials(options)

        # Create Google service account credentials
        scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
        try:
            self._credentials = (
                service_account.Credentials.from_service_account_info(
                    self.credentials, scopes=scopes
                )
            )
        except Exception as e:
            raise ValueError(
                f"Failed to create credentials from service account: {e}"
            )

        # Fetch and cache metadata for type information
        self._metadata_cache = None

    @staticmethod
    def _parse_property_ids(options):
        """Parse and validate property_ids from connection options."""
        property_ids_json = options.get("property_ids")

        if not property_ids_json:
            raise ValueError(
                "Google Analytics connector requires 'property_ids' (list) "
                "in options. Example: property_ids=['123456789']"
            )

        try:
            if isinstance(property_ids_json, str):
                property_ids = json.loads(property_ids_json)
            else:
                property_ids = property_ids_json

            if not isinstance(property_ids, list) or len(property_ids) == 0:
                raise ValueError("property_ids must be a non-empty list")

            for pid in property_ids:
                if not isinstance(pid, str):
                    raise ValueError(
                        f"All property IDs must be strings, got: {type(pid)}"
                    )

            return property_ids
        except (json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Invalid 'property_ids': {e}")

    @staticmethod
    def _parse_credentials(options):
        """Parse and validate service account credentials."""
        credentials_json = options.get("credentials_json")
        if not credentials_json:
            raise ValueError(
                "Google Analytics connector requires 'credentials_json' "
                "in options"
            )

        if isinstance(credentials_json, str):
            try:
                credentials = json.loads(credentials_json)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON in 'credentials_json': {e}"
                )
        else:
            credentials = credentials_json

        required_fields = [
            "type", "client_email", "private_key", "token_uri"
        ]
        missing_fields = [
            f for f in required_fields if f not in credentials
        ]
        if missing_fields:
            raise ValueError(
                f"Service account credentials missing required fields: "
                f"{missing_fields}"
            )

        return credentials

    @classmethod
    def _load_prebuilt_reports(cls) -> dict:
        """
        Load prebuilt report configurations from prebuilt_reports.json.
        Uses class-level caching to avoid repeated file reads.

        Returns:
            Dictionary mapping report names to their configurations
        """
        if cls._prebuilt_reports_cache is not None:
            return cls._prebuilt_reports_cache

        # Find the prebuilt_reports.json file relative to this module
        current_file = Path(__file__).resolve()
        prebuilt_reports_path = current_file.parent / "prebuilt_reports.json"

        if not prebuilt_reports_path.exists():
            # If file doesn't exist, return empty dict (all reports are custom)
            cls._prebuilt_reports_cache = {}
            return cls._prebuilt_reports_cache

        try:
            with open(prebuilt_reports_path, 'r') as f:
                cls._prebuilt_reports_cache = json.load(f)
            return cls._prebuilt_reports_cache
        except Exception as e:
            raise ValueError(
                f"Failed to load prebuilt reports from "
                f"{prebuilt_reports_path}: {e}"
            )

    def _resolve_table_options(
        self, table_options: dict[str, str]
    ) -> dict[str, str]:
        """
        Resolve table options by merging prebuilt report configuration
        with user overrides.

        If table_options contains 'prebuilt_report', loads that report's
        configuration and merges it with any additional options provided
        by the user.
        """
        prebuilt_report_name = table_options.get("prebuilt_report")

        if not prebuilt_report_name:
            # No prebuilt report specified, return options as-is
            return table_options

        # Load prebuilt reports
        prebuilt_reports = self._load_prebuilt_reports()

        if prebuilt_report_name not in prebuilt_reports:
            available_reports = ', '.join(sorted(prebuilt_reports.keys()))
            raise ValueError(
                f"Prebuilt report '{prebuilt_report_name}' not found. "
                f"Available prebuilt reports: {available_reports}"
            )

        # Start with prebuilt config
        prebuilt_config = prebuilt_reports[prebuilt_report_name].copy()

        # Merge with user-provided options (user options take precedence)
        resolved_options = prebuilt_config.copy()
        for key, value in table_options.items():
            if key != "prebuilt_report":
                resolved_options[key] = value

        return resolved_options

    def _get_access_token(self) -> str:
        """
        Obtain or refresh the OAuth access token using Google's official
        auth library.
        """
        if not self._credentials.valid:
            auth_request = Request()
            self._credentials.refresh(auth_request)

        return self._credentials.token

    def _fetch_metadata(self) -> dict:
        """
        Fetch metadata for dimensions and metrics from the GA4 Data API.
        Returns a dictionary with:
          - 'metric_types': mapping of metric names to their types
          - 'available_dimensions': set of valid dimension names
          - 'available_metrics': set of valid metric names

        This is called once and cached for type inference and validation.
        For multi-property connectors, fetches metadata from the first
        property (standard dimensions/metrics are the same across all).
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        first_property_id = self.property_ids[0]
        url = f"{self.base_url}/properties/{first_property_id}/metadata"
        headers = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(url, headers=headers, timeout=60)
            response.raise_for_status()
            metadata = response.json()

            metric_types = {}
            available_dimensions = set()
            available_metrics = set()

            for dimension in metadata.get("dimensions", []):
                api_name = dimension.get("apiName")
                if api_name:
                    available_dimensions.add(api_name)

            for metric in metadata.get("metrics", []):
                api_name = metric.get("apiName")
                metric_type = metric.get("type")
                if api_name:
                    available_metrics.add(api_name)
                    if metric_type:
                        metric_types[api_name] = metric_type

            self._metadata_cache = {
                "metric_types": metric_types,
                "available_dimensions": available_dimensions,
                "available_metrics": available_metrics,
            }
            return self._metadata_cache

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Failed to fetch metadata from Google Analytics API: {e}"
            )

    def _get_pyspark_type_for_metric(self, metric_type: str):
        """Map Google Analytics metric type to PySpark data type."""
        if metric_type == "TYPE_INTEGER":
            return LongType()
        elif metric_type == "TYPE_MILLISECONDS":
            return LongType()
        elif metric_type in [
            "TYPE_FLOAT",
            "TYPE_CURRENCY",
            "TYPE_SECONDS",
            "TYPE_MINUTES",
            "TYPE_HOURS",
            "TYPE_FEET",
            "TYPE_MILES",
            "TYPE_METERS",
            "TYPE_KILOMETERS",
            "TYPE_STANDARD",
        ]:
            return DoubleType()
        else:
            return StringType()

    def _validate_dimensions_and_metrics(
        self, dimensions: list, metrics: list
    ):
        """
        Validate that requested dimensions and metrics exist in the
        property metadata and that they don't exceed API limits.

        API Limits validated:
        - Maximum 9 dimensions per request
        - Maximum 10 metrics per request
        """
        metadata = self._fetch_metadata()
        available_dimensions = metadata.get("available_dimensions", set())
        available_metrics = metadata.get("available_metrics", set())

        # Check for API limits (validated empirically via test suite)
        max_dimensions = 9
        max_metrics = 10

        dimension_count = len(dimensions)
        metric_count = len(metrics)

        unknown_dimensions = [
            d for d in dimensions if d not in available_dimensions
        ]
        unknown_metrics = [
            m for m in metrics if m not in available_metrics
        ]

        has_errors = (
            unknown_dimensions or unknown_metrics
            or dimension_count > max_dimensions
            or metric_count > max_metrics
        )
        if not has_errors:
            return

        over_limits = (
            dimension_count > max_dimensions
            or metric_count > max_metrics
        )
        error_parts = self._build_validation_error({
            "dimensions": dimensions,
            "metrics": metrics,
            "dimension_count": dimension_count,
            "metric_count": metric_count,
            "max_dimensions": max_dimensions,
            "max_metrics": max_metrics,
            "unknown_dimensions": unknown_dimensions,
            "unknown_metrics": unknown_metrics,
            "available_dimensions": available_dimensions,
            "available_metrics": available_metrics,
            "over_limits": over_limits,
            "property_id": self.property_ids[0],
        })
        raise ValueError("".join(error_parts))

    @staticmethod
    def _build_validation_error(ctx):
        """Build a comprehensive validation error message."""
        error_parts = ["Invalid report configuration:"]

        if ctx["dimension_count"] > ctx["max_dimensions"]:
            error_parts.append(
                f"\n  Too many dimensions: {ctx['dimension_count']} "
                f"(max {ctx['max_dimensions']})"
            )
            error_parts.append(
                f"    The GA4 API limits requests to "
                f"{ctx['max_dimensions']} dimensions."
            )
            error_parts.append(
                f"    Your request has: {ctx['dimensions']}"
            )

        if ctx["metric_count"] > ctx["max_metrics"]:
            error_parts.append(
                f"\n  Too many metrics: {ctx['metric_count']} "
                f"(max {ctx['max_metrics']})"
            )
            error_parts.append(
                f"    The GA4 API limits requests to "
                f"{ctx['max_metrics']} metrics."
            )
            error_parts.append(
                f"    Your request has: {ctx['metrics']}"
            )

        if ctx["unknown_dimensions"]:
            error_parts.append(
                f"\n  Unknown dimensions: {ctx['unknown_dimensions']}"
            )
            sample = sorted(list(ctx["available_dimensions"]))[:10]
            error_parts.append(
                f"    Available dimensions include: {sample}..."
            )

        if ctx["unknown_metrics"]:
            error_parts.append(
                f"\n  Unknown metrics: {ctx['unknown_metrics']}"
            )
            sample = sorted(list(ctx["available_metrics"]))[:10]
            error_parts.append(
                f"    Available metrics include: {sample}..."
            )

        error_parts.append(
            "\n\nTo see all available dimensions and metrics "
            "for your property:"
        )
        error_parts.append(
            f"\n  GET https://analyticsdata.googleapis.com/v1beta/"
            f"properties/{ctx['property_id']}/metadata"
        )

        if ctx["over_limits"]:
            error_parts.append(
                "\n\nTo work around dimension/metric limits:"
            )
            error_parts.append(
                "\n  - Split your report into multiple smaller "
                "reports"
            )
            error_parts.append(
                "\n  - Prioritize the most important dimensions "
                "and metrics"
            )

        return error_parts

    def _make_api_request(
        self, endpoint: str, body: dict, property_id: str,
        retry_count: int = 3
    ) -> dict:
        """
        Make an authenticated API request to Google Analytics Data API
        with retry logic.
        """
        url = f"{self.base_url}/properties/{property_id}:{endpoint}"
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        for attempt in range(retry_count):
            response = requests.post(
                url, headers=headers, json=body, timeout=120
            )

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                wait_time = (2**attempt) * 5
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)

                if attempt < retry_count - 1:
                    time.sleep(wait_time)
                    continue
                raise Exception(
                    f"Rate limit exceeded after {retry_count} retries: "
                    f"{response.text}"
                )

            elif response.status_code == 401:
                if attempt == 0:
                    auth_request = Request()
                    self._credentials.refresh(auth_request)
                    access_token = self._credentials.token
                    headers["Authorization"] = f"Bearer {access_token}"
                    continue
                raise Exception(
                    f"Authentication failed: {response.status_code} - "
                    f"{response.text}"
                )

            elif response.status_code == 403:
                raise Exception(
                    f"Permission denied. Ensure service account has "
                    f"access to property {property_id}: {response.text}"
                )

            else:
                raise Exception(
                    f"API request failed: {response.status_code} - "
                    f"{response.text}"
                )

        raise Exception(
            f"API request failed after {retry_count} retries"
        )

    def _get_effective_options(
        self, table_name, table_options, merge_overrides=False
    ):
        """
        Resolve table options by checking prebuilt reports and merging.

        For prebuilt reports identified by table_name, loads their config.
        If merge_overrides is True, user-provided table_options are merged
        on top (used by read_table for runtime settings like start_date).
        Prebuilt primary_keys are normalized to include property_id.
        """
        prebuilt_reports = self._load_prebuilt_reports()
        is_prebuilt = table_name in prebuilt_reports
        has_custom_dims = "dimensions" in table_options

        if is_prebuilt and not has_custom_dims:
            config = prebuilt_reports[table_name].copy()
            # Normalize: prebuilt primary_keys don't include property_id
            if config.get("primary_keys"):
                config["primary_keys"] = (
                    ["property_id"] + config["primary_keys"]
                )
            if merge_overrides:
                for key, value in table_options.items():
                    config[key] = value
            return config

        if is_prebuilt and has_custom_dims:
            print(
                f"⚠️  WARNING: Custom config for '{table_name}' "
                f"(shadowing prebuilt report)"
            )
            print("    Consider using a different source_table name.")

        return self._resolve_table_options(table_options)

    @staticmethod
    def _parse_dimensions_and_metrics(table_options):
        """Parse and validate dimensions and metrics from table options."""
        dimensions_json = table_options.get("dimensions", "[]")
        metrics_json = table_options.get("metrics", "[]")

        try:
            dimensions = json.loads(dimensions_json)
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid JSON in 'dimensions' option: {dimensions_json}"
            )

        try:
            metrics = json.loads(metrics_json)
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid JSON in 'metrics' option: {metrics_json}"
            )

        if not isinstance(dimensions, list):
            raise ValueError("'dimensions' must be a JSON array of strings")

        if not isinstance(metrics, list) or len(metrics) == 0:
            raise ValueError(
                "'metrics' must be a JSON array of strings with at least "
                "one metric"
            )

        return dimensions, metrics

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        Returns the list of available prebuilt reports. Users can:
        1. Use a prebuilt report name directly as the source_table
        2. Define custom reports with any name via table_options
        """
        prebuilt_reports = self._load_prebuilt_reports()
        return list(prebuilt_reports.keys())

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        Supports prebuilt reports (by table_name) and custom reports
        (via table_options with dimensions/metrics).
        """
        table_options = self._get_effective_options(
            table_name, table_options
        )
        dimensions, metrics = self._parse_dimensions_and_metrics(
            table_options
        )

        # Validate dimensions and metrics exist
        self._validate_dimensions_and_metrics(dimensions, metrics)

        # Fetch metadata to get proper types for metrics
        metadata = self._fetch_metadata()
        metric_types = metadata.get("metric_types", {})

        # Build schema fields
        schema_fields = [
            StructField("property_id", StringType(), False)
        ]

        # Only pure date dimensions use DateType
        date_dimensions = ["date", "firstSessionDate"]
        for dim in dimensions:
            if dim in date_dimensions:
                schema_fields.append(
                    StructField(dim, DateType(), True)
                )
            else:
                schema_fields.append(
                    StructField(dim, StringType(), True)
                )

        for metric in metrics:
            metric_type = metric_types.get(metric)
            if metric_type:
                pyspark_type = self._get_pyspark_type_for_metric(
                    metric_type
                )
                schema_fields.append(
                    StructField(metric, pyspark_type, True)
                )
            else:
                schema_fields.append(
                    StructField(metric, StringType(), True)
                )

        return StructType(schema_fields)

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        Supports prebuilt reports (by table_name) and custom reports
        (via table_options).
        """
        table_options = self._get_effective_options(
            table_name, table_options
        )

        dimensions_json = table_options.get("dimensions", "[]")
        try:
            dimensions = json.loads(dimensions_json)
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid JSON in 'dimensions' option: {dimensions_json}"
            )

        if not isinstance(dimensions, list):
            raise ValueError("'dimensions' must be a JSON array of strings")

        # Determine primary keys
        if "primary_keys" in table_options:
            primary_keys = table_options.get("primary_keys")
        else:
            primary_keys = ["property_id"] + dimensions

        metadata = {"primary_keys": primary_keys}
        if "date" in dimensions:
            metadata["cursor_field"] = "date"
            # Use "cdc" to enable MERGE behavior for lookback_days
            metadata["ingestion_type"] = "cdc"
        else:
            metadata["ingestion_type"] = "snapshot"

        return metadata

    def _parse_metric_value(self, value_str: str, metric_type: str):
        """Parse metric value string according to its type from the API."""
        try:
            if metric_type == "TYPE_INTEGER":
                return int(value_str)
            elif metric_type == "TYPE_MILLISECONDS":
                return int(value_str)
            elif metric_type in [
                "TYPE_FLOAT",
                "TYPE_CURRENCY",
                "TYPE_SECONDS",
                "TYPE_MINUTES",
                "TYPE_HOURS",
                "TYPE_FEET",
                "TYPE_MILES",
                "TYPE_METERS",
                "TYPE_KILOMETERS",
                "TYPE_STANDARD",
            ]:
                return float(value_str)
            else:
                return value_str
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_date_value(dim_value):
        """Parse YYYYMMDD string to YYYY-MM-DD format."""
        try:
            year = int(dim_value[0:4])
            month = int(dim_value[4:6])
            day = int(dim_value[6:8])
            return f"{year:04d}-{month:02d}-{day:02d}"
        except (ValueError, IndexError):
            return None

    def _build_report_request(
        self, dimensions, metrics, table_options, start_offset
    ):
        """Build the GA4 runReport API request body."""
        lookback_days = int(table_options.get("lookback_days", 3))
        page_size = min(
            int(table_options.get("page_size", 10000)), 100000
        )

        if start_offset and "last_date" in start_offset:
            last_date_str = start_offset["last_date"]
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
            start_date = last_date - timedelta(days=lookback_days)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = "today"
        else:
            start_date_str = table_options.get("start_date", "30daysAgo")
            end_date_str = "today"

        date_ranges = [
            {"startDate": start_date_str, "endDate": end_date_str}
        ]
        max_date_ranges = 4
        if len(date_ranges) > max_date_ranges:
            raise ValueError(
                f"Too many date ranges: {len(date_ranges)} "
                f"(maximum {max_date_ranges}). The Google Analytics "
                f"Data API limits requests to {max_date_ranges} "
                f"date ranges."
            )

        request_body = {
            "dateRanges": date_ranges,
            "dimensions": [{"name": dim} for dim in dimensions],
            "metrics": [{"name": metric} for metric in metrics],
            "limit": page_size,
            "offset": 0,
        }

        if "date" in dimensions:
            request_body["orderBys"] = [
                {"dimension": {"dimensionName": "date"}, "desc": False}
            ]

        self._add_optional_filters(request_body, table_options)
        return request_body

    @staticmethod
    def _add_optional_filters(request_body, table_options):
        """Add optional dimension and metric filters to request body."""
        dimension_filter_json = table_options.get("dimension_filter")
        if dimension_filter_json:
            try:
                request_body["dimensionFilter"] = json.loads(
                    dimension_filter_json
                )
            except json.JSONDecodeError:
                raise ValueError(
                    f"Invalid JSON in 'dimension_filter': "
                    f"{dimension_filter_json}"
                )

        metric_filter_json = table_options.get("metric_filter")
        if metric_filter_json:
            try:
                request_body["metricFilter"] = json.loads(
                    metric_filter_json
                )
            except json.JSONDecodeError:
                raise ValueError(
                    f"Invalid JSON in 'metric_filter': "
                    f"{metric_filter_json}"
                )

    def _parse_api_row(
        self, row, dimension_headers, metric_headers, property_id
    ):
        """Parse a single API response row into a record dict.

        Returns (record, date_string_or_None).
        """
        record = {"property_id": property_id}
        row_date = None
        date_dims = {"date", "firstSessionDate"}

        dimension_values = row.get("dimensionValues", [])
        for i, dim_header in enumerate(dimension_headers):
            dim_name = dim_header["name"]
            dim_value = (
                dimension_values[i]["value"]
                if i < len(dimension_values) else None
            )

            is_parseable_date = (
                dim_name in date_dims
                and dim_value and len(dim_value) == 8
            )
            parsed = (
                self._parse_date_value(dim_value)
                if is_parseable_date else None
            )
            if parsed:
                record[dim_name] = parsed
                if dim_name == "date":
                    row_date = parsed
            else:
                record[dim_name] = dim_value

        metric_values = row.get("metricValues", [])
        for i, metric_header in enumerate(metric_headers):
            metric_name = metric_header["name"]
            metric_type = metric_header.get("type", "TYPE_STRING")
            value_str = (
                metric_values[i]["value"]
                if i < len(metric_values) else None
            )

            if value_str is None or value_str == "":
                record[metric_name] = None
            else:
                record[metric_name] = self._parse_metric_value(
                    value_str, metric_type
                )

        return record, row_date

    def _fetch_property_data(self, property_id, request_body, page_size):
        """Fetch all pages of report data for a single property."""
        rows_list = []
        max_date = None
        offset = 0

        while True:
            request_body["offset"] = offset
            response = self._make_api_request(
                "runReport", request_body, property_id
            )

            dimension_headers = response.get("dimensionHeaders", [])
            metric_headers = response.get("metricHeaders", [])
            rows = response.get("rows", [])

            if not rows:
                break

            for row in rows:
                record, row_date = self._parse_api_row(
                    row, dimension_headers, metric_headers, property_id
                )
                rows_list.append(record)
                if row_date and (
                    max_date is None or row_date > max_date
                ):
                    max_date = row_date

            if len(rows) < page_size:
                break

            offset += page_size

        return rows_list, max_date

    def _fetch_report_data(self, request_body, page_size):
        """Fetch report data from all configured properties."""
        all_rows = []
        max_date = None

        for property_id in self.property_ids:
            property_rows, property_max_date = (
                self._fetch_property_data(
                    property_id, request_body, page_size
                )
            )
            all_rows.extend(property_rows)
            if property_max_date and (
                max_date is None or property_max_date > max_date
            ):
                max_date = property_max_date

        return all_rows, max_date

    def read_table(
        self, table_name: str, start_offset: dict,
        table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read the records of a table and return an iterator of records
        and an offset.

        Supports prebuilt reports (by table_name) and custom reports
        (via table_options with dimensions, metrics, filters, etc.).
        """
        table_options = self._get_effective_options(
            table_name, table_options, merge_overrides=True
        )
        dimensions, metrics = self._parse_dimensions_and_metrics(
            table_options
        )

        request_body = self._build_report_request(
            dimensions, metrics, table_options, start_offset
        )
        page_size = min(
            int(table_options.get("page_size", 10000)), 100000
        )

        all_rows, max_date = self._fetch_report_data(
            request_body, page_size
        )

        if max_date:
            next_offset = {"last_date": max_date}
        else:
            next_offset = start_offset if start_offset else {}

        return iter(all_rows), next_offset
