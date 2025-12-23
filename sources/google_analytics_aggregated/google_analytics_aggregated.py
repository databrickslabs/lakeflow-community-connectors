import json
import time
from datetime import datetime, timedelta
from typing import Iterator, Any
import requests

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
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


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Google Analytics Aggregated Data connector with connection-level options.

        Expected options:
            - property_id: Google Analytics 4 property ID (numeric string, e.g., "123456789").
            - credentials_json: Service account JSON credentials as a JSON object or string.
        """
        property_id = options.get("property_id")
        if not property_id:
            raise ValueError(
                "Google Analytics connector requires 'property_id' in options"
            )

        credentials_json = options.get("credentials_json")
        if not credentials_json:
            raise ValueError(
                "Google Analytics connector requires 'credentials_json' in options"
            )

        self.property_id = property_id
        self.base_url = "https://analyticsdata.googleapis.com/v1beta"

        # Parse credentials if provided as string
        if isinstance(credentials_json, str):
            try:
                self.credentials = json.loads(credentials_json)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in 'credentials_json': {e}")
        else:
            self.credentials = credentials_json

        # Validate service account credentials structure
        required_fields = ["type", "client_email", "private_key", "token_uri"]
        missing_fields = [f for f in required_fields if f not in self.credentials]
        if missing_fields:
            raise ValueError(
                f"Service account credentials missing required fields: {missing_fields}"
            )

        # Create Google service account credentials
        scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
        try:
            self._credentials = service_account.Credentials.from_service_account_info(
                self.credentials, scopes=scopes
            )
        except Exception as e:
            raise ValueError(f"Failed to create credentials from service account: {e}")

    def _get_access_token(self) -> str:
        """
        Obtain or refresh the OAuth access token using Google's official auth library.
        The google-auth library handles token refresh, expiry, and caching automatically.
        """
        # Refresh the token if needed (google-auth handles expiry checking internally)
        if not self._credentials.valid:
            auth_request = Request()
            self._credentials.refresh(auth_request)

        return self._credentials.token

    def _make_api_request(
        self, endpoint: str, body: dict, retry_count: int = 3
    ) -> dict:
        """
        Make an authenticated API request to Google Analytics Data API with retry logic.

        Args:
            endpoint: API endpoint path (without base URL)
            body: Request body as dictionary
            retry_count: Number of retries for rate limiting

        Returns:
            Response JSON as dictionary
        """
        url = f"{self.base_url}/properties/{self.property_id}:{endpoint}"
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        for attempt in range(retry_count):
            response = requests.post(url, headers=headers, json=body)

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                # Rate limit exceeded - exponential backoff
                wait_time = (2**attempt) * 5  # 5, 10, 20 seconds
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)

                if attempt < retry_count - 1:
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(
                        f"Rate limit exceeded after {retry_count} retries: {response.text}"
                    )

            elif response.status_code == 401:
                # Token might be expired, try refreshing once
                if attempt == 0:
                    # Force refresh the credentials
                    auth_request = Request()
                    self._credentials.refresh(auth_request)
                    access_token = self._credentials.token
                    headers["Authorization"] = f"Bearer {access_token}"
                    continue
                else:
                    raise Exception(
                        f"Authentication failed: {response.status_code} - {response.text}"
                    )

            elif response.status_code == 403:
                raise Exception(
                    f"Permission denied. Ensure service account has access to property {self.property_id}: {response.text}"
                )

            else:
                raise Exception(
                    f"API request failed: {response.status_code} - {response.text}"
                )

        raise Exception(f"API request failed after {retry_count} retries")

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        For Google Analytics Aggregated Data, we support user-defined custom reports.
        """
        return ["custom_report"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.

        For Google Analytics, the schema is dynamic based on requested dimensions and metrics.
        The table_options must contain:
            - dimensions: JSON array of dimension names (e.g., ["date", "country"])
            - metrics: JSON array of metric names (e.g., ["activeUsers", "sessions"])
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        # Parse dimensions and metrics from table_options
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
            raise ValueError(f"Invalid JSON in 'metrics' option: {metrics_json}")

        if not isinstance(dimensions, list):
            raise ValueError("'dimensions' must be a JSON array of strings")

        if not isinstance(metrics, list) or len(metrics) == 0:
            raise ValueError(
                "'metrics' must be a JSON array of strings with at least one metric"
            )

        # Build schema fields
        schema_fields = []

        # Add dimension fields (all dimensions are strings)
        for dim in dimensions:
            schema_fields.append(StructField(dim, StringType(), True))

        # Add metric fields (we'll use a placeholder schema since we don't know types upfront)
        # In practice, metrics can be TYPE_INTEGER (LongType), TYPE_FLOAT (DoubleType), etc.
        # We'll use DoubleType as default which can accommodate both integers and floats
        for metric in metrics:
            # Use StringType for metrics initially since API returns them as strings
            # The actual type parsing happens in the connector logic
            schema_fields.append(StructField(metric, StringType(), True))

        return StructType(schema_fields)

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.

        Returns metadata including primary keys, cursor field, and ingestion type.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        # Parse dimensions from table_options to determine primary keys
        dimensions_json = table_options.get("dimensions", "[]")
        try:
            dimensions = json.loads(dimensions_json)
        except json.JSONDecodeError:
            raise ValueError(
                f"Invalid JSON in 'dimensions' option: {dimensions_json}"
            )

        if not isinstance(dimensions, list):
            raise ValueError("'dimensions' must be a JSON array of strings")

        # Primary keys are all dimensions (composite key)
        primary_keys = dimensions if dimensions else []

        # Determine cursor field and ingestion type
        # If 'date' dimension is present, use it as cursor for append ingestion
        cursor_field = None
        if "date" in dimensions:
            cursor_field = "date"
            ingestion_type = "append"
        else:
            # Without date dimension, treat as snapshot
            ingestion_type = "snapshot"

        metadata = {
            "primary_keys": primary_keys,
            "ingestion_type": ingestion_type,
        }

        # Only add cursor_field if it exists
        if cursor_field:
            metadata["cursor_field"] = cursor_field

        return metadata

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """
        Read the records of a table and return an iterator of records and an offset.

        Table options:
            - dimensions (required): JSON array of dimension names
            - metrics (required): JSON array of metric names
            - start_date (optional): Start date for first sync (YYYY-MM-DD or relative like "30daysAgo")
            - lookback_days (optional): Number of days to look back for incremental syncs (default: 3)
            - dimension_filter (optional): Filter expression for dimensions (JSON object)
            - metric_filter (optional): Filter expression for metrics (JSON object)
            - page_size (optional): Number of rows per page (default: 10000, max: 100000)
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        # Parse required options
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
            raise ValueError(f"Invalid JSON in 'metrics' option: {metrics_json}")

        if not isinstance(metrics, list) or len(metrics) == 0:
            raise ValueError(
                "'metrics' must be a JSON array with at least one metric"
            )

        # Parse optional parameters
        lookback_days = int(table_options.get("lookback_days", 3))
        page_size = int(table_options.get("page_size", 10000))
        page_size = min(page_size, 100000)  # API maximum

        # Determine date range based on offset and options
        if start_offset and "last_date" in start_offset:
            # Incremental read - use lookback window
            last_date_str = start_offset["last_date"]
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
            start_date = last_date - timedelta(days=lookback_days)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = "today"
        else:
            # Initial read
            start_date_str = table_options.get("start_date", "30daysAgo")
            end_date_str = "today"

        # Build the request body
        request_body = {
            "dateRanges": [{"startDate": start_date_str, "endDate": end_date_str}],
            "dimensions": [{"name": dim} for dim in dimensions],
            "metrics": [{"name": metric} for metric in metrics],
            "limit": page_size,
            "offset": 0,
        }

        # Add sorting by date if present (ascending order for incremental reads)
        if "date" in dimensions:
            request_body["orderBys"] = [
                {"dimension": {"dimensionName": "date"}, "desc": False}
            ]

        # Add optional filters if provided
        dimension_filter_json = table_options.get("dimension_filter")
        if dimension_filter_json:
            try:
                request_body["dimensionFilter"] = json.loads(dimension_filter_json)
            except json.JSONDecodeError:
                raise ValueError(
                    f"Invalid JSON in 'dimension_filter': {dimension_filter_json}"
                )

        metric_filter_json = table_options.get("metric_filter")
        if metric_filter_json:
            try:
                request_body["metricFilter"] = json.loads(metric_filter_json)
            except json.JSONDecodeError:
                raise ValueError(
                    f"Invalid JSON in 'metric_filter': {metric_filter_json}"
                )

        # Fetch all pages
        all_rows = []
        offset = 0
        max_date = None

        while True:
            request_body["offset"] = offset

            # Make API request
            response = self._make_api_request("runReport", request_body)

            # Extract dimension and metric headers
            dimension_headers = response.get("dimensionHeaders", [])
            metric_headers = response.get("metricHeaders", [])
            rows = response.get("rows", [])

            if not rows:
                break

            # Parse rows into dictionaries
            for row in rows:
                record = {}

                # Parse dimension values
                dimension_values = row.get("dimensionValues", [])
                for i, dim_header in enumerate(dimension_headers):
                    dim_name = dim_header["name"]
                    dim_value = (
                        dimension_values[i]["value"] if i < len(dimension_values) else None
                    )

                    # Parse date dimension from YYYYMMDD to YYYY-MM-DD for easier handling
                    if dim_name == "date" and dim_value and len(dim_value) == 8:
                        try:
                            parsed_date = f"{dim_value[0:4]}-{dim_value[4:6]}-{dim_value[6:8]}"
                            record[dim_name] = parsed_date

                            # Track max date for cursor
                            if max_date is None or parsed_date > max_date:
                                max_date = parsed_date
                        except (ValueError, IndexError):
                            record[dim_name] = dim_value
                    else:
                        record[dim_name] = dim_value

                # Parse metric values (keep as strings to match schema)
                metric_values = row.get("metricValues", [])
                for i, metric_header in enumerate(metric_headers):
                    metric_name = metric_header["name"]
                    metric_value = (
                        metric_values[i]["value"] if i < len(metric_values) else None
                    )
                    record[metric_name] = metric_value

                all_rows.append(record)

            # Check if we've reached the last page
            if len(rows) < page_size:
                break

            offset += page_size

        # Determine next offset
        # For append ingestion with date cursor, track the maximum date seen
        if max_date:
            next_offset = {"last_date": max_date}
        else:
            # For snapshot or no date dimension, return same offset to signal completion
            next_offset = start_offset if start_offset else {}

        return iter(all_rows), next_offset
