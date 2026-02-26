# pylint: disable=too-many-lines
import io
import json
import logging
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Iterator

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.qualtrics.qualtrics_schemas import (
    TABLE_SCHEMAS,
    TABLE_METADATA,
    SUPPORTED_TABLES,
)
from databricks.labs.community_connector.sources.qualtrics.qualtrics_utils import (
    QualtricsConfig,
    normalize_keys,
)

# Configure logging for DLT/Lakeflow compatibility (stderr works best in Databricks)
logger = logging.getLogger("QualtricsConnector")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(
        logging.Formatter("%(asctime)s - QUALTRICS - %(levelname)s - %(message)s")
    )
    logger.addHandler(_handler)


class QualtricsLakeflowConnect(LakeflowConnect):  # pylint: disable=too-many-instance-attributes
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Qualtrics source connector with authentication parameters.

        Args:
            options: Dictionary containing:
                - api_token: Qualtrics API token
                - datacenter_id: Datacenter identifier (e.g., 'fra1', 'ca1',
                  'yourdatacenterid')
                - max_surveys: (Optional) Maximum number of surveys to
                  consolidate when surveyId is not provided (default: 50)
        """
        self.api_token = options.get("api_token")
        self.datacenter_id = options.get("datacenter_id")

        if not self.api_token:
            raise ValueError("api_token is required")
        if not self.datacenter_id:
            raise ValueError("datacenter_id is required")

        self.base_url = f"https://{self.datacenter_id}.qualtrics.com/API/v3"

        # Configure a session with proper headers for connection pooling
        # This improves performance by reusing TCP connections across requests
        self._session = requests.Session()
        self._session.headers.update({
            "X-API-TOKEN": self.api_token,
            "Content-Type": "application/json"
        })

        # Validate credentials on initialization
        self._validate_credentials()

        # Auto-consolidation configuration
        max_surveys_str = options.get(
            "max_surveys", str(QualtricsConfig.DEFAULT_MAX_SURVEYS)
        )
        try:
            self.max_surveys = int(max_surveys_str)
        except ValueError:
            logger.warning(
                f"Invalid max_surveys value '{max_surveys_str}', "
                f"using default {QualtricsConfig.DEFAULT_MAX_SURVEYS}"
            )
            self.max_surveys = QualtricsConfig.DEFAULT_MAX_SURVEYS

        self._init_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._default_max_records_per_batch = 100_000

        # Reader method mappings
        self._reader_methods = {
            "surveys": self._read_surveys,
            "survey_definitions": self._read_survey_definitions,
            "survey_responses": self._read_survey_responses,
            "distributions": self._read_distributions,
            "mailing_lists": self._read_mailing_lists,
            "mailing_list_contacts": self._read_mailing_list_contacts,
            "directory_contacts": self._read_directory_contacts,
            "directories": self._read_directories,
            "users": self._read_users,
        }

    def _validate_credentials(self) -> None:
        """
        Validate API credentials by making a lightweight API call.

        Raises:
            ValueError: If the API returns an error response
        """
        url = f"{self.base_url}/surveys?pageSize=1"
        try:
            response = self._session.get(url, timeout=QualtricsConfig.REQUEST_TIMEOUT)

            if not response.ok:
                # Extract Qualtrics error message from response
                error_message = f"HTTP {response.status_code}"
                try:
                    error_json = response.json()
                    meta = error_json.get("meta", {})
                    http_status = meta.get("httpStatus", "")
                    error_info = meta.get("error", {})
                    error_detail = error_info.get("errorMessage", "")
                    if error_detail:
                        error_message = f"{http_status}: {error_detail}"
                    elif http_status:
                        error_message = http_status
                except Exception:
                    pass

                raise ValueError(
                    f"Qualtrics API credential validation failed. {error_message}"
                )

            logger.info("Qualtrics API credentials validated successfully")

        except requests.exceptions.ConnectionError as e:
            raise ValueError(
                f"Cannot connect to Qualtrics API at {self.base_url}. "
                f"Verify datacenter_id '{self.datacenter_id}' is correct."
            ) from e
        except requests.exceptions.Timeout:
            raise ValueError(
                f"Connection to Qualtrics API timed out at {self.base_url}."
            )

    def list_tables(self) -> list[str]:
        """
        List all available tables supported by this connector.

        Returns:
            List of table names
        """
        return SUPPORTED_TABLES.copy()

    # =========================================================================
    # Schema and Metadata (delegated to qualtrics_schemas module)
    # =========================================================================

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Get the schema for the specified table.

        Args:
            table_name: Name of the table
            table_options: Additional options (e.g., surveyId for survey_responses)

        Returns:
            StructType representing the table schema
        """
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {SUPPORTED_TABLES}"
            )
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Get metadata for the specified table.

        Args:
            table_name: Name of the table
            table_options: Additional options

        Returns:
            Dictionary containing primary_keys, cursor_field, and ingestion_type
        """
        if table_name not in TABLE_METADATA:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {SUPPORTED_TABLES}"
            )
        return TABLE_METADATA[table_name]

    # =========================================================================
    # Table Reading (routing)
    # =========================================================================

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read data from the specified table.

        Args:
            table_name: Name of the table to read
            start_offset: Starting offset for incremental reads
            table_options: Additional options (e.g., surveyId for survey_responses)

        Returns:
            Tuple of (iterator of records, end offset)
        """
        if table_name not in self._reader_methods:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {SUPPORTED_TABLES}"
            )

        max_records = int(
            table_options.get(
                "max_records_per_batch", self._default_max_records_per_batch
            )
        )

        reader_method = self._reader_methods[table_name]

        # surveys, directories, and users don't need table_options
        if table_name in ("surveys", "directories", "users"):
            return reader_method(start_offset, max_records)

        return reader_method(start_offset, table_options, max_records)

    # =========================================================================
    # HTTP Helpers
    # =========================================================================

    def _fetch_paginated_list(  # pylint: disable=too-many-locals,too-many-branches,too-many-arguments,too-many-positional-arguments
        self,
        endpoint: str,
        start_offset: dict,
        cursor_field: str = None,
        extra_params: dict = None,
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Generic paginated list API helper for standard Qualtrics list endpoints.

        Bounded by *max_records* per call.  When all pages are exhausted
        and a ``cursor_field`` is present, the cursor is capped at
        ``_init_time`` to guarantee termination under ``availableNow``.

        Args:
            endpoint: API endpoint path (e.g., "/surveys", "/distributions")
            start_offset: Dictionary with cursor info
            cursor_field: Field name for incremental filtering (e.g., "lastModified")
            extra_params: Additional query params (e.g., {"surveyId": "SV_xxx"})
            max_records: Maximum number of records to return (None = unlimited)

        Returns:
            Tuple of (iterator of normalized records, new offset)
        """
        all_items: list[dict] = []
        skip_token = start_offset.get("skipToken") if start_offset else None
        cursor_value = start_offset.get(cursor_field) if start_offset and cursor_field else None
        pages_exhausted = True

        while max_records is None or len(all_items) < max_records:
            url = f"{self.base_url}{endpoint}"
            params = {"pageSize": QualtricsConfig.DEFAULT_PAGE_SIZE}

            if skip_token:
                params["skipToken"] = skip_token
            if extra_params:
                params.update(extra_params)

            try:
                response = self._make_request("GET", url, params=params)
                result = response.get("result", {})
                elements = result.get("elements", [])

                if not elements:
                    break

                if cursor_field and cursor_value:
                    items_to_add = [
                        item for item in elements
                        if item.get(cursor_field, "") and item.get(cursor_field, "") > cursor_value
                    ]
                else:
                    items_to_add = elements

                for item in items_to_add:
                    all_items.append(item)
                    if max_records is not None and len(all_items) >= max_records:
                        pages_exhausted = False
                        break

                if not pages_exhausted:
                    break

                next_page = result.get("nextPage")
                if next_page and "skipToken=" in next_page:
                    skip_token = next_page.split("skipToken=")[-1].split("&")[0]
                else:
                    break

            except Exception as e:
                logger.error(f"Error fetching {endpoint}: {e}", exc_info=True)
                break

        new_offset: dict = {}
        if cursor_field:
            if all_items:
                dates = [item.get(cursor_field, "") for item in all_items if item.get(cursor_field)]
                if dates:
                    max_date = max(dates)
                    if pages_exhausted and max_date > self._init_time:
                        max_date = self._init_time
                    new_offset[cursor_field] = max_date
                elif cursor_value:
                    new_offset[cursor_field] = cursor_value
            elif cursor_value:
                new_offset[cursor_field] = cursor_value

        normalized = (normalize_keys(item) for item in all_items)
        return normalized, new_offset

    def _iterate_all_surveys(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-statements
        self,
        start_offset: dict,
        table_options: dict[str, str],
        single_survey_reader,
        cursor_field: str,
        data_type: str,
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Generic helper to consolidate data across all surveys using parallel processing.

        Uses ThreadPoolExecutor to fetch data from multiple surveys concurrently,
        providing 3-5x speedup compared to sequential processing while respecting
        Qualtrics rate limits (3000 requests/min per brand).

        Bounded by *max_records* total and capped at ``_init_time`` for
        termination guarantee.

        Args:
            start_offset: Dictionary with per-survey cursors {"surveys": {...}}
            table_options: Passed to _get_all_survey_ids (currently not used)
            single_survey_reader: Function(survey_id, offset) -> (Iterator[dict], dict)
            cursor_field: Field name for global cursor (e.g., "lastModified")
            data_type: Data type name for logging (e.g., "definitions")
            max_records: Maximum total records to keep (None = unlimited)

        Returns:
            Tuple of (iterator of all records, consolidated offset dict)
        """
        survey_ids = self._get_all_survey_ids(table_options)
        if not survey_ids:
            logger.warning(f"No surveys found to fetch {data_type} for")
            return iter([]), {}

        num_surveys = len(survey_ids)
        num_workers = min(QualtricsConfig.MAX_PARALLEL_WORKERS, num_surveys)
        logger.info(
            f"Fetching {data_type} for {num_surveys} survey(s) "
            f"using {num_workers} parallel workers"
        )

        per_survey_offsets = start_offset.get("surveys", {}) if start_offset else {}
        all_records: list[dict] = []
        new_per_survey_offsets: dict = {}
        success_count = 0
        failure_count = 0

        def fetch_survey_data(survey_id: str) -> tuple:
            """Fetch data for a single survey. Returns (survey_id, records, offset, error)."""
            try:
                survey_offset = per_survey_offsets.get(survey_id, {})
                records_iter, new_survey_offset = single_survey_reader(survey_id, survey_offset)
                records = list(records_iter)
                return (survey_id, records, new_survey_offset, None)
            except Exception as e:
                return (survey_id, [], None, str(e))

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_survey = {
                executor.submit(fetch_survey_data, survey_id): survey_id
                for survey_id in survey_ids
            }

            for future in as_completed(future_to_survey):
                survey_id = future_to_survey[future]
                try:
                    sid, records, new_offset, error = future.result()
                    if error:
                        logger.warning(f"Failed to fetch {data_type} for survey {sid}: {error}")
                        failure_count += 1
                        if sid in per_survey_offsets:
                            new_per_survey_offsets[sid] = per_survey_offsets[sid]
                    else:
                        remaining = None
                        if max_records is not None:
                            remaining = max_records - len(all_records)
                            if remaining <= 0:
                                new_per_survey_offsets[sid] = new_offset
                                success_count += 1
                                continue
                            records = records[:remaining]
                        all_records.extend(records)
                        new_per_survey_offsets[sid] = new_offset
                        success_count += 1
                        logger.info(
                            f"Fetched {len(records)} {data_type} record(s) for survey {sid} "
                            f"({success_count + failure_count}/{num_surveys})"
                        )
                except Exception as e:
                    logger.error(f"Unexpected error processing survey {survey_id}: {e}")
                    failure_count += 1

        batch_full = max_records is not None and len(all_records) >= max_records

        logger.info(
            f"Completed fetching {data_type}: {success_count} succeeded, "
            f"{failure_count} failed, {len(all_records)} total records"
        )

        new_offset: dict = {"surveys": new_per_survey_offsets}
        if all_records and cursor_field:
            dates = [r.get(cursor_field, "") for r in all_records if r.get(cursor_field)]
            if dates:
                max_date = max(dates)
                if not batch_full and max_date > self._init_time:
                    max_date = self._init_time
                new_offset[cursor_field] = max_date

        return iter(all_records), new_offset

    def _get_all_survey_ids(self, table_options: dict[str, str]) -> list[str]:
        """
        Get survey IDs for consolidation.

        If surveyId is provided in table_options, uses that (supports comma-separated list).
        Otherwise, fetches all surveys up to the max_surveys limit, sorted by lastModified
        date (newest first) to prioritize recently updated surveys.

        Args:
            table_options: May contain 'surveyId' parameter (single or comma-separated)

        Returns:
            List of survey IDs
        """
        survey_id_input = table_options.get("surveyId") or table_options.get("surveyid")
        if survey_id_input:
            # Parse comma-separated list, strip whitespace
            survey_ids = [sid.strip() for sid in survey_id_input.split(",") if sid.strip()]
            logger.info(f"Using {len(survey_ids)} specified survey(s): {survey_ids}")
            return survey_ids

        # Fetch all surveys
        surveys_iter, _ = self._read_surveys({})
        surveys = list(surveys_iter)

        if not surveys:
            logger.warning("No surveys found")
            return []

        # Sort surveys by lastModified date (newest first) to prioritize recent surveys
        # when max_surveys limit is applied
        surveys.sort(key=lambda s: s.get("lastModified", ""), reverse=True)

        # Get survey IDs up to max limit (includes all surveys - active and inactive)
        survey_ids = []
        for survey in surveys:
            survey_id = survey.get("id")
            if survey_id:
                survey_ids.append(survey_id)

            # Check max limit
            if len(survey_ids) >= self.max_surveys:
                logger.warning(
                    f"Reached max_surveys limit of {self.max_surveys}. "
                    "Consider increasing this limit in connection config if needed."
                )
                break

        logger.info(
            f"Found {len(survey_ids)} survey(s) to process "
            f"(max_surveys={self.max_surveys}), sorted by lastModified (newest first)"
        )
        return survey_ids

    def _make_request(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        method: str,
        url: str,
        params: dict = None,
        json_body: dict = None,
        max_retries: int = QualtricsConfig.MAX_HTTP_RETRIES
    ) -> dict:
        """
        Make an HTTP request to Qualtrics API with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            params: Query parameters
            json_body: JSON body for POST requests
            max_retries: Maximum number of retry attempts

        Returns:
            Parsed JSON response
        """
        for attempt in range(max_retries):
            try:
                if method == "GET":
                    response = self._session.get(
                        url, params=params, timeout=QualtricsConfig.REQUEST_TIMEOUT
                    )
                elif method == "POST":
                    response = self._session.post(
                        url, json=json_body, timeout=QualtricsConfig.REQUEST_TIMEOUT
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get(
                        "Retry-After", QualtricsConfig.RATE_LIMIT_DEFAULT_WAIT
                    ))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                # Capture response body for debugging before raising
                if not response.ok:
                    try:
                        error_detail = response.json()
                        logger.error(f"API Error Response: {error_detail}")
                    except Exception:
                        logger.error(f"API Error Response (raw): {response.text}")

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Request failed after {max_retries} attempts: {e}")

                # Exponential backoff
                wait_time = 2 ** attempt
                logger.warning(
                    f"Request failed, retrying in {wait_time} seconds... "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)

        raise Exception("Request failed")

    # =========================================================================
    # Table Readers: Surveys
    # =========================================================================

    def _read_surveys(
        self, start_offset: dict, max_records: int = None
    ) -> (Iterator[dict], dict):
        """
        Read surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing pagination token and cursor timestamp
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of survey records, new offset)
        """
        return self._fetch_paginated_list(
            "/surveys", start_offset, cursor_field="lastModified",
            max_records=max_records,
        )

    # =========================================================================
    # Table Readers: Survey Definitions
    # =========================================================================

    def _read_survey_definitions(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read survey definition from Qualtrics API.

        Args:
            start_offset: Dictionary containing cursor timestamp
                - For single survey: {"lastModified": "2024-01-01T00:00:00Z"}
                - For auto-consolidation: {"surveys": {"SV_123":
                  {"lastModified": "..."}, ...}, "lastModified": "..."}
            table_options: Optional 'surveyId' parameter
                - Single survey: "SV_123" - Returns definition for that survey
                - Multiple surveys: "SV_123, SV_456, SV_789" - Returns
                  definitions for specified surveys (comma-separated)
                - All surveys: omit surveyId - Returns definitions for
                  all surveys (auto-consolidation)
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of survey definition records, offset dict)
        """
        survey_id_input = table_options.get("surveyId") or table_options.get("surveyid")

        # Single survey (no comma) - use simple offset structure for backward compatibility
        if survey_id_input and "," not in survey_id_input:
            return self._read_single_survey_definition(survey_id_input.strip(), start_offset)

        # Multiple surveys (comma-separated) or all surveys -
        # use consolidated path with per-survey offsets
        if survey_id_input:
            logger.info(
                "Multiple surveyIds provided, auto-consolidating "
                "definitions from specified surveys"
            )
        else:
            logger.info("No surveyId provided, auto-consolidating definitions from all surveys")
        return self._read_all_survey_definitions(start_offset, table_options, max_records)

    def _read_single_survey_definition(  # pylint: disable=too-many-locals
        self, survey_id: str, start_offset: dict
    ) -> (Iterator[dict], dict):
        """
        Read a single survey definition from Qualtrics API.

        Args:
            survey_id: The survey ID to fetch
            start_offset: Dictionary containing cursor timestamp

        Returns:
            Tuple of (iterator of survey definition record, new offset dict)
        """
        last_modified_cursor = start_offset.get("lastModified") if start_offset else None

        url = f"{self.base_url}/survey-definitions/{survey_id}"

        try:
            response = self._make_request("GET", url)
            result = response.get("result", {})

            if not result:
                logger.warning(f"No survey definition found for survey {survey_id}")
                # Return existing cursor if no definition found
                new_offset = {}
                if last_modified_cursor:
                    new_offset["lastModified"] = last_modified_cursor
                return iter([]), new_offset

            # Check if this definition was modified since last sync (CDC mode)
            survey_last_modified = result.get("LastModified")
            if last_modified_cursor and survey_last_modified:
                if survey_last_modified <= last_modified_cursor:
                    # No changes since last sync, skip this definition
                    logger.info(
                        f"Survey {survey_id} not modified since "
                        f"{last_modified_cursor}, skipping"
                    )
                    return iter([]), {"lastModified": last_modified_cursor}

            # Process the result to serialize complex nested fields as JSON strings
            # This is needed because the API returns variable structures (dict or array)
            # for fields like Blocks, which can't be handled by a fixed schema
            processed = {}

            # Copy simple string fields as-is
            simple_fields = [
                "SurveyID", "SurveyName", "SurveyStatus", "OwnerID", "CreatorID",
                "BrandID", "BrandBaseURL", "LastModified", "LastAccessed",
                "LastActivated", "QuestionCount"
            ]
            for field_name in simple_fields:
                processed[field_name] = result.get(field_name)

            # Serialize complex nested fields as JSON strings
            complex_fields = [
                "Questions", "Blocks", "SurveyFlow", "SurveyOptions",
                "ResponseSets", "Scoring", "ProjectInfo"
            ]
            for field_name in complex_fields:
                value = result.get(field_name)
                if value is not None:
                    processed[field_name] = json.dumps(value)
                else:
                    processed[field_name] = None

            # Normalize all keys to snake_case before returning
            normalized = normalize_keys(processed)

            # Calculate new offset based on this definition's last_modified
            new_offset = {}
            if survey_last_modified:
                capped = min(survey_last_modified, self._init_time)
                new_offset["lastModified"] = capped
            elif last_modified_cursor:
                new_offset["lastModified"] = last_modified_cursor

            return iter([normalized]), new_offset

        except Exception as e:
            logger.error(f"Error fetching survey definition for {survey_id}: {e}", exc_info=True)
            raise

    def _read_all_survey_definitions(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read survey definitions for all surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing per-survey cursor timestamps
            table_options: Not used for auto-consolidation
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of all survey definition records,
                  new offset dict with per-survey cursors)
        """
        return self._iterate_all_surveys(
            start_offset, table_options,
            self._read_single_survey_definition,
            cursor_field="last_modified",
            data_type="definitions",
            max_records=max_records,
        )

    # =========================================================================
    # Table Readers: Survey Responses
    # =========================================================================

    def _read_survey_responses(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read survey responses using the Qualtrics export API.

        This requires a 3-step process:
        1. Create export job
        2. Poll for completion
        3. Download and parse results

        Args:
            start_offset: Dictionary containing cursor timestamp(s)
            table_options: Optional 'surveyId' parameter
                - Single survey: "SV_123" - Returns responses for that survey
                - Multiple surveys: "SV_123, SV_456, SV_789" - Returns
                  responses for specified surveys (comma-separated)
                - All surveys: omit surveyId - Returns responses for
                  all surveys (auto-consolidation)
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of response records, new offset)
        """
        survey_id_input = table_options.get("surveyId") or table_options.get("surveyid")

        # Single survey (no comma) - use simple offset structure for backward compatibility
        if survey_id_input and "," not in survey_id_input:
            return self._read_single_survey_responses(survey_id_input.strip(), start_offset)

        # Multiple surveys (comma-separated) or all surveys -
        # use consolidated path with per-survey offsets
        if survey_id_input:
            logger.info(
                "Multiple surveyIds provided, consolidating "
                "responses from specified surveys"
            )
        else:
            logger.info("No surveyId provided, auto-consolidating responses from all surveys")
        return self._read_all_survey_responses(start_offset, table_options, max_records)

    def _read_single_survey_responses(
        self, survey_id: str, start_offset: dict
    ) -> (Iterator[dict], dict):
        """
        Read survey responses for a single survey using the Qualtrics export API.

        Args:
            survey_id: The survey ID to export responses from
            start_offset: Dictionary containing cursor timestamp

        Returns:
            Tuple of (iterator of response records, new offset)
        """
        recorded_date_cursor = start_offset.get("recordedDate") if start_offset else None

        # Step 1: Create export
        # Note: useLabels parameter cannot be used with JSON format per Qualtrics API
        export_body = {
            "format": "json"
        }

        # Add incremental filter if cursor exists
        if recorded_date_cursor:
            export_body["startDate"] = recorded_date_cursor

        progress_id = self._create_response_export(survey_id, export_body)

        # Step 2: Poll for completion
        file_id = self._poll_export_progress(survey_id, progress_id)

        # Step 3: Download and parse
        responses = self._download_response_export(survey_id, file_id)

        # Calculate new offset with _init_time cap for termination
        new_offset = {}
        if responses:
            recorded_dates = [
                resp.get("recordedDate", "")
                for resp in responses
                if resp.get("recordedDate")
            ]
            if recorded_dates:
                max_recorded_date = min(max(recorded_dates), self._init_time)
                new_offset["recordedDate"] = max_recorded_date
            elif recorded_date_cursor:
                new_offset["recordedDate"] = recorded_date_cursor
        elif recorded_date_cursor:
            new_offset["recordedDate"] = recorded_date_cursor

        return iter(responses), new_offset

    def _read_all_survey_responses(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read survey responses for all surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing per-survey cursor timestamps
            table_options: Not used for auto-consolidation
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of all response records, new offset dict with per-survey cursors)
        """
        return self._iterate_all_surveys(
            start_offset, table_options,
            self._read_single_survey_responses,
            cursor_field="recordedDate",
            data_type="responses",
            max_records=max_records,
        )

    def _create_response_export(self, survey_id: str, export_body: dict) -> str:
        """
        Create a response export job.

        Args:
            survey_id: Survey ID to export responses from
            export_body: Export configuration

        Returns:
            Progress ID to track the export job
        """
        url = f"{self.base_url}/surveys/{survey_id}/export-responses"

        try:
            response = self._make_request("POST", url, json_body=export_body)
            result = response.get("result", {})
            progress_id = result.get("progressId")

            if not progress_id:
                raise ValueError("Failed to create export: no progressId returned")

            return progress_id

        except Exception as e:
            error_msg = f"Failed to create response export for survey {survey_id}: {e}"
            logger.error(error_msg, exc_info=True)
            logger.info(
                "This survey might not have any responses yet, or the API "
                "token lacks response export permissions."
            )
            raise Exception(error_msg)

    def _poll_export_progress(
        self,
        survey_id: str,
        progress_id: str,
        max_attempts: int = QualtricsConfig.MAX_EXPORT_POLL_ATTEMPTS
    ) -> str:
        """
        Poll the export progress until completion.

        Args:
            survey_id: Survey ID
            progress_id: Progress ID from export creation
            max_attempts: Maximum number of polling attempts

        Returns:
            File ID when export is complete
        """
        url = f"{self.base_url}/surveys/{survey_id}/export-responses/{progress_id}"

        for attempt in range(max_attempts):
            try:
                response = self._make_request("GET", url)
                result = response.get("result", {})
                status = result.get("status")
                percent_complete = result.get("percentComplete", 0)

                if status == "complete":
                    file_id = result.get("fileId")
                    if not file_id:
                        raise ValueError("Export complete but no fileId returned")
                    return file_id
                elif status == "failed":
                    raise Exception("Export job failed")

                # Wait before next poll (adaptive based on progress)
                if percent_complete < 50:
                    time.sleep(QualtricsConfig.EXPORT_POLL_INTERVAL_SLOW)
                else:
                    time.sleep(QualtricsConfig.EXPORT_POLL_INTERVAL_FAST)

            except Exception as e:
                if attempt == max_attempts - 1:
                    raise Exception(f"Export polling failed: {e}")
                time.sleep(2)

        raise Exception("Export did not complete within timeout period")

    def _download_response_export(self, survey_id: str, file_id: str) -> list[dict]:
        """
        Download and parse the response export file.

        Args:
            survey_id: Survey ID
            file_id: File ID from completed export

        Returns:
            List of response records
        """
        url = f"{self.base_url}/surveys/{survey_id}/export-responses/{file_id}/file"

        try:
            # Download ZIP file with extended timeout for large exports
            # Use 5x the standard timeout since exports can be large files
            response = self._session.get(
                url, timeout=QualtricsConfig.REQUEST_TIMEOUT * 5
            )
            response.raise_for_status()

            # Extract JSON from ZIP
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_content:
                # Find the JSON file in the ZIP
                json_files = [f for f in zip_content.namelist() if f.endswith('.json')]

                if not json_files:
                    raise ValueError("No JSON file found in export ZIP")

                # Read the first JSON file
                json_content = zip_content.read(json_files[0])
                data = json.loads(json_content)

            # Extract responses array
            responses = data.get("responses", [])

            # Process responses to handle nested structures correctly
            processed_responses = []
            for resp in responses:
                processed_response = self._process_response_record(resp, survey_id)
                processed_responses.append(processed_response)

            return processed_responses

        except Exception as e:
            raise Exception(f"Failed to download response export: {e}")

    def _process_response_record(self, record: dict, survey_id: str) -> dict:
        """
        Process a response record to ensure proper structure.

        Args:
            record: Raw response record from API
            survey_id: Survey ID to add to the record (API doesn't return this)

        Returns:
            Processed response record
        """
        processed = self._extract_simple_fields(record)
        processed["surveyId"] = survey_id
        processed["values"] = self._process_question_values(record)

        # Process other map fields
        processed["labels"] = record.get("labels")
        processed["displayedFields"] = record.get("displayedFields")
        processed["displayedValues"] = record.get("displayedValues")
        processed["embeddedData"] = self._extract_embedded_data(record)

        # Normalize all keys to snake_case before returning
        return normalize_keys(processed)

    def _extract_simple_fields(self, record: dict) -> dict:
        """Extract simple metadata fields from response record."""
        processed = {}
        simple_fields = [
            "responseId", "recordedDate", "startDate", "endDate",
            "status", "ipAddress", "progress", "duration", "finished",
            "distributionChannel", "userLanguage", "locationLatitude", "locationLongitude"
        ]

        values_map = record.get("values", {})

        for field_name in simple_fields:
            # Try top-level first
            if field_name in record and record[field_name] is not None:
                processed[field_name] = record[field_name]
            # Fallback to values map (Qualtrics sometimes puts metadata here)
            elif field_name in values_map:
                value_obj = values_map[field_name]
                if isinstance(value_obj, dict):
                    processed[field_name] = value_obj.get("textEntry")
                else:
                    processed[field_name] = value_obj
            else:
                processed[field_name] = None

        # Handle responseId field
        if not processed.get("responseId") and "_recordId" in values_map:
            record_id_obj = values_map["_recordId"]
            if isinstance(record_id_obj, dict):
                processed["responseId"] = record_id_obj.get("textEntry")
            else:
                processed["responseId"] = record_id_obj

        return processed

    def _process_question_values(self, record: dict) -> dict:
        """Process question response values, filtering out metadata."""
        metadata_fields = {
            "responseId", "surveyId", "recordedDate", "startDate", "endDate",
            "status", "ipAddress", "progress", "duration", "finished",
            "distributionChannel", "userLanguage", "locationLatitude", "locationLongitude",
            "_recordId"
        }

        values = record.get("values", {})
        if not values:
            return None

        processed_values = {}
        for qid, value_data in values.items():
            # Skip metadata fields - only keep actual question responses
            if qid in metadata_fields:
                continue

            if isinstance(value_data, dict):
                # Keep structure, set None for missing fields
                processed_values[qid] = {
                    "choiceText": value_data.get("choiceText"),
                    "choiceId": value_data.get("choiceId"),
                    "textEntry": value_data.get("textEntry")
                }
            else:
                # If value is not a dict, create minimal structure
                processed_values[qid] = {
                    "choiceText": None,
                    "choiceId": None,
                    "textEntry": str(value_data) if value_data is not None else None
                }

        return processed_values if processed_values else None

    def _extract_embedded_data(self, record: dict) -> dict:
        """Extract embeddedData field from record or values map."""
        embedded_data = record.get("embeddedData")
        if embedded_data is None:
            values_map = record.get("values", {})
            if "embeddedData" in values_map:
                ed_obj = values_map["embeddedData"]
                if isinstance(ed_obj, dict):
                    embedded_data = ed_obj.get("textEntry")
        return embedded_data

    # =========================================================================
    # Table Readers: Distributions
    # =========================================================================

    def _read_distributions(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read distributions from Qualtrics API.

        Args:
            start_offset: Dictionary containing pagination token and cursor timestamp(s)
            table_options: Optional 'surveyId' parameter
                - Single survey: "SV_123" - Returns distributions for that survey
                - Multiple surveys: "SV_123, SV_456, SV_789" - Returns
                  distributions for specified surveys (comma-separated)
                - All surveys: omit surveyId - Returns distributions for
                  all surveys (auto-consolidation)
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of distribution records, new offset)
        """
        survey_id_input = table_options.get("surveyId") or table_options.get("surveyid")

        # Single survey (no comma) - use simple offset structure for backward compatibility
        if survey_id_input and "," not in survey_id_input:
            return self._read_single_survey_distributions(
                survey_id_input.strip(), start_offset, max_records
            )

        # Multiple surveys (comma-separated) or all surveys -
        # use consolidated path with per-survey offsets
        if survey_id_input:
            logger.info(
                "Multiple surveyIds provided, auto-consolidating "
                "distributions from specified surveys"
            )
        else:
            logger.info("No surveyId provided, auto-consolidating distributions from all surveys")
        return self._read_all_survey_distributions(start_offset, table_options, max_records)

    def _read_single_survey_distributions(
        self, survey_id: str, start_offset: dict,
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read distributions for a single survey from Qualtrics API.

        Args:
            survey_id: The survey ID to fetch distributions for
            start_offset: Dictionary containing pagination token and cursor timestamp
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of distribution records, new offset)
        """
        return self._fetch_paginated_list(
            "/distributions",
            start_offset,
            cursor_field="modifiedDate",
            extra_params={"surveyId": survey_id},
            max_records=max_records,
        )

    def _read_all_survey_distributions(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read distributions for all surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing per-survey cursor timestamps
            table_options: Not used for auto-consolidation
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of all distribution records, new offset dict with per-survey cursors)
        """
        return self._iterate_all_surveys(
            start_offset, table_options,
            self._read_single_survey_distributions,
            cursor_field="modifiedDate",
            data_type="distributions",
            max_records=max_records,
        )

    # =========================================================================
    # Table Readers: Mailing List Contacts
    # =========================================================================

    def _read_mailing_list_contacts(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read contacts from a specific mailing list in Qualtrics API.

        Note: Mailing list contacts table uses snapshot mode (full refresh) as the API
        does not return lastModifiedDate field for incremental sync.

        Args:
            start_offset: Dictionary containing pagination token (ignored for snapshot mode)
            table_options: Must contain 'directoryId' and 'mailingListId'
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of contact records, empty offset dict)
        """
        directory_id = table_options.get("directoryId") or table_options.get("directoryid")
        if not directory_id:
            raise ValueError(
                "directoryId is required in table_options for mailing_list_contacts table"
            )

        mailing_list_id = table_options.get("mailingListId") or table_options.get("mailinglistid")
        if not mailing_list_id:
            raise ValueError(
                "mailingListId is required in table_options for mailing_list_contacts table"
            )

        endpoint = f"/directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
        return self._fetch_paginated_list(
            endpoint, start_offset, cursor_field=None, max_records=max_records,
        )

    # =========================================================================
    # Table Readers: Directory Contacts
    # =========================================================================

    def _read_directory_contacts(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read all contacts from a directory in Qualtrics API.

        Note: Directory contacts table uses snapshot mode (full refresh) as the API
        does not return lastModifiedDate field for incremental sync. This endpoint
        returns all contacts across all mailing lists in the directory.

        Args:
            start_offset: Dictionary containing pagination token (ignored for snapshot mode)
            table_options: Must contain 'directoryId'
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of contact records, empty offset dict)
        """
        directory_id = table_options.get("directoryId") or table_options.get("directoryid")
        if not directory_id:
            raise ValueError(
                "directoryId is required in table_options for directory_contacts table"
            )

        endpoint = f"/directories/{directory_id}/contacts"
        return self._fetch_paginated_list(
            endpoint, start_offset, cursor_field=None, max_records=max_records,
        )

    # =========================================================================
    # Table Readers: Mailing Lists
    # =========================================================================

    def _read_mailing_lists(
        self, start_offset: dict, table_options: dict[str, str],
        max_records: int = None,
    ) -> (Iterator[dict], dict):
        """
        Read mailing lists from Qualtrics API.

        Uses snapshot mode (full refresh). Converts epoch timestamps to ISO 8601
        for consistency with other tables (surveys, etc.).

        Args:
            start_offset: Dictionary containing pagination token
            table_options: Must contain 'directoryId' parameter
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of mailing list records, offset dict)
        """
        directory_id = table_options.get("directoryId") or table_options.get("directoryid")
        if not directory_id:
            raise ValueError(
                "directoryId is required in table_options for mailing_lists table"
            )

        endpoint = f"/directories/{directory_id}/mailinglists"
        raw_iter, offset = self._fetch_paginated_list(
            endpoint, start_offset, cursor_field=None, max_records=max_records,
        )

        def convert_timestamps(record):
            """Convert epoch ms to ISO 8601 for consistency with surveys table."""
            for ts_field in ["creation_date", "last_modified_date"]:
                if ts_field in record and record[ts_field] is not None:
                    try:
                        record[ts_field] = datetime.utcfromtimestamp(
                            record[ts_field] / 1000
                        ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except (ValueError, TypeError, OSError):
                        record[ts_field] = None
            return record

        return (convert_timestamps(r) for r in raw_iter), offset

    # =========================================================================
    # Table Readers: Directories
    # =========================================================================

    def _read_directories(
        self, start_offset: dict, max_records: int = None
    ) -> (Iterator[dict], dict):
        """
        Read directories (XM Directory pools) from Qualtrics API.

        Uses snapshot mode (full refresh).

        Args:
            start_offset: Dictionary containing pagination token
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of directory records, offset dict)
        """
        return self._fetch_paginated_list(
            "/directories", start_offset, cursor_field=None,
            max_records=max_records,
        )

    # =========================================================================
    # Table Readers: Users
    # =========================================================================

    def _read_users(
        self, start_offset: dict, max_records: int = None
    ) -> (Iterator[dict], dict):
        """
        Read users from Qualtrics API.

        Uses snapshot mode (full refresh) - retrieves all users in the organization.

        Args:
            start_offset: Dictionary containing pagination token
            max_records: Maximum number of records to return

        Returns:
            Tuple of (iterator of user records, offset dict)
        """
        return self._fetch_paginated_list(
            "/users", start_offset, cursor_field=None,
            max_records=max_records,
        )
