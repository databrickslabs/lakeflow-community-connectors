import requests
import json
import time
import zipfile
import io
import logging
import re
from pyspark.sql.types import *
from datetime import datetime
from typing import Dict, List, Iterator, Any

# Configure logging
logger = logging.getLogger(__name__)


class QualtricsConfig:
    """Configuration constants for Qualtrics connector."""

    # Export polling configuration
    MAX_EXPORT_POLL_ATTEMPTS = 60  # Max polling attempts (2 min total at 2s/attempt)
    EXPORT_POLL_INTERVAL_FAST = 1  # seconds (when >50% complete)
    EXPORT_POLL_INTERVAL_SLOW = 2  # seconds (when <50% complete)

    # HTTP retry configuration (uses exponential backoff: 2^attempt)
    MAX_HTTP_RETRIES = 3  # Results in 1s, 2s, 4s waits = 7s total max

    # Rate limiting
    RATE_LIMIT_DEFAULT_WAIT = 60  # seconds (when Retry-After header missing)

    # Pagination
    DEFAULT_PAGE_SIZE = 100

    # Request timeout
    REQUEST_TIMEOUT = 30  # seconds per HTTP request

    # Auto-consolidation configuration (when surveyId not provided)
    MAX_SURVEYS_TO_CONSOLIDATE = 100  # Safety limit to prevent excessive API calls
    CONSOLIDATION_DELAY_BETWEEN_SURVEYS = 0.5  # seconds (to respect rate limits)


class LakeflowConnect:
    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize the Qualtrics source connector with authentication parameters.

        Args:
            options: Dictionary containing:
                - api_token: Qualtrics API token
                - datacenter_id: Datacenter identifier (e.g., 'fra1', 'ca1', 'yourdatacenterid')
        """
        self.api_token = options.get("api_token")
        self.datacenter_id = options.get("datacenter_id")

        if not self.api_token:
            raise ValueError("api_token is required")
        if not self.datacenter_id:
            raise ValueError("datacenter_id is required")

        self.base_url = f"https://{self.datacenter_id}.qualtrics.com/API/v3"
        self.headers = {
            "X-API-TOKEN": self.api_token,
            "Content-Type": "application/json"
        }

        # Supported tables
        self.tables = ["surveys", "survey_definitions", "survey_responses", "distributions", "contacts"]
    
    def list_tables(self) -> List[str]:
        """
        List all available tables supported by this connector.

        Returns:
            List of table names
        """
        return self.tables

    def _to_snake_case(self, name: str) -> str:
        """
        Convert camelCase or PascalCase to snake_case.

        Args:
            name: Field name in camelCase or PascalCase

        Returns:
            Field name in snake_case
        """
        # Insert underscore before uppercase letters that follow lowercase letters
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        # Insert underscore before uppercase letters that follow numbers or lowercase letters
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def _normalize_keys(self, data: dict) -> dict:
        """
        Recursively transform all keys in a dict to snake_case.

        Args:
            data: Dictionary with API field names (camelCase or PascalCase)

        Returns:
            Dictionary with snake_case field names
        """
        if not isinstance(data, dict):
            return data

        normalized = {}
        for key, value in data.items():
            snake_key = self._to_snake_case(key)

            if isinstance(value, dict):
                normalized[snake_key] = self._normalize_keys(value)
            elif isinstance(value, list):
                normalized[snake_key] = [
                    self._normalize_keys(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                normalized[snake_key] = value

        return normalized

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Get the schema for the specified table.
        
        Args:
            table_name: Name of the table
            table_options: Additional options (e.g., surveyId for survey_responses)
            
        Returns:
            StructType representing the table schema
        """
        if table_name not in self.tables:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.tables}"
            )
        
        if table_name == "surveys":
            return self._get_surveys_schema()
        elif table_name == "survey_definitions":
            return self._get_survey_definitions_schema()
        elif table_name == "survey_responses":
            return self._get_survey_responses_schema()
        elif table_name == "distributions":
            return self._get_distributions_schema()
        elif table_name == "contacts":
            return self._get_contacts_schema()
        else:
            raise ValueError(f"Unknown table: {table_name}")
    
    def _get_surveys_schema(self) -> StructType:
        """Get schema for surveys table.

        Note: Based on actual API response from GET /surveys endpoint.
        Fields like brandId, brandBaseURL, organizationId, expiration are NOT
        returned by the list surveys endpoint (they may be in GET /surveys/{id}).
        """
        return StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("creation_date", StringType(), True),
            StructField("last_modified", StringType(), True)
        ])

    def _get_survey_definitions_schema(self) -> StructType:
        """Get schema for survey_definitions table.

        Note: Based on GET /survey-definitions/{surveyId} endpoint.
        Returns full survey structure including questions, blocks, flow.
        
        The Qualtrics API returns variable structures for nested fields (e.g., Blocks
        can be a dict or array depending on the survey). To handle this variability,
        we use a simplified schema that captures key metadata fields and stores
        complex nested structures as flexible StringType (JSON strings).
        """
        return StructType([
            # Survey identification - these are consistently typed
            StructField("survey_id", StringType(), True),
            StructField("survey_name", StringType(), True),
            StructField("survey_status", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("creator_id", StringType(), True),
            StructField("brand_id", StringType(), True),
            StructField("brand_base_url", StringType(), True),
            StructField("last_modified", StringType(), True),
            StructField("last_accessed", StringType(), True),
            StructField("last_activated", StringType(), True),
            StructField("question_count", StringType(), True),
            # Complex nested structures - stored as StringType (JSON)
            # These fields have variable structure depending on survey configuration
            StructField("questions", StringType(), True),
            StructField("blocks", StringType(), True),
            StructField("survey_flow", StringType(), True),
            StructField("survey_options", StringType(), True),
            StructField("response_sets", StringType(), True),
            StructField("scoring", StringType(), True),
            StructField("project_info", StringType(), True)
        ])

    def _get_survey_responses_schema(self) -> StructType:
        """Get schema for survey_responses table."""
        return StructType([
            StructField("response_id", StringType(), True),
            StructField("survey_id", StringType(), True),
            StructField("recorded_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("status", LongType(), True),
            StructField("ip_address", StringType(), True),
            StructField("progress", LongType(), True),
            StructField("duration", LongType(), True),
            StructField("finished", BooleanType(), True),
            StructField("distribution_channel", StringType(), True),
            StructField("user_language", StringType(), True),
            StructField("location_latitude", StringType(), True),
            StructField("location_longitude", StringType(), True),
            StructField("values", MapType(
                StringType(),
                StructType([
                    StructField("choice_text", StringType(), True),
                    StructField("choice_id", StringType(), True),
                    StructField("text_entry", StringType(), True)
                ])
            ), True),
            StructField("labels", MapType(StringType(), StringType()), True),
            StructField("displayed_fields", ArrayType(StringType()), True),
            StructField("displayed_values", MapType(StringType(), StringType()), True),
            StructField("embedded_data", MapType(StringType(), StringType()), True)
        ])

    def _get_distributions_schema(self) -> StructType:
        """Get schema for distributions table.

        Note: Based on actual API response from GET /distributions endpoint.
        Schema includes nested structures for surveyLink, recipients, message, etc.
        """
        return StructType([
            StructField("id", StringType(), True),
            StructField("parent_distribution_id", StringType(), True),
            StructField("owner_id", StringType(), True),
            StructField("organization_id", StringType(), True),
            StructField("request_type", StringType(), True),
            StructField("request_status", StringType(), True),
            StructField("send_date", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("modified_date", StringType(), True),
            StructField("headers", StructType([
                StructField("from_email", StringType(), True),
                StructField("from_name", StringType(), True),
                StructField("reply_to_email", StringType(), True),
                StructField("subject", StringType(), True)
            ]), True),
            StructField("recipients", StructType([
                StructField("mailing_list_id", StringType(), True),
                StructField("contact_id", StringType(), True),
                StructField("library_id", StringType(), True),
                StructField("sample_id", StringType(), True)
            ]), True),
            StructField("message", StructType([
                StructField("library_id", StringType(), True),
                StructField("message_id", StringType(), True),
                StructField("message_type", StringType(), True)
            ]), True),
            StructField("survey_link", StructType([
                StructField("survey_id", StringType(), True),
                StructField("expiration_date", StringType(), True),
                StructField("link_type", StringType(), True)
            ]), True),
            StructField("stats", StructType([
                StructField("sent", LongType(), True),
                StructField("failed", LongType(), True),
                StructField("started", LongType(), True),
                StructField("bounced", LongType(), True),
                StructField("opened", LongType(), True),
                StructField("skipped", LongType(), True),
                StructField("finished", LongType(), True),
                StructField("complaints", LongType(), True),
                StructField("blocked", LongType(), True)
            ]), True)
        ])

    def _get_contacts_schema(self) -> StructType:
        """Get schema for contacts table.

        Note: Based on actual API response, the fields are:
        contactId, firstName, lastName, email, phone, extRef, language,
        unsubscribed, mailingListUnsubscribed, contactLookupId
        """
        return StructType([
            StructField("contact_id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("ext_ref", StringType(), True),
            StructField("language", StringType(), True),
            StructField("unsubscribed", BooleanType(), True),
            StructField("mailing_list_unsubscribed", BooleanType(), True),
            StructField("contact_lookup_id", StringType(), True)
        ])
    
    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Dict:
        """
        Get metadata for the specified table.
        
        Args:
            table_name: Name of the table
            table_options: Additional options
            
        Returns:
            Dictionary containing primary_keys, cursor_field, and ingestion_type
        """
        if table_name not in self.tables:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.tables}"
            )
        
        if table_name == "surveys":
            return {
                "primary_keys": ["id"],
                "cursor_field": "last_modified",
                "ingestion_type": "cdc"
            }
        elif table_name == "survey_definitions":
            return {
                "primary_keys": ["survey_id"],
                "cursor_field": "last_modified",
                "ingestion_type": "cdc"
            }
        elif table_name == "survey_responses":
            return {
                "primary_keys": ["response_id"],
                "cursor_field": "recorded_date",
                "ingestion_type": "append"
            }
        elif table_name == "distributions":
            return {
                "primary_keys": ["id"],
                "cursor_field": "modified_date",
                "ingestion_type": "cdc"
            }
        elif table_name == "contacts":
            return {
                "primary_keys": ["contact_id"],
                "cursor_field": None,
                "ingestion_type": "snapshot"
            }
        else:
            raise ValueError(f"Unknown table: {table_name}")
    
    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
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
        if table_name not in self.tables:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {self.tables}"
            )

        if table_name == "surveys":
            return self._read_surveys(start_offset)
        elif table_name == "survey_definitions":
            return self._read_survey_definitions(start_offset, table_options)
        elif table_name == "survey_responses":
            return self._read_survey_responses(start_offset, table_options)
        elif table_name == "distributions":
            return self._read_distributions(start_offset, table_options)
        elif table_name == "contacts":
            return self._read_contacts(start_offset, table_options)
        else:
            raise ValueError(f"Unknown table: {table_name}")

    def _get_all_survey_ids(self, table_options: Dict[str, str]) -> List[str]:
        """
        Get all survey IDs for auto-consolidation.

        Args:
            table_options: May contain filters like:
                - only_active_surveys: "true" to filter only active surveys (default: true)
                - max_surveys: Maximum number of surveys to process (default: 100)

        Returns:
            List of survey IDs
        """
        # Check if we should only include active surveys (default: true)
        only_active = table_options.get("only_active_surveys", "true").lower() == "true"

        # Get max surveys limit
        max_surveys_str = table_options.get("max_surveys", str(QualtricsConfig.MAX_SURVEYS_TO_CONSOLIDATE))
        try:
            max_surveys = int(max_surveys_str)
        except ValueError:
            logger.warning(f"Invalid max_surveys value '{max_surveys_str}', using default {QualtricsConfig.MAX_SURVEYS_TO_CONSOLIDATE}")
            max_surveys = QualtricsConfig.MAX_SURVEYS_TO_CONSOLIDATE

        # Fetch all surveys
        surveys_iter, _ = self._read_surveys({})
        surveys = list(surveys_iter)

        if not surveys:
            logger.warning("No surveys found")
            return []

        # Filter surveys
        survey_ids = []
        for survey in surveys:
            # Apply active filter
            if only_active and not survey.get("is_active", False):
                continue

            survey_id = survey.get("id")
            if survey_id:
                survey_ids.append(survey_id)

            # Check max limit
            if len(survey_ids) >= max_surveys:
                logger.warning(f"Reached max_surveys limit of {max_surveys}. Consider increasing this limit in table_options if needed.")
                break

        logger.info(f"Found {len(survey_ids)} survey(s) to process (only_active={only_active}, max_surveys={max_surveys})")
        return survey_ids
    
    def _read_surveys(self, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read surveys from Qualtrics API.
        
        Args:
            start_offset: Dictionary containing pagination token and cursor timestamp
            
        Returns:
            Tuple of (iterator of survey records, new offset)
        """
        all_surveys = []
        skip_token = start_offset.get("skipToken") if start_offset else None
        last_modified_cursor = start_offset.get("lastModified") if start_offset else None
        
        # Fetch all pages
        while True:
            url = f"{self.base_url}/surveys"
            params = {"pageSize": QualtricsConfig.DEFAULT_PAGE_SIZE}
            
            if skip_token:
                params["skipToken"] = skip_token
            
            try:
                response = self._make_request("GET", url, params=params)
                
                result = response.get("result", {})
                elements = result.get("elements", [])
                
                if not elements:
                    break
                
                # Filter by lastModified if doing incremental sync
                if last_modified_cursor:
                    filtered_elements = []
                    for survey in elements:
                        survey_last_modified = survey.get("lastModified", "")
                        if survey_last_modified and survey_last_modified > last_modified_cursor:
                            filtered_elements.append(survey)
                    all_surveys.extend(filtered_elements)
                else:
                    all_surveys.extend(elements)
                
                # Check for next page
                next_page = result.get("nextPage")
                if next_page:
                    # Extract skipToken from nextPage URL
                    if "skipToken=" in next_page:
                        skip_token = next_page.split("skipToken=")[-1].split("&")[0]
                    else:
                        break
                else:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching surveys: {e}", exc_info=True)
                break
        
        # Calculate new offset
        new_offset = {}
        if all_surveys:
            # Find max lastModified from surveys that have it
            last_modified_dates = [
                survey.get("lastModified", "")
                for survey in all_surveys
                if survey.get("lastModified")
            ]
            if last_modified_dates:
                max_last_modified = max(last_modified_dates)
                new_offset["lastModified"] = max_last_modified
            elif last_modified_cursor:
                new_offset["lastModified"] = last_modified_cursor
        elif last_modified_cursor:
            # No new data, keep the same cursor
            new_offset["lastModified"] = last_modified_cursor

        # Normalize all keys to snake_case before returning
        normalized_surveys = (self._normalize_keys(survey) for survey in all_surveys)
        return normalized_surveys, new_offset

    def _read_survey_definitions(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read survey definition from Qualtrics API.

        Args:
            start_offset: Dictionary containing cursor timestamp
                - For single survey: {"lastModified": "2024-01-01T00:00:00Z"}
                - For auto-consolidation: {"surveys": {"SV_123": {"lastModified": "..."}, ...}, "lastModified": "..."}
            table_options: Optional 'surveyId' parameter
                - If provided: Returns definition for that specific survey
                - If not provided: Returns definitions for all surveys (auto-consolidation)
                Additional options for auto-consolidation:
                - only_active_surveys: "true" to filter only active surveys (default: true)
                - max_surveys: Maximum number of surveys to process (default: 100)

        Returns:
            Tuple of (iterator of survey definition records, offset dict)
        """
        survey_id = table_options.get("surveyId")

        # If surveyId is provided, fetch that specific survey (backward compatibility)
        if survey_id:
            return self._read_single_survey_definition(survey_id, start_offset)

        # Otherwise, fetch all surveys and consolidate their definitions
        logger.info("No surveyId provided, auto-consolidating definitions from all surveys")
        return self._read_all_survey_definitions(start_offset, table_options)

    def _read_single_survey_definition(self, survey_id: str, start_offset: dict) -> (Iterator[dict], dict):
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
                    logger.info(f"Survey {survey_id} not modified since {last_modified_cursor}, skipping")
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
            for field in simple_fields:
                processed[field] = result.get(field)

            # Serialize complex nested fields as JSON strings
            complex_fields = [
                "Questions", "Blocks", "SurveyFlow", "SurveyOptions",
                "ResponseSets", "Scoring", "ProjectInfo"
            ]
            for field in complex_fields:
                value = result.get(field)
                if value is not None:
                    processed[field] = json.dumps(value)
                else:
                    processed[field] = None

            # Normalize all keys to snake_case before returning
            normalized = self._normalize_keys(processed)

            # Calculate new offset based on this definition's last_modified
            new_offset = {}
            if survey_last_modified:
                new_offset["lastModified"] = survey_last_modified
            elif last_modified_cursor:
                new_offset["lastModified"] = last_modified_cursor

            return iter([normalized]), new_offset

        except Exception as e:
            logger.error(f"Error fetching survey definition for {survey_id}: {e}", exc_info=True)
            raise

    def _read_all_survey_definitions(self, start_offset: dict, table_options: Dict[str, str]) -> (Iterator[dict], dict):
        """
        Read survey definitions for all surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing per-survey cursor timestamps
                Format: {"surveys": {"SV_123": {"lastModified": "2024-01-01"}, ...}, "lastModified": "..."}
            table_options: Optional filters (only_active_surveys, max_surveys)

        Returns:
            Tuple of (iterator of all survey definition records, new offset dict with per-survey cursors)
        """
        # Get all survey IDs
        survey_ids = self._get_all_survey_ids(table_options)

        if not survey_ids:
            logger.warning("No surveys found to fetch definitions for")
            return iter([]), {}

        logger.info(f"Fetching survey definitions for {len(survey_ids)} survey(s)")

        # Track per-survey offsets
        per_survey_offsets = start_offset.get("surveys", {}) if start_offset else {}

        # Fetch definition for each survey
        all_definitions = []
        new_per_survey_offsets = {}
        success_count = 0
        failure_count = 0
        skipped_count = 0

        for idx, survey_id in enumerate(survey_ids, 1):
            try:
                logger.info(f"Fetching definition {idx}/{len(survey_ids)} for survey {survey_id}")

                # Get this survey's specific offset
                survey_offset = per_survey_offsets.get(survey_id, {})

                # Fetch definition for this survey
                definition_iter, new_survey_offset = self._read_single_survey_definition(survey_id, survey_offset)
                definitions = list(definition_iter)

                if definitions:
                    all_definitions.extend(definitions)
                    success_count += 1
                else:
                    # No definitions returned (either not found or not modified)
                    skipped_count += 1

                # Save the new offset for this survey
                new_per_survey_offsets[survey_id] = new_survey_offset

                # Add delay between requests to respect rate limits
                if idx < len(survey_ids):
                    time.sleep(QualtricsConfig.CONSOLIDATION_DELAY_BETWEEN_SURVEYS)

            except Exception as e:
                logger.warning(f"Failed to fetch definition for survey {survey_id}: {e}")
                failure_count += 1
                # Keep the old offset if we failed
                if survey_id in per_survey_offsets:
                    new_per_survey_offsets[survey_id] = per_survey_offsets[survey_id]
                # Continue with other surveys even if one fails
                continue

        logger.info(f"Completed fetching survey definitions: {success_count} succeeded, {skipped_count} skipped (not modified), {failure_count} failed, {len(all_definitions)} total records")

        # Build consolidated offset
        new_offset = {"surveys": new_per_survey_offsets}

        # Also include a global lastModified for convenience (max across all surveys)
        if all_definitions:
            last_modified_dates = [
                defn.get("last_modified", "")
                for defn in all_definitions
                if defn.get("last_modified")
            ]
            if last_modified_dates:
                new_offset["lastModified"] = max(last_modified_dates)

        return iter(all_definitions), new_offset

    def _read_survey_responses(
        self, start_offset: dict, table_options: Dict[str, str]
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
                - If provided: Returns responses for that specific survey
                - If not provided: Returns responses for all surveys (auto-consolidation)
                Additional options for auto-consolidation:
                - only_active_surveys: "true" to filter only active surveys (default: true)
                - max_surveys: Maximum number of surveys to process (default: 100)

        Returns:
            Tuple of (iterator of response records, new offset)
        """
        survey_id = table_options.get("surveyId")

        # If surveyId is provided, fetch that specific survey (backward compatibility)
        if survey_id:
            return self._read_single_survey_responses(survey_id, start_offset)

        # Otherwise, fetch all surveys and consolidate their responses
        logger.info("No surveyId provided, auto-consolidating responses from all surveys")
        return self._read_all_survey_responses(start_offset, table_options)

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

        # Calculate new offset
        new_offset = {}
        if responses:
            # Find max recordedDate from responses that have it
            recorded_dates = [
                resp.get("recordedDate", "")
                for resp in responses
                if resp.get("recordedDate")
            ]
            if recorded_dates:
                max_recorded_date = max(recorded_dates)
                new_offset["recordedDate"] = max_recorded_date
            elif recorded_date_cursor:
                new_offset["recordedDate"] = recorded_date_cursor
        elif recorded_date_cursor:
            # No new data, keep the same cursor
            new_offset["recordedDate"] = recorded_date_cursor

        return iter(responses), new_offset

    def _read_all_survey_responses(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read survey responses for all surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing per-survey cursor timestamps
                Format: {"surveys": {"SV_123": {"recordedDate": "2024-01-01"}, ...}, "recordedDate": "..."}
            table_options: Optional filters (only_active_surveys, max_surveys)

        Returns:
            Tuple of (iterator of all response records, new offset dict with per-survey cursors)
        """
        # Get all survey IDs
        survey_ids = self._get_all_survey_ids(table_options)

        if not survey_ids:
            logger.warning("No surveys found to fetch responses for")
            return iter([]), {}

        logger.info(f"Fetching survey responses for {len(survey_ids)} survey(s)")

        # Track per-survey offsets
        per_survey_offsets = start_offset.get("surveys", {}) if start_offset else {}

        # Fetch responses for each survey
        all_responses = []
        new_per_survey_offsets = {}
        success_count = 0
        failure_count = 0

        for idx, survey_id in enumerate(survey_ids, 1):
            try:
                logger.info(f"Fetching responses {idx}/{len(survey_ids)} for survey {survey_id}")

                # Get this survey's specific offset
                survey_offset = per_survey_offsets.get(survey_id, {})

                # Fetch responses for this survey
                responses_iter, new_survey_offset = self._read_single_survey_responses(survey_id, survey_offset)
                responses = list(responses_iter)
                all_responses.extend(responses)
                success_count += 1

                # Save the new offset for this survey
                new_per_survey_offsets[survey_id] = new_survey_offset

                # Add delay between surveys to respect rate limits
                if idx < len(survey_ids):
                    time.sleep(QualtricsConfig.CONSOLIDATION_DELAY_BETWEEN_SURVEYS)

            except Exception as e:
                logger.warning(f"Failed to fetch responses for survey {survey_id}: {e}")
                failure_count += 1
                # Keep the old offset if we failed
                if survey_id in per_survey_offsets:
                    new_per_survey_offsets[survey_id] = per_survey_offsets[survey_id]
                # Continue with other surveys even if one fails
                continue

        logger.info(f"Completed fetching survey responses: {success_count} succeeded, {failure_count} failed, {len(all_responses)} total records")

        # Build consolidated offset
        new_offset = {"surveys": new_per_survey_offsets}

        # Also include a global recordedDate for convenience (max across all surveys)
        if all_responses:
            recorded_dates = [
                resp.get("recordedDate", "")
                for resp in all_responses
                if resp.get("recordedDate")
            ]
            if recorded_dates:
                new_offset["recordedDate"] = max(recorded_dates)

        return iter(all_responses), new_offset
    
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
            logger.info("This survey might not have any responses yet, or the API token lacks response export permissions.")
            raise Exception(error_msg)
    
    def _poll_export_progress(
        self, survey_id: str, progress_id: str, max_attempts: int = QualtricsConfig.MAX_EXPORT_POLL_ATTEMPTS
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
    
    def _download_response_export(self, survey_id: str, file_id: str) -> List[dict]:
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
            # Download ZIP file
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            # Extract JSON from ZIP
            zip_content = zipfile.ZipFile(io.BytesIO(response.content))

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
            for response in responses:
                processed_response = self._process_response_record(response, survey_id)
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
        processed = {}

        # Copy simple fields - try both top-level and from values map
        simple_fields = [
            "responseId", "recordedDate", "startDate", "endDate",
            "status", "ipAddress", "progress", "duration", "finished",
            "distributionChannel", "userLanguage", "locationLatitude", "locationLongitude"
        ]

        values_map = record.get("values", {})

        for field in simple_fields:
            # Try top-level first
            if field in record and record[field] is not None:
                processed[field] = record[field]
            # Fallback to values map (Qualtrics sometimes puts metadata here)
            elif field in values_map:
                value_obj = values_map[field]
                if isinstance(value_obj, dict):
                    # Extract from textEntry if it's a dict
                    processed[field] = value_obj.get("textEntry")
                else:
                    processed[field] = value_obj
            else:
                processed[field] = None

        # Add surveyId - API doesn't return this, but we know it from the query
        processed["surveyId"] = survey_id
        
        # Special handling for responseId which might be _recordId in values
        if not processed.get("responseId") and "_recordId" in values_map:
            record_id_obj = values_map["_recordId"]
            if isinstance(record_id_obj, dict):
                processed["responseId"] = record_id_obj.get("textEntry")
            else:
                processed["responseId"] = record_id_obj
        
        # Process values (question responses)
        # Filter out metadata fields that we've already extracted
        metadata_fields = {
            "responseId", "surveyId", "recordedDate", "startDate", "endDate",
            "status", "ipAddress", "progress", "duration", "finished",
            "distributionChannel", "userLanguage", "locationLatitude", "locationLongitude",
            "_recordId"  # Internal field
        }
        
        values = record.get("values", {})
        if values:
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
            processed["values"] = processed_values if processed_values else None
        else:
            processed["values"] = None
        
        # Process other map fields
        processed["labels"] = record.get("labels")
        processed["displayedFields"] = record.get("displayedFields")
        processed["displayedValues"] = record.get("displayedValues")
        
        # Process embeddedData - might be top-level or nested
        embedded_data = record.get("embeddedData")
        if embedded_data is None and "embeddedData" in values_map:
            # Try to get from values map
            ed_obj = values_map["embeddedData"]
            if isinstance(ed_obj, dict):
                embedded_data = ed_obj.get("textEntry")
        processed["embeddedData"] = embedded_data

        # Normalize all keys to snake_case before returning
        return self._normalize_keys(processed)
    
    def _read_distributions(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read distributions from Qualtrics API.

        Args:
            start_offset: Dictionary containing pagination token and cursor timestamp(s)
            table_options: Optional 'surveyId' parameter
                - If provided: Returns distributions for that specific survey
                - If not provided: Returns distributions for all surveys (auto-consolidation)
                Additional options for auto-consolidation:
                - only_active_surveys: "true" to filter only active surveys (default: true)
                - max_surveys: Maximum number of surveys to process (default: 100)

        Returns:
            Tuple of (iterator of distribution records, new offset)
        """
        survey_id = table_options.get("surveyId")

        # If surveyId is provided, fetch that specific survey (backward compatibility)
        if survey_id:
            return self._read_single_survey_distributions(survey_id, start_offset)

        # Otherwise, fetch all surveys and consolidate their distributions
        logger.info("No surveyId provided, auto-consolidating distributions from all surveys")
        return self._read_all_survey_distributions(start_offset, table_options)

    def _read_single_survey_distributions(
        self, survey_id: str, start_offset: dict
    ) -> (Iterator[dict], dict):
        """
        Read distributions for a single survey from Qualtrics API.

        Args:
            survey_id: The survey ID to fetch distributions for
            start_offset: Dictionary containing pagination token and cursor timestamp

        Returns:
            Tuple of (iterator of distribution records, new offset)
        """
        all_distributions = []
        skip_token = start_offset.get("skipToken") if start_offset else None
        modified_date_cursor = start_offset.get("modifiedDate") if start_offset else None

        # Fetch all pages
        while True:
            url = f"{self.base_url}/distributions"
            params = {
                "surveyId": survey_id,
                "pageSize": QualtricsConfig.DEFAULT_PAGE_SIZE
            }

            if skip_token:
                params["skipToken"] = skip_token

            try:
                response = self._make_request("GET", url, params=params)

                result = response.get("result", {})
                elements = result.get("elements", [])

                if not elements:
                    break

                # Filter by modifiedDate if doing incremental sync
                if modified_date_cursor:
                    filtered_elements = []
                    for dist in elements:
                        dist_modified_date = dist.get("modifiedDate", "")
                        if dist_modified_date and dist_modified_date > modified_date_cursor:
                            filtered_elements.append(dist)
                    all_distributions.extend(filtered_elements)
                else:
                    all_distributions.extend(elements)

                # Check for next page
                next_page = result.get("nextPage")
                if next_page:
                    # Extract skipToken from nextPage URL
                    if "skipToken=" in next_page:
                        skip_token = next_page.split("skipToken=")[-1].split("&")[0]
                    else:
                        break
                else:
                    break

            except Exception as e:
                logger.error(f"Error fetching distributions: {e}", exc_info=True)
                break

        # Calculate new offset
        new_offset = {}
        if all_distributions:
            # Find max modifiedDate from distributions that have it
            modified_dates = [
                dist.get("modifiedDate", "")
                for dist in all_distributions
                if dist.get("modifiedDate")
            ]
            if modified_dates:
                max_modified_date = max(modified_dates)
                new_offset["modifiedDate"] = max_modified_date
            elif modified_date_cursor:
                new_offset["modifiedDate"] = modified_date_cursor
        elif modified_date_cursor:
            # No new data, keep the same cursor
            new_offset["modifiedDate"] = modified_date_cursor

        # Normalize all keys to snake_case before returning
        normalized_distributions = (self._normalize_keys(dist) for dist in all_distributions)
        return normalized_distributions, new_offset

    def _read_all_survey_distributions(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read distributions for all surveys from Qualtrics API.

        Args:
            start_offset: Dictionary containing per-survey cursor timestamps
                Format: {"surveys": {"SV_123": {"modifiedDate": "2024-01-01"}, ...}, "modifiedDate": "..."}
            table_options: Optional filters (only_active_surveys, max_surveys)

        Returns:
            Tuple of (iterator of all distribution records, new offset dict with per-survey cursors)
        """
        # Get all survey IDs
        survey_ids = self._get_all_survey_ids(table_options)

        if not survey_ids:
            logger.warning("No surveys found to fetch distributions for")
            return iter([]), {}

        logger.info(f"Fetching distributions for {len(survey_ids)} survey(s)")

        # Track per-survey offsets
        per_survey_offsets = start_offset.get("surveys", {}) if start_offset else {}

        # Fetch distributions for each survey
        all_distributions = []
        new_per_survey_offsets = {}
        success_count = 0
        failure_count = 0

        for idx, survey_id in enumerate(survey_ids, 1):
            try:
                logger.info(f"Fetching distributions {idx}/{len(survey_ids)} for survey {survey_id}")

                # Get this survey's specific offset
                survey_offset = per_survey_offsets.get(survey_id, {})

                # Fetch distributions for this survey
                distributions_iter, new_survey_offset = self._read_single_survey_distributions(survey_id, survey_offset)
                distributions = list(distributions_iter)
                all_distributions.extend(distributions)
                success_count += 1

                # Save the new offset for this survey
                new_per_survey_offsets[survey_id] = new_survey_offset

                # Add delay between surveys to respect rate limits
                if idx < len(survey_ids):
                    time.sleep(QualtricsConfig.CONSOLIDATION_DELAY_BETWEEN_SURVEYS)

            except Exception as e:
                logger.warning(f"Failed to fetch distributions for survey {survey_id}: {e}")
                failure_count += 1
                # Keep the old offset if we failed
                if survey_id in per_survey_offsets:
                    new_per_survey_offsets[survey_id] = per_survey_offsets[survey_id]
                # Continue with other surveys even if one fails
                continue

        logger.info(f"Completed fetching distributions: {success_count} succeeded, {failure_count} failed, {len(all_distributions)} total records")

        # Build consolidated offset
        new_offset = {"surveys": new_per_survey_offsets}

        # Also include a global modifiedDate for convenience (max across all surveys)
        if all_distributions:
            modified_dates = [
                dist.get("modifiedDate", "")
                for dist in all_distributions
                if dist.get("modifiedDate")
            ]
            if modified_dates:
                new_offset["modifiedDate"] = max(modified_dates)

        return iter(all_distributions), new_offset

    def _read_contacts(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read contacts from Qualtrics API.

        Note: Contacts table uses snapshot mode (full refresh) as the API
        does not return lastModifiedDate field for incremental sync.

        Args:
            start_offset: Dictionary containing pagination token (ignored for snapshot mode)
            table_options: Must contain 'directoryId' and 'mailingListId'

        Returns:
            Tuple of (iterator of contact records, empty offset dict)
        """
        directory_id = table_options.get("directoryId")
        if not directory_id:
            raise ValueError(
                "directoryId is required in table_options for contacts table"
            )

        mailing_list_id = table_options.get("mailingListId")
        if not mailing_list_id:
            raise ValueError(
                "mailingListId is required in table_options for contacts table"
            )

        all_contacts = []
        skip_token = None

        # Fetch all pages
        while True:
            url = f"{self.base_url}/directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
            params = {"pageSize": QualtricsConfig.DEFAULT_PAGE_SIZE}

            if skip_token:
                params["skipToken"] = skip_token

            try:
                response = self._make_request("GET", url, params=params)

                result = response.get("result", {})
                elements = result.get("elements", [])

                if not elements:
                    break

                # Add all contacts (no filtering for snapshot mode)
                all_contacts.extend(elements)

                # Check for next page
                next_page = result.get("nextPage")
                if next_page:
                    # Extract skipToken from nextPage URL
                    if "skipToken=" in next_page:
                        skip_token = next_page.split("skipToken=")[-1].split("&")[0]
                    else:
                        break
                else:
                    break

            except Exception as e:
                logger.error(f"Error fetching contacts: {e}", exc_info=True)
                break

        # Return empty offset for snapshot mode
        new_offset = {}

        # Normalize all keys to snake_case before returning
        normalized_contacts = (self._normalize_keys(contact) for contact in all_contacts)
        return normalized_contacts, new_offset

    def _make_request(
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
                    response = requests.get(url, headers=self.headers, params=params)
                elif method == "POST":
                    response = requests.post(url, headers=self.headers, json=json_body)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", QualtricsConfig.RATE_LIMIT_DEFAULT_WAIT))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                # Capture response body for debugging before raising
                if not response.ok:
                    try:
                        error_detail = response.json()
                        logger.error(f"API Error Response: {error_detail}")
                    except:
                        logger.error(f"API Error Response (raw): {response.text}")
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Request failed after {max_retries} attempts: {e}")
                
                # Exponential backoff
                wait_time = 2 ** attempt
                logger.warning(f"Request failed, retrying in {wait_time} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
        
        raise Exception("Request failed")

