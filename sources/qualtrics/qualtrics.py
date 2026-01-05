import requests
import json
import time
import zipfile
import io
import logging
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
            StructField("ownerId", StringType(), True),
            StructField("isActive", BooleanType(), True),
            StructField("creationDate", StringType(), True),
            StructField("lastModified", StringType(), True)
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
            StructField("SurveyID", StringType(), True),
            StructField("SurveyName", StringType(), True),
            StructField("SurveyDescription", StringType(), True),
            StructField("SurveyOwnerID", StringType(), True),
            StructField("SurveyBrandID", StringType(), True),
            StructField("DivisionID", StringType(), True),
            StructField("SurveyLanguage", StringType(), True),
            StructField("SurveyActiveResponseSet", StringType(), True),
            StructField("SurveyStatus", StringType(), True),
            StructField("SurveyStartDate", StringType(), True),
            StructField("SurveyExpirationDate", StringType(), True),
            StructField("SurveyCreationDate", StringType(), True),
            StructField("CreatorID", StringType(), True),
            StructField("LastModified", StringType(), True),
            StructField("LastAccessed", StringType(), True),
            StructField("LastActivated", StringType(), True),
            StructField("Deleted", StringType(), True),
            StructField("ProjectCategory", StringType(), True),
            StructField("ProjectType", StringType(), True),
            # Complex nested structures - stored as StringType (JSON)
            # These fields have variable structure depending on survey configuration
            StructField("Questions", StringType(), True),
            StructField("Blocks", StringType(), True),
            StructField("Flow", StringType(), True),
            StructField("EmbeddedData", StringType(), True),
            StructField("SurveyOptions", StringType(), True),
            StructField("ResponseSets", StringType(), True),
            StructField("LoopAndMerge", StringType(), True),
            StructField("Scoring", StringType(), True)
        ])

    def _get_survey_responses_schema(self) -> StructType:
        """Get schema for survey_responses table."""
        return StructType([
            StructField("responseId", StringType(), True),
            StructField("surveyId", StringType(), True),
            StructField("recordedDate", StringType(), True),
            StructField("startDate", StringType(), True),
            StructField("endDate", StringType(), True),
            StructField("status", LongType(), True),
            StructField("ipAddress", StringType(), True),
            StructField("progress", LongType(), True),
            StructField("duration", LongType(), True),
            StructField("finished", BooleanType(), True),
            StructField("distributionChannel", StringType(), True),
            StructField("userLanguage", StringType(), True),
            StructField("locationLatitude", StringType(), True),
            StructField("locationLongitude", StringType(), True),
            StructField("values", MapType(
                StringType(),
                StructType([
                    StructField("choiceText", StringType(), True),
                    StructField("choiceId", StringType(), True),
                    StructField("textEntry", StringType(), True)
                ])
            ), True),
            StructField("labels", MapType(StringType(), StringType()), True),
            StructField("displayedFields", ArrayType(StringType()), True),
            StructField("displayedValues", MapType(StringType(), StringType()), True),
            StructField("embeddedData", MapType(StringType(), StringType()), True)
        ])

    def _get_distributions_schema(self) -> StructType:
        """Get schema for distributions table.

        Note: Based on actual API response from GET /distributions endpoint.
        Schema includes nested structures for surveyLink, recipients, message, etc.
        """
        return StructType([
            StructField("id", StringType(), True),
            StructField("parentDistributionId", StringType(), True),
            StructField("ownerId", StringType(), True),
            StructField("organizationId", StringType(), True),
            StructField("requestType", StringType(), True),
            StructField("requestStatus", StringType(), True),
            StructField("sendDate", StringType(), True),
            StructField("createdDate", StringType(), True),
            StructField("modifiedDate", StringType(), True),
            StructField("headers", StructType([
                StructField("fromEmail", StringType(), True),
                StructField("fromName", StringType(), True),
                StructField("replyToEmail", StringType(), True)
            ]), True),
            StructField("recipients", StructType([
                StructField("mailingListId", StringType(), True),
                StructField("contactId", StringType(), True),
                StructField("libraryId", StringType(), True),
                StructField("sampleId", StringType(), True)
            ]), True),
            StructField("message", StructType([
                StructField("libraryId", StringType(), True),
                StructField("messageId", StringType(), True),
                StructField("messageType", StringType(), True)
            ]), True),
            StructField("surveyLink", StructType([
                StructField("surveyId", StringType(), True),
                StructField("expirationDate", StringType(), True),
                StructField("linkType", StringType(), True)
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
            StructField("contactId", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("extRef", StringType(), True),
            StructField("language", StringType(), True),
            StructField("unsubscribed", BooleanType(), True),
            StructField("mailingListUnsubscribed", BooleanType(), True),
            StructField("contactLookupId", StringType(), True)
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
                "cursor_field": "lastModified",
                "ingestion_type": "cdc"
            }
        elif table_name == "survey_definitions":
            return {
                "primary_keys": ["SurveyID"],
                "cursor_field": None,
                "ingestion_type": "snapshot"
            }
        elif table_name == "survey_responses":
            return {
                "primary_keys": ["responseId"],
                "cursor_field": "recordedDate",
                "ingestion_type": "append"
            }
        elif table_name == "distributions":
            return {
                "primary_keys": ["id"],
                "cursor_field": "modifiedDate",
                "ingestion_type": "cdc"
            }
        elif table_name == "contacts":
            return {
                "primary_keys": ["contactId"],
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
        
        return iter(all_surveys), new_offset

    def _read_survey_definitions(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read survey definition from Qualtrics API.

        Args:
            start_offset: Dictionary (ignored for snapshot mode)
            table_options: Must contain 'surveyId'

        Returns:
            Tuple of (iterator of survey definition records, empty offset dict)
        """
        survey_id = table_options.get("surveyId")
        if not survey_id:
            raise ValueError(
                "surveyId is required in table_options for survey_definitions table"
            )

        url = f"{self.base_url}/survey-definitions/{survey_id}"

        try:
            response = self._make_request("GET", url)
            result = response.get("result", {})

            if not result:
                logger.warning(f"No survey definition found for survey {survey_id}")
                return iter([]), {}

            # Process the result to serialize complex nested fields as JSON strings
            # This is needed because the API returns variable structures (dict or array)
            # for fields like Blocks, which can't be handled by a fixed schema
            processed = {}
            
            # Copy simple string fields as-is
            simple_fields = [
                "SurveyID", "SurveyName", "SurveyDescription", "SurveyOwnerID",
                "SurveyBrandID", "DivisionID", "SurveyLanguage", "SurveyActiveResponseSet",
                "SurveyStatus", "SurveyStartDate", "SurveyExpirationDate",
                "SurveyCreationDate", "CreatorID", "LastModified", "LastAccessed",
                "LastActivated", "Deleted", "ProjectCategory", "ProjectType"
            ]
            for field in simple_fields:
                processed[field] = result.get(field)
            
            # Serialize complex nested fields as JSON strings
            complex_fields = [
                "Questions", "Blocks", "Flow", "EmbeddedData",
                "SurveyOptions", "ResponseSets", "LoopAndMerge", "Scoring"
            ]
            for field in complex_fields:
                value = result.get(field)
                if value is not None:
                    processed[field] = json.dumps(value)
                else:
                    processed[field] = None

            return iter([processed]), {}

        except Exception as e:
            logger.error(f"Error fetching survey definition for {survey_id}: {e}", exc_info=True)
            raise

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
            start_offset: Dictionary containing cursor timestamp
            table_options: Must contain 'surveyId'
            
        Returns:
            Tuple of (iterator of response records, new offset)
        """
        survey_id = table_options.get("surveyId")
        if not survey_id:
            raise ValueError(
                "surveyId is required in table_options for survey_responses table"
            )
        
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
        
        return processed
    
    def _read_distributions(
        self, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read distributions from Qualtrics API.

        Args:
            start_offset: Dictionary containing pagination token and cursor timestamp
            table_options: Must contain 'surveyId'

        Returns:
            Tuple of (iterator of distribution records, new offset)
        """
        survey_id = table_options.get("surveyId")
        if not survey_id:
            raise ValueError(
                "surveyId is required in table_options for distributions table"
            )

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

        return iter(all_contacts), new_offset

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

