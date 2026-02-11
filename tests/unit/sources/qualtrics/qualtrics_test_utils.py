import random
import time
import traceback
from datetime import datetime
from typing import Dict, List, Tuple

import requests


class LakeflowConnectTestUtils:
    """
    Test utilities for Qualtrics connector.
    Provides write-back functionality for testing Qualtrics survey response ingestion.

    Uses the Qualtrics Create Response API to create test survey responses
    programmatically. This is more reliable than the Sessions API, which has
    restrictions on supported question types and may require special permissions.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        """
        Initialize Qualtrics test utilities with connection options.

        Args:
            options: Dictionary containing:
                - api_token: Qualtrics API token
                - datacenter_id: Datacenter identifier
        """
        self.options = options
        self.api_token = options.get("api_token")
        self.datacenter_id = options.get("datacenter_id")

        if not self.api_token or not self.datacenter_id:
            raise ValueError("api_token and datacenter_id are required")

        self.base_url = f"https://{self.datacenter_id}.qualtrics.com/API/v3"
        self.headers = {
            "X-API-TOKEN": self.api_token,
            "Content-Type": "application/json"
        }

    def get_source_name(self) -> str:
        """Return the source connector name."""
        return "qualtrics"

    def list_insertable_tables(self) -> List[str]:
        """
        List all tables that support insert/write-back functionality.

        Currently only survey_responses can be created programmatically via
        the Create Response API.

        Returns:
            List of table names that support inserting new data
        """
        return ["survey_responses"]

    def generate_rows_and_write(
        self, table_name: str, number_of_rows: int
    ) -> Tuple[bool, List[Dict], Dict[str, str]]:
        """
        Generate specified number of rows and write them to Qualtrics.

        For survey_responses, this creates survey response records using the
        Create Response API. Test responses are marked with embedded data
        for identification.

        Args:
            table_name: Name of the table to write to
            number_of_rows: Number of rows to generate and write

        Returns:
            Tuple containing:
            - Boolean indicating success of the operation
            - List of rows as dictionaries that were written
            - Dictionary mapping written column names to returned column names
        """
        try:
            if number_of_rows <= 0:
                print(f"Invalid number_of_rows: {number_of_rows}")
                return False, [], {}

            if table_name not in self.list_insertable_tables():
                print(f"Table {table_name} does not support write operations")
                return False, [], {}

            if table_name == "survey_responses":
                return self._generate_and_write_survey_responses(number_of_rows)
            else:
                print(f"Write operation not implemented for table: {table_name}")
                return False, [], {}

        except Exception as e:
            print(f"Error in generate_rows_and_write for {table_name}: {e}")
            traceback.print_exc()
            return False, [], {}

    def _generate_and_write_survey_responses(
        self, count: int
    ) -> Tuple[bool, List[Dict], Dict[str, str]]:
        """
        Generate and write survey responses using the Qualtrics Create Response API.

        Uses POST /surveys/{surveyId}/responses which directly creates a completed
        response without requiring a session workflow. This works with all survey
        question types unlike the Sessions API.

        Args:
            count: Number of responses to create

        Returns:
            Tuple of (success, written_rows, column_mapping)
        """
        # Get survey ID from the most recent survey (for testing)
        survey_id = self._get_test_survey_id()
        if not survey_id:
            print("No survey found for testing. Please create a survey first.")
            return False, [], {}

        print(f"Using survey ID: {survey_id} for test response creation")

        written_rows = []
        created_response_ids = []
        success_count = 0

        for i in range(count):
            # Generate a unique test response
            test_id = f"test_{int(time.time())}_{random.randint(1000, 9999)}_{i}"

            # Create response data
            response_data = self._build_response_payload(test_id)

            # Write the response using Create Response API
            response_id = self._create_survey_response(survey_id, response_data)
            if response_id:
                # Store the written row with the fields we care about for matching
                written_row = {
                    "language": "EN",
                    "response_id": response_id,
                    "test_id": test_id,
                }
                written_rows.append(written_row)
                created_response_ids.append(response_id)
                success_count += 1
                print(
                    f"Successfully created test response {i + 1}/{count} "
                    f"(response_id: {response_id}, test_id: {test_id})"
                )
            else:
                print(f"Failed to create test response {i+1}/{count}")

            # Small delay to avoid rate limiting
            if i < count - 1:
                time.sleep(0.5)

        if success_count > 0:
            # Create column mapping (write field names -> read field names)
            column_mapping = self._get_column_mapping()

            # Wait for eventual consistency (responses need time to appear in exports)
            # In testing, Create Response API responses appear in exports within ~5s,
            # but we use 15s for safety margin across different environments.
            wait_seconds = 15
            print(f"\nWaiting {wait_seconds}s for responses to be available in export...")
            print("(Qualtrics needs time to process and make responses available)")
            time.sleep(wait_seconds)

            return True, written_rows, column_mapping
        else:
            return False, [], {}

    def _get_test_survey_id(self) -> str:
        """
        Get a survey ID for testing.
        Returns the first active survey found.
        """
        try:
            url = f"{self.base_url}/surveys"
            params = {"pageSize": 10}

            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            surveys = data.get("result", {}).get("elements", [])

            # Prefer active surveys
            for survey in surveys:
                if survey.get("isActive"):
                    return survey.get("id")

            # Fallback to any survey
            if surveys:
                return surveys[0].get("id")

            return None

        except Exception as e:
            print(f"Error getting test survey: {e}")
            return None

    def _build_response_payload(self, test_id: str) -> Dict:
        """
        Build the payload for the Create Response API.

        Args:
            test_id: Unique test identifier for tracking

        Returns:
            Dictionary with the API request payload
        """
        return {
            "values": {
                "finished": 1,
                "status": 0,
                "distributionChannel": "anonymous",
                "userLanguage": "EN",
            },
            "embeddedData": {
                "test_id": test_id,
                "source": "automated_test",
                "timestamp": datetime.utcnow().isoformat(),
            },
        }

    def _create_survey_response(self, survey_id: str, payload: Dict) -> str:
        """
        Create a survey response using the Qualtrics Create Response API.

        Uses POST /surveys/{surveyId}/responses to directly create a completed
        response. This endpoint works with all question types and does not require
        the Sessions API workflow.

        Args:
            survey_id: Survey ID to create response for
            payload: Request payload with values and embeddedData

        Returns:
            The responseId string if successful, None otherwise
        """
        try:
            url = f"{self.base_url}/surveys/{survey_id}/responses"

            response = requests.post(url, headers=self.headers, json=payload, timeout=15)

            if response.status_code == 200:
                result = response.json().get("result", {})
                response_id = result.get("responseId")
                if response_id:
                    return response_id
                else:
                    print("Create Response API returned 200 but no responseId")
                    return None
            else:
                print(f"Create Response API failed: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Request error creating survey response: {e}")
            return None
        except Exception as e:
            print(f"Error creating survey response: {e}")
            return None

    def _get_column_mapping(self) -> Dict[str, str]:
        """
        Create mapping from written column names to returned column names.

        The Create Response API writes fields that map to the connector's
        snake_case normalized field names:
        - language (write) -> user_language (read, snake_case of userLanguage)

        Returns:
            Dictionary mapping write field names to read field names
        """
        # IMPORTANT: Field names are normalized to snake_case by the connector,
        # so we must use snake_case names here (e.g., "user_language" not "userLanguage")
        column_mapping = {
            "language": "user_language",
        }

        return column_mapping
