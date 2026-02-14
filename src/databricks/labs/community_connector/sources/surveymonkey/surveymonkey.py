import requests
import time
from pyspark.sql.types import StructType
from typing import Dict, List, Tuple, Iterator

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.sources.surveymonkey.surveymonkey_schemas import (
    TABLE_SCHEMAS,
    OBJECT_CONFIG,
    SUPPORTED_TABLES,
)


class SurveymonkeyLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict) -> None:
        """
        Initialize the SurveyMonkey connector with API credentials.

        Args:
            options: Dictionary containing:
                - access_token: SurveyMonkey OAuth access token
                - base_url (optional): API base URL, defaults to US data center
        """
        self.access_token = options["access_token"]
        # Support both US and EU data centers
        self.base_url = options.get("base_url", "https://api.surveymonkey.com/v3")
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ─── Interface Methods ────────────────────────────────────────────────────

    def list_tables(self) -> List[str]:
        """List available SurveyMonkey tables/objects."""
        return SUPPORTED_TABLES.copy()

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """Get the Spark schema for a SurveyMonkey table."""
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """Get metadata for a SurveyMonkey table."""
        self._validate_table(table_name)
        config = OBJECT_CONFIG[table_name]
        result = {
            "primary_keys": config["primary_keys"],
            "ingestion_type": config["ingestion_type"],
        }
        # Only include cursor_field for cdc ingestion type
        if config["ingestion_type"] == "cdc" and config["cursor_field"]:
            result["cursor_field"] = config["cursor_field"]
        return result

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read data from a SurveyMonkey table."""
        self._validate_table(table_name)

        config = OBJECT_CONFIG[table_name]

        # Validate required parameters (some tables can iterate over all parents if not provided)
        requires_survey_id = config.get("requires_survey_id", False)
        requires_page_id = config.get("requires_page_id", False)
        requires_group_id = config.get("requires_group_id", False)

        # Tables that can iterate over all parent objects if parent_id not provided
        can_iterate_surveys = table_name in ["survey_responses", "survey_pages", "survey_questions", "collectors", "survey_rollups"]
        can_iterate_pages = table_name in ["survey_questions"]
        can_iterate_groups = table_name in ["group_members"]

        if requires_survey_id and not table_options.get("survey_id") and not can_iterate_surveys:
            raise ValueError(
                f"Table '{table_name}' requires 'survey_id' in table_options"
            )
        if requires_page_id and not table_options.get("page_id") and not can_iterate_pages:
            raise ValueError(
                f"Table '{table_name}' requires 'page_id' in table_options"
            )
        if requires_group_id and not table_options.get("group_id") and not can_iterate_groups:
            raise ValueError(
                f"Table '{table_name}' requires 'group_id' in table_options"
            )

        # Determine ingestion type and read accordingly
        if config["ingestion_type"] == "cdc":
            cursor_field = config["cursor_field"]
            is_incremental = (
                start_offset is not None
                and start_offset.get(cursor_field) is not None
            )
            if is_incremental:
                return self._read_data_incremental(table_name, start_offset, table_options)
            else:
                return self._read_data_full(table_name, table_options)
        else:
            # Snapshot ingestion
            return self._read_data_full(table_name, table_options)

    # ─── Helpers ──────────────────────────────────────────────────────────────

    def _validate_table(self, table_name: str) -> None:
        """Validate that the table is supported."""
        if table_name not in TABLE_SCHEMAS:
            raise ValueError(
                f"Unsupported table: {table_name}. Supported tables are: {SUPPORTED_TABLES}"
            )

    def _make_request(
        self, url: str, params: dict = None, retries: int = 3, allow_empty_on_error: bool = False
    ) -> dict:
        """Make an API request with retry logic and rate limit handling."""
        for attempt in range(retries):
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - wait and retry
                reset_time = int(response.headers.get("X-Ratelimit-App-Global-Minute-Reset", 60))
                time.sleep(min(reset_time, 60))
                continue
            elif response.status_code in (400, 403) and allow_empty_on_error:
                # Permission denied or bad request - return empty data
                return {"data": []}
            else:
                raise Exception(
                    f"SurveyMonkey API error: {response.status_code} {response.text}"
                )

        raise Exception("Max retries exceeded due to rate limiting")

    def _build_endpoint_url(self, table_name: str, table_options: Dict[str, str]) -> str:
        """Build the API endpoint URL with path parameters substituted."""
        config = OBJECT_CONFIG[table_name]
        endpoint = config["endpoint"]

        # Substitute path parameters
        if "{survey_id}" in endpoint:
            endpoint = endpoint.replace("{survey_id}", table_options["survey_id"])
        if "{page_id}" in endpoint:
            endpoint = endpoint.replace("{page_id}", table_options["page_id"])
        if "{group_id}" in endpoint:
            endpoint = endpoint.replace("{group_id}", table_options["group_id"])

        return f"{self.base_url}{endpoint}"

    def _clean_empty_dicts(self, obj):
        """Recursively convert empty dicts to None for nested structures."""
        if isinstance(obj, dict):
            if not obj:
                return None
            return {k: self._clean_empty_dicts(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_empty_dicts(item) for item in obj]
        return obj

    def _add_parent_identifiers(
        self, table_name: str, record: dict, table_options: Dict[str, str]
    ) -> dict:
        """Add parent identifiers to child records."""
        config = OBJECT_CONFIG[table_name]

        if config.get("requires_survey_id"):
            record["survey_id"] = table_options.get("survey_id")
        if config.get("requires_page_id"):
            record["page_id"] = table_options.get("page_id")
        if config.get("requires_group_id"):
            record["group_id"] = table_options.get("group_id")

        return record

    # ─── Table Readers ────────────────────────────────────────────────────────

    def _read_data_full(
        self, table_name: str, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read all data from a SurveyMonkey table (full refresh)."""
        config = OBJECT_CONFIG[table_name]

        # Handle special case for users (single record endpoint)
        if table_name == "users":
            return self._read_single_user()

        # Handle special case for survey_responses - need to iterate over surveys
        if table_name == "survey_responses" and not table_options.get("survey_id"):
            return self._read_all_survey_responses(table_options, start_modified_at=None)

        # Handle special case for survey_pages - use details endpoint
        if table_name == "survey_pages":
            return self._read_survey_pages(table_options)

        # Handle special case for survey_questions - use details endpoint
        if table_name == "survey_questions":
            return self._read_all_survey_questions(table_options)

        # Handle special case for collectors - need to iterate over surveys
        if table_name == "collectors" and not table_options.get("survey_id"):
            return self._read_all_collectors(table_options)

        # Handle special case for group_members - need to iterate over groups
        if table_name == "group_members" and not table_options.get("group_id"):
            return self._read_all_group_members(table_options)

        # Handle special case for survey_rollups - need survey_id
        if table_name == "survey_rollups":
            return self._read_survey_rollups(table_options)

        url = self._build_endpoint_url(table_name, table_options)
        per_page = config["per_page"]

        all_records = []
        latest_cursor_value = None
        page = 1

        while True:
            params = {
                "page": page,
                "per_page": per_page,
            }

            # For surveys, request additional fields
            if table_name == "surveys":
                params["include"] = "response_count,date_created,date_modified,language,question_count"
                params["sort_by"] = "date_modified"
                params["sort_order"] = "ASC"

            # Some endpoints require special permissions - allow empty results on 400/403
            allow_empty = table_name in ["workgroups", "webhooks", "benchmark_bundles", "groups"]
            data = self._make_request(url, params, allow_empty_on_error=allow_empty)

            # Handle single record response (like /users/me)
            if "data" not in data:
                data = self._clean_empty_dicts(data)
                return iter([data]), {}

            # Handle list response
            records = data.get("data", [])
            if not records:
                break

            # Add parent identifiers for child objects and clean empty dicts
            for record in records:
                record = self._add_parent_identifiers(table_name, record, table_options)
                record = self._clean_empty_dicts(record)
                all_records.append(record)

                # Track latest cursor value for cdc tables
                if config["ingestion_type"] == "cdc" and config["cursor_field"]:
                    cursor_value = record.get(config["cursor_field"])
                    if cursor_value:
                        if latest_cursor_value is None or cursor_value > latest_cursor_value:
                            latest_cursor_value = cursor_value

            # Check for more pages
            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)  # Rate limiting

        # Build offset
        offset = {}
        if config["ingestion_type"] == "cdc" and config["cursor_field"] and latest_cursor_value:
            offset[config["cursor_field"]] = latest_cursor_value

        return iter(all_records), offset

    def _read_data_incremental(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read incremental data from a SurveyMonkey table."""
        config = OBJECT_CONFIG[table_name]
        cursor_field = config["cursor_field"]
        cursor_start = start_offset.get(cursor_field)

        # Handle special case for survey_responses
        if table_name == "survey_responses" and not table_options.get("survey_id"):
            return self._read_all_survey_responses(table_options, start_modified_at=cursor_start)

        # Handle special case for collectors across all surveys
        if table_name == "collectors" and not table_options.get("survey_id"):
            return self._read_all_collectors(table_options, start_modified_at=cursor_start)

        url = self._build_endpoint_url(table_name, table_options)
        per_page = config["per_page"]

        all_records = []
        latest_cursor_value = cursor_start
        page = 1

        while True:
            params = {
                "page": page,
                "per_page": per_page,
                "sort_by": cursor_field,
                "sort_order": "ASC",
            }

            # Add incremental filter
            if cursor_start:
                params["start_modified_at"] = cursor_start

            # For surveys, request additional fields
            if table_name == "surveys":
                params["include"] = "response_count,date_created,date_modified,language,question_count"

            data = self._make_request(url, params)
            records = data.get("data", [])

            if not records:
                break

            for record in records:
                record = self._add_parent_identifiers(table_name, record, table_options)
                record = self._clean_empty_dicts(record)
                all_records.append(record)

                # Track latest cursor value
                cursor_value = record.get(cursor_field)
                if cursor_value:
                    if latest_cursor_value is None or cursor_value > latest_cursor_value:
                        latest_cursor_value = cursor_value

            # Check for more pages
            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        offset = {cursor_field: latest_cursor_value} if latest_cursor_value else {}
        return iter(all_records), offset

    def _read_single_user(self) -> Tuple[Iterator[dict], dict]:
        """Read the current user's information."""
        url = f"{self.base_url}/users/me"
        data = self._make_request(url)
        data = self._clean_empty_dicts(data)
        return iter([data]), {}

    def _read_all_survey_responses(
        self, table_options: Dict[str, str], start_modified_at: str = None
    ) -> Tuple[Iterator[dict], dict]:
        """Read responses across all surveys."""
        # First, get all surveys
        surveys_url = f"{self.base_url}/surveys"
        all_surveys = []
        page = 1

        while True:
            params = {"page": page, "per_page": 1000}
            data = self._make_request(surveys_url, params)
            surveys = data.get("data", [])

            if not surveys:
                break

            all_surveys.extend(surveys)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        # Then, get responses for each survey
        all_responses = []
        latest_cursor_value = start_modified_at

        for survey in all_surveys:
            survey_id = survey["id"]
            responses_url = f"{self.base_url}/surveys/{survey_id}/responses/bulk"
            page = 1

            while True:
                params = {
                    "page": page,
                    "per_page": 100,
                    "sort_by": "date_modified",
                    "sort_order": "ASC",
                }

                if start_modified_at:
                    params["start_modified_at"] = start_modified_at

                try:
                    data = self._make_request(responses_url, params)
                except Exception:
                    # Skip surveys with no access or errors
                    break

                responses = data.get("data", [])

                if not responses:
                    break

                for response in responses:
                    response["survey_id"] = survey_id
                    response = self._clean_empty_dicts(response)
                    all_responses.append(response)

                    cursor_value = response.get("date_modified")
                    if cursor_value:
                        if latest_cursor_value is None or cursor_value > latest_cursor_value:
                            latest_cursor_value = cursor_value

                if "next" not in data.get("links", {}):
                    break

                page += 1
                time.sleep(0.1)

        offset = {"date_modified": latest_cursor_value} if latest_cursor_value else {}
        return iter(all_responses), offset

    def _read_all_survey_questions(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read all questions from a survey by iterating through pages."""
        survey_id = table_options.get("survey_id")

        # If no survey_id provided, iterate over all surveys
        if not survey_id:
            return self._read_all_questions_all_surveys()

        # Use the /surveys/{id}/details endpoint to get pages and questions
        details_url = f"{self.base_url}/surveys/{survey_id}/details"
        all_questions = []

        try:
            data = self._make_request(details_url)
            pages = data.get("pages", [])

            for survey_page in pages:
                page_id = survey_page["id"]
                questions = survey_page.get("questions", [])

                for question in questions:
                    question["survey_id"] = survey_id
                    question["page_id"] = page_id
                    question = self._clean_empty_dicts(question)
                    all_questions.append(question)
        except Exception:
            pass

        return iter(all_questions), {}

    def _read_all_questions_all_surveys(self) -> Tuple[Iterator[dict], dict]:
        """Read all questions from all surveys."""
        # First, get all surveys
        surveys_url = f"{self.base_url}/surveys"
        all_surveys = []
        page = 1

        while True:
            params = {"page": page, "per_page": 1000}
            data = self._make_request(surveys_url, params)
            surveys = data.get("data", [])

            if not surveys:
                break

            all_surveys.extend(surveys)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        # Then, get questions for each survey via details endpoint
        all_questions = []

        for survey in all_surveys:
            survey_id = survey["id"]
            details_url = f"{self.base_url}/surveys/{survey_id}/details"

            try:
                data = self._make_request(details_url)
                pages = data.get("pages", [])

                for survey_page in pages:
                    page_id = survey_page["id"]
                    questions = survey_page.get("questions", [])

                    for question in questions:
                        question["survey_id"] = survey_id
                        question["page_id"] = page_id
                        question = self._clean_empty_dicts(question)
                        all_questions.append(question)
            except Exception:
                # Skip surveys with no access or errors
                continue

            time.sleep(0.1)

        return iter(all_questions), {}

    def _read_survey_pages(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read pages from a specific survey or all surveys."""
        survey_id = table_options.get("survey_id")

        if survey_id:
            # Read pages for a specific survey
            details_url = f"{self.base_url}/surveys/{survey_id}/details"
            all_pages = []

            try:
                data = self._make_request(details_url)
                pages = data.get("pages", [])

                for survey_page in pages:
                    survey_page["survey_id"] = survey_id
                    survey_page = self._clean_empty_dicts(survey_page)
                    all_pages.append(survey_page)
            except Exception:
                pass

            return iter(all_pages), {}

        # No survey_id - read pages from all surveys
        surveys_url = f"{self.base_url}/surveys"
        all_surveys = []
        page = 1

        while True:
            params = {"page": page, "per_page": 1000}
            data = self._make_request(surveys_url, params)
            surveys = data.get("data", [])

            if not surveys:
                break

            all_surveys.extend(surveys)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        # Get pages for each survey using the details endpoint
        all_pages = []

        for survey in all_surveys:
            sid = survey["id"]
            details_url = f"{self.base_url}/surveys/{sid}/details"

            try:
                data = self._make_request(details_url)
                pages = data.get("pages", [])

                for survey_page in pages:
                    survey_page["survey_id"] = sid
                    survey_page = self._clean_empty_dicts(survey_page)
                    all_pages.append(survey_page)
            except Exception:
                # Skip surveys with no access or errors
                continue

            time.sleep(0.1)

        return iter(all_pages), {}

    def _read_all_collectors(
        self, table_options: Dict[str, str], start_modified_at: str = None
    ) -> Tuple[Iterator[dict], dict]:
        """Read all collectors from all surveys."""
        # First, get all surveys
        surveys_url = f"{self.base_url}/surveys"
        all_surveys = []
        page = 1

        while True:
            params = {"page": page, "per_page": 1000}
            data = self._make_request(surveys_url, params)
            surveys = data.get("data", [])

            if not surveys:
                break

            all_surveys.extend(surveys)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        # Then, get collectors for each survey
        all_collectors = []
        latest_cursor_value = start_modified_at

        for survey in all_surveys:
            survey_id = survey["id"]
            collectors_url = f"{self.base_url}/surveys/{survey_id}/collectors"
            page = 1

            while True:
                params = {
                    "page": page,
                    "per_page": 1000,
                    "sort_by": "date_modified",
                    "sort_order": "ASC",
                }

                if start_modified_at:
                    params["start_modified_at"] = start_modified_at

                try:
                    data = self._make_request(collectors_url, params)
                except Exception:
                    # Skip surveys with no access or errors
                    break

                collectors = data.get("data", [])

                if not collectors:
                    break

                for collector in collectors:
                    collector["survey_id"] = survey_id
                    collector = self._clean_empty_dicts(collector)
                    all_collectors.append(collector)

                    cursor_value = collector.get("date_modified")
                    if cursor_value:
                        if latest_cursor_value is None or cursor_value > latest_cursor_value:
                            latest_cursor_value = cursor_value

                if "next" not in data.get("links", {}):
                    break

                page += 1
                time.sleep(0.1)

        offset = {"date_modified": latest_cursor_value} if latest_cursor_value else {}
        return iter(all_collectors), offset

    def _read_all_group_members(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read all members from all groups."""
        # First, get all groups
        groups_url = f"{self.base_url}/groups"
        all_groups = []
        page = 1

        while True:
            params = {"page": page, "per_page": 1000}
            try:
                data = self._make_request(groups_url, params)
            except Exception:
                # Groups endpoint may not be available for all accounts
                break

            groups = data.get("data", [])

            if not groups:
                break

            all_groups.extend(groups)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        # Then, get members for each group
        all_members = []

        for group in all_groups:
            group_id = group["id"]
            members_url = f"{self.base_url}/groups/{group_id}/members"
            page = 1

            while True:
                params = {"page": page, "per_page": 1000}

                try:
                    data = self._make_request(members_url, params)
                except Exception:
                    # Skip groups with no access or errors
                    break

                members = data.get("data", [])

                if not members:
                    break

                for member in members:
                    member["group_id"] = group_id
                    member = self._clean_empty_dicts(member)
                    all_members.append(member)

                if "next" not in data.get("links", {}):
                    break

                page += 1
                time.sleep(0.1)

        return iter(all_members), {}

    def _read_survey_rollups(
        self, table_options: Dict[str, str]
    ) -> Tuple[Iterator[dict], dict]:
        """Read rollup statistics for a specific survey or all surveys."""
        survey_id = table_options.get("survey_id")

        if survey_id:
            return self._read_rollups_for_survey(survey_id)

        # No survey_id - read rollups from all surveys
        surveys_url = f"{self.base_url}/surveys"
        all_surveys = []
        page = 1

        while True:
            params = {"page": page, "per_page": 1000}
            data = self._make_request(surveys_url, params)
            surveys = data.get("data", [])

            if not surveys:
                break

            all_surveys.extend(surveys)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        # Get rollups for each survey
        all_rollups = []

        for survey in all_surveys:
            sid = survey["id"]
            rollups, _ = self._read_rollups_for_survey(sid)
            all_rollups.extend(list(rollups))
            time.sleep(0.1)

        return iter(all_rollups), {}

    def _read_rollups_for_survey(
        self, survey_id: str
    ) -> Tuple[Iterator[dict], dict]:
        """Read rollup statistics for a specific survey."""
        rollups_url = f"{self.base_url}/surveys/{survey_id}/rollups"
        all_rollups = []
        page = 1

        while True:
            params = {"page": page, "per_page": 100}

            try:
                data = self._make_request(rollups_url, params)
            except Exception:
                # Skip surveys with no access or errors
                break

            rollups = data.get("data", [])

            if not rollups:
                break

            for rollup in rollups:
                rollup["survey_id"] = survey_id
                rollup = self._clean_empty_dicts(rollup)
                all_rollups.append(rollup)

            if "next" not in data.get("links", {}):
                break

            page += 1
            time.sleep(0.1)

        return iter(all_rollups), {}

    def test_connection(self) -> dict:
        """Test the connection to SurveyMonkey API."""
        try:
            url = f"{self.base_url}/users/me"
            response = requests.get(url, headers=self.headers)

            if response.status_code == 200:
                user_data = response.json()
                return {
                    "status": "success",
                    "message": f"Connected as {user_data.get('email', 'unknown')}",
                }
            else:
                return {
                    "status": "error",
                    "message": f"API error: {response.status_code} {response.text}",
                }
        except Exception as e:
            return {"status": "error", "message": f"Connection failed: {str(e)}"}
