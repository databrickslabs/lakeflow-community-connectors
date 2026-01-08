import csv
import io
import json
import time
import zipfile
import requests
from typing import Iterator, Any
from datetime import datetime, timedelta

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
    TimestampType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the UiPath Orchestrator connector with connection-level options.

        Expected options:
            - organization_name: UiPath organization name
            - tenant_name: UiPath tenant name
            - client_id: OAuth App ID for client credentials flow
            - client_secret: OAuth App Secret for client credentials flow
            - folder_id: Organization unit ID (folder) to query
            - scope (optional): OAuth scopes (defaults to "OR.Queues OR.Execution")
        """
        self.organization_name = options.get("organization_name")
        self.tenant_name = options.get("tenant_name")
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        self.folder_id = options.get("folder_id")
        self.scope = options.get("scope", "OR.Queues OR.Execution")

        if not self.organization_name:
            raise ValueError("UiPath connector requires 'organization_name' in options")
        if not self.tenant_name:
            raise ValueError("UiPath connector requires 'tenant_name' in options")
        if not self.client_id:
            raise ValueError("UiPath connector requires 'client_id' in options")
        if not self.client_secret:
            raise ValueError("UiPath connector requires 'client_secret' in options")
        if not self.folder_id:
            raise ValueError("UiPath connector requires 'folder_id' in options")

        self.identity_base_url = f"https://cloud.uipath.com/{self.organization_name}/identity_"
        self.api_base_url = f"https://cloud.uipath.com/{self.organization_name}/{self.tenant_name}/orchestrator_"

        # Access token will be obtained and cached
        self._access_token = None
        self._token_expires_at = None

        # Configure session
        self._session = requests.Session()

    def _get_access_token(self) -> str:
        """
        Obtain or refresh access token using OAuth client credentials flow.
        Tokens are cached and refreshed when expired.
        """
        # Return cached token if still valid
        if self._access_token and self._token_expires_at:
            if datetime.now() < self._token_expires_at:
                return self._access_token

        # Obtain new access token
        token_url = f"{self.identity_base_url}/connect/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope,
        }

        response = requests.post(
            token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to obtain access token: {response.status_code} {response.text}"
            )

        token_data = response.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)

        # Set expiration time with a 5-minute buffer
        self._token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)

        return self._access_token

    def _get_headers(self) -> dict:
        """
        Get HTTP headers with access token and folder ID.
        """
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "X-UIPATH-OrganizationUnitId": self.folder_id,
            "Accept": "application/json",
        }

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.
        """
        return ["queue_items"]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        if table_name == "queue_items":
            # Nested Robot struct
            robot_struct = StructType(
                [
                    StructField("Id", LongType(), True),
                    StructField("Name", StringType(), True),
                    StructField("Type", StringType(), True),
                ]
            )

            # Nested ReviewerUser struct
            reviewer_user_struct = StructType(
                [
                    StructField("Id", LongType(), True),
                    StructField("Username", StringType(), True),
                    StructField("Email", StringType(), True),
                ]
            )

            # Main queue_items schema
            schema = StructType(
                [
                    StructField("Id", LongType(), False),
                    StructField("QueueDefinitionId", LongType(), True),
                    StructField("Name", StringType(), True),
                    StructField("Priority", StringType(), True),
                    StructField("Status", StringType(), True),
                    StructField("ReviewStatus", StringType(), True),
                    StructField("Reference", StringType(), True),
                    StructField("ProcessingException", StringType(), True),
                    StructField("DueDate", TimestampType(), True),
                    StructField("RiskSlaDate", TimestampType(), True),
                    StructField("DeferDate", TimestampType(), True),
                    StructField("StartProcessing", TimestampType(), True),
                    StructField("EndProcessing", TimestampType(), True),
                    StructField("SecondsInPreviousAttempts", IntegerType(), True),
                    StructField("AncestorId", LongType(), True),
                    StructField("RetryNumber", IntegerType(), True),
                    StructField("SpecificContent", StringType(), True),  # JSON string
                    StructField("Output", StringType(), True),  # JSON string
                    StructField("Progress", StringType(), True),
                    StructField("CreationTime", TimestampType(), True),
                    StructField("LastModificationTime", TimestampType(), True),
                    StructField("Robot", robot_struct, True),
                    StructField("ReviewerUser", reviewer_user_struct, True),
                    StructField("OrganizationUnitId", LongType(), True),
                ]
            )
            return schema

        raise ValueError(f"Unknown table: {table_name}")

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        if table_name == "queue_items":
            return {
                "primary_keys": ["Id"],
                "cursor_field": "LastModificationTime",
                "ingestion_type": "append",
            }

        raise ValueError(f"Unknown table: {table_name}")

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read data from a table and return an iterator of records along with the next offset.

        For queue_items, table_options can include:
            - queue_definition_id (required): ID of the queue to export items from
            - filter (optional): OData filter expression
            - expand (optional): Related entities to expand (default: "Robot,ReviewerUser")
        """
        if table_name not in self.list_tables():
            raise ValueError(
                f"Table '{table_name}' is not supported. Supported tables: {self.list_tables()}"
            )

        if table_name == "queue_items":
            queue_definition_id = table_options.get("queue_definition_id")
            if not queue_definition_id:
                raise ValueError(
                    "table_options must include 'queue_definition_id' for queue_items table"
                )

            # Build filter for incremental reads
            filter_expr = table_options.get("filter", "")
            if start_offset and "last_modification_time" in start_offset:
                last_mod_time = start_offset["last_modification_time"]
                time_filter = f"(LastModificationTime ge {last_mod_time})"
                if filter_expr:
                    filter_expr = f"({filter_expr}) and {time_filter}"
                else:
                    filter_expr = time_filter

            expand = table_options.get("expand", "Robot,ReviewerUser")

            # Execute 3-step export process
            records_iterator = self._export_queue_items(
                queue_definition_id, filter_expr, expand
            )

            # Track the maximum LastModificationTime for the next offset
            max_modification_time = start_offset.get("last_modification_time", "")

            # Wrap iterator to track max timestamp
            def tracked_iterator():
                nonlocal max_modification_time
                for record in records_iterator:
                    last_mod = record.get("LastModificationTime")
                    if last_mod and last_mod > max_modification_time:
                        max_modification_time = last_mod
                    yield record

            return tracked_iterator(), {
                "last_modification_time": max_modification_time
            }

        raise ValueError(f"Unknown table: {table_name}")

    def _export_queue_items(
        self, queue_definition_id: str, filter_expr: str, expand: str
    ) -> Iterator[dict]:
        """
        Execute the 3-step export process for queue items:
        1. Initiate export
        2. Poll export status
        3. Download CSV and parse records
        """
        # Step 1: Initiate export
        export_id = self._initiate_export(queue_definition_id, filter_expr, expand)

        # Step 2: Poll export status until completed
        self._poll_export_status(export_id)

        # Step 3: Get download link and download CSV
        download_url = self._get_download_link(export_id)

        # Download and parse CSV
        csv_content = self._download_csv(download_url)

        # Parse CSV and yield records
        yield from self._parse_csv(csv_content)

    def _initiate_export(
        self, queue_definition_id: str, filter_expr: str, expand: str
    ) -> int:
        """
        Step 1: Initiate export and return export ID.
        """
        url = f"{self.api_base_url}/odata/QueueDefinitions({queue_definition_id})/UiPathODataSvc.Export"

        # Build query parameters
        params = []
        if filter_expr:
            params.append(f"$filter={filter_expr}")
        if expand:
            params.append(f"$expand={expand}")

        if params:
            url += "?" + "&".join(params)

        headers = self._get_headers()
        headers["Content-Length"] = "0"

        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to initiate export: {response.status_code} {response.text}"
            )

        export_data = response.json()
        export_id = export_data.get("Id")

        if not export_id:
            raise RuntimeError(f"Export response missing 'Id': {export_data}")

        return export_id

    def _poll_export_status(
        self, export_id: int, max_wait_seconds: int = 600, poll_interval: int = 5
    ) -> None:
        """
        Step 2: Poll export status until completed or failed.
        """
        url = f"{self.api_base_url}/odata/Exports({export_id})"
        headers = self._get_headers()

        start_time = time.time()

        while True:
            if time.time() - start_time > max_wait_seconds:
                raise TimeoutError(
                    f"Export {export_id} did not complete within {max_wait_seconds} seconds"
                )

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to poll export status: {response.status_code} {response.text}"
                )

            export_status = response.json()
            status = export_status.get("Status")

            if status == "Completed":
                return
            elif status == "Failed":
                raise RuntimeError(f"Export {export_id} failed")
            elif status in ["New", "InProgress"]:
                time.sleep(poll_interval)
            else:
                raise RuntimeError(f"Unknown export status: {status}")

    def _get_download_link(self, export_id: int) -> str:
        """
        Step 3: Get download link for the completed export.
        """
        url = f"{self.api_base_url}/odata/Exports({export_id})/UiPath.Server.Configuration.OData.GetDownloadLink"
        headers = self._get_headers()

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to get download link: {response.status_code} {response.text}"
            )

        download_data = response.json()
        download_uri = download_data.get("Uri")

        if not download_uri:
            raise RuntimeError(f"Download link response missing 'Uri': {download_data}")

        return download_uri

    def _download_csv(self, download_url: str) -> str:
        """
        Download and extract CSV file from the signed URL.
        UiPath may return either a plain CSV or a ZIP file containing the CSV.
        """
        # No authentication required for signed URLs
        response = requests.get(download_url)

        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to download CSV: {response.status_code} {response.text}"
            )

        content = response.content
        
        # Check if the content is a ZIP file (starts with 'PK')
        if content.startswith(b'PK'):
            # Extract CSV from ZIP
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                    # Get the first CSV file in the archive
                    csv_files = [name for name in zip_file.namelist() if name.endswith('.csv')]
                    if not csv_files:
                        raise RuntimeError("No CSV file found in the ZIP archive")
                    
                    # Read the first CSV file
                    with zip_file.open(csv_files[0]) as csv_file:
                        return csv_file.read().decode('utf-8')
            except zipfile.BadZipFile as e:
                raise RuntimeError(f"Failed to extract ZIP file: {e}")
        else:
            # Plain CSV text
            return response.text

    def _parse_csv(self, csv_content: str) -> Iterator[dict]:
        """
        Parse CSV content and yield records as dictionaries.
        Handles UiPath's actual CSV column names and nested JSON fields.
        
        UiPath CSV columns:
        - Status, Reference, Priority, Id (match our expected names)
        - "Specific Data" -> SpecificContent
        - "Created (absolute)" -> CreationTime
        - "Deadline (absolute)" -> DueDate
        - "Postpone (absolute)" -> DeferDate
        - "Started (absolute)" -> StartProcessing
        - "Ended (absolute)" -> EndProcessing
        - "Retry No." -> RetryNumber
        - "Transaction Execution Time" -> SecondsInPreviousAttempts
        - Robot, Output, Progress (similar names)
        """
        # Use StringIO with proper newline handling for CSV with multiline fields
        csv_file = io.StringIO(csv_content, newline='')
        csv_reader = csv.DictReader(csv_file)

        for row in csv_reader:
            record = {}

            # Parse basic fields (direct matches)
            record["Id"] = self._parse_int(row.get("Id"))
            record["Priority"] = row.get("Priority")
            record["Status"] = row.get("Status")
            record["Reference"] = row.get("Reference")
            record["Progress"] = row.get("Progress")

            # Fields not in UiPath export - set to reasonable defaults
            record["QueueDefinitionId"] = None  # Not in export CSV
            record["Name"] = None  # Queue name not in export CSV
            record["ReviewStatus"] = None  # Not in export CSV
            record["OrganizationUnitId"] = None  # Not in export CSV
            record["RiskSlaDate"] = None  # Not in export CSV

            # Map UiPath column names to our field names
            record["ProcessingException"] = row.get("Exception") or row.get("Exception Details")
            record["DueDate"] = row.get("Deadline (absolute)")
            record["DeferDate"] = row.get("Postpone (absolute)")
            record["StartProcessing"] = row.get("Started (absolute)")
            record["EndProcessing"] = row.get("Ended (absolute)")
            record["CreationTime"] = row.get("Created (absolute)")
            
            # Use CreationTime as LastModificationTime if not available
            record["LastModificationTime"] = row.get("LastModificationTime") or row.get("Created (absolute)")

            # Parse integer fields
            record["SecondsInPreviousAttempts"] = self._parse_int(
                row.get("Transaction Execution Time")
            )
            record["AncestorId"] = self._parse_int(row.get("AncestorId"))
            record["RetryNumber"] = self._parse_int(row.get("Retry No."))

            # Parse JSON fields - UiPath uses "Specific Data" instead of "SpecificContent"
            record["SpecificContent"] = row.get("Specific Data")
            record["Output"] = row.get("Output")

            # Parse nested Robot struct (column name is just "Robot")
            robot_value = row.get("Robot")
            if robot_value and robot_value.strip():
                record["Robot"] = {"Name": robot_value}
            else:
                record["Robot"] = None

            # Parse nested ReviewerUser struct (column name is "Reviewer Name")
            reviewer_value = row.get("Reviewer Name")
            if reviewer_value and reviewer_value.strip():
                record["ReviewerUser"] = {"Username": reviewer_value}
            else:
                record["ReviewerUser"] = None

            yield record

    def _parse_int(self, value: str) -> int:
        """Parse integer value, return None if empty or invalid."""
        if not value or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _parse_robot(self, row: dict) -> dict:
        """
        Parse Robot nested structure from CSV row.
        CSV may have Robot.Id, Robot.Name, Robot.Type columns.
        """
        robot_id = row.get("Robot.Id") or row.get("RobotId")
        robot_name = row.get("Robot.Name") or row.get("RobotName")
        robot_type = row.get("Robot.Type") or row.get("RobotType")

        if not robot_id and not robot_name:
            return None

        robot = {}
        if robot_id:
            robot["Id"] = self._parse_int(robot_id)
        if robot_name:
            robot["Name"] = robot_name
        if robot_type:
            robot["Type"] = robot_type

        return robot if robot else None

    def _parse_reviewer_user(self, row: dict) -> dict:
        """
        Parse ReviewerUser nested structure from CSV row.
        CSV may have ReviewerUser.Id, ReviewerUser.Username, ReviewerUser.Email columns.
        """
        user_id = row.get("ReviewerUser.Id") or row.get("ReviewerUserId")
        username = row.get("ReviewerUser.Username") or row.get("ReviewerUsername")
        email = row.get("ReviewerUser.Email") or row.get("ReviewerEmail")

        if not user_id and not username:
            return None

        user = {}
        if user_id:
            user["Id"] = self._parse_int(user_id)
        if username:
            user["Username"] = username
        if email:
            user["Email"] = email

        return user if user else None

