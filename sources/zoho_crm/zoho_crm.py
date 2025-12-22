import json
import re
import requests
import time
from datetime import datetime, timedelta
from typing import Iterator, Any, Optional
from urllib.parse import quote

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    DoubleType,
    ArrayType,
    IntegerType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the Zoho CRM connector with connection-level options.

        Expected options:
            - client_id: OAuth Client ID from Zoho API Console
            - client_secret: OAuth Client Secret from Zoho API Console
            - refresh_token: Long-lived refresh token obtained from OAuth flow
            - base_url (optional): Zoho accounts URL for OAuth. Defaults to https://accounts.zoho.com
              Examples: https://accounts.zoho.com (US), https://accounts.zoho.eu (EU),
                        https://accounts.zoho.in (IN), https://accounts.zoho.com.au (AU)
            - initial_load_start_date (optional): Starting point for the first sync. If omitted, syncs all historical data.

        Note: To obtain the refresh_token, follow the OAuth setup guide in
        sources/zoho_crm/configs/README.md or visit:
        https://www.zoho.com/accounts/protocol/oauth/web-apps/authorization.html
        """
        self.client_id = options.get("client_id")
        self.client_secret = options.get("client_secret")
        self.refresh_token = options.get("refresh_token")

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError("Zoho CRM connector requires 'client_id', 'client_secret', and 'refresh_token' in options")

        # base_url is the accounts/OAuth URL (e.g., https://accounts.zoho.eu)
        self.accounts_url = options.get("base_url", "https://accounts.zoho.com").rstrip("/")

        # Derive the API URL from the accounts URL by extracting the domain suffix
        # https://accounts.zoho.eu -> https://www.zohoapis.eu
        # https://accounts.zoho.com -> https://www.zohoapis.com
        match = re.search(r"accounts\.zoho\.(.+)$", self.accounts_url)
        domain_suffix = match.group(1) if match else "com"
        self.api_url = f"https://www.zohoapis.{domain_suffix}"

        self.initial_load_start_date = options.get("initial_load_start_date")

        # OAuth token management
        self.access_token = None
        self.token_expires_at = None

        # Configure a session for API requests
        self._session = requests.Session()

        # Cache for module and field metadata
        self._modules_cache = None
        self._fields_cache = {}

        # Track maximum Modified_Time seen during read operations
        self._current_max_modified_time = None

    def _get_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.
        Access tokens expire after 1 hour (3600 seconds).
        """
        # Check if we have a valid token
        if self.access_token and self.token_expires_at:
            if datetime.now() < self.token_expires_at - timedelta(minutes=5):
                return self.access_token

        # Refresh the access token using the accounts URL
        token_url = f"{self.accounts_url}/oauth/v2/token"

        data = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }

        response = requests.post(token_url, data=data)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error_detail = response.text
            raise Exception(f"Failed to refresh access token: {e}. Response: {error_detail}")

        token_data = response.json()

        if "access_token" not in token_data:
            raise Exception(
                f"Token refresh response missing 'access_token'. "
                f"Response: {token_data}. "
                f"Please check your client_id, client_secret, and refresh_token are valid."
            )

        self.access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

        return self.access_token

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
    ) -> dict:
        """
        Make an authenticated API request to Zoho CRM.
        Handles token refresh and rate limiting.
        """
        access_token = self._get_access_token()

        url = f"{self.api_url}{endpoint}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {access_token}",
        }

        if data:
            headers["Content-Type"] = "application/json"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                if method.upper() == "GET":
                    response = self._session.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    response = self._session.post(url, headers=headers, json=data, params=params)
                elif method.upper() == "PUT":
                    response = self._session.put(url, headers=headers, json=data, params=params)
                elif method.upper() == "DELETE":
                    response = self._session.delete(url, headers=headers, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        # Exponential backoff: 2^attempt seconds
                        wait_time = 2**attempt
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception("Rate limit exceeded. Please wait and try again later.")

                response.raise_for_status()

                # Handle empty responses (some Zoho modules return empty body)
                if not response.text or response.text.strip() == "":
                    return {}

                return response.json()

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401:
                    # Token might be invalid, try refreshing once
                    if attempt == 0:
                        self.access_token = None
                        access_token = self._get_access_token()
                        headers["Authorization"] = f"Zoho-oauthtoken {access_token}"
                        continue
                raise

        raise Exception(f"Failed to make request after {max_retries} attempts")

    def _get_modules(self) -> list[dict]:
        """
        Retrieve all available modules from Zoho CRM.
        Results are cached to avoid repeated API calls.
        """
        if self._modules_cache is not None:
            return self._modules_cache

        response = self._make_request("GET", "/crm/v8/settings/modules")
        modules = response.get("modules", [])

        # Modules to exclude - analytics modules, system modules, and modules without proper API support
        excluded_modules = {
            "Visits",  # No fields available
            "Actions_Performed",  # No fields available
            "Email_Sentiment",  # Analytics module with different API
            "Email_Analytics",  # Analytics module with different API
            "Email_Template_Analytics",  # Analytics module with different API
            "Locking_Information__s",  # System module (403 forbidden)
        }

        # Filter for API-supported modules (default or custom types), excluding problematic ones
        supported_modules = [
            m for m in modules if m.get("api_supported") and m.get("generated_type") in ["default", "custom"] and m.get("api_name") not in excluded_modules
        ]

        self._modules_cache = supported_modules
        return supported_modules

    def _get_fields(self, module_name: str) -> list[dict]:
        """
        Retrieve field metadata for a specific module.
        Results are cached per module.
        """
        if module_name in self._fields_cache:
            return self._fields_cache[module_name]

        params = {"module": module_name}
        try:
            response = self._make_request("GET", "/crm/v8/settings/fields", params=params)
            fields = response.get("fields", [])
        except Exception as e:
            # Some modules may not support fields API or return empty responses
            print(f"Warning: Could not fetch fields for {module_name}: {e}")
            fields = []

        self._fields_cache[module_name] = fields
        return fields

    def list_tables(self) -> list[str]:
        """
        List names of all tables (modules) supported by this connector.
        Uses the Modules API to dynamically discover available modules.
        """
        modules = self._get_modules()
        return [m["api_name"] for m in modules]

    def _zoho_type_to_spark_type(self, field: dict) -> StructField:
        """
        Convert a Zoho CRM field definition to a Spark StructField.
        """
        api_name = field["api_name"]
        data_type = field.get("data_type", "text")
        json_type = field.get("json_type")
        nullable = not field.get("required", False)

        # Map Zoho data types to Spark types
        if data_type == "bigint":
            spark_type = LongType()
        elif data_type in ["text", "textarea", "email", "phone", "website", "autonumber"]:
            spark_type = StringType()
        elif data_type == "picklist":
            spark_type = StringType()
        elif data_type == "multiselectpicklist":
            spark_type = ArrayType(StringType(), True)
        elif data_type == "integer":
            spark_type = LongType()  # Prefer LongType over IntegerType
        elif data_type in ["double", "currency", "percent"]:
            spark_type = DoubleType()
        elif data_type == "boolean":
            spark_type = BooleanType()
        elif data_type in ["date", "datetime"]:
            spark_type = StringType()  # Store as ISO 8601 string
        elif data_type in ["lookup", "ownerlookup"]:
            # Lookup fields are nested objects
            lookup_struct = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True),
                ]
            )
            spark_type = lookup_struct
        elif data_type == "multiselectlookup":
            lookup_struct = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]
            )
            spark_type = ArrayType(lookup_struct, True)
        elif data_type in ["fileupload", "imageupload", "profileimage"]:
            spark_type = StringType()
        elif data_type == "subform":
            # Subforms are arrays of nested objects
            # We'll use a flexible structure since subform schemas vary
            spark_type = ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                    ]
                ),
                True,
            )
        elif data_type == "consent_lookup":
            spark_type = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                ]
            )
        elif data_type == "event_reminder":
            spark_type = StringType()
        elif data_type == "RRULE":
            spark_type = StructType(
                [
                    StructField("FREQ", StringType(), True),
                    StructField("INTERVAL", StringType(), True),
                ]
            )
        elif data_type == "ALARM":
            spark_type = StructType(
                [
                    StructField("ACTION", StringType(), True),
                ]
            )
        else:
            # Default to StringType for unknown types
            spark_type = StringType()

        # Handle special JSON types - store as JSON strings for flexibility
        if json_type == "jsonarray":
            spark_type = StringType()  # Store as JSON string
        elif json_type == "jsonobject":
            spark_type = StringType()  # Store as JSON string

        return StructField(api_name, spark_type, nullable)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """
        Fetch the schema of a module dynamically from Zoho CRM.
        Uses the Fields Metadata API to build the schema.
        """
        # Check if table exists
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' is not supported. " f"Available tables: {', '.join(available_tables)}")

        # Get field metadata for this module
        fields = self._get_fields(table_name)

        # If no fields are available, return a minimal schema with just id
        if not fields:
            print(f"Warning: No fields available for {table_name}, using minimal schema")
            return StructType([StructField("id", LongType(), False)])

        # Convert Zoho fields to Spark schema
        struct_fields = []
        for field in fields:
            try:
                struct_field = self._zoho_type_to_spark_type(field)
                struct_fields.append(struct_field)
            except Exception as e:
                # Log warning and skip problematic fields
                print(f"Warning: Could not convert field {field.get('api_name')}: {e}")
                continue

        return StructType(struct_fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        """
        Fetch the metadata of a module.

        For Zoho CRM:
        - Primary key is always 'id'
        - Cursor field is 'Modified_Time' for CDC
        - Most modules support CDC ingestion
        """
        # Check if table exists
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' is not supported. " f"Available tables: {', '.join(available_tables)}")

        # Get the schema to check if Modified_Time exists
        schema = self.get_table_schema(table_name, table_options)
        field_names = schema.fieldNames()
        has_modified_time = "Modified_Time" in field_names
        has_id = "id" in field_names

        # Attachments are append-only
        if table_name == "Attachments":
            return {
                "primary_keys": ["id"] if has_id else [],
                "ingestion_type": "append",
            }

        # Analytics and special modules without Modified_Time use snapshot
        if not has_modified_time:
            return {
                "primary_keys": ["id"] if has_id else [],
                "ingestion_type": "snapshot",
            }

        # All other modules support CDC with Modified_Time cursor
        return {
            "primary_keys": ["id"],
            "cursor_field": "Modified_Time",
            "ingestion_type": "cdc",
        }

    def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]) -> (Iterator[dict], dict):
        """
        Read records from a Zoho CRM module.
        Supports incremental reads using Modified_Time cursor and pagination.
        Also fetches deleted records for CDC.
        """
        print(f"[DEBUG] read_table called for '{table_name}'")
        print(f"[DEBUG] start_offset: {start_offset}")
        print(f"[DEBUG] initial_load_start_date: {self.initial_load_start_date}")

        # Check if table exists
        available_tables = self.list_tables()
        if table_name not in available_tables:
            raise ValueError(f"Table '{table_name}' is not supported. " f"Available tables: {', '.join(available_tables)}")

        # Determine the cursor time for incremental reads
        cursor_time = None
        if start_offset and "cursor_time" in start_offset:
            cursor_time = start_offset["cursor_time"]
            print(f"[DEBUG] Using cursor_time from start_offset: {cursor_time}")
        elif self.initial_load_start_date:
            cursor_time = self.initial_load_start_date
            print(f"[DEBUG] Using cursor_time from initial_load_start_date: {cursor_time}")
        else:
            print("[DEBUG] No cursor_time - will fetch all records")

        # Apply a 5-minute lookback window to catch late updates
        if cursor_time:
            cursor_dt = datetime.fromisoformat(cursor_time.replace("Z", "+00:00"))
            lookback_dt = cursor_dt - timedelta(minutes=5)
            cursor_time = lookback_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            print(f"[DEBUG] cursor_time after lookback adjustment: {cursor_time}")

        # Get metadata to determine ingestion type
        metadata = self.read_table_metadata(table_name, table_options)
        ingestion_type = metadata.get("ingestion_type")

        # For snapshot tables, don't use cursor_time (they don't support filtering)
        if ingestion_type == "snapshot":
            cursor_time = None

        # Fetch regular records
        records_iter = self._read_records(table_name, cursor_time)

        # For CDC ingestion, also fetch deleted records
        if ingestion_type == "cdc" and cursor_time:
            # Collect all records first to avoid generator re-entrance issues
            all_records = list(records_iter)
            deleted_records = list(self._read_deleted_records(table_name, cursor_time))

            # Combine all records
            def combined_iter():
                yield from all_records
                yield from deleted_records

            records_iter = combined_iter()

        # Calculate the next offset based on the maximum Modified_Time seen
        max_modified_time = self._current_max_modified_time
        if max_modified_time:
            next_offset = {"cursor_time": max_modified_time}
        else:
            # No records found, keep the same offset
            next_offset = start_offset or {}

        print(f"[DEBUG] read_table returning. next_offset: {next_offset}")
        return records_iter, next_offset

    def _get_json_fields(self, module_name: str) -> set:
        """
        Get field names that should be serialized as JSON strings.
        These are fields with json_type 'jsonobject' or 'jsonarray'.
        """
        cache_key = f"{module_name}_json_fields"
        if cache_key in self._fields_cache:
            return self._fields_cache[cache_key]

        fields = self._get_fields(module_name)
        json_fields = set()
        for field in fields:
            json_type = field.get("json_type")
            if json_type in ("jsonobject", "jsonarray"):
                json_fields.add(field.get("api_name"))

        self._fields_cache[cache_key] = json_fields
        return json_fields

    def _normalize_record(self, record: dict, json_fields: set) -> dict:
        """
        Normalize a record for Spark compatibility.
        Only serializes fields that are declared as JSON strings in schema.
        """
        normalized = {}
        for key, value in record.items():
            if value is None:
                normalized[key] = None
            elif key in json_fields and isinstance(value, (dict, list)):
                # Only serialize fields that are declared as StringType for JSON
                normalized[key] = json.dumps(value)
            else:
                normalized[key] = value
        return normalized

    def _read_records(self, module_name: str, cursor_time: Optional[str] = None) -> Iterator[dict]:
        """
        Read records from a module with pagination.
        """
        print(f"[DEBUG] _read_records called for '{module_name}' with cursor_time: {cursor_time}")
        self._current_max_modified_time = cursor_time

        # Get fields that need JSON serialization
        json_fields = self._get_json_fields(module_name)
        print(f"[DEBUG] JSON fields to serialize: {json_fields}")

        # Get field names for this module (required by Zoho API)
        fields = self._get_fields(module_name)
        field_names = [f["api_name"] for f in fields] if fields else []
        print(f"[DEBUG] Field names for {module_name}: {len(field_names)} fields")

        page = 1
        per_page = 200  # Maximum allowed by Zoho CRM
        total_records_yielded = 0

        while True:
            params = {
                "page": page,
                "per_page": per_page,
                "sort_order": "asc",
                "sort_by": "Modified_Time",
            }

            # Add fields parameter (required by Zoho API despite docs saying optional)
            if field_names:
                params["fields"] = ",".join(field_names)

            # Add incremental filter if cursor_time is provided
            if cursor_time:
                # URL encode the criteria
                criteria = f"(Modified_Time:greater_equal:{cursor_time})"
                params["criteria"] = criteria

            print(f"[DEBUG] API request: GET /crm/v8/{module_name} with params: {params}")

            try:
                response = self._make_request("GET", f"/crm/v8/{module_name}", params=params)
                print(f"[DEBUG] API response keys: {response.keys() if response else 'None'}")
            except Exception as e:
                print(f"[DEBUG] ERROR fetching page {page} for {module_name}: {e}")
                raise

            data = response.get("data", [])
            info = response.get("info", {})
            print(f"[DEBUG] Page {page}: got {len(data)} records, info: {info}")

            # Track the maximum Modified_Time seen
            for record in data:
                modified_time = record.get("Modified_Time")
                if modified_time:
                    if not self._current_max_modified_time or modified_time > self._current_max_modified_time:
                        self._current_max_modified_time = modified_time

                total_records_yielded += 1
                yield self._normalize_record(record, json_fields)

            # Check if there are more pages
            more_records = info.get("more_records", False)
            print(f"[DEBUG] more_records: {more_records}, data empty: {len(data) == 0}")
            if not more_records or not data:
                print(f"[DEBUG] Stopping pagination. Total records yielded: {total_records_yielded}")
                break

            page += 1

    def _read_deleted_records(self, module_name: str, cursor_time: Optional[str] = None) -> Iterator[dict]:
        """
        Read deleted records from a module.
        Returns records marked for deletion with a special field.
        """
        page = 1
        per_page = 200

        while True:
            params = {
                "type": "all",
                "page": page,
                "per_page": per_page,
            }

            try:
                response = self._make_request("GET", f"/crm/v8/{module_name}/deleted", params=params)
            except Exception as e:
                print(f"[DEBUG] ERROR fetching deleted records for {module_name}: {e}")
                raise

            data = response.get("data", [])
            info = response.get("info", {})

            # Filter by cursor_time if provided
            for record in data:
                deleted_time = record.get("deleted_time")

                # Only include records deleted after cursor_time
                if cursor_time and deleted_time:
                    if deleted_time < cursor_time:
                        continue

                # Mark this record as deleted
                record["_zoho_deleted"] = True

                # Update max modified time to deleted_time
                if deleted_time:
                    if not self._current_max_modified_time or deleted_time > self._current_max_modified_time:
                        self._current_max_modified_time = deleted_time

                yield record

            # Check if there are more pages
            more_records = info.get("more_records", False)
            if not more_records or not data:
                break

            page += 1
