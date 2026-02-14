import requests
import base64
import json
from pyspark.sql.types import *
from datetime import datetime, timedelta
from typing import Iterator, Any
import time
from databricks.labs.community_connector.interface import LakeflowConnect


class MixpanelLakeflowConnect(LakeflowConnect):
    # Constants
    BATCH_SIZE_DAYS = 7  # Number of days to fetch in a single API call

    def __init__(self, options: dict[str, str]) -> None:
        # Authentication options - support both service account and API secret
        if "username" in options and "secret" in options:
            # Service account authentication
            self.username = options["username"]
            self.secret = options["secret"]
            self.project_id = options.get("project_id")
            auth_str = f"{self.username}:{self.secret}"
            self.auth_header = {
                "Authorization": "Basic " + base64.b64encode(auth_str.encode()).decode(),
                "Content-Type": "application/json",
            }
        elif "api_secret" in options:
            # API secret authentication
            self.api_secret = options["api_secret"]
            self.project_id = options.get("project_id")
            auth_str = f"{self.api_secret}:"
            self.auth_header = {
                "Authorization": "Basic " + base64.b64encode(auth_str.encode()).decode(),
                "Content-Type": "application/json",
            }
            print(self.auth_header)
        else:
            raise ValueError("Authentication credentials required: either (username, secret) or api_secret")
        
        # Configuration options
        self.region = options.get("region", "US")
        self.timezone = options.get("project_timezone", "US/Pacific")
        
        # Historical data settings
        self.historical_days = int(options.get("historical_days", 10))  # Default to 10 days of history
        
        # Set base URLs based on region
        if self.region.upper() == "EU":
            self.base_url = "https://data-eu.mixpanel.com/api/2.0"
            self.cohorts_base_url = "https://eu.mixpanel.com/api"
        else:
            self.base_url = "https://data.mixpanel.com/api/2.0"
            self.cohorts_base_url = "https://mixpanel.com/api"
        
        # Cache for schemas
        self._schema_cache = {}

    def _parse_datetime(self, datetime_str: str) -> datetime:
        """
        Parse datetime string with multiple format support
        Supports: %Y-%m-%dT%H:%M:%S, %Y-%m-%d %H:%M:%S, %Y-%m-%dT%H:%M:%SZ
        """
        # Support multiple datetime formats
        datetime_formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ]

        for fmt in datetime_formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue

        # If no format matches, raise an error with the formats we tried
        raise ValueError(
            f"Unable to parse datetime '{datetime_str}'. "
            f"Supported formats: {', '.join(datetime_formats)}"
        )

    # Standard property keys for different Mixpanel object types
    _EVENT_STANDARD_KEYS = {
        "time", "distinct_id", "$browser", "$browser_version",
        "$city", "$current_url", "$device_id", "$initial_referrer",
        "$initial_referring_domain", "$lib_version", "$mp_api_endpoint",
        "$mp_api_timestamp_ms", "$os", "$region", "$screen_height",
        "$screen_width", "mp_country_code", "mp_lib",
        "mp_processing_time_ms", "mp_sent_by_lib_version"
    }
    
    _ENGAGE_STANDARD_KEYS = {
        "$first_name", "$last_name", "$email", "$created", "$last_seen",
        "$name", "$phone", "$city", "$region", "$country_code",
        "$timezone", "$browser", "$os"
    }

    def _separate_standard_and_custom_properties(
        self, 
        properties: dict, 
        standard_keys: set
    ) -> tuple[dict, dict]:
        """
        Generic function to separate standard properties from custom properties.
        
        Args:
            properties: The properties dict to process
            standard_keys: Set of keys considered "standard" for this object type
            
        Returns:
            Tuple of (standard_props, custom_props)
        """
        standard_props = {}
        custom_props = {}
        
        for key, value in properties.items():
            if key in standard_keys:
                standard_props[key] = value
            else:
                custom_props[key] = value
        
        return standard_props, custom_props

    def _process_event(self, event: dict) -> dict:
        """
        Process event to separate standard properties from custom properties.
        """
        properties = event.get("properties", {})
        
        # Extract $insert_id to top level (not part of standard_props)
        insert_id = properties.get("$insert_id")
        
        standard_props, custom_props = self._separate_standard_and_custom_properties(
            properties, self._EVENT_STANDARD_KEYS
        )
        
        return {
            "event": event.get("event"),
            "$insert_id": insert_id,
            "properties": {
                **standard_props,
                "custom_properties": custom_props
            }
        }

    def _process_engage_profile(self, profile: dict) -> dict:
        """
        Process engage (people) profile to separate standard properties from custom properties.
        """
        properties = profile.get("$properties", {})
        standard_props, custom_props = self._separate_standard_and_custom_properties(
            properties, self._ENGAGE_STANDARD_KEYS
        )
        
        return {
            "$distinct_id": profile.get("$distinct_id"),
            "$properties": {
                **standard_props,
                "custom_properties": custom_props
            }
        }

    def list_tables(self) -> list[str]:
        """
        List available tables/streams in Mixpanel
        """
        return [
            "events",
            "cohorts",
            "cohort_members",
            "engage"
        ]

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        Args:
            table_name: The name of the table to fetch the schema for.
            table_options: A dictionary of options for accessing the table.
        Returns:
            A StructType object representing the schema of the table.
        """
        # Check cache first
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        schemas = {
            "events": StructType([
                StructField("event", StringType()),
                StructField("$insert_id", StringType()),
                StructField("properties", StructType([
                    StructField("time", LongType()),
                    StructField("distinct_id", StringType()),
                    StructField("$browser", StringType()),
                    StructField("$browser_version", LongType()),
                    StructField("$city", StringType()),
                    StructField("$current_url", StringType()),
                    StructField("$device_id", StringType()),
                    StructField("$initial_referrer", StringType()),
                    StructField("$initial_referring_domain", StringType()),
                    StructField("$lib_version", StringType()),
                    StructField("$mp_api_endpoint", StringType()),
                    StructField("$mp_api_timestamp_ms", LongType()),
                    StructField("$os", StringType()),
                    StructField("$region", StringType()),
                    StructField("$screen_height", LongType()),
                    StructField("$screen_width", LongType()),
                    StructField("mp_country_code", StringType()),
                    StructField("mp_lib", StringType()),
                    StructField("mp_processing_time_ms", LongType()),
                    StructField("mp_sent_by_lib_version", StringType()),
                    StructField("custom_properties", MapType(StringType(), StringType())),
                ])),
                StructField("generated_timestamp", LongType()),
            ]),
            "cohorts": StructType([
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("count", LongType()),
                StructField("is_visible", BooleanType()),
                StructField("is_dynamic", BooleanType()),
                StructField("created", StringType()),
                StructField("project_id", LongType()),
                StructField("generated_timestamp", LongType()),
            ]),
            "cohort_members": StructType([
                StructField("cohort_id", LongType()),
                StructField("distinct_id", StringType()),
                StructField("generated_timestamp", LongType()),
            ]),
            "engage": StructType([
                StructField("$distinct_id", StringType()),
                StructField("$properties", StructType([
                    StructField("$first_name", StringType()),
                    StructField("$last_name", StringType()),
                    StructField("$email", StringType()),
                    StructField("$created", StringType()),
                    StructField("$last_seen", StringType()),
                    StructField("$name", StringType()),
                    StructField("$phone", StringType()),
                    StructField("$city", StringType()),
                    StructField("$region", StringType()),
                    StructField("$country_code", StringType()),
                    StructField("$timezone", StringType()),
                    StructField("$browser", StringType()),
                    StructField("$os", StringType()),
                    StructField("custom_properties", MapType(StringType(), StringType())),
                ])),
                StructField("generated_timestamp", LongType()),
            ])
        }
        
        if table_name not in schemas:
            raise ValueError(f"Unknown table: {table_name}")
        
        # Cache the result
        schema = schemas[table_name]
        self._schema_cache[table_name] = schema
        
        return schema

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        Args:
            table_name: The name of the table to fetch the metadata for.
            table_options: A dictionary of options for accessing the table.
        Returns:
            A dictionary containing the metadata of the table. It includes the following keys:
                - primary_keys: List of string names of the primary key columns of the table.
                - cursor_field: The name of the field to use as a cursor for incremental loading.
                - ingestion_type: The type of ingestion to use for the table.
        """
        metadata = {
            "events": {
                "primary_keys": ["$insert_id"],
                "cursor_field": "properties.time",
                "ingestion_type": "cdc"
            },
            "cohorts": {
                "primary_keys": ["id"],
                "cursor_field": None,
                "ingestion_type": "snapshot"
            },
            "cohort_members": {
                "primary_keys": ["cohort_id", "distinct_id"],
                "cursor_field": None,
                "ingestion_type": "snapshot"
            },
            "engage": {
                "primary_keys": ["$distinct_id"],
                "cursor_field": "$properties.$last_seen",
                "ingestion_type": "cdc"
            }
        }
        
        if table_name not in metadata:
            raise ValueError(f"Unknown table: {table_name}")
        
        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read the records of a table and return an iterator of records and an offset.
        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
            table_options: A dictionary of options for accessing the table.
        Returns:
            An iterator of records in JSON format and an offset.
        """
        # Handle None start_offset by providing default empty dict
        if start_offset is None:
            start_offset = {}
            
        if table_name == "events":
            return self._read_events_table(start_offset)
        elif table_name == "cohorts":
            return self._read_cohorts_table(start_offset)
        elif table_name == "cohort_members":
            return self._read_cohort_members_table(start_offset)
        elif table_name == "engage":
            return self._read_engage_table(start_offset)
        else:
            raise ValueError(f"Unknown table: {table_name}")

    def _read_events_table(self, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read ALL events data from start_date to today using multiple 7-day API calls
        """
        # Extract offset information, handle None offset
        start_date = start_offset.get("start_date") if start_offset else None

        if not start_date:
            # For initial snapshot, start from configured historical days ago
            start_date = (datetime.now() - timedelta(days=self.historical_days)).strftime("%Y-%m-%d")

        # End date is today (inclusive)
        today = datetime.now().strftime("%Y-%m-%d")

        # If start_date is ahead of today, set it to today
        if start_date > today:
            print(f"Start date {start_date} is ahead of today {today}, setting start date to today")
            start_date = today

        all_records = []
        current_start = start_date
        total_api_calls = 0

        # Loop through date ranges in 7-day chunks until we reach today
        while current_start <= today:
            # Calculate end date for this chunk
            chunk_end = (datetime.strptime(current_start, "%Y-%m-%d") + timedelta(days=self.BATCH_SIZE_DAYS - 1)).strftime("%Y-%m-%d")
            if chunk_end > today:
                chunk_end = today

            # Build export URL for this date range
            url = f"{self.base_url}/export"
            params = {
                "from_date": current_start,
                "to_date": chunk_end,
            }

            # Only add project_id for service account authentication (username + secret)
            if self.project_id and hasattr(self, 'username') and hasattr(self, 'secret'):
                params["project_id"] = self.project_id

            try:
                print(f"Fetching events from {current_start} to {chunk_end}")

                # Rate limiting: add delay between requests (except first one)
                if total_api_calls > 0:
                    time.sleep(0.34)  # Slightly more than 1/3 second to stay under 3 req/sec

                response = requests.get(url, params=params, headers=self.auth_header, timeout=120)
                response.raise_for_status()
                total_api_calls += 1

                # Mixpanel export returns JSONL format
                response_lines = response.text.strip().split('\n')
                total_lines = len(response_lines)
                non_empty_lines = len([line for line in response_lines if line.strip()])
                chunk_records = 0
                json_errors = 0

                print(f"API Response: {total_lines} total lines, {non_empty_lines} non-empty lines")

                for line_num, line in enumerate(response_lines):
                    if line.strip():
                        try:
                            event = json.loads(line)

                            # Process event to separate standard and custom properties
                            processed_event = self._process_event(event)
                            processed_event["generated_timestamp"] = int(time.time() * 1000)
                            all_records.append(processed_event)
                            chunk_records += 1
                        except json.JSONDecodeError as e:
                            json_errors += 1
                            print(f"JSON decode error on line {line_num + 1}: {e}")
                            print(f"Problematic line (first 100 chars): {line[:100]}")
                            continue

                print(f"Fetched {chunk_records} events from {current_start} to {chunk_end} ({json_errors} JSON errors)")

            except requests.exceptions.RequestException as e:
                print(f"Error fetching events data for {current_start} to {chunk_end}: {e}")
                # For rate limit errors, return what we have so far and continue from where we failed
                if "429" in str(e):
                    print("Rate limit hit - returning partial data")
                    # Return data fetched so far with offset to continue from current_start
                    next_offset = {"start_date": current_start}
                    return iter(all_records), next_offset

            # Move to next chunk
            current_start = (datetime.strptime(chunk_end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        # All data fetched successfully - set up incremental mode for tomorrow
        next_start_date = (datetime.strptime(today, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        next_offset = {"start_date": next_start_date}

        print(f"Total: {total_api_calls} API calls, {len(all_records)} events from {start_date} to {today}")

        # Return the records as an iterator
        def record_iterator():
            for record in all_records:
                yield record

        return record_iterator(), next_offset

    def _read_cohorts_table(self, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read all cohorts data (full refresh/snapshot).
        The Mixpanel API doesn't support incremental filtering, so we fetch all cohorts each time.
        """
        # Use cohorts-specific endpoint
        url = f"{self.cohorts_base_url}/query/cohorts/list"
        records = []

        try:
            response = requests.post(url, headers=self.auth_header, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Handle both response formats: list directly or dict with "cohorts" key
            cohorts = data if isinstance(data, list) else data.get("cohorts", [])

            for cohort in cohorts:
                # Add generated timestamp
                cohort["generated_timestamp"] = int(time.time() * 1000)
                records.append(cohort)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching cohorts data: {e}")
            # Return partial data with current offset
            return iter(records), start_offset if start_offset else {}

        print(f"Fetched {len(records)} cohorts (full refresh)")
        
        # For snapshot tables, return the same offset
        return iter(records), start_offset if start_offset else {}

    def _read_cohort_members_table(self, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read cohort membership data (which users belong to which cohorts).
        This is a snapshot table that fetches all current cohort memberships.
        """
        # First, get all cohorts
        url = f"{self.cohorts_base_url}/query/cohorts/list"
        records = []
        
        try:
            # Fetch all cohorts
            response = requests.post(url, headers=self.auth_header, timeout=30)
            response.raise_for_status()
            cohorts_data = response.json()
            
            # Handle both response formats: list directly or dict with "cohorts" key
            cohorts = cohorts_data if isinstance(cohorts_data, list) else cohorts_data.get("cohorts", [])
            print(f"Found {len(cohorts)} cohorts, fetching members...")
            
            # For each cohort, fetch its members
            for cohort in cohorts:
                cohort_id = cohort.get("id")
                if not cohort_id:
                    continue
                
                # Fetch cohort members using the engage API with cohort filter
                members_url = f"{self.cohorts_base_url}/query/engage"
                params = {"where": f'properties["$cohort"] == "{cohort_id}"'}
                
                # Only add project_id for service account authentication
                if self.project_id and hasattr(self, 'username') and hasattr(self, 'secret'):
                    params["project_id"] = self.project_id
                
                try:
                    members_response = requests.post(
                        members_url, 
                        params=params, 
                        headers=self.auth_header, 
                        timeout=60
                    )
                    members_response.raise_for_status()
                    members_data = members_response.json()
                    
                    # Extract distinct_id from each member
                    for member in members_data.get("results", []):
                        distinct_id = member.get("$distinct_id")
                        if distinct_id:
                            records.append({
                                "cohort_id": cohort_id,
                                "distinct_id": distinct_id,
                                "generated_timestamp": int(time.time() * 1000)
                            })
                    
                    # Rate limiting: small delay between cohort member fetches
                    if len(cohorts) > 1:
                        time.sleep(0.34)
                        
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching members for cohort {cohort_id}: {e}")
                    # Continue to next cohort
                    continue
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching cohorts list: {e}")
            return iter(records), start_offset if start_offset else {}
        
        print(f"Fetched {len(records)} cohort member relationships across {len(cohorts)} cohorts")
        
        # For snapshot tables, return the same offset (no incremental cursor)
        return iter(records), start_offset if start_offset else {}

    def _read_engage_table(self, start_offset: dict) -> (Iterator[dict], dict):
        """
        Read engage (people profiles) data with incremental loading and pagination
        """
        # Use cohorts-specific endpoint structure for engage
        url = f"{self.cohorts_base_url}/query/engage"

        # Extract cursor for incremental sync
        last_seen_cursor = start_offset.get("last_seen") if start_offset else None
        current_page = start_offset.get("page", 0) if start_offset else 0
        session_id = start_offset.get("session_id") if start_offset else None

        # If no cursor, use historical days for initial sync
        if not last_seen_cursor:
            start_time = (datetime.now() - timedelta(days=self.historical_days)).isoformat()
            print(f"Initial engage sync from: {start_time}")
        else:
            start_time = last_seen_cursor
            print(f"Incremental engage sync from: {start_time}")

        records = []
        page = current_page
        current_session_id = session_id
        latest_last_seen = last_seen_cursor

        # Set up parameters similar to other endpoints
        params = {"page": page}
        if current_session_id:
            params["session_id"] = current_session_id
        # Only add project_id for service account authentication (username + secret)
        if self.project_id and hasattr(self, 'username') and hasattr(self, 'secret'):
            params["project_id"] = self.project_id

        while True:
            try:
                response = requests.post(url, params=params, headers=self.auth_header, timeout=30)
                response.raise_for_status()
                data = response.json()

                if not data.get("results"):
                    break

                current_session_id = data.get("session_id")

                for profile in data["results"]:
                    # Apply incremental filtering based on $last_seen (from $properties)
                    profile_props = profile.get("$properties", {})
                    profile_last_seen = profile_props.get("$last_seen")
                    
                    if profile_last_seen:
                        try:
                            parsed_last_seen = self._parse_datetime(profile_last_seen)
                            profile_last_seen_iso = parsed_last_seen.isoformat()

                            # Apply incremental filtering
                            if profile_last_seen_iso >= start_time:
                                # Process profile to separate standard and custom properties
                                processed_profile = self._process_engage_profile(profile)
                                processed_profile["generated_timestamp"] = int(time.time() * 1000)
                                records.append(processed_profile)

                                # Track latest last_seen timestamp for next sync
                                if not latest_last_seen or profile_last_seen_iso > latest_last_seen:
                                    latest_last_seen = profile_last_seen_iso
                        except Exception as e:
                            print(f"Error parsing last_seen timestamp '{profile_last_seen}': {e}")
                            # Include record anyway but don't update cursor
                            processed_profile = self._process_engage_profile(profile)
                            processed_profile["generated_timestamp"] = int(time.time() * 1000)
                            records.append(processed_profile)
                    else:
                        # Include records without last_seen timestamp
                        processed_profile = self._process_engage_profile(profile)
                        processed_profile["generated_timestamp"] = int(time.time() * 1000)
                        records.append(processed_profile)

                page += 1
                params["page"] = page
                if current_session_id:
                    params["session_id"] = current_session_id

                # Check if we have more pages
                if len(data.get("results", [])) < data.get("page_size", 1000):
                    break

            except requests.exceptions.RequestException as e:
                print(f"Error fetching engage data: {e}")
                break

        # Update offset with latest cursor for next incremental sync
        next_offset = {
            "last_seen": latest_last_seen,
            "page": 0,  # Reset page for next sync
            "session_id": None  # Reset session for next sync
        } if latest_last_seen else (start_offset if start_offset else {})

        print(f"Fetched {len(records)} engage records, next sync from: {latest_last_seen}")
        return iter(records), next_offset