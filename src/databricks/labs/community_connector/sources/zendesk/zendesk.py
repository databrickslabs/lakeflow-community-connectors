import base64
import time
from datetime import datetime
from typing import Dict, List, Iterator
import requests
from pyspark.sql.types import *
from databricks.labs.community_connector.interface import LakeflowConnect


class ZendeskLakeflowConnect(LakeflowConnect):
    def __init__(self, options: dict) -> None:
        self.subdomain = options["subdomain"]
        self.email = options["email"]
        self.api_token = options["api_token"]
        self.base_url = f"https://{self.subdomain}.zendesk.com/api/v2"
        user = f"{self.email}/token"
        token = self.api_token
        auth_str = f"{user}:{token}"
        self.auth_header = {
            "Authorization": "Basic " + base64.b64encode(auth_str.encode()).decode(),
            "Content-Type": "application/json",
        }
        self._init_time = int(time.time())
        self._default_max_records_per_batch = 100_000

    def list_tables(self) -> List[str]:
        """Return the list of available Zendesk tables."""
        return [
            "tickets",
            "organizations",
            "articles",
            "brands",
            "groups",
            "ticket_comments",
            "topics",
            "users",
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        schemas = {
            "tickets": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("external_id", StringType()),
                    StructField("via", MapType(StringType(), StringType())),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("type", StringType()),
                    StructField("subject", StringType()),
                    StructField("raw_subject", StringType()),
                    StructField("description", StringType()),
                    StructField("priority", StringType()),
                    StructField("status", StringType()),
                    StructField("recipient", StringType()),
                    StructField("requester_id", LongType()),
                    StructField("submitter_id", LongType()),
                    StructField("assignee_id", LongType()),
                    StructField("organization_id", LongType()),
                    StructField("group_id", LongType()),
                    StructField("collaborator_ids", ArrayType(LongType())),
                    StructField("follower_ids", ArrayType(LongType())),
                    StructField("email_cc_ids", ArrayType(LongType())),
                    StructField("forum_topic_id", LongType()),
                    StructField("problem_id", LongType()),
                    StructField("has_incidents", BooleanType()),
                    StructField("is_public", BooleanType()),
                    StructField("due_at", StringType()),
                    StructField("tags", ArrayType(StringType())),
                    StructField(
                        "custom_fields",
                        ArrayType(MapType(StringType(), StringType())),
                    ),
                    StructField(
                        "satisfaction_rating", MapType(StringType(), StringType())
                    ),
                    StructField("sharing_agreement_ids", ArrayType(LongType())),
                    StructField(
                        "fields", ArrayType(MapType(StringType(), StringType()))
                    ),
                    StructField("followup_ids", ArrayType(LongType())),
                    StructField("ticket_form_id", LongType()),
                    StructField("brand_id", LongType()),
                    StructField("allow_channelback", BooleanType()),
                    StructField("allow_attachments", BooleanType()),
                    StructField("from_messaging_channel", BooleanType()),
                    StructField("generated_timestamp", LongType()),
                ]
            ),
            "organizations": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("external_id", StringType()),
                    StructField("name", StringType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("domain_names", ArrayType(StringType())),
                    StructField("details", StringType()),
                    StructField("notes", StringType()),
                    StructField("group_id", LongType()),
                    StructField("shared_tickets", BooleanType()),
                    StructField("shared_comments", BooleanType()),
                    StructField("tags", ArrayType(StringType())),
                    StructField(
                        "organization_fields", MapType(StringType(), StringType())
                    ),
                ]
            ),
            "articles": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("html_url", StringType()),
                    StructField("author_id", LongType()),
                    StructField("comments_disabled", BooleanType()),
                    StructField("draft", BooleanType()),
                    StructField("promoted", BooleanType()),
                    StructField("position", LongType()),
                    StructField("vote_sum", LongType()),
                    StructField("vote_count", LongType()),
                    StructField("section_id", LongType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("name", StringType()),
                    StructField("title", StringType()),
                    StructField("source_locale", StringType()),
                    StructField("locale", StringType()),
                    StructField("outdated", BooleanType()),
                    StructField("outdated_locales", ArrayType(StringType())),
                    StructField("edited_at", StringType()),
                    StructField("user_segment_id", LongType()),
                    StructField("permission_group_id", LongType()),
                    StructField("content_tag_ids", ArrayType(LongType())),
                    StructField("label_names", ArrayType(StringType())),
                    StructField("body", StringType()),
                ]
            ),
            "brands": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("name", StringType()),
                    StructField("brand_url", StringType()),
                    StructField("subdomain", StringType()),
                    StructField("host_mapping", StringType()),
                    StructField("has_help_center", BooleanType()),
                    StructField("help_center_state", StringType()),
                    StructField("active", BooleanType()),
                    StructField("default", BooleanType()),
                    StructField("is_deleted", BooleanType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("ticket_form_ids", ArrayType(LongType())),
                    StructField("signature_template", StringType()),
                ]
            ),
            "groups": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("name", StringType()),
                    StructField("deleted", BooleanType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("description", StringType()),
                    StructField("default", BooleanType()),
                    StructField("is_public", BooleanType()),
                ]
            ),
            "ticket_comments": StructType(
                [
                    StructField("id", LongType()),
                    StructField("type", StringType()),
                    StructField("request_id", LongType()),
                    StructField("requester_id", LongType()),
                    StructField("status", StringType()),
                    StructField("subject", StringType()),
                    StructField("priority", StringType()),
                    StructField("organization_id", LongType()),
                    StructField("description", StringType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("assignee_id", LongType()),
                    StructField("group_id", LongType()),
                    StructField("collaborator_ids", ArrayType(LongType())),
                    StructField(
                        "custom_fields",
                        ArrayType(MapType(StringType(), StringType())),
                    ),
                    StructField("email_cc_ids", ArrayType(LongType())),
                    StructField("follower_ids", ArrayType(LongType())),
                    StructField("ticket_form_id", LongType()),
                    StructField("brand_id", LongType()),
                    StructField(
                        "comments", ArrayType(MapType(StringType(), StringType()))
                    ),
                ]
            ),
            "topics": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("html_url", StringType()),
                    StructField("name", StringType()),
                    StructField("description", StringType()),
                    StructField("position", LongType()),
                    StructField("follower_count", LongType()),
                    StructField("community_id", LongType()),
                    StructField("created_at", StringType()),
                    StructField("updated_at", StringType()),
                    StructField("user_segment_id", LongType()),
                    StructField("manageable_by", StringType()),
                    StructField("user_segment_ids", ArrayType(LongType())),
                ]
            ),
            "users": StructType(
                [
                    StructField("id", LongType()),
                    StructField("url", StringType()),
                    StructField("email", StringType()),
                    StructField("name", StringType()),
                    StructField("active", BooleanType()),
                    StructField("alias", StringType()),
                    StructField("created_at", StringType()),
                    StructField("custom_role_id", LongType()),
                    StructField("default_group_id", LongType()),
                    StructField("details", StringType()),
                    StructField("external_id", StringType()),
                    StructField("iana_time_zone", StringType()),
                    StructField("last_login_at", StringType()),
                    StructField("locale", StringType()),
                    StructField("locale_id", LongType()),
                    StructField("moderator", BooleanType()),
                    StructField("notes", StringType()),
                    StructField("only_private_comments", BooleanType()),
                    StructField("organization_id", LongType()),
                    StructField("phone", StringType()),
                    StructField("photo", MapType(StringType(), StringType())),
                    StructField("report_csv", BooleanType()),
                    StructField("restricted_agent", BooleanType()),
                    StructField("role", StringType()),
                    StructField("role_type", LongType()),
                    StructField("shared", BooleanType()),
                    StructField("shared_agent", BooleanType()),
                    StructField("shared_phone_number", BooleanType()),
                    StructField("signature", StringType()),
                    StructField("suspended", BooleanType()),
                    StructField("tags", ArrayType(StringType())),
                    StructField("ticket_restriction", StringType()),
                    StructField("time_zone", StringType()),
                    StructField("two_factor_auth_enabled", BooleanType()),
                    StructField("updated_at", StringType()),
                    StructField("user_fields", MapType(StringType(), StringType())),
                    StructField("verified", BooleanType()),
                ]
            ),
        }

        if table_name not in schemas:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return schemas[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        """
        metadata = {
            "tickets": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "organizations": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "articles": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "brands": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "groups": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "ticket_comments": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "topics": {"primary_keys": ["id"], "cursor_field": "updated_at"},
            "users": {"primary_keys": ["id"], "cursor_field": "updated_at"},
        }

        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return metadata[table_name]

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """Read data from the specified Zendesk table with incremental support.

        Each incremental call fetches up to ``max_records_per_batch`` records
        (default 100 000).  The cursor is capped at ``_init_time`` to
        guarantee termination for ``availableNow`` triggers.
        """
        api_config = {
            "tickets": {
                "endpoint": "incremental/tickets.json",
                "response_key": "tickets",
                "supports_incremental": True,
            },
            "organizations": {
                "endpoint": "incremental/organizations.json",
                "response_key": "organizations",
                "supports_incremental": True,
            },
            "articles": {
                "endpoint": "help_center/articles.json",
                "response_key": "articles",
                "supports_incremental": False,
                "supports_pagination": True,
            },
            "brands": {
                "endpoint": "brands.json",
                "response_key": "brands",
                "supports_incremental": False,
                "supports_pagination": True,
            },
            "groups": {
                "endpoint": "groups.json",
                "response_key": "groups",
                "supports_incremental": False,
                "supports_pagination": True,
            },
            "ticket_comments": {
                "endpoint": "incremental/ticket_events.json",
                "response_key": "ticket_events",
                "supports_incremental": True,
                "include": "comment_events",
            },
            "topics": {
                "endpoint": "community/topics.json",
                "response_key": "topics",
                "supports_incremental": False,
                "supports_pagination": True,
            },
            "users": {
                "endpoint": "incremental/users.json",
                "response_key": "users",
                "supports_incremental": True,
            },
        }

        if table_name not in api_config:
            raise ValueError(f"Table '{table_name}' is not supported.")

        config = api_config[table_name]
        max_records = int(
            table_options.get(
                "max_records_per_batch", self._default_max_records_per_batch
            )
        )

        if config.get("supports_incremental", False):
            return self._read_incremental(
                table_name, config, start_offset, max_records
            )
        else:
            return self._read_paginated(table_name, config, start_offset)

    def _parse_timestamp(self, timestamp_str: str) -> int:
        """Parse a Zendesk timestamp string to Unix timestamp."""
        try:
            # pylint: disable=no-member
            return int(datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").timestamp())
        except (ValueError, TypeError):
            return 0

    def _extract_ticket_comments(self, data: dict) -> tuple:
        """Extract comment records from ticket events data."""
        records = []
        max_time = 0
        for event in data.get("ticket_events", []):
            if "child_events" in event:
                for child in event["child_events"]:
                    if child.get("event_type") == "Comment":
                        comment_record = {
                            "id": event.get("id"),
                            "ticket_id": event.get("ticket_id"),
                            "created_at": event.get("created_at"),
                            "updated_at": event.get("updated_at"),
                            **child,
                        }
                        records.append(comment_record)
            event_time = self._parse_timestamp(event.get("created_at", ""))
            max_time = max(max_time, event_time)
        return records, max_time

    def _extract_records_with_time(self, data: dict, response_key: str) -> tuple:
        """Extract records and find the max updated_at time."""
        records = data.get(response_key, [])
        max_time = 0
        for record in records:
            record_time = self._parse_timestamp(record.get("updated_at", ""))
            max_time = max(max_time, record_time)
        return records, max_time

    def _read_incremental(
        self,
        table_name: str,
        config: dict,
        start_offset: dict,
        max_records: int,
    ):
        """Read data from incremental API endpoints with bounded batch size.

        Uses Zendesk's ``end_time`` response field to resume pagination
        across microbatches.  The cursor is capped at ``_init_time`` so
        that a single trigger run only drains data that existed when the
        connector was instantiated.
        """
        start_time = start_offset.get("start_time", 0) if start_offset else 0
        resume_after = start_offset.get("resume_after") if start_offset else None
        endpoint = config["endpoint"]

        api_cursor = resume_after if resume_after is not None else start_time

        all_records = []
        last_time = start_time
        reached_end = False

        while len(all_records) < max_records:
            url = f"{self.base_url}/{endpoint}?start_time={api_cursor}"
            if "include" in config:
                url += f"&include={config['include']}"

            resp = requests.get(url, headers=self.auth_header)
            if resp.status_code != 200:
                raise Exception(
                    f"Zendesk API error for {table_name}: {resp.status_code} {resp.text}"
                )

            data = resp.json()

            if table_name == "ticket_comments":
                records, max_time = self._extract_ticket_comments(data)
            else:
                records, max_time = self._extract_records_with_time(
                    data, config["response_key"]
                )

            all_records.extend(records)
            last_time = max(last_time, max_time)

            end_time = data.get("end_time")
            if data.get("end_of_stream", True) or not end_time or end_time == api_cursor:
                reached_end = True
                break

            api_cursor = end_time

        if not all_records:
            return [], start_offset or {}

        if not reached_end:
            next_offset = {
                "start_time": last_time,
                "resume_after": api_cursor,
            }
        else:
            capped = min(last_time, self._init_time)
            next_offset = {"start_time": capped}

        return all_records, next_offset

    def _read_paginated(self, table_name: str, config: dict, start_offset: dict):
        """Read data from paginated API endpoints"""
        endpoint = config["endpoint"]
        response_key = config["response_key"]

        # For paginated endpoints, use page number from offset
        page = 1
        if start_offset and "page" in start_offset:
            page = start_offset["page"]

        url = f"{self.base_url}/{endpoint}?page={page}&per_page=100"

        all_records = []
        current_page = page

        while True:
            current_url = f"{self.base_url}/{endpoint}?page={current_page}&per_page=100"
            resp = requests.get(current_url, headers=self.auth_header)

            if resp.status_code != 200:
                # Some endpoints might return 404 when no more pages
                if resp.status_code == 404:
                    break
                raise Exception(
                    f"Zendesk API error for {table_name}: {resp.status_code} {resp.text}"
                )

            data = resp.json()
            records = data.get(response_key, [])

            if not records:
                break

            all_records.extend(records)

            # Check if there's a next page
            next_page = data.get("next_page")
            if not next_page:
                break

            current_page += 1

            # Optional: Add a reasonable limit to prevent infinite loops
            if current_page > 1000:  # Adjust as needed
                break

        return all_records, {"page": current_page}
