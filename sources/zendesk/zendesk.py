import requests
import base64
from pyspark.sql.types import *
from datetime import datetime
from typing import Dict, List, Iterator


class LakeflowConnect:
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

    def list_tables(self) -> List[str]:
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

    def get_table_schema(self, table_name: str, table_options: Dict[str, str]) -> StructType:
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
                    StructField("satisfaction_rating", MapType(StringType(), StringType())),
                    StructField("sharing_agreement_ids", ArrayType(LongType())),
                    StructField("fields", ArrayType(MapType(StringType(), StringType()))),
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
                    StructField("organization_fields", MapType(StringType(), StringType())),
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
                    StructField("comments", ArrayType(MapType(StringType(), StringType()))),
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

    def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> dict:
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
        # Map table names to their API endpoints and response keys
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

        if config.get("supports_incremental", False):
            return self._read_incremental(table_name, config, start_offset)
        else:
            return self._read_paginated(table_name, config, start_offset)

    def _read_incremental(self, table_name: str, config: dict, start_offset: dict):
        """Read data from incremental API endpoints"""
        start_time = 0
        if start_offset and "start_time" in start_offset:
            start_time = start_offset["start_time"]

        endpoint = config["endpoint"]
        response_key = config["response_key"]

        # Build URL with query parameters
        url = f"{self.base_url}/{endpoint}?start_time={start_time}"
        if "include" in config:
            url += f"&include={config['include']}"

        all_records = []
        next_page = url
        last_time = start_time

        while next_page:
            resp = requests.get(next_page, headers=self.auth_header)
            if resp.status_code != 200:
                raise Exception(
                    f"Zendesk API error for {table_name}: {resp.status_code} {resp.text}"
                )

            data = resp.json()

            # Handle ticket_comments specially
            if table_name == "ticket_comments":
                # Extract comments from ticket events
                ticket_events = data.get("ticket_events", [])
                for event in ticket_events:
                    # Create a record that combines ticket info with comments
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
                                all_records.append(comment_record)
                    # Update last_time
                    try:
                        event_time = int(
                            datetime.strptime(
                                event.get("created_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                            ).timestamp()
                        )
                        if event_time > last_time:
                            last_time = event_time
                    except Exception:
                        pass
            else:
                records = data.get(response_key, [])
                all_records.extend(records)

                # Update last_time based on updated_at field
                for record in records:
                    try:
                        record_time = int(
                            datetime.strptime(
                                record.get("updated_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                            ).timestamp()
                        )
                        if record_time > last_time:
                            last_time = record_time
                    except Exception:
                        pass

            next_page = data.get("next_page")
            end_of_stream = data.get("end_of_stream", True)

            if end_of_stream or not next_page:
                break

        return all_records, {"start_time": last_time}

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
