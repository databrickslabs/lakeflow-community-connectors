"""Schemas derived from Rootly public OpenAPI; preserve nested JSON:API attributes.

Source: https://rootly-heroku.s3.amazonaws.com/swagger/v1/swagger.json
Input SHA256: f3b255f4b64ad713dac2b04d43803adeea9081ea4dbf9ff2caa5140c8cd93355
Regenerator: tests/unit/sources/rootly/generate_schemas.py
Open-ended objects use VariantType; strings (including dates) remain strings.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    VariantType,
)

INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("description", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("color", StringType(), True),
        StructField("position", LongType(), True),
        StructField("notify_emails", ArrayType(StringType(), True), True),
        StructField(
            "slack_channels",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "slack_aliases",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_SEVERITY_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SCHEMA, True),
    ]
)

INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", VariantType(), True),
        StructField("relationships", VariantType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_SEVERITY_SCHEMA = StructType(
    [
        StructField("data", INCIDENTS_ATTRIBUTES_SEVERITY_DATA_SCHEMA, True),
        StructField(
            "included", ArrayType(INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA, True), True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_PROPERTIES_ITEM_SCHEMA = StructType(
    [
        StructField("catalog_property_id", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("managed_by", StringType(), True),
        StructField("external_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("public_description", StringType(), True),
        StructField("public_name", StringType(), True),
        StructField("notify_emails", ArrayType(StringType(), True), True),
        StructField("color", StringType(), True),
        StructField("position", LongType(), True),
        StructField(
            "slack_channels",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "slack_aliases",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "properties",
            ArrayType(
                INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_PROPERTIES_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField(
            "attributes", INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_SCHEMA, True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_SCHEMA = StructType(
    [
        StructField("data", INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_SCHEMA, True),
        StructField(
            "included", ArrayType(INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA, True), True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_INCIDENT_TYPES_ITEM_DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("description", StringType(), True),
        StructField("public_description", StringType(), True),
        StructField("public_name", StringType(), True),
        StructField("color", StringType(), True),
        StructField("position", LongType(), True),
        StructField("notify_emails", ArrayType(StringType(), True), True),
        StructField(
            "slack_channels",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "slack_aliases",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "properties",
            ArrayType(
                INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_PROPERTIES_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_INCIDENT_TYPES_ITEM_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField(
            "attributes", INCIDENTS_ATTRIBUTES_INCIDENT_TYPES_ITEM_DATA_ATTRIBUTES_SCHEMA, True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_INCIDENT_TYPES_ITEM_SCHEMA = StructType(
    [
        StructField("data", INCIDENTS_ATTRIBUTES_INCIDENT_TYPES_ITEM_DATA_SCHEMA, True),
        StructField(
            "included", ArrayType(INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA, True), True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_SERVICES_ITEM_DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("managed_by", StringType(), True),
        StructField("description", StringType(), True),
        StructField("public_description", StringType(), True),
        StructField("public_name", StringType(), True),
        StructField("notify_emails", ArrayType(StringType(), True), True),
        StructField("color", StringType(), True),
        StructField("position", LongType(), True),
        StructField("backstage_id", StringType(), True),
        StructField("external_id", StringType(), True),
        StructField("pagerduty_id", StringType(), True),
        StructField("opsgenie_id", StringType(), True),
        StructField("cortex_id", StringType(), True),
        StructField("service_now_ci_sys_id", StringType(), True),
        StructField("github_repository_name", StringType(), True),
        StructField("github_repository_branch", StringType(), True),
        StructField("gitlab_repository_name", StringType(), True),
        StructField("gitlab_repository_branch", StringType(), True),
        StructField("kubernetes_deployment_name", StringType(), True),
        StructField("environment_ids", ArrayType(StringType(), True), True),
        StructField("service_ids", ArrayType(StringType(), True), True),
        StructField("owner_group_ids", ArrayType(StringType(), True), True),
        StructField("owner_user_ids", ArrayType(LongType(), True), True),
        StructField("alert_urgency_id", StringType(), True),
        StructField("escalation_policy_id", StringType(), True),
        StructField("alerts_email_enabled", BooleanType(), True),
        StructField("alerts_email_address", StringType(), True),
        StructField(
            "slack_channels",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "slack_aliases",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("alert_broadcast_enabled", BooleanType(), True),
        StructField(
            "alert_broadcast_channel",
            INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA,
            True,
        ),
        StructField("incident_broadcast_enabled", BooleanType(), True),
        StructField(
            "incident_broadcast_channel",
            INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA,
            True,
        ),
        StructField(
            "properties",
            ArrayType(
                INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_PROPERTIES_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_SERVICES_ITEM_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", INCIDENTS_ATTRIBUTES_SERVICES_ITEM_DATA_ATTRIBUTES_SCHEMA, True),
    ]
)

INCIDENTS_ATTRIBUTES_SERVICES_ITEM_SCHEMA = StructType(
    [
        StructField("data", INCIDENTS_ATTRIBUTES_SERVICES_ITEM_DATA_SCHEMA, True),
        StructField(
            "included", ArrayType(INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA, True), True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_FUNCTIONALITIES_ITEM_DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("managed_by", StringType(), True),
        StructField("description", StringType(), True),
        StructField("public_description", StringType(), True),
        StructField("public_name", StringType(), True),
        StructField("notify_emails", ArrayType(StringType(), True), True),
        StructField("color", StringType(), True),
        StructField("backstage_id", StringType(), True),
        StructField("external_id", StringType(), True),
        StructField("pagerduty_id", StringType(), True),
        StructField("opsgenie_id", StringType(), True),
        StructField("opsgenie_team_id", StringType(), True),
        StructField("cortex_id", StringType(), True),
        StructField("service_now_ci_sys_id", StringType(), True),
        StructField("position", LongType(), True),
        StructField("environment_ids", ArrayType(StringType(), True), True),
        StructField("service_ids", ArrayType(StringType(), True), True),
        StructField("owner_group_ids", ArrayType(StringType(), True), True),
        StructField("owner_user_ids", ArrayType(LongType(), True), True),
        StructField("escalation_policy_id", StringType(), True),
        StructField(
            "slack_channels",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "slack_aliases",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "properties",
            ArrayType(
                INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_PROPERTIES_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_FUNCTIONALITIES_ITEM_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField(
            "attributes", INCIDENTS_ATTRIBUTES_FUNCTIONALITIES_ITEM_DATA_ATTRIBUTES_SCHEMA, True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_FUNCTIONALITIES_ITEM_SCHEMA = StructType(
    [
        StructField("data", INCIDENTS_ATTRIBUTES_FUNCTIONALITIES_ITEM_DATA_SCHEMA, True),
        StructField(
            "included", ArrayType(INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA, True), True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_GROUPS_ITEM_DATA_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("managed_by", StringType(), True),
        StructField("description", StringType(), True),
        StructField("public_description", StringType(), True),
        StructField("public_name", StringType(), True),
        StructField("notify_emails", ArrayType(StringType(), True), True),
        StructField("color", StringType(), True),
        StructField("position", LongType(), True),
        StructField("backstage_id", StringType(), True),
        StructField("external_id", StringType(), True),
        StructField("pagerduty_id", StringType(), True),
        StructField("pagerduty_service_id", StringType(), True),
        StructField("opsgenie_id", StringType(), True),
        StructField("victor_ops_id", StringType(), True),
        StructField("pagertree_id", StringType(), True),
        StructField("cortex_id", StringType(), True),
        StructField("service_now_ci_sys_id", StringType(), True),
        StructField("scim_group_id", StringType(), True),
        StructField("user_ids", ArrayType(LongType(), True), True),
        StructField("admin_ids", ArrayType(LongType(), True), True),
        StructField("alerts_email_enabled", BooleanType(), True),
        StructField("alerts_email_address", StringType(), True),
        StructField("alert_urgency_id", StringType(), True),
        StructField(
            "slack_channels",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField(
            "slack_aliases",
            ArrayType(
                INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("alert_broadcast_enabled", BooleanType(), True),
        StructField(
            "alert_broadcast_channel",
            INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA,
            True,
        ),
        StructField("incident_broadcast_enabled", BooleanType(), True),
        StructField(
            "incident_broadcast_channel",
            INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA,
            True,
        ),
        StructField("auto_add_members_when_attached", BooleanType(), True),
        StructField("auto_add_members_scope", StringType(), True),
        StructField("schedule_override_policy", StringType(), True),
        StructField(
            "properties",
            ArrayType(
                INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_DATA_ATTRIBUTES_PROPERTIES_ITEM_SCHEMA, True
            ),
            True,
        ),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_GROUPS_ITEM_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", INCIDENTS_ATTRIBUTES_GROUPS_ITEM_DATA_ATTRIBUTES_SCHEMA, True),
    ]
)

INCIDENTS_ATTRIBUTES_GROUPS_ITEM_SCHEMA = StructType(
    [
        StructField("data", INCIDENTS_ATTRIBUTES_GROUPS_ITEM_DATA_SCHEMA, True),
        StructField(
            "included", ArrayType(INCIDENTS_ATTRIBUTES_SEVERITY_INCLUDED_ITEM_SCHEMA, True), True
        ),
    ]
)

INCIDENTS_ATTRIBUTES_ZOOM_MEETING_GLOBAL_DIAL_IN_NUMBERS_ITEM_SCHEMA = StructType(
    [
        StructField("country", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("number", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

INCIDENTS_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sequential_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("kind", StringType(), True),
        StructField("slug", StringType(), True),
        StructField("parent_incident_id", StringType(), True),
        StructField("duplicate_incident_id", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("private", BooleanType(), True),
        StructField("source", StringType(), True),
        StructField("status", StringType(), True),
        StructField("url", StringType(), True),
        StructField("short_url", StringType(), True),
        StructField("public_title", StringType(), True),
        StructField("user", VariantType(), True),
        StructField("severity", INCIDENTS_ATTRIBUTES_SEVERITY_SCHEMA, True),
        StructField(
            "environments", ArrayType(INCIDENTS_ATTRIBUTES_ENVIRONMENTS_ITEM_SCHEMA, True), True
        ),
        StructField(
            "incident_types", ArrayType(INCIDENTS_ATTRIBUTES_INCIDENT_TYPES_ITEM_SCHEMA, True), True
        ),
        StructField("services", ArrayType(INCIDENTS_ATTRIBUTES_SERVICES_ITEM_SCHEMA, True), True),
        StructField(
            "functionalities",
            ArrayType(INCIDENTS_ATTRIBUTES_FUNCTIONALITIES_ITEM_SCHEMA, True),
            True,
        ),
        StructField("groups", ArrayType(INCIDENTS_ATTRIBUTES_GROUPS_ITEM_SCHEMA, True), True),
        StructField("labels", VariantType(), True),
        StructField("slack_channel_id", StringType(), True),
        StructField("slack_channel_name", StringType(), True),
        StructField("slack_channel_url", StringType(), True),
        StructField("slack_channel_short_url", StringType(), True),
        StructField("slack_channel_deep_link", StringType(), True),
        StructField("slack_channel_archived", BooleanType(), True),
        StructField("slack_last_message_ts", StringType(), True),
        StructField("zoom_meeting_id", StringType(), True),
        StructField("zoom_meeting_start_url", StringType(), True),
        StructField("zoom_meeting_join_url", StringType(), True),
        StructField("zoom_meeting_password", StringType(), True),
        StructField("zoom_meeting_pstn_password", StringType(), True),
        StructField("zoom_meeting_h323_password", StringType(), True),
        StructField(
            "zoom_meeting_global_dial_in_numbers",
            ArrayType(INCIDENTS_ATTRIBUTES_ZOOM_MEETING_GLOBAL_DIAL_IN_NUMBERS_ITEM_SCHEMA, True),
            True,
        ),
        StructField("google_drive_id", StringType(), True),
        StructField("google_drive_parent_id", StringType(), True),
        StructField("google_drive_url", StringType(), True),
        StructField("google_meeting_id", StringType(), True),
        StructField("google_meeting_url", StringType(), True),
        StructField("microsoft_teams_meeting_id", StringType(), True),
        StructField("microsoft_teams_meeting_url", StringType(), True),
        StructField("microsoft_teams_channel_id", StringType(), True),
        StructField("microsoft_teams_channel_name", StringType(), True),
        StructField("microsoft_teams_channel_url", StringType(), True),
        StructField("microsoft_teams_channel_short_url", StringType(), True),
        StructField("microsoft_teams_chat_id", StringType(), True),
        StructField("microsoft_teams_chat_url", StringType(), True),
        StructField("microsoft_teams_team_id", StringType(), True),
        StructField("google_chat_space_id", StringType(), True),
        StructField("google_chat_space_name", StringType(), True),
        StructField("google_chat_space_url", StringType(), True),
        StructField("google_chat_space_short_url", StringType(), True),
        StructField("google_chat_space_archived", BooleanType(), True),
        StructField("google_chat_space_domain_id", StringType(), True),
        StructField("webex_meeting_id", StringType(), True),
        StructField("webex_meeting_url", StringType(), True),
        StructField("jira_issue_key", StringType(), True),
        StructField("jira_issue_id", StringType(), True),
        StructField("jira_issue_url", StringType(), True),
        StructField("github_issue_id", StringType(), True),
        StructField("github_issue_url", StringType(), True),
        StructField("gitlab_issue_id", StringType(), True),
        StructField("gitlab_issue_url", StringType(), True),
        StructField("asana_task_id", StringType(), True),
        StructField("asana_task_url", StringType(), True),
        StructField("linear_issue_key", StringType(), True),
        StructField("linear_issue_id", StringType(), True),
        StructField("linear_issue_url", StringType(), True),
        StructField("trello_card_id", StringType(), True),
        StructField("trello_card_url", StringType(), True),
        StructField("zendesk_ticket_id", StringType(), True),
        StructField("zendesk_ticket_url", StringType(), True),
        StructField("pagerduty_incident_id", StringType(), True),
        StructField("pagerduty_incident_number", StringType(), True),
        StructField("pagerduty_incident_url", StringType(), True),
        StructField("opsgenie_incident_id", StringType(), True),
        StructField("opsgenie_incident_url", StringType(), True),
        StructField("opsgenie_alert_id", StringType(), True),
        StructField("opsgenie_alert_url", StringType(), True),
        StructField("service_now_incident_id", StringType(), True),
        StructField("service_now_incident_key", StringType(), True),
        StructField("service_now_incident_url", StringType(), True),
        StructField("mattermost_channel_id", StringType(), True),
        StructField("mattermost_channel_name", StringType(), True),
        StructField("mattermost_channel_url", StringType(), True),
        StructField("confluence_page_id", StringType(), True),
        StructField("confluence_page_url", StringType(), True),
        StructField("datadog_notebook_id", StringType(), True),
        StructField("datadog_notebook_url", StringType(), True),
        StructField("shortcut_story_id", StringType(), True),
        StructField("shortcut_story_url", StringType(), True),
        StructField("shortcut_task_id", StringType(), True),
        StructField("shortcut_task_url", StringType(), True),
        StructField("motion_task_id", StringType(), True),
        StructField("motion_task_url", StringType(), True),
        StructField("clickup_task_id", StringType(), True),
        StructField("clickup_task_url", StringType(), True),
        StructField("victor_ops_incident_id", StringType(), True),
        StructField("victor_ops_incident_url", StringType(), True),
        StructField("quip_page_id", StringType(), True),
        StructField("quip_page_url", StringType(), True),
        StructField("sharepoint_page_id", StringType(), True),
        StructField("sharepoint_page_url", StringType(), True),
        StructField("airtable_base_key", StringType(), True),
        StructField("airtable_table_name", StringType(), True),
        StructField("airtable_record_id", StringType(), True),
        StructField("airtable_record_url", StringType(), True),
        StructField("freshservice_ticket_id", StringType(), True),
        StructField("freshservice_ticket_url", StringType(), True),
        StructField("freshservice_task_id", StringType(), True),
        StructField("freshservice_task_url", StringType(), True),
        StructField("mitigation_message", StringType(), True),
        StructField("resolution_message", StringType(), True),
        StructField("cancellation_message", StringType(), True),
        StructField("scheduled_for", StringType(), True),
        StructField("scheduled_until", StringType(), True),
        StructField("muted_service_ids", ArrayType(StringType(), True), True),
        StructField("retrospective_progress_status", StringType(), True),
        StructField("in_triage_by", VariantType(), True),
        StructField("started_by", VariantType(), True),
        StructField("mitigated_by", VariantType(), True),
        StructField("resolved_by", VariantType(), True),
        StructField("closed_by", VariantType(), True),
        StructField("cancelled_by", VariantType(), True),
        StructField("in_triage_at", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("detected_at", StringType(), True),
        StructField("acknowledged_at", StringType(), True),
        StructField("mitigated_at", StringType(), True),
        StructField("resolved_at", StringType(), True),
        StructField("closed_at", StringType(), True),
        StructField("cancelled_at", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

INCIDENTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", INCIDENTS_ATTRIBUTES_SCHEMA, True),
    ]
)

USERS_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("full_name_with_team", StringType(), True),
        StructField("time_zone", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("scim_managed", BooleanType(), True),
        StructField("scim_external_id", StringType(), True),
        StructField("joined_via", StringType(), True),
    ]
)

USERS_RELATIONSHIPS_ROLE_DATA_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

USERS_RELATIONSHIPS_ROLE_SCHEMA = StructType(
    [
        StructField("data", USERS_RELATIONSHIPS_ROLE_DATA_SCHEMA, True),
    ]
)

USERS_RELATIONSHIPS_SCHEMA = StructType(
    [
        StructField("role", USERS_RELATIONSHIPS_ROLE_SCHEMA, True),
        StructField("on_call_role", USERS_RELATIONSHIPS_ROLE_SCHEMA, True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", USERS_ATTRIBUTES_SCHEMA, True),
        StructField("relationships", USERS_RELATIONSHIPS_SCHEMA, True),
    ]
)

ESCALATION_POLICIES_ATTRIBUTES_BUSINESS_HOURS_SCHEMA = StructType(
    [
        StructField("time_zone", StringType(), True),
        StructField("days", ArrayType(StringType(), True), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
    ]
)

ESCALATION_POLICIES_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("repeat_count", LongType(), True),
        StructField("created_by_user_id", LongType(), True),
        StructField("last_updated_by_user_id", LongType(), True),
        StructField("group_ids", ArrayType(StringType(), True), True),
        StructField("service_ids", ArrayType(StringType(), True), True),
        StructField("business_hours", ESCALATION_POLICIES_ATTRIBUTES_BUSINESS_HOURS_SCHEMA, True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

ESCALATION_POLICIES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", ESCALATION_POLICIES_ATTRIBUTES_SCHEMA, True),
    ]
)

SCHEDULES_ATTRIBUTES_BUSINESS_HOURS_SCHEMA = StructType(
    [
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("include_weekends", BooleanType(), True),
    ]
)

SCHEDULES_ATTRIBUTES_SCHEMA = StructType(
    [
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("all_time_coverage", BooleanType(), True),
        StructField("time_zone", StringType(), True),
        StructField(
            "slack_user_group",
            INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA,
            True,
        ),
        StructField(
            "slack_channel",
            INCIDENTS_ATTRIBUTES_SEVERITY_DATA_ATTRIBUTES_SLACK_CHANNELS_ITEM_SCHEMA,
            True,
        ),
        StructField("owner_group_ids", ArrayType(StringType(), True), True),
        StructField("owner_user_id", LongType(), True),
        StructField("sync_linear_enabled", BooleanType(), True),
        StructField("include_shadows_in_slack_notifications", BooleanType(), True),
        StructField("shift_start_notifications_enabled", BooleanType(), True),
        StructField("shift_update_notifications_enabled", BooleanType(), True),
        StructField("shift_report_enabled", BooleanType(), True),
        StructField("shift_report_day_of_week", StringType(), True),
        StructField("shift_report_time_of_day", StringType(), True),
        StructField("shift_report_time_zone", StringType(), True),
        StructField("business_hours", SCHEDULES_ATTRIBUTES_BUSINESS_HOURS_SCHEMA, True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

SCHEDULES_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("attributes", SCHEDULES_ATTRIBUTES_SCHEMA, True),
    ]
)

TABLE_SCHEMAS = {
    "incidents": INCIDENTS_SCHEMA,
    "services": INCIDENTS_ATTRIBUTES_SERVICES_ITEM_DATA_SCHEMA,
    "users": USERS_SCHEMA,
    "teams": INCIDENTS_ATTRIBUTES_GROUPS_ITEM_DATA_SCHEMA,
    "severities": INCIDENTS_ATTRIBUTES_SEVERITY_DATA_SCHEMA,
    "escalation_policies": ESCALATION_POLICIES_SCHEMA,
    "schedules": SCHEDULES_SCHEMA,
}
