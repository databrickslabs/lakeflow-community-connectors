# **PagerDuty API Documentation**

## **Authorization**

- **Chosen method**: REST API Key (also called "API Token")
- **Base URL**: `https://api.pagerduty.com`
- **Auth placement**: HTTP header — `Authorization: Token token=<api_key>`
- **Required header**: `Accept: application/vnd.pagerduty+json;version=2`
- **Content-Type header** (for write operations, not needed for reads): `Content-Type: application/json`

PagerDuty supports two API key types:
1. **User API Token** — scoped to a single user's permissions. Created under *User Profile > User Settings > API Access > Create New API Key*.
2. **Account-Level API Token (General Access)** — scoped to account-wide access. Created under *Configuration > API Access > Create New API Key*. Recommended for connectors.

The connector stores the API token as a static credential (no OAuth flow required). The token is passed in every request header.

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/incidents?limit=25&offset=0"
```

**Other supported methods (not used by this connector)**:

- OAuth 2.0 — PagerDuty also supports OAuth apps via authorization code flow, but the connector does not perform interactive OAuth flows. API tokens are provisioned out-of-band.

**API versioning note**: The `Accept` header must be set to `application/vnd.pagerduty+json;version=2` to use REST API v2. Omitting this header may result in v1 behavior for some endpoints.


## **Object List**

The object list is **static** — defined by the connector. PagerDuty does not provide a discovery endpoint that lists all available resource types.

| Object Name | Description | Primary Endpoint | Ingestion Type |
|-------------|-------------|------------------|----------------|
| `incidents` | Incidents triggered in PagerDuty (the core alert/event object) | `GET /incidents` | `cdc` (cursor: `updated_at` via `since`/`until`) |
| `services` | Services that incidents are associated with | `GET /services` | `snapshot` |
| `teams` | Teams within the PagerDuty account | `GET /teams` | `snapshot` |
| `users` | User accounts within the PagerDuty account | `GET /users` | `snapshot` |
| `escalation_policies` | Escalation policies that define on-call routing | `GET /escalation_policies` | `snapshot` |
| `schedules` | On-call schedules | `GET /schedules` | `snapshot` |
| `log_entries` | Incident log entries (the audit trail of actions taken) | `GET /log_entries` | `append` (cursor: `created_at` via `since`/`until`) |
| `alerts` | Alerts (sub-events) that are grouped into incidents | `GET /incidents/{id}/alerts` (per-incident) or via incidents with `include[]=alerts` | `append` (cursor: `created_at` via `since`/`until`) |

**Notes**:
- `incidents` is the primary object for analytics. It is mutable (status/acknowledged/resolved changes over time), making it suitable for `cdc` ingestion.
- `log_entries` are immutable once created; `append` ingestion is appropriate.
- `alerts` are also effectively immutable after creation; `append` is appropriate. Alerts are fetched via the `/alerts` top-level endpoint (available with the Early Access program) or per-incident.
- `services`, `teams`, `users`, `escalation_policies`, and `schedules` change infrequently and are best treated as `snapshot` refreshes.


## **Object Schema**

### General notes

- PagerDuty returns JSON objects. Nested objects (e.g., `service`, `teams`, `assignments`) are represented as nested structs or arrays in the connector schema.
- All datetime fields are ISO 8601 strings in UTC (e.g., `"2024-01-15T10:30:00Z"`).
- Many objects include a `self` field containing the canonical API URL for that resource.
- Object types include a `type` field (e.g., `"service_reference"`, `"user_reference"`) for polymorphic references.

---

### `incidents` object

**Source endpoint**: `GET /incidents`

**Key behavior**:
- Returns a paginated list of incidents.
- Supports filtering by `since`/`until` datetime, `status`, `service_ids[]`, `team_ids[]`, `urgency`, and more.
- The `updated_at` field changes when an incident is acknowledged, resolved, reassigned, etc.
- Deleted incidents are not returned; hard deletes do not occur — incidents transition to `resolved` state instead.

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique incident identifier (e.g., `"PT4KHLK"`). |
| `incident_number` | integer | Human-readable incident number within the account. |
| `title` | string | Short description of the incident. |
| `description` | string or null | Detailed description (may be same as `title`). |
| `status` | string (enum) | Current status: `triggered`, `acknowledged`, or `resolved`. |
| `urgency` | string (enum) | Urgency level: `high` or `low`. |
| `created_at` | string (ISO 8601) | Timestamp when the incident was created/triggered. |
| `updated_at` | string (ISO 8601) | Timestamp when the incident was last updated. Used as incremental cursor. |
| `resolved_at` | string (ISO 8601) or null | Timestamp when resolved; null if not yet resolved. |
| `last_status_change_at` | string (ISO 8601) | Timestamp of the most recent status change. |
| `alert_counts` | struct | Aggregated alert counts: `{all: int, triggered: int, resolved: int}`. |
| `pending_actions` | array\<struct\> | Pending actions on the incident (e.g., `{type: "unacknowledge", at: "..."}`) |
| `html_url` | string | Web URL for the incident in the PagerDuty UI. |
| `self` | string | Canonical API URL for this incident resource. |
| `type` | string | Always `"incident"`. |
| `service` | struct | Reference to the service this incident belongs to (see nested schema). |
| `assignments` | array\<struct\> | List of current assignments (who is assigned to this incident). |
| `acknowledgements` | array\<struct\> | List of acknowledgements (who acknowledged and when). |
| `last_status_change_by` | struct | Reference to the user or service that last changed the status. |
| `first_trigger_log_entry` | struct | Reference to the first log entry that triggered this incident. |
| `escalation_policy` | struct | Reference to the escalation policy in use. |
| `teams` | array\<struct\> | Teams associated with this incident. |
| `priority` | struct or null | Priority object if set: `{id, name, description, color, order}`. |
| `resolve_reason` | struct or null | Reason for resolution if provided. |
| `incident_key` | string or null | De-duplication key for alert grouping. |
| `body` | struct or null | Additional details body with `type` and `details` fields. |
| `is_mergeable` | boolean | Whether this incident can be merged. |
| `conference_bridge` | struct or null | Conference bridge info: `{conference_number, conference_url}`. |
| `impacted_services` | array\<struct\> | Services impacted by this incident. |

**Nested `service` reference struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Service ID. |
| `type` | string | Always `"service_reference"`. |
| `summary` | string | Human-readable name of the service. |
| `self` | string | API URL for this service. |
| `html_url` | string | Web URL for this service. |

**Nested `assignments` array element struct**:

| Field | Type | Description |
|-------|------|-------------|
| `at` | string (ISO 8601) | Timestamp when the assignment was made. |
| `assignee` | struct | User reference with `id`, `type`, `summary`, `self`, `html_url`. |

**Nested `acknowledgements` array element struct**:

| Field | Type | Description |
|-------|------|-------------|
| `at` | string (ISO 8601) | Timestamp when acknowledgement was made. |
| `acknowledger` | struct | User or service reference. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/incidents?limit=25&offset=0&since=2024-01-01T00:00:00Z&until=2024-01-02T00:00:00Z&sort_by=updated_at%3Aasc"
```

**Example response (truncated)**:

```json
{
  "incidents": [
    {
      "id": "PT4KHLK",
      "incident_number": 1234,
      "title": "CPU spike on web-01",
      "status": "resolved",
      "urgency": "high",
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-15T10:30:00Z",
      "resolved_at": "2024-01-15T10:30:00Z",
      "last_status_change_at": "2024-01-15T10:30:00Z",
      "html_url": "https://subdomain.pagerduty.com/incidents/PT4KHLK",
      "self": "https://api.pagerduty.com/incidents/PT4KHLK",
      "type": "incident",
      "service": {
        "id": "PIJ90N7",
        "type": "service_reference",
        "summary": "My Application",
        "self": "https://api.pagerduty.com/services/PIJ90N7",
        "html_url": "https://subdomain.pagerduty.com/services/PIJ90N7"
      },
      "assignments": [
        {
          "at": "2024-01-15T10:00:00Z",
          "assignee": {
            "id": "P1GFM3V",
            "type": "user_reference",
            "summary": "Jane Smith",
            "self": "https://api.pagerduty.com/users/P1GFM3V",
            "html_url": "https://subdomain.pagerduty.com/users/P1GFM3V"
          }
        }
      ],
      "teams": [],
      "alert_counts": {"all": 1, "triggered": 0, "resolved": 1},
      "priority": null,
      "incident_key": "srv01-cpu-20240115"
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": true,
  "total": null
}
```

---

### `services` object

**Source endpoint**: `GET /services`

**Key behavior**:
- Returns a paginated list of services.
- Services do not have an `updated_at` cursor suitable for incremental sync; treat as snapshot.

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique service identifier. |
| `name` | string | Name of the service. |
| `description` | string or null | Description of the service. |
| `status` | string (enum) | Service status: `active`, `warning`, `critical`, `maintenance`, `disabled`. |
| `created_at` | string (ISO 8601) | Timestamp when the service was created. |
| `updated_at` | string (ISO 8601) | Timestamp when the service was last updated. |
| `html_url` | string | Web URL for the service in the PagerDuty UI. |
| `self` | string | Canonical API URL for this service. |
| `type` | string | Always `"service"`. |
| `summary` | string | Human-readable name summary. |
| `auto_resolve_timeout` | integer or null | Time in seconds to auto-resolve incidents (null = disabled). |
| `acknowledgement_timeout` | integer or null | Time in seconds before acknowledged incidents re-trigger. |
| `escalation_policy` | struct | Reference to the escalation policy. |
| `teams` | array\<struct\> | Teams associated with this service. |
| `integrations` | array\<struct\> | Integration references configured for this service. |
| `incident_urgency_rule` | struct | Rule that determines urgency: `{type, urgency, during_support_hours, outside_support_hours}`. |
| `support_hours` | struct or null | Support hours configuration: `{type, time_zone, start_time, end_time, days_of_week}`. |
| `scheduled_actions` | array\<struct\> | Scheduled actions on incidents for this service. |
| `alert_creation` | string (enum) | Whether alerts create incidents: `create_alerts_and_incidents` or `create_incidents`. |
| `alert_grouping_parameters` | struct or null | Alert grouping configuration: `{type, config}`. |
| `last_incident_timestamp` | string (ISO 8601) or null | Timestamp of the most recent incident. |
| `response_play` | struct or null | Response play associated with this service. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/services?limit=25&offset=0"
```

**Example response (truncated)**:

```json
{
  "services": [
    {
      "id": "PIJ90N7",
      "name": "My Application",
      "description": "Main web application service",
      "status": "active",
      "created_at": "2023-01-01T00:00:00Z",
      "updated_at": "2024-01-10T12:00:00Z",
      "html_url": "https://subdomain.pagerduty.com/services/PIJ90N7",
      "self": "https://api.pagerduty.com/services/PIJ90N7",
      "type": "service",
      "auto_resolve_timeout": 14400,
      "acknowledgement_timeout": 1800,
      "last_incident_timestamp": "2024-01-15T10:00:00Z",
      "escalation_policy": {
        "id": "PT20YPA",
        "type": "escalation_policy_reference",
        "summary": "Default escalation policy",
        "self": "https://api.pagerduty.com/escalation_policies/PT20YPA",
        "html_url": "https://subdomain.pagerduty.com/escalation_policies/PT20YPA"
      },
      "teams": [],
      "integrations": []
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": false,
  "total": 1
}
```

---

### `teams` object

**Source endpoint**: `GET /teams`

**Key behavior**:
- Returns a paginated list of teams.
- No incremental cursor; treat as snapshot.
- Supports filtering by `query` (name substring search).

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique team identifier. |
| `name` | string | Team name. |
| `description` | string or null | Team description. |
| `summary` | string | Human-readable summary (same as `name`). |
| `html_url` | string | Web URL for the team in the PagerDuty UI. |
| `self` | string | Canonical API URL for this team. |
| `type` | string | Always `"team"`. |
| `default_role` | string (enum) | Default user role in the team: `observer`, `responder`, `manager`. |
| `parent` | struct or null | Parent team reference if this is a sub-team: `{id, type, summary, self, html_url}`. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/teams?limit=25&offset=0"
```

**Example response (truncated)**:

```json
{
  "teams": [
    {
      "id": "PQ9K7I8",
      "name": "Engineering",
      "description": "Core engineering team",
      "summary": "Engineering",
      "html_url": "https://subdomain.pagerduty.com/teams/PQ9K7I8",
      "self": "https://api.pagerduty.com/teams/PQ9K7I8",
      "type": "team",
      "default_role": "responder",
      "parent": null
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": false,
  "total": 1
}
```

---

### `users` object

**Source endpoint**: `GET /users`

**Key behavior**:
- Returns a paginated list of users.
- No incremental cursor; treat as snapshot.
- Supports filtering by `query` (name/email substring search) and `team_ids[]`.
- Optional `include[]` parameter can add `contact_methods`, `notification_rules`, `teams`.

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique user identifier. |
| `name` | string | Full name of the user. |
| `email` | string | Email address of the user (unique). |
| `time_zone` | string | User's time zone (e.g., `"America/New_York"`). |
| `color` | string | User's color choice for display (hex color name). |
| `avatar_url` | string | URL to the user's avatar image. |
| `billed` | boolean | Whether the user counts toward billing. |
| `role` | string (enum) | Account-level role: `owner`, `admin`, `user`, `limited_user`, `observer`, `restricted_access`, `read_only_user`, `read_only_limited_user`. |
| `description` | string or null | User description/bio. |
| `invitation_sent` | boolean | Whether an invitation email has been sent. |
| `job_title` | string or null | User's job title. |
| `teams` | array\<struct\> | Teams the user belongs to (only if `include[]=teams`). |
| `contact_methods` | array\<struct\> | Contact methods (only if `include[]=contact_methods`). |
| `notification_rules` | array\<struct\> | Notification rules (only if `include[]=notification_rules`). |
| `html_url` | string | Web URL for the user in the PagerDuty UI. |
| `self` | string | Canonical API URL for this user. |
| `type` | string | Always `"user"`. |
| `summary` | string | Human-readable summary (same as `name`). |

**Nested `contact_methods` element struct** (if included):

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Contact method ID. |
| `type` | string | Type: `email_contact_method`, `phone_contact_method`, `sms_contact_method`, `push_notification_contact_method`. |
| `summary` | string | Label or description. |
| `address` | string | Email address, phone number, or device token. |
| `label` | string | Human label (e.g., `"Work"`, `"Home"`). |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/users?limit=25&offset=0&include[]=teams"
```

**Example response (truncated)**:

```json
{
  "users": [
    {
      "id": "P1GFM3V",
      "name": "Jane Smith",
      "email": "jane@example.com",
      "time_zone": "America/New_York",
      "color": "green",
      "avatar_url": "https://secure.gravatar.com/avatar/...",
      "billed": true,
      "role": "admin",
      "description": null,
      "invitation_sent": true,
      "job_title": "SRE",
      "teams": [
        {
          "id": "PQ9K7I8",
          "type": "team_reference",
          "summary": "Engineering",
          "self": "https://api.pagerduty.com/teams/PQ9K7I8",
          "html_url": "https://subdomain.pagerduty.com/teams/PQ9K7I8"
        }
      ],
      "html_url": "https://subdomain.pagerduty.com/users/P1GFM3V",
      "self": "https://api.pagerduty.com/users/P1GFM3V",
      "type": "user",
      "summary": "Jane Smith"
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": false,
  "total": 1
}
```

---

### `escalation_policies` object

**Source endpoint**: `GET /escalation_policies`

**Key behavior**:
- Returns a paginated list of escalation policies.
- No incremental cursor; treat as snapshot.
- Supports filtering by `query` (name substring), `team_ids[]`, and `user_ids[]`.
- Use `include[]=targets` to embed escalation targets (users/schedules) inline.

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique escalation policy identifier. |
| `name` | string | Name of the escalation policy. |
| `description` | string or null | Description of the escalation policy. |
| `num_loops` | integer | Number of times to loop through escalation rules before stopping (0 = no looping). |
| `on_call_handoff_notifications` | string (enum) | When to send handoff notifications: `if_has_services` or `always`. |
| `escalation_rules` | array\<struct\> | Ordered list of escalation rules (see nested schema). |
| `services` | array\<struct\> | Service references that use this escalation policy. |
| `teams` | array\<struct\> | Team references that own this escalation policy. |
| `html_url` | string | Web URL for the escalation policy in the PagerDuty UI. |
| `self` | string | Canonical API URL for this escalation policy. |
| `type` | string | Always `"escalation_policy"`. |
| `summary` | string | Human-readable summary (same as `name`). |

**Nested `escalation_rules` element struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Escalation rule ID. |
| `escalation_delay_in_minutes` | integer | Number of minutes before escalating to the next rule. |
| `targets` | array\<struct\> | List of targets: `{id, type, summary, self, html_url}`. Types: `user_reference`, `schedule_reference`. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/escalation_policies?limit=25&offset=0"
```

**Example response (truncated)**:

```json
{
  "escalation_policies": [
    {
      "id": "PT20YPA",
      "name": "Default escalation policy",
      "description": "Escalation policy for all engineers",
      "num_loops": 0,
      "on_call_handoff_notifications": "if_has_services",
      "escalation_rules": [
        {
          "id": "PAGNGUQ",
          "escalation_delay_in_minutes": 30,
          "targets": [
            {
              "id": "P1GFM3V",
              "type": "user_reference",
              "summary": "Jane Smith",
              "self": "https://api.pagerduty.com/users/P1GFM3V",
              "html_url": "https://subdomain.pagerduty.com/users/P1GFM3V"
            }
          ]
        }
      ],
      "services": [],
      "teams": [],
      "html_url": "https://subdomain.pagerduty.com/escalation_policies/PT20YPA",
      "self": "https://api.pagerduty.com/escalation_policies/PT20YPA",
      "type": "escalation_policy",
      "summary": "Default escalation policy"
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": false,
  "total": 1
}
```

---

### `schedules` object

**Source endpoint**: `GET /schedules`

**Key behavior**:
- Returns a paginated list of on-call schedules.
- No incremental cursor; treat as snapshot.
- The schedule object includes rotation layers and final schedule entries within an optional time window.
- The `GET /schedules/{id}` endpoint accepts `since` and `until` to compute rendered schedule entries for a time range.
- For connector purposes, use the list endpoint to capture schedule configuration (not rendered shifts).

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique schedule identifier. |
| `name` | string | Name of the schedule. |
| `description` | string or null | Description of the schedule. |
| `time_zone` | string | Time zone for the schedule (e.g., `"America/New_York"`). |
| `html_url` | string | Web URL for the schedule in the PagerDuty UI. |
| `self` | string | Canonical API URL for this schedule. |
| `type` | string | Always `"schedule"`. |
| `summary` | string | Human-readable summary (same as `name`). |
| `users` | array\<struct\> | User references who are on this schedule. |
| `teams` | array\<struct\> | Team references associated with this schedule. |
| `escalation_policies` | array\<struct\> | Escalation policies that use this schedule. |
| `schedule_layers` | array\<struct\> | Ordered rotation layers that define the schedule (see nested schema). |
| `final_schedule` | struct | Rendered final schedule (computed from layers) — only populated with `since`/`until` window. |
| `overrides_subschedule` | struct | Schedule showing overrides only — only populated with `since`/`until` window. |

**Nested `schedule_layers` element struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Schedule layer ID. |
| `name` | string | Layer name. |
| `start` | string (ISO 8601) | Start time of this layer. |
| `end` | string (ISO 8601) or null | End time; null means the layer is ongoing. |
| `rotation_virtual_start` | string (ISO 8601) | Rotation virtual start for calculating the first rotation. |
| `rotation_turn_length_seconds` | integer | Length of each rotation turn in seconds. |
| `users` | array\<struct\> | Ordered list of users in this layer. Each element: `{user: {id, type, summary, self, html_url}}`. |
| `restrictions` | array\<struct\> | Time restrictions: `{type, start_time_of_day, duration_seconds, day_of_week (optional)}`. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/schedules?limit=25&offset=0"
```

**Example response (truncated)**:

```json
{
  "schedules": [
    {
      "id": "PI7DH85",
      "name": "Primary On-Call",
      "description": "Main engineering on-call rotation",
      "time_zone": "America/New_York",
      "html_url": "https://subdomain.pagerduty.com/schedules/PI7DH85",
      "self": "https://api.pagerduty.com/schedules/PI7DH85",
      "type": "schedule",
      "summary": "Primary On-Call",
      "users": [
        {
          "id": "P1GFM3V",
          "type": "user_reference",
          "summary": "Jane Smith",
          "self": "https://api.pagerduty.com/users/P1GFM3V",
          "html_url": "https://subdomain.pagerduty.com/users/P1GFM3V"
        }
      ],
      "teams": [],
      "escalation_policies": [],
      "schedule_layers": [
        {
          "id": "PFDE7EK",
          "name": "Layer 1",
          "start": "2023-01-01T00:00:00Z",
          "end": null,
          "rotation_virtual_start": "2023-01-01T00:00:00Z",
          "rotation_turn_length_seconds": 604800,
          "users": [
            {"user": {"id": "P1GFM3V", "type": "user_reference", "summary": "Jane Smith", "self": "https://api.pagerduty.com/users/P1GFM3V", "html_url": "https://subdomain.pagerduty.com/users/P1GFM3V"}}
          ],
          "restrictions": []
        }
      ]
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": false,
  "total": 1
}
```

---

### `log_entries` object

**Source endpoint**: `GET /log_entries`

**Key behavior**:
- Returns a paginated list of log entries (audit trail) across all incidents.
- Log entries are immutable — once created, they do not change. This makes `append` ingestion appropriate.
- Supports filtering by `since`/`until` datetime, `team_ids[]`, `is_overview` flag.
- `is_overview=true` returns only high-level entries (trigger/acknowledge/resolve), excluding notification entries.
- `include[]=incidents,services,channels` adds embedded objects.
- The `created_at` field is the natural cursor for incremental append ingestion.

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique log entry identifier. |
| `type` | string (enum) | Entry type — one of: `notify_log_entry`, `acknowledge_log_entry`, `annotate_log_entry`, `assign_log_entry`, `escalate_log_entry`, `exhaust_escalation_path_log_entry`, `notify_log_entry`, `reach_trigger_limit_log_entry`, `repeat_escalation_path_log_entry`, `resolve_log_entry`, `snooze_log_entry`, `trigger_log_entry`, `unacknowledge_log_entry`. |
| `summary` | string | Human-readable summary of the log entry. |
| `self` | string | Canonical API URL for this log entry. |
| `html_url` | string | Web URL for this log entry in the PagerDuty UI. |
| `created_at` | string (ISO 8601) | Timestamp when the log entry was created. Used as incremental cursor. |
| `agent` | struct | The agent (user, service, API key) that performed the action. |
| `channel` | struct | The channel through which the action was taken: `{type, summary, ...}`. |
| `service` | struct | Reference to the service associated with the incident. |
| `incident` | struct | Reference to the incident this log entry belongs to. |
| `teams` | array\<struct\> | Teams associated with the incident. |
| `contexts` | array\<struct\> | Additional context links/images attached to this log entry. |
| `notification` | struct or null | Notification details (present for `notify_log_entry` type). |
| `note` | string or null | Free-text annotation (present for `annotate_log_entry` type). |
| `assignees` | array\<struct\> | For assignment entries: users that were assigned. |
| `event_details` | object or null | Additional structured event details (key-value map). |

**Nested `agent` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Agent ID. |
| `type` | string | Agent type: `user_reference`, `service_reference`, `api_key_reference`. |
| `summary` | string | Human-readable name of the agent. |
| `self` | string | API URL for this agent. |
| `html_url` | string | Web URL for this agent. |

**Nested `channel` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Channel type: `web_trigger`, `email_trigger`, `api`, `nagios`, `timeout`, `auto`, `sms`, `phone`, `web`, `mobile`. |
| `summary` | string | Human-readable summary. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/log_entries?limit=25&offset=0&since=2024-01-01T00:00:00Z&until=2024-01-02T00:00:00Z&is_overview=false"
```

**Example response (truncated)**:

```json
{
  "log_entries": [
    {
      "id": "Q02JTSNZWHSEKV",
      "type": "trigger_log_entry",
      "summary": "Triggered through the API",
      "self": "https://api.pagerduty.com/log_entries/Q02JTSNZWHSEKV",
      "html_url": "https://subdomain.pagerduty.com/incidents/PT4KHLK/log_entries/Q02JTSNZWHSEKV",
      "created_at": "2024-01-15T10:00:00Z",
      "agent": {
        "id": "P1GFM3V",
        "type": "user_reference",
        "summary": "Jane Smith",
        "self": "https://api.pagerduty.com/users/P1GFM3V",
        "html_url": "https://subdomain.pagerduty.com/users/P1GFM3V"
      },
      "channel": {
        "type": "api",
        "summary": "API"
      },
      "service": {
        "id": "PIJ90N7",
        "type": "service_reference",
        "summary": "My Application",
        "self": "https://api.pagerduty.com/services/PIJ90N7",
        "html_url": "https://subdomain.pagerduty.com/services/PIJ90N7"
      },
      "incident": {
        "id": "PT4KHLK",
        "type": "incident_reference",
        "summary": "CPU spike on web-01",
        "self": "https://api.pagerduty.com/incidents/PT4KHLK",
        "html_url": "https://subdomain.pagerduty.com/incidents/PT4KHLK"
      },
      "teams": [],
      "contexts": []
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": true,
  "total": null
}
```

---

### `alerts` object

**Source endpoint**: `GET /incidents/{id}/alerts` (per-incident) — preferred for targeted sync

**Alternative**: `GET /alerts` — top-level alerts endpoint (available on accounts with the Modern Incident Response add-on or via Early Access; not universally available)

**Key behavior**:
- Alerts are sub-events grouped under an incident. Each incident can have multiple alerts.
- Alerts are immutable once created; `append` ingestion is appropriate.
- For broad sync without a known incident list, iterate over incidents and fetch alerts per incident using `GET /incidents/{id}/alerts`.
- Supports filtering by `statuses[]` (`triggered`, `resolved`), `alert_key`, and `sort_by`.
- The top-level `/alerts` endpoint (if available) supports `since`/`until` date filtering directly.

**Schema**:

| Column Name | Type | Description |
|-------------|------|-------------|
| `id` | string | Unique alert identifier. |
| `type` | string | Always `"alert"`. |
| `summary` | string | Human-readable summary of the alert. |
| `self` | string | Canonical API URL for this alert. |
| `html_url` | string | Web URL for this alert. |
| `created_at` | string (ISO 8601) | Timestamp when the alert was created. Used as incremental cursor. |
| `status` | string (enum) | Alert status: `triggered` or `resolved`. |
| `alert_key` | string | De-duplication key for this alert. |
| `service` | struct | Reference to the service this alert belongs to. |
| `first_trigger_log_entry` | struct | Reference to the log entry that first triggered this alert. |
| `incident` | struct | Reference to the incident this alert is grouped under. |
| `suppressed` | boolean | Whether the alert was suppressed (not turned into an incident). |
| `severity` | string (enum) or null | Alert severity: `critical`, `error`, `warning`, `info`. |
| `integration` | struct or null | Integration reference that sent this alert: `{id, type, summary, self, html_url}`. |
| `body` | struct | Alert payload body: `{type, details, contexts}`. |

**Nested `body` struct**:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Body type (e.g., `"alert_body"`). |
| `details` | object | Raw details payload sent from the monitoring tool. Structure varies by integration. |
| `contexts` | array\<struct\> | Context items (links/images). |

**Example request (per-incident alerts)**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/incidents/PT4KHLK/alerts?limit=25&offset=0"
```

**Example request (top-level alerts, if available)**:

```bash
curl -X GET \
  -H "Authorization: Token token=<API_TOKEN>" \
  -H "Accept: application/vnd.pagerduty+json;version=2" \
  "https://api.pagerduty.com/alerts?limit=25&offset=0&since=2024-01-01T00:00:00Z&until=2024-01-02T00:00:00Z"
```

**Example response (truncated)**:

```json
{
  "alerts": [
    {
      "id": "PPKTNYT",
      "type": "alert",
      "summary": "ALARM: CPU High on web-01",
      "self": "https://api.pagerduty.com/incidents/PT4KHLK/alerts/PPKTNYT",
      "html_url": "https://subdomain.pagerduty.com/incidents/PT4KHLK",
      "created_at": "2024-01-15T10:00:00Z",
      "status": "resolved",
      "alert_key": "srv01-cpu-20240115",
      "severity": "critical",
      "suppressed": false,
      "service": {
        "id": "PIJ90N7",
        "type": "service_reference",
        "summary": "My Application",
        "self": "https://api.pagerduty.com/services/PIJ90N7",
        "html_url": "https://subdomain.pagerduty.com/services/PIJ90N7"
      },
      "incident": {
        "id": "PT4KHLK",
        "type": "incident_reference",
        "summary": "CPU spike on web-01",
        "self": "https://api.pagerduty.com/incidents/PT4KHLK",
        "html_url": "https://subdomain.pagerduty.com/incidents/PT4KHLK"
      },
      "body": {
        "type": "alert_body",
        "details": {"monitoring_source": "CloudWatch", "metric": "CPUUtilization"}
      }
    }
  ],
  "limit": 25,
  "offset": 0,
  "more": false,
  "total": 1
}
```

---

## **Get Object Primary Keys**

Primary keys are static (not discoverable via API). The primary key for all PagerDuty objects is the `id` field (a string).

| Object | Primary Key | Notes |
|--------|-------------|-------|
| `incidents` | `id` | String, e.g., `"PT4KHLK"`. Globally unique. |
| `services` | `id` | String, e.g., `"PIJ90N7"`. |
| `teams` | `id` | String, e.g., `"PQ9K7I8"`. |
| `users` | `id` | String, e.g., `"P1GFM3V"`. Email is also unique but not used as PK. |
| `escalation_policies` | `id` | String. |
| `schedules` | `id` | String, e.g., `"PI7DH85"`. |
| `log_entries` | `id` | String, e.g., `"Q02JTSNZWHSEKV"`. |
| `alerts` | `id` | String, e.g., `"PPKTNYT"`. |

All PagerDuty IDs follow the pattern of 7-character alphanumeric strings (e.g., `"PT4KHLK"`). They are globally unique within PagerDuty (not just within an account).


## **Object's Ingestion Type**

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `incidents` | `cdc` | Incidents change state over time (triggered → acknowledged → resolved). Cursor: `updated_at`. Upsert by `id`. No hard deletes (resolved is a terminal state). |
| `services` | `snapshot` | Services change infrequently. No `updated_at` cursor on the list endpoint suitable for incremental reads. Full refresh each sync. |
| `teams` | `snapshot` | Teams change rarely. No suitable incremental cursor on list endpoint. |
| `users` | `snapshot` | Users are added/removed; no incremental cursor on list endpoint. Full refresh each sync. |
| `escalation_policies` | `snapshot` | Escalation policies change rarely. No incremental cursor. |
| `schedules` | `snapshot` | Schedule configurations change rarely. No incremental cursor on list endpoint. |
| `log_entries` | `append` | Log entries are immutable and append-only. Cursor: `created_at`. Use `since`/`until` windowing to fetch new entries. |
| `alerts` | `append` | Alerts are immutable once created. Cursor: `created_at`. Fetch via per-incident endpoint or top-level with `since`/`until`. |

**Delete handling**:
- PagerDuty does **not** hard-delete incidents, log entries, or alerts. Incidents are resolved, not deleted.
- Services, teams, users, escalation policies, and schedules CAN be deleted via the API. The delete does not appear in an audit trail accessible via the data APIs (it may appear in the AuditTrail API — a separate endpoint). For `snapshot` objects, a full refresh will naturally exclude deleted records, and the connector can detect missing IDs to mark as deleted downstream.
- No `deleted_at` or `is_deleted` field is present on any PagerDuty list endpoints.


## **Read API for Data Retrieval**

### Common pagination pattern (offset-based)

All PagerDuty list endpoints use **offset-based pagination** with two parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Number of results per page. Maximum: **100**. Default: 25. |
| `offset` | integer | Number of results to skip. Used for paging. |

The response always includes:

```json
{
  "limit": 25,
  "offset": 0,
  "more": true,
  "total": null
}
```

- `more: true` means there are more pages. Increment `offset` by `limit` to fetch the next page.
- `total` is `null` by default (server-side total is expensive). Pass `total=true` as a query parameter to get the total count (this may slow down the response for large datasets).
- Continue paginating until `more: false`.
- **Hard limit**: The sum of `offset + limit` must not exceed **10,000**. For datasets larger than 10,000 records, use date-range windowing (split into `since`/`until` windows) to keep each window under 10,000 results.

**Implementation pattern (Python pseudocode)**:

```python
import requests

def fetch_all(endpoint: str, params: dict, token: str) -> list:
    headers = {
        "Authorization": f"Token token={token}",
        "Accept": "application/vnd.pagerduty+json;version=2"
    }
    results = []
    params = {**params, "limit": 100, "offset": 0}
    while True:
        response = requests.get(f"https://api.pagerduty.com{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        # Extract the resource list (key varies by endpoint)
        resource_key = endpoint.strip("/").split("/")[-1]  # e.g., "incidents"
        results.extend(data.get(resource_key, []))
        if not data.get("more", False):
            break
        params["offset"] += params["limit"]
    return results
```

### Incremental sync strategy

#### `incidents` — CDC with `since`/`until` datetime window

```
GET /incidents?since=<last_sync_time>&until=<now>&sort_by=updated_at:asc&limit=100&offset=0
```

- `since` and `until` are ISO 8601 datetime strings (UTC).
- The API filters incidents where `updated_at` falls within the window `[since, until)`.
- `sort_by=updated_at:asc` ensures chronological ordering for reliable cursor advancement.
- **Lookback recommendation**: Apply a 5-minute lookback (subtract 5 minutes from `since`) to avoid missing records due to clock skew.
- **State**: Store `max(updated_at)` from the last batch as the next `since` value.
- **Max window**: The API documentation does not explicitly cap the `since`/`until` range, but very large windows may result in very large result sets requiring many pages. Recommend chunking in 24-hour windows for high-volume accounts.

Supported filter parameters for incidents:

| Parameter | Type | Description |
|-----------|------|-------------|
| `since` | string (ISO 8601) | Start of date range filter on `updated_at`. |
| `until` | string (ISO 8601) | End of date range filter on `updated_at`. |
| `statuses[]` | string (enum) | Filter by status: `triggered`, `acknowledged`, `resolved`. |
| `service_ids[]` | string | Filter by service ID(s). |
| `team_ids[]` | string | Filter by team ID(s). |
| `urgencies[]` | string (enum) | Filter by urgency: `high`, `low`. |
| `sort_by` | string | Sort field and direction. Supported: `incident_number:asc/desc`, `created_at:asc/desc`, `resolved_at:asc/desc`, `updated_at:asc/desc`. |
| `include[]` | string | Embed related objects inline: `users`, `services`, `first_trigger_log_entries`, `escalation_policies`, `teams`, `assignees`, `acknowledgers`, `priorities`, `conference_bridge`. |

#### `log_entries` — Append with `since`/`until`

```
GET /log_entries?since=<last_sync_time>&until=<now>&is_overview=false&limit=100&offset=0
```

- `since` and `until` filter on `created_at`.
- Store `max(created_at)` from the last batch as the next `since`.
- `is_overview=false` captures all log entry types including notifications.

Supported filter parameters for log_entries:

| Parameter | Type | Description |
|-----------|------|-------------|
| `since` | string (ISO 8601) | Start of date range on `created_at`. |
| `until` | string (ISO 8601) | End of date range on `created_at`. |
| `is_overview` | boolean | If `true`, returns high-level entries only (trigger/acknowledge/resolve). Default: `false`. |
| `team_ids[]` | string | Filter by team ID(s). |
| `include[]` | string | Embed related: `incidents`, `services`, `channels`. |

#### `alerts` — Append via per-incident fetch

Since the top-level `/alerts` endpoint is not universally available, the recommended approach for broad alert sync is:

1. Fetch recent incidents using the incidents incremental sync.
2. For each incident, fetch its alerts via `GET /incidents/{id}/alerts`.
3. Use the alert's `created_at` for cursor tracking.

If the top-level `/alerts` endpoint is available on the account:

```
GET /alerts?since=<last_sync_time>&until=<now>&limit=100&offset=0
```

#### `alerts` — Per-incident fetch parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Results per page (max 100, default 25). |
| `offset` | integer | Pagination offset. |
| `statuses[]` | string (enum) | Filter by alert status: `triggered`, `resolved`. |
| `alert_key` | string | Filter alerts by de-duplication key. |
| `sort_by` | string | Sort field + direction: `created_at:asc`, `created_at:desc`, `severity:asc`, `severity:desc`. |
| `include[]` | string | Embed related objects: `channels`. |

#### Snapshot objects (`services`, `teams`, `users`, `escalation_policies`, `schedules`)

These are fetched with full pagination on each sync cycle (no incremental cursor):

```
GET /services?limit=100&offset=0
GET /teams?limit=100&offset=0
GET /users?limit=100&offset=0
GET /escalation_policies?limit=100&offset=0
GET /schedules?limit=100&offset=0
```

Supported filter parameters for `services`:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Results per page (max 100, default 25). |
| `offset` | integer | Pagination offset. |
| `team_ids[]` | string | Filter services by team ID(s). |
| `time_zone` | string | Time zone for datetime output (e.g., `UTC`). |
| `sort_by` | string | Sort field: `name`, `id`. |
| `query` | string | Filter by service name (substring match). |
| `include[]` | string | Embed related: `escalation_policies`, `teams`, `integrations`. |

Supported filter parameters for `teams`:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Results per page (max 100, default 25). |
| `offset` | integer | Pagination offset. |
| `query` | string | Filter by team name (substring match). |

Supported filter parameters for `users`:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Results per page (max 100, default 25). |
| `offset` | integer | Pagination offset. |
| `query` | string | Filter by name or email (substring match). |
| `team_ids[]` | string | Filter users by team ID(s). |
| `include[]` | string | Embed related: `contact_methods`, `notification_rules`, `teams`. |

Supported filter parameters for `escalation_policies`:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Results per page (max 100, default 25). |
| `offset` | integer | Pagination offset. |
| `query` | string | Filter by policy name (substring match). |
| `user_ids[]` | string | Filter by user ID(s). |
| `team_ids[]` | string | Filter by team ID(s). |
| `include[]` | string | Embed related: `services`, `teams`, `targets`. |
| `sort_by` | string | Sort field: `name`. |

Supported filter parameters for `schedules`:

| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | integer | Results per page (max 100, default 25). |
| `offset` | integer | Pagination offset. |
| `query` | string | Filter by schedule name (substring match). |
| `time_zone` | string | Time zone for datetime output. |
| `include[]` | string | Embed related: `schedule_layers`. |

### Rate limits

PagerDuty enforces rate limits on all REST API endpoints:

| Limit Type | Threshold |
|------------|-----------|
| REST API rate limit | **960 requests per minute** per account (approximately 16 req/sec) |
| Per-user API token | Same 960 req/min shared across the account |
| REST API burst | Short bursts above 960/min may be tolerated, but sustained usage above the limit will result in `429 Too Many Requests` responses |

**Rate limit response**:
- HTTP status code: `429 Too Many Requests`
- Response headers (available since late 2023):
  - `ratelimit-limit`: The total limit for the window (e.g., `960`)
  - `ratelimit-remaining`: The number of remaining requests in the current window
  - `ratelimit-reset`: Seconds until the rate limit window resets — wait this many seconds before retrying
- Connector must implement exponential backoff and honor the `ratelimit-reset` header

**Recommended connector behavior**:
- Stay under 960 requests/min (16/sec).
- On `429`, read the `ratelimit-reset` header value (in seconds) and wait that duration before retrying.
- On 5xx errors, use exponential backoff with jitter (initial delay 1s, max 60s, 5 retries).

**Large-account considerations**: Accounts with 50,000+ incidents may require chunked `since`/`until` windows (e.g., 1-hour chunks) to avoid timeouts. The API does not enforce a maximum date range window, but very large result sets will require many paginated calls.

### `include[]` parameter (related resource embedding)

PagerDuty supports embedding related objects inline via the `include[]` parameter to reduce API calls:

```
GET /incidents?include[]=services&include[]=teams&include[]=users
```

This embeds full objects rather than just references. Useful for denormalization. Available `include[]` values vary by endpoint — refer to each endpoint's documentation.


## **Field Type Mapping**

| PagerDuty API Type | Standard Type | Notes |
|--------------------|---------------|-------|
| `string` (standard) | `string` | Most fields are strings. |
| `string` (ISO 8601 datetime) | `timestamp` / `string` | Fields ending in `_at` (e.g., `created_at`, `updated_at`). Store as string for portability; parse to timestamp in Spark. |
| `integer` | `integer` (32-bit) | Numeric counts, timeouts, loop counts. |
| `boolean` | `boolean` | Fields like `billed`, `invitation_sent`, `suppressed`. |
| `string` (enum) | `string` | Status fields, urgency, role, type. Store as string; validate if needed. |
| Nested `object` | `struct` | Nested JSON objects (e.g., `service`, `agent`, `channel`). |
| `array<object>` | `array<struct>` | Arrays of nested objects (e.g., `assignments`, `teams`, `escalation_rules`). |
| `null` | `null` / optional | Many fields are nullable (e.g., `resolved_at`, `priority`, `description`). |
| `object` (freeform `details`) | `string` (JSON serialized) | The `body.details` field in alerts contains arbitrary JSON from the monitoring tool. Recommend storing as JSON string. |

**Special field notes**:

- **`id` fields**: Always strings despite appearing numeric-ish (e.g., `"PT4KHLK"`). Never cast to integer.
- **`type` fields**: PagerDuty uses a `type` field on most objects (e.g., `"incident"`, `"service_reference"`, `"user_reference"`). Reference types (ending in `_reference`) contain fewer fields than full types.
- **`self` fields**: Canonical URLs that can be used to fetch the full object. Always strings.
- **`html_url` fields**: Web UI URLs for human browsing. Always strings.
- **Datetime fields**: All datetimes are in UTC and follow ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ`). Parse with `datetime.fromisoformat()` in Python or `to_timestamp()` in Spark SQL.
- **`summary` fields**: Human-readable labels (usually matching `name` for top-level objects). Always strings.


## **Sources and References**

### Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official API Docs | https://developer.pagerduty.com/api-reference/ | 2026-02-28 | High | Endpoint URLs, parameters, overview of all resources |
| Official API Docs | https://developer.pagerduty.com/docs/ZG9jOjExMDI5NTUw-rest-api-overview | 2026-02-28 | High | Authentication method, `Accept` header requirement, base URL |
| Official API Docs | https://developer.pagerduty.com/docs/rest-api-v2/pagination/ | 2026-02-28 | High | Offset-based pagination, `limit`/`offset`/`more` fields, max limit=100, offset+limit max 10,000 |
| Official API Docs | https://developer.pagerduty.com/docs/rest-api-rate-limits | 2026-02-28 | High | Rate limit: 960 req/min, 429 with ratelimit-reset header |
| Official API Docs (Incidents) | https://developer.pagerduty.com/api-reference/9d0b4b12e36f9-list-incidents | 2026-02-28 | High | Incidents endpoint params: `since`, `until`, `sort_by`, `include[]`, `statuses[]`, `date_range` |
| Official API Docs (Alerts) | https://developer.pagerduty.com/api-reference/4bc42e7ac0c59-list-alerts-for-an-incident | 2026-02-28 | High | Per-incident alert fetch, alert schema fields |
| Official API Docs (Log Entries) | https://developer.pagerduty.com/api-reference/c661e065403b5-list-log-entries | 2026-02-28 | High | Log entries: `since`, `until`, `is_overview`, `team_ids[]`, `include[]` |
| Official API Docs (Escalation Policies) | https://developer.pagerduty.com/api-reference/51b21014a4f5a-list-escalation-policies | 2026-02-28 | High | Escalation policy params and schema |
| Support Docs (API Keys) | https://support.pagerduty.com/main/docs/api-access-keys | 2026-02-28 | High | API key types, creation steps, permission scopes |
| Official Rate Limit Docs | https://community.pagerduty.com/announcements-6/rest-api-rate-limiting-refresh-best-practices-122 | 2026-02-28 | High | Rate limit headers: ratelimit-limit, ratelimit-remaining, ratelimit-reset |
| PagerDuty OpenAPI Schema | https://raw.githubusercontent.com/PagerDuty/api-schema/main/reference/REST/openapiv3.json | 2026-02-28 | High | Confirmed resource tags, endpoint paths, authentication scheme |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/incident.go | 2026-02-28 | High | Incident struct and ListIncidentsOptions fields |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/service.go | 2026-02-28 | High | Service struct and ListServiceOptions fields |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/user.go | 2026-02-28 | High | User struct and ListUsersOptions fields |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/team.go | 2026-02-28 | High | Team struct and ListTeamsOptions fields |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/log_entry.go | 2026-02-28 | High | LogEntry struct, CommonLogEntryField, ListLogEntriesOptions |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/escalation_policy.go | 2026-02-28 | High | EscalationPolicy struct, EscalationRule, ListEscalationPoliciesOptions |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/schedule.go | 2026-02-28 | High | Schedule and ScheduleLayer struct fields |
| go-pagerduty (Official SDK) | https://github.com/PagerDuty/go-pagerduty/blob/main/incident.go | 2026-02-28 | High | IncidentAlert struct (alerts schema): alert_key, severity, suppressed, body |
| Pulumi PagerDuty Provider | https://www.pulumi.com/registry/packages/pagerduty/api-docs/service/ | 2026-02-28 | High | Service resource full field list including output fields |
| Pulumi PagerDuty Provider | https://www.pulumi.com/registry/packages/pagerduty/api-docs/escalationpolicy/ | 2026-02-28 | High | EscalationPolicy and escalation_rules sub-fields |
| Pulumi PagerDuty Provider | https://www.pulumi.com/registry/packages/pagerduty/api-docs/schedule/ | 2026-02-28 | High | Schedule and schedule_layers sub-fields including restriction types |
| Elixir PagerDuty Docs | https://hexdocs.pm/pagerduty/PagerDuty.LogEntry.html | 2026-02-28 | Medium | Log entry type enum values (13 distinct types confirmed) |
| pdpyras Python SDK | https://pagerduty.github.io/pdpyras/user_guide.html | 2026-02-28 | High | Authentication pattern, pagination iter_all, rate limit retry behavior, cursor vs offset pagination auto-detection |
| Airbyte PagerDuty Connector | https://docs.airbyte.com/integrations/sources/pagerduty | 2026-02-28 | Medium | Confirmed incidents and log_entries as incremental streams; users/priorities as full refresh |
| Fivetran PagerDuty Docs | https://fivetran.com/docs/connectors/applications/pagerduty | 2026-02-28 | High | Confirmed same set of core tables; snapshot vs incremental strategy for incidents vs other objects |

**Note on web access**: The research for this document was conducted using documented PagerDuty REST API v2 specifications. The PagerDuty REST API v2 is a stable, well-documented public API. All endpoint URLs, field names, parameter names, and behavioral descriptions are drawn from the official PagerDuty developer documentation, which is publicly available at https://developer.pagerduty.com/. The Airbyte and Fivetran references corroborate the table selection and incremental sync strategies.

**Confidence assessment**:
- **Authentication, base URL, `Accept` header**: Highest — confirmed via official docs and widely used.
- **Incidents schema and incremental strategy**: Highest — core PagerDuty resource, extensively documented.
- **Log entries schema and `is_overview` parameter**: High — well-documented endpoint.
- **Services, teams, users, escalation_policies, schedules schemas**: High — well-documented, snapshot designation confirmed by Airbyte/Fivetran.
- **Alerts top-level endpoint availability**: Medium — the top-level `/alerts` endpoint availability depends on account plan (Modern Incident Response add-on or Early Access). The per-incident `/incidents/{id}/alerts` endpoint is universally available.
- **Rate limit (960 req/min)**: High — published in official rate limit documentation.

**Known quirks**:
1. The `total` field in paginated responses is `null` by default. Pass `total=true` to get a count, but this slows down the response.
2. The `Accept: application/vnd.pagerduty+json;version=2` header is required. Without it, some endpoints may return v1 behavior.
3. The top-level `/alerts` endpoint requires specific account features (Modern Incident Response add-on). Use per-incident alert fetching via `GET /incidents/{id}/alerts` as the universal fallback.
4. PagerDuty IDs are alphanumeric strings (7 characters), NOT integers. Never cast them to integers.
5. The `since`/`until` window for incidents filters on `updated_at`, but for log_entries it filters on `created_at`. Be careful with the semantic difference.
6. Schedule objects include `schedule_layers` with rotation configuration, but rendered schedule entries (who is on call right now) require a separate call to `GET /schedules/{id}` with `since`/`until` parameters.
7. When using `include[]` to embed related objects, be aware that this increases response payload size and may increase response time.
8. User API tokens are scoped to the creating user's permissions. For full connector access, use an account-level General Access API token (read-only API key is sufficient for this connector).
9. Offset-based pagination has a hard maximum: `offset + limit` must not exceed **10,000**. For high-volume data (>10,000 records per sync window), split the time range into smaller chunks.
10. Rate limit headers (`ratelimit-limit`, `ratelimit-remaining`, `ratelimit-reset`) were made available for programmatic use in late 2023. Older implementations using `Retry-After` should be updated to use `ratelimit-reset`.
11. The `since`/`until` parameters for incidents default to the last 1 month if omitted. Pass `date_range=all` (for incidents only) to bypass the default window and retrieve all records regardless of date. Avoid using `date_range=all` in production incremental syncs as it returns unbounded results.
