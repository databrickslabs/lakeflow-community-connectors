# Google Workspace Reports API User Activity Community Connector

This connector ingests administrative activity logs from the **Google Workspace Reports API** into Databricks Delta tables via Lakeflow Connect.

## Overview

Google Workspace Reports API provides comprehensive administrative and user activity events including:
- **Administrative actions**: User management, security policies, device management
- **Login events**: Successful/failed authentication, session details
- **SAML authentication**: SSO authentication events
- **User account changes**: Account lifecycle events
- **Group membership**: Group and organizational unit changes
- **Enterprise groups**: Enterprise-level group management

## Prerequisites

- **Google Workspace Account**: An active Google Workspace domain with administrator access
- **Google Cloud Project**: A project in Google Cloud Console with the **Admin SDK** enabled
- **OAuth Client Credentials**: Web application client (client_id + client_secret)
  - Created in Google Cloud Console → APIs & Services → Credentials
  - Requires OAuth consent screen configuration
- **Required OAuth Scope**: `https://www.googleapis.com/auth/admin.reports.audit.readonly`
- **Network Access**: The cluster must reach `https://www.googleapis.com`
- **Databricks Workspace** with Lakeflow Community Connectors support

## Authentication Model

This connector uses **Unity Catalog COMMUNITY connection** with **OAuth 2.0 `u2m` flow**:

| Component | Responsibility | Details |
|-----------|---|---|
| UC Connection | Owns OAuth dance | Stores client_id/secret, runs authorization flow, refreshes tokens |
| Connector | Runtime only | Receives access_token at query time, treats it as opaque |
| User | Creates connection | Provides client_id/secret, completes in-browser Google consent |

**Key points:**
- The connector does NOT handle OAuth token exchange or refresh
- Token expiry mid-query (401) means the connection was revoked — re-run setup to re-authorize
- No client_id/secret, no refresh_token needed in the connector code

### Optional: Domain-Wide Delegation

For service account deployments with domain-wide delegation enabled, you can optionally impersonate an admin:

```yaml
impersonated_admin_email: admin@yourdomain.com
```

Without this, the connector uses the authorized user's delegated admin privileges.

## Setup

### Step 1 — Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. **Select a project** → **New Project**
3. Name it (e.g., `Google Workspace Lakeflow Connector`)
4. Wait for project creation

### Step 2 — Enable Admin SDK

1. In the project, go to **APIs & Services** → **Library**
2. Search for **"Admin SDK"**
3. Click **Enable**
4. (Optional) Search for and enable **"Google Reports API"** (often enabled with Admin SDK)

### Step 3 — Configure OAuth Consent Screen

1. **APIs & Services** → **OAuth consent screen**
2. **User Type**: Choose **Internal** (if org uses Google Workspace) or **External** (for testing)
3. Fill in **App information**:
   - App name: (e.g., `Lakeflow Connector`)
   - User support email: Your email
4. Fill in **Developer contact information**:
   - Email addresses: Your email
5. Click **Save and Continue**
6. **Scopes** → **Add or Remove Scopes**
   - Add: `https://www.googleapis.com/auth/admin.reports.audit.readonly`
7. Click **Update** → **Save and Continue**
8. Click **Save and Continue** on the test users page (optional for external apps)
9. Review summary and click **Back to Dashboard**

### Step 4 — Create OAuth 2.0 Web Application Credentials

1. **APIs & Services** → **Credentials** → **Create Credentials** → **OAuth client ID**
2. **Application type**: **Web application**
3. **Name**: (e.g., `Lakeflow Connector Web Client`)
4. **Authorized redirect URIs**:
   - Add: `http://localhost`
   - (The CLI picks a free port at runtime; Google requires the host to be registered)
5. Click **Create**
6. **Copy** the following:
   - **Client ID** (e.g., `123456789-abc.apps.googleusercontent.com`)
   - **Client Secret** (e.g., `GOCSPX-xxx...`)
7. **Save securely** — you'll need these in Step 5

### Step 5 — Create the Unity Catalog COMMUNITY Connection

#### Option A — Databricks UI

1. Go to **Catalog** → **Add Data** → **Lakeflow Community Connector**
2. **Source**: Select **google_user_activity**
3. **Connection**: Click **Create a new connection** or select existing
4. **Name**: (e.g., `google_user_activity_connection`)
5. **OAuth setup**: Choose **U2M (User-to-Machine)**
6. **Client ID**: Paste from Step 4
7. **Client Secret**: Paste from Step 4
8. Click **Authenticate** — your browser opens
9. **Sign in** to Google with your workspace admin account
10. **Grant consent** to the `admin.reports.audit.readonly` scope
11. Browser closes, connection is created
12. (Optional) **Impersonated admin email**: Leave blank unless using domain-wide delegation

#### Option B — `community-connector` CLI

```bash
community-connector create-connection \
  --name google_user_activity_connection \
  --source-name google_user_activity \
  --auth-type u2m \
  --options '{
    "client_id": "<YOUR_CLIENT_ID>",
    "client_secret": "<YOUR_CLIENT_SECRET>"
  }'
```

The CLI opens your browser for OAuth consent. After you authorize, the connection is created.

## Supported Objects

The connector reads from Google Workspace Reports API activity endpoints:

| Table | Description | Ingestion Type | Primary Key | Cursor |
|-------|-------------|----------------|-------------|--------|
| `admin` | Administrative actions (user, org, security) | append | `lw_id` | time |
| `login` | Login attempts (successful/failed) | append | `lw_id` | time |
| `saml` | SAML authentication events | append | `lw_id` | time |
| `user_accounts` | User account lifecycle events | append | `lw_id` | time |
| `groups` | Group membership changes | append | `lw_id` | time |
| `groups_enterprise` | Enterprise group management | append | `lw_id` | time |

## Table Configuration

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|--------|----------|-------------|
| `source_table` | Yes | Application name: `admin`, `login`, `saml`, `user_accounts`, `groups`, `groups_enterprise` |
| `destination_catalog` | No | Target catalog (defaults to pipeline default) |
| `destination_schema` | No | Target schema (defaults to pipeline default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Source-Specific Options

These are set in `table_configuration` and must be listed in the connection's `externalOptionsAllowList`:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|----------|
| `user_key` | string | No | User filter (typically `all` for all users) | `all` |
| `start_time` | string | No | ISO 8601 start timestamp | `2026-01-01T00:00:00Z` |
| `end_time` | string | No | ISO 8601 end timestamp | `2026-01-31T23:59:59Z` |
| `max_results` | integer | No | Max records per API request (default: 500, max: 500) | `500` |
| `applications` | string | No | Comma-separated app override | `admin,login,saml` |

### Deduplication

`lw_id` is a deterministic hash derived from:
```
activity.id.time + activity.id.uniqueQualifier + event_name
```

This ensures records are deduplicated when time-window overlaps return previously seen events.

## How to Run

### Step 1: Create an Ingestion Pipeline

1. **Databricks Workspace** → **Jobs & Pipelines** → **Create Ingestion pipeline**
2. **Source**: Select **Custom Connector** → **google_user_activity**
3. **Git Repository**: (Use your forked/cloned Lakeflow repo)
4. **Connection**: Select the connection from Step 5
5. **Ingestion Target**: Choose catalog + schema
6. **Asset root path**: Choose a root path for pipeline assets
7. Click **Create**

### Step 2: Configure Pipeline Spec

Edit the generated `ingest.py` file:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "google_user_activity"

pipeline_spec = {
    "connection_name": "google_user_activity_connection",  # Your UC connection name
    "objects": [
        # Start with login events (simplest to validate)
        {
            "table": {
                "source_table": "login",
                "table_configuration": {
                    "user_key": "all",
                    "start_time": "2026-01-01T00:00:00Z",
                    "end_time": "2026-01-31T23:59:59Z",
                }
            }
        },
        # Admin events
        {
            "table": {
                "source_table": "admin",
            }
        },
        # SAML events
        {
            "table": {
                "source_table": "saml",
            }
        },
    ],
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

### Step 3: Run and Schedule

- **First run**: Ingests historical events (default lookback: 1000 days)
- **Subsequent runs**: Incremental ingestion with overlap window (10 minutes) to prevent data loss
- **Schedule**: On your preferred cadence (hourly for real-time, daily for batch)

## Data Schema

Events are returned as nested JSON structures. Fields vary by application type.

### Common fields across all applications:

- `id.time` — Event timestamp (RFC 3339 format)
- `id.uniqueQualifier` — Unique event identifier
- `actor.email` — User/service that performed the action
- `ipAddress` — Source IP address
- `events` — Array of event details per action

### Example fields per application:

**admin**:
- `events[].name` — Action type (e.g., `CREATE_USER`, `DELETE_USER`)
- `events[].parameters` — Action details (e.g., user email, OU)

**login**:
- `events[].name` — `login_success`, `login_failure`
- `events[].parameters.login_type` — `user`, `admin`, `service_account`

**saml**:
- `events[].name` — SAML-specific events
- `events[].parameters.saml_provider_name` — Provider identifier

## Windowing & Pagination

The connector implements time-window pagination to handle large result sets:

- **Initial lookback**: 1000 days (Phase 2: configurable)
- **Overlap**: 10 minutes (prevents record loss between windows)
- **Guard delay**: 2 minutes (avoids collecting events still arriving)
- **Max per request**: 500 records (Phase 2: configurable)

## Efficiency Features

- **Deterministic deduplication**: `lw_id` hash ensures idempotent ingestion
- **Time-window pagination**: Handles large datasets without memory overhead
- **Configurable filters**: User key, applications, time range per table
- **Rate limit handling**: Backoff on 429/5xx errors (Phase 2)

## Troubleshooting

### Authentication Issues

- **401 Unauthorized**: Token expired or revoked — re-run `create-connection` to re-authorize
- **403 Forbidden**: Missing scope or insufficient permissions
  - Ensure OAuth consent grants `admin.reports.audit.readonly`
  - For Google Workspace organizations, admin may need to approve the app
- **404 Not Found**: Invalid user key or application name
  - Check `user_key` (typically `all`)
  - Check `source_table` is one of the 6 supported applications

### No Data Returned

- Check `start_time` / `end_time` range — events outside this window are not returned
- Verify the authenticated user has audit log visibility in your Google Workspace domain
- Ensure your Google Workspace admin has delegated audit log access to the authenticated user

### Rate Limiting

- Google Reports API has rate limits; the connector retries on 429 with exponential backoff (Phase 2)
- If persistent, reduce `max_results` or widen schedule intervals

## Phase Status

This connector is **Phase 1: Simulate-mode** ready with Phase 2 (live) prepared:

**Phase 1 (Current)**:
- ✅ Full connector spec with OAuth u2m definition
- ✅ Comprehensive README with setup instructions
- ✅ Simulator spec and test scaffold
- ✅ Unit test structure
- ✅ All authentication patterns for COMMUNITY connection

**Phase 2 (After Live Validation)**:
- Google Reports API integration
- Time-window pagination logic
- Event deduplication via `lw_id` hash
- Full event schema discovery
- Rate limiting and retry logic

## References

- **Official Google Reports API**: https://developers.google.com/admin-sdk/reports/reference/rest
- **Activity Resource**: https://developers.google.com/admin-sdk/reports/reference/rest/v1/activities
- **Admin Activity Reference**: https://developers.google.com/admin-sdk/reports/v1/appendix/activity/admin
- **Supported Applications**: https://developers.google.com/admin-sdk/reports/reference/rest/v1/activities/list

## Implementation Notes

- No `from __future__ import annotations` (per PR #173)
- Flat primary keys only (per PR #174)
- Uses `LongType` for numeric fields (not `IntegerType`)
- OAuth handled by UC COMMUNITY connection layer
- Connector receives only `access_token` at runtime

## License

This connector is part of the Databricks Lakeflow Community Connectors project.
