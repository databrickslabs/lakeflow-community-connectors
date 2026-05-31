---
name: gmail-connector
description: Tool layer for ad-hoc Gmail reads against the project's `lakeflow_connect` table surface. Runs read queries on a fixed Databricks cluster via the Command Execution REST API; scheduled-ingestion requests hand off to `deploy-connector`. Use whenever the user asks the agent to read something from their mailbox.
args:
  - name: connection_name
    description: Name of the UC COMMUNITY connection holding the Gmail OAuth grant. Create one (see "Connection prerequisite") if absent.
    required: false
---

# Gmail Connector

This skill turns the project's Gmail tables into a tool surface the agent can call. You — the agent — translate the user's request into table reads with optional filter options, run them on a Databricks cluster via the Command Execution REST API, and return the answer.

You **never** invent table names, option keys, or REST paths. The connector README is the source of truth for the tables; the path table below is the source of truth for the cluster API.

---

## Cluster

This skill runs every Gmail snippet on the fixed cluster:

```
0528-081139-nh2l2jnu
```

Use that id verbatim in every Command Execution request. **Do not ask the user for a cluster id** and do not substitute a different one — the connector wheel + Python environment this skill assumes are only known to be present on that cluster. If a request to it fails with a state other than `RUNNING`, ask the user before starting it; do not fall back to another cluster.

---

## Critical: Command Execution REST paths

This is the single most common cause of `Bad Target` / 404 in this skill. Two URL trees, both under `/api/1.2/`, and they are *not* nested — `contexts` and `commands` are siblings, not parent/child.

| Action | Method | Path |
|---|---|---|
| Open a Python context | `POST` | `/api/1.2/contexts/create` |
| Check context status | `GET`  | `/api/1.2/contexts/status` |
| Tear context down | `POST` | `/api/1.2/contexts/destroy` |
| Submit a command | `POST` | `/api/1.2/commands/execute` |
| Poll command status | `GET`  | `/api/1.2/commands/status` |
| Cancel a command | `POST` | `/api/1.2/commands/cancel` |

Paths that **look** right but return errors:

| Wrong path | What happens | What to use instead |
|---|---|---|
| `/api/1.2/commands/contexts/create` | `{"error":"Bad Target: /api/1.2/commands/contexts/create"}` | `/api/1.2/contexts/create` |
| `/api/1.2/commands/contexts/destroy` | Same `Bad Target` | `/api/1.2/contexts/destroy` |
| `/api/2.0/commands/...` (any) | 404 on most workspaces | The corresponding `/api/1.2/...` path |

The Databricks Go SDK and the docs.databricks.com pages reference a `2.0` alias for Command Execution. It isn't live on the workspaces we run against — don't trust it. The clusters API is a separate, unrelated surface at `/api/2.1/clusters/...`; the version number there is correct.

---

## What this skill is for

The Gmail connector exposes Gmail as **Spark tables** — one table per Gmail object (`messages`, `threads`, `labels`, `drafts`, `profile`, `settings`, `filters`, `forwarding_addresses`, `send_as`, `delegates`). Each accepts a small set of filter options (`q`, `labelIds`, `maxResults`, `includeSpamTrash`, `format`). Your tool surface is exactly those tables and options — nothing more.

Read the connector README before planning a request; it is the authoritative manifest:

- `src/databricks/labs/community_connector/sources/gmail/README.md`

It enumerates each table, its primary key, ingestion type, schema highlights, and the keys accepted under `table_configuration`.

The one explicit carve-out, owned by a sibling skill:

- **Scheduled / continuous ingestion** ("land my inbox in UC hourly") → hand off to the `deploy-connector` skill with `source_name=gmail`. That skill provisions an SDP pipeline via the `community-connector` CLI. Don't loop the read envelope here to fake it.

> **Today's limit.** The connector source contains an agent-dispatcher with richer ops (`list_operations`, `search_messages`, `list_attachments`, `download_attachment`, …), but that surface is **not yet on the Spark format dispatch path** in the deployed builds — i.e. setting `option("operation", ...)` does nothing. Until that wiring ships, ignore those ops; the table read path is the only surface you can call. **Attachment-byte downloads in particular are out of reach via this skill today** — the table surface returns attachment *metadata* on `messages.payload`, not bytes.

If the user asks for something the table surface can't do (write actions, byte-level attachment fetch, push notifications), say so plainly.

---

## How you call into the connector

Every read is a Python snippet executed in a long-lived context on the fixed cluster (`0528-081139-nh2l2jnu`):

```python
df = (spark.read.format("lakeflow_connect")
        .option("connection_name", "<CONN>")
        .option("tableName", "<TABLE_NAME>")
        .option("<filter_key>", "<value>")    # 0+ from README's table_configuration
        .load()
        .limit(<N>))                          # always cap rows you don't need
print(json.dumps([r.asDict(recursive=True) for r in df.collect()], default=str))
```

### Bootstrap (do once at the start of a Gmail-shaped conversation)

1. **Confirm the cluster** is `RUNNING`:
   ```bash
   databricks api get /api/2.1/clusters/get \
     --json "{\"cluster_id\":\"0528-081139-nh2l2jnu\"}" -o json | jq '{state, spark_version}'
   ```
   If terminated, ask the user before starting it.

2. **Open a Python context.** Path: `/api/1.2/contexts/create` (not `/api/1.2/commands/contexts/create` — see the path table above):
   ```bash
   databricks api post /api/1.2/contexts/create \
     --json '{"clusterId":"0528-081139-nh2l2jnu","language":"python"}' -o json
   ```
   Returns `{"id": "<CONTEXT_ID>"}`. Remember it for the rest of the session.

3. **Register the data source on the first command.** Spark's data-source registry is per-driver-session, so the *first* command you ship into a fresh context **must** lead with:
   ```python
   import json
   from databricks.labs.community_connector.sources.gmail import GmailDataSource
   spark.dataSource.register(GmailDataSource)
   ```
   Subsequent commands in the same context **must not** re-emit those lines — the registration is cached. Re-registering is wasted overhead, not an error. If you open a new context later (cluster restart, you destroyed the previous one), the first command in *that* context must include them again.

### Execution envelope (every read)

1. **Build the snippet.** End it with `print(json.dumps([r.asDict(recursive=True) for r in df.collect()], default=str))` so the result comes back parseable.

2. **Submit** (path: `/api/1.2/commands/execute`):
   ```bash
   databricks api post /api/1.2/commands/execute \
     --json '{"clusterId":"0528-081139-nh2l2jnu","contextId":"<CTX>","language":"python","command":"<SNIPPET>"}' \
     -o json
   ```

3. **Poll** until terminal (path: `/api/1.2/commands/status`):
   ```bash
   databricks api get /api/1.2/commands/status \
     --json "{\"clusterId\":\"0528-081139-nh2l2jnu\",\"contextId\":\"<CTX>\",\"commandId\":\"<CMD>\"}" -o json
   ```
   `status` walks `Queued → Running → Finished | Error | Cancelled`.

4. **Read the result:**
   - `Finished`, `results.resultType == "text"` → `results.data` is the JSON you printed. Parse and use.
   - `Error` → `results.summary` is the one-liner; `results.cause` is the Python traceback from the cluster.

Quoting notes that bite if you skip them:
- The `command` field is a JSON string containing Python source — preserve newlines (`\n`) and quote your inner strings.
- Spark's `CaseInsensitiveStringMap` lowercases option keys, but the connector handles the standard spellings; use the keys the README documents.

### Teardown

Free the context when you're done. Path: `/api/1.2/contexts/destroy`:

```bash
databricks api post /api/1.2/contexts/destroy \
  --json '{"clusterId":"0528-081139-nh2l2jnu","contextId":"<CTX>"}'
```

Contexts are a cluster resource — don't leak them across sessions.

---

## Composing reads to satisfy a request

Plan a request as one or more table reads with `table_configuration` filters from the README. Examples for orientation only — the README is authoritative for what each table accepts:

- **"Find emails from Alice last week"** → read `messages` with `q="from:alice@example.com newer_than:7d"`; cap with `.limit(N)`.
- **"What labels do I have?"** → read `labels`, no filters.
- **"50 unread inbox messages, headers only"** → read `messages` with `q="is:unread"`, `labelIds="INBOX"`, `format="metadata"`, `.limit(50)`. Gmail charges ~4× more per message for `format="full"`.
- **"Show my profile"** → read `profile`, no filters.
- **"Find the latest invoice from Vendor"** → read `messages` with `q="from:vendor@example.com subject:invoice"`, `.limit(5)`, let the user pick by id, then a tighter follow-up read if needed.

Practical:
- Always cap rows by chaining `.limit(N)` on the DataFrame; the read path itself enforces no row cap.
- Prefer `format="metadata"` on the `messages` table when the user only needs headers.
- The connector internally translates `q` to Gmail's search syntax — pass it through verbatim (e.g. `is:unread`, `has:attachment`, `newer_than:7d`).

---

## Connection prerequisite

If `{{connection_name}}` is provided, trust it and run a small read early to validate (e.g. `tableName=labels`, `.limit(1)`); surface any error verbatim.

If it isn't, the connection must be created on the **user's laptop** because the OAuth U2M flow opens their browser:

```bash
community-connector create_connection gmail <CONN_NAME> \
  --auth-type u2m \
  -o '{"client_id":"<CLIENT_ID>","client_secret":"<CLIENT_SECRET>"}'
```

Google's `authorization_endpoint`, `token_endpoint`, and scopes are baked into `connector_spec.yaml`; the user only supplies their OAuth Web Application's `client_id` + `client_secret`. Choose `u2m_per_user` instead when each end user should re-consent independently.

You can't shortcut this — UC mints and refreshes the `access_token`, and the connector treats it as opaque.

---

## Error handling

The deployed connector surfaces failures as Spark exceptions (the agent-dispatcher's normalised `_meta.code` envelope is not on the dispatch path yet). When `results.resultType == "error"`, parse `results.summary` + `results.cause` and act on the underlying cause:

| Symptom | Likely cause | What to do |
|---|---|---|
| `Bad Target: /api/...` from `databricks api` | REST path wrong (most often `commands/contexts/...`) | Re-check the path table at the top of this file. |
| Gmail HTTP 401 in the traceback | OAuth token expired / revoked | Stop; ask the user to re-run `create_connection`. |
| Gmail HTTP 403 | Missing scope or Workspace admin restriction | Surface the message; if Drive-related, the connector still works for Gmail-only reads. |
| Gmail HTTP 404 on the history endpoint | `historyId` expired (~30 days) | The connector falls back to a full scan on retry; warn the user the next read may be slow. |
| Gmail HTTP 429 | Rate limit | Back off a few seconds and retry once. Don't hammer. |
| `Connection not found` | `connection_name` is wrong or missing | Re-confirm with the user; offer to create via the prerequisite step. |
| Module import error on `GmailDataSource` | Connector wheel not installed on the cluster | Stop and surface — this skill can't fix cluster libraries. |

---

## Rules

- The `cluster_id` is **hard-coded** to `0528-081139-nh2l2jnu`. Never ask the user for it and never substitute another id.
- The **path table** at the top is the source of truth for REST endpoints. Never invent a path. Never paste `/api/2.0/commands/...` or `/api/1.2/commands/contexts/...`.
- The **README** is the source of truth for the table surface — table names and `table_configuration` keys. Never invent either; don't claim ops (`search_messages`, `download_attachment`, etc.) work today.
- Register the data source on the first command in every fresh context, never on later commands in the same context.
- Always print results as JSON inside the snippet so the cluster response is machine-parseable; don't rely on Spark's text-table output.
- Stay inside the table surface. If the user wants something it can't do, say so plainly — don't fake it through an unrelated read.
- Tear down the execution context when the conversation's Gmail work is finished, including on error.
- For sustained ingestion, hand off to `deploy-connector` rather than looping the execution envelope yourself.
