---
name: gmail-connector
description: Tool layer for Gmail reads. Runs read queries on a fixed Databricks cluster via the Command Execution REST API; scheduled-ingestion requests hand off to `deploy-connector`. Use whenever the user asks the agent to read something from their mailbox.
args:
  - name: connection_name
    description: Name of the UC COMMUNITY connection holding the Gmail OAuth grant.
    required: false
---

# Gmail Connector

This skill turns the project's Gmail tables into a tool surface the agent can call. You — the agent — translate the user's request into table reads with optional filter options, run them on a Databricks cluster via the Command Execution REST API, and return the answer.

You **never** invent table names, option keys, or REST paths. The table surface in "What this skill is for" is the source of truth for the tables and options; the path table below is the source of truth for the cluster API.

---

## Cluster

This skill runs every Gmail snippet on the fixed cluster:

```
0528-081139-nh2l2jnu
```

Use that id verbatim in every Command Execution request. **Do not ask the user for a cluster id** and do not substitute a different one. If a request to it fails with a state other than `RUNNING`, report the error back to user and terminate. 

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

The one explicit carve-out, owned by a sibling skill:

- **Scheduled / continuous ingestion** ("land my inbox in UC hourly") → hand off to the `deploy-connector` skill with `source_name=gmail`. That skill provisions an SDP pipeline via the `community-connector` CLI. Don't loop the read envelope here to fake it.

If the user asks for something the table surface can't do (write actions, byte-level attachment fetch, push notifications), say so plainly.

---

## How you call into the connector

Every read is a Python snippet executed in a long-lived context on the cluster:

```python
df = (spark.read.format("lakeflow_connect")
        .option("databricks.connection", "<CONN>")   # UC injects the OAuth access_token from this connection
        .option("tableName", "<TABLE_NAME>")
        .option("<filter_key>", "<value>")    # 0+ from the option set above
        .load()
        .limit(<N>))                          # always cap rows you don't need
print(json.dumps([r.asDict(recursive=True) for r in df.collect()], default=str))
```

The connection option key **must** be `databricks.connection` — that exact string is what Unity Catalog watches for to inject the connection's OAuth `access_token` into the connector options at query time. Other spellings (`connection_name`, `connectionName`) are silently ignored: UC injects nothing and the read fails inside the connector with `Gmail connector requires 'access_token' in options`.

### Bootstrap (do once at the start of a Gmail-shaped conversation)

1. **Confirm the cluster** is `RUNNING`:
   ```bash
   databricks api get /api/2.1/clusters/get \
     --json "{\"cluster_id\":\"0528-081139-nh2l2jnu\"}" -o json | jq '{state, spark_version}'
   ```
   If terminated, ask the user before starting it.

2. **Open a Python context** (`/api/1.2/contexts/create`):
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

1. **Build the snippet** from the template above, ending with the `print(json.dumps(...))` line so the result comes back parseable.

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
- Spark's `CaseInsensitiveStringMap` lowercases option keys, but the connector handles the standard spellings; use the option keys listed in "What this skill is for".

### Teardown

Free the context when you're done. Path: `/api/1.2/contexts/destroy`:

```bash
databricks api post /api/1.2/contexts/destroy \
  --json '{"clusterId":"0528-081139-nh2l2jnu","contextId":"<CTX>"}'
```

Contexts are a cluster resource — don't leak them across sessions; tear down even when a read errored.

---

## Composing reads to satisfy a request

Plan a request as one or more table reads with the filter options listed in "What this skill is for". Examples for orientation only:

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

## Error handling

The deployed connector surfaces failures as Spark exceptions. When `results.resultType == "error"`, parse `results.summary` + `results.cause` and act on the underlying cause:

| Symptom | Likely cause | What to do |
|---|---|---|
| `Bad Target: /api/...` from `databricks api` | REST path wrong (most often `commands/contexts/...`) | Re-check the path table at the top of this file. |
| Gmail HTTP 401 in the traceback | OAuth token expired / revoked | Stop; ask the user to re-authorize the connection. |
| Gmail HTTP 403 | Missing scope or Workspace admin restriction | Surface the message; if Drive-related, the connector still works for Gmail-only reads. |
| Gmail HTTP 404 on the history endpoint | `historyId` expired (~30 days) | The connector falls back to a full scan on retry; warn the user the next read may be slow. |
| Gmail HTTP 429 | Rate limit | Back off a few seconds and retry once. Don't hammer. |
| `Connection not found` | connection name is wrong or missing | Re-confirm the connection name with the user. |
| `Gmail connector requires 'access_token' in options` | Read used the wrong connection option key, so UC injected no token | Use `.option("databricks.connection", "<CONN>")` — not `connection_name`/`connectionName`. |
| Module import error on `GmailDataSource` | Connector wheel not installed on the cluster | Stop and surface — this skill can't fix cluster libraries. |
