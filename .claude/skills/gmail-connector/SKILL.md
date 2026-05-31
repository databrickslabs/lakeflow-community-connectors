---
name: gmail-connector
description: Tool layer for Gmail tasks. Loads the connector's operation catalogue from a Databricks cluster via the Command Execution REST API and composes operations (search, fetch, attachments) to satisfy the user's intent. Use whenever the user asks the agent to do something with their mailbox.
args:
  - name: cluster_id
    description: All-purpose cluster the agent runs Gmail operations on (required — must be supplied by the user, never invented).
    required: true
  - name: connection_name
    description: Name of the UC COMMUNITY connection holding the Gmail OAuth grant. Create one (see "Connection prerequisite") if absent.
    required: false
---

# Gmail Connector

This skill turns the project's Gmail Spark APIs into an MCP-style tool surface. You — the agent — translate the user's natural request into a sequence of connector operations, run them on a Databricks cluster, and return the answer.

You **never** invent operations or parameter names. The cluster is the source of truth: load the catalogue once with `list_operations`, then drive everything else from it.

---

## What the user can ask for

The connector is **read-side only**. Anything in this list is in scope:

- "Search / find / show me emails matching X" — typed filters on sender, subject, date window, label, attachments, unread, plus free-form Gmail query syntax.
- "Open / show / summarise this specific email" — by id, with decoded plain-text and HTML body.
- "What's attached to this email?" / "Download the attachment / file" — Gmail-native parts and Drive-hosted links, written to a UC Volume.
- "List my Gmail labels / drafts / filters / forwarding addresses / send-as aliases / delegates / account info."
- "Ingest my Gmail tables into Unity Catalog on a schedule" — hand off to the `deploy-connector` skill with `source_name=gmail`; do not loop the read path.

## What the user **cannot** ask for through this skill

If the user asks for any of the following, stop, explain it isn't supported by this connector, and offer the closest in-scope alternative:

- **Anything that writes to Gmail** — send, reply, draft, archive, delete, mark read, modify labels, create/edit filters or signatures. The connector ships read-only OAuth scopes (`gmail.readonly` + `drive.readonly`).
- **Mailboxes other than the connection's** — there's no per-call account switching beyond the OAuth subject the connection was created for.
- **Real-time push / webhooks** — the connector polls; it doesn't receive Gmail push notifications.

If the user asks for something Gmail-shaped but not in the loaded catalogue (e.g. a calendar event, a Drive folder listing), check `list_operations` first; if it isn't there, say so plainly rather than guessing.

---

## How you actually call into the connector

Every call has the same envelope — a Python snippet executed in a long-lived context on the user's cluster:

```python
df = (spark.read.format("lakeflow_connect")
        .option("connection_name", "{{connection_name}}")
        .option("operation", "<OP_NAME>")
        .option("<param>", "<value>")        # 0+ params from list_operations
        .load())
```

Composing a snippet, shipping it to the cluster, and parsing the result is one helper, not a per-task ritual — see *Execution envelope* below.

### Bootstrap (do once at the start of a Gmail-shaped conversation)

1. **Confirm the cluster** the user gave you (`{{cluster_id}}`) is `RUNNING`:
   ```bash
   databricks api get /api/2.1/clusters/get \
     --json "{\"cluster_id\":\"{{cluster_id}}\"}" -o json | jq '{state, spark_version}'
   ```
   If terminated, ask the user before starting it.
2. **Open a Python context** on that cluster and remember the returned `id`:
   ```bash
   databricks api post /api/2.0/commands/contexts/create \
     --json '{"clusterId":"{{cluster_id}}","language":"python"}' -o json
   ```
3. **Register the data source and load the catalogue**:
   ```python
   import json
   from databricks.labs.community_connector.sources.gmail import GmailDataSource
   spark.dataSource.register(GmailDataSource)
   df = (spark.read.format("lakeflow_connect")
           .option("connection_name", "{{connection_name}}")
           .option("operation", "list_operations").load())
   print(json.dumps([r.asDict() for r in df.collect()]))
   ```
   The printed JSON gives you `name`, `description`, `kind`, `result_schema_json`, and `parameters_json` for **every** operation this connection exposes. Cache it for the rest of the session. If `list_operations` fails, the connector isn't installed on the cluster — surface that and stop.

After that, individual user requests don't repeat steps 1–3. They just run more snippets against the same context.

### Execution envelope (the universal "call any operation")

For any one operation:

1. Build the snippet — pick `OP_NAME` and only the parameters that the catalogue declares for it. End the snippet with `print(json.dumps([r.asDict(recursive=True) for r in df.collect()], default=str))` so the result comes back parseable.
2. Submit to the cluster:
   ```bash
   databricks api post /api/2.0/commands/execute \
     --json '{"clusterId":"{{cluster_id}}","contextId":"<CTX>","language":"python","command":"<SNIPPET>"}' -o json
   ```
3. Poll until terminal status:
   ```bash
   databricks api get /api/2.0/commands/status \
     --json "{\"clusterId\":\"{{cluster_id}}\",\"contextId\":\"<CTX>\",\"commandId\":\"<CMD>\"}" -o json
   ```
   `status` walks `Queued → Running → Finished | Error | Cancelled`.
4. Read the result:
   - `Finished`, `results.resultType == "text"` → `results.data` is the JSON you printed. Parse and use.
   - `Error` → `results.summary` is the one-liner; `results.cause` is the traceback. The dispatcher writes a canonical code into `_meta.code` of the printed row when the error is connector-level (see *Error codes* below).

A few quoting notes that bite if you skip them:
- The `command` field is a JSON string containing Python source — preserve newlines (`\n`) and quote your inner strings.
- Spark's `CaseInsensitiveStringMap` lowercases option keys, but the connector already handles the standard parameter spellings; just match whatever `list_operations` shows.
- `spark.dataSource.register(GmailDataSource)` is idempotent inside a context; do it once at bootstrap, not per call.

### Teardown

At the end of the session, free the context:

```bash
databricks api post /api/2.0/commands/contexts/destroy \
  --json '{"clusterId":"{{cluster_id}}","contextId":"<CTX>"}'
```

Contexts are a cluster resource — don't leak them.

---

## Composing operations to satisfy a request

Walk the catalogue first, then plan. Patterns that tend to work:

- **"Find emails from Alice last week"** → `search_messages` with `from_address=alice@…` and `newer_than=7d` (and `limit` if the user wants a small sample). Returns header-only rows — cheap, no bodies fetched.
- **"Open the latest invoice from Vendor"** → first `search_messages` (`from_address=vendor@…`, `subject=invoice`, `limit=5`) to choose the message id, then `get_message` for full body + attachments + Drive file ids.
- **"Download the PDF attached to that mail"** → `list_attachments` to read `attachment_id` / `drive_file_id` and the right `kind`, then `download_attachment` with `volume_path` under `/Volumes/<cat>/<schema>/<vol>/<filename>`. For Gmail-native parts pass `message_id` + `attachment_id`; for Drive parts pass `drive_file_id` (optionally `export_mime_type` for Google-native docs).
- **"Is the connection alive?"** → `validate_connection`. One row, check `_meta.status == "ok"`.
- **"What can you do?"** → answer from the loaded catalogue, not from this file. Each entry has a `description` written for exactly this.

When a request mixes capabilities (e.g. "download the latest attachment from Alice"), you compose: `search_messages` → pick id → `list_attachments` → `download_attachment`. Each is a normal execution-envelope call.

Always cap rows you don't need: `search_messages` honours `limit` (default 50, max 500); `read_table` honours nothing, so chain `.limit(N)` inside the snippet.

---

## Picking the right read path

The catalogue exposes two read shapes; choose by intent:

- `search_messages` — header-only triage. Cheap (Gmail charges ~5 quota units per message vs ~20 for full). Use any time the user wants to *find* messages.
- `read_table` — full table scan with the table's natural schema. Use when the user wants raw `messages`, `threads`, `labels`, `drafts`, `profile`, `settings`, `filters`, `forwarding_addresses`, `send_as`, or `delegates`. For `messages`/`threads` it accepts the same typed filters as `search_messages` (composed into Gmail's `q` syntax).

When the user wants periodic / scheduled ingestion of full tables into UC, **stop driving the read path** and route to the `deploy-connector` skill with `source_name=gmail`. That spins up an SDP pipeline backed by the same connector, which is the right vehicle for ongoing sync.

---

## Connection prerequisite

If `{{connection_name}}` is provided, trust it and try `validate_connection` early; surface `_meta` errors verbatim.

If it isn't, the connection must be created on the **user's laptop** because the OAuth U2M flow opens their browser:

```bash
community-connector create_connection gmail <CONN_NAME> \
  --auth-type u2m \
  -o '{"client_id":"<CLIENT_ID>","client_secret":"<CLIENT_SECRET>"}'
```

Google's `authorization_endpoint`, `token_endpoint`, and scopes are baked into `connector_spec.yaml`; the user only supplies their OAuth Web Application's `client_id` + `client_secret`. Choose `u2m_per_user` instead when each end user should re-consent independently.

You can't shortcut this — UC mints and refreshes the `access_token`, and the connector treats it as opaque. If `validate_connection` returns `auth_failed` mid-session, send the user back here to re-consent.

---

## Error codes (canonical `_meta.code` values)

The dispatcher normalises connector failures. When you see one, decide whether to retry, recompose, or surface to the user:

| Code | Meaning | What you should do |
|---|---|---|
| `auth_failed` | OAuth token expired or revoked. | Stop; ask the user to re-run the `create_connection` flow. |
| `permission_denied` | Usually missing `drive.readonly` on a Drive-hosted attachment. | Tell the user; offer to retry once they re-consent with both scopes. |
| `not_found` | Bad `message_id` / `attachment_id` / `drive_file_id`. | Recheck the id you computed — typically a stale search result. |
| `rate_limited` | Gmail 429. | Back off (a few seconds), retry once. Don't hammer. |
| `bad_request` | Missing/malformed option. | Re-read the parameter list from `list_operations`; you probably picked a wrong key. |
| `connection_failed` | The UC connection itself is broken. | Run `validate_connection`, surface the message, stop. |

---

## Rules

- The `cluster_id` always comes from the user. Don't reuse a stale one across sessions.
- `list_operations` is the source of truth for what's callable — load it at bootstrap, and re-load whenever the connector might have changed (e.g. after the user redeploys).
- Always print results as JSON inside the snippet so the cluster response is machine-parseable; never rely on the Spark text-table output.
- Stay inside the catalogue. If the user wants a write action or anything outside it, say so plainly — don't try to fake it through a read op.
- Tear down the execution context when the conversation's Gmail work is finished, including on error.
- For sustained ingestion, hand off to `deploy-connector` rather than looping the execution envelope yourself.
