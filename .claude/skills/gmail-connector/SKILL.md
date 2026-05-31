---
name: gmail-connector
description: Tool layer for Gmail tasks. Loads the connector's operation catalogue dynamically from a Databricks cluster via the Command Execution REST API and composes those operations to satisfy the user's intent. The tool surface is whatever `list_operations` returns; scheduled-ingestion requests hand off to `deploy-connector`. Use whenever the user asks the agent to do something with their mailbox.
args:
  - name: cluster_id
    description: All-purpose cluster the agent runs Gmail operations on (required — must be supplied by the user, never invented).
    required: true
  - name: connection_name
    description: Name of the UC COMMUNITY connection holding the Gmail OAuth grant. Create one (see "Connection prerequisite") if absent.
    required: false
---

# Gmail Connector

This skill turns the project's Gmail Spark APIs into a tool surface the agent can call. You — the agent — translate the user's natural request into a sequence of connector operations, run them on a Databricks cluster, and return the answer.

You **never** invent operations or parameter names, and you don't infer the surface from this file. The cluster is the source of truth: load the catalogue once with `list_operations`, then drive everything from what it reports.

---

## What this skill is for

This skill is the **tool layer** for Gmail. Your tool surface is exactly what `list_operations` returns when called against the user's connection — nothing more, nothing less. Each catalogue entry carries a `description` and `parameters_json` written for exactly this purpose: read them to decide whether a request is satisfiable and how to call it. If the catalogue doesn't expose what the user is asking for, say so plainly — don't fake it with an op that isn't a fit.

There is one explicit carve-out, because it's owned by a sibling skill rather than the read-side tool surface:

- **Standing up a scheduled / continuous ingestion pipeline** (e.g. "ingest my labels table into UC every hour", "land my inbox in `main.gmail.messages` daily") — hand off to the `deploy-connector` skill with `source_name=gmail`. That skill provisions an SDP pipeline via the `community-connector` CLI, which is the right vehicle for ongoing sync. Do not loop the read envelope in this skill to fake periodic ingestion.

For everything else — including whether write actions, push notifications, multi-mailbox routing, etc. are supported — let `list_operations` answer. The catalogue is the truth; this file is not.

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

After bootstrap, read the catalogue's entries — their `description` and `parameters_json` fields tell you what each op does and what it needs. Plan a request as a sequence of ops the catalogue actually exposes; an op you don't see is one you can't call.

A request often decomposes across multiple ops, each one a normal execution-envelope call. For example, "download the latest attachment from Alice" typically becomes: find the candidate messages, pick the right one, enumerate its attachments, fetch the bytes — each step a separate op chosen from the catalogue.

Two general practices help regardless of which ops the catalogue exposes:

- **Prefer lightweight discovery before heavy fetches.** If the catalogue offers a triage-style op (header-only listing) alongside a full-fetch op, run the triage one first to narrow the set, then fetch full bodies only for what you actually need. Gmail charges roughly 4× more per message for full-format reads than for metadata-only.
- **Cap rows you don't need.** Some ops accept a `limit` parameter; for ones that don't, chain `.limit(N)` inside the snippet before `.collect()`.

When the request is about *ongoing or scheduled* ingestion rather than an ad-hoc read, stop here and hand off to the `deploy-connector` skill — see the carve-out in the previous section.

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
