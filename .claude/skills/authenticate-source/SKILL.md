---
name: authenticate-source
description: Set up authentication for a source connector — generate connector spec, collect credentials interactively, and validate auth.
---

# Authenticate Source

Set up authentication for **{{source_name}}** in three sequential steps.

## Prerequisites

- API doc at `src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}_api_doc.md`
- Python 3.10+

---

## Step 1 — Ensure connector_spec.yaml exists

Check if `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml` already exists.

- **If it exists:** skip generation, proceed to Step 2.
- **If it does not exist:** launch `connector-spec-generator` subagent.
  - Subagent_type: connector-spec-generator
  - Prompt: source name, API doc path
  - Output: src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml
    - CONNECTION/AUTH SECTION ONLY — no implementation file exists yet.
    - Set external_options_allowlist to empty string.
  - Gate: verify output file exists.

---

## Step 2 — Collect Credentials

Run this step directly — not as a subagent.
This step is **interactive** — the script blocks until the user submits the browser form.

2a. Ensure venv:
   ```bash
   python3.10 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
   ```

2b. Run authenticate script in background (`block_until_ms: 0` — it blocks until form submission):
   ```bash
   source .venv/bin/activate && python tools/scripts/authenticate.py -s {{source_name}} -m browser
   ```

2c. Extract the URL

Read the background Bash output. The script prints a line like:

```
→ http://localhost:9876
```

The port may differ from 9876 if already in use.

2d. Ask the user to fill in credentials

Use `AskUserQuestion` to show the user the URL and ask them to:
- Open the URL in their browser
- Fill in their credentials
- Click Save
- Confirm here once done

Wait for the user to explicitly confirm before proceeding. If the user reports an error, help them debug.


2e. After confirmation, verify `tests/unit/sources/{{source_name}}/configs/dev_config.json` exists.

---

## Step 3 — Launch the `connector-auth-validator` subagent 

Subagent_type: connector-auth-validator
prompt: Generate and run an auth verification test for {{source_name}}.
```

Report error if the validation failed.

---

## Rules

- Steps run **sequentially** — each depends on the prior step's output.
- If a subagent fails, report clearly — do not redo its work.
