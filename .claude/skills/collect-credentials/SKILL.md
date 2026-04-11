---
name: collect-credentials
description: Run the authenticate script to collect credentials from the user via a browser form.
---

# Collect Credentials

## Goal

Run the `authenticate.py` script to collect credentials for the **{{source_name}}** connector. The script starts a local HTTP server that serves a browser form based on the connector spec. The user fills in their credentials in the browser and clicks Save.

## Prerequisites

- `connector_spec.yaml` must already exist at `src/databricks/labs/community_connector/sources/{{source_name}}/connector_spec.yaml`
- Python 3.10+ with project dependencies installed

## Output

- `tests/unit/sources/{{source_name}}/configs/dev_config.json` — credentials file

## Steps

1. Ensure the venv and dependencies are ready:
```bash
python3.10 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
```

2. Run the authenticate script in background (Bash with `timeout: 0`) because it blocks until the user submits the form:
```bash
source .venv/bin/activate && python tools/scripts/authenticate.py -s {{source_name}} -m browser
```

3. Read the background Bash output to extract the URL. The script prints a line like:
```
→ http://localhost:9876
```
The port may differ from 9876 if that port is already in use.

4. Show the URL to the user via `AskUserQuestion`. Ask them to:
   - Open the URL in their browser
   - Fill in their credentials
   - Click Save
   - Confirm here once done

5. Wait for the user to explicitly confirm before proceeding. If the user reports an error, help them debug.

6. After confirmation, verify that `tests/unit/sources/{{source_name}}/configs/dev_config.json` exists.

## Important Notes

- This skill is **interactive** — the script blocks until the user submits the browser form.
- The caller must be able to run Bash in the background and communicate with the user (e.g., via `AskUserQuestion`) while the script is running.
- The script automatically shuts down after the user submits the form.
