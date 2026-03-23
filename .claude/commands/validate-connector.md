Run Phase 2 (auth setup, live tests, optional deployment) for a connector that has already been developed via `/develop-connector` or `/batch-develop-connectors`.

Usage: /validate-connector <source_name>

Arguments: $ARGUMENTS

---

Parse arguments: first positional = **source_name** (required, lowercase). Stop and ask if missing.

Paths: `SRC=src/databricks/labs/community_connector/sources/{source_name}`, `TESTS=tests/unit/sources/{source_name}`

## Entry Check

Before starting, verify Phase 1 artifacts exist:

- `{SRC}/{source_name}_api_doc.md`
- `{SRC}/{source_name}.py`
- `{SRC}/connector_spec.yaml`

If any are missing, tell the developer which files are absent and that they should run `/develop-connector {source_name}` first. Stop.

---

## Step 1 — Auth Setup

Run the `/authenticate-source` skill. Read and follow `.claude/skills/authenticate-source/SKILL.md`.
Pass the source name. Finish all steps in the skill sequentially.

Gate: confirm `{TESTS}/configs/dev_config.json` exists and auth test passes before proceeding.

---

## Step 2 — Live Testing & Fixes

Subagent: `connector-tester` (foreground, wait for completion)

Prompt: source name, implementation path `{SRC}/{source_name}.py`, `dev_config.json` path `{TESTS}/configs/dev_config.json`.

After subagent: run `pytest {TESTS}/ -v --tb=short` yourself using a **synchronous** Bash call with `timeout=60000` (60s). Never run pytest in background.

If tests fail, do NOT proceed — report failures to the developer.

Gate: confirm all tests pass.

---

## Step 3 — Deployment (Optional)

Use `AskUserQuestion`: "Live tests pass. Proceed to deploy a pipeline?"
Options: "Yes, deploy now" / "No, stop here".

If yes: run the `/deploy-connector` skill. Read and follow `.claude/skills/deploy-connector/SKILL.md`.
Pass `source_name` with `use_local_source=true`. This is interactive — ask the developer for input at each step.

---

## Final Output

```
Connector:  {source_name}
Auth:       ✓ validated
Tests:      ✓ all passed
Deployment: <deployed to <pipeline_name> | skipped>
```

If the connector was developed from a PR branch, remind the developer to merge it:
```
git checkout master && gh pr merge feat/connector-{source_name}
```
