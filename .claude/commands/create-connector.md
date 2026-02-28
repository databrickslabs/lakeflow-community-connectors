Build a new community connector end-to-end.

Usage: /create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]

Arguments: $ARGUMENTS

---

Parse arguments: first positional = **source_name** (required, lowercase); `tables=` = comma-separated tables (optional); `doc=` = API doc URL or path (optional). Stop and ask if source_name is missing.

Paths: `SRC=src/databricks/labs/community_connector/sources/{source_name}`, `TESTS=tests/unit/sources/{source_name}`

## Protocols

**Plan first**: Present tables in scope + 6-step workflow. Hard-stop with `AskUserQuestion`: "Does this plan look good?" ("Yes, proceed" / "I have adjustments"). Do NOT start Step 1 until confirmed.

**Task tracking**: Once confirmed, `TaskCreate` for all 6 steps. Mark `in_progress` before launching each step, `completed` after.

**Confirmation gate** (steps 1–5): After each step, `AskUserQuestion` with a summary of what was produced (files created, tables found, test results). Options: "Continue" / "Review first". Do NOT proceed without confirmation. Step 6 skips the gate.

**Subagent pattern**: `Task(subagent_type=..., run_in_background=true)` → wait for automatic completion notification (do **NOT** call `TaskOutput`) → verify output files with `Glob`. Every subagent prompt must include: source name, all relevant file paths, and table scope. Subagents have no prior context.

---

## Step 1 — API Research
Subagent: `source-api-researcher` → `{SRC}/{source_name}_api_doc.md`

Prompt: source name, doc URL/path (if any), table scope. Tell it not to ask the user.
Gate: summarize tables and auth method found.

---

## Step 2 — Auth Setup

Three sub-steps, sequential:

**2a.** Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml`
Prompt: source name, API doc path. Connection/auth section only; `external_options_allowlist` empty.

**2b.** Collect credentials **yourself** — this is interactive (browser form) and cannot run as a background subagent. Read and follow `.claude/skills/collect-credentials/SKILL.md`.
Verify: `{TESTS}/configs/dev_config.json` exists.

**2c.** Subagent: `connector-auth-validator` → `{TESTS}/auth_test.py`
Prompt: source name, API doc path, `dev_config.json` path.
Gate: confirm auth test passes.

---

## Step 3 — Implementation
Subagent: `connector-dev` → python files under `{SRC}/`

Prompt: source name, API doc path, tables to implement.
Gate: verify implementation file(s) exist.

---

## Step 4 — Testing & Fixes
Subagent: `connector-tester` → `{TESTS}/test_{source_name}_lakeflow_connect.py` (all passing)

Prompt: source name, implementation path, `dev_config.json` path.
After subagent: run `pytest {TESTS}/ -v --tb=short` yourself. If tests fail, do NOT proceed — report failure to user.
Gate: confirm all tests pass.

---

## Step 5 — Docs + Complete Spec

**5a.** Subagent: `connector-doc-writer` → `{SRC}/README.md`
Prompt: source name, implementation and API doc paths.

**5b.** Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml` (complete with `external_options_allowlist`)
Prompt: source name, implementation path.

Gate: verify both files exist.

---

## Step 6 — Deployment
Subagent: `connector-deployer` → `{SRC}/pyproject.toml` + built distribution

Prompt: source name, connector directory path.
No gate — proceed directly to final summary.

---

## Final Summary

```
Connector: {source_name}
Tables:    [list]
Source:    src/databricks/labs/community_connector/sources/{source_name}/
Tests:     tests/unit/sources/{source_name}/
```

If a subagent fails (e.g. couldn't write its output file), report the failure clearly to the user — do not attempt to redo the subagent's work yourself. If the user wants to resume from a step, skip earlier ones.
