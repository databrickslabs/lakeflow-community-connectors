Run Phase 1 (research, implement, spec, docs, contract tests, PR) for a single connector without any user interaction or credentials.

Usage: /develop-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]

Arguments: $ARGUMENTS

---

Parse arguments: first positional = **source_name** (required, lowercase); `tables=` = comma-separated tables (optional); `doc=` = API doc URL or path (optional). Stop and ask if source_name is missing.

Paths: `SRC=src/databricks/labs/community_connector/sources/{source_name}`, `TESTS=tests/unit/sources/{source_name}`

## Rules

- **No user confirmation gates.** Do not use `AskUserQuestion` between steps. Run all steps automatically.
- **No credentials.** Do not collect or require any source credentials. Skip auth entirely.
- **Subagent pattern**: `Agent(subagent_type=..., run_in_background=false)` — run each subagent in the foreground and wait for completion before the next step. Every subagent prompt must include: source name, all relevant file paths, and table scope. Subagents have no prior context.
- **Verify outputs**: after each subagent, use `Glob` to confirm expected files exist. If a subagent fails to produce its output, report the failure and stop — do not attempt to redo the subagent's work.
- **Task tracking**: `TaskCreate` for all steps at the start. Mark `in_progress` before each step, `completed` after.

---

## Step 1 — API Research

Subagent: `source-api-researcher` → `{SRC}/{source_name}_api_doc.md`

Prompt: source name, doc URL/path (if any), table scope (if provided). Tell it not to ask the user — if no table scope is provided, it should determine important tables automatically using Airbyte/Fivetran as references.

Verify: `{SRC}/{source_name}_api_doc.md` exists.

---

## Step 2 — Implementation

Subagent: `connector-dev` → python files under `{SRC}/`

Prompt: source name, API doc path, tables to implement.

Verify: `{SRC}/{source_name}.py` exists.

---

## Step 3 — Spec

Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml`

Prompt: source name, implementation path.

Verify: `{SRC}/connector_spec.yaml` exists.

---

## Step 4 — Docs

Subagent: `connector-doc-writer` → `{SRC}/README.md`

Prompt: source name, implementation path, API doc path.

Verify: `{SRC}/README.md` exists.

---

## Step 5 — Contract Tests

Run contract tests synchronously — never in background. No credentials needed.

```bash
python3.10 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]" -q
pytest tests/unit/sources/test_contract.py -k {source_name} -v --tb=short
```

If contract tests fail, do NOT open a PR. Report the failures clearly so the developer can investigate and rerun. Stop here.

---

## Step 6 — Commit and Open PR

Commit all files under `SRC` and `TESTS`:

```bash
git checkout -b feat/connector-{source_name}
git add {SRC}/ {TESTS}/
git commit -m "feat({source_name}): add {source_name} connector [needs-live-testing]"
```

Open a GitHub PR using `gh pr create`:
- Title: `feat({source_name}): add {source_name} connector`
- Label: `needs-live-testing` (create with `gh label create` if it doesn't exist)
- Body (use a HEREDOC):

```
## What's included
- API research doc: `{SRC}/{source_name}_api_doc.md`
- Implementation: `{SRC}/{source_name}.py`
- Connector spec: `{SRC}/connector_spec.yaml`
- Documentation: `{SRC}/README.md`

## Contract test results
<paste pytest output summary>

## Next step: live testing
Check out this branch and run:
  /validate-connector {source_name}
```

---

## Final Output

```
Connector: {source_name}
Branch:    feat/connector-{source_name}
PR:        <url>
Contract:  <N>/<N> passed
Files:
  {SRC}/{source_name}_api_doc.md
  {SRC}/{source_name}.py
  {SRC}/connector_spec.yaml
  {SRC}/README.md
```
