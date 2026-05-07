Run Phase 1 (research, implement, spec, docs, simulate-mode tests, PR) for a single connector without any user interaction or credentials.

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

Subagent: `connector-dev` → python files under `{SRC}/` AND `endpoints.yaml` under the simulator spec dir.

Prompt: source name, API doc path, tables to implement. Tell it explicitly that both the connector and `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml` are required outputs.

Verify (both must exist):
- `{SRC}/{source_name}.py`
- `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`

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

## Step 5 — Simulate-Mode Tests

Subagent: `connector-tester` (foreground, wait for completion).

Prompt:
- `mode=simulate` (no credentials, no live calls)
- Source name, implementation path `{SRC}/{source_name}.py`, simulator spec path `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`
- Test path `{TESTS}/test_{source_name}_lakeflow_connect.py` (the agent will create it)
- Tell the agent to bootstrap the corpus from `TABLE_SCHEMAS` via `tools.corpus_from_schema.write_corpus_from_schemas` before running pytest.

After the subagent returns, run a final synchronous verification yourself:

```bash
source .venv/bin/activate
pytest tests/unit/sources/{source_name}/ -v --tb=short
```

If any test still fails, do NOT open a PR. Report the failures clearly so the developer can investigate and rerun. Stop here.

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
- Simulator spec: `source_simulator/specs/{source_name}/endpoints.yaml`
- Connector spec: `{SRC}/connector_spec.yaml`
- Documentation: `{SRC}/README.md`

## Simulate-mode test results
<paste pytest output summary>

The simulator spec and corpus have not yet been validated against the live API.

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
Simulate:  <N>/<N> passed
Files:
  {SRC}/{source_name}_api_doc.md
  {SRC}/{source_name}.py
  src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml
  {SRC}/connector_spec.yaml
  {SRC}/README.md
```
