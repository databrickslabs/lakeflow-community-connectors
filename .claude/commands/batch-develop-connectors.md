Run Phase 1 (research, implement, spec, docs, simulate-mode tests, PR) for multiple connectors sequentially — one connector fully completes before the next begins. No user interaction or credentials required.

Usage: /batch-develop-connectors source1 source2 ... [doc:source=<url>]

Arguments: $ARGUMENTS

---

## Argument Parsing

- Positional arguments = list of source names (lowercase, required). Stop and ask if none provided.
- `doc:<source>=<url>` — optional per-source API doc URL, e.g. `doc:stripe=https://stripe.com/docs/api`
- Example: `/batch-develop-connectors stripe github linear doc:stripe=https://stripe.com/docs/api`

## Rules

- **No user confirmation gates.** Do not use `AskUserQuestion` at any point.
- **Sequential loop.** Complete all steps for one connector before starting the next.
- **Fail-forward.** If any step fails for a connector, record the failure and move on to the next connector. Do not abort the batch.
- **Task tracking**: `TaskCreate` one task per connector at the start. Mark each `in_progress` when its pipeline starts, `completed` (or failed) when it ends.

---

## Per-Connector Pipeline

For each source in the list, run the following steps in order. This is identical to `/develop-connector` but embedded directly here so it can track state across the batch.

Paths: `SRC=src/databricks/labs/community_connector/sources/{source_name}`, `TESTS=tests/unit/sources/{source_name}`

**Subagent pattern**: run each subagent in the foreground (`run_in_background=false`) and wait for completion before the next step. Every subagent prompt must include: source name, all relevant file paths, and table scope. Subagents have no prior context.

### Step 1 — API Research
Subagent: `source-api-researcher` → `{SRC}/{source_name}_api_doc.md`

Prompt: source name, doc URL (if provided via `doc:<source>=`), no table scope (let it determine important tables automatically). Tell it not to ask the user.

Verify: `{SRC}/{source_name}_api_doc.md` exists. If missing → mark connector failed, continue to next.

### Step 2 — Implementation
Subagent: `connector-dev` → `{SRC}/{source_name}.py` AND `endpoints.yaml`.

Prompt: source name, API doc path, tables from the API doc. Tell it explicitly that both the connector and `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml` are required outputs.

Verify (both must exist):
- `{SRC}/{source_name}.py`
- `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`

If either is missing → mark connector failed, continue to next.

### Step 3 — Spec
Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml`

Prompt: source name, implementation path.

Verify: `{SRC}/connector_spec.yaml` exists. If missing → mark connector failed, continue to next.

### Step 4 — Docs
Subagent: `connector-doc-writer` → `{SRC}/README.md`

Prompt: source name, implementation path, API doc path.

Verify: `{SRC}/README.md` exists. If missing → mark connector failed, continue to next.

### Step 5 — Simulate-Mode Tests

Subagent: `connector-tester` (foreground, wait for completion).

Prompt:
- `mode=simulate` (no credentials, no live calls)
- Source name, implementation path `{SRC}/{source_name}.py`, simulator spec path `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`
- Test path `{TESTS}/test_{source_name}_lakeflow_connect.py` (the agent will create it)
- Tell the agent to bootstrap the corpus from `TABLE_SCHEMAS` via `tools.corpus_from_schema.write_corpus_from_schemas` before running pytest.

After the subagent returns, run a final synchronous verification:

```bash
source .venv/bin/activate 2>/dev/null || (python3.10 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]" -q)
pytest tests/unit/sources/{source_name}/ -v --tb=short
```

If any test still fails → mark connector failed (record pytest output), do NOT open a PR, continue to next connector.

### Step 6 — Commit and Open PR

```bash
git checkout -b feat/connector-{source_name}
git add {SRC}/ {TESTS}/
git commit -m "feat({source_name}): add {source_name} connector [needs-live-testing]"
```

Open PR via `gh pr create`:
- Title: `feat({source_name}): add {source_name} connector`
- Label: `needs-live-testing` (create with `gh label create` if it doesn't exist)
- Body:

```
## What's included
- API research doc, implementation, simulator spec, connector spec, documentation

## Simulate-mode test results
<N>/<N> passed

The simulator spec and corpus have not yet been validated against the live API.

## Next step: live testing
  /validate-connector {source_name}
```

Record the PR URL. Return to master/main branch before starting the next connector.

---

## Final Summary

After all connectors are processed, print:

```
Batch complete: {N_done}/{N_total} connectors developed

Connector     Status      PR                    Simulate Tests
-----------   ---------   -------------------   ---------------
stripe        ✓ done      https://github.com/…  8/8 passed
github        ✓ done      https://github.com/…  6/6 passed
linear        ✗ failed    —                     step 2 (impl) error
```

For any failed connector, include a brief note on which step failed and why.
