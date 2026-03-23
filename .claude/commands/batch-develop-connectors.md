Run Phase 1 (research, implement, spec, docs, contract tests, PR) for multiple connectors sequentially — one connector fully completes before the next begins. No user interaction or credentials required.

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
Subagent: `connector-dev` → `{SRC}/{source_name}.py`

Prompt: source name, API doc path, tables from the API doc.

Verify: `{SRC}/{source_name}.py` exists. If missing → mark connector failed, continue to next.

### Step 3 — Spec
Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml`

Prompt: source name, implementation path.

Verify: `{SRC}/connector_spec.yaml` exists. If missing → mark connector failed, continue to next.

### Step 4 — Docs
Subagent: `connector-doc-writer` → `{SRC}/README.md`

Prompt: source name, implementation path, API doc path.

Verify: `{SRC}/README.md` exists. If missing → mark connector failed, continue to next.

### Step 5 — Contract Tests

Run synchronously. No credentials needed.

```bash
source .venv/bin/activate 2>/dev/null || (python3.10 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]" -q)
pytest tests/unit/sources/test_contract.py -k {source_name} -v --tb=short
```

If contract tests fail → mark connector failed (record pytest output), do NOT open a PR, continue to next connector.

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
- API research doc, implementation, connector spec, documentation

## Contract test results
<N>/<N> passed

## Next step: live testing
  /validate-connector {source_name}
```

Record the PR URL. Return to master/main branch before starting the next connector.

---

## Final Summary

After all connectors are processed, print:

```
Batch complete: {N_done}/{N_total} connectors developed

Connector     Status      PR                    Contract Tests
-----------   ---------   -------------------   ---------------
stripe        ✓ done      https://github.com/…  8/8 passed
github        ✓ done      https://github.com/…  6/6 passed
linear        ✗ failed    —                     step 2 (impl) error
```

For any failed connector, include a brief note on which step failed and why.
