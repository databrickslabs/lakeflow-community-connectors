# Engineering-Driven Connector Workflow Design

## Problem

The existing `/create-connector` workflow is **user-driven**: a developer must sit through every step, provide credentials via a browser form, and confirm each stage. This is fine for building one connector interactively, but blocks automation:

- You cannot batch-develop 10 connectors overnight without a human present.
- Credential collection blocks the entire pipeline, even though research and implementation don't need them.

## Core Insight: Two Phases with a Hard Boundary

The existing 6-step workflow splits cleanly into two phases at the credential boundary:

| Phase | Steps | Credentials? | Blocking? |
|---|---|---|---|
| **Develop** | Research → Implement (connector + simulator spec) → Spec → Docs → Simulate-mode tests | No | No |
| **Validate** | Auth → Record-mode tests + fixes → Deploy | Yes | Yes (browser form) |

The split is enforced by `CONNECTOR_TEST_MODE`:
- **simulate**: corpus is synthesized from `TABLE_SCHEMAS`; the test suite drives the connector through a fake source built from `endpoints.yaml`. No network, no credentials.
- **record**: tests hit the real source and write a cassette; the live validator diffs every response against what the spec + corpus would produce, surfacing simulator drift.

Phase 1 can run fully automated, in parallel, across many connectors. Phase 2 is interactive and must be triggered by a developer with credentials.

---

## Proposed Components

### 1. `/batch-develop-connectors` Command (new)

**File:** `.claude/commands/batch-develop-connectors.md`

```
Usage: /batch-develop-connectors source1 source2 ... [doc:source=<url>]
```

Processes connectors **one at a time**, completing the full Phase 1 pipeline for each before moving to the next. The main session orchestrates the existing specialist agents directly — no new agent type needed.

**Loop model:**

```
/batch-develop-connectors stripe github linear

For each connector (stripe, then github, then linear):
  1. spawn source-api-researcher   → wait → verify output
  2. spawn connector-dev           → wait → verify connector + endpoints.yaml exist
  3. spawn connector-spec-generator → wait → verify output
  4. spawn connector-doc-writer    → wait → verify output
  5. spawn connector-tester (mode=simulate) → corpus_from_schema bootstrap, simulate-mode tests pass
  6. git commit + open PR          → labeled "needs-live-testing"
  → move to next connector
```

If a step fails for a connector, log the failure and **skip to the next connector** — do not abort the whole batch. Record the failure in the final summary.

**Final summary format:**

```
Connector     Status      PR       Simulate Tests
-----------   ---------   -------  ---------------
stripe        ✓ done      #42      8/8 passed
github        ✓ done      #43      6/6 passed
linear        ✗ failed    —        step 2 (impl) error — see log
```

No new agents. All existing agents (`source-api-researcher`, `connector-dev`, `connector-spec-generator`, `connector-doc-writer`) are reused unchanged.

---

### 2. `/develop-connector` Command (new)

**File:** `.claude/commands/develop-connector.md`

```
Usage: /develop-connector <source_name> [doc=<url>]
```

The single-connector version of the batch command — useful when a developer wants to run Phase 1 for one source and watch it progress. Unlike `batch-develop-connectors`, this runs in the foreground and prints step-by-step progress, but still skips all credential-requiring steps and user confirmation gates.

Use this for debugging a specific connector's pipeline or when you don't need parallelism.

---

### 4. `/validate-connector` Command (new)

**File:** `.claude/commands/validate-connector.md`

```
Usage: /validate-connector <source_name>
```

The interactive Phase 2 command. Picks up exactly where Phase 1 left off.

**Steps:**

```
1. authenticate-source skill        → dev_config.json (browser form)
2. connector-tester (mode=record)   → cassette written, live-validator drift addressed, all tests passing
3. deploy-connector skill           → pipeline deployed (optional)
```

This is almost identical to steps 2, 4, and 6 of the existing `create-connector`, just extracted into a standalone command. The `authenticate-source` skill is reused unchanged; `connector-tester` is invoked with `mode=record` so the same agent handles both phases (see below).

**Entry check:** Before starting, verify that Phase 1 artifacts exist:
- `src/.../sources/{source_name}/{source_name}_api_doc.md`
- `src/.../sources/{source_name}/{source_name}.py`
- `src/.../sources/{source_name}/connector_spec.yaml`

If any are missing, report which Phase 1 steps need to be completed first.

---

## Handoff Mechanism: GitHub PRs as Task Queue

After Phase 1, each connector lands as a GitHub PR:

```
PR title:  feat(stripe): add stripe connector
Label:     needs-live-testing
Branch:    feat/connector-stripe

PR body:
  ## What's included
  - API research doc
  - Implementation (N tables)
  - Connector spec + README

  ## Contract test results
  8/8 passed

  ## Next step
  Check out this branch and run:
    /validate-connector stripe
```

Developers find work by filtering PRs: `label:needs-live-testing`. This requires zero additional tooling — GitHub's PR list is the task queue.

**Workflow for a developer picking up a connector:**

```bash
gh pr checkout 42          # check out the branch
claude                     # open Claude Code
/validate-connector stripe # Phase 2: auth → live tests → deploy
gh pr merge 42             # merge when done
```

---

## What Changes vs What Stays the Same

| Component | Status | Notes |
|---|---|---|
| `source-api-researcher` agent | **Unchanged** | Already supports `tell it not to ask the user` instruction |
| `connector-dev` agent | **Updated** | Also produces `source_simulator/specs/{source}/endpoints.yaml` so simulate-mode tests run immediately |
| `connector-spec-generator` agent | **Unchanged** | |
| `connector-doc-writer` agent | **Unchanged** | |
| `connector-tester` agent | **Updated** | Accepts `mode={simulate\|record}`; iteration guidance branches by mode |
| `test-and-fix-connector` skill | **Updated** | Same `mode` param; simulate fixes either side, record treats live-validator drift as primary fix target |
| `authenticate-source` skill | **Unchanged** | Phase 2 only |
| `deploy-connector` skill | **Unchanged** | Phase 2 only |
| `create-connector` command | **Unchanged** | Still the end-user path |
| `batch-develop-connectors` command | **New** | Sequential batch loop, main session |
| `develop-connector` command | **New** | Single-source foreground version |
| `validate-connector` command | **New** | Phase 2 interactive entry point |

---

## Simulate-Mode Tests as Quality Gate

The Phase 1 quality gate is the existing per-connector test suite (`tests/unit/sources/{source}/test_{source}_lakeflow_connect.py`) run in **simulate mode**:

```
CONNECTOR_TEST_MODE=replay pytest tests/unit/sources/{source}/ -v
```

For this to work without credentials, two artifacts must exist:
1. `endpoints.yaml` (produced by `connector-dev` from the API research doc).
2. A corpus under `source_simulator/specs/{source}/corpus/` — the `connector-tester` agent bootstraps this from the connector's `TABLE_SCHEMAS` via `tools.corpus_from_schema.write_corpus_from_schemas`.

This catches:
- Wrong/missing interface methods (the harness invokes them).
- Incorrect parameter wiring or pagination logic (the simulator returns shaped responses).
- Schema regressions (records are typed against `TABLE_SCHEMAS`).
- Malformed `connector_spec.yaml`.

What it does NOT catch (deferred to record mode):
- Drift between `endpoints.yaml` and the real API.
- Auth, rate-limit, and side-effect issues that only surface against live hosts.

If simulate tests fail, the Phase 1 agent does NOT open a PR. Instead it reports the failure and leaves the branch in place so the developer can investigate.

This prevents broken connectors from entering the `needs-live-testing` queue.

---

## Implementation Plan

Build in this order:

1. **`develop-connector` command** — single-source Phase 1; test the pipeline end-to-end on one connector first
2. **`batch-develop-connectors` command** — wraps `develop-connector` logic in a loop with a summary
3. **`validate-connector` command** — Phase 2 extraction from `create-connector`; simplest

Each can be built and tested independently before the next.
