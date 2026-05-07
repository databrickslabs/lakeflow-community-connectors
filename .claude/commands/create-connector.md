Build a new community connector end-to-end in a single interactive session (research → auth → implement → test → docs → deploy).

> **Experimental — not recommended for developers.** This command is end-user-oriented: it blocks on credential collection between research and implementation, which prevents batch automation and forces the developer to be present throughout. For most development work, prefer the two-phase developer flow:
>
> - `/develop-connector <source_name>` — research, implement, test in simulate mode, open a PR (no credentials needed).
> - `/validate-connector <source_name>` — authenticate, run record-mode tests against the live source, optionally deploy.
>
> Use `/create-connector` only when you intentionally want a one-shot interactive flow.

Usage: /create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]

Arguments: $ARGUMENTS

---

Parse arguments: first positional = **source_name** (required, lowercase); `tables=` = comma-separated tables (optional); `doc=` = API doc URL or path (optional). Stop and ask if source_name is missing.

Paths: `SRC=src/databricks/labs/community_connector/sources/{source_name}`, `TESTS=tests/unit/sources/{source_name}`

## Protocols

**Plan first**: Present tables in scope + 6-step workflow. Hard-stop with `AskUserQuestion`: "Does this plan look good?" ("Yes, proceed" / "I have adjustments"). Do NOT start Step 1 until confirmed.

**Task tracking**: Once confirmed, `TaskCreate` for all 6 steps. Mark `in_progress` before launching each step, `completed` after.

**Confirmation gate** (steps 1–5): After each step, commit all new/modified files under `SRC` and `TESTS` with a message like `feat({source_name}): step N - <short description>`. Then `AskUserQuestion` with a summary of what was produced (files created, tables found, test results). Options: "Continue" / "Review first". Do NOT proceed without confirmation. Step 6 skips the gate.

**Subagent pattern**: `Task(subagent_type=..., run_in_background=true)` → wait for automatic completion notification — do **NOT** poll using `TaskOutput`, `sleep`, or `cat` on the output file. Once notified, verify output files with `Glob`. Every subagent prompt must include: source name, all relevant file paths, and table scope. Subagents have no prior context.

---

## Step 1 — API Research
Subagent: `source-api-researcher` → `{SRC}/{source_name}_api_doc.md`

Prompt: source name, doc URL/path (if any), table scope. Tell it not to ask the user.
Gate: summarize tables and auth method found.

---

## Step 2 — Auth Setup

Run the `/authenticate-source` skill. Read and follow `.claude/skills/authenticate-source/SKILL.md`.
Finish all the steps in the skill sequentially.

Gate: confirm auth test passes.

---

## Step 3 — Implementation
Subagent: `connector-dev` → python files under `{SRC}/`

Prompt: source name, API doc path, tables to implement.
Gate: verify implementation file(s) exist.

---

## Step 4 — Simulator spec, record-mode seed, then default-mode tests

Subagent: `connector-tester` → produces:
- `{TESTS}/test_{source_name}_lakeflow_connect.py` (subclasses
  `LakeflowConnectTests`, sets `simulator_source = "{source_name}"`,
  declares stand-in `replay_config = {...}`).
- `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`
  (one entry per HTTP path the connector hits; correct param roles,
  pagination style, response wrapper).
- `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/corpus/*.json`
  (seeded from a record-mode run if credentials are available, else
  synthesized from connector schemas via `corpus_from_schema`).

Prompt: source name, implementation path, and the credentials path
(the local JSON file the user picked when running
``collect-credentials``) so the subagent can run record mode if
applicable.

After subagent: run `pytest {TESTS}/ -v --tb=short` yourself using a
**synchronous** Bash call with `timeout=60000` (60s). No env vars —
default mode is simulate-only. Never run pytest in background. Never
use `sleep`, `tail`, `wc -l`, or `ps aux` to monitor it. If pytest
times out, do NOT increase the timeout — instead tighten
`dev_table_config.json` (halve `window_hours`, `lookback_days`, or
`max_records_per_batch`) and retry. If tests fail, do NOT proceed —
report failure to user.

Gate: confirm all tests pass in default (simulate) mode and the spec
validation report from the record-mode run had no unresolved drift.

---

## Step 5 — Docs + Complete Spec

**5a.** Subagent: `connector-doc-writer` → `{SRC}/README.md`
Prompt: source name, implementation and API doc paths.

**5b.** Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml` (complete with `external_options_allowlist`)
Prompt: source name, implementation path.

Gate: verify both files exist.

**Post-gate**: After confirmation, use `AskUserQuestion` — "The connector is fully developed, tested, and documented. Step 6 (packaging & deployment) is optional." Options: "Proceed with deployment" / "Stop here". If they stop, skip to Final Summary.

---

## Step 6 — Deployment

Run the `/deploy-connector` skill. Read and follow `.claude/skills/deploy-connector/SKILL.md`.
Pass the source name with `use_local_source=true`. Finish all the steps in the skill sequentially.
This is an interactive process — ask the user for input at each step rather than assuming values.

---

## Final Summary

```
Connector: {source_name}
Tables:    [list]
Source:    src/databricks/labs/community_connector/sources/{source_name}/
Tests:     tests/unit/sources/{source_name}/
```

If a subagent fails (e.g. couldn't write its output file), report the failure clearly to the user — do not attempt to redo the subagent's work yourself. If the user wants to resume from a step, skip earlier ones.
