---
name: self-review-connector
description: "Single step only: audit a completed connector — implementation, testing & simulator validation, artifacts, security smells, cross-doc consistency — and produce a scored markdown review report. Read-mostly; does not modify connector code."
disable-model-invocation: true
---

# Self-Review the Connector

## Goal

Mechanically audit the **{{source_name}}** connector and emit a
markdown report at
`tests/unit/sources/{{source_name}}/SELF_REVIEW.md` containing:

- Overall score (0–100) and verdict (`READY` / `ALMOST` /
  `NEEDS WORK` / `NOT READY`).
- Top 3–5 prioritized recommendations.
- Per-section findings with `path:line` citations so the developer
  can navigate to each spot in seconds.

The skill **does not modify connector code or run agents**. Output is
the input to a fix loop, not part of one.

## When to use

After `connector-dev` → `connector-tester` → `connector-doc-writer`
→ `connector-spec-generator`. Run before declaring the connector
ready to ship, or any time the developer wants a checkpoint.

## Inputs

- `source_name` (required).
- Optional: report path. Default
  `tests/unit/sources/{{source_name}}/SELF_REVIEW.md`. The default
  path is gitignored — same convention as `dev_table_config.json`.
- PR auto-detection. When run on a branch that has an open PR, the
  skill posts the report as a sticky comment and adds the
  `connector-self-reviewed` label on pass. The repo's CI workflow
  (`.github/workflows/connector-self-review.yml`) requires that
  label to merge — see **Posting to GitHub** below.

## Severity & scoring

Every check resolves to `pass` / `warn` / `fail`. Apply weights:

| Severity | `pass` | `warn` | `fail` |
|---|---|---|---|
| BLOCKER | +3 | −3 | −10 |
| MAJOR   | +2 | −1 | −3  |
| MINOR   | +1 |  0 | −1  |

Sum points; normalize to 0–100 against the max (all checks pass).
**Any BLOCKER `fail` caps the verdict at `NOT READY`** regardless of
point total — abstract methods missing or tests failing means the
connector cannot ship even if everything else is perfect.

Bands:

- **90+ — READY**: minor polish only.
- **75–89 — ALMOST**: 1–2 MAJOR findings to address.
- **50–74 — NEEDS WORK**: meaningful gaps.
- **<50 or any BLOCKER fail — NOT READY**.

## Procedure

For each section below, run the listed checks in order. For each
check, record one line in the report:

```
✅ A1. <one-line description> — `<file>:<line>`
⚠️ A7. 3 of 11 HTTP calls lack `timeout=` — `source.py:142, 178, 256`
❌ E2. Connection-parameter naming mismatch (see Top recommendations)
```

Cite a path on every finding except where genuinely none exists
(e.g. "no record-mode run on file"). Citations are what make the
report actionable.

---

### Section A — Connector implementation (10 checks)

Source under audit:
`src/databricks/labs/community_connector/sources/{{source_name}}/{{source_name}}.py`

| # | Severity | What | How |
|---|---|---|---|
| A1 | BLOCKER | Class extends `LakeflowConnect` | `grep -nE "class .*\\(LakeflowConnect\\b" src/.../sources/{{source_name}}/{{source_name}}.py` |
| A2 | BLOCKER | Abstract methods present (`list_tables`, `get_table_schema`, `read_table_metadata`, `read_table`) | `grep -nE "def (list_tables\\|get_table_schema\\|read_table_metadata\\|read_table)\\(" ...` |
| A3 | BLOCKER | `read_table_deletes` implemented if any table is `cdc_with_deletes` | parse `read_table_metadata` returns; grep `cdc_with_deletes`; if found, require `def read_table_deletes` |
| A4 | MAJOR | Read pattern is a deliberate choice — one of: standard, sliding window (Strategy A), server-side limit (Strategy B), or partitioned stream | look for `_read_incremental_by_window` / `_read_incremental_by_limit` / `SupportsPartitionedStream` mixin / single-page snapshots |
| A5 | MAJOR | Termination contract: `_init_time` cap present for incremental tables | `grep -nE "_init_time\\s*=" ...`; verify a guard `cursor >= self._init_time → return iter([]), same_offset` |
| A6 | MAJOR | Admission control: every incremental table reads `max_records_per_batch` from `table_options`. Append-only tables use server-side limit (Strategy B), not client-side truncation | `grep -nE "max_records_per_batch" ...` cross-checked with ingestion type per table |
| A7 | MAJOR | All `requests.get/post/put/delete/patch` and `Session.*` calls have `timeout=` | `grep -nE "(requests\\.\\|self\\.session\\.)(get\\|post\\|put\\|delete\\|patch)\\(" ...` then visual check |
| A8 | MINOR | Schemas use `LongType` not `IntegerType`; `StructType` not `MapType` | `grep -nE "IntegerType\\|MapType" ...` |
| A9 | MINOR | `get_table_schema` does not flatten nested fields | spot-check schemas for `StructType` returns |
| A10 | MAJOR | Imports clean — no imports outside `sources/{{source_name}}/`, `interface/`, `libs/`, `requests`, `pyspark`, std-lib | `grep -nE "^(from\\|import) " ...` |

---

### Section B — Testing & simulator validation (14 checks)

Test file: `tests/unit/sources/{{source_name}}/test_{{source_name}}_lakeflow_connect.py`
Spec dir: `src/databricks/labs/community_connector/source_simulator/specs/{{source_name}}/`

| # | Severity | What | How |
|---|---|---|---|
| B1 | BLOCKER | Test file exists | file existence |
| B2 | BLOCKER | Subclasses `LakeflowConnectTests` (and `SupportsPartitionedStreamTests` if connector is partitioned) | grep |
| B3 | BLOCKER | `connector_class` set | grep |
| B4 | BLOCKER | `simulator_source = "{{source_name}}"` set | grep |
| B5 | BLOCKER | `replay_config = {...}` set; keys match every `options.get("...")` in connector `__init__` | grep + parse `__init__` |
| B6 | BLOCKER | `endpoints.yaml` exists in spec dir | file existence |
| B7 | BLOCKER | At least one corpus JSON file in `corpus/` | listing |
| B8 | MAJOR | Every URL the connector hits has a matching spec entry. Walk every `requests.get/post(...)` literal in connector source; URL pattern must match a `path:` in `endpoints.yaml` | static URL extraction + spec match |
| B9 | BLOCKER | `test_read_terminates` passes in default (simulate) mode | `pytest tests/unit/sources/{{source_name}}/ -k test_read_terminates -q` |
| B10 | BLOCKER | All other tests pass in default mode | `pytest tests/unit/sources/{{source_name}}/ -q` |
| B11 | MAJOR | **Live/record mode has been run at least once and validated cleanly.** See "Detecting record-mode evidence" below | parse `cassettes/*.json.validation.json` |
| B12 | MINOR | Coverage is full — no spec endpoints went unhit | parse `cassettes/*.json.coverage.json` |
| B13 | MAJOR | If the connector caps cursor at `_init_time` (A5 passed), the spec for cursor-bearing endpoints declares `synthesize_future_records: {cursor_field, count}` so `test_read_terminates` actually exercises the cap | grep `endpoints.yaml` for the directive |
| B14 | MINOR | `test_utils_class` is set when connector supports write-back (`list_insertable_tables` returns non-empty) | grep + cross-check |

#### Detecting record-mode evidence (B11)

Live/record runs of the simulator emit (next to the test file):

```
tests/unit/sources/{{source_name}}/cassettes/Test{{Source}}Connector.json
tests/unit/sources/{{source_name}}/cassettes/Test{{Source}}Connector.json.coverage.json
tests/unit/sources/{{source_name}}/cassettes/Test{{Source}}Connector.json.validation.json
```

Gitignored — they exist only on the developer's machine after they
ran record mode. **They are the evidence.**

For B11:

1. Look for any `cassettes/*.json.validation.json`. If none → B11 =
   `fail`: "No record-mode run on file. Run
   `CONNECTOR_TEST_MODE=record CONNECTOR_TEST_CONFIG_PATH=… pytest …`."
2. Parse the most recent validation report. **If the report has a
   top-level `summary` block with `status: clean | drift`** (the
   recommended enhancement to `validator.py`), use that directly.
   Otherwise fall back to counting per-endpoint drift entries.
3. If drift count > 0 → `fail` listing drifting endpoints.
4. Compare validation-report mtime against
   `src/.../sources/{{source_name}}/{{source_name}}.py` mtime and
   `endpoints.yaml` mtime. If validation report is older → `warn`:
   "Record run is stale relative to current connector / spec; re-run
   record mode."

For B12: same shape — read `coverage.json`, list endpoints in spec
that weren't hit (`coverage.endpoints_in_spec − coverage.endpoints_hit`).

---

### Section C — Artifacts (9 checks)

| # | Severity | Artifact | Path | Validation |
|---|---|---|---|---|
| C1 | BLOCKER | Implementation | `src/.../sources/{{source_name}}/{{source_name}}.py` | exists; `python -m py_compile` clean |
| C2 | BLOCKER | API doc | `src/.../sources/{{source_name}}/{{source_name}}_api_doc.md` | exists; non-empty; mentions every table from `list_tables()` |
| C3 | BLOCKER | Connector spec | `src/.../sources/{{source_name}}/connector_spec.yaml` | YAML parses; has `connection_parameters`; has `external_options_allowlist` if connector reads `table_options` keys |
| C4 | BLOCKER | Public README | `src/.../sources/{{source_name}}/README.md` | exists; non-empty; mentions every table and every connection parameter |
| C5 | BLOCKER | Package metadata | `src/.../sources/{{source_name}}/pyproject.toml` | TOML parses; `dependencies` includes `requests` (and any other live deps the connector imports) |
| C6 | MINOR | Generated merged source | `src/.../sources/{{source_name}}/_generated_{{source_name}}_python_source.py` | exists; mtime ≥ `{{source_name}}.py` mtime (re-run `python tools/scripts/merge_python_source.py {{source_name}}` if stale) |
| C7 | BLOCKER | Simulator spec | `src/.../source_simulator/specs/{{source_name}}/endpoints.yaml` | YAML parses; loadable via `endpoint_spec.load_specs` |
| C8 | BLOCKER | Simulator corpus | `src/.../source_simulator/specs/{{source_name}}/corpus/*.json` | every file is valid JSON |
| C9 | BLOCKER | Test class | `tests/unit/sources/{{source_name}}/test_{{source_name}}_lakeflow_connect.py` | (covered by B1) |

---

### Section D — Security (10 checks)

Static-analysis pass for obvious smells. Does **not** replace a real
security review for high-risk integrations.

Search scope: `src/databricks/labs/community_connector/sources/{{source_name}}/`

| # | Severity | What | How |
|---|---|---|---|
| D1 | BLOCKER | No hardcoded secrets | grep for: `api_key=` / `password=` / `token=` followed by a literal; `Bearer ` followed by a literal; `xox[baprs]-`; `gh[ps]_[A-Za-z0-9]{36,}`; `AKIA[0-9A-Z]{16}` |
| D2 | BLOCKER | No `eval` / `exec` / `compile` / `__import__` of dynamic strings | `grep -nE "\\b(eval\\|exec\\|compile)\\(" ...` |
| D3 | MAJOR | No `subprocess`, `os.system`, `os.popen`, `shell=True` (or, if present, justified) | `grep -nE "subprocess\\\|os\\.system\\\|os\\.popen\\\|shell=True" ...` |
| D4 | MAJOR | No `verify=False` on `requests` calls | `grep -n "verify=False" ...` |
| D5 | BLOCKER | No `pickle.load(s)` on untrusted data (response bodies) | `grep -nE "pickle\\.load" ...` |
| D6 | MAJOR | YAML uses `safe_load`, not `load` | `grep -nE "yaml\\.load\\(" ...` (without `safe_`) |
| D7 | MAJOR | No path traversal: `Path(table_options[...])` or `open(table_options[...])` patterns without sanitization | grep + visual review |
| D8 | MAJOR | No secrets in logs — `logger.*(...)` containing `headers`, `auth`, `token`, `password`, `secret`, or response bodies before scrubbing | `grep -nE "log(ger)?\\.(debug\\|info\\|warning\\|error)\\(.*(headers\\\|auth\\\|token\\\|password\\\|secret)" ...` |
| D9 | MAJOR | No `http://` base URLs in production code (test/spec/corpus paths exempt) | `grep -nE "http://" ...` |
| D10 | MINOR | Dependencies pinned in `pyproject.toml` (`>=X.Y,<Z.0`); no unbounded `>=X` | parse TOML |

---

### Section E — Cross-doc consistency (7 checks)

Three artifacts describe the connector at different levels: the
**API doc** (`<source>_api_doc.md`), the **public README**
(`README.md`), and the **code** (`<source>.py` +
`connector_spec.yaml`). They must agree.

| # | Severity | What | How |
|---|---|---|---|
| E1 | MAJOR | `list_tables()` ≡ tables in API doc ≡ tables in README | parse all three; set diffs |
| E2 | MAJOR | Connection-parameter keys: `__init__` `options.get("...")` ≡ `connector_spec.yaml` `connection_parameters` ≡ README mentions | parse all three; set diffs |
| E3 | MINOR | Per-table schemas: column names from `connector.get_table_schema(t, {})` (run via simulator) are a subset of fields in API doc | run `get_table_schema` against simulator; compare |
| E4 | MAJOR | Per-table primary keys: `read_table_metadata(t, {})['primary_keys']` matches API doc's natural identifier | parse both |
| E5 | MAJOR | `external_options_allowlist` includes every `table_options.get("...")` key the connector reads | grep source vs YAML |
| E6 | MINOR | README quick-start snippet uses connection-parameter keys that match `connector_spec.yaml` | parse README code blocks |
| E7 | MINOR | README does not contradict API doc on rate limits, pagination semantics, or auth method | best-effort textual; flag for human review |

---

## Output report format

Write the report to `tests/unit/sources/{{source_name}}/SELF_REVIEW.md`
with this layout:

```markdown
# Self-Review — {{source_name}}

**Overall: 87 / 100 — ALMOST**

Run at: 2026-05-06T15:32:00Z

## Top recommendations

1. **MAJOR** — `endpoints.yaml` is missing `synthesize_future_records`
   on `/repos/{owner}/{repo}/commits`. Without it,
   `test_read_terminates` doesn't actually verify the `_init_time`
   cap. *Fix: add `synthesize_future_records: {cursor_field:
   commit.author.date, count: 3}`.*
2. **MAJOR** — `__init__` reads `options.get("api_token")` but
   `connector_spec.yaml` lists the parameter as `token`. Real users
   following the README will fail.
   *Fix: align all three on one name.*
3. **MAJOR** — Last record-mode run was 23 days ago and connector
   source has been edited since. Re-run
   `CONNECTOR_TEST_MODE=record pytest …`.
4. **MINOR** — 3 `requests.get(...)` calls lack `timeout=` —
   `source.py:142, 178, 256`.

## A. Connector implementation — 27 / 30
- ✅ A1. Class extends LakeflowConnect — `source.py:34`
- ✅ A2. All abstract methods implemented
- ✅ A3. `read_table_deletes` implemented (1 cdc_with_deletes table)
- ✅ A4. Read pattern: sliding window (Strategy A) for `commits`,
       snapshot for `branches`
- ✅ A5. `_init_time` cap present — `source.py:67-72`
- ✅ A6. `max_records_per_batch` honored on all incremental tables
- ⚠️ A7. 3 of 11 HTTP calls lack `timeout=` — see Top recommendations
- ✅ A8–A10

## B. Testing & simulator validation — 22 / 26
- ✅ B1–B7
- ✅ B8. All 8 connector URLs have spec entries
- ✅ B9–B10. 47 passed / 3 skipped in default mode
- ⚠️ B11. Last record run: 2026-04-13. Status: clean (0 drift).
       **Stale.**
- ✅ B12. Coverage 100%
- ❌ B13. `synthesize_future_records` directive missing on commits
       endpoint
- ✅ B14

## C. Artifacts — 19 / 20
- ✅ C1–C8
- ⚠️ C6. `_generated_<source>_python_source.py` is older than
       `<source>.py` — re-run
       `python tools/scripts/merge_python_source.py <source>`

## D. Security — 14 / 15
- ✅ D1–D7
- ⚠️ D8. `logger.debug(f"Sending request: {request.headers}")` at
       `source.py:201`
- ✅ D9–D10

## E. Cross-doc consistency — 5 / 9
- ✅ E1, E3, E4, E6, E7
- ❌ E2. Connection-parameter naming mismatch — see Top recommendations
- ⚠️ E5. `external_options_allowlist` is missing `lookback_days`,
       `window_seconds`
```

## Posting to GitHub

The skill is the merge gate. It is **the only thing** that runs the
audit — there is no CI fallback. The companion workflow
(`.github/workflows/connector-self-review.yml`) just checks for the
label this skill applies.

### Phase 6 — Identify the PR

1. Detect the current branch:
   ```bash
   git rev-parse --abbrev-ref HEAD
   ```
2. Find the open PR via GitHub. The remote branch name may have a
   username prefix (e.g., `user/branch-name`), so search for PRs
   whose head branch contains the local branch name:
   ```bash
   gh pr list --repo databrickslabs/lakeflow-community-connectors \
     --state open --json number,url,headRefName \
     --jq '[.[] | select(.headRefName | contains("<branch>"))][0]'
   ```
3. If no PR is found, write the local report only and stop with:
   "No open PR found for current branch. Local report at <path>."

### Phase 7 — Post a sticky comment

Post the full report as a single PR comment. **Sticky**: find an
existing comment with the marker
`<!-- connector-self-review:<source_name> -->` at the top and
update in place; otherwise create a new comment. No spam on
re-runs.

```bash
# find existing
EXISTING_ID=$(gh api repos/{owner}/{repo}/issues/{number}/comments --paginate \
  | jq '.[] | select(.body | contains("<!-- connector-self-review:<source_name> -->")) | .id' \
  | head -1)

BODY="<!-- connector-self-review:<source_name> -->
$(cat <report path>)"

if [ -n "$EXISTING_ID" ]; then
  gh api --method PATCH "repos/{owner}/{repo}/issues/comments/$EXISTING_ID" -f body="$BODY"
else
  gh api --method POST "repos/{owner}/{repo}/issues/{number}/comments" -f body="$BODY"
fi
```

### Phase 8 — Label management (the merge gate)

This is the binary signal CI looks at. Two outcomes:

**On pass** (no BLOCKER fail; verdict ∈ {READY, ALMOST}):

```bash
gh pr edit <number> --add-label "connector-self-reviewed"
```

Then print:

```
Self-review passed (<verdict>). Label `connector-self-reviewed` added.
The CI gate will see the label and allow merge once other checks
pass. Sticky comment updated: <comment URL>.
```

**On fail** (any BLOCKER fail, or verdict ∈ {NEEDS WORK, NOT READY}):

1. Do **not** add the label.
2. **Remove** the label if it was previously present (this happens
   automatically on `synchronize` events too — see the workflow —
   but call it out so the developer isn't confused about a stale
   pass):
   ```bash
   gh pr edit <number> --remove-label "connector-self-reviewed" 2>/dev/null || true
   ```
3. Print the failure detail and which checks need fixing:
   ```
   Self-review FAILED with N BLOCKER fail(s) and M MAJOR finding(s).
   Top fixes:
     1. <first BLOCKER or MAJOR with citation>
     2. ...
   Fix the issues above and re-run /self-review-connector --source <name>.
   ```

### Why a label rather than a status check or inline review

- **Required status check** would need the audit to run on CI. The
  skill is Claude-powered; running Claude on every PR push is
  expensive, slow, and non-deterministic. We want the developer to
  run it locally, on demand, against their committed work.
- **Inline PR review** with `path:line` comments is nice for
  human-style feedback but doesn't naturally gate merge. Comments
  aren't a check.
- **A label** is a binary signal that GitHub's branch-protection
  rules can require via a workflow that simply checks the label
  exists. The companion workflow does that — see the design doc
  for shape. New pushes automatically remove the label, forcing
  the developer to re-review.

This pattern is borrowed verbatim from `plugin-marketplace`'s
`/plugin-self-review` skill, where the `plugin-self-reviewed` label
plays the same role.

If `gh` is not authenticated or the user lacks PR write permissions,
surface the error verbatim and skip Phases 7–8 — do **not** attempt
to retry or escalate. The local report is still useful.

## Rules

- **Read-mostly.** Do not modify connector code, simulator spec,
  corpus, or test files. The only write is the report.
- **No agents, no network.** Every check is local: `Read` / `Bash`
  with `grep`, `python -m py_compile`, `pytest`, JSON / YAML / TOML
  parsing.
- **Cite a path on every finding** (`file:line`) unless the finding
  is "thing does not exist," in which case state the expected path
  and the run-command that produces it.
- **Be concise.** Each finding is one line in the report. The Top
  Recommendations block is 3-5 items max — not the full list.
- **Run pytest synchronously.** For B9 / B10, never background;
  block on completion. If pytest hangs, do not increase timeout —
  flag the hang as a finding under section A or B as appropriate
  and continue with the rest of the audit.
- **Do not re-run record mode.** B11 reads the existing artifact;
  if the artifact doesn't exist, that's the finding. The skill is
  not authorized to spend the user's API quota.

## Scope boundaries

This skill audits the connector. It does **not**:

- Run any agent (no spawning).
- Modify code (read-mostly).
- Make network calls (no live API hits, no record mode).
- Replace a real security review for high-risk integrations.
- Gate CI — it's developer-invoked self-check.
