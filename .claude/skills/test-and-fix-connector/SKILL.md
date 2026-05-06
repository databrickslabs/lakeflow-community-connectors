---
name: test-and-fix-connector
description: "Single step only: author the simulator spec, seed/validate it via a record run, and run/fix connector tests. Do NOT use for full connector creation — use the create-connector agent instead."
disable-model-invocation: true
---

# Test and Fix the Connector

## Goal
Validate the **{{source_name}}** connector by (a) authoring an
in-process source-simulator spec, (b) seeding its corpus and
validating the spec via a record-mode run against a real source
instance, and then (c) running the standard pytest suite in default
simulate mode, diagnosing failures, and applying minimal fixes until
all tests pass.

## 0. Rules
- **Write tests first, then run.** Do NOT run exploratory scripts (e.g. `python -c "..."`, `which python`, import checks) before the test file exists. Read the implementation source and reference test — that is sufficient.
- **Run pytest synchronously** — never in background. Do not use `sleep`, poll loops, `ps aux | grep pytest`, `wc -l`, or `tail` on output files. Run `pytest ...` directly and block until it completes.
- Only run standalone scripts to isolate a specific failing HTTP call *after* pytest has already failed and pointed to the problem.

## 1. Setup Test Files
Create a `test_{source_name}_lakeflow_connect.py` under the `tests/unit/sources/{source_name}/` directory.
**Always follow `tests/unit/sources/example/test_example_lakeflow_connect.py` as the primary example** — it shows the correct pattern: subclass `LakeflowConnectTests`, set `connector_class`, set `simulator_source = "{source_name}"`, set `replay_config = {...}` with stand-in credentials, and pass a small `sample_records` (e.g. 5).

### Partitioned Connector Tests
If the connector implements `SupportsPartition` or `SupportsPartitionedStream` (check the connector class definition for these mixins), the test class **must** also inherit from `SupportsPartitionedStreamTests` alongside `LakeflowConnectTests`. This adds partition-specific test coverage (partition key validation, partitioned reads, micro-batch convergence, etc.).

## 2. Author the Simulator Spec

Tests run against an in-process simulator by default — no network, no
credentials. The simulator is driven by a per-source endpoint spec
plus a corpus of stand-in records. Both live next to the framework,
*not* under `tests/`:

```
src/databricks/labs/community_connector/source_simulator/specs/{source_name}/
├── endpoints.yaml          # spec — author this manually (see below)
└── corpus/
    └── *.json              # one file per "table" returned by the connector
```

**Reference patterns:**

- `src/databricks/labs/community_connector/source_simulator/DESIGN.md` —
  full reference for `endpoints.yaml` syntax: filter / sort / page /
  per_page / offset / limit / ignore param roles, response wrappers
  (with dotted `records_key` like `d.results` for OData), pagination
  styles (`none`, `page_number`, `page_number_with_link_header`,
  `offset_limit`), and the `handler:` escape hatch for endpoints that
  don't fit declarative shape.
- `source_simulator/specs/github/endpoints.yaml`,
  `source_simulator/specs/qualtrics/endpoints.yaml`, and
  `source_simulator/specs/sap_successfactors/endpoints.yaml` are good
  read-along examples spanning REST + OData v2.

Create one endpoint entry per URL the connector hits. Read the
connector's `_make_request` (or equivalent) and the source API doc to
identify each path, method, query params, and response wrapper shape.

**Optional: cap-validation directive.** If the connector caps its
returned cursor at an init-time snapshot (admission-control pattern),
add `synthesize_future_records: {cursor_field: <dotted path>, count: 3}`
to that endpoint. The simulator clones records with cursor values
past wall-clock now() at startup; the connector's `until=<init_time>`
filter must exclude them or `test_read_terminates` won't converge.

## 3. Record-Mode Run: Seed Corpus & Validate Spec

Once the spec is authored, run pytest in **record mode** against a
real source instance. This:

1. Hits the live API via the connector — exactly what production does.
2. Writes a cassette of every request/response pair to
   `tests/unit/sources/{source_name}/cassettes/`.
3. Runs the **live validator**: every response is diffed against
   what the spec + corpus would produce, and per-endpoint drift is
   reported next to the cassette as `<cassette>.validation.json`.
4. Records a coverage report next to the cassette
   (`<cassette>.coverage.json`) — endpoints listed in spec but never
   hit, or hit but absent from spec, surface as gaps.

**Credentials are passed at run time via env var.** The repo no
longer carries any per-source credentials file; ask the user to
either:

- Inline the JSON: `CONNECTOR_TEST_CONFIG_JSON='{"...":"..."}'`
- Or point at a local file at any path they choose:
  `CONNECTOR_TEST_CONFIG_PATH=~/secrets/{source_name}.json`

Then run:

```bash
source .venv/bin/activate
CONNECTOR_TEST_MODE=record \
  CONNECTOR_TEST_CONFIG_PATH=~/secrets/{source_name}.json \
  pytest tests/unit/sources/{source_name}/ -v
```

After the run, **seed the corpus from the cassette** so subsequent
default-mode runs have records to serve:

```bash
python -m databricks.labs.community_connector.source_simulator.tools.cassette_to_corpus \
  --cassette tests/unit/sources/{source_name}/cassettes/Test{SourceName}Connector.json \
  --spec    src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml \
  --output  src/databricks/labs/community_connector/source_simulator/specs/{source_name}/corpus
```

If you don't have credentials at all, fall back to the
no-creds bootstrap: synthesize the corpus from the connector's
`get_table_schema` output via
`source_simulator.tools.corpus_from_schema.write_corpus_from_schemas`.
Records from this path are shape-correct but synthetic — the live
validator can't run, so spec drift surfaces only on a future
record-mode run.

### Iterate on spec drift

The validation report names every endpoint where the spec's response
shape disagrees with reality. For each diff:
- **Wrapper mismatch** — adjust `response.wrapper.records_key` or
  `extras`.
- **Pagination mismatch** — adjust `response.pagination_style`, or
  reach for `handler:` if the API doesn't fit any declarative style.
- **Filter param mismatch** — adjust `params.<name>.{role, op, field}`.
- **URL mismatch** — coverage shows requests with no matching spec
  entry; add the missing endpoint.

Re-run record mode after each spec edit. The cassette + corpus
refresh as new requests get exercised.

### Table options (`dev_table_config.json`)

Optional, gitignored. If present, it's loaded by the test harness and
passed as `table_options` per table.

**CRITICAL when running in record/live mode:** tests often hit
large-data accounts. Constrain query scope and admission control to
keep record runs fast and deterministic:
- Connector uses a sliding window (`window_seconds`, `window_hours`,
  etc.) → set to a tight value, e.g. 60s.
- Connector exposes `max_records_per_batch` → set to a small value
  (e.g. 5) so the batch terminates quickly.

Read the connector source code, identify the relevant option names,
and configure them yourself. Do not wait for the user.

## 4. Default Simulate-Mode Run

Once the spec validates clean and the corpus is seeded, the everyday
flow is just:

```bash
source .venv/bin/activate
pytest tests/unit/sources/{source_name}/ -v
```

No credentials, no network, no cassette involvement. The simulator
serves responses straight from `endpoints.yaml` + `corpus/*.json`.

This is what CI runs on every PR. Failures here always indicate
either a connector bug or a spec/corpus drift from reality — both
worth fixing.

## 5. Fix Failures

Based on test output, update either:

- The implementation under
  `src/databricks/labs/community_connector/sources/{source_name}/` —
  for connector-side bugs.
- The spec at
  `source_simulator/specs/{source_name}/endpoints.yaml` — for
  request-shape or response-wrapper mismatches.
- The corpus at `source_simulator/specs/{source_name}/corpus/*.json`
  — for missing or shape-incorrect records (rare; usually re-seed
  via cassette_to_corpus instead of editing by hand).

Use both the test results and the source API documentation, as well
as any relevant libraries and test code, to guide your corrections.

## Debugging Hangs and Slow Tests

Tests that hang (no output, no error) are almost always an API call that never returns. Systematic approach:

1. **Enable debug logging** to see where it stalls:
```bash
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v -s --log-cli-level=DEBUG
```
If `urllib3` logs `Starting new HTTPS connection` with no response line, the HTTP call itself is hanging.

2. **Isolate the HTTP call** — reproduce the exact request (same URL, headers, params) in a standalone script. If that also hangs, the problem is the query parameters, not the connector logic.

3. **Narrow down by elimination** — remove or change one query parameter at a time. Common culprits: unbounded history scans (`date_range=all`), ascending sort on large datasets, missing date-range filters.

4. **Start with the most constrained query** — small `limit`, narrow time window, status filters. Once it works, progressively relax to find the boundary.

5. **Check for missing timeouts** — every `requests.get()`/`session.get()` must have a `timeout` parameter. Without it, a slow API hangs forever with no error. For testing, set a short timeout (e.g., 10 seconds) so failed requests surface quickly instead of blocking for minutes.

6. **Timeout means the query bound is too wide** — if a request times out, do NOT increase the timeout. Instead, halve the window or limit in `dev_table_config.json` and retry. Keep halving until the request succeeds. For example: start at `window_hours=1`, then try `0.5` (30 min), `0.25` (15 min), etc. The right fix is always a tighter query, not a longer wait.

7. **Suspect large-account behavior** — test credentials may connect to an account with millions of records. If queries time out:
   - First, check if the connector already has `table_options` (like `window_seconds`, `limit`, or `max_records_per_batch`) and ensure they are set to very small values in your `dev_table_config.json`.
   - If the connector lacks these mechanisms, add server-side date filtering or switch to the sliding time-window pattern (see implement-connector SKILL) to constrain the query.

## Merge files
After completion, run `python tools/scripts/merge_python_source.py {source_name}` to re-generate the merged connector file `_generated_{source_name}_python_source.py`.
