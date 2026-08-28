---
name: test-and-fix-connector
description: "Single step only: run the per-source pytest suite, diagnose failures, and fix the connector or simulator until everything passes. Branches on mode={simulate|record}. Do NOT use for full connector creation — use the create-connector agent instead."
disable-model-invocation: true
---

# Test and Fix the Connector

## Goal

Validate the **{{source_name}}** connector by running its pytest suite
and applying minimal fixes until everything passes. The skill operates
in one of two modes; the invocation prompt tells you which.

| Mode       | When                              | Inputs                                                                      | Diagnostic posture                                                                                          |
|------------|-----------------------------------|------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| `simulate` | Phase 1 (`/develop-connector`)    | Connector + `endpoints.yaml` exist; no creds                                 | Either the connector or the simulator spec/corpus could be wrong — judge each failure on the merits         |
| `record`   | Phase 2 (`/validate-connector`)   | Live creds; cassette and live validator are authoritative                    | Live-validator drift is the primary signal — prefer fixing the simulator spec/corpus over the connector     |

Read the mode you were given and follow only the sections marked for it.

## 0. Rules (both modes)

- **Write tests first, then run.** Do NOT run exploratory scripts (e.g. `python -c "..."`, `which python`, import checks) before the test file exists. Read the implementation source and reference test — that is sufficient.
- **Run pytest synchronously** — never in background. Do not use `sleep`, poll loops, `ps aux | grep pytest`, `wc -l`, or `tail` on output files. Run `pytest ...` directly and block until it completes.
- Only run standalone scripts to isolate a specific failing HTTP call *after* pytest has already failed and pointed to the problem.

## 1. Setup Test Files (both modes — skip if file already exists)

Create `tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py`. **Always follow `tests/unit/sources/example/test_example_lakeflow_connect.py` as the primary example** — it shows the correct pattern: subclass `LakeflowConnectTests`, set `connector_class`, set `simulator_source = "{source_name}"`, set `replay_config = {...}` with stand-in credentials, and pass a small `sample_records` (e.g. 5).

### Partitioned Connector Tests
If the connector implements `SupportsPartition` or `SupportsPartitionedStream` (check the connector class definition for these mixins), the test class **must** also inherit from `SupportsPartitionedStreamTests` alongside `LakeflowConnectTests`.

### Write-back Tests (opt-in)
Write-back tests live in a **separate mixin** (`tests/unit/sources/test_write_back_suite.LakeflowConnectWriteBackTests`) and require `CONNECTOR_TEST_MODE=live` against a real source. Do **not** mix it in for read-only connectors — the default `LakeflowConnectTests` is enough. Write-back support is set up in a different step (see `write-back-testing` skill).

---

## SIMULATE MODE

You're in this mode when the invocation says `mode=simulate`. Skip this whole section if you're in record mode.

### S1. Confirm Inputs

Verify these exist before running anything:

- `src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py`
- `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`

If `endpoints.yaml` is missing, that's a `connector-dev` defect — report and stop. Do not author it yourself.

### S2. Bootstrap the Corpus from `TABLE_SCHEMAS`

```bash
source .venv/bin/activate 2>/dev/null || (python3.10 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]" -q)
```

Then synthesize a corpus from the connector's Spark schemas:

```bash
python -c "
from pathlib import Path
from databricks.labs.community_connector.sources.{source_name}.{source_name} import {SourceName}LakeflowConnect
from databricks.labs.community_connector.source_simulator.tools.corpus_from_schema import write_corpus_from_schemas
connector = {SourceName}LakeflowConnect({{...stand-in creds matching test class replay_config...}})
write_corpus_from_schemas(
    connector=connector,
    output_dir=Path('src/databricks/labs/community_connector/source_simulator/specs/{source_name}/corpus'),
    records_per_table=5,
)
"
```

Records are shape-correct but synthetic. They're enough for shape-level tests; record mode replaces them with real data later.

### S3. Run Simulate-Mode Tests

```bash
pytest tests/unit/sources/{source_name}/ -v
```

No `CONNECTOR_TEST_MODE` env var (default = simulate). No credentials, no network.

### S4. Diagnose and Fix

Failures in simulate mode could mean:

- **Connector bug** — wrong method signature, malformed request shape, off-by-one in pagination logic, schema mismatch with what `read_table` actually yields.
- **Spec bug** — `endpoints.yaml` declares wrong `wrapper.records_key`, wrong `pagination_style`, missing param `role`, or missing endpoint.
- **Corpus bug** — primary keys not unique, cursor field not monotonic. Usually re-bootstrap rather than hand-edit.

Both sides are unvalidated until record mode runs. Use the test output and API doc to decide which is wrong. When in doubt, re-read the relevant `endpoints.yaml` against the connector's actual `_make_request` to find the asymmetry.

Iterate until all tests pass. Then exit.

---

## RECORD MODE

You're in this mode when the invocation says `mode=record`. Skip this whole section if you're in simulate mode.

### R1. Confirm Inputs

Verify these exist:

- `src/databricks/labs/community_connector/sources/{source_name}/{source_name}.py`
- `src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml`
- A test file `tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py`
- Live credentials reachable via `CONNECTOR_TEST_CONFIG_JSON` or `CONNECTOR_TEST_CONFIG_PATH` (or a `dev_config.json` written by `authenticate-source`)

### R2. Run Record-Mode Tests

```bash
source .venv/bin/activate
CONNECTOR_TEST_MODE=record \
  CONNECTOR_TEST_CONFIG_PATH=~/secrets/{source_name}.json \
  pytest tests/unit/sources/{source_name}/ -v
```

This:

1. Hits the live API via the connector — exactly what production does.
2. Writes a cassette of every request/response pair to `tests/unit/sources/{source_name}/cassettes/`.
3. Runs the **live validator**: every response is diffed against what the spec + corpus would produce; per-endpoint drift is reported next to the cassette as `<cassette>.validation.json`.
4. Records a coverage report at `<cassette>.coverage.json` — endpoints listed in spec but never hit, or hit but absent from spec, surface as gaps.

### R3. Re-seed the Corpus from the Cassette

```bash
python -m databricks.labs.community_connector.source_simulator.tools.cassette_to_corpus \
  --cassette tests/unit/sources/{source_name}/cassettes/Test{SourceName}Connector.json \
  --spec    src/databricks/labs/community_connector/source_simulator/specs/{source_name}/endpoints.yaml \
  --output  src/databricks/labs/community_connector/source_simulator/specs/{source_name}/corpus
```

This replaces the synthesized Phase 1 corpus with real records — subsequent simulate-mode runs serve realistic data.

### R4. Iterate on Spec Drift

The validation report names every endpoint where the spec disagrees with reality. **Fix the spec first; fix the connector only when the API call itself failed**.

- **Wrapper mismatch** — adjust `response.wrapper.records_key` or `extras`.
- **Pagination mismatch** — adjust `response.pagination_style`, or reach for `handler:` if the API doesn't fit any declarative style.
- **Filter param mismatch** — adjust `params.<name>.{role, op, field}`.
- **URL mismatch** — coverage shows requests with no matching spec entry; add the missing endpoint.

Re-run record mode after each spec edit. The cassette + corpus refresh as new requests get exercised.

### R5. Table Options (`dev_table_config.json`)

Optional, gitignored. If present, it's loaded by the test harness and passed as `table_options` per table.

**CRITICAL when running in record mode:** tests often hit large-data accounts. Constrain query scope and admission control to keep record runs fast and deterministic:

- Connector uses a sliding window (`window_seconds`, `window_hours`, etc.) → set to a tight value, e.g. 60s.
- Connector exposes `max_records_per_batch` → set to a small value (e.g. 5) so the batch terminates quickly.

Read the connector source code, identify the relevant option names, and configure them yourself. Do not wait for the user.

### R6. Final Simulate-Mode Pass

After validator drift is gone and the cassette is fresh, run a final default-mode pass to confirm everything still works offline:

```bash
pytest tests/unit/sources/{source_name}/ -v
```

This must also be green before exiting.

---

## Debugging Hangs and Slow Tests (record mode mostly)

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

6. **Timeout means the query bound is too wide** — if a request times out, do NOT increase the timeout. Instead, halve the window or limit in `dev_table_config.json` and retry. Keep halving until the request succeeds.

7. **Suspect large-account behavior** — test credentials may connect to an account with millions of records. If queries time out:
   - First, check if the connector already has `table_options` (like `window_seconds`, `limit`, or `max_records_per_batch`) and ensure they are set to very small values in your `dev_table_config.json`.
   - If the connector lacks these mechanisms, add server-side date filtering or switch to the sliding time-window pattern (see implement-connector SKILL) to constrain the query.

## Merge files

After completion, run `python tools/scripts/merge_python_source.py {source_name}` to re-generate the merged connector file `_generated_{source_name}_python_source.py`.
