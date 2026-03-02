---
name: test-and-fix-connector
description: "Single step only: run and fix connector tests when the implementation already exists. Do NOT use for full connector creation — use the create-connector agent instead."
disable-model-invocation: true
---

# Test and Fix the Connector

## Goal
Validate the generated connector for **{{source_name}}** by executing the provided test suite, diagnosing failures, and applying minimal, targeted fixes until all tests pass.

## 1. Setup Test Files
Create a `test_{source_name}_lakeflow_connect.py` under the `tests/unit/sources/{source_name}/` directory.
Use `tests/unit/sources/test_suite.py` to run tests and follow `tests/unit/sources/example/test_example_lakeflow_connect.py` or other sources as an example.

## 2. Test Configuration (Credentials & Bounds)
Use the configuration files in `tests/unit/sources/{source_name}/configs/` to initialize your tests. Do **not** mock data; tests must connect to an actual instance.

### A. Authentication (`dev_config.json`)
If `dev_config.json` does not exist, create it and ask the developers to provide the required parameters to connect to a test instance of the source.
Example:
```json
{
  "user": "YOUR_USER_NAME",
  "password": "YOUR_PASSWORD",
  "token": "YOUR_TOKEN"
}
```

### B. Table Options (`dev_table_config.json`)
If it does not exist, create `dev_table_config.json` and supply the necessary `table_options` parameters for testing different cases.

**CRITICAL: Control query scope and batch size (do this automatically):** 
Tests often run against large data volume accounts. To avoid hangs and ensure tests complete quickly, you **must** constrain both the query scope and the batch size:
- **Query Scoping**: If the connector uses a sliding window or server-side limit (e.g., `window_seconds` or `limit`), inspect the connector implementation and add these options to `dev_table_config.json` with very small values (e.g., `window_seconds`: "60", `limit`: "5"). This ensures the query itself is bounded and prevents the API from scanning the entire dataset.
- **Admission Control**: Always find the option that controls the per-microbatch record limit (usually `max_records_per_batch`) and add it to `dev_table_config.json` with a small value (e.g., "5"). This makes the call return faster and avoids accumulating too many records.

Do **not** wait for the user to provide these — read the connector source code, identify the relevant option names, and configure them yourself.

*Note: Be sure to remove these config files after testing is complete and before committing any changes.*

## 3. Execution
Run the tests using the project virtual environment (Python 3.10+ required):
```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/unit/sources/{source_name}/test_{source_name}_lakeflow_connect.py -v
```

## 4. Fix Failures
Based on test failures, update the implementation under `src/databricks/labs/community_connector/sources/{source_name}` as needed. Use both the test results and the source API documentation, as well as any relevant libraries and test code, to guide your corrections.

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

6. **Suspect large-account behavior** — test credentials may connect to an account with millions of records. If queries time out:
   - First, check if the connector already has `table_options` (like `window_seconds`, `limit`, or `max_records_per_batch`) and ensure they are set to very small values in your `dev_table_config.json`.
   - If the connector lacks these mechanisms, add server-side date filtering or switch to the sliding time-window pattern (see implement-connector SKILL) to constrain the query.

## Merge files
After completion, run `python tools/scripts/merge_python_source.py {source_name}` to re-generate the merged connector file `_generated_{source_name}_python_source.py`. 
