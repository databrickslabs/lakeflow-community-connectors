# Self-Review — remberg

**Overall: 95 / 100 — READY**

Run at: 2026-08-06

Scope: PR #266 (`remberg-deferred-objects`) — six new sub-resource tables
via parent fan-out, plus the 0-indexed pagination fix.

## Top recommendations

1. **MAJOR** — No record-mode run on file. `tests/unit/sources/remberg/cassettes/`
   does not exist, so there is no `*.json.validation.json` to prove the
   simulator spec matches live responses. The connector *was* validated
   against a live remberg sandbox, but via a deployed Databricks pipeline
   rather than `CONNECTOR_TEST_MODE=live pytest`, which is what produces this
   artifact. To close it:
   `CONNECTOR_TEST_MODE=live CONNECTOR_TEST_CONFIG_PATH=… pytest tests/unit/sources/remberg/`.
   *Caveat: `ticket_conversations` returns customer email bodies and the
   cassette scrub is emails-only — review before that file leaves the machine.*
2. **MINOR** — B12 coverage cannot be evaluated for the same reason (no
   `*.json.coverage.json`). Resolved by the same run.
3. **MINOR** — `ticket_conversations` is the one new table never exercised
   against live data. Implemented and covered in simulate mode; noted
   explicitly in the PR body.

No BLOCKER failures. Nothing here gates merge.

## A. Connector implementation — 28 / 28

- ✅ A1. Class extends `LakeflowConnect` — `remberg.py:684`
- ✅ A2. All four abstract methods present — `remberg.py:843, 847, 851, 856`
- ✅ A3. No `cdc_with_deletes` table (remberg exposes no deletes feed), so
  `read_table_deletes` is correctly absent — `remberg.py:72`
- ✅ A4. Read pattern is deliberate and threefold: bounded `updatedAt`-range
  incremental (`_read_incremental`), full-refresh snapshot (`_read_snapshot`),
  and parent fan-out (`_read_child`) — `remberg.py:900, 869, 1043`
- ✅ A5. Termination cap present: `_init_ts_iso` / `_init_dt` captured in
  `__init__` — `remberg.py:716-717`; guard at `remberg.py:1012`. Snapshot
  tables use the `{"done": True}` sentinel instead.
- ✅ A6. `max_records_per_batch` honored on both incremental paths —
  `remberg.py:928, 1067`. Fan-out splits at parent granularity so a parent's
  children are never torn across microbatches.
- ✅ A7. The single HTTP call site passes `timeout=DEFAULT_TIMEOUT` —
  `remberg.py:765-770`
- ✅ A8. No `IntegerType` / `MapType`; `LongType` used for `forms.counter`
- ✅ A9. Nested fields kept as `StructType`, not flattened
- ✅ A10. Imports clean — std-lib, `requests`, `pyspark.sql.types`, and the
  interface only — `remberg.py:81-102`
- ✅ A11. Pylint 10.00/10 under the exact CI flag set
- ✅ A12. No ingestion-agent surface
- ✅ A13. `RembergDataSource(LakeflowSource)` with `_lakeflow_connect_cls` set
  and `_format_name` left at default — `__init__.py:9-15`

## B. Testing & simulator validation — 29 / 35

- ✅ B1–B4. Test file present; subclasses `LakeflowConnectTests`;
  `connector_class` and `simulator_source = "remberg"` set —
  `test_remberg_lakeflow_connect.py:16-18`
- ✅ B5. `replay_config` keys (`api_key`, `base_url`) match exactly the
  connection options `__init__` reads — `test_remberg_lakeflow_connect.py:19`
- ✅ B6–B7. `endpoints.yaml` present; 15 corpus JSON files, all valid arrays
- ✅ B8. All 15 endpoints the connector hits (10 `TABLE_ENDPOINTS` + 5
  `CHILD_ENDPOINTS` routes) have matching spec entries — 15 specs loaded
- ✅ B9. `test_read_terminates` passes in simulate mode
- ✅ B10. Full suite: 23 passed, 2 skipped (partitioned-stream and deletes
  suites, both N/A)
- ❌ B11. No record-mode run on file — see Top recommendations
- ⚠️ B12. Coverage not evaluable (no `coverage.json`) — same cause
- ✅ B13. `synthesize_future_records` declared on every cursor-bearing
  endpoint, so the `_init_ts` cap is genuinely exercised. Fan-out tables
  inherit it through their parent endpoints.
- ✅ B14. No write-back surface; `test_utils_class` correctly unset

### Regression coverage worth noting

Two tests added in this PR assert the invariant that broke live: reading a
table at `limit` 1/2/3/1000 must return identical records, and the first
wire request must ask for page 0. Both fail if `PAGE_BASE` is reverted to 1
— verified by reverting and re-running.

## C. Artifacts — 28 / 28

- ✅ C1. `remberg.py` compiles clean
- ✅ C2. `remberg_api_doc.md` mentions all 15 tables
- ✅ C3. `connector_spec.yaml` parses; `connection.parameters` =
  `[api_key, base_url]`; `external_options_allowlist` present
- ✅ C4. `README.md` mentions all 15 tables and both connection parameters
- ✅ C5. `pyproject.toml` parses; `requests>=2.28.0,<3.0` declared
- ✅ C6. `_generated_remberg_python_source.py` newer than `remberg.py`
- ✅ C7. `endpoints.yaml` loads via `load_specs` — 15 specs
- ✅ C8. All 15 corpus files valid JSON arrays
- ✅ C9. Test class present
- ✅ C10. `pyspark` confined to `[project.optional-dependencies].dev`

## D. Security — 22 / 22

- ✅ D1. No hardcoded secrets; API key read from `options` only
- ✅ D2. No `eval` / `exec` / `compile`
- ✅ D3. No `subprocess` / `os.system` / `shell=True`
- ✅ D4. No `verify=False`
- ✅ D5. No `pickle`
- ✅ D6. No `yaml.load`
- ✅ D7. No filesystem access — no `open(` / `Path(` in the connector
- ✅ D8. Two `logger` calls, neither touching headers/auth/body —
  `remberg.py:778` (status + backoff), `remberg.py:809` (404 path)
- ✅ D9. No `http://` URLs; `DEFAULT_BASE_URL` is HTTPS
- ✅ D10. Dependencies bounded (`>=2.28.0,<3.0`)

Note: parent ids are `quote(..., safe="")`-encoded before path interpolation
in `_fetch_children` — `remberg.py:1143`, so an unexpected id value cannot
alter request path structure.

## E. Cross-doc consistency — 11 / 11

- ✅ E1. `list_tables()` ≡ API doc ≡ README — all 15 tables in all three
- ✅ E2. Connection params agree across `__init__`, spec and README:
  `{api_key, base_url}`, symmetric diff empty
- ✅ E3. Schema columns are a subset of API-doc fields
- ✅ E4. Primary keys documented, including the two composite keys
  (`work_order_times`, `ticket_conversations`) that exist because those
  records carry no `id`
- ✅ E5. `external_options_allowlist` covers every `table_options` key the
  connector reads: `start_timestamp, lookback_seconds, limit,
  max_records_per_batch, full_parent_scan` — no gaps
- ✅ E6. README quick-start uses the spec's parameter names
- ✅ E7. README, API doc and code now agree that `page` is 0-indexed — this
  PR corrected the API doc, which previously claimed 1-indexed

## Note on scope

This PR includes one change outside `sources/remberg/`:
`PageParam.base` in the simulator (`endpoint_spec.py`, `pagination.py`).
It defaults to `1`, making it a no-op for every other connector; the full
suite (1375 tests) passes. Flagged for maintainer preference in the PR body —
the alternative was custom simulator handlers for all 13 remberg endpoints.
