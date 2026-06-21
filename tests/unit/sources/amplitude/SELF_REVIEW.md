# Self-Review — amplitude

**Overall: 96 / 100 — READY**

Run at: 2026-06-21T13:05:00Z

## Top recommendations

1. **MAJOR (warn) — A6** `max_records_per_batch` is not read by incremental tables.
   Amplitude's Export API has no server-side record limit; admission control is via
   `window_hours` (events) and `window_days` (users/sessions). This is deliberate and
   documented in `amplitude.py:326–330`. Users of high-volume projects must reduce
   `window_hours` to keep per-call work within memory and timeout bounds; the README
   documents this. No code change needed, but worth noting for operator awareness.
2. **MINOR (warn) — A8** `MapType(StringType, StringType)` used for five dynamic-key
   columns (`event_properties`, `user_properties`, `group_properties`, `groups`, `data`).
   Intentional — Amplitude's per-project, per-event-type keys cannot be statically
   modelled; documented in `amplitude_schemas.py:9`.
3. **MINOR (warn) — D10** `lakeflow-community-connectors>=0.1.0` in `pyproject.toml`
   has no upper-bound pin. Matches the example connector's pattern; acceptable.

> **Bug fixed during this review:**  
> `active_users_counts.primary_keys` was `["date"]`. When the `g=` (group-by) table
> option is set, the API returns multiple rows per date (one per group value). CDC upserts
> keyed only on `date` would silently overwrite earlier segments — a data-loss bug.
> Fixed to `["date", "segment"]` in `amplitude_schemas.py:222`. Two regression tests
> added (`test_flatten_user_counts_multiple_segments_unique_per_date`,
> `test_active_users_counts_primary_key_includes_segment`).

---

## A. Connector implementation — 24 / 28

- ✅ A1. Class extends `LakeflowConnect` — `amplitude.py:52`
- ✅ A2. All four abstract methods implemented — `amplitude.py:128,132,138,144`
- ✅ A3. No `cdc_with_deletes` table; `read_table_deletes` not required
- ✅ A4. Read pattern: sliding hourly window (events), sliding day window
        (active_users_counts, average_session_length), full snapshot (events_list, cohorts, annotations)
- ✅ A5. `_init_ts` / `_init_date` cap present — `amplitude.py:79–80`; guards at
        `amplitude.py:216` and `amplitude.py:337`
- ⚠️ A6. `max_records_per_batch` not read — see Top recommendations
- ✅ A7. All HTTP calls go through `_request_with_retry` which injects
        `timeout=DEFAULT_TIMEOUT`; Export API explicitly passes `timeout=EXPORT_TIMEOUT`
        — `amplitude.py:96,227`
- ⚠️ A8. `MapType(StringType, StringType)` for dynamic columns — see Top recommendations
- ✅ A9. No nested-field flattening; dynamic columns remain MapType; category is StructType
- ✅ A10. Imports clean — `requests`, std-lib, pyspark, interface only — `amplitude.py:25–49`
- ✅ A11. Pylint 10/10 (`databricks.labs.community_connector.sources.amplitude.*`)
- ✅ A12. No `SupportsIngestionAgent` or `AgentOperation` surface present
- ✅ A13. `AmplitudeDataSource(LakeflowSource)` with `_lakeflow_connect_cls` — `__init__.py:9–10`

---

## B. Testing & simulator validation — 35 / 35

- ✅ B1. Test file exists — `tests/unit/sources/amplitude/test_amplitude_lakeflow_connect.py`
- ✅ B2. Subclasses `LakeflowConnectTests`
- ✅ B3. `connector_class = AmplitudeLakeflowConnect`
- ✅ B4. `simulator_source = "amplitude"`
- ✅ B5. `replay_config` covers all four `options.get(...)` keys (`api_key`, `secret_key`,
        `data_region`, `start_date`)
- ✅ B6. `endpoints.yaml` exists — 6 specs loaded
- ✅ B7. Six corpus files present (`events`, `events_list`, `active_users_counts`,
        `average_session_length`, `cohorts`, `annotations`)
- ✅ B8. All 6 connector URLs have matching spec entries
- ✅ B9. `test_read_terminates` passes in simulate mode
- ✅ B10. **29 passed / 2 skipped** in simulate mode (unit + integration + framework tests)
- ✅ B11. Record-mode cassette on file (2026-06-21). Validation report: 6/6 endpoints
         validated, 0 mismatched. Not stale (newer than `amplitude.py` and `endpoints.yaml`)
- ✅ B12. Coverage 100% — all 6 spec endpoints hit in record run
- ✅ B13. `synthesize_future_records` declared on all three cursor-bearing endpoints
         (`/api/2/export` → `server_upload_time`; `/api/2/users` and `/api/2/sessions/average`
         → `date`) — `endpoints.yaml:30,73,94`
- ✅ B14. No insertable tables; `test_utils_class` not required

---

## C. Artifacts — 28 / 28

- ✅ C1. `amplitude.py` compiles cleanly (`python -m py_compile`)
- ✅ C2. `amplitude_api_doc.md` — 709 lines; covers all six tables
- ✅ C3. `connector_spec.yaml` — YAML valid; `connection.parameters` lists `api_key`,
        `secret_key`, `data_region`, `start_date`; `display_name: Amplitude` present;
        `external_options_allowlist` set
- ✅ C4. `README.md` — 251 lines; all tables and connection parameters documented
- ✅ C5. `pyproject.toml` — `requests>=2.28.0,<3.0` in runtime deps; `pyspark` in dev only
- ✅ C6. `_generated_amplitude_python_source.py` mtime = 2026-06-21 18:33 ≥ `amplitude.py`
        and `amplitude_schemas.py` mtime
- ✅ C7. `endpoints.yaml` loads cleanly via `endpoint_spec.load_specs` (6 specs)
- ✅ C8. All 6 corpus JSON files parse cleanly
- ✅ C9. Test class exists (covered by B1)
- ✅ C10. `pyspark` absent from `[project.dependencies]`

---

## D. Security — 21 / 22

- ✅ D1. No hardcoded secrets
- ✅ D2. No `eval` / `exec` / `compile`
- ✅ D3. No `subprocess` / `os.system` / `shell=True`
- ✅ D4. No `verify=False`
- ✅ D5. No `pickle.load`
- ✅ D6. No unsafe `yaml.load`
- ✅ D7. No path-traversal patterns
- ✅ D8. No secrets logged
- ✅ D9. No `http://` URLs in production code
- ⚠️ D10. `lakeflow-community-connectors>=0.1.0` has no upper-bound — see Top recommendations

---

## E. Cross-doc consistency — 11 / 11

- ✅ E1. `list_tables()` returns `['events', 'events_list', 'active_users_counts',
        'average_session_length', 'cohorts', 'annotations']`; API doc and README both
        list all six
- ✅ E2. Connection keys `api_key`, `secret_key`, `data_region`, `start_date` are
        identical in `__init__`, `connector_spec.yaml`, and README
- ✅ E3. Schema field names are a subset of API-doc fields for all tables
- ✅ E4. Primary keys match API doc: `uuid` (events), `value` (events_list), `["date","segment"]`
        (active_users_counts), `date` (average_session_length), `id` (cohorts, annotations)
- ✅ E5. `external_options_allowlist` covers all `table_options.get(...)` keys:
        `window_hours`, `window_days`, `m`, `i`, `g`
- ✅ E6. README quick-start snippet uses `api_key`, `secret_key`, `data_region` — matches spec
- ✅ E7. README auth method (HTTP Basic), rate-limit guidance, and pagination semantics
        are consistent with API doc
