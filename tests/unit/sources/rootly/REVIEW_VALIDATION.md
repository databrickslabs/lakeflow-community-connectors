# Rootly review follow-up validation — 2026-11-11

This revision addresses PR #304's incremental incidents, page-size cap, join documentation, and
unreachable retry-error comments. Package version stays 0.1.0 because Rootly is a new connector
in this unmerged PR. Anyone running the earlier snapshot-only revision must fully refresh or
recreate the incident pipeline when adopting the new CDC ingestion mode.

## Automated validation

- Combined simulator and Rootly suites on Python 3.13.5: **156 passed, 2 skipped**.
  Command: `PYTHONPATH=src python -m pytest tests/unit/source_simulator tests/unit/sources/rootly -q`.
  Skips are the inapplicable partitioned-read and CDC-delete tests.
- Incremental regressions cover one-page batch bounds, equal timestamps across pages, a
  JSON-round-tripped checkpoint resumed by a new connector, stable paging parameters across a
  page-size change, exact time-window boundaries, later source updates, empty runs, retries without
  checkpoint mutation, malformed cursor/row metadata, and future data excluded by the run cutoff.
- Ruff lint/format pass for handwritten Rootly source, tests and simulator handler.
- CI-style pylint: **10.00/10** for the implementation and changed simulator handler.
- Generated standalone source rebuilt; Rootly wheel rebuilt and verified with
  `python tools/scripts/build_wheelhouse.py rootly --check --base origin/master`.
- Standalone generated-source smoke: registration, incremental read and stable-offset termination pass.

## Targeted live GET checks

Performed beginning 2026-11-11 23:00 UTC using a test credential supplied through macOS Keychain.
The checks used the revised connector's request/validation code. No source writes were made;
no credentials, response bodies, resource IDs, timestamps from records, or cursor values are
included in this report or copied into synthetic fixtures.

| Check | Result | Coverage |
|---|---|---|
| Initial incremental incident request | Pass | Two records; no `page[number]`; `sort=updated_at` and exclusive upper bound |
| Incident continuation | Pass | Next two records; unchanged window/sort, opaque cursor, no duplicate IDs between these pages |
| Inclusive updated-at lower bound | Pass | Two records; first returned update time equals the requested boundary |
| `page_size=1000` | Pass | Accepted by incidents, services, users, teams, severities, escalation_policies, schedules; incidents returned 1,000 rows |

These were targeted direct API checks, not a new record-mode simulator validation or a full-account
export. The November 3 cassette coverage/validation reports describe the earlier snapshot revision.
Timestamp ties, restart/retry behavior, later-run updates, and run termination are validated with
synthetic data; no production records were changed to test them. Deletions are not tracked by
incremental incident ingestion. Databricks pipeline smoke testing remains pending. Upstream fork
CI requires a maintainer to authorize the latest commit with `safe-to-test` per CONTRIBUTING.md.
