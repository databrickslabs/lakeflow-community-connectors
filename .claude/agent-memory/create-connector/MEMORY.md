# Create-Connector Orchestrator Memory

## Workflow Notes

### Subagents cannot be nested inside Claude Code sessions
Attempting to run `claude --agent ...` from within a Claude Code session fails with:
"Claude Code cannot be launched inside another Claude Code session."
Workaround: Execute each phase's logic directly as the orchestrator.

### authenticate.py requires python3.10, not python3 or python
Use `python3.10 tools/scripts/authenticate.py` — the `python3` alias may point to an
incompatible version with syntax or import errors.

### authenticate.py CLI mode fails when there is no TTY (inside tool execution)
Use browser mode (`-m browser`) or create dev_config.json directly. The CLI mode uses
`getpass` which raises EOFError when stdin is not a real terminal.

### PYTHONPATH must be set to project root for tests
Tests import from `tests.unit.sources.test_utils` and
`databricks.labs.community_connector.*`. Always run:
`PYTHONPATH=. python3.10 -m pytest tests/unit/sources/{source}/...`

---

## Test Suite Conventions

- `LakeflowConnectTester` requires the connector class to be injected into
  `test_suite.LakeflowConnect` before calling `run_all_tests()`.
- `table_configs={}` is fine for connectors with no required table options.
- IntegerType fields in schemas cause test failures — always use LongType.
- Non-nullable fields (`nullable=False`) must never return None in records.
- Snapshot tables do not need cursor_field in metadata.
- Append tables do not need primary_keys validated by the test suite.

---

## Packaging

- pyproject.toml lives inside the source directory (not project root).
- Build from within the source directory using `python -m build`.
- The `requests` library must be listed as a dependency for HTTP-based connectors
  (not assumed to be available).
