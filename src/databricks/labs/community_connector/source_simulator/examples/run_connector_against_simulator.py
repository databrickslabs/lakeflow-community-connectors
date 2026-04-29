"""Run a connector against the simulator end-to-end, in-process.

Demonstrates the e2e use case for the simulator: a connector's HTTP traffic
is served by an in-process simulator (spec + corpus), no network access, no
live credentials.

Usage::

    python -m databricks.labs.community_connector.source_simulator.examples.run_connector_against_simulator

The example uses the GitHub connector, which has a committed spec + corpus
under ``source_simulator/specs/github/``. To use a different connector,
swap the ``CONNECTOR_CLASS`` and ``simulator_source`` lines below.

In an e2e pipeline (Spark Declarative Pipeline), the same pattern applies:
the orchestrator wraps the worker code in ``Simulator(...)`` so each Spark
worker runs the connector against the simulator in its own process. The
simulator code rides along in the connector package, available to every
worker via normal Python import.
"""

from __future__ import annotations

import json
from pathlib import Path

from databricks.labs.community_connector.source_simulator import Simulator, MODE_SIMULATE
from databricks.labs.community_connector import source_simulator as _sim_pkg
from databricks.labs.community_connector.sources.github.github import (
    GithubLakeflowConnect,
)


# Paths to the simulator artifacts. In a normal install these come from the
# connector package; here we resolve relative to the source_simulator package.
_SPECS_ROOT = Path(_sim_pkg.__file__).parent / "specs"
SPEC_PATH = _SPECS_ROOT / "github" / "endpoints.yaml"
CORPUS_DIR = _SPECS_ROOT / "github" / "corpus"

# Fake credentials — the simulator doesn't validate them. The token has to
# be present because the connector's __init__ rejects empty tokens.
CONNECTOR_OPTIONS = {"token": "simulator-fake-token"}

# Tables to read. Limit to a handful for output brevity.
SAMPLE_TABLES = ["commits", "issues", "pull_requests", "branches", "users"]
SAMPLE_TABLE_OPTIONS = {
    # Mirrors what dev_table_config.json provides for these tables when
    # running against the live API.
    "commits": {"owner": "yyoli-db", "repo": "lakeflow-community-connectors"},
    "issues": {"owner": "yyoli-db", "repo": "lakeflow-community-connectors"},
    "pull_requests": {"owner": "yyoli-db", "repo": "lakeflow-community-connectors"},
    "branches": {"owner": "yyoli-db", "repo": "lakeflow-community-connectors"},
    "users": {},
}


def main() -> int:
    print(f"Spec:   {SPEC_PATH}")
    print(f"Corpus: {CORPUS_DIR}")
    print()

    with Simulator(
        mode=MODE_SIMULATE, spec_path=SPEC_PATH, corpus_dir=CORPUS_DIR
    ) as sim:
        connector = GithubLakeflowConnect(CONNECTOR_OPTIONS)

        for table in SAMPLE_TABLES:
            opts = SAMPLE_TABLE_OPTIONS.get(table, {})
            try:
                records, offset = connector.read_table(table, {}, opts)
                rows = list(records)
            except Exception as e:
                print(f"  [{table}] read failed: {e}")
                continue
            print(f"  [{table}]  {len(rows)} record{'s' if len(rows) != 1 else ''}")
            if rows:
                # Show one record's top-level keys for shape illustration.
                sample = rows[0]
                if isinstance(sample, dict):
                    print(f"      sample keys: {sorted(list(sample.keys()))[:8]}")

        print()
        print("Coverage report:")
        for hit in sim.coverage.report():
            print(f"  {hit.method:4}  {hit.url}  ×{hit.count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
