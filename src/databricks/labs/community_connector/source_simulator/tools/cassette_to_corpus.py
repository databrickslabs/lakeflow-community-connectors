"""Extract a record corpus from a cassette + endpoint spec.

Walks a cassette, matches each interaction against the spec, pulls out the
records, deduplicates by primary key (or by full record if no PK is
declared), and writes one ``corpus/<table>.json`` per ``corpus:`` referenced
in the spec.

Usage::

    python -m databricks.labs.community_connector.source_simulator.tools.cassette_to_corpus \\
        --cassette tests/unit/sources/github/cassettes/TestGithubConnector.json \\
        --spec    src/.../source_simulator/specs/github/endpoints.yaml \\
        --output  src/.../source_simulator/specs/github/corpus/

The output directory is created if it doesn't exist. Existing corpus files
are overwritten — re-running is the expected workflow when reality drifts.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from databricks.labs.community_connector.source_simulator.cassette import Cassette
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
    load_specs,
    match_endpoint,
)


def extract_corpus(
    cassette: Cassette,
    specs: List[EndpointSpec],
) -> Dict[str, Any]:
    """Return ``{table_name: records}`` extracted from the cassette.

    Records are deduplicated when the spec implies a primary key by listing
    a ``filter`` with ``op: eq`` on a stable-looking field. When no PK is
    inferable, records are deduped by their full JSON serialization.
    """
    by_table: Dict[str, list] = defaultdict(list)
    seen_keys: Dict[str, set] = defaultdict(set)
    single_entities: Dict[str, dict] = {}

    for interaction in cassette.interactions:
        req = interaction.request
        body_text = interaction.response.body_text
        if not body_text:
            continue
        try:
            body = json.loads(body_text)
        except (TypeError, ValueError):
            continue

        match = match_endpoint(specs, req.method, req.url + _query_str(req.query))
        if match is None:
            continue
        spec, _ = match
        if spec.corpus is None:
            continue

        if spec.response.single_entity:
            if isinstance(body, dict):
                single_entities[spec.corpus] = body
            continue

        records = _extract_records(body)
        if not records:
            continue
        for r in records:
            key = _record_key(r, spec)
            if key in seen_keys[spec.corpus]:
                continue
            seen_keys[spec.corpus].add(key)
            by_table[spec.corpus].append(r)

    out: Dict[str, Any] = {}
    out.update(by_table)
    for table, entity in single_entities.items():
        out[table] = entity
    return out


def write_corpus(corpus: Dict[str, Any], output_dir: Path) -> List[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    for table, value in corpus.items():
        path = output_dir / f"{table}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(value, f, indent=2, ensure_ascii=False)
            f.write("\n")
        written.append(path)
    return written


# ----- internals --------------------------------------------------------


def _query_str(query: Dict[str, str]) -> str:
    if not query:
        return ""
    return "?" + "&".join(f"{k}={v}" for k, v in query.items())


def _extract_records(body: Any) -> List[dict]:
    """Best-effort: top-level array of dicts wins; otherwise dig for one."""
    if isinstance(body, list):
        return [r for r in body if isinstance(r, dict)]
    if isinstance(body, dict):
        for hint in ("records", "items", "data", "results", "entries", "values", "elements"):
            if hint in body and isinstance(body[hint], list):
                return [r for r in body[hint] if isinstance(r, dict)]
        candidates = [v for v in body.values() if isinstance(v, list) and v]
        candidates = [c for c in candidates if all(isinstance(x, dict) for x in c)]
        if candidates:
            candidates.sort(key=len, reverse=True)
            return candidates[0]
    return []


def _record_key(record: dict, spec: EndpointSpec) -> str:
    """Best-effort uniqueness key.

    Prefers stable, conventional PK fields (``id``, ``sha``, ``uuid``,
    ``node_id``) over spec-declared eq filters — eq filters often target
    *related* fields (like ``author.login``) that can be null on some
    records, which causes false-distinct keys for records that are
    actually duplicates.
    """
    from databricks.labs.community_connector.source_simulator.corpus import (
        get_field,
    )
    from databricks.labs.community_connector.source_simulator.endpoint_spec import (
        FilterOp,
    )

    # 1) Common PK names — most reliable, present on virtually every REST
    #    record we'd extract.
    for k in ("id", "sha", "uuid", "node_id", "name"):
        if k in record and record[k] is not None:
            return f"{k}={record[k]!r}"

    # 2) Spec-declared eq filters as a fallback for unusual schemas.
    for fp in spec.filters:
        if fp.op == FilterOp.EQ:
            v = get_field(record, fp.field)
            if v is not None:
                return f"{fp.field}={v!r}"

    # 3) Final fallback: serialize the whole record. Slow, but never wrong.
    return json.dumps(record, sort_keys=True)


# ----- CLI --------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cassette_to_corpus",
        description="Extract a record corpus from a cassette + endpoint spec.",
    )
    parser.add_argument("--cassette", type=Path, required=True, help="Path to cassette JSON.")
    parser.add_argument("--spec", type=Path, required=True, help="Path to endpoints.yaml.")
    parser.add_argument(
        "--output", type=Path, required=True, help="Output corpus directory."
    )
    args = parser.parse_args(argv)

    if not args.cassette.exists():
        print(f"error: cassette not found: {args.cassette}", file=sys.stderr)
        return 2
    if not args.spec.exists():
        print(f"error: spec not found: {args.spec}", file=sys.stderr)
        return 2

    cassette = Cassette.load(args.cassette)
    specs = load_specs(args.spec)
    corpus = extract_corpus(cassette, specs)
    written = write_corpus(corpus, args.output)

    for p in written:
        records = json.loads(p.read_text())
        n = len(records) if isinstance(records, list) else 1
        try:
            display = p.relative_to(Path.cwd())
        except ValueError:
            display = p
        print(f"  wrote {display}  ({n} record{'s' if n != 1 else ''})")

    if not written:
        print("  no corpus files written — cassette had no matches against the spec",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
