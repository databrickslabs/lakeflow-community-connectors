"""Replay a recorded DICOMweb cassette against the spec+corpus to
recompute the per-endpoint validation diff without needing live creds.

The cassette is a frozen recording of what the live Orthanc server
returned; the spec+corpus is what the simulator would produce. The diff
is deterministic in (cassette, spec, corpus). Running this script after
a corpus edit lets you confirm the corpus refresh closes the validation
gap before scheduling a real live record-mode run.

Run with:

    python tests/unit/sources/dicomweb/replay_validation.py

Writes the refreshed report next to the live cassette at
``cassettes/TestDICOMwebConnector.json.validation.json`` — the same
path the live-record path would write to. Exit code is 0 when all
endpoints validated, 1 otherwise.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlencode

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import Cassette
from databricks.labs.community_connector.source_simulator.corpus import CorpusStore
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    load_specs,
)
from databricks.labs.community_connector.source_simulator.validator import (
    LiveValidator,
)


def _make_prepared(method: str, url: str, query: dict) -> PreparedRequest:
    prep = PreparedRequest()
    full_url = url
    if query:
        sep = "&" if "?" in url else "?"
        full_url = f"{url}{sep}{urlencode(query)}"
    prep.prepare(method=method, url=full_url, headers={"User-Agent": "replay"})
    return prep


def _make_response(record) -> Response:
    resp = Response()
    resp.status_code = record.status_code
    if record.body_text is not None:
        encoding = record.encoding or "utf-8"
        resp._content = record.body_text.encode(encoding)  # noqa: SLF001
        resp.encoding = encoding
    else:
        resp._content = b""  # noqa: SLF001
    for k, v in (record.headers or {}).items():
        resp.headers[k] = v
    resp.url = record.url or ""
    return resp


def main() -> int:
    repo = Path(__file__).resolve().parents[4]
    cassette_path = (
        repo / "tests/unit/sources/dicomweb/cassettes/TestDICOMwebConnector.json"
    )
    spec_path = (
        repo
        / "src/databricks/labs/community_connector/source_simulator/specs/dicomweb/endpoints.yaml"
    )
    corpus_dir = (
        repo
        / "src/databricks/labs/community_connector/source_simulator/specs/dicomweb/corpus"
    )

    cassette = Cassette.load(cassette_path)
    specs = load_specs(spec_path)
    corpus = CorpusStore.load(corpus_dir)
    validator = LiveValidator(specs=specs, corpus=corpus)

    for ix in cassette.interactions:
        prep = _make_prepared(ix.request.method, ix.request.url, ix.request.query)
        resp = _make_response(ix.response)
        validator.observe(prep, resp)

    report = validator.to_json()

    out_path = cassette_path.with_suffix(cassette_path.suffix + ".validation.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")

    summary = report["summary"]
    print(json.dumps(summary, indent=2))
    for ep in report["endpoints"]:
        marker = " " if ep["status"] == "validated" else "!"
        print(
            f"{marker} {ep['status']:12s} {ep['method']:4s} {ep['spec_path']} "
            f"matched={ep['matched']} mismatched={ep['mismatched']}"
        )
        for issue in ep["issues"]:
            print(f"     - {issue}")
    print(f"\nWrote {out_path}")
    return 0 if summary["endpoints_mismatched"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
