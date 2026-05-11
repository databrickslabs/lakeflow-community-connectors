"""Validate that the spec + corpus accurately model the live source API.

When ``Simulator(mode='live', ...)`` is given a spec + corpus alongside a
cassette, it doubles as a **proxy + spec validator**:

1. Forward the request to the real source. Get the live response.
2. Run the same request through ``SimulateHandler`` to see what the spec
   would have produced.
3. Diff the two responses — flag mismatches per endpoint.
4. At the end of the run, write a JSON report.

This is the closed loop that keeps the spec honest. If a connector test
passes against ``simulate`` but the live response no longer matches what
the spec produces, the next live test run will surface the drift in the
validation log.

Mismatches that matter (graduated severity):

- **status_code mismatch** — most decisive: if live returns 404 and we
  serve 200, the spec is fundamentally wrong about what this endpoint
  exists to do.
- **shape mismatch** — live returns a dict, spec returns an array (or
  vice versa). Wrapper config in the spec is wrong.
- **field set mismatch** — top-level keys present in live but missing
  from the spec/corpus, or vice versa. Corpus needs refresh, or spec's
  wrapper extras are wrong.
- **type mismatch** — live's ``id`` is an int, corpus has a string.
  Schema drift; corpus is stale.

The validator only inspects the *first record* of an array response when
diffing field shapes — sampling, not exhaustive comparison. Field-level
value comparisons aren't useful (live data drifts; corpus is synthetic).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import RequestRecord
from databricks.labs.community_connector.source_simulator.corpus import CorpusStore
from databricks.labs.community_connector.source_simulator.endpoint_spec import (
    EndpointSpec,
    match_endpoint,
)
from databricks.labs.community_connector.source_simulator.handler import (
    SimulateHandler,
    UnknownEndpoint,
)
from databricks.labs.community_connector.source_simulator.interceptor import (
    request_record_from_prepared,
)


# ----- result shapes -----------------------------------------------------


@dataclass
class EndpointValidation:
    method: str
    spec_path: Optional[str]  # None when no spec entry matched
    sample_url: str  # one example of the URL hit (for debugging)
    calls: int = 0
    matched: int = 0  # calls where spec response matched live
    mismatched: int = 0
    no_spec: int = 0  # calls with no spec entry
    issues: List[str] = field(default_factory=list)  # deduped sample of issue strings

    @property
    def status(self) -> str:
        if self.no_spec:
            return "no_spec"
        if self.mismatched:
            return "mismatched"
        if self.matched:
            return "validated"
        return "unknown"


# ----- validator ---------------------------------------------------------


class LiveValidator:
    """Runs every live request through the spec+corpus too, diffs responses."""

    def __init__(self, specs: List[EndpointSpec], corpus: CorpusStore) -> None:
        self.specs = specs
        self._handler = SimulateHandler(specs=specs, corpus=corpus)
        # Keyed by (method, spec_path) — collapses many concrete URLs into one
        # logical endpoint. For unmatched requests, key is (method, raw_url).
        self._results: Dict[Tuple[str, str], EndpointValidation] = {}

    def observe(self, prep: PreparedRequest, live_response: Response) -> None:
        """Record one request/response pair against the spec."""
        req_rec = request_record_from_prepared(prep)
        match = match_endpoint(self.specs, req_rec.method, prep.url or "")

        if match is None:
            self._record(
                method=req_rec.method,
                spec_path=None,
                sample_url=prep.url or "",
                live_response=live_response,
                no_spec=True,
                issues=["no matching endpoint spec"],
            )
            return

        spec, _path_params = match
        try:
            spec_response = self._handler.handle(prep)
        except UnknownEndpoint as e:
            self._record(
                method=req_rec.method,
                spec_path=spec.path,
                sample_url=prep.url or "",
                live_response=live_response,
                no_spec=True,
                issues=[f"handler raised UnknownEndpoint: {e}"],
            )
            return
        except Exception as e:
            self._record(
                method=req_rec.method,
                spec_path=spec.path,
                sample_url=prep.url or "",
                live_response=live_response,
                mismatched=True,
                issues=[f"handler raised {type(e).__name__}: {e}"],
            )
            return

        issues = _diff(live_response, spec_response, spec)
        self._record(
            method=req_rec.method,
            spec_path=spec.path,
            sample_url=prep.url or "",
            live_response=live_response,
            mismatched=bool(issues),
            issues=issues,
        )

    # --- internals ---

    def _record(
        self,
        *,
        method: str,
        spec_path: Optional[str],
        sample_url: str,
        live_response: Response,
        no_spec: bool = False,
        mismatched: bool = False,
        issues: Optional[List[str]] = None,
    ) -> None:
        key = (method.upper(), spec_path or sample_url)
        ev = self._results.get(key)
        if ev is None:
            ev = EndpointValidation(
                method=method.upper(), spec_path=spec_path, sample_url=sample_url
            )
            self._results[key] = ev
        ev.calls += 1
        if no_spec:
            ev.no_spec += 1
        elif mismatched:
            ev.mismatched += 1
        else:
            ev.matched += 1
        for issue in issues or []:
            if issue not in ev.issues and len(ev.issues) < 5:
                ev.issues.append(issue)

    def report(self) -> List[EndpointValidation]:
        return sorted(
            self._results.values(),
            key=lambda r: (r.spec_path or r.sample_url, r.method),
        )

    def to_json(self) -> dict:
        return {
            "version": 1,
            "generated_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "endpoints": [
                {
                    "method": r.method,
                    "spec_path": r.spec_path,
                    "sample_url": r.sample_url,
                    "status": r.status,
                    "calls": r.calls,
                    "matched": r.matched,
                    "mismatched": r.mismatched,
                    "no_spec": r.no_spec,
                    "issues": r.issues,
                }
                for r in self.report()
            ],
            "summary": self._summary(),
        }

    def _summary(self) -> dict:
        results = self.report()
        return {
            "endpoints_total": len(results),
            "endpoints_validated": sum(1 for r in results if r.status == "validated"),
            "endpoints_mismatched": sum(1 for r in results if r.status == "mismatched"),
            "endpoints_missing_spec": sum(1 for r in results if r.status == "no_spec"),
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_json(), f, indent=2, ensure_ascii=False)
            f.write("\n")


# ----- response diff -----------------------------------------------------


def _diff(
    live: Response, spec_response: Response, spec: EndpointSpec
) -> List[str]:
    """Return a list of issue strings; empty means responses match."""
    issues: List[str] = []

    if live.status_code != spec_response.status_code:
        issues.append(
            f"status_code: live={live.status_code} spec={spec_response.status_code}"
        )

    live_body = _try_json(live)
    spec_body = _try_json(spec_response)
    if live_body is None or spec_body is None:
        # Non-JSON; we can't reasonably diff. Status check above is the only
        # signal.
        return issues

    if type(live_body) is not type(spec_body):
        issues.append(
            f"body shape: live={type(live_body).__name__} "
            f"spec={type(spec_body).__name__}"
        )
        return issues

    if isinstance(live_body, dict):
        live_keys = set(live_body.keys())
        spec_keys = set(spec_body.keys())
        missing_in_spec = live_keys - spec_keys
        if missing_in_spec:
            issues.append(
                f"top-level keys in live missing from spec: "
                f"{sorted(list(missing_in_spec))[:5]}"
            )

    live_records = _extract_records(live_body, spec)
    spec_records = _extract_records(spec_body, spec)

    if live_records and spec_records:
        live_first_fields = _field_shape(live_records[0])
        spec_first_fields = _field_shape(spec_records[0])
        missing_in_spec = set(live_first_fields) - set(spec_first_fields)
        if missing_in_spec:
            issues.append(
                f"record fields in live missing from corpus: "
                f"{sorted(list(missing_in_spec))[:5]}"
            )
        for f in set(live_first_fields) & set(spec_first_fields):
            if (
                live_first_fields[f] != spec_first_fields[f]
                and live_first_fields[f] != "null"
                and spec_first_fields[f] != "null"
            ):
                issues.append(
                    f"field {f!r}: live={live_first_fields[f]} "
                    f"corpus={spec_first_fields[f]}"
                )

    return issues


def _try_json(resp: Response) -> Any:
    try:
        return resp.json()
    except (ValueError, TypeError):
        return None


def _extract_records(body: Any, spec: EndpointSpec) -> Optional[List[Any]]:
    """Best-effort extraction of the records array from a response body."""
    if isinstance(body, list):
        return body
    if not isinstance(body, dict):
        return None
    wrapper = spec.response.wrapper
    if wrapper is not None:
        # Walk the dotted records_key.
        cur: Any = body
        for part in wrapper.records_key.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur if isinstance(cur, list) else None
    # Generic fallback — common record-array field names.
    for hint in ("records", "items", "data", "results", "value", "entries"):
        if hint in body and isinstance(body[hint], list):
            return body[hint]
    return None


def _field_shape(record: Any) -> Dict[str, str]:
    if not isinstance(record, dict):
        return {}
    return {k: _shape_of(v) for k, v in record.items()}


def _shape_of(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    return type(value).__name__
