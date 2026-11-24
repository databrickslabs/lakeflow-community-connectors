"""Track which endpoints the connector exercises during a Simulator session.

A small companion to ``Simulator``. While the simulator is active in proxy
posture (live mode), the tracker records every intercepted request and emits
a per-endpoint hit count + sample of param shapes at the end of the run.

Used to spot two kinds of gaps:
- An endpoint declared in the spec but never hit by any test.
- An endpoint the connector hits with no spec entry.

The tracker doesn't know about the spec — it just records what it sees.
Cross-referencing against ``endpoints.yaml`` happens in the reporter that
loads both at the end.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from databricks.labs.community_connector.source_simulator.cassette import RequestRecord


@dataclass
class EndpointHit:
    method: str
    url: str
    count: int = 0
    # Distinct sets of (sorted_query_keys) seen for this endpoint, capped at
    # a small number so noisy connectors don't blow the report up.
    param_shapes: List[Tuple[str, ...]] = field(default_factory=list)


@dataclass
class CoverageTracker:
    """Records every request the simulator forwards in proxy posture."""

    _hits: Dict[Tuple[str, str], EndpointHit] = field(default_factory=dict)
    _param_shapes_seen: Dict[Tuple[str, str], set] = field(
        default_factory=lambda: defaultdict(set)
    )
    max_param_shapes: int = 5

    def observe(self, req: RequestRecord) -> None:
        key = (req.method.upper(), req.url)
        hit = self._hits.get(key)
        if hit is None:
            hit = EndpointHit(method=key[0], url=key[1])
            self._hits[key] = hit
        hit.count += 1

        param_shape = tuple(sorted(req.query.keys()))
        seen = self._param_shapes_seen[key]
        if param_shape not in seen and len(seen) < self.max_param_shapes:
            seen.add(param_shape)
            hit.param_shapes.append(param_shape)

    def report(self) -> List[EndpointHit]:
        """Return endpoint hits sorted by URL, then method."""
        return sorted(self._hits.values(), key=lambda h: (h.url, h.method))

    def to_json(self) -> dict:
        return {
            "version": 1,
            "endpoints": [
                {
                    "method": h.method,
                    "url": h.url,
                    "count": h.count,
                    "param_shapes": [list(p) for p in h.param_shapes],
                }
                for h in self.report()
            ],
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_json(), f, indent=2, ensure_ascii=False)
            f.write("\n")
