"""Regenerate ``corpus/messages.json`` from the in-tree HL7 v2 samples.

Run this script whenever the sample HL7 files in
``tests/unit/sources/hl7_v2/samples/`` change, or when the simulator's
corpus needs to grow new segment-type coverage. The output mirrors a
GCP Healthcare API ``hl7V2Messages`` list response shape: each entry
carries ``name``, ``data`` (base64 of the raw HL7 wire payload),
``sendTime``, and ``createTime``. The seven samples below were chosen
to collectively cover all 23 entries in
``hl7_v2_schemas.SEGMENT_TABLES`` (verified separately).

Usage::

    .venv/bin/python \\
      src/databricks/labs/community_connector/source_simulator/specs/hl7_v2/corpus/regenerate.py
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

# (sample-filename, createTime, sendTime). createTimes are spaced one
# day apart and sit before wall-clock now() so the cursor-bearing
# tests have material to advance through.
_SAMPLES = [
    ("sample_adt_comprehensive.hl7", "2024-01-15T08:00:00Z", "2024-01-15T08:00:05Z"),
    ("sample_batch_mixed.hl7",        "2024-01-16T09:00:00Z", "2024-01-16T09:00:05Z"),
    ("sample_mdt_txa.hl7",            "2024-01-17T10:00:00Z", "2024-01-17T10:00:05Z"),
    ("sample_siu_scheduling.hl7",     "2024-01-18T11:00:00Z", "2024-01-18T11:00:05Z"),
    ("sample_vxu_immunization.hl7",   "2024-01-19T12:00:00Z", "2024-01-19T12:00:05Z"),
    ("sample_oru_concat_notes.hl7",   "2024-01-20T13:00:00Z", "2024-01-20T13:00:05Z"),
    ("sample_dft_financial.hl7",      "2024-01-21T14:00:00Z", "2024-01-21T14:00:05Z"),
    ("sample_adt_zsegments.hl7",      "2024-01-22T15:00:00Z", "2024-01-22T15:00:05Z"),
]

_REPO = Path(__file__).resolve().parents[8]
_SAMPLES_DIR = _REPO / "tests" / "unit" / "sources" / "hl7_v2" / "samples"
_OUTPUT = Path(__file__).parent / "messages.json"


def _build_one(filename: str, create_time: str, send_time: str, index: int) -> dict:
    raw = (_SAMPLES_DIR / filename).read_bytes()
    base = filename.replace(".hl7", "")
    return {
        "name": (
            f"projects/p/locations/l/datasets/d/hl7V2Stores/s/messages/{base}-{index}"
        ),
        "data": base64.b64encode(raw).decode("ascii"),
        "sendTime": send_time,
        "createTime": create_time,
    }


def main() -> None:
    out = [
        _build_one(filename, ct, st, i)
        for i, (filename, ct, st) in enumerate(_SAMPLES, start=1)
    ]
    _OUTPUT.write_text(json.dumps(out, indent=2))
    print(f"Wrote {_OUTPUT} with {len(out)} messages.")


if __name__ == "__main__":
    main()
