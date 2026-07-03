"""Shared test helpers for HL7 v2 tests."""
from __future__ import annotations

from pathlib import Path

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _split_messages
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)

SAMPLES_DIR = Path(__file__).parent / "samples"


def load_sample(filename: str) -> str:
    """Read a sample .hl7 file from the samples directory."""
    return (SAMPLES_DIR / filename).read_text()


def parse_first(raw: str):
    """Split raw text and parse the first message."""
    msgs = _split_messages(raw)
    assert msgs, "No messages found in input"
    return parse_message(msgs[0])


def segments_of_type(msg, seg_type: str):
    """Return all segments matching *seg_type* (case-insensitive)."""
    return [s for s in msg.segments if s.segment_type.upper() == seg_type.upper()]


def extract_segment(msg, seg_type: str, extractor, index: int = 0) -> dict:
    """Extract a dict from the *index*-th segment of *seg_type*."""
    segs = segments_of_type(msg, seg_type)
    assert len(segs) > index, (
        f"Expected at least {index + 1} {seg_type} segment(s), got {len(segs)}"
    )
    return extractor(segs[index])
