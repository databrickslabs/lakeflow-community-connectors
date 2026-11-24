"""Extractor coverage matrix — ensures every extractor has sample data.

Verifies that all 23 typed extractors in _EXTRACTORS can be exercised
by at least one .hl7 sample file.
"""
from __future__ import annotations

import pytest

from tests.unit.sources.hl7_v2.hl7_v2_test_utils import SAMPLES_DIR, load_sample, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import (
    _EXTRACTORS,
    _split_messages,
)
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)

SAMPLE_FILES = sorted(f.name for f in SAMPLES_DIR.glob("*.hl7"))

EXTRACTOR_TO_SEG = {
    "msh": "MSH", "evn": "EVN", "pid": "PID", "pd1": "PD1",
    "pv1": "PV1", "pv2": "PV2", "nk1": "NK1", "mrg": "MRG",
    "al1": "AL1", "iam": "IAM", "dg1": "DG1", "pr1": "PR1",
    "orc": "ORC", "obr": "OBR", "obx": "OBX", "nte": "NTE",
    "spm": "SPM", "in1": "IN1", "gt1": "GT1", "ft1": "FT1",
    "rxa": "RXA", "sch": "SCH", "txa": "TXA",
}


class TestExtractorCoverage:
    """Verify every extractor in _EXTRACTORS is exercised by at least one sample."""

    @pytest.mark.parametrize("key", list(EXTRACTOR_TO_SEG.keys()))
    def test_segment_covered_in_samples(self, key):
        seg_type = EXTRACTOR_TO_SEG[key]
        found = False
        for fname in SAMPLE_FILES:
            raw = load_sample(fname)
            for msg_str in _split_messages(raw):
                msg = parse_message(msg_str)
                if msg is None:
                    continue
                segs = segments_of_type(msg, seg_type)
                if segs:
                    extractor = _EXTRACTORS[key]
                    row = extractor(segs[0])
                    assert isinstance(row, dict)
                    found = True
                    break
            if found:
                break
        assert found, (
            f"No sample file contains a {seg_type} segment — "
            f"add a sample to tests/unit/sources/hl7_v2/samples/"
        )

    def test_all_extractors_registered(self):
        """Every key in EXTRACTOR_TO_SEG should be in _EXTRACTORS."""
        for key in EXTRACTOR_TO_SEG:
            assert key in _EXTRACTORS, f"Extractor '{key}' not in _EXTRACTORS"

    def test_no_extra_extractors(self):
        """Every key in _EXTRACTORS should be in our coverage map."""
        for key in _EXTRACTORS:
            assert key in EXTRACTOR_TO_SEG, (
                f"Extractor '{key}' in _EXTRACTORS but not in coverage map — "
                f"add it to EXTRACTOR_TO_SEG and create tests"
            )
