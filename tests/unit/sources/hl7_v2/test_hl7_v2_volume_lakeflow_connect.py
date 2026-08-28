"""Tests for the HL7 v2 community connector (Unity Catalog Volume source mode).

Volume mode reads HL7 files directly from a Unity Catalog Volume path that
the pipeline runtime FUSE-mounts on the executor — no SDK, no HTTP, no auth
options.  In tests we substitute a class-scoped temporary directory for the
``/Volumes/...`` mount, populate it with the same HL7 sample messages the
GCP simulator corpus uses, and stamp each file's mtime to match the
corresponding corpus ``createTime`` so cursor-bearing tests advance through
identical material in both modes.

The base ``LakeflowConnectTests`` harness still installs the GCP simulator
(via ``simulator_source = "hl7_v2"``), but volume mode never makes HTTP
calls — the simulator simply sits idle.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.hl7_v2._hl7v2_null_cols import allow_null_columns as _HL7V2_NULL_COLS
from tests.unit.sources.test_suite import LakeflowConnectTests


_SAMPLES_DIR = Path(__file__).parent / "samples"

# (sample-filename, createTime to stamp as mtime). Mirrors the seven
# corpus rows defined in
# ``source_simulator/specs/hl7_v2/corpus/regenerate.py`` so the cursor
# walks the same time-points whether the data comes from the GCP simulator
# or from this on-disk fixture.
_VOLUME_SAMPLES: tuple[tuple[str, str], ...] = (
    ("sample_adt_comprehensive.hl7", "2024-01-15T08:00:00Z"),
    ("sample_batch_mixed.hl7",        "2024-01-16T09:00:00Z"),
    ("sample_mdt_txa.hl7",            "2024-01-17T10:00:00Z"),
    ("sample_siu_scheduling.hl7",     "2024-01-18T11:00:00Z"),
    ("sample_vxu_immunization.hl7",   "2024-01-19T12:00:00Z"),
    ("sample_oru_concat_notes.hl7",   "2024-01-20T13:00:00Z"),
    ("sample_dft_financial.hl7",      "2024-01-21T14:00:00Z"),
)


def _iso_to_epoch(iso: str) -> float:
    """Parse an RFC3339 ``...Z`` string into a POSIX timestamp."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).replace(
        tzinfo=timezone.utc
    ).timestamp()


class TestHL7V2VolumeConnector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    simulator_source = "hl7_v2"
    sample_records = 5
    allow_null_columns = _HL7V2_NULL_COLS

    # Populated in ``setup_class``; carries the temp directory whose files
    # stand in for the ``/Volumes/...`` mount that exists at production time.
    _volume_dir: tempfile.TemporaryDirectory | None = None

    @classmethod
    def setup_class(cls):
        cls._volume_dir = tempfile.TemporaryDirectory(prefix="hl7v2-volume-")
        cls._populate_volume(Path(cls._volume_dir.name))
        try:
            super().setup_class()
        except Exception:
            cls._cleanup_volume()
            raise

    @classmethod
    def teardown_class(cls):
        try:
            super().teardown_class()
        finally:
            cls._cleanup_volume()

    @classmethod
    def _cleanup_volume(cls) -> None:
        td = cls._volume_dir
        cls._volume_dir = None
        if td is not None:
            td.cleanup()

    @classmethod
    def _populate_volume(cls, dest: Path) -> None:
        """Copy each sample into ``dest`` and stamp its mtime.

        ``os.utime`` sets both atime and mtime to the same value — the
        connector keys off mtime via ``DirEntry.stat().st_mtime`` and is
        agnostic to atime.
        """
        for filename, iso in _VOLUME_SAMPLES:
            src = _SAMPLES_DIR / filename
            dst = dest / filename
            shutil.copyfile(src, dst)
            epoch = _iso_to_epoch(iso)
            os.utime(dst, (epoch, epoch))

    @classmethod
    def _replay_config(cls):
        # Built lazily so the temp directory exists when this is called
        # (during ``LakeflowConnectTests.setup_class`` -> ``_load_config``).
        assert cls._volume_dir is not None, (
            "Volume tempdir not initialised — setup_class must run before "
            "_replay_config()."
        )
        return {
            "source_type": "volume",
            "volume_path": cls._volume_dir.name,
            "file_glob": "*.hl7",
        }
