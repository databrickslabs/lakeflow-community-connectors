"""Counters and logging for PyKX -> Python conversion paths."""

from __future__ import annotations

import logging
import threading
from typing import Literal

logger = logging.getLogger(__name__)

ConversionPath = Literal["pa_raw", "pa", "pd_fallback"]
ConversionMode = Literal["pandas", "arrow_pandas", "arrow_direct"]


class _PicklableLock:
    """Process-local lock that can travel with the merged connector class."""

    def __init__(self):
        self._lock = threading.Lock()

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)

    def __getstate__(self):
        return {}

    def __setstate__(self, _state):
        self._lock = threading.Lock()


_PATH_LOCK = _PicklableLock()
_PATH_COUNTS: dict[ConversionPath, int] = {
    "pa_raw": 0,
    "pa": 0,
    "pd_fallback": 0,
}


def reset_conversion_path_counts() -> None:
    with _PATH_LOCK:
        for key in _PATH_COUNTS:
            _PATH_COUNTS[key] = 0


def get_conversion_path_counts() -> dict[str, int]:
    with _PATH_LOCK:
        return dict(_PATH_COUNTS)


def record_conversion_path(path: ConversionPath) -> None:
    with _PATH_LOCK:
        _PATH_COUNTS[path] += 1


def log_conversion_path_summary(
    *,
    date_partition: str,
    conversion_mode: str,
    rows_emitted: int,
) -> None:
    counts = get_conversion_path_counts()
    total_conversions = sum(counts.values())
    logger.info(
        "KX conversion summary date=%s mode=%s rows=%s pykx_paths=%s "
        "(pa_raw=%s pa=%s pd_fallback=%s total_pages=%s)",
        date_partition,
        conversion_mode,
        rows_emitted,
        counts,
        counts["pa_raw"],
        counts["pa"],
        counts["pd_fallback"],
        total_conversions,
    )
