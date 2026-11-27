"""Process-local state that serializes as a fresh, empty instance."""

from __future__ import annotations

import threading


class ProcessLocal:
    """Pickle as a new empty instance of the same class.

    The merged source ships module state to executors by value; state built on
    the driver must not make an executor skip its own q or runtime setup.
    """

    def __reduce__(self):
        return (type(self), ())


class ProcessLocalDict(ProcessLocal, dict):
    """Process-local dict."""


class ProcessLocalSet(ProcessLocal, set):
    """Process-local set."""


class ProcessLocalLock(ProcessLocal):
    """Process-local lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)
