"""Shared helpers for source tests.

The legacy ``configs/dev_config.json`` per-source convention has been
removed. Credentials for live / record runs come from one of:

1. ``CONNECTOR_TEST_CONFIG_JSON`` env var — inline JSON string.
2. ``CONNECTOR_TEST_CONFIG_PATH`` env var — path to a JSON file.
3. An explicit ``default_path`` passed to ``load_config(...)`` — a
   local file at any path the caller chooses.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional, Union


_CONFIG_JSON_ENV = "CONNECTOR_TEST_CONFIG_JSON"
_CONFIG_PATH_ENV = "CONNECTOR_TEST_CONFIG_PATH"


def load_config(default_path: Optional[Union[str, Path]] = None) -> Any:
    """Load credentials for a connector test.

    Precedence (first match wins):

    1. ``CONNECTOR_TEST_CONFIG_JSON`` env var — inline JSON. CI-friendly:
       inject from a secret store without staging anything to disk.
    2. ``CONNECTOR_TEST_CONFIG_PATH`` env var — path to a JSON file.
    3. ``default_path`` argument — a fallback local path the caller
       supplies. Optional.

    Raises ``RuntimeError`` if none of the three resolve.
    """
    inline = os.environ.get(_CONFIG_JSON_ENV, "").strip()
    if inline:
        try:
            return json.loads(inline)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"{_CONFIG_JSON_ENV} is not valid JSON: {e}"
            ) from e

    env_path = os.environ.get(_CONFIG_PATH_ENV, "").strip()
    if env_path:
        path = Path(env_path)
        if not path.exists():
            raise RuntimeError(
                f"{_CONFIG_PATH_ENV} points to a non-existent file: {path}"
            )
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    if default_path is not None:
        path = Path(default_path)
        if not path.exists():
            raise RuntimeError(
                f"Config file not found: {path}\n"
                f"  Fix: set {_CONFIG_PATH_ENV}=<path> or "
                f"{_CONFIG_JSON_ENV}=<inline JSON>, or place a JSON file "
                f"at {path}."
            )
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError(
        f"No credentials provided. Set {_CONFIG_PATH_ENV}=<path> or "
        f"{_CONFIG_JSON_ENV}=<inline JSON>, or pass an explicit "
        "fallback path to load_config()."
    )
