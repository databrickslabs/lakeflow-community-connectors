"""Test utility functions for loading configs."""
import json
from pathlib import Path
from typing import Any


def load_config(config_path: Path) -> Any:
    """Load configuration from the given path and return the parsed JSON."""
    with open(config_path, "r") as f:
        return json.load(f)
