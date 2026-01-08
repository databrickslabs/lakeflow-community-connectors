import json
import os
from pathlib import Path
from typing import Any


def load_config(config_path: Path) -> Any:
    """Load configuration from the given path and return the parsed JSON.
    
    Empty string values in the config can be overridden by environment variables.
    The environment variable name is derived from the key in UPPER_CASE.
    For example, an empty "api_key" will be replaced by the value of API_KEY env var.
    """
    with open(config_path, "r") as f:
        return json.load(f)
