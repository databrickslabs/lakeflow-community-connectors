"""video_categories table option validation."""
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import YouTubeLakeflowConnect

_OPTIONS = {"api_key": "test-key"}


def test_video_categories_requires_region_code():
    conn = YouTubeLakeflowConnect(_OPTIONS)
    with pytest.raises(ValueError, match="region_code"):
        list(conn.read_table("video_categories", {}, {})[0])


def test_video_categories_rejects_blank_region_code():
    conn = YouTubeLakeflowConnect(_OPTIONS)
    with pytest.raises(ValueError, match="region_code"):
        list(conn.read_table("video_categories", {}, {"region_code": "  "})[0])
