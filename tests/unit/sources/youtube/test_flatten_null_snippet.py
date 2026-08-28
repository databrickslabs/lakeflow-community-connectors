"""Flatten helpers must tolerate API fields set to null, not only missing."""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from databricks.labs.community_connector.sources.youtube.youtube import (
    _flatten_comment_thread,
    _flatten_video_category,
)


def test_flatten_comment_thread_snippet_null():
    row = _flatten_comment_thread({"id": "t1", "snippet": None})
    assert row["id"] == "t1"
    assert row["snippet_canReply"] is None
    assert row["snippet_totalReplyCount"] is None


def test_flatten_video_category_snippet_null():
    row = _flatten_video_category({"id": "c1", "snippet": None})
    assert row["id"] == "c1"
    assert row["snippet_assignable"] is None
