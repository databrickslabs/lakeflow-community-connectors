"""Unit tests for the GitHub ``repository_files`` table.

These mock the connector's HTTP session, so they exercise the tree-walk,
filtering, batching, and blob-decoding logic without network or credentials.
They require pyspark (imported via the connector's schema module), so they run
in CI / a Databricks workspace rather than on a bare laptop.
"""

import base64
from types import SimpleNamespace

from databricks.labs.community_connector.sources.github.github import (
    GithubLakeflowConnect,
)


def _resp(status=200, payload=None, headers=None):
    return SimpleNamespace(
        status_code=status,
        headers=headers or {},
        text="",
        json=lambda: payload,
    )


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _make_connector(route_map):
    """Build a connector whose session.get is driven by a URL -> response map.

    ``route_map`` keys are matched as substrings of the request URL. The most
    specific (longest) matching key wins.
    """
    connector = GithubLakeflowConnect({"token": "fake"})

    def fake_get(url, params=None, timeout=None):  # noqa: ARG001
        recursive = bool(params and params.get("recursive"))
        # Disambiguate the two /git/trees/{sha} calls (recursive vs walk).
        candidates = [k for k in route_map if k in url]
        if "/git/trees/" in url:
            key = "TREE_RECURSIVE" if recursive else "TREE_WALK"
            tagged = [
                k for k in route_map if k.split(" ")[0] in url and k.endswith(key)
            ]
            candidates = tagged or candidates
        if not candidates:
            raise AssertionError(f"unexpected URL: {url} params={params}")
        best = max(candidates, key=len)
        return _resp(payload=route_map[best])

    connector._session = SimpleNamespace(get=fake_get)  # noqa: SLF001
    return connector


def _base_routes():
    text_blob = _b64(b"print('hi')\n")
    bin_blob = _b64(bytes([0x00, 0xFF, 0x80, 0x7F]))
    return {
        "/repos/o/r": {"default_branch": "main"},
        "/repos/o/r/commits/main": {
            "sha": "commit123",
            "commit": {"tree": {"sha": "tree123"}},
        },
        "/git/trees/tree123 TREE_RECURSIVE": {
            "truncated": False,
            "tree": [
                {"path": "app.py", "type": "blob", "sha": "b1", "size": 12, "mode": "100644"},
                {"path": "logo.png", "type": "blob", "sha": "b2", "size": 4, "mode": "100644"},
                {
                    "path": "huge.py", "type": "blob", "sha": "b3",
                    "size": 5_000_000, "mode": "100644",
                },
                {"path": "src", "type": "tree", "sha": "subtree", "mode": "040000"},
            ],
        },
        "/git/blobs/b1": {"content": text_blob, "encoding": "base64"},
        "/git/blobs/b2": {"content": bin_blob, "encoding": "base64"},
    }


def test_repository_files_default_branch_and_filtering():
    connector = _make_connector(_base_routes())
    records, offset = connector.read_table(
        "repository_files",
        None,
        {"owner": "o", "repo": "r", "include_extensions": "py"},
    )
    rows = {r["path"]: r for r in records}

    # Only .py files selected; the tree entry is skipped, png filtered out.
    assert set(rows) == {"app.py", "huge.py"}

    app = rows["app.py"]
    assert app["content"] == "print('hi')\n"
    assert app["is_binary"] is False
    assert app["commit_sha"] == "commit123"
    assert app["ref"] == "main"
    assert app["blob_sha"] == "b1"
    assert app["skipped_reason"] is None

    # Oversized file emits a metadata row with null content.
    huge = rows["huge.py"]
    assert huge["content"] is None
    assert huge["skipped_reason"] == "size_exceeds_max_file_bytes"

    # Snapshot single-batch mode returns an empty offset.
    assert offset == {}


def test_repository_files_binary_handling():
    connector = _make_connector(_base_routes())
    # No extension filter -> png (binary) is included in the listing.
    records, _ = connector.read_table(
        "repository_files",
        None,
        {"owner": "o", "repo": "r", "max_file_bytes": "1000000", "exclude_globs": "*.py"},
    )
    rows = {r["path"]: r for r in records}
    assert "logo.png" in rows
    png = rows["logo.png"]
    assert png["is_binary"] is True
    assert png["content"] is None
    assert png["content_base64"] is None  # include_binary defaults to false
    assert png["skipped_reason"] == "binary_excluded"


def test_repository_files_include_binary_base64():
    connector = _make_connector(_base_routes())
    records, _ = connector.read_table(
        "repository_files",
        None,
        {"owner": "o", "repo": "r", "include_globs": "*.png", "include_binary": "true"},
    )
    png = next(r for r in records if r["path"] == "logo.png")
    assert png["is_binary"] is True
    assert base64.b64decode(png["content_base64"]) == bytes([0x00, 0xFF, 0x80, 0x7F])


def test_repository_files_explicit_ref_skips_default_branch_lookup():
    routes = _base_routes()
    routes["/repos/o/r/commits/release"] = {
        "sha": "rel999",
        "commit": {"tree": {"sha": "tree123"}},
    }
    connector = _make_connector(routes)
    records, _ = connector.read_table(
        "repository_files",
        None,
        {"owner": "o", "repo": "r", "ref": "release", "include_extensions": "py"},
    )
    app = next(r for r in records if r["path"] == "app.py")
    assert app["ref"] == "release"
    assert app["commit_sha"] == "rel999"


def test_repository_files_index_batching_terminates():
    connector = _make_connector(_base_routes())
    opts = {"owner": "o", "repo": "r", "include_extensions": "py", "max_records_per_batch": "1"}

    batch1, off1 = connector.read_table("repository_files", None, opts)
    rows1 = list(batch1)
    assert len(rows1) == 1 and off1 == {"index": 1}

    batch2, off2 = connector.read_table("repository_files", off1, opts)
    rows2 = list(batch2)
    assert len(rows2) == 1 and off2 == {"index": 2}

    # No files left: empty batch and a stable offset -> framework stops.
    batch3, off3 = connector.read_table("repository_files", off2, opts)
    assert list(batch3) == [] and off3 == off2

    paths = {rows1[0]["path"], rows2[0]["path"]}
    assert paths == {"app.py", "huge.py"}


def test_repository_files_truncated_tree_falls_back_to_walk():
    text_blob = _b64(b"x = 1\n")
    routes = {
        "/repos/o/r/commits/main": {
            "sha": "c1",
            "commit": {"tree": {"sha": "root"}},
        },
        # Recursive call reports truncation -> connector must walk levels.
        "/git/trees/root TREE_RECURSIVE": {"truncated": True, "tree": []},
        "/git/trees/root TREE_WALK": {
            "tree": [
                {"path": "top.py", "type": "blob", "sha": "b1", "size": 6, "mode": "100644"},
                {"path": "pkg", "type": "tree", "sha": "sub", "mode": "040000"},
            ]
        },
        "/git/trees/sub TREE_WALK": {
            "tree": [
                {"path": "mod.py", "type": "blob", "sha": "b2", "size": 6, "mode": "100644"},
            ]
        },
        "/git/blobs/b1": {"content": text_blob, "encoding": "base64"},
        "/git/blobs/b2": {"content": text_blob, "encoding": "base64"},
    }
    connector = _make_connector(routes)
    records, _ = connector.read_table(
        "repository_files",
        None,
        {"owner": "o", "repo": "r", "ref": "main"},
    )
    # Full paths are reconstructed across the directory walk.
    assert {r["path"] for r in records} == {"top.py", "pkg/mod.py"}
