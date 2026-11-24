"""Regression/behavior tests for the resumable ID-sorted ``assets`` path
(`_incremental_assets_resumable`) — fix (b).

The `/assets` endpoint is keyset-paginated by id (its opaque nextCursor is an
`AFTER:id:` token) but filtered incrementally on lastModifiedOn. This path lets
a run killed mid-collection (e.g. m2m token expiry on a large full-load) resume
from the last page instead of restarting, while only advancing the
lastModifiedOn watermark when a full id-pass completes.

These exercise the pure offset logic with a fake keyset paginator — no network.
"""

import sys
from pathlib import Path

from databricks.labs.community_connector.sources.collibra import collibra as collibra_mod
from databricks.labs.community_connector.sources.collibra.collibra import (
    CollibraLakeflowConnect,
)

_FAR_FUTURE = 10**15


# --------------------------------------------------------------------------- #
# Fake keyset-paginated asset store
# --------------------------------------------------------------------------- #

class _FakeAssets:
    """Simulates /assets keyset pagination.

    Assets are id-sorted. The opaque nextCursor is modeled as the last id on a
    page; ``start_cursor`` resumes *after* that id. Pages are ``page_size`` big.
    Records carry a ``lastModifiedOn`` so the incremental window can be tested
    independently of id order.
    """

    def __init__(self, assets: list[dict], page_size: int = 2):
        # assets already in id order
        self.assets = assets
        self.page_size = page_size
        self.calls: list[str] = []  # start_cursor per invocation, for asserts

    def __call__(self, session, url, params, label, records_key="results",
                 *, start_cursor="", timeout=30):
        self.calls.append(start_cursor)
        # Resume: find index after the id encoded in start_cursor.
        if start_cursor:
            ids = [a["id"] for a in self.assets]
            begin = ids.index(start_cursor) + 1
        else:
            begin = 0
        i = begin
        while i < len(self.assets):
            page = self.assets[i:i + self.page_size]
            i += self.page_size
            is_last = i >= len(self.assets)
            next_cursor = None if is_last else page[-1]["id"]
            yield page, next_cursor


def _asset(aid: str, last_modified: int) -> dict:
    return {"id": aid, "name": aid, "lastModifiedOn": last_modified}


def _connector() -> CollibraLakeflowConnect:
    c = CollibraLakeflowConnect({"org": "simulator", "access_token": "fake"})
    c._init_ts = _FAR_FUTURE
    return c


def _drain(result):
    records, offset = result
    return [r["id"] for r in records], offset


def _read(conn, start_offset, table_options):
    base_params = {"limit": "2", "sortField": "ID", "sortOrder": "ASC"}
    return conn._incremental_assets_resumable(base_params, start_offset, table_options)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

def test_resume_mid_pass_covers_full_collection_no_dups(monkeypatch):
    """Cap forces a mid-pass stop; resuming from the persisted page_token emits
    the remainder. Union == full collection, no dups, no loss."""
    assets = [_asset(f"a{i:02d}", 100 + i) for i in range(6)]  # a00..a05
    fake = _FakeAssets(assets, page_size=2)
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", fake)
    conn = _connector()
    opts = {"max_records_per_batch": "2"}

    # First run: cap=2 → one page, then stop at page boundary.
    ids1, off1 = _drain(_read(conn, {}, opts))
    assert ids1 == ["a00", "a01"]
    assert off1.get("page_token"), "should persist a resume token mid-pass"
    assert "pass_ts" in off1
    # Watermark must NOT advance mid-pass.
    assert off1["cursor"] == 0

    # Second run: resume from the token.
    ids2, off2 = _drain(_read(conn, off1, opts))
    assert ids2 == ["a02", "a03"]
    assert off2.get("page_token")

    # Third run: resume again → last chunk, pass completes.
    ids3, off3 = _drain(_read(conn, off2, opts))
    assert ids3 == ["a04", "a05"]
    assert "page_token" not in off3, "pass complete clears the resume token"

    # Full union, no dups, no loss.
    allids = ids1 + ids2 + ids3
    assert allids == [f"a{i:02d}" for i in range(6)]
    assert len(allids) == len(set(allids))


def test_watermark_only_advances_on_pass_completion(monkeypatch):
    """The committed lastModifiedOn watermark stays put mid-pass and jumps to
    the frozen snapshot boundary (pass_ts) only when the pass completes."""
    assets = [_asset(f"a{i:02d}", 100 + i) for i in range(4)]
    fake = _FakeAssets(assets, page_size=2)
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", fake)
    conn = _connector()
    opts = {"max_records_per_batch": "2"}

    _, off1 = _drain(_read(conn, {}, opts))
    assert off1["cursor"] == 0 and off1.get("page_token")   # held

    _, off2 = _drain(_read(conn, off1, opts))
    # pass complete → watermark advances to the frozen pass_ts (== _init_ts)
    assert off2["cursor"] == _FAR_FUTURE
    assert "page_token" not in off2


def test_pass_ts_frozen_across_resume(monkeypatch):
    """A resumed pass keeps the ORIGINAL snapshot boundary even if _init_ts
    changed, so the window is consistent across the multi-run pass."""
    assets = [_asset(f"a{i:02d}", 100 + i) for i in range(4)]
    fake = _FakeAssets(assets, page_size=2)
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", fake)
    conn = _connector()
    opts = {"max_records_per_batch": "2"}

    _, off1 = _drain(_read(conn, {}, opts))
    frozen = off1["pass_ts"]
    # Simulate a later run with a moved-on init time; the pass must ignore it.
    conn._init_ts = _FAR_FUTURE + 5000
    _, off2 = _drain(_read(conn, off1, opts))
    # Completed pass advances to the FROZEN boundary, not the new _init_ts.
    assert off2["cursor"] == frozen == _FAR_FUTURE


def test_fresh_pass_after_completion_starts_from_new_floor(monkeypatch):
    """After a pass completes, a fresh pass uses strict `> committed` and emits
    only records modified after the committed floor."""
    assets = [_asset(f"a{i:02d}", 100 + i) for i in range(4)]  # lm 100..103
    fake = _FakeAssets(assets, page_size=10)  # single page → completes at once
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", fake)
    conn = _connector()

    # First full pass (no cap): emits everything ≤ pass_ts, completes.
    ids1, off1 = _drain(_read(conn, {}, {}))
    assert ids1 == ["a00", "a01", "a02", "a03"]
    committed = off1["cursor"]
    assert committed == _FAR_FUTURE and "page_token" not in off1

    # Next run from the committed floor: converges, emits nothing new.
    ids2, off2 = _drain(_read(conn, off1, {}))
    assert ids2 == []
    assert off2 == {"cursor": committed}


def test_incremental_window_excludes_committed_and_future(monkeypatch):
    """Only records with committed < lastModifiedOn <= pass_ts are emitted."""
    assets = [
        _asset("a00", 50),    # <= committed floor → excluded
        _asset("a01", 150),   # in window
        _asset("a02", _FAR_FUTURE + 1),  # > pass_ts (after init) → excluded
        _asset("a03", 200),   # in window
    ]
    fake = _FakeAssets(assets, page_size=10)
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", fake)
    conn = _connector()

    ids, off = _drain(_read(conn, {"cursor": 100}, {}))
    assert ids == ["a01", "a03"]           # 150 and 200 only
    assert off["cursor"] == _FAR_FUTURE     # pass completed to snapshot boundary


def test_name_sort_falls_back_to_full_drain(monkeypatch):
    """sort_field=NAME is a NON-unique keyset, so `_read_assets` must NOT use
    the resumable path — it drains fully and persists no page_token."""
    assets = [_asset(f"a{i:02d}", 100 + i) for i in range(6)]

    # Full-drain path uses cursor_paginate (flat record iterator).
    def fake_flat(session, url, params, label, records_key="results", *, timeout=30):
        for a in assets:
            yield a
    monkeypatch.setattr(collibra_mod, "cursor_paginate", fake_flat)

    # If the resumable path were wrongly taken, this would blow up / be used.
    def boom(*a, **k):
        raise AssertionError("resumable path must not run for NAME sort")
        yield  # pragma: no cover
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", boom)

    conn = _connector()
    records, off = conn._read_assets(
        {}, {"sort_field": "NAME", "max_records_per_batch": "2"}
    )
    ids = [r["id"] for r in records]
    # Full drain ignores the cap → all 6, and no resume token persisted.
    assert ids == [f"a{i:02d}" for i in range(6)]
    assert "page_token" not in off


def test_caught_up_converges(monkeypatch):
    """When the committed floor has reached the snapshot boundary and no pass is
    in flight, emit nothing and return the offset unchanged (converges)."""
    fake = _FakeAssets([_asset("a00", 100)], page_size=10)
    monkeypatch.setattr(collibra_mod, "cursor_paginate_pages", fake)
    conn = _connector()

    start = {"cursor": _FAR_FUTURE}
    ids, off = _drain(_read(conn, start, {}))
    assert ids == []
    assert off == start
    # Should not have paged the API at all.
    assert fake.calls == []
