"""Reusable resumable-CDC read helper for community connectors.

Many sources expose a keyset- or continuation-paginated list endpoint that is
*sorted by a stable key* (an id, or an opaque page token) but filtered
*incrementally on a different field* (a ``lastModified`` cursor). Those are two
independent dimensions, so a run killed partway through a scan — commonly an
m2m OAuth token expiring on a long full-load — cannot simply advance the
modified-since watermark: a partial slice has an arbitrary max cursor, and
advancing the watermark to it would silently drop not-yet-seen records.

``resumable_cdc_read`` implements that resumable state machine once, so a
connector supplies only the source-specific pieces:

  * ``paginate``  — a page iterator yielding ``(batch, next_token)`` per page,
                    resumable from an opaque token (``""`` = first page,
                    ``next_token is None`` marks the last page).
  * ``cursor_of`` — extract the incremental cursor from a raw record.
  * ``shape``     — transform a raw record into an output row.

The framework already checkpoints the returned offset dict across runs and
re-supplies it as ``start_offset`` (see ``interface/lakeflow_connect.py`` and
``sparkpds/lakeflow_datasource.py``): this helper persists nothing itself. It
only decides *what* to put in the offset so that

  * within a pass, progress is by the stable page key, so truncation never
    skips a record;
  * the modified-since watermark advances ONLY after a provably complete pass,
    so the next fresh pass's strict ``> watermark`` filter cannot skip an
    un-emitted record; and
  * ``max_records`` caps the batch at a *page* boundary and persists the page
    token, so the next run resumes there instead of restarting.

Offset schema (generic keys; a pre-existing connector may translate to its own
persisted keys — see the Collibra ``assets`` reader):

  * ``watermark``    — committed modified-since floor (advances on completion)
  * ``resume_token`` — opaque page token to resume an in-progress pass
  * ``snapshot_ts``  — upper bound frozen at pass start, kept across resumes

Cursor values are compared with the standard ordering operators, so any
consistently ordered, JSON-serialisable type works (int epoch ms, ISO-8601 UTC
strings, ...). ``cursor_of`` and ``snapshot_ts`` must yield the same type.
"""

from typing import Any, Callable, Iterator, Optional

WATERMARK = "watermark"
RESUME_TOKEN = "resume_token"
SNAPSHOT_TS = "snapshot_ts"

# A page iterator yields (batch, next_token) per page; next_token is None on the
# last page (no resume point after it).
PageIterator = Iterator[tuple[list[dict[str, Any]], Optional[str]]]


def resumable_cdc_read(
    *,
    start_offset: Optional[dict],
    snapshot_ts: Any,
    paginate: Callable[[str], PageIterator],
    cursor_of: Callable[[dict], Any],
    shape: Callable[[dict], dict],
    max_records: Optional[int],
) -> tuple[Iterator[dict], dict]:
    """Run one resumable incremental batch over a keyset/continuation source.

    Args:
        start_offset: The checkpointed offset from the previous call (``None``/
            empty on the first ever call). Uses the ``watermark`` /
            ``resume_token`` / ``snapshot_ts`` keys.
        snapshot_ts: Upper bound for a *fresh* pass — the connector's init-time
            timestamp, so a Trigger.AvailableNow microbatch only drains data
            that existed when the connector started and therefore terminates.
        paginate: ``resume_token -> Iterator[(batch, next_token)]``. Begins
            paging from ``resume_token`` (``""`` = first page).
        cursor_of: ``raw -> cursor`` (or ``None`` when a record has no
            comparable cursor; such records are always emitted and never move
            the watermark).
        shape: ``raw -> output row``.
        max_records: Page-granular batch cap, or ``None`` to drain the whole
            pass in one batch.

    Returns:
        ``(records, end_offset)``. On a completed pass the offset carries only
        the advanced ``watermark``; mid-pass it carries the held watermark plus
        the ``resume_token`` and frozen ``snapshot_ts``. When already caught up,
        ``end_offset`` echoes the committed watermark so the trigger converges.
    """
    start_offset = start_offset or {}
    watermark = start_offset.get(WATERMARK)
    resume_token = start_offset.get(RESUME_TOKEN) or ""
    pass_ts = start_offset.get(SNAPSHOT_TS)

    # Fresh pass whenever no pass is in flight (no resume token, or no frozen
    # snapshot bound to resume under). Freeze the bound at the pass start.
    if not resume_token or pass_ts is None:
        resume_token = ""
        pass_ts = snapshot_ts

    # Already caught up: the floor reached the snapshot bound and no pass is in
    # flight — nothing new can be emitted, so converge (echo the watermark).
    if not resume_token and watermark is not None and watermark >= pass_ts:
        return iter([]), {WATERMARK: watermark}

    records: list[dict[str, Any]] = []
    next_resume: Optional[str] = None
    pass_complete = True
    for batch, next_token in paginate(resume_token):
        for raw in batch:
            cursor = cursor_of(raw)
            # Incremental window: (watermark, snapshot_ts].
            if watermark is not None and cursor is not None and cursor <= watermark:
                continue
            if cursor is not None and cursor > pass_ts:
                continue
            records.append(shape(raw))

        # Page-granular cap: stop at this boundary and resume here next run.
        # Only a page with a real next_token is a resume point; next_token None
        # means this was the last page, so the pass has actually completed.
        if (
            max_records is not None
            and len(records) >= max_records
            and next_token is not None
        ):
            next_resume = next_token
            pass_complete = False
            break

    if pass_complete:
        # Whole key-space scanned under one snapshot: advance the committed
        # watermark to the snapshot bound and clear the pass state.
        new_watermark = pass_ts if watermark is None else max(watermark, pass_ts)
        return iter(records), {WATERMARK: new_watermark}

    # Mid-pass: hold the watermark, persist the resume point + frozen bound.
    return iter(records), {
        WATERMARK: watermark,
        RESUME_TOKEN: next_resume,
        SNAPSHOT_TS: pass_ts,
    }
