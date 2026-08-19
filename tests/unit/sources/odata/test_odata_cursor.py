"""OData connector unit tests — cursor group.

Split from the former monolithic ``test_odata_lakeflow_connect.py``.
Shared metadata/helpers live in ``_odata_test_helpers``.
"""

import json
import re
import time

import pytest
import responses
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect

from tests.unit.sources.odata._odata_test_helpers import (
    _FK_NULL_MD,
    GUID_CURSOR_METADATA_XML,
    PROBE_TABLE,
    SERVICE_URL,
    _batch_responder,
    _churn_children_cb,
    _drop_lb,
    _make,
    _mids_reject_expand_callback,
    _mock_metadata,
    _mock_nested_metadata,
    _mock_probe_metadata,
    _probe_filter_floor,
    _probe_mids_callback,
    _skip_probe_preflight,
)


def test_cursor_comparisons_are_chronological_not_lexical():
    """Client-side cursor ordering must match the SERVER's chronological
    ordering. OData's JSON format makes fractional seconds optional per
    value (Olingo/SAP trim trailing zeros), so one column renders both
    ``…00Z`` and ``…00.5Z`` — and Python string order puts the LATER
    ``.5Z`` first (``.`` < ``Z``), which silently drops re-filtered rows
    and regresses watermark maxes."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_le,
        cursor_max,
        cursor_newer,
        max_or,
    )

    # The bug cases: fractional vs whole second, differing precision.
    assert cursor_newer("2024-01-01T23:00:00.5Z", "2024-01-01T23:00:00Z")
    assert not cursor_le("2024-01-01T23:00:00.5Z", "2024-01-01T23:00:00Z")
    assert cursor_newer("2024-01-01T23:00:00.51Z", "2024-01-01T23:00:00.5Z")
    assert cursor_max(["2024-01-01T23:00:00.5Z", "2024-01-01T23:00:00Z"]) == (
        "2024-01-01T23:00:00.5Z"
    )
    assert max_or("2024-01-01T23:00:00Z", "2024-01-01T23:00:00.5Z") == ("2024-01-01T23:00:00.5Z")
    # Sub-microsecond precision (SQL Server datetime2(7) emits 7-digit
    # fractions): Python datetimes truncate to µs, so the PARSED keys tie —
    # the raw-text tie-break must still order chronologically, or the
    # <= since re-filter drops a strictly-newer row the server correctly
    # returned (the round-13 loss mechanism one scale down).
    assert cursor_newer("2024-01-01T23:00:00.1234568Z", "2024-01-01T23:00:00.1234567Z")
    assert not cursor_le("2024-01-01T23:00:00.1234568Z", "2024-01-01T23:00:00.1234567Z")
    assert (
        cursor_max(["2024-01-01T23:00:00.1234567Z", "2024-01-01T23:00:00.1234568Z"])
        == "2024-01-01T23:00:00.1234568Z"
    )  # true max regardless of order
    # Differing digit counts below the µs boundary compare zero-padded
    # (.12345675 > .1234567 == .12345670) — raw-text comparison would
    # invert here because the shorter fraction's 'Z' sorts above digits.
    assert cursor_newer("2024-01-01T23:00:00.12345675Z", "2024-01-01T23:00:00.1234567Z")
    # Equal instants rendered two ways: the consistent raw tie-break errs
    # only in the duplicate-safe direction at the re-filter — a same-instant
    # re-read is either dropped (correct) or kept (MERGE-deduped duplicate),
    # never a lost newer row.
    assert cursor_le("2024-01-01T23:00:00+00:00", "2024-01-01T23:00:00Z")  # dropped: correct
    assert not cursor_le("2024-01-01T23:00:00Z", "2024-01-01T23:00:00+00:00")  # kept: dup-safe
    assert cursor_max(["2024-01-01T23:00:00Z", "2024-01-01T23:00:00+00:00"]) == (
        "2024-01-01T23:00:00Z"
    )
    # Identical texts still tie exactly.
    assert not cursor_newer("2024-01-01T23:00:00Z", "2024-01-01T23:00:00Z")
    assert cursor_le("2024-01-01T23:00:00Z", "2024-01-01T23:00:00Z")
    # Offsets order chronologically, not textually.
    assert cursor_newer("2024-01-01T23:00:00Z", "2024-01-02T08:59:00+10:00")
    # Non-ISO values keep their natural ordering; ints untouched.
    assert cursor_newer("b", "a") and not cursor_newer("A", "a")
    assert cursor_newer(10, 9) and cursor_le(9, 10)
    # A shape-mixed pair degrades to raw comparison instead of raising.
    assert cursor_newer("zzz", "2024-01-01T00:00:00Z")


# ---------------------------------------------------------------------------
# Incremental read
# ---------------------------------------------------------------------------


@responses.activate
def test_incremental_first_call_has_no_cursor_filter():
    """No wall-clock ceiling means the first call (`since=None`) sends
    no `$filter` clause derived from the cursor. The server returns rows
    from the natural start of the table; `max_records_per_batch` is the
    per-call cap. This is what makes the connector usable for both
    continuous polling and non-timestamp cursor types."""
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        # First (unfiltered) request returns the row; the default `auto`
        # drain issues one confirming keyset seek (carrying `gt`) — the
        # seek-honouring server returns empty, ending the collection.
        if " gt " in request.url.replace("%20", " "):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "10"},
    )
    rows = list(records)
    # ``Name`` is padded to None: the emit boundary fills declared columns the
    # server omitted so a null-omitting response parses cleanly.
    assert rows == [{"Id": 1, "Name": None, "ModifiedAt": "2024-03-01T00:00:00Z"}]
    assert _drop_lb(offset) == {"cursor": "2024-03-01T00:00:00Z"}
    # Neither `le` nor `gt` should appear on the FIRST call — no cursor
    # filter at all when resuming from an empty offset.
    normalised = captured_urls[0].replace("%20", " ")
    assert " le " not in normalised
    assert " gt " not in normalised


@responses.activate
def test_incremental_non_utc_offset_watermark_is_percent_encoded():
    """A source emitting local-offset timestamps (SAP-style) puts a ``+`` in
    the watermark. The generated ``$filter`` must carry it as ``%2B`` — a raw
    ``+`` is decoded as a SPACE by form-decoding servers, turning the filter
    into a malformed timestamp and 400-ing every incremental batch."""
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        if " gt " in request.url.replace("%20", " "):
            return (
                200,
                {},
                '{"value": [{"Id": 2, "ModifiedAt": "2024-03-02T00:00:00+10:00"}]}'
                if len(captured_urls) == 1
                else '{"value": []}',
            )
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-03-01T00:00:00+10:00"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [2]
    assert _drop_lb(offset) == {"cursor": "2024-03-02T00:00:00+10:00"}
    # The offset's ``+`` reached the wire percent-encoded, never raw.
    first = captured_urls[0]
    assert "%2B10:00" in first
    assert "+10:00" not in first


@responses.activate
def test_incremental_fractional_second_rendering_not_dropped():
    """A server that renders fractional seconds only when non-zero (spec-
    allowed; Olingo/SAP trim trailing zeros) returns ``…00.5Z`` for a row
    newer than the ``…00Z`` watermark. Lexically ``.`` < ``Z``, so a raw
    ``<=`` re-filter dropped the row the server correctly returned — with
    nothing else new the batch came back empty and the stream quiesced with
    the row permanently invisible. The chronological comparison keeps it,
    and the watermark max must not regress behind it either."""
    _mock_metadata()

    def _callback(request):
        if " gt " in request.url.replace("%20", " "):
            # Server-side chronological gt correctly returns the .5Z row
            # for since=…00Z; the confirming drain seek returns empty.
            if "23:00:00Z" in request.url.replace("%3A", ":"):
                return (
                    200,
                    {},
                    '{"value": [{"Id": 2, "ModifiedAt": "2024-01-01T23:00:00.5Z"}]}',
                )
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)
    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-01-01T23:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [2]  # not silently dropped
    # Watermark advanced to the fractional value (no lexical regression).
    assert _drop_lb(offset) == {"cursor": "2024-01-01T23:00:00.5Z"}


@responses.activate
def test_incremental_supports_integer_cursor():
    """Cursor type is opaque to the filter logic — monotonic IDs work
    just like timestamps. Verifies the resume URL carries an `OrderID gt
    N` clause with an unquoted integer literal."""
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Orders", callback=_callback)

    c = _make()
    start = {"cursor": 10248}
    records, offset = c.read_table("Orders", start, {"cursor_field": "OrderId"})
    assert list(records) == []
    assert offset == start
    normalised = captured_urls[0].replace("%20", " ")
    assert "OrderId gt 10248" in normalised
    # The literal is unquoted (matches Edm.Int32 syntax, not Edm.String).
    assert "'10248'" not in normalised


@responses.activate
def test_incremental_resume_uses_gt_filter_and_terminates():
    _mock_metadata()
    captured_urls = []

    def _callback(request):
        captured_urls.append(request.url)
        # Return no new rows so termination kicks in.
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    start = {"cursor": "2024-03-01T00:00:00Z"}
    records, offset = c.read_table(
        "Customers",
        start,
        {"cursor_field": "ModifiedAt"},
    )
    assert list(records) == []
    # Caller passes start_offset back unchanged on the "no data" path.
    assert offset == start
    # We tried the API once (cursor < init_ts), URL must include the `gt` clause.
    assert any("gt" in u for u in captured_urls)


@responses.activate
def test_incremental_continuous_polling_picks_up_new_rows():
    """A connector instance reused across multiple `read_table` calls
    sees fresh source state on each call. Mirrors what a continuous
    SDP pipeline does: one connector, many micro-batches, source
    growing under us. Each subsequent call should advance through the
    new rows.

    The mock is a seek-honouring server (the only faithful model now
    that the default `auto` flat cursor read drains): it filters the
    corpus by the connector's `ModifiedAt gt <v>` resume / keyset-seek
    lower bound, so each batch returns exactly the rows above the
    parked watermark."""
    _mock_metadata()
    corpus = [
        {"Id": 1, "ModifiedAt": "2024-03-01T00:00:00Z"},
        {"Id": 2, "ModifiedAt": "2024-03-02T00:00:00Z"},
    ]

    def _callback(request):
        url = request.url.replace("%20", " ")
        rows = corpus
        # Honour every `ModifiedAt gt <v>` lower bound on the URL (the
        # cross-batch resume filter and the in-batch keyset-seek drain
        # both carry one); the tightest bound wins.
        bounds = re.findall(r"ModifiedAt gt ([0-9T:\-Z]+)", url)
        if bounds:
            lo = max(bounds)
            rows = [r for r in rows if r["ModifiedAt"] > lo]
        rows = sorted(rows, key=lambda r: (r["ModifiedAt"], r["Id"]))
        return (200, {}, json.dumps({"value": rows}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    # Batch 1: no offset, both rows drain. Trim of the trailing distinct
    # cursor cohort holds Id=2 back; emits [Id=1]; offset = 2024-03-01.
    rows1, offset1 = c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows1] == [1]
    assert _drop_lb(offset1) == {"cursor": "2024-03-01T00:00:00Z"}

    # Batch 2: feeding offset1 back, the held-back Id=2 is re-read above
    # the watermark and emitted; offset advances to 2024-03-02.
    rows2, offset2 = c.read_table("Customers", offset1, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows2] == [2]
    assert _drop_lb(offset2) == {"cursor": "2024-03-02T00:00:00Z"}

    # A new row arrives while the stream is idle.
    corpus.append({"Id": 3, "ModifiedAt": "2024-03-05T00:00:00Z"})

    # Batch 3: the same connector instance picks up the fresh row.
    rows3, offset3 = c.read_table("Customers", offset2, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows3] == [3]
    assert _drop_lb(offset3) == {"cursor": "2024-03-05T00:00:00Z"}

    # Batch 4: caught up — no rows above the watermark, stable offset
    # signals "no more data" to Spark.
    rows4, offset4 = c.read_table("Customers", offset3, {"cursor_field": "ModifiedAt"})
    assert list(rows4) == []
    assert offset4 == offset3

    # Batch 3: new row appeared in the source. The continuous-polling
    # connector picks it up using only the `gt` filter — no frozen
    # snapshot ceiling getting in the way.
    rows3, offset3 = c.read_table("Customers", offset2, {"cursor_field": "ModifiedAt"})
    assert [r["Id"] for r in rows3] == [3]
    assert _drop_lb(offset3) == {"cursor": "2024-03-05T00:00:00Z"}


@responses.activate
def test_incremental_trims_trailing_same_cursor_cohort_when_truncated():
    """Cap-hit boundary: trim the trailing same-cursor cohort so the next
    call's `cursor gt <last>` doesn't drop the cohort's unread siblings.
    Re-fetched cohort members are deduped at the destination by MERGE on
    the primary key."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-03T00:00:00Z"},
                {"Id": 4, "ModifiedAt": "2024-05-03T00:00:00Z"},  # trimmed
                {"Id": 5, "ModifiedAt": "2024-05-03T00:00:00Z"},  # trimmed (cap)
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "5"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    assert _drop_lb(offset) == {"cursor": "2024-05-02T00:00:00Z"}


@responses.activate
def test_incremental_trims_boundary_cohort_on_natural_exhaustion_too():
    """Trim also runs on naturally-exhausted batches. With a
    low-cardinality cursor, same-cursor siblings could arrive between
    this batch and a future call (stop/restart, concurrent insert) —
    trimming forces the next call's `cursor gt <previous_distinct>` to
    re-fetch the whole cohort plus any new arrivals."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},  # trimmed
                {"Id": 3, "ModifiedAt": "2024-05-02T00:00:00Z"},  # trimmed
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert _drop_lb(offset) == {"cursor": "2024-05-01T00:00:00Z"}


@responses.activate
def test_incremental_all_same_cursor_truncated_raises():
    """If the whole truncated batch shares one cursor, the cap is smaller
    than the same-cursor cohort and we can't trim without losing data —
    surface that loudly."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    with pytest.raises(RuntimeError, match="max_records_per_batch"):
        records, _ = c.read_table(
            "Customers",
            {},
            {"cursor_field": "ModifiedAt", "max_records_per_batch": "3"},
        )
        list(records)


@responses.activate
def test_incremental_all_same_cursor_natural_exhaustion_emits_as_is():
    """When the whole batch shares one cursor AND it's the natural end
    of the result set, there's nowhere to retreat to — emit the cohort
    rather than losing it. Accept the residual race that same-cursor
    rows arriving later won't be picked up; resolved by giving the
    cursor field higher cardinality."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "100"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2, 3]
    assert _drop_lb(offset) == {"cursor": "2024-05-01T00:00:00Z"}


@responses.activate
def test_incremental_first_batch_null_cursor_rows_raises():
    """Regression: flat incremental path used to build
    ``end_offset = {"cursor": records[-1].get(cursor_field)}``,
    which becomes ``{"cursor": None}`` when the trailing record carries
    a null cursor (and the same-cohort fall-through keeps the rows).
    Combined with the old truthy guard ``if start_offset and
    start_offset == end_offset``, the first streaming batch
    (``start_offset = {}``) bypassed the guard and committed null-cursor
    rows with the offset advancing to ``{"cursor": None}`` — subsequent
    triggers re-emit the same rows. The fix normalizes the
    no-cursor-data case to ``{}`` and routes through
    ``_finalize_cursor_read``, which raises so the operator sees the
    cause."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": None},
                {"Id": 2, "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    with pytest.raises(RuntimeError, match="did not advance"):
        records, _ = c.read_table(
            "Customers",
            {},
            {
                "cursor_field": "ModifiedAt",
                "max_records_per_batch": "100",
                "cursor_nulls": "error",
            },
        )
        list(records)


@responses.activate
def test_incremental_batch_mode_null_cursor_rows_emit_without_raise():
    """Batch reader (`LakeflowBatchReader`) passes ``start_offset=None``
    and discards the returned offset. The no-progress guard is a
    streaming concern — without an offset that the framework re-issues,
    null-cursor data can't loop. ``_finalize_cursor_read`` treats
    ``start_offset is None`` as the batch-reader signal and emits rows
    as-is. The companion streaming test
    (``test_incremental_first_batch_null_cursor_rows_raises``) shows
    the same data raises when ``start_offset={}`` — this test locks
    the batch/streaming split so a future refactor that re-normalizes
    None to {} (re-introducing the bug class) breaks loudly."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": None},
                {"Id": 2, "ModifiedAt": None},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, _ = c.read_table(
        "Customers",
        None,
        {
            "cursor_field": "ModifiedAt",
            "max_records_per_batch": "100",
            "cursor_nulls": "error",
        },
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]


@responses.activate
def test_incremental_coalesce_default_emits_null_rows_and_advances():
    """Default ``cursor_nulls=coalesce``: a null-only streaming batch is
    emitted (column left null) and the watermark advances via a
    synthetic floor, so no no-progress RuntimeError fires."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": None}, {"Id": 2, "ModifiedAt": None}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})
    rows = list(records)
    assert [r["Id"] for r in rows] == [1, 2]
    # The real null is preserved in the emitted rows (synthetic is internal).
    assert all(r["ModifiedAt"] is None for r in rows)
    # Watermark advanced to the default synthetic floor (year 2000), not {}.
    assert offset["cursor"].startswith("2000-01-01T00:00:00.")


@responses.activate
def test_incremental_coalesce_floor_year_configurable():
    """``cursor_nulls=coalesce:<YYYY>`` overrides the temporal synthetic
    floor year (default 2000)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": None}]},
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "coalesce:1990"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert rows[0]["ModifiedAt"] is None
    assert offset["cursor"].startswith("1990-01-01T00:00:00.")


@responses.activate
def test_cursor_nulls_floor_year_with_non_coalesce_raises():
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    with pytest.raises(ValueError, match="floor year is only valid with 'coalesce'"):
        records, _ = c.read_table(
            "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "error:1990"}
        )
        list(records)


@responses.activate
def test_incremental_ignore_skips_null_rows():
    """``cursor_nulls=ignore`` drops null-cursor rows entirely; only the
    real-cursor row is emitted and drives the watermark."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": None},
                {"Id": 2, "ModifiedAt": "2024-01-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    records, offset = c.read_table(
        "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "ignore"}
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [2]
    assert _drop_lb(offset) == {"cursor": "2024-01-01T00:00:00Z"}


@responses.activate
def test_cursor_nulls_invalid_value_raises():
    _mock_metadata()
    responses.get(
        f"{SERVICE_URL}Customers",
        json={"value": [{"Id": 1, "ModifiedAt": "2024-01-01T00:00:00Z"}]},
    )
    c = _make()
    with pytest.raises(ValueError, match="cursor_nulls"):
        records, _ = c.read_table(
            "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_nulls": "bogus"}
        )
        list(records)


@responses.activate
def test_incremental_orderby_appends_primary_key_tiebreaker():
    """`$orderby` must be a total order, not just by cursor.

    OData servers that paginate via `@odata.nextLink` typically derive
    the skiptoken from the order-by columns. When the cursor (here
    ModifiedAt) has duplicates and `$orderby` is cursor-only, the
    skiptoken's strict `>` on the cursor drops the unread tail of a
    same-cursor cohort that straddles a page boundary. Appending the
    primary key forces a unique total order so the skiptoken is stable.
    """
    _mock_metadata()
    captured = {}

    def _callback(request):
        captured["url"] = request.url
        return (200, {}, '{"value": []}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Customers", callback=_callback)

    c = _make()
    c.read_table("Customers", {}, {"cursor_field": "ModifiedAt"})
    # `Id` is Customers' Key in METADATA_XML.
    url = captured["url"]
    assert "ModifiedAt" in url and "asc" in url
    assert "Id" in url
    # Both terms must appear consecutively in the orderby clause. The
    # comma between them may be raw `,` or `%2C`; the space may be raw
    # ` ` or `%20`. Use a normalised check.
    normalised = url.replace("%20", " ").replace("%2C", ",")
    assert "$orderby=ModifiedAt asc,Id asc" in normalised


@responses.activate
def test_incremental_client_strict_gt_drops_boundary_row():
    """A defensive client-side strict-`>` filter guards against any
    server returning a record equal to `since`. The previous batch's
    boundary record never appears twice — the client filter drops it
    before the trim runs."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                # Server returned a record at the boundary cursor (would
                # happen if a server treated `gt` as `ge`).
                {"Id": 1, "ModifiedAt": "2024-05-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-05-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-05-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {"cursor": "2024-05-01T00:00:00Z"},
        {"cursor_field": "ModifiedAt"},
    )
    rows = list(records)
    # Id 1 dropped by the strict-`>` client filter. Id 3 (the trailing
    # cohort at 2024-05-03) is then trimmed so the next call's
    # `cursor gt 2024-05-02` re-fetches it.
    assert [r["Id"] for r in rows] == [2]
    assert _drop_lb(offset) == {"cursor": "2024-05-02T00:00:00Z"}


@responses.activate
def test_incremental_max_records_caps_batch_with_boundary_trim():
    """When the cap is hit, the trailing same-cursor cohort (here just one
    distinct row at the boundary) is trimmed. The next call re-fetches it
    via `cursor gt <prev_distinct>` and the destination MERGEs on PK."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [
                {"Id": 1, "ModifiedAt": "2024-04-01T00:00:00Z"},
                {"Id": 2, "ModifiedAt": "2024-04-02T00:00:00Z"},
                {"Id": 3, "ModifiedAt": "2024-04-03T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )

    c = _make()
    records, offset = c.read_table(
        "Customers",
        {},
        {"cursor_field": "ModifiedAt", "max_records_per_batch": "2"},
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [1]
    assert _drop_lb(offset) == {"cursor": "2024-04-01T00:00:00Z"}


@responses.activate
def test_lookback_dedup_suppresses_unchanged_overlap_re_emits():
    """``cursor_lookback_dedup=on``: rows re-fetched by the overlap window
    that were already delivered UNCHANGED are suppressed; the offset carries
    the exact ``lb_seen`` map. A pure-overlap follow-up batch emits nothing
    and idles (offset echo), exactly like today's pure-overlap batch."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):  # top-level auto drain probe
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "on",
    }
    records, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert [r["Id"] for r in records] == [11, 12]
    assert offset["cursor"] == "2024-01-03T00:00:00Z"
    assert len(offset["lb_seen"]) == 2  # both delivered rows tracked
    # Batch 2: the same rows come back through the 1h overlap — all suppressed.
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert list(records2) == []
    assert offset2 == offset  # idle echo; lb_seen preserved


@responses.activate
def test_lookback_dedup_reemits_changed_row_same_cursor():
    """A row whose NON-CURSOR column changed between batches (cursor
    untouched — a source that updates without advancing the cursor) must be
    re-emitted: the content hash differs. The unchanged sibling stays
    suppressed. This is the hazard that keying on (PK, cursor) alone would
    silently swallow."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "on",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    children[0] = {"Id": 11, "Label": "CHANGED", "ModifiedAt": "2024-01-02T12:00:00Z"}
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    got = [(r["Id"], r["Label"]) for r in records2]
    assert got == [(11, "CHANGED")]  # changed row re-emitted, sibling suppressed
    # The changed row's new hash replaces its entry (batch 3 suppresses it).
    records3, _ = c.read_table("Parents__Children", offset2, opts)
    assert list(records3) == []


@responses.activate
def test_lookback_dedup_cap_overflow_keeps_newest_and_reemits_rest():
    """Above the entry cap, the highest-cursor entries are kept and the
    evicted rows degrade to plain re-emits — the pre-dedup behavior, never
    loss. cap=1 over two in-window rows: the newer row stays suppressed;
    the evicted older row follows the pre-dedup idle rule (deferred on a
    quiescent trigger, re-emitted on the next PROGRESSING batch)."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "1",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert len(offset["lb_seen"]) == 1  # capped; newest (Id 12) kept
    # Quiescent trigger: the evicted row's re-read produces no lb_seen delta,
    # so the pre-dedup idle rule holds — deferred, not replayed per trigger.
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert list(records2) == []
    # A PROGRESSING batch (new row 13) delivers the evicted re-read alongside
    # the new row; the capped entry (Id 12) stays suppressed.
    children.append({"Id": 13, "Label": "c", "ModifiedAt": "2024-01-04T00:00:00Z"})
    records3, offset3 = c.read_table("Parents__Children", offset2, opts)
    assert sorted(r["Id"] for r in records3) == [11, 13]
    assert offset3["cursor"] == "2024-01-04T00:00:00Z"
    assert len(offset3["lb_seen"]) == 1  # still capped; newest (Id 13) kept


@responses.activate
def test_lookback_dedup_explicit_off_reemits_overlap():
    """``cursor_lookback_dedup=off`` restores the pre-dedup behavior:
    overlap rows re-flow every batch (idled on quiescent triggers) and no
    ``lb_seen`` rides the offset."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"}
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "off",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert "lb_seen" not in offset
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    # Pure-overlap batch: the row was re-FETCHED and flowed through the
    # pre-existing suppressed-idle rule (returns [] with lookback on, echoes
    # the offset) — the pre-dedup semantics hold, no lb_seen rides.
    assert list(records2) == []
    assert offset2 == offset
    assert "lb_seen" not in offset2


def test_lookback_dedup_parse_modes():
    """Option grammar: absent/empty → the DEFAULT cap (dedup is on by
    default); off/false/0 → 0; on/true → default cap (the boolean
    spellings match the connector's other flag options); positive int →
    that cap; garbage and non-positive raise curated errors."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        LOOKBACK_DEDUP_DEFAULT_CAP,
        parse_lookback_dedup,
    )

    assert parse_lookback_dedup({}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup(None) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": ""}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": "off"}) == 0
    assert parse_lookback_dedup({"cursor_lookback_dedup": "false"}) == 0
    assert parse_lookback_dedup({"cursor_lookback_dedup": "0"}) == 0
    assert parse_lookback_dedup({"cursor_lookback_dedup": "on"}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": "true"}) == LOOKBACK_DEDUP_DEFAULT_CAP
    assert parse_lookback_dedup({"cursor_lookback_dedup": "250"}) == 250
    with pytest.raises(ValueError, match="cursor_lookback_dedup"):
        parse_lookback_dedup({"cursor_lookback_dedup": "sometimes"})
    with pytest.raises(ValueError, match="cursor_lookback_dedup"):
        parse_lookback_dedup({"cursor_lookback_dedup": "-3"})


@responses.activate
def test_lookback_dedup_quiescent_delta_keeps_auto_history():
    """A quiescent trigger whose only offset movement is ``lb_seen``
    bookkeeping (the dedup delta-delivery branch) must NOT record its
    overlap-only walk duration into the ``auto`` lookback history —
    ``_LOOKBACK_AUTO_WINDOW`` such triggers in a row would flush every
    real walk duration out of the rolling window and shrink the ``auto``
    window below the walks it must cover."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):  # top-level auto drain probe
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "auto",
        "cursor_lookback_dedup": "on",
    }
    # Batch 1 (progressing): records a real walk duration. The auto window
    # was still 0 during it (no history yet), so dedup stayed inert.
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    history = offset["lb_history"]
    assert len(history) == 1
    assert "lb_seen" not in offset
    # Batch 2 (quiescent): the window is active now; the overlap re-reads
    # enter tracking for the first time — a one-time lb_seen delta delivery
    # with NO cursor progress. The history must carry through UNCHANGED.
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert sorted(r["Id"] for r in records2) == [11, 12]
    assert offset2["lb_history"] == history  # not polluted by the re-read
    assert len(offset2["lb_seen"]) == 2
    # Batch 3 (quiescent, tracked): fully suppressed — identity idle.
    records3, offset3 = c.read_table("Parents__Children", offset2, opts)
    assert list(records3) == []
    assert offset3 == offset2


@responses.activate
def test_lookback_dedup_cap_eviction_deterministic_on_cursor_ties():
    """Cap eviction breaks cursor ties by PK key, not fetch order: an
    order-unstable server must not flap the surviving cap-set between
    batches — each flap would be an ``lb_seen`` delta that re-emits the
    newly evicted (already-delivered) row on every quiescent trigger."""
    _mock_nested_metadata()
    children = [
        {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
        {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-02T12:00:00Z"},  # cursor TIE
    ]

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (200, {}, json.dumps({"value": [{"Id": 1, "Children": list(children)}]}))

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "1",
    }
    _, offset = c.read_table("Parents__Children", {"cursor": "2024-01-02T00:00:00Z"}, opts)
    assert len(offset["lb_seen"]) == 1
    # Deterministic winner: the tie broke on the PK key (Id 12), not on
    # the fetch order that happened to put Id 11 first.
    assert "12" in next(iter(offset["lb_seen"]))
    # Quiescent trigger with the server returning the tie in the OPPOSITE
    # order: the same entry must survive (no lb_seen delta), so the batch
    # idles instead of re-emitting the flapped-out row every trigger.
    children.reverse()
    records2, offset2 = c.read_table("Parents__Children", offset, opts)
    assert list(records2) == []
    assert offset2 == offset


@responses.activate
def test_lookback_dedup_tolerates_corrupt_lb_seen_state():
    """``lb_seen`` rides the user-visible checkpoint (same discipline as
    the ``lb_history`` validation in ``_resolve_active_lookback``): a
    hand-edited/corrupt shape must degrade to plain re-emits — never a
    crash and never suppression — and the entry is rebuilt well-formed."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "ModifiedAt",
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "on",
    }
    # "[1, 11]" is Id 11's real composite-PK key, so the corrupt entry IS
    # looked up; non-dict lb_seen exercises the container guard instead.
    for corrupt in (
        "garbage",
        ["not", "a", "dict"],
        {"[1, 11]": []},
        {"[1, 11]": 42},
        {"[1, 11]": ["x"]},
    ):
        records, offset = c.read_table(
            "Parents__Children",
            {"cursor": "2024-01-02T00:00:00Z", "lb_seen": corrupt},
            opts,
        )
        assert sorted(r["Id"] for r in records) == [11, 12], corrupt
        assert offset["cursor"] == "2024-01-03T00:00:00Z"
        assert len(offset["lb_seen"]) == 2  # rebuilt exact and well-formed
        assert all(isinstance(v, list) and len(v) == 2 for v in offset["lb_seen"].values())


@responses.activate
def test_lookback_dedup_cap_eviction_non_timestamp_cursor_no_crash():
    """Cap eviction must not assume a timestamp cursor: the connector
    supports any server-orderable cursor (integer IDs, GUIDs, strings —
    see ``_read_incremental``), and a bare ``parse_iso8601`` on such a
    value would crash the read at the first cap overflow. Non-ISO cursors
    fall back to best-effort classes; ties break on the PK key."""
    _mock_nested_metadata()

    def _parents(req):
        from urllib.parse import unquote

        if "Id gt" in unquote(req.url):
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00Z"},
                                {"Id": 12, "Label": "b", "ModifiedAt": "2024-01-03T00:00:00Z"},
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    opts = {
        "expand_contained": "true",
        "cursor_field": "Label",  # Edm.String — never ISO-parses
        "cursor_lookback_seconds": "3600",
        "cursor_lookback_dedup": "1",
    }
    # First streaming batch (no floor): both rows emitted, the 1-entry cap
    # overflows, and eviction sorts the non-ISO cursors — no crash. "a" and
    # "b" tie in the length-fallback class, so the PK key decides,
    # deterministically.
    records, offset = c.read_table("Parents__Children", {}, opts)
    assert sorted(r["Id"] for r in records) == [11, 12]
    assert offset["cursor"] == "b"
    assert len(offset["lb_seen"]) == 1
    assert "12" in next(iter(offset["lb_seen"]))  # PK tiebreak, fetch-order-free


def test_lb_seen_order_key_never_raises_and_orders():
    """Eviction order-key contract: never raises (non-timestamp cursors
    are supported, and the cursor slot rides the user-visible checkpoint)
    and orders ISO timestamps chronologically, numerics numerically, and
    everything else by string length, with ``None`` at the very bottom."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _lb_seen_order_key as key,
    )

    # Never raises: non-ISO strings, GUIDs, non-str shapes, nan/inf, empty.
    for v in (
        None,
        "a",
        "12345",
        "550e8400-e29b-41d4-a716-446655440000",
        {"weird": 1},
        ["x"],
        "nan",
        "inf",
        "",
    ):
        key(v)
    # Numerics: exact magnitude order (lexical order would invert this).
    assert key("99") < key("100")
    # Timestamps: chronological, and ranked above every non-ISO class.
    assert key("2024-01-01T00:00:00Z") < key("2024-01-02T00:00:00Z")
    assert key("100") < key("2024-01-01T00:00:00Z")
    # Fallback class: longer string wins; None sorts at the very bottom.
    assert key("ab") < key("abc")
    assert key(None) < key("a")


def test_lookback_dedup_filter_streams_suppression():
    """The streaming half of ``cursor_lookback_dedup``: a proven-unchanged
    overlap row is rejected AT EMIT TIME (never buffered) with its entry
    carried; changed/new rows are admitted and deliberately leave NO entry
    on the filter — finalize computes delivered-row entries from the final
    post-trim buffer, so an admitted-then-trimmed row can't poison
    ``lb_seen``. Corrupt checkpoint entries never match."""
    from databricks.labs.community_connector.sources.odata._contained import (
        _lb_row_digest,
        _lb_row_key,
        _LookbackDedupFilter,
    )

    row = {"Id": 1, "Label": "a"}
    prior = {_lb_row_key(row, ["Id"]): ["c1", _lb_row_digest(row)]}
    flt = _LookbackDedupFilter(prior, ["Id"])
    assert flt.admit(dict(row)) is False  # unchanged → suppressed, unbuffered
    assert flt.carried == prior  # entry carried forward
    assert flt.admit({"Id": 1, "Label": "B"}) is True  # changed → delivered
    assert flt.admit({"Id": 2, "Label": "x"}) is True  # new → delivered
    assert set(flt.carried) == set(prior)  # delivered rows left no entry
    for bad in ([], 42, ["x"], "junk"):  # corrupt entries never match
        f2 = _LookbackDedupFilter({_lb_row_key(row, ["Id"]): bad}, ["Id"])
        assert f2.admit(dict(row)) is True


def test_cursor_newer_numeric_rendering_symmetry():
    """Numerically-equal but textually-different cursor renderings must be
    the same instant in BOTH directions ("5000.0" vs "5000" — the lexical
    tie fallback called one strictly newer), and Int64 values beyond float
    precision (tied float sort keys) must order truly via exact Decimal."""
    from databricks.labs.community_connector.sources.odata._helpers import cursor_newer

    assert cursor_newer("5000.0", "5000") is False
    assert cursor_newer("5000", "5000.0") is False
    assert cursor_newer("9007199254740993", "9007199254740992") is True  # > 2^53
    assert cursor_newer("9007199254740992", "9007199254740993") is False
    # Timestamp semantics untouched (ISO text never Decimal-parses).
    assert cursor_newer("2024-01-02T00:00:00.5Z", "2024-01-02T00:00:00Z") is True
    assert cursor_newer("2024-01-02T00:00:00Z", "2024-01-02T00:00:00.5Z") is False


@responses.activate
def test_cursor_lookback_non_utc_watermark_single_escape_on_wire():
    """The lookback floor returns a BARE ISO string; the single percent-escape
    happens at literal generation (``_cursor_filter`` → ``odata_literal``). A
    pre-escaped floor would be re-fed through ``odata_literal``, where the
    ``%2B`` fails the ISO sniff and double-escapes into a QUOTED garbage
    string (``'…%252B10:00'``) — a wrong-type comparison or 400 on every
    lookback-floored batch against a non-UTC source."""
    _mock_nested_metadata()
    captured: list[str] = []

    def _parents(req):
        captured.append(req.url)  # RAW url — escape fidelity is the point here
        if "Id%20gt" in req.url or "Id gt" in req.url:  # top-level drain probe
            return (200, {}, json.dumps({"value": []}))
        return (
            200,
            {},
            json.dumps(
                {
                    "value": [
                        {
                            "Id": 1,
                            "Children": [
                                {"Id": 11, "Label": "a", "ModifiedAt": "2024-01-02T12:00:00+10:00"}
                            ],
                        }
                    ]
                }
            ),
        )

    responses.add_callback(responses.GET, f"{SERVICE_URL}Parents", callback=_parents)
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=lambda r: (200, {}, json.dumps({"value": []})),
    )
    c = _make()
    records, offset = c.read_table(
        "Parents__Children",
        {"cursor": "2024-01-02T00:00:00+10:00"},
        {
            "expand_contained": "true",
            "cursor_field": "ModifiedAt",
            "cursor_lookback_seconds": "3600",
        },
    )
    rows = list(records)
    assert [r["Id"] for r in rows] == [11]
    assert _drop_lb(offset) == {"cursor": "2024-01-02T12:00:00+10:00"}
    filtered = [u for u in captured if "ModifiedAt" in u]
    # The floored filter reached the wire ONCE-escaped and unquoted.
    assert any("2024-01-01T23:00:00%2B10:00" in u for u in filtered), captured
    assert all("%252B" not in u for u in captured)  # never double-escaped
    assert all("'2024" not in u for u in captured)  # never a quoted string literal


@responses.activate
def test_cursor_lookback_explicit_rejected_on_flat_table():
    """An explicit cursor_lookback_seconds is only meaningful for the non-atomic
    contained walks (expand or leaf-cursor/probe); on a flat table it has
    nothing to floor and is rejected. (It IS now allowed on a leaf-cursor
    contained path without expand_contained — see the leaf-cursor lookback
    tests.)"""
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="explicit value.*contained path"):
        c.read_table(
            "Customers",
            {"cursor": "2024-01-01T00:00:00Z"},
            {"cursor_field": "ModifiedAt", "cursor_lookback_seconds": "300"},
        )


@responses.activate
def test_cursor_lookback_non_timestamp_cursor_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="datetime/timestamp cursor|not ISO-8601"):
        c.read_table(
            "Parents__Children",
            {"cursor": 11},  # int cursor value
            {
                "expand_contained": "true",
                "cursor_field": "Id",
                "cursor_lookback_seconds": "300",
            },
        )


@responses.activate
def test_cursor_lookback_invalid_value_raises():
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid cursor_lookback_seconds|must be >= 0"):
        c.read_table(
            "Parents__Children",
            {"cursor": "2024-01-01T00:00:00Z"},
            {
                "expand_contained": "true",
                "cursor_field": "ModifiedAt",
                "cursor_lookback_seconds": "-5",
            },
        )


def test_cursor_lookback_parse_modes():
    """Default is ``auto``; ``off`` disables; an integer is static seconds."""
    c = _make()
    assert c._parse_cursor_lookback({}) == "auto"
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "auto"}) == "auto"
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "off"}) == 0
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "0"}) == 0
    assert c._parse_cursor_lookback({"cursor_lookback_seconds": "300"}) == 300


def test_cursor_lookback_factor_and_ceiling_parse():
    """``auto`` tuning knobs parse with defaults and validate positivity."""
    c = _make()
    assert c._parse_cursor_lookback_factor({}) == 1.5
    assert c._parse_cursor_lookback_factor({"cursor_lookback_factor": "2.5"}) == 2.5
    assert c._parse_cursor_lookback_ceiling({}) == 3600
    assert c._parse_cursor_lookback_ceiling({"cursor_lookback_max_seconds": "600"}) == 600
    with pytest.raises(ValueError, match="cursor_lookback_factor must be > 0"):
        c._parse_cursor_lookback_factor({"cursor_lookback_factor": "0"})
    with pytest.raises(ValueError, match="Invalid cursor_lookback_factor"):
        c._parse_cursor_lookback_factor({"cursor_lookback_factor": "abc"})
    with pytest.raises(ValueError, match="cursor_lookback_max_seconds must be > 0"):
        c._parse_cursor_lookback_ceiling({"cursor_lookback_max_seconds": "0"})


def test_cursor_lookback_auto_resolve_max_of_recent_scaled_clamped():
    """``auto`` sizes the window from the MAX of the last-N walk durations
    × factor, clamped to the ceiling; static mode ignores the history."""
    c = _make()
    c._cursor_lookback = "auto"
    c._cursor_lookback_factor = 1.5
    c._cursor_lookback_max_seconds = 3600
    assert c._resolve_active_lookback({}) == 0  # no history yet
    assert c._resolve_active_lookback({"lb_history": [40, 100, 60]}) == 150  # max(100) × 1.5
    assert c._resolve_active_lookback({"lb_history": [100000]}) == 3600  # clamped
    # sub-second history -> sub-second window (no floor to 0)
    assert c._resolve_active_lookback({"lb_history": [0.3]}) == 0.45  # max(0.3) × 1.5
    assert c._resolve_active_lookback({"lb_history": [0.02, 0.3, 0.1]}) == 0.45  # max(0.3) × 1.5
    # nanosecond-scale history survives (9 dp), not floored to zero
    assert c._resolve_active_lookback({"lb_history": [0.000000002]}) == 0.000000003  # ×1.5
    # custom factor / ceiling
    c._cursor_lookback_factor = 3.0
    c._cursor_lookback_max_seconds = 250
    assert c._resolve_active_lookback({"lb_history": [50, 80]}) == 240  # max(80) × 3.0
    assert c._resolve_active_lookback({"lb_history": [200]}) == 250  # clamped to custom ceiling
    c._cursor_lookback = 50
    assert c._resolve_active_lookback({"lb_history": [100]}) == 50  # static ignores history


def test_cursor_lookback_auto_attach_history():
    """``auto`` appends every completed progressing walk (including sub-second,
    at ms precision) to a rolling last-N history, carries prior while
    in-flight, leaves idle/static offsets untouched."""
    c = _make()
    c._cursor_lookback = "auto"
    # completed progressing walk -> append
    assert c._attach_lookback_state({"cursor": "X"}, {}, False, 12.0) == {
        "cursor": "X",
        "lb_history": [12],
    }
    # append onto prior, capped to the window (5) — oldest dropped
    assert c._attach_lookback_state(
        {"cursor": "X"}, {"lb_history": [1, 2, 3, 4, 5]}, False, 9.0
    ) == {
        "cursor": "X",
        "lb_history": [2, 3, 4, 5, 9],
    }
    # sub-second walk -> NOW recorded (down to nanosecond precision), so a fast
    # source still gets a (fast) overlap window instead of zero
    assert c._attach_lookback_state({"cursor": "X"}, {}, False, 0.2) == {
        "cursor": "X",
        "lb_history": [0.2],
    }
    # nanosecond-scale walk is kept (rounded to 9 dp), not floored to zero
    assert c._attach_lookback_state({"cursor": "X"}, {"lb_history": [7]}, False, 0.000000123) == {
        "cursor": "X",
        "lb_history": [7, 0.000000123],
    }
    # in-flight carries the prior history unchanged and stamps the cycle
    # anchor (wall clock; exact value asserted in the dedicated cycle-span
    # test) so completion can measure the whole capped cycle
    in_flight = c._attach_lookback_state({"pending_fetches": []}, {"lb_history": [9]}, True, 0.0)
    assert in_flight["pending_fetches"] == [] and in_flight["lb_history"] == [9]
    assert isinstance(in_flight["lb_cycle_started"], float)
    # idle (out is the same object as start) -> untouched
    start = {"cursor": "X", "lb_history": [7]}
    assert c._attach_lookback_state(start, start, False, 5.0) is start
    # static mode never writes bookkeeping
    c._cursor_lookback = 50
    assert c._attach_lookback_state({"cursor": "X"}, {}, False, 12.0) == {"cursor": "X"}


@responses.activate
def test_lookback_overlap_larger_than_cap_completes_and_idles():
    """Overlap re-reads (rows at-or-below the committed watermark) must not
    count toward max_records_per_batch: a lookback window holding >= cap
    rows otherwise wedges the stream into an eternal park/complete cycle
    that re-emits the same duplicates on every trigger and never reaches
    the end == start idle."""
    _mock_nested_metadata()
    responses.get(f"{SERVICE_URL}Parents", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Parents(1)/Children",
        callback=_churn_children_cb(
            [
                {"Id": 11, "ModifiedAt": "2024-05-01T00:10:00Z"},
                {"Id": 12, "ModifiedAt": "2024-05-01T00:20:00Z"},
                {"Id": 13, "ModifiedAt": "2024-05-01T00:30:00Z"},
            ]
        ),
    )
    c = _make()
    watermark = "2024-05-01T00:30:00Z"
    opts = {
        "cursor_field": "ModifiedAt",
        "max_records_per_batch": "2",  # smaller than the 3-row overlap
        "cursor_lookback_seconds": "3600",
        "pagination": "nextlink",
    }
    records, offset = c.read_table("Parents__Children", {"cursor": watermark}, opts)
    list(records)
    # The pure-overlap walk completes (no park) and idles at the watermark.
    assert _drop_lb(offset) == {"cursor": watermark}
    for stale in ("parent_idx", "parent_keys", "chain_next_link", "truncated_chain_cursor"):
        assert stale not in offset


@responses.activate
def test_cursor_field_not_on_any_segment_raises():
    """When cursor_field isn't a property anywhere along the contained
    path, the connector should raise with an actionable message."""
    _mock_nested_metadata()
    c = _make()
    with pytest.raises(ValueError, match="not a property"):
        c.read_table("Parents__Children__Notes", None, {"cursor_field": "DoesNotExist"})


@responses.activate
def test_cursor_probe_hydrates_only_dirty_parents():
    """The probe issues one shallow ``$expand($orderby=cursor desc;$top=1)`` per
    leaf-grandparent tuple, reads each leaf-parent's newest leaf, and hydrates
    ONLY those whose newest leaf cursor is > since. Clean leaf-parents are never
    fetched (their hydrate URL is unregistered — a request would error)."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"

    # Level-0 enumeration of Roots (nextlink mode → short page is the end).
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}, {"Id": 2}]})
    # Probe per root returns each Mid's newest leaf cursor. Mid 10 + Mid 21 are
    # dirty (newest > since); 11 + 20 are clean (newest <= since).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids",
        json={
            "value": [
                {"Id": 20, "Leaves": [{"RecordLastModified": "2019-01-01T00:00:00Z"}]},
                {"Id": 21, "Leaves": [{"RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ]
        },
    )
    # Hydrate ONLY the dirty leaf-parents. Clean ones (Mids(11), Mids(20))
    # are deliberately left unregistered.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids(21)/Leaves",
        json={"value": [{"Id": 2101, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
    )

    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
            "expand_contained": "false",
        },
    )
    rows = list(recs)
    # Only the two dirty leaves, each with the full ancestor FK chain.
    assert sorted((r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows) == [
        (1, 10, 1001),
        (2, 21, 2101),
    ]
    # Watermark advanced to the global max leaf cursor.
    assert offset["cursor"] == "2020-07-01T00:00:00Z"
    # The probe orders the inner $expand by the cursor desc and takes top 1 —
    # the max-cursor leaf by construction, with NO inner $filter to mis-order.
    from urllib.parse import unquote

    probe_calls = [unquote(c.request.url) for c in responses.calls if "/Mids?" in c.request.url]
    assert probe_calls
    for u in probe_calls:
        assert (
            "$expand=Leaves($orderby=RecordLastModified desc;$top=1;$select=RecordLastModified)"
            in u
        )
        assert "$filter=" not in u.split("$expand=", 1)[1]  # no inner filter
    # No hydrate request was ever made for a clean leaf-parent.
    hydrate_urls = [c.request.url for c in responses.calls if "/Leaves" in c.request.url]
    assert not any("Mids(11)" in u or "Mids(20)" in u for u in hydrate_urls)


@responses.activate
def test_cursor_probe_first_batch_no_watermark_reads_all():
    """With no committed cursor yet (first batch, since=None) the probe is
    bypassed entirely — it would mark every leaf-parent dirty, so its
    per-grandparent ``$expand`` round-trips prune nothing. The connector falls
    back to the plain N+1 enumerator: identical full set, no probe requests."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2020-05-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(11)/Leaves",
        json={"value": [{"Id": 1101, "RecordLastModified": "2020-05-01T00:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {},  # streaming first batch: no cursor
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert sorted(r["Id"] for r in rows) == [1001, 1101]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # First batch bypasses the probe: no inner ``$expand`` round-trips at all.
    assert not any("%24expand" in call.request.url for call in responses.calls)


@responses.activate
def test_cursor_probe_resumes_across_cap_with_dirty_chain_iterator():
    """The injected dirty-chain iterator composes with the leaf-cursor cap /
    ``parent_idx`` resume: with ``max_records_per_batch=1`` over two dirty
    parents (two distinct-cursor leaves each), driving ``read_table`` to
    completion captures every changed leaf exactly once with its FK chain,
    re-probing skipped parents on each resumed batch."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    all_leaves = {
        "Roots(1)/Mids(10)/Leaves": [
            {"Id": 1001, "RecordLastModified": "2020-03-01T00:00:00Z"},
            {"Id": 1002, "RecordLastModified": "2020-04-01T00:00:00Z"},
        ],
        "Roots(2)/Mids(21)/Leaves": [
            {"Id": 2101, "RecordLastModified": "2020-05-01T00:00:00Z"},
            {"Id": 2102, "RecordLastModified": "2020-06-01T00:00:00Z"},
        ],
    }

    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots",
        callback=lambda r: (200, {}, json.dumps({"value": [{"Id": 1}, {"Id": 2}]})),
    )
    # Probe returns each Mid's newest leaf cursor (max over its leaves) — both
    # exceed `since`, so both stay dirty across every re-probe on resume.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=lambda r: (
            200,
            {},
            json.dumps(
                {"value": [{"Id": 10, "Leaves": [{"RecordLastModified": "2020-04-01T00:00:00Z"}]}]}
            ),
        ),
    )
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(2)/Mids",
        callback=lambda r: (
            200,
            {},
            json.dumps(
                {"value": [{"Id": 21, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]}]}
            ),
        ),
    )

    def _make_leaf_cb(path):
        def _cb(request):
            floor = _probe_filter_floor(request)
            leaves = [
                l for l in all_leaves[path] if floor is None or l["RecordLastModified"] > floor
            ]
            return (200, {}, json.dumps({"value": leaves}))

        return _cb

    for path in all_leaves:
        responses.add_callback(responses.GET, f"{SERVICE_URL}{path}", callback=_make_leaf_cb(path))

    c = _make()
    _skip_probe_preflight(c)
    opts = {
        "cursor_field": "RecordLastModified",
        "cursor_probe": "nested-expand",
        "pagination": "nextlink",
        "max_records_per_batch": "1",
        # Dedup off: this test pins the probe walk's strict exactly-once
        # capped-resume guarantee; default-on dedup re-delivers the overlap
        # once after a capped cycle (documented MERGE-idempotent re-emit).
        "cursor_lookback_dedup": "off",
    }
    offset = {"cursor": since}
    seen = []
    for _ in range(30):
        recs, offset = c.read_table(PROBE_TABLE, offset, opts)
        batch = list(recs)
        if not batch:
            break
        seen.extend(batch)
    # Every changed leaf captured exactly once, with the full FK chain.
    assert sorted((r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in seen) == [
        (1, 10, 1001),
        (1, 10, 1002),
        (2, 21, 2101),
        (2, 21, 2102),
    ]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"


@responses.activate
def test_cursor_probe_invalid_value_raises():
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="Invalid cursor_probe"):
        c.read_table(
            PROBE_TABLE,
            {},
            {"cursor_field": "RecordLastModified", "cursor_probe": "maybe"},
        )


@responses.activate
def test_cursor_probe_conflicts_with_expand_contained():
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="conflicts with expand_contained"):
        c.read_table(
            PROBE_TABLE,
            {},
            {
                "cursor_field": "RecordLastModified",
                "cursor_probe": "nested-expand",
                "expand_contained": "true",
            },
        )


@responses.activate
def test_cursor_probe_on_flat_table_raises():
    _mock_metadata()
    c = _make()
    with pytest.raises(ValueError, match="only on contained-collection paths"):
        c.read_table(
            "Customers", {}, {"cursor_field": "ModifiedAt", "cursor_probe": "nested-expand"}
        )


@responses.activate
def test_cursor_probe_without_cursor_field_raises():
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="requires a cursor_field"):
        c.read_table(PROBE_TABLE, {}, {"cursor_probe": "nested-expand"})


@responses.activate
def test_cursor_probe_with_ancestor_cursor_raises():
    """``MidOnly`` lives on the Mid ancestor, not the leaf — cursor_probe only
    accelerates leaf-owned cursors, so it must reject an ancestor cursor."""
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="requires cursor_field on the leaf"):
        c.read_table(
            PROBE_TABLE,
            {},
            {"cursor_field": "MidOnly", "cursor_probe": "nested-expand"},
        )


@responses.activate
def test_cursor_probe_explicit_raises_when_leaf_parent_is_snapshot():
    """``Roots__Plains__Items``: 3 segments, but the leaf-parent ``Plains`` is a
    batch-snapshot level (no cursor field) — distance from the leaf to the
    nearest snapshot ancestor is 1, so the probe can't save work. The exact
    ``Instances/Projects/WorkPackageDetails`` shape: an explicit opt-in is
    rejected (depth alone does not qualify a path)."""
    _mock_probe_metadata()
    c = _make()
    with pytest.raises(ValueError, match="batch-snapshot level"):
        c.read_table(
            "Roots__Plains__Items",
            {},
            {"cursor_field": "RecordLastModified", "cursor_probe": "nested-expand"},
        )


@responses.activate
def test_cursor_probe_default_inert_when_leaf_parent_is_snapshot():
    """Even default-on, a depth-3 path whose leaf-parent is snapshot
    (``Roots__Plains__Items``) is INAPPLICABLE — distance to the nearest
    snapshot ancestor is 1 — so it uses the plain N+1 leaf walk, issues NO
    ``$expand`` probe, and skips the preflight. Matches the user's
    ``Instances/Projects/WorkPackageDetails`` case."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Plains", json={"value": [{"Id": 5}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Plains(5)/Items",
        json={"value": [{"Id": 50, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    recs, offset = c.read_table(
        "Roots__Plains__Items",
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Plains_Id"], r["Id"]) for r in rows] == [(1, 5, 50)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert not any("%24expand" in call.request.url for call in responses.calls)


@responses.activate
def test_cursor_probe_default_on_engages_without_opt_in():
    """cursor_probe defaults to AUTO: on a probe-eligible deep path whose server
    honours inner-$expand ordering, the cascade uses the nested-$expand probe
    with no option set — the probe runs and only dirty leaf-parents are
    hydrated. (Preflight pre-seeded; covered separately.)"""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe: Mid 10 dirty (newest > since), Mid 11 clean (newest <= since).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        # No cursor_probe key — relies on the default (on).
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = list(recs)
    # Probe engaged: only the dirty Mid 10 hydrated; clean Mid 11 skipped.
    assert [(r["Mids_Id"], r["Id"]) for r in rows] == [(10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert any("$expand=Leaves" in c.request.url for c in responses.calls)
    assert not any("Mids(11)/Leaves" in c.request.url for c in responses.calls)


@responses.activate
def test_cursor_probe_skips_parent_whose_newest_leaf_predates_watermark():
    """The client-side comparison is what fixes the original bug: a leaf-parent
    that HAS leaves but whose newest leaf cursor is <= since must be skipped.
    Mid 10's newest leaf is newer than since (dirty → hydrated); Mid 11 has a
    leaf but its newest predates since (clean → never hydrated)."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-12-31T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Mids_Id"], r["Id"]) for r in rows] == [(10, 1001)]
    # Mid 11 has a leaf, but its newest predates `since` → never hydrated.
    assert not any("Mids(11)/Leaves" in c.request.url for c in responses.calls)


@responses.activate
def test_cursor_probe_default_inert_on_two_segment_path():
    """Even default-on, ``Roots__Mids`` is INAPPLICABLE (the leaf-parent
    ``Roots`` is a snapshot level, distance 1), so it uses the plain N+1 leaf
    walk, issues NO ``$expand`` probe, and skips the preflight."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    recs, offset = c.read_table(
        "Roots__Mids",
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Id"]) for r in rows] == [(1, 10)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # No probe: the standard leaf walk never emits an $expand.
    assert not any("%24expand" in call.request.url for call in responses.calls)


@responses.activate
def test_cursor_probe_preflight_passes_when_inner_orderby_honored():
    """The capability check passes (no raise, cached verified) when the inner
    ``$expand($orderby desc;$top=1)`` returns the same newest leaf as trusted
    direct navigation."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),  # matches direct max
    )
    # Direct-nav desc top2: two distinct cursors → discriminating, true max 2020-09.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    supported, conclusive = c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {"page_size": "1000"}, "RecordLastModified"
    )
    # Conclusive pass: the discriminating sample's inner-$expand matched the
    # trusted direct-nav max, so the caller may persist the verdict.
    assert supported is True
    assert conclusive is True
    assert c.__dict__["_cursor_probe_verified"][(("Roots", "Mids", "Leaves"), None)] == (
        None,
        True,
        False,
    )


@responses.activate
def test_cursor_probe_misorder_verdict_shared_across_instances():
    """Under the ``auto`` cascade a mis-order FAIL rides the process cache —
    the offset only ever carries the pass, so without this a mis-ordering
    server would re-pay the preflight GETs on every recreated reader."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # NOT the true max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c1 = _make()
    assert c1._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert c1._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is False
    n_before = len(responses.calls)
    c2 = _make()
    assert c2._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert len(responses.calls) == n_before  # no preflight re-run


@responses.activate
def test_cursor_probe_conclusive_pass_shared_across_instances():
    """A conclusive pass reaches a fresh instance through the process cache
    even with no offset to carry ``cursor_probe_ok``."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),  # matches direct max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c1 = _make()
    assert c1._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (True, True)
    n_before = len(responses.calls)
    c2 = _make()
    assert c2._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (True, True)
    assert len(responses.calls) == n_before


@responses.activate
def test_cursor_probe_strict_ignores_shared_cache():
    """Strict mode (explicit ``cursor_probe=nested-expand``) neither trusts nor
    writes the shared cache: a cached False doesn't spare the probe (it runs
    and passes on this healthy server), and the strict pass doesn't overwrite
    the recorded verdict — explicit modes keep no recorded state."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    c._store_capability("cursor_probe_ok", False, table_name="Roots__Mids__Leaves")
    n_before = len(responses.calls)
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
    ) == (True, True)
    assert len(responses.calls) > n_before  # the probe really ran
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is False


@responses.activate
def test_cursor_probe_strict_raises_despite_cached_pass():
    """The inverse: a cached True must not let strict mode skip the probe — a
    genuinely mis-ordering server still raises with fresh evidence."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # NOT the true max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    c._store_capability("cursor_probe_ok", True, table_name="Roots__Mids__Leaves")
    with pytest.raises(ValueError, match=r"honour \$orderby/\$top inside \$expand"):
        c._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
        )


@responses.activate
def test_cursor_probe_auto_cascades_when_server_rejects_expand_probe():
    """A server that REJECTS the nested-``$expand`` probe with an HTTP error
    (not a silent mis-order — e.g. Hexagon Smart API 400s on inner-``$expand``
    options) must make ``auto`` **cascade**, not raise: the preflight returns
    ``(False, False)`` and records a definitive ``cursor_probe_ok=False`` instead
    of letting the raw HTTP error escape and fail the read."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_reject_expand_callback
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is False


@responses.activate
def test_cursor_probe_strict_raises_actionable_when_server_rejects_expand_probe():
    """Strict ``nested-expand`` surfaces a ``$expand`` REJECTION as an actionable
    ``ValueError`` (pointing at cursor_probe=batch/false) — not the raw HTTP
    error a bare fetch would let escape."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_reject_expand_callback
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    with pytest.raises(ValueError, match=r"rejected the probe query"):
        c._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
        )


@responses.activate
def test_cursor_probe_auto_read_succeeds_when_server_rejects_expand_probe():
    """End-to-end: ``read_table`` with ``cursor_probe=auto`` on a server that
    400s the nested-``$expand`` probe must **complete** via the N+1 fallback
    (rows emitted, no exception) — the bug was that the raw HTTP error escaped
    and failed the read. ``contained_fetch=single`` keeps the fallback a plain
    walk so no ``$batch`` mock is needed."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET, f"{SERVICE_URL}Roots(1)/Mids", callback=_mids_reject_expand_callback
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"Id": 1001, "RecordLastModified": "2020-09-01T00:00:00Z"},
                {"Id": 1000, "RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
        match_querystring=False,
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "auto",
            "contained_fetch": "single",  # plain N+1 fallback (no $batch)
            "pagination": "nextlink",
        },
    )
    rows = list(recs)  # must not raise
    assert sorted(r["Id"] for r in rows) == [1000, 1001]
    assert offset["cursor"] == "2020-09-01T00:00:00Z"


@responses.activate
def test_cursor_probe_race_newer_leaf_is_skipped_not_failed():
    """A probe-shaped ``$expand`` newest NEWER than the direct-nav reference is
    a concurrent-write race (the two fetches aren't atomic), not mis-ordering
    evidence — a genuinely mis-ordering server returns an OLDER leaf. The
    sample is skipped (never a definitive fail, never a raise) and NOTHING is
    persisted. But unlike a genuinely non-discriminating scan, a verdict-less
    scan that contained a RACE skip DECLINES the probe for this batch
    (``(False, False)`` — cascade to $batch / the plain walk): the race may be
    hiding a mis-ordering server, and engaging unverified could drop rows
    behind the advancing watermark. The next batch re-checks."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-12-01T00:00:00Z"),  # NEWER than reference
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    # Race-tainted inconclusive: declined this batch, nothing persisted.
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is None
    assert c.__dict__["_cursor_probe_verified"][(("Roots", "Mids", "Leaves"), None)] == (
        None,
        False,
        True,
    )


@responses.activate
def test_cursor_probe_race_does_not_abort_scan_to_clean_sample():
    """A racing sample must not abort the scan: with the first leaf-parent
    racing (newer) and a second cleanly discriminating (probe returns the true
    newest), the preflight skips the racer, reaches the clean parent, and
    records a conclusive PASS — rather than being derailed by the race."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}, {"Id": 2}]})
    # Parent 1: probe-shaped $expand newest is NEWER than reference → race/skip.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-12-01T00:00:00Z"),
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    # Parent 2: probe newest MATCHES the reference max → clean conclusive pass.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(2)/Mids",
        callback=_probe_mids_callback("2020-09-01T00:00:00Z"),
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (True, True)
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is True


@responses.activate
def test_cursor_probe_strict_does_not_raise_on_race():
    """Strict mode must not raise the pipeline on a transient concurrent-write
    race: a newer-than-reference sample is skipped, and with no other sample
    the race-tainted scan DECLINES the probe for this batch
    (``(False, False)`` — the read degrades to the $batch/plain cascade for
    one batch, rows identical) rather than raising OR engaging unverified."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-12-01T00:00:00Z"),  # NEWER → race/skip
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
    ) == (False, False)


@responses.activate
def test_cursor_probe_preflight_fetch_error_degrades_instead_of_raising():
    """A preflight that errors out BEFORE reaching a verdict — the trusted
    direct-navigation reference fetch 400s (e.g. a server that rejects
    ``$orderby … desc``/``$select`` on direct navigation) — must not escape a
    ``cursor_probe=auto`` read as a raw HTTP error. Unlike the probe-shape
    rejection (whose sibling fetches just succeeded → definitive), this is
    indistinguishable from a transient: non-strict degrades to the
    ``$batch``/plain cascade for THIS batch and records NOTHING (the next
    batch re-probes); strict raises an actionable message instead of the raw
    failure."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"error": {"message": "The query specified in the URI is not valid."}},
        status=400,
    )
    c = _make()
    assert c._verify_cursor_probe_support(
        ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
    ) == (False, False)
    # Nothing cached or recorded anywhere — neither the instance cache nor the
    # shared capability cache — so the next batch re-probes.
    assert (("Roots", "Mids", "Leaves"), None) not in c.__dict__.get("_cursor_probe_verified", {})
    assert c._cached_capability("cursor_probe_ok", table_name="Roots__Mids__Leaves") is None

    c2 = _make()
    with pytest.raises(ValueError, match="failed before reaching a verdict"):
        c2._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=True
        )


@responses.activate
def test_cursor_probe_preflight_programming_error_propagates(monkeypatch):
    """The never-raise contract covers HTTP/capability failures ONLY: the
    degrade-and-continue handler catches the dedicated fetch-failure type,
    not ``Exception`` — a latent programming error inside the preflight's
    own logic must surface, not silently pin the stream to the slow path."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})

    def _boom(self, *args, **kwargs):
        raise AttributeError("latent bug in preflight logic")

    monkeypatch.setattr(ODataLakeflowConnect, "_cursor_probe_check_sample", _boom)
    c = _make()
    with pytest.raises(AttributeError, match="latent bug"):
        c._verify_cursor_probe_support(
            ["Roots", "Mids", "Leaves"], None, {}, "RecordLastModified", strict=False
        )


@responses.activate
def test_cursor_probe_read_table_raises_when_server_misorders_inner_expand():
    """Fail fast: when the inner ``$expand($orderby desc;$top=1)`` returns a
    non-newest leaf (server ignores inner ordering), read_table raises during
    the preflight instead of silently dropping rows."""
    _mock_probe_metadata()
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # NOT the true max
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    c = _make()
    with pytest.raises(ValueError, match=r"honour \$orderby/\$top inside \$expand"):
        c.read_table(
            PROBE_TABLE,
            {"cursor": "2020-01-01T00:00:00Z"},
            {
                "cursor_field": "RecordLastModified",
                "cursor_probe": "nested-expand",
                "pagination": "nextlink",
            },
        )


@responses.activate
def test_cursor_probe_conclusive_pass_persists_ok_flag_in_offset():
    """Under ``cursor_probe=auto`` a conclusive preflight pass stamps
    ``cursor_probe_ok`` into the resume offset, so a per-batch-recreated reader
    can trust it next batch. (Non-``auto`` modes don't persist it — see
    ``test_nonauto_clears_recorded_preflight_verdicts``.)"""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe + preflight enumeration of Mid 10 (one dirty leaf-parent).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]}]},
    )
    # Preflight direct-nav reference: two distinct cursors → discriminating,
    # true max 2020-06 (matches the inner-$expand newest above → conclusive ok).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"},
                {"Id": 1000, "RecordLastModified": "2020-02-01T00:00:00Z"},
            ]
        },
    )
    c = _make()  # no _skip_probe_preflight: the real preflight runs and passes
    _, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "auto",
            "pagination": "nextlink",
        },
    )
    assert offset.get("cursor_probe_ok") is True


@responses.activate
def test_cursor_probe_offset_flag_skips_preflight_requests():
    """When the resume offset already carries ``cursor_probe_ok`` (set by an
    earlier batch), a freshly-constructed reader skips the preflight entirely —
    no direct-navigation capability requests are issued — and still hydrates
    only the dirty leaf-parent via the probe."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe: Mid 10 dirty, Mid 11 clean.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    # NOTE: Mids(11)/Leaves is left unregistered — the preflight would hit it
    # (direct-nav reference) if it ran; trusting the offset flag must avoid that.
    c = _make()  # cold instance cache; trust comes from the offset flag alone
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since, "cursor_probe_ok": True},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "auto",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Mids_Id"], r["Id"]) for r in rows] == [(10, 1001)]
    assert offset.get("cursor_probe_ok") is True
    # The preflight's direct-navigation reference query (``$orderby cursor
    # desc;$top=2``) was never issued — the only leaf fetch is Mid 10's hydrate
    # (ascending cursor walk, no ``desc``), and the clean Mid 11 is untouched.
    from urllib.parse import unquote

    leaf_calls = [
        unquote(call.request.url) for call in responses.calls if "/Leaves" in call.request.url
    ]
    assert leaf_calls == [
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves?$top=1000"
        "&$filter=RecordLastModified gt 2020-01-01T00:00:00Z&$orderby=RecordLastModified asc,Id asc"
    ]
    assert not any("desc" in u for u in leaf_calls)
    assert not any("Mids(11)" in u for u in leaf_calls)


@responses.activate
def test_cursor_probe_lookback_floors_filter_and_reincludes_overlap_parent():
    """cursor_probe utilises cursor_lookback: with a window set, the probe's
    dirty-detection AND the hydrate filter floor to (committed - window), so a
    leaf-parent whose newest leaf fell in the overlap (<= since, > read_since) is
    re-flagged dirty and re-hydrated — catching a mid-walk arrival. The committed
    watermark stays the TRUE max (never floored)."""
    _mock_probe_metadata()
    since = "2020-06-10T00:00:00Z"  # read_since = since - 1 day = 2020-06-09
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe newest-leaf per Mid: 10 new (> since), 11 in overlap (> read_since,
    # <= since), 12 below the window (clean).
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-11T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2020-06-09T12:00:00Z"}]},
                {"Id": 12, "Leaves": [{"RecordLastModified": "2020-06-08T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-11T00:00:00Z"}]},
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(11)/Leaves",
        json={"value": [{"Id": 1101, "RecordLastModified": "2020-06-09T12:00:00Z"}]},
    )
    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
            "cursor_lookback_seconds": "86400",  # 1 day
        },
    )
    rows = list(recs)
    # Overlap parent (Mid 11) re-included; below-window parent (Mid 12) skipped.
    assert sorted((r["Mids_Id"], r["Id"]) for r in rows) == [(10, 1001), (11, 1101)]
    assert not any("Mids(12)/Leaves" in call.request.url for call in responses.calls)
    # Committed watermark = TRUE max, not floored.
    assert offset["cursor"] == "2020-06-11T00:00:00Z"
    # The hydrate filter floored to read_since (2020-06-09), not `since`.
    from urllib.parse import unquote

    hydrate = [unquote(c.request.url) for c in responses.calls if "/Mids(1" in c.request.url]
    assert hydrate and all("2020-06-09" in u for u in hydrate)
    assert not any("gt 2020-06-10" in u for u in hydrate)


@responses.activate
def test_cursor_probe_batch_hydrates_via_batch_endpoint():
    """``cursor_probe=batch`` skips the nested-$expand probe and hydrates the
    per-leaf-parent ``cursor gt since`` reads through OData ``$batch``: no probe
    ``$expand``, no per-leaf-parent GET, and ``batch_ok`` is persisted."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}, {"Id": 11}]})
    responder = _batch_responder(
        [
            # dirty leaf-parent → one changed leaf
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            # clean leaf-parent → server-filtered empty page
            ("Mids(11)/Leaves", {"value": []}),
            # $batch capability preflight
            ("Roots", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "batch",
            "pagination": "nextlink",
            "expand_contained": "false",
        },
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows] == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    assert offset.get("batch_ok") is True
    # No nested-$expand probe anywhere, and the leaf hydrate went through
    # $batch — never a per-leaf-parent GET to /Leaves.
    assert not any("$expand" in call.request.url for call in responses.calls)
    assert not any(
        call.request.method == "GET" and "/Leaves" in call.request.url for call in responses.calls
    )
    # Both leaf-parents were hydrated via the batch (filter pushed server-side).
    assert any("Mids(10)/Leaves" in u for u in responder.seen)
    assert any("Mids(11)/Leaves" in u for u in responder.seen)
    # No $top on the hydrate sub-requests (server-driven paging).
    assert not any("Mids(10)/Leaves" in u and "$top=" in u for u in responder.seen)


@responses.activate
def test_cursor_probe_batch_size_suffix_chunks_requests():
    """``cursor_probe=batch:2`` hydrates via ``$batch`` like ``batch`` but caps
    each request at 2 leaf-parent ops: 3 leaf-parents → rounds of 2 + 1."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={"value": [{"Id": 10}, {"Id": 11}, {"Id": 12}]},
    )
    responder = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            ("Mids(11)/Leaves", {"value": []}),
            (
                "Mids(12)/Leaves",
                {"value": [{"Id": 1201, "RecordLastModified": "2020-06-02T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # capability preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch:2", "pagination": "nextlink"},
    )
    assert sorted(r["Id"] for r in recs) == [1001, 1201]
    # Hydrate $batch POSTs (those carrying /Leaves) are capped at 2 ops:
    # 3 leaf-parents, chunk size 2 → rounds of 2 then 1.
    op_counts = []
    for call in responses.calls:
        if call.request.method != "POST":
            continue
        reqs = json.loads(call.request.body)["requests"]
        if any("/Leaves" in r["url"] for r in reqs):
            op_counts.append(len(reqs))
    assert sorted(op_counts) == [1, 2]


@responses.activate
def test_cursor_probe_batch_size_invalid_suffix_raises():
    """A non-positive / non-integer ``:N`` suffix, or a suffix on a non-batch
    mode, is rejected before any network call."""
    _mock_probe_metadata()
    c = _make()
    for bad in ("batch:0", "batch:-1", "batch:abc", "auto:2", "nested-expand:5"):
        with pytest.raises(ValueError, match="Invalid cursor_probe"):
            c.read_table(
                PROBE_TABLE,
                {"cursor": "2020-01-01T00:00:00Z"},
                {"cursor_field": "RecordLastModified", "cursor_probe": bad},
            )


@responses.activate
def test_cursor_probe_batch_follows_nextlink_continuation():
    """A batched leaf sub-response carrying ``@odata.nextLink`` is re-batched
    until the collection drains — all pages are collected."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    responder = _batch_responder(
        [
            # continuation page (matched first — more specific)
            (
                "$skiptoken=p2",
                {"value": [{"Id": 1002, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ),
            # first page emits a service-relative nextLink
            (
                "Mids(10)/Leaves",
                {
                    "value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}],
                    "@odata.nextLink": "Roots(1)/Mids(10)/Leaves?$skiptoken=p2",
                },
            ),
            ("Roots", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    rows = sorted(r["Id"] for r in recs)
    assert rows == [1001, 1002]  # both pages collected across batch rounds
    assert offset["cursor"] == "2020-07-01T00:00:00Z"
    assert any("$skiptoken=p2" in u for u in responder.seen)


@responses.activate
def test_cursor_probe_batch_falls_back_to_plain_walk_when_unsupported():
    """``cursor_probe=batch`` against a server that rejects ``$batch`` (405)
    degrades to the plain N+1 GET walk — never raises, rows still correct."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(f"{SERVICE_URL}Roots(1)/Mids", json={"value": [{"Id": 10}]})
    # $batch unsupported.
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    # Plain N+1 leaf GET still serves the hydrate.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
    )
    c = _make()
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "cursor_probe": "batch", "pagination": "nextlink"},
    )
    rows = [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs]
    assert rows == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-06-01T00:00:00Z"
    # Probed and found unsupported → persisted False (not True) so later
    # microbatches skip the probe and go straight to the plain walk.
    assert offset.get("batch_ok") is False
    # A real GET hydrate happened (plain walk fallback).
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )


@responses.activate
def test_cursor_probe_auto_cascades_to_batch_when_server_misorders_inner_expand():
    """DEFAULT (unset → auto): when the probe preflight finds the server
    mis-orders inner ``$expand``, ``auto`` does NOT raise — it cascades to the
    ``$batch`` hydrate (drop-safe) and persists ``batch_ok``."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    # Probe enumeration: inner-$expand newest is WRONG (server mis-orders);
    # plain enumeration (no $expand) lists Mid 10 for the hydrate.
    responses.add_callback(
        responses.GET,
        f"{SERVICE_URL}Roots(1)/Mids",
        callback=_probe_mids_callback("2020-02-01T00:00:00Z"),  # not the true max
    )
    # Preflight direct-nav reference: 2 distinct cursors, true max 2020-09 →
    # discriminating, and != the inner-$expand newest → mis-order verdict.
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={
            "value": [
                {"RecordLastModified": "2020-09-01T00:00:00Z"},
                {"RecordLastModified": "2020-05-01T00:00:00Z"},
            ]
        },
    )
    responder = _batch_responder(
        [
            (
                "Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-09-01T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    # No cursor_probe key → default auto. Must NOT raise.
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {"cursor_field": "RecordLastModified", "pagination": "nextlink"},
    )
    rows = [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in recs]
    assert rows == [(1, 10, 1001)]
    assert offset["cursor"] == "2020-09-01T00:00:00Z"
    assert offset.get("batch_ok") is True
    # Cascaded: the leaf hydrate went through $batch, not the probe.
    assert any("Mids(10)/Leaves" in u for u in responder.seen)


# ---------------------------------------------------------------------------
# cursor_probe=nested-expand → $batch hydrate of the probe's dirty parents
# ---------------------------------------------------------------------------


@responses.activate
def test_cursor_probe_nested_expand_hydrates_dirty_via_batch():
    """nested-expand identifies dirty leaf-parents via the nested-``$expand``
    probe, then — when the server supports ``$batch`` — hydrates ONLY those via
    ``$batch`` (no per-parent GET). Both verdicts (cursor_probe_ok, batch_ok)
    persist."""
    from urllib.parse import unquote

    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}, {"Id": 2}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(2)/Mids",
        json={
            "value": [
                {"Id": 20, "Leaves": [{"RecordLastModified": "2019-01-01T00:00:00Z"}]},
                {"Id": 21, "Leaves": [{"RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ]
        },
    )
    responder = _batch_responder(
        [
            (
                "Roots(1)/Mids(10)/Leaves",
                {"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
            ),
            (
                "Roots(2)/Mids(21)/Leaves",
                {"value": [{"Id": 2101, "RecordLastModified": "2020-07-01T00:00:00Z"}]},
            ),
            ("Roots", {"value": [{"Id": 1}]}),  # $batch preflight
        ]
    )
    responses.add_callback(responses.POST, f"{SERVICE_URL}$batch", callback=responder)

    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert sorted((r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows) == [
        (1, 10, 1001),
        (2, 21, 2101),
    ]
    assert offset["cursor"] == "2020-07-01T00:00:00Z"
    # Probe ran (identify via nested-$expand) ...
    assert any(
        "$expand=Leaves" in unquote(call.request.url)
        for call in responses.calls
        if "/Mids?" in call.request.url
    )
    # ... and the dirty hydrate went through $batch — never a per-parent GET.
    assert not any(
        call.request.method == "GET" and "/Leaves" in call.request.url for call in responses.calls
    )
    assert any("Mids(10)/Leaves" in u for u in responder.seen)
    assert any("Mids(21)/Leaves" in u for u in responder.seen)
    # Clean leaf-parents are never hydrated.
    assert not any("Mids(11)/Leaves" in u or "Mids(20)/Leaves" in u for u in responder.seen)
    # cursor_probe=nested-expand is non-auto → its probe verdict is scrubbed from
    # the offset (so a later switch to auto re-probes). batch_ok is owned by
    # contained_fetch (default auto here) and persists.
    assert "cursor_probe_ok" not in offset
    assert offset.get("batch_ok") is True


@responses.activate
def test_cursor_probe_nested_expand_falls_back_to_n1_when_batch_unsupported():
    """When the ``$batch`` preflight fails (fail-closed), nested-expand still
    prunes to the dirty parents but hydrates them via the plain N+1 walk —
    one per-parent GET, clean parents untouched."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.post(f"{SERVICE_URL}$batch", json={"detail": "Method Not Allowed"}, status=405)
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )

    c = _make()
    _skip_probe_preflight(c)
    recs, offset = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows] == [(1, 10, 1001)]
    # Dirty parent hydrated via plain GET (N+1 fallback); clean parent untouched.
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )
    assert not any("Mids(11)/Leaves" in call.request.url for call in responses.calls)
    assert offset.get("batch_ok") is not True  # preflight failed → batch not used


@responses.activate
def test_cursor_probe_nested_expand_contained_fetch_single_forces_n1():
    """An explicit ``contained_fetch=single`` overrides the probe's ``$batch``
    hydrate: the probe still prunes to dirty parents, but they go down the plain
    N+1 walk — no ``$batch`` POST at all (preflight skipped)."""
    _mock_probe_metadata()
    since = "2020-01-01T00:00:00Z"
    responses.get(f"{SERVICE_URL}Roots", json={"value": [{"Id": 1}]})
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids",
        json={
            "value": [
                {"Id": 10, "Leaves": [{"RecordLastModified": "2020-06-01T00:00:00Z"}]},
                {"Id": 11, "Leaves": [{"RecordLastModified": "2019-06-01T00:00:00Z"}]},
            ]
        },
    )
    responses.get(
        f"{SERVICE_URL}Roots(1)/Mids(10)/Leaves",
        json={"value": [{"Id": 1001, "RecordLastModified": "2020-06-01T00:00:00Z"}]},
        match_querystring=False,
    )

    c = _make()
    _skip_probe_preflight(c)
    recs, _ = c.read_table(
        PROBE_TABLE,
        {"cursor": since},
        {
            "cursor_field": "RecordLastModified",
            "cursor_probe": "nested-expand",
            "contained_fetch": "single",
            "pagination": "nextlink",
        },
    )
    rows = list(recs)
    assert [(r["Roots_Id"], r["Mids_Id"], r["Id"]) for r in rows] == [(1, 10, 1001)]
    # No $batch was even attempted (the explicit single skips the preflight).
    assert not any(call.request.method == "POST" for call in responses.calls)
    # Dirty parent hydrated via plain GET; clean parent untouched.
    assert any(
        call.request.method == "GET" and "Mids(10)/Leaves" in call.request.url
        for call in responses.calls
    )
    assert not any("Mids(11)/Leaves" in call.request.url for call in responses.calls)


def test_cursor_completion_floored_at_since():
    """A completing batch whose max cursor sits BELOW the committed
    watermark (lookback overlap after the watermark-defining row was
    deleted) must not regress the committed cursor."""
    c = _make()
    assert c._cursor_max_end_offset(["2020-05-30T00:00:00Z"], "2020-06-01T00:00:00Z") == {
        "cursor": "2020-06-01T00:00:00Z"
    }
    assert c._cursor_max_end_offset(["2020-06-02T00:00:00Z"], "2020-06-01T00:00:00Z") == {
        "cursor": "2020-06-02T00:00:00Z"
    }
    # Same floor on the expand walk's completion fold.
    assert c._build_expand_end_offset(
        [{"M": "2020-05-30T00:00:00Z"}], "M", {"cursor": "2020-06-01T00:00:00Z"}, []
    ) == {"cursor": "2020-06-01T00:00:00Z"}


@responses.activate
def test_latest_offset_honours_pagination_option():
    """Round-28: ``latest_offset`` parses/applies ``pagination=`` like
    ``get_partitions``/``read_partition`` do — the fence probe must not walk
    under a stale or default mode (and an invalid value must raise the same
    curated error)."""
    responses.get(f"{SERVICE_URL}$metadata", body=GUID_CURSOR_METADATA_XML, status=200)

    def _accounts(request):
        from urllib.parse import unquote as _unq

        # Round-45 desc self-check: nothing above the probed max.
        if "gt" in _unq(request.url):
            return (200, {}, '{"value": []}')
        return (200, {}, '{"value": [{"Name": "2020-06-01T00:00:00Z"}]}')

    responses.add_callback(responses.GET, f"{SERVICE_URL}Accounts", callback=_accounts)
    c = _make()
    with pytest.raises(ValueError, match="pagination"):
        c.latest_offset("Accounts__Contacts", {"cursor_field": "Name", "pagination": "bogus"})
    off = c.latest_offset("Accounts__Contacts", {"cursor_field": "Name", "pagination": "nextlink"})
    assert off == {"cursor": "2020-06-01T00:00:00Z"}
    assert c._pagination == "nextlink"


@responses.activate
def test_same_origin_nextlink_still_followed():
    """The origin guard must not break legitimate same-host pagination
    (absolute nextLink on the service's own host)."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        json={
            "value": [{"Id": 1, "Name": "A"}],
            "@odata.nextLink": f"{SERVICE_URL}Customers?$skiptoken=p2",
        },
        match_querystring=False,
    )
    responses.get(
        f"{SERVICE_URL}Customers?$skiptoken=p2",
        json={"value": [{"Id": 2, "Name": "B"}]},
    )
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in list(rows)] == [1, 2]


@responses.activate
def test_same_host_redirect_followed():
    """A same-origin 3xx (server-side URL normalization) is still followed
    manually so legitimate redirects keep working."""
    _mock_metadata()
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Customers",
        status=301,
        headers={"Location": f"{SERVICE_URL}Customers/"},
    )
    responses.get(f"{SERVICE_URL}Customers/", json={"value": [{"Id": 1, "Name": "A"}]})
    c = _make({"token": "t"})
    rows, _ = c.read_table("Customers", None, {"pagination": "nextlink"})
    assert [r["Id"] for r in list(rows)] == [1]


def test_lookback_history_records_cycle_span_not_final_batch(monkeypatch):
    """A capped walk spanning several batches must record the WHOLE cycle's
    wall-clock span (first capped batch to completion, trigger intervals
    included) into lb_history — the churn-exposure window of the cycle is the
    full span, not the final batch's drain time. In-flight batches stamp
    lb_cycle_started once; completion consumes and clears it."""
    c = _make()
    now = {"t": 1000.0}
    monkeypatch.setattr(time, "time", lambda: now["t"])
    # Batch 1 (capped, walk took 5s): anchors the cycle at 995.0.
    out1 = c._attach_lookback_state({"parent_idx": 1}, {}, True, 5.0)
    assert out1["lb_cycle_started"] == 995.0
    # Batch 2 (still capped, later trigger): anchor carried unchanged.
    now["t"] = 1300.0
    out2 = c._attach_lookback_state({"parent_idx": 2}, out1, True, 3.0)
    assert out2["lb_cycle_started"] == 995.0
    # Completion at t=1600 with a 2s final drain: records the 605s span
    # (1600 - 995), not 2s, and drops the anchor from the offset.
    now["t"] = 1600.0
    out3 = c._attach_lookback_state({"cursor": "x"}, out2, False, 2.0)
    assert out3["lb_history"] == [605.0]
    assert "lb_cycle_started" not in out3
    # Single-batch walk (no anchor): records its own elapsed as before.
    out4 = c._attach_lookback_state({"cursor": "y"}, {"cursor": "x"}, False, 2.5)
    assert out4["lb_history"] == [2.5]


def test_cursor_nulls_empty_string_defaults():
    """cursor_nulls="" means unset → coalesce default, consistent with
    delta_tracking/pagination/expand_contained empty-string handling."""
    c = _make({"token": "t"})
    assert c._parse_cursor_nulls({"cursor_nulls": ""}) == ("coalesce", 2000)


# ---------------------------------------------------------------------------
# Round 38 — same-instant park identity, partitioned cursor_nulls parity,
# service_url query rejection, wire hygiene, metadata cache eviction
# ---------------------------------------------------------------------------


def test_cursor_same_instant_helper():
    from databricks.labs.community_connector.sources.odata._helpers import cursor_same_instant

    # Rendering variants of one instant.
    assert cursor_same_instant("2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000Z")
    assert cursor_same_instant("2024-01-01T00:00:00Z", "2024-01-01T00:00:00+00:00")
    assert cursor_same_instant("2024-01-01T00:00:00.5Z", "2024-01-01T00:00:00.500Z")
    # Genuinely different instants — including sub-microsecond (7-digit).
    assert not cursor_same_instant("2024-01-01T00:00:00Z", "2024-01-01T00:00:01Z")
    assert not cursor_same_instant("2024-01-01T00:00:00.0000001Z", "2024-01-01T00:00:00.0000002Z")
    # Non-ISO values: same instant only when raw-equal.
    assert cursor_same_instant(5, 5) and not cursor_same_instant(5, 6)
    assert not cursor_same_instant("abc", "abd")
    assert cursor_same_instant(None, None)


def test_cursor_newer_incomparable_pair_never_raises():
    """cursor_newer's TypeError fallback used to re-raise for str-vs-int
    (an IEEE754Compatible server alternating 5000 and "5000" across
    requests) and propagate through cursor_max/max_or — killing the batch
    from a watermark fold. Incomparable pairs now degrade to a consistent
    False, matching _chain_strictly_before's documented posture."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_max,
        cursor_newer,
        max_or,
    )

    assert cursor_newer(5000, "5000") is False
    assert cursor_newer("5000", 5000) is False
    assert cursor_max([5000, "5000", 4999]) is not None  # no raise
    assert max_or(5000, "5000") is not None  # no raise


def test_cursor_same_instant_numeric_strings():
    """IEEE754Compatible numeric-string renderings of one value are the same
    instant; sub-float-precision Int64 pairs stay distinct (exact Decimal
    compare, no float collapse)."""
    from databricks.labs.community_connector.sources.odata._helpers import cursor_same_instant

    assert cursor_same_instant(5000, "5000")
    assert cursor_same_instant("5000", 5000)
    assert cursor_same_instant("5000.0", 5000)
    assert cursor_same_instant("9007199254740993", 9007199254740993)
    assert not cursor_same_instant("9007199254740992", 9007199254740993)
    assert not cursor_same_instant("5001", 5000)
    assert not cursor_same_instant("abc", 5000)


def test_cursor_ordering_numeric_string_bridge():
    """Mixed numeric renderings order TRULY (exact Decimal), not as a flat
    tie: a server that permanently switches an Int64 cursor to string
    rendering against an int checkpoint made cursor_le(new_row, since) read
    True for genuinely newer rows — the client re-filter dropped every
    returned row and the stream silently stalled with data pending."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_le,
        cursor_max,
        cursor_newer,
    )

    assert cursor_newer("6000", 5000) is True
    assert cursor_newer(5000, "6000") is False
    assert cursor_le("6000", 5000) is False  # the stall's exact predicate
    assert cursor_le("4000", 5000) is True
    assert cursor_max([5000, "6000", 4000]) == "6000"
    # Non-numeric incomparable pairs keep the consistent-False posture.
    assert cursor_newer("abc", 5000) is False and cursor_newer(5000, "abc") is False


def test_trim_boundary_groups_mixed_renderings():
    """The boundary trim groups the cohort by SAME-INSTANT, not raw
    equality: a same-value cohort spanning a page-rendering seam (ints on
    page 1, strings on page 2) used to trim only the differently-rendered
    tail while the watermark landed EQUAL to the trimmed rows' value — gt
    never re-fetched them (permanent loss)."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        trim_to_distinct_cursor_boundary,
    )

    records = [{"Id": 1, "Seq": 5000}, {"Id": 2, "Seq": 6000}, {"Id": 3, "Seq": "6000"}]
    trimmed = trim_to_distinct_cursor_boundary(records, "Seq")
    assert [r["Id"] for r in trimmed] == [1]  # whole 6000-cohort trimmed as one
    # Timestamp rendering variants group too.
    records = [
        {"Id": 1, "T": "2024-01-01T00:00:00Z"},
        {"Id": 2, "T": "2024-02-01T00:00:00Z"},
        {"Id": 3, "T": "2024-02-01T00:00:00.000Z"},
    ]
    assert [r["Id"] for r in trim_to_distinct_cursor_boundary(records, "T")] == [1]
    # Null-cohort behavior preserved: all-null trims to empty.
    assert trim_to_distinct_cursor_boundary([{"T": None}, {"T": None}], "T") == []


def test_cursor_lookback_factor_rejects_nan():
    """NaN passes every ``<=`` comparison (all False) and used to sail
    through the validator, then kill the read with an uncurated
    ``cannot convert float NaN to integer`` deep in the lookback resolve."""
    c = _make()
    with pytest.raises(ValueError, match="cursor_lookback_factor"):
        c._parse_cursor_lookback_factor({"cursor_lookback_factor": "nan"})


# ---------------------------------------------------------------------------
# Round 44 — numeric-string cursor ordering (Decimal sort key), basic-format
# ISO guard, streaming-snapshot cap warning, reset-offset hygiene
# ---------------------------------------------------------------------------


def test_cursor_numeric_string_pairs_order_numerically():
    """Two numeric STRINGS used to compare ordinally ("1000" < "999") — the
    round-38 Decimal bridge lived in the except-TypeError path, which
    str/str never reaches. The sort key now Decimal-keys numeric strings,
    aligning cursor_newer with cursor_same_instant's existing numeric
    treatment of the same pair class."""
    from databricks.labs.community_connector.sources.odata._helpers import (
        cursor_le,
        cursor_max,
        cursor_newer,
    )

    assert cursor_newer("1000", "999") is True
    assert cursor_newer("999", "1000") is False
    assert cursor_le("1000", "999") is False
    assert cursor_newer("100000", "99999") is True
    # Watermark folds must be order-independent at the digit boundary.
    assert cursor_max(["999", "1000"]) == "1000"
    assert cursor_max(["1000", "999"]) == "1000"
    # Cross-rendering (int vs numeric string) still orders in the primary path.
    assert cursor_newer("5000", 4000) is True
    assert cursor_newer(4000, "5000") is False


def test_lb_history_garbage_sanitized():
    """lb_history rides the user-visible checkpoint: a hand-edited entry
    ("abc", NaN, a negative) used to crash the window resolve uncurated —
    or float the read filter ABOVE the watermark (negative window = silent
    exclusion band). Non-finite/non-positive/non-numeric entries are now
    filtered before sizing."""
    c = _make()
    c._cursor_lookback = "auto"
    assert c._resolve_active_lookback({"lb_history": ["abc", -5.0, float("nan"), True]}) == 0
    assert c._resolve_active_lookback({"lb_history": ["abc", 2.0]}) == 3.0  # 2.0 × 1.5


@responses.activate
def test_null_ancestor_fk_fails_loudly_expand_path():
    """A parent entity that omits/nulls its own primary key (non-conformant —
    OData keys are never null) would stamp a null onto the NON-nullable FK
    column, sending a null MERGE key into apply_changes. Mirrors the leaf-PK
    never_pad protection onto the ancestor-FK side: fail loudly, don't emit."""
    import re as _re

    responses.get(f"{SERVICE_URL}$metadata", body=_FK_NULL_MD, status=200)
    responses.add(
        responses.GET,
        f"{SERVICE_URL}Parents",
        json={"value": [{"Name": "P1", "Children": [{"Id": 11, "Label": "a"}]}]},
        match_querystring=False,
    )
    responses.get(f"{SERVICE_URL}Parents", json={"value": []})
    # Absorb any (malformed) continuation drain so the ONLY failure is the guard.
    responses.add_callback(
        responses.GET,
        _re.compile(rf"{_re.escape(SERVICE_URL)}Parents\(.*\)/Children.*"),
        callback=lambda req: (200, {}, '{"value": []}'),
    )
    c = _make()
    # Non-nullable FK column, by construction.
    schema = c.get_table_schema("Parents__Children", {"expand_contained": "true"})
    fk = [f for f in schema.fields if f.name == "Parents_Id"][0]
    assert fk.nullable is False
    with pytest.raises(ValueError, match="missing its key"):
        list(c.read_table("Parents__Children", None, {"expand_contained": "true"})[0])
