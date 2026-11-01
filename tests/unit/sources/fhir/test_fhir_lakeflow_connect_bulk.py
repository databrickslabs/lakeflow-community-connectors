"""Integration tests for the bulk export path in FhirLakeflowConnect."""

from unittest.mock import MagicMock, patch

from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect


def _make_options(**overrides):
    opts = {
        "base_url": "https://fhir.example.com/r4",
        "auth_type": "none",
    }
    opts.update(overrides)
    return opts


def _make_connector(**overrides):
    return FhirLakeflowConnect(_make_options(**overrides))


# ── Bulk path routing ─────────────────────────────────────────────────────────


def test_bulk_path_not_used_when_disabled():
    c = _make_connector()
    assert not c._bulk_enabled


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_path_used_when_enabled(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = (
        "2024-06-01T00:00:00Z",
        {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "p1",
                    "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
                },
            ]
        },
    )
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    assert c._bulk_enabled

    records, offset = c.read_table("Patient", None, {"profile": "uk_core"})
    record_list = list(records)
    assert len(record_list) == 1
    assert record_list[0]["id"] == "p1"
    mock_instance.export.assert_called_once()


# ── Cursor / offset behavior ─────────────────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_uses_transaction_time_as_cursor(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = (
        "2024-06-15T12:00:00Z",
        {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "p1",
                    "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
                },
            ]
        },
    )
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    _, offset = c.read_table("Patient", None, {})
    assert offset["cursor"] == "2024-06-15T12:00:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_short_circuits_when_cursor_ge_init_ts(MockBulkClient):
    mock_instance = MagicMock()
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    # Set a cursor far in the future (past init_ts)
    start_offset = {"cursor": "2099-01-01T00:00:00+00:00"}
    records, offset = c.read_table("Patient", start_offset, {})
    assert list(records) == []
    assert offset == start_offset
    mock_instance.export.assert_not_called()


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_returns_empty_when_no_records(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = ("2024-06-01T00:00:00Z", {})
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    records, offset = c.read_table("Patient", None, {})
    assert list(records) == []
    # An empty export still yields a valid new snapshot instant; keeping it is
    # what lets a low-churn table progress instead of re-exporting forever.
    assert offset["cursor"] == "2024-06-01T00:00:00Z"


# ── Incremental mode passthrough ──────────────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_passes_incremental_mode(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = (
        "2024-06-01T00:00:00Z",
        {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "p1",
                    "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
                },
            ]
        },
    )
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="typefilter")
    c.read_table("Patient", {"cursor": "2024-01-01T00:00:00Z"}, {})

    call_kwargs = mock_instance.export.call_args
    assert (
        call_kwargs.kwargs.get("incremental_mode") == "typefilter"
        or (call_kwargs.args and len(call_kwargs.args) > 2 and call_kwargs.args[2] == "typefilter")
        or call_kwargs[1].get("incremental_mode") == "typefilter"
    )


# ── Group scope initialization ────────────────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_group_scope_passes_group_id(MockBulkClient):
    MockBulkClient.return_value = MagicMock()
    _make_connector(
        bulk_export_enabled="true",
        bulk_export_scope="group",
        bulk_export_group_id="g-abc123",
    )
    call_kwargs = MockBulkClient.call_args
    assert call_kwargs.kwargs.get("scope") == "group" or call_kwargs[1].get("scope") == "group"
    assert (
        call_kwargs.kwargs.get("group_id") == "g-abc123"
        or call_kwargs[1].get("group_id") == "g-abc123"
    )


# ── Regression tests: full-mode offset advance (F1) ───────────────────────────


def _patient(pid):
    return {
        "resourceType": "Patient",
        "id": pid,
        "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
    }


def _mock_bulk(MockBulkClient, tx_time, resources):
    inst = MagicMock()
    inst.export.return_value = (tx_time, resources)
    MockBulkClient.return_value = inst
    return inst


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_full_mode_advances_offset_to_init_ts(MockBulkClient):
    """Full mode must advance the offset, or the rows are never written."""
    _mock_bulk(MockBulkClient, None, {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="full")

    records, offset = c.read_table("Patient", None, {})
    assert len(list(records)) == 1
    assert offset == {"run_ts": c._init_ts}


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_full_mode_short_circuits_second_call_in_trigger(MockBulkClient):
    """Second call with this trigger's offset returns empty so the trigger ends."""
    inst = _mock_bulk(MockBulkClient, None, {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="full")

    _, offset = c.read_table("Patient", None, {})
    records2, offset2 = c.read_table("Patient", offset, {})
    assert list(records2) == []
    assert offset2 == offset
    assert inst.export.call_count == 1  # no second export in the same trigger


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_full_mode_re_exports_on_next_trigger(MockBulkClient):
    """A new process (new _init_ts) re-exports even with a stored offset."""
    inst = _mock_bulk(MockBulkClient, None, {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="full")

    records, offset = c.read_table("Patient", {"run_ts": "2020-01-01T00:00:00+00:00"}, {})
    assert len(list(records)) == 1
    assert offset == {"run_ts": c._init_ts}
    assert inst.export.call_count == 1


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_full_mode_never_passes_since(MockBulkClient):
    inst = _mock_bulk(MockBulkClient, None, {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="full")

    c.read_table("Patient", {"run_ts": "2020-01-01T00:00:00+00:00"}, {})
    assert inst.export.call_args.kwargs["since"] is None


# ── Regression tests: admission control (F3) ──────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_emits_whole_export_no_truncation(MockBulkClient):
    """The bulk cursor covers the whole manifest, so the batch must not be capped.

    Truncating while advancing the cursor to transactionTime would drop the
    remainder permanently -- the next export starts after the data we skipped.
    """
    _mock_bulk(
        MockBulkClient,
        "2024-06-01T00:00:00Z",
        {"Patient": [_patient(f"p{i}") for i in range(50)]},
    )
    c = _make_connector(bulk_export_enabled="true")

    records, offset = c.read_table("Patient", None, {"max_records_per_batch": "10"})
    emitted = list(records)
    assert len(emitted) == 50
    assert {r["id"] for r in emitted} == {f"p{i}" for i in range(50)}
    assert offset["cursor"] == "2024-06-01T00:00:00Z"


# ── Regression tests: overlap widens the query, not the cursor (F12) ──────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_overlap_widens_since_sent_to_server(MockBulkClient):
    inst = _mock_bulk(
        MockBulkClient, "2024-06-01T12:30:00+00:00", {"Patient": [_patient("p1")]}
    )
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="600")

    c.read_table("Patient", {"cursor": "2024-06-01T12:00:00+00:00"}, {})
    # "Z", not "+00:00": in typefilter mode this is embedded in a nested
    # _typeFilter query where an encoded "+" decodes back to a space.
    assert inst.export.call_args.kwargs["since"] == "2024-06-01T11:50:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_overlap_stores_transaction_time_unmodified(MockBulkClient):
    """The stored cursor must move forward, never backward."""
    _mock_bulk(MockBulkClient, "2024-06-01T12:30:00+00:00", {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="3600")

    records, offset = c.read_table("Patient", {"cursor": "2024-06-01T12:00:00+00:00"}, {})
    # Records must survive: an offset clamped back to the previous cursor would
    # be read as end-of-data and the whole exported batch discarded.
    assert len(list(records)) == 1
    assert offset["cursor"] == "2024-06-01T12:30:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_overlap_terminates_when_transaction_time_repeats(MockBulkClient):
    """Same snapshot instant twice means no new data, so the trigger must end."""
    _mock_bulk(MockBulkClient, "2024-06-01T12:00:00+00:00", {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="600")

    previous = {"cursor": "2024-06-01T12:00:00+00:00", "run_ts": c._init_ts}
    records, offset = c.read_table("Patient", previous, {})
    assert list(records) == []
    assert offset == previous


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_zero_overlap_leaves_since_untouched(MockBulkClient):
    inst = _mock_bulk(
        MockBulkClient, "2024-06-01T12:30:00+00:00", {"Patient": [_patient("p1")]}
    )
    c = _make_connector(bulk_export_enabled="true")

    c.read_table("Patient", {"cursor": "2024-06-01T12:00:00+00:00"}, {})
    assert inst.export.call_args.kwargs["since"] == "2024-06-01T12:00:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_naive_transaction_time_does_not_crash(MockBulkClient):
    """A transactionTime with no offset must not blow up the aware comparison."""
    _mock_bulk(MockBulkClient, "2024-06-01T12:30:00", {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="600")

    records, offset = c.read_table("Patient", {"cursor": "2024-06-01T12:00:00+00:00"}, {})
    assert len(list(records)) == 1
    assert offset["cursor"] == "2024-06-01T12:30:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_untyped_manifest_bucket_is_not_dropped(MockBulkClient):
    """A single-type export whose manifest omits "type" must still be ingested."""
    _mock_bulk(MockBulkClient, "2024-06-01T12:00:00+00:00", {"Unknown": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true")

    records, offset = c.read_table("Patient", None, {})
    emitted = list(records)
    assert len(emitted) == 1
    assert emitted[0]["id"] == "p1"
    assert offset["cursor"] == "2024-06-01T12:00:00Z"


# ── Regression tests: attribute init and mode validation ──────────────────────


def test_invalid_incremental_mode_rejected_at_construction():
    import pytest

    with pytest.raises(ValueError, match="Invalid bulk_incremental_mode"):
        _make_connector(bulk_export_enabled="true", bulk_incremental_mode="weekly")


# ── Regression tests: option validation (F14) ─────────────────────────────────


def test_non_numeric_poll_interval_raises_actionable_error():
    import pytest

    with pytest.raises(ValueError, match="bulk_export_poll_interval"):
        _make_connector(bulk_export_enabled="true", bulk_export_poll_interval="30s")


def test_zero_timeout_rejected():
    import pytest

    with pytest.raises(ValueError, match="must be >= 1"):
        _make_connector(bulk_export_enabled="true", bulk_export_timeout="0")


def test_negative_overlap_rejected():
    import pytest

    with pytest.raises(ValueError, match="must be >= 0"):
        _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="-5")


def test_blank_option_falls_back_to_default():
    c = _make_connector(bulk_export_enabled="true", bulk_export_poll_interval="")
    assert c._bulk_enabled


# ── Regression tests: second-round review findings ────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_empty_export_still_advances_cursor(MockBulkClient):
    """A low-churn table must progress, not re-export the same window forever."""
    _mock_bulk(MockBulkClient, "2024-07-01T00:00:00Z", {"Patient": []})
    c = _make_connector(bulk_export_enabled="true")

    records, offset = c.read_table("Patient", {"cursor": "2024-01-01T00:00:00Z"}, {})
    assert list(records) == []
    assert offset["cursor"] == "2024-07-01T00:00:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_wrongly_typed_bucket_is_not_relabelled(MockBulkClient):
    """Only the untyped bucket may be adopted; a real other type must not be."""
    _mock_bulk(
        MockBulkClient,
        "2024-06-01T12:00:00Z",
        {"OperationOutcome": [{"resourceType": "OperationOutcome", "id": "oo1"}]},
    )
    c = _make_connector(bulk_export_enabled="true")

    records, _ = c.read_table("Patient", None, {})
    assert list(records) == []


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_mixed_typed_and_untyped_buckets_are_merged(MockBulkClient):
    """Untyped entries alongside typed ones must not be silently dropped."""
    _mock_bulk(
        MockBulkClient,
        "2024-06-01T12:00:00Z",
        {"Patient": [_patient("p1"), _patient("p2")], "Unknown": [_patient("p3")]},
    )
    c = _make_connector(bulk_export_enabled="true")

    records, _ = c.read_table("Patient", None, {})
    assert {r["id"] for r in records} == {"p1", "p2", "p3"}


def test_non_finite_page_delay_rejected():
    import pytest

    for bad in ("nan", "inf", "-inf"):
        with pytest.raises(ValueError, match="page_delay"):
            c = _make_connector()
            c.read_table("Patient", None, {"page_delay": bad})


def test_parse_ts_accepts_real_world_fhir_instants():
    """Cursor parsing must not crash on formats real servers emit (py3.10+)."""
    from databricks.labs.community_connector.sources.fhir.fhir import _parse_ts

    forms = [
        "2024-06-01T12:30:00Z",
        "2024-06-01T12:30:00.1234567Z",  # 7-digit ticks (.NET/SQL Server)
        "2024-06-01T12:30:00.12Z",  # 2-digit fraction
        "2024-06-01T12:30:00+0000",  # compact offset
        "2024-06-01T12:30:00",  # no offset at all
    ]
    parsed = [_parse_ts(f) for f in forms]
    assert all(p.tzinfo is not None for p in parsed)
    assert _parse_ts("2024-06-01T12:30:00Z") == _parse_ts("2024-06-01T12:30:00+0000")


def test_parse_ts_still_rejects_garbage():
    import pytest

    from databricks.labs.community_connector.sources.fhir.fhir import _parse_ts

    with pytest.raises(ValueError):
        _parse_ts("not-a-timestamp")


# ── Regression tests: third review pass ───────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_cursor_never_regresses_on_earlier_transaction_time(MockBulkClient):
    """A start-time server can report a transactionTime before the stored cursor.

    Storing it would re-read the same span every trigger, which is exactly the
    monotonicity guarantee the README and connector_spec claim.
    """
    _mock_bulk(MockBulkClient, "2024-06-01T11:00:00Z", {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="3600")

    records, offset = c.read_table("Patient", {"cursor": "2024-06-01T12:00:00Z"}, {})
    # The batch must survive even though the cursor could not advance.
    assert len(list(records)) == 1
    assert offset["cursor"] == "2024-06-01T12:00:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_untyped_bucket_merge_does_not_duplicate(MockBulkClient):
    """A server listing the same file both typed and untyped must not double-emit."""
    _mock_bulk(
        MockBulkClient,
        "2024-06-01T12:00:00Z",
        {"Patient": [_patient("p1")], "Unknown": [_patient("p1"), _patient("p2")]},
    )
    c = _make_connector(bulk_export_enabled="true")

    ids = [r["id"] for r in c.read_table("Patient", None, {})[0]]
    assert sorted(ids) == ["p1", "p2"]


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_widen_since_normalises_non_utc_cursor_to_z(MockBulkClient):
    """A cursor with a non-UTC offset must still yield a "Z" instant."""
    inst = _mock_bulk(
        MockBulkClient, "2024-06-01T13:00:00Z", {"Patient": [_patient("p1")]}
    )
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="600")

    c.read_table("Patient", {"cursor": "2024-06-01T12:00:00+01:00"}, {})
    since = inst.export.call_args.kwargs["since"]
    assert since.endswith("Z")
    assert "+" not in since
    assert since == "2024-06-01T10:50:00Z"


# ── Regression tests: fourth review pass ──────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_records_survive_when_cursor_cannot_advance(MockBulkClient):
    """Holding the cursor must not discard the batch the export just paid for.

    A start-time server reports a transactionTime before the stored cursor, so
    the cursor is held — but the rows are real and must still be written.
    """
    _mock_bulk(
        MockBulkClient, "2024-06-01T11:00:00Z", {"Patient": [_patient("p1"), _patient("p2")]}
    )
    c = _make_connector(bulk_export_enabled="true", bulk_overlap_seconds="3600")

    records, offset = c.read_table("Patient", {"cursor": "2024-06-01T12:00:00Z"}, {})
    assert len(list(records)) == 2
    assert offset["cursor"] == "2024-06-01T12:00:00Z"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_only_one_export_per_trigger(MockBulkClient):
    """A $export is a whole server-side job; a trigger must never run two.

    Guards the unbounded-loop case: a server whose clock trails the driver used
    to keep producing cursors below _init_ts, so every call re-exported.
    """
    inst = _mock_bulk(MockBulkClient, "2020-01-01T00:00:00Z", {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true")

    records, offset = c.read_table("Patient", None, {})
    assert len(list(records)) == 1
    records2, offset2 = c.read_table("Patient", offset, {})
    assert list(records2) == []
    assert offset2 == offset
    assert inst.export.call_count == 1


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_typefilter_since_is_canonical_z_with_default_overlap(MockBulkClient):
    """With overlap=0 the cursor still must not reach _typeFilter as "+00:00".

    Percent-encoded "+" decodes to a space inside the nested query, corrupting
    the date bound on the very servers typefilter mode exists for.
    """
    inst = _mock_bulk(MockBulkClient, "2024-06-02T00:00:00Z", {"Patient": [_patient("p1")]})
    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="typefilter")

    c.read_table("Patient", {"cursor": "2024-06-01T12:00:00+00:00"}, {})
    since = inst.export.call_args.kwargs["since"]
    assert since == "2024-06-01T12:00:00Z"
    assert "+" not in since


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_respelled_transaction_time_does_not_reexport(MockBulkClient):
    """The same instant spelled differently must not look like new data."""
    inst = _mock_bulk(
        MockBulkClient, "2024-06-01T12:00:00.000+00:00", {"Patient": [_patient("p1")]}
    )
    c = _make_connector(bulk_export_enabled="true")

    _, offset = c.read_table("Patient", {"cursor": "2024-06-01T12:00:00Z"}, {})
    assert offset["cursor"] == "2024-06-01T12:00:00Z"
    records2, _ = c.read_table("Patient", offset, {})
    assert list(records2) == []
    assert inst.export.call_count == 1
