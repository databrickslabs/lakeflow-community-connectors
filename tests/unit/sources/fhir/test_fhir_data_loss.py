"""Regression tests for three data-loss paths found in review (2026-10-21).

1. REST batches cut inside a run of equal lastUpdated values lost the rest of the tie.
2. Bulk manifest error[] entries were only logged while the cursor advanced.
3. Full bulk mode was declared "cdc", so server-side deletes never left the target.
"""

import json
import logging
from unittest.mock import patch

import pytest

from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect, _canonical_ts
from databricks.labs.community_connector.sources.fhir.fhir_bulk import BulkExportClient
from databricks.labs.community_connector.sources.fhir.fhir_utils import SmartAuthClient

ITER_PAGES = "databricks.labs.community_connector.sources.fhir.fhir.iter_bundle_pages"


def _connector(**overrides):
    opts = {"base_url": "https://fhir.example.com/r4", "auth_type": "none"}
    opts.update(overrides)
    return FhirLakeflowConnect(opts)


def _patient(pid, ts):
    return {"resourceType": "Patient", "id": pid, "meta": {"lastUpdated": ts}}


def _read(conn, resources, start_offset=None, max_records=2):
    with patch(ITER_PAGES, return_value=iter(resources)):
        records, offset = conn.read_table(
            "Patient", start_offset, {"max_records_per_batch": str(max_records)}
        )
        return [r["resource_id"] for r in records], offset


# ── 1. REST timestamp ties ────────────────────────────────────────────────────

T1 = "2026-10-01T00:00:00.000Z"
T2 = "2026-10-01T00:00:01.000Z"
T3 = "2026-10-01T00:00:02.000Z"


def test_batch_is_not_cut_inside_a_timestamp_tie():
    # Five records share one instant; the old code kept 2 and then queried
    # lastUpdated=gt<T1>, losing the other 3 permanently.
    ids, offset = _read(_connector(), [_patient(str(i), T1) for i in range(5)])
    assert ids == ["0", "1", "2", "3", "4"]
    assert offset == {"cursor": T1}


def test_batch_stops_at_first_record_after_the_tie():
    resources = [_patient("a", T1), _patient("b", T2), _patient("c", T2), _patient("d", T3)]
    ids, offset = _read(_connector(), resources)
    # Cap of 2 lands on "b" (T2); "c" shares T2 so it is kept; "d" is left for
    # the next read, which asks for gt<T2> and will fetch it.
    assert ids == ["a", "b", "c"]
    assert offset == {"cursor": T2}


def test_batch_without_ties_still_respects_max_records():
    ids, offset = _read(_connector(), [_patient("a", T1), _patient("b", T2), _patient("c", T3)])
    assert ids == ["a", "b"]
    assert offset == {"cursor": T2}


def test_truncated_unsorted_results_raise_instead_of_skipping():
    # Server ignored _sort. The batch is cut after "b", so the cursor would be
    # T3 and "c" (T2) would be skipped forever by the next gt<T3> query.
    resources = [_patient("a", T3), _patient("b", T1), _patient("c", T2)]
    with pytest.raises(RuntimeError, match="out of _lastUpdated order"):
        _read(_connector(), resources)


def test_unsorted_results_are_fine_when_nothing_is_truncated():
    resources = [_patient("a", T3), _patient("b", T1)]
    ids, offset = _read(_connector(), resources, max_records=10)
    assert ids == ["a", "b"]
    assert offset == {"cursor": T3}


# ── 2. Bulk manifest errors ───────────────────────────────────────────────────


def _bulk_client(ignore_errors=False):
    auth = SmartAuthClient(token_url="", client_id="", auth_type="none")
    return BulkExportClient(
        base_url="https://fhir.example.com/r4", auth_client=auth, ignore_errors=ignore_errors
    )


def _outcome(severity):
    issue = {"code": "processing"}
    if severity:
        issue["severity"] = severity
    return {"resourceType": "OperationOutcome", "issue": [issue]}


def _manifest_with_error_file():
    return {
        "output": [{"type": "Patient", "url": "https://fhir.example.com/patient.ndjson"}],
        "error": [{"type": "OperationOutcome", "url": "https://fhir.example.com/err.ndjson"}],
    }


def _fake_download(error_outcomes):
    """Fake BulkExportClient._download_ndjson, which yields NDJSON text lines."""

    def fake(url, requires_token):
        if "err" in url:
            return iter(json.dumps(o) for o in error_outcomes)
        return iter([json.dumps({"resourceType": "Patient", "id": "p1"})])

    return fake


@pytest.mark.parametrize("severity", ["error", "fatal", None])
def test_error_outcome_fails_the_export(severity):
    c = _bulk_client()
    c._download_ndjson = _fake_download([_outcome(severity)])
    with pytest.raises(RuntimeError, match="export is incomplete"):
        c._download_and_parse(_manifest_with_error_file())


@pytest.mark.parametrize("severity", ["warning", "information"])
def test_warning_outcome_is_only_logged(severity, caplog):
    c = _bulk_client()
    c._download_ndjson = _fake_download([_outcome(severity)])
    with caplog.at_level(logging.WARNING):
        result = c._download_and_parse(_manifest_with_error_file())
    assert [res["id"] for _, res in result] == ["p1"]
    assert "OperationOutcome" in caplog.text


def test_unreadable_error_file_fails_the_export():
    c = _bulk_client()

    def fake(url, requires_token):
        if "err" in url:
            raise RuntimeError("HTTP 500")
        return iter([json.dumps({"resourceType": "Patient", "id": "p1"})])

    c._download_ndjson = fake
    with pytest.raises(RuntimeError, match="could not be downloaded"):
        c._download_and_parse(_manifest_with_error_file())


def test_ignore_errors_accepts_partial_export(caplog):
    c = _bulk_client(ignore_errors=True)
    c._download_ndjson = _fake_download([_outcome("error")])
    with caplog.at_level(logging.WARNING):
        result = c._download_and_parse(_manifest_with_error_file())
    assert [res["id"] for _, res in result] == ["p1"]
    assert "bulk_export_ignore_errors" in caplog.text


def test_ignore_errors_option_reaches_bulk_client():
    c = _connector(bulk_export_enabled="true", bulk_export_ignore_errors="true")
    assert c._bulk_client._ignore_errors is True
    assert _connector(bulk_export_enabled="true")._bulk_client._ignore_errors is False


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_export_error_leaves_cursor_unadvanced(MockBulkClient):
    # The read raises, so read_table never returns an end offset: the framework
    # keeps the old cursor and the next trigger retries the export.
    MockBulkClient.return_value.export.side_effect = RuntimeError("export is incomplete")
    c = _connector(bulk_export_enabled="true")
    with pytest.raises(RuntimeError, match="export is incomplete"):
        c.read_table("Patient", {"cursor": T1}, {})


def test_deleted_manifest_entries_are_reported(caplog):
    c = _bulk_client()
    c._download_ndjson = _fake_download([])
    manifest = {
        "output": [{"type": "Patient", "url": "https://fhir.example.com/patient.ndjson"}],
        "deleted": [{"type": "Bundle", "url": "https://fhir.example.com/del.ndjson"}],
    }
    with caplog.at_level(logging.WARNING):
        c._download_and_parse(manifest)
    assert "1 file(s) of deleted resources" in caplog.text


# ── 3. Full mode ingests as snapshot ──────────────────────────────────────────


def test_full_bulk_mode_is_snapshot():
    c = _connector(bulk_export_enabled="true", bulk_incremental_mode="full")
    assert c.read_table_metadata("Patient", {}) == {
        "primary_keys": ["resource_id"],
        "ingestion_type": "snapshot",
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"bulk_export_enabled": "true"},
        {"bulk_export_enabled": "true", "bulk_incremental_mode": "typefilter"},
    ],
)
def test_incremental_modes_stay_cdc(overrides):
    meta = _connector(**overrides).read_table_metadata("Patient", {})
    assert meta["ingestion_type"] == "cdc"
    assert meta["cursor_field"]


def test_rest_mode_ignores_full_bulk_setting():
    # bulk_incremental_mode only matters when bulk export is on.
    meta = _connector(bulk_incremental_mode="full").read_table_metadata("Patient", {})
    assert meta["ingestion_type"] == "cdc"


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_snapshot_batch_read_returns_full_export(MockBulkClient):
    # The snapshot path calls read_table(table, None, ...) once per batch read.
    MockBulkClient.return_value.export.return_value = (
        None,
        [(json.dumps(r), r) for r in (_patient("p1", T1), _patient("p2", T2))],
    )
    c = _connector(bulk_export_enabled="true", bulk_incremental_mode="full")
    records, _ = c.read_table("Patient", None, {})
    assert [r["resource_id"] for r in records] == ["p1", "p2"]


# ── 4. The _init_ts cap is an upper bound on the query ────────────────────────


@pytest.mark.parametrize("since", [None, T1])
def test_rest_query_is_bounded_by_init_ts(since):
    """Resources written after the trigger started wait for the next trigger."""
    c = _connector()
    with patch(ITER_PAGES, return_value=iter([])) as mock_pages:
        c.read_table("Patient", {"cursor": since} if since else {}, {})
    bounds = mock_pages.call_args.args[2]["_lastUpdated"]
    expected_le = "le" + _canonical_ts(c._init_ts)
    assert bounds == ([f"gt{since}", expected_le] if since else [expected_le])
