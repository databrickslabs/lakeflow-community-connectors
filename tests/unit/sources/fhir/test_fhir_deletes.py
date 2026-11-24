"""Tests for track_deletes: deletions read from each type's REST _history.

Entry shapes mirror public HAPI R4 (measured 2026-11-01): history is
newest-first, and DELETE entries carry only fullUrl, request and response,
with no resource and no lastModified.
"""

from unittest.mock import patch

import pytest

from databricks.labs.community_connector.libs.utils import parse_value
from databricks.labs.community_connector.sources.fhir.fhir import (
    FhirLakeflowConnect,
    _history_id,
    _parse_ts,
)
from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    HISTORY_OVERLAP_SECONDS,
)
from databricks.labs.community_connector.sources.fhir.fhir_utils import RESOURCE_SCHEMA

ITER_ENTRIES = "databricks.labs.community_connector.sources.fhir.fhir.iter_bundle_entries"
BASE = "https://fhir.example.com/r4"
PREVIOUS_TRIGGER = {"cursor": "2026-11-01T00:00:00Z", "run_ts": "2026-11-01T00:05:00+00:00"}


def _connector(**overrides):
    opts = {"base_url": BASE, "auth_type": "none", "track_deletes": "true"}
    opts.update(overrides)
    c = FhirLakeflowConnect(opts)
    c._fhir_version = "4.0.1"  # skip the /metadata call
    return c


def _version(pid, ts, method="PUT"):
    return {
        "fullUrl": f"{BASE}/Patient/{pid}",
        "request": {"method": method, "url": f"Patient/{pid}"},
        "resource": {"resourceType": "Patient", "id": pid, "meta": {"lastUpdated": ts}},
    }


def _delete(pid, version=2):
    return {
        "fullUrl": f"{BASE}/Patient/{pid}",
        "request": {"method": "DELETE", "url": f"Patient/{pid}/_history/{version}"},
        "response": {"status": "200 OK", "etag": f'W/"{version}"'},
    }


def _read_deletes(conn, entries, start_offset=PREVIOUS_TRIGGER):
    with patch(ITER_ENTRIES, return_value=iter(entries)) as mock_iter:
        records, offset = conn.read_table_deletes("Patient", start_offset, {})
        return list(records), offset, mock_iter


# ── Metadata ──────────────────────────────────────────────────────────────────


def test_track_deletes_declares_cdc_with_deletes():
    assert _connector().read_table_metadata("Patient", {})["ingestion_type"] == "cdc_with_deletes"


def test_deletes_off_by_default():
    meta = _connector(track_deletes="false").read_table_metadata("Patient", {})
    assert meta["ingestion_type"] == "cdc"


def test_full_bulk_mode_stays_snapshot_with_track_deletes():
    c = _connector(bulk_export_enabled="true", bulk_incremental_mode="full")
    assert c.read_table_metadata("Patient", {})["ingestion_type"] == "snapshot"


# ── Offsets and triggers ──────────────────────────────────────────────────────


def test_first_trigger_reads_nothing_and_starts_the_cursor():
    c = _connector()
    records, offset, mock_iter = _read_deletes(c, [], start_offset={})
    assert records == []
    mock_iter.assert_not_called()
    expected = _parse_ts(c._init_ts).timestamp() - HISTORY_OVERLAP_SECONDS
    assert _parse_ts(offset["cursor"]).timestamp() == pytest.approx(expected)
    assert offset["run_ts"] == c._init_ts


def test_second_call_in_a_trigger_short_circuits():
    c = _connector()
    _, offset, _ = _read_deletes(c, [_delete("p1")])
    records, offset2, mock_iter = _read_deletes(c, [_delete("p1")], start_offset=offset)
    assert records == []
    assert offset2 == offset
    mock_iter.assert_not_called()


def test_next_trigger_queries_history_since_cursor():
    _, _, mock_iter = _read_deletes(_connector(), [])
    args, kwargs = mock_iter.call_args
    assert args[1] == "Patient/_history"
    assert args[2] == {"_since": PREVIOUS_TRIGGER["cursor"], "_count": "100"}


# ── Delete detection and sequencing ───────────────────────────────────────────


def test_delete_is_sequenced_by_the_closest_newer_entry():
    entries = [
        _version("p2", "2026-11-01T10:00:00Z", "POST"),
        _delete("p1"),
        _version("p1", "2026-11-01T09:00:00Z"),
    ]
    records, _, _ = _read_deletes(_connector(), entries)
    assert [(r["resource_id"], r["last_updated"]) for r in records] == [
        ("p1", "2026-11-01T10:00:00Z")
    ]
    # Later than p1's last version, so the delete wins the merge.
    assert _parse_ts(records[0]["last_updated"]) > _parse_ts("2026-11-01T09:00:00Z")


def test_newest_delete_uses_the_read_time():
    c = _connector()
    records, _, _ = _read_deletes(c, [_delete("p1"), _version("p1", "2026-11-01T09:00:00Z")])
    assert records[0]["last_updated"] == records[0]["ingested_at"]


def test_delete_with_its_own_timestamp_uses_it():
    entry = _delete("p1")
    entry["response"]["lastModified"] = "2026-11-01T09:30:00Z"
    records, _, _ = _read_deletes(_connector(), [entry])
    assert records[0]["last_updated"] == "2026-11-01T09:30:00Z"


def test_resource_recreated_after_delete_is_not_deleted():
    entries = [_version("p1", "2026-11-01T11:00:00Z", "PUT"), _delete("p1")]
    records, _, _ = _read_deletes(_connector(), entries)
    assert records == []


def test_repeated_deletes_of_one_resource_emit_once():
    records, _, _ = _read_deletes(_connector(), [_delete("p1", 4), _delete("p1", 2)])
    assert [r["resource_id"] for r in records] == ["p1"]


def test_delete_records_satisfy_the_schema_and_keys():
    (record,), _, _ = _read_deletes(_connector(), [_delete("p1")])
    assert record["resource_type"] == "Patient"
    assert record["resource_id"] == "p1"
    assert record["resource"] is None
    parse_value(record, RESOURCE_SCHEMA)  # must not raise


def test_history_unsupported_names_the_requirement():
    c = _connector()
    with patch(ITER_ENTRIES, side_effect=RuntimeError("HTTP 405")):
        with pytest.raises(RuntimeError, match="track_deletes=true needs .*Patient/_history"):
            c.read_table_deletes("Patient", PREVIOUS_TRIGGER, {})


@pytest.mark.parametrize(
    "entry, expected",
    [
        ({"request": {"url": "Patient/380816/_history/2"}}, "380816"),
        ({"request": {"url": "Patient/380816"}}, "380816"),
        ({"fullUrl": f"{BASE}/Patient/380816"}, "380816"),
        ({"request": {"url": "Observation/1"}}, None),
        ({}, None),
    ],
)
def test_history_id(entry, expected):
    assert _history_id(entry, "Patient") == expected
