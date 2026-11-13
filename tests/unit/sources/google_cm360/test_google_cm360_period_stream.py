"""Native period CDC contract, checkpoint and actual bridge regression tests."""

import copy
import json
from datetime import date, timedelta
from unittest.mock import Mock

import pytest
from pyspark.sql.streaming.datasource import ReadAllAvailable

from databricks.labs.community_connector.interface import SupportsPartitionedStream
from databricks.labs.community_connector.sources.google_cm360 import google_cm360 as module
from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
    LakeflowBatchReader,
    LakeflowPartitionedStreamReader,
)


TABLE = "reach_report_period_incremental"
OPTIONS = {
    "sync_start_date": "2025-01-01", "sync_end_date": "2026-10-13",
    "delivery_country_codes": '["US"]', "reporting_timezone": "Europe/Paris",
}


@pytest.mark.parametrize('end', ['2026-11-30', None])
def test_future_start_preserves_native_schedule_without_a_report_period(end):
    assert module._campaign_period('2026-11-01', end, date(2026, 10, 13)) == (
        None, None, 'START_AFTER_CUTOFF')


@pytest.mark.parametrize('start,end', [
    ('2026-11-01', '2026-10-31'), ('2026-10-01', '2026-09-30'),
    ('2026-11-01', 'malformed'),
])
def test_invalid_schedule_is_not_hidden_by_future_start(start, end):
    with pytest.raises(ValueError):
        module._campaign_period(start, end, date(2026, 10, 13))


def test_cutoff_and_inclusive_window_boundaries_are_unchanged():
    cutoff = date(2026, 10, 13)
    assert module._campaign_period('2026-10-13', None, cutoff) == (cutoff, cutoff, 'COMPLETE')
    assert module._campaign_period('2026-07-14', None, cutoff)[2] == 'COMPLETE'
    assert module._campaign_period('2026-07-13', None, cutoff)[2] == 'WINDOW_LIMIT'


@pytest.mark.parametrize('all_future', [False, True])
def test_actual_bridge_retains_future_members_and_recovers_old_checkpoint(all_future):
    old_options = dict(OPTIONS, sync_end_date='2026-10-12')
    source = source_with(campaigns(1))
    bridge = LakeflowPartitionedStreamReader(
        {'tableName': TABLE, **old_options}, source.get_table_schema(TABLE, old_options), source)
    state = {}
    while True:
        end = bridge.latestOffset(state, ReadAllAvailable())
        if end == state:
            break
        for part in bridge.partitions(state, end):
            list(bridge.read(part))
        state = json.loads(json.dumps(end))
    rows = campaigns(2)
    for row in rows if all_future else rows[1:]:
        row.update(campaign_start_date='2026-11-01', campaign_end_date='2026-11-30')
    source = source_with(rows)
    bridge = LakeflowPartitionedStreamReader(
        {'tableName': TABLE, **OPTIONS}, source.get_table_schema(TABLE, OPTIONS), source)
    discovered = bridge.latestOffset(state, ReadAllAvailable())
    assert list(bridge.partitions(state, discovered)) == []
    state = json.loads(json.dumps(discovered))
    final = bridge.latestOffset(state, ReadAllAvailable())
    # A fresh reader must recover the frozen membership, including NULL reach rows.
    recovered = source_with([])
    reader = LakeflowPartitionedStreamReader(
        {'tableName': TABLE, **OPTIONS}, recovered.get_table_schema(TABLE, OPTIONS), recovered)
    result = [row for part in reader.partitions(state, final) for row in reader.read(part)]
    assert {row['campaign_id'] for row in result} == {'1', '2'}
    future = [row for row in result if row['coverage_status'] == 'START_AFTER_CUTOFF']
    assert len(future) == (2 if all_future else 1)
    assert all(row['snapshot_campaign_count'] == 2 for row in result)
    assert all(row['period_start'] is None and row['period_end'] is None
               and row['unique_reach_impression_reach'] is None for row in future)
    assert reader.latestOffset(final, ReadAllAvailable()) == final
    replay = [row for part in reader.partitions(state, final) for row in reader.read(part)]
    assert replay == result
    assert recovered._period_export.call_count == (0 if all_future else 2)


def test_schedule_error_identifies_key_and_dates_without_names_or_credentials():
    row = campaigns(1)[0]
    row.update(campaign_start_date='2026-11-01', campaign_end_date='2026-10-31')
    source = source_with([row])
    with pytest.raises(module.Cm360DataError) as error:
        source.latest_offset(TABLE, OPTIONS, {})
    message = str(error.value)
    assert 'campaign_id=1' in message and '2026-11-01' in message
    assert 'secret-do-not-persist' not in message and 'Campaign' not in message.replace('Campaign period', '')


def campaigns(count):
    return [{
        "profile_id": "123", "account_id": "24", "account_name": "Example",
        "subaccount_id": None, "subaccount_name": None,
        "advertiser_id": "7", "advertiser_name": "Example",
        "campaign_id": str(index + 1), "campaign_name": "Campaign",
        "delivery_country": "United States",
        "report_file_last_modified_at": "2026-10-13T00:00:00+00:00",
        "campaign_start_date": (date(2026, 8, 1) + timedelta(days=index)).isoformat(),
        "campaign_end_date": "2026-09-30", "report_date": "2026-10-13", "impressions": 1,
    } for index in range(count)]


def source_with(rows):
    source = module.GoogleCm360LakeflowConnect({
        "access_token": "secret-do-not-persist", "profile_id": "123",
    })
    def discover(_options):
        source._period_profile_metadata = module.ProfileMetadata("24", "Example", None, None)
        source._period_country_id = "840"
        return copy.deepcopy(rows)
    source._period_campaigns = Mock(side_effect=discover)
    def export(report, *_):
        filters = report["reachCriteria"]["dimensionFilters"]
        return [{"campaign_id": f["id"], "advertiser_id": "7",
                 "unique_reach_impression_reach": "-" if f["id"] == "2" else "0",
                 "report_file_last_modified_at": "2026-10-14T00:00:00+00:00"}
                for f in filters if f["dimensionName"] == "campaign"]
    source._period_export = Mock(side_effect=export)
    return source


def test_incremental_contract_is_additive_and_cutoff_keyed():
    source = source_with(campaigns(1))
    assert isinstance(source, SupportsPartitionedStream)
    assert TABLE in source.list_tables()
    assert source.read_table_metadata(TABLE, {}) == {
        "primary_keys": ["profile_id", "delivery_country", "advertiser_id", "campaign_id",
                         "source_cutoff_date"],
        "cursor_field": "report_file_last_modified_at", "ingestion_type": "cdc",
    }
    fields = source.get_table_schema(TABLE, {}).fields
    assert fields[-1].name == "snapshot_campaign_count" and not fields[-1].nullable
    assert source.read_table_metadata("reach_report_period", {})["ingestion_type"] == "snapshot"


def test_setup_checkpoints_frozen_work_without_rows_or_credentials():
    source = source_with(campaigns(3))
    end = source.latest_offset(TABLE, OPTIONS, {})
    assert end["position"] == 0
    assert all(isinstance(v, (str, int, bool)) for v in end.values())
    assert "secret-do-not-persist" not in json.dumps(end)
    assert not source.get_partitions(TABLE, OPTIONS, {}, end)
    assert source.latest_offset(TABLE, OPTIONS, {}) == end
    source._period_campaigns.assert_called_once()
    source._period_export.assert_not_called()


@pytest.mark.parametrize("mutation", ["identity", "cutoff", "corrupt", "position"])
def test_invalid_or_incompatible_checkpoint_cannot_reset_work(mutation):
    source = source_with(campaigns(2))
    state = source.latest_offset(TABLE, OPTIONS, {})
    options = dict(OPTIONS)
    if mutation == "identity":
        options["advertiser_ids"] = '["different"]'
    elif mutation == "cutoff":
        options["sync_end_date"] = "2026-10-14"
    elif mutation == "corrupt":
        state["manifest"] = state["manifest"][:-1]
    else:
        state["position"] = -1
    with pytest.raises(ValueError):
        source_with(campaigns(2)).latest_offset(TABLE, options, state)


@pytest.mark.parametrize("rows", [[], campaigns(1) * 2])
def test_empty_or_duplicate_discovery_fails_without_publishing(rows):
    source = source_with(rows)
    with pytest.raises(ValueError, match="empty|duplicate"):
        source.latest_offset(TABLE, OPTIONS, {})
    source._period_export.assert_not_called()


def test_actual_bridge_resumes_twenty_three_groups_across_three_readers():
    state, counts = {}, []
    for expected in (10, 20, 23):
        source = source_with(campaigns(23))
        bridge = LakeflowPartitionedStreamReader(
            {"tableName": TABLE, **OPTIONS}, source.get_table_schema(TABLE, OPTIONS), source,
        )
        rows = []
        while True:
            end = bridge.latestOffset(state, ReadAllAvailable())
            assert bridge.latestOffset(state, ReadAllAvailable()) == end
            if end == state:
                break
            for part in bridge.partitions(state, end):
                rows.extend(bridge.read(part))
            state = json.loads(json.dumps(end))
        assert state["position"] == expected
        counts.append(len(rows))
    assert counts == [10, 10, 3]


def test_coalesced_replay_reads_entire_exact_interval_with_frozen_metadata():
    source = source_with(campaigns(7))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    start, end = {**setup, "position": 2}, {**setup, "position": 7}
    replay = source_with(list(reversed(campaigns(1))))
    partitions = replay.get_partitions(TABLE, OPTIONS, start, end)
    assert len(partitions) == 1
    rows = list(replay.read_partition(TABLE, json.loads(json.dumps(partitions[0])), OPTIONS))
    assert {r["campaign_id"] for r in rows} == {"3", "4", "5", "6", "7"}
    assert all(r["snapshot_campaign_count"] == 7 for r in rows)
    replay._period_campaigns.assert_not_called()


def test_failed_interval_emits_no_prefix_and_new_reader_preserves_committed_work():
    source = source_with(campaigns(3))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    committed = {**setup, "position": 1}
    end = {**setup, "position": 3}
    source._period_export.side_effect = [
        [{"campaign_id": "2", "advertiser_id": "7", "unique_reach_impression_reach": "-",
          "report_file_last_modified_at": "2026-10-14T00:00:00+00:00"}],
        module.Cm360AuthenticationError("HTTP 401"),
    ]
    part = source.get_partitions(TABLE, OPTIONS, committed, end)[0]
    with pytest.raises(module.Cm360AuthenticationError, match="401"):
        source.read_partition(TABLE, part, OPTIONS)
    replay = source_with([])
    part = replay.get_partitions(TABLE, OPTIONS, committed, end)[0]
    rows = list(replay.read_partition(TABLE, part, OPTIONS))
    assert [r["campaign_id"] for r in rows] == ["2", "3"]
    assert [r["unique_reach_impression_reach"] for r in rows] == ["-", "0"]
    replay._period_campaigns.assert_not_called()


def test_all_window_limited_campaigns_complete_without_native_reach_export():
    rows = campaigns(2)
    for row in rows:
        row["campaign_start_date"] = "2026-01-01"
    source = source_with(rows)
    start = source.latest_offset(TABLE, OPTIONS, {})
    end = source.latest_offset(TABLE, OPTIONS, start)
    part = source.get_partitions(TABLE, OPTIONS, start, end)[0]
    result = list(source.read_partition(TABLE, part, OPTIONS))
    assert len(result) == 2
    assert all(r["coverage_status"] == "WINDOW_LIMIT" and
               r["unique_reach_impression_reach"] is None for r in result)
    assert source.latest_offset(TABLE, OPTIONS, end) == end
    source._period_export.assert_not_called()


def test_completed_cutoff_is_noop_then_later_cutoff_freezes_new_work():
    source = source_with(campaigns(1))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    completed = {**setup, "position": 1}
    assert source_with([]).latest_offset(TABLE, OPTIONS, completed) == completed
    new_options = {**OPTIONS, "sync_end_date": "2026-10-14"}
    next_source = source_with(campaigns(2))
    next_setup = next_source.latest_offset(TABLE, new_options, completed)
    assert next_setup["position"] == 0
    assert not next_source.get_partitions(TABLE, new_options, completed, next_setup)
    end = next_source.latest_offset(TABLE, new_options, next_setup)
    part = next_source.get_partitions(TABLE, new_options, next_setup, end)[0]
    assert all(row["source_cutoff_date"] == "2026-10-14"
               for row in next_source.read_partition(TABLE, part, new_options))


def test_partition_carries_epoch_deadline_without_persisting_query_budget(monkeypatch):
    source = source_with(campaigns(2))
    start = source.latest_offset(TABLE, OPTIONS, {})
    end = source.latest_offset(TABLE, OPTIONS, start)
    part = source.get_partitions(TABLE, OPTIONS, start, end)[0]
    assert "deadline_epoch" in part and "deadline" not in json.dumps(end)
    # A different executor cannot restart the driver's query budget.
    part["deadline_epoch"] = 0
    executor = source_with([])
    with pytest.raises(TimeoutError):
        executor.read_partition(TABLE, part, OPTIONS)
    executor._period_export.assert_not_called()
    assert executor._snapshot_deadline is None


def test_repeated_offset_keeps_announced_end_after_deadline(monkeypatch):
    source = source_with(campaigns(2))
    now = [1000.0]
    monkeypatch.setattr(module.time, "time", lambda: now[0])
    setup = source.latest_offset(TABLE, OPTIONS, {})
    end = source.latest_offset(TABLE, OPTIONS, setup)
    assert end["position"] == 2
    now[0] = 2801.0
    assert source.latest_offset(TABLE, OPTIONS, setup) == end
    assert source.latest_offset(TABLE, OPTIONS, end) == end
    part = source.get_partitions(TABLE, OPTIONS, setup, end)[0]
    with pytest.raises(TimeoutError, match="deadline"):
        source.read_partition(TABLE, part, OPTIONS)
    source._period_export.assert_not_called()


def test_export_returning_after_deadline_cannot_publish_rows(monkeypatch):
    source = source_with(campaigns(1))
    clock = [0.0]
    monkeypatch.setattr(module.time, "time", lambda: 1000 + clock[0])
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    start = source.latest_offset(TABLE, OPTIONS, {})
    end = source.latest_offset(TABLE, OPTIONS, start)
    part = source.get_partitions(TABLE, OPTIONS, start, end)[0]
    def late_export(*_):
        clock[0] = 1801
        return []
    source._period_export.side_effect = late_export
    with pytest.raises(TimeoutError, match="deadline"):
        source.read_partition(TABLE, part, OPTIONS)
    assert source._snapshot_deadline is None and source._client._snapshot_deadline is None


def test_export_after_ten_minutes_can_finish_within_thirty_minutes(monkeypatch):
    source = source_with(campaigns(1))
    clock = [0.0]
    monkeypatch.setattr(module.time, "time", lambda: 1000 + clock[0])
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    start = source.latest_offset(TABLE, OPTIONS, {})
    end = source.latest_offset(TABLE, OPTIONS, start)
    part = source.get_partitions(TABLE, OPTIONS, start, end)[0]
    native_export = source._period_export.side_effect
    def slow_export(*args):
        clock[0] = 601
        return native_export(*args)
    source._period_export.side_effect = slow_export
    rows = list(source.read_partition(TABLE, part, OPTIONS))
    assert len(rows) == 1 and rows[0]["unique_reach_impression_reach"] == "0"
    assert part["deadline_epoch"] == 2800
    assert source._snapshot_deadline is None and source._client._snapshot_deadline is None


def test_existing_batch_and_daily_streams_keep_their_original_reader_paths():
    source = source_with(campaigns(1))
    assert not source.is_partitioned("reach_report_daily")
    rows = [source._period_campaign_row(campaigns(1)[0], module._parse_read_options(OPTIONS))]
    source.read_table = Mock(return_value=(iter(rows), {}))
    bridge = LakeflowBatchReader(
        {"tableName": "reach_report_period"},
        source.get_table_schema("reach_report_period", {}), source,
    )
    parts = bridge.partitions()
    assert len(parts) == 1 and parts[0].value is None
    assert len(list(bridge.read(parts[0]))) == 1
    source.read_table.assert_called_once()


def test_oversized_manifest_cannot_be_checkpointed():
    rows = campaigns(1)
    rows[0]["campaign_name"] = "x" * (1024 * 1024)
    with pytest.raises(ValueError, match="1MiB"):
        source_with(rows).latest_offset(TABLE, OPTIONS, {})


@pytest.mark.parametrize("offset", [[], "", 0, {"version": 1}])
def test_non_dictionary_or_incomplete_checkpoint_is_not_initial_discovery(offset):
    source = source_with(campaigns(1))
    with pytest.raises(ValueError):
        source.latest_offset(TABLE, OPTIONS, offset)
    source._period_campaigns.assert_not_called()


def test_normal_planning_uses_one_atomic_ten_group_interval():
    source = source_with(campaigns(23))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    end = source.latest_offset(TABLE, OPTIONS, setup)
    assert end["position"] == 10
    assert source.latest_offset(TABLE, OPTIONS, setup) == end
    parts = source.get_partitions(TABLE, OPTIONS, setup, end)
    assert len(parts) == 1
    assert len(list(source.read_partition(TABLE, parts[0], OPTIONS))) == 10
    assert source.latest_offset(TABLE, OPTIONS, end) == end


def test_recovery_replay_and_new_work_share_the_ten_group_budget():
    source = source_with(campaigns(23))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    committed, pending = {**setup, "position": 3}, {**setup, "position": 7}
    resumed = source_with([])
    part = resumed.get_partitions(TABLE, OPTIONS, committed, pending)[0]
    replayed = list(resumed.read_partition(TABLE, part, OPTIONS))
    end = resumed.latest_offset(TABLE, OPTIONS, pending)
    assert end["position"] == 13
    part = resumed.get_partitions(TABLE, OPTIONS, pending, end)[0]
    remaining = list(resumed.read_partition(TABLE, part, OPTIONS))
    assert len(replayed) + len(remaining) == 10
    assert resumed.latest_offset(TABLE, OPTIONS, end) == end
    resumed._period_campaigns.assert_not_called()


def test_old_one_group_checkpoint_resumes_without_discovery_or_reset():
    source = source_with(campaigns(23))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    committed, pending = {**setup, "position": 10}, {**setup, "position": 11}
    resumed = source_with([])
    part = resumed.get_partitions(TABLE, OPTIONS, committed, pending)[0]
    assert len(list(resumed.read_partition(TABLE, part, OPTIONS))) == 1
    end = resumed.latest_offset(TABLE, OPTIONS, pending)
    assert end["position"] == 20
    part = resumed.get_partitions(TABLE, OPTIONS, pending, end)[0]
    assert len(list(resumed.read_partition(TABLE, part, OPTIONS))) == 9
    assert resumed.latest_offset(TABLE, OPTIONS, end) == end


def test_failed_seventh_group_does_not_publish_any_of_the_interval():
    source = source_with(campaigns(10))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    end = {**setup, "position": 10}
    export = source._period_export.side_effect
    calls = []
    def fail_seventh(*args):
        calls.append(args)
        if len(calls) == 7:
            raise module.Cm360AuthenticationError("HTTP 401")
        return export(*args)
    source._period_export.side_effect = fail_seventh
    part = source.get_partitions(TABLE, OPTIONS, setup, end)[0]
    with pytest.raises(module.Cm360AuthenticationError, match="401"):
        source.read_partition(TABLE, part, OPTIONS)
    assert len(calls) == 7
    resumed = source_with([])
    part = resumed.get_partitions(TABLE, OPTIONS, setup, end)[0]
    assert len(list(resumed.read_partition(TABLE, part, OPTIONS))) == 10
    resumed._period_campaigns.assert_not_called()


def test_failed_group_logs_position_and_elapsed_without_source_data(monkeypatch, caplog):
    source = source_with(campaigns(3))
    setup = source.latest_offset(TABLE, OPTIONS, {})
    part = source.get_partitions(TABLE, OPTIONS, setup, {**setup, "position": 3})[0]
    clock = [100.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    original = source._period_export.side_effect
    failure = module.Cm360SnapshotTimeout("secret-do-not-persist")
    calls = []
    def export(*args):
        calls.append(args)
        clock[0] += 7.5
        if len(calls) == 2:
            raise failure
        return original(*args)
    source._period_export.side_effect = export
    with pytest.raises(module.Cm360SnapshotTimeout) as caught:
        source.read_partition(TABLE, part, OPTIONS)
    assert caught.value is failure and len(calls) == 2
    assert caplog.messages == [
        "CM360 incremental group failed position=2 total=3 interval_first=0 "
        "interval_last=3 elapsed_seconds=7.500"
    ]
    if hasattr(failure, "add_note"):
        assert caught.value.__notes__ == caplog.messages
    assert source._snapshot_deadline is None and source._client._snapshot_deadline is None


def drain(source, options, state=None):
    """Run the actual partitioned-stream bridge until the cutoff is complete."""
    bridge = LakeflowPartitionedStreamReader(
        {"tableName": TABLE, **options}, source.get_table_schema(TABLE, options), source)
    state, rows = state or {}, []
    while True:
        end = bridge.latestOffset(state, ReadAllAvailable())
        if end == state:
            return state, rows
        for part in bridge.partitions(state, end):
            rows.extend(bridge.read(part))
        state = json.loads(json.dumps(end))


def ended_mix():
    """Cutoff 2026-10-13 after 2026-10-12: ended long ago, ended on the window edge,
    running, too long. The window edge is the previous cutoff minus 14 days."""
    rows = campaigns(4)
    rows[0].update(campaign_start_date="2026-08-01", campaign_end_date="2026-09-27")
    rows[1].update(campaign_start_date="2026-08-02", campaign_end_date="2026-09-28")
    rows[2].update(campaign_start_date="2026-08-03", campaign_end_date="2026-12-31")
    rows[3].update(campaign_start_date="2026-01-01", campaign_end_date="2026-12-31")
    return rows


def completed_previous_cutoff(rows, cutoff="2026-10-12"):
    """Checkpoint state of a finished earlier cutoff (written without the option)."""
    state, _ = drain(source_with(rows), dict(OPTIONS, sync_end_date=cutoff))
    return state


def test_long_ended_campaigns_stay_in_the_cohort_without_a_reach_report():
    options = dict(OPTIONS, refresh_ended_within_days="14")
    previous = completed_previous_cutoff(ended_mix())
    source = source_with(ended_mix())
    _, rows = drain(source, options, previous)
    by_id = {row["campaign_id"]: row for row in rows}
    assert set(by_id) == {"1", "2", "3", "4"}
    assert all(row["snapshot_campaign_count"] == 4 for row in rows)
    # Ended 2026-09-27 < 2026-10-12 - 14 days: frozen, no report, value left NULL.
    assert by_id["1"]["coverage_status"] == "NOT_REFRESHED"
    assert by_id["1"]["unique_reach_impression_reach"] is None
    assert str(by_id["1"]["period_end"]) == "2026-09-27"
    # Ended exactly 14 days before the previous cutoff, and still running: refreshed.
    assert by_id["2"]["coverage_status"] == "COMPLETE"
    assert by_id["3"]["coverage_status"] == "COMPLETE"
    assert by_id["2"]["unique_reach_impression_reach"] == "-"
    assert by_id["3"]["unique_reach_impression_reach"] == "0"
    # Spans over 92 days never had a report; their status is unchanged.
    assert by_id["4"]["coverage_status"] == "WINDOW_LIMIT"
    requested = {f["id"] for call in source._period_export.call_args_list
                 for f in call.args[0]["reachCriteria"]["dimensionFilters"]
                 if f["dimensionName"] == "campaign"}
    assert requested == {"2", "3"}


def test_absent_option_keeps_refreshing_every_complete_campaign():
    source = source_with(ended_mix())
    _, rows = drain(source, OPTIONS)
    assert {r["campaign_id"]: r["coverage_status"] for r in rows} == {
        "1": "COMPLETE", "2": "COMPLETE", "3": "COMPLETE", "4": "WINDOW_LIMIT"}
    assert source._period_export.call_count == 3


def test_zero_days_refreshes_only_campaigns_running_through_the_previous_cutoff():
    rows = ended_mix()
    rows[1]["campaign_end_date"] = "2026-10-12"
    previous = completed_previous_cutoff(rows)
    _, result = drain(source_with(rows), dict(OPTIONS, refresh_ended_within_days="0"), previous)
    assert {r["campaign_id"]: r["coverage_status"] for r in result} == {
        "1": "NOT_REFRESHED", "2": "COMPLETE", "3": "COMPLETE", "4": "WINDOW_LIMIT"}


def test_cutoff_where_every_campaign_ended_completes_without_reports():
    rows = ended_mix()[:1]
    previous = completed_previous_cutoff(rows)
    source = source_with(rows)
    state, result = drain(source, dict(OPTIONS, refresh_ended_within_days="14"), previous)
    assert [r["coverage_status"] for r in result] == ["NOT_REFRESHED"]
    assert result[0]["snapshot_campaign_count"] == 1
    assert state["position"] == 1
    source._period_export.assert_not_called()


def test_replay_keeps_the_frozen_refresh_decision_whatever_the_current_option():
    options = dict(OPTIONS, refresh_ended_within_days="14")
    previous = completed_previous_cutoff(ended_mix())
    setup = source_with(ended_mix()).latest_offset(TABLE, options, previous)
    assert setup["position"] == 0 and setup["manifest"] != previous["manifest"]
    end = {**setup, "position": 2}
    # A new reader without the option (or with another value) must neither
    # request reach for the frozen campaign nor drop it.
    for replay_options in (OPTIONS, dict(OPTIONS, refresh_ended_within_days="365")):
        replay = source_with([])
        part = replay.get_partitions(TABLE, replay_options, setup, end)[0]
        rows = list(replay.read_partition(TABLE, part, replay_options))
        frozen = [r for r in rows if r["campaign_id"] == "1"]
        assert [r["coverage_status"] for r in frozen] == ["NOT_REFRESHED"]
        replay._period_campaigns.assert_not_called()
        for call in replay._period_export.call_args_list:
            ids = {f["id"] for f in call.args[0]["reachCriteria"]["dimensionFilters"]
                   if f["dimensionName"] == "campaign"}
            assert "1" not in ids


def test_unfinished_checkpoint_without_the_option_resumes_after_it_is_enabled():
    setup = source_with(ended_mix()).latest_offset(TABLE, OPTIONS, {})
    committed = {**setup, "position": 1}
    resumed = source_with([])
    state, rows = drain(resumed, dict(OPTIONS, refresh_ended_within_days="14"), committed)
    assert state["position"] == 4
    # The old manifest still refreshes every COMPLETE member of its cutoff.
    assert {r["campaign_id"]: r["coverage_status"] for r in rows} == {
        "1": "COMPLETE", "2": "COMPLETE", "3": "COMPLETE"}
    resumed._period_campaigns.assert_not_called()


def test_first_cutoff_of_a_stream_refreshes_every_complete_campaign():
    source = source_with(ended_mix())
    _, rows = drain(source, dict(OPTIONS, refresh_ended_within_days="14"))
    assert "NOT_REFRESHED" not in {r["coverage_status"] for r in rows}
    assert source._period_export.call_count == 3


def test_after_a_gap_a_campaign_ended_in_between_is_refreshed_once_more():
    # Previous cutoff 2026-09-01, next 2026-10-13: a campaign ending 2026-09-20 was
    # still running at the previous cutoff, so its stored value is partial.
    rows = ended_mix()
    rows[0]["campaign_end_date"] = "2026-08-10"
    rows[2]["campaign_end_date"] = "2026-09-20"
    previous = completed_previous_cutoff(rows, cutoff="2026-09-01")
    _, result = drain(source_with(rows), dict(OPTIONS, refresh_ended_within_days="14"), previous)
    by_id = {r["campaign_id"]: r for r in result}
    assert by_id["3"]["coverage_status"] == "COMPLETE"
    assert str(by_id["3"]["period_end"]) == "2026-09-20"
    assert by_id["3"]["unique_reach_impression_reach"] == "0"
    # Already past the window at the previous cutoff (2026-08-10 < 2026-08-18): frozen.
    assert by_id["1"]["coverage_status"] == "NOT_REFRESHED"


def test_campaign_first_seen_long_after_its_end_is_refreshed_once():
    # Late-reported delivery: campaign 1 ended long ago but was not in the previous
    # cohort, so no stored value exists. It must get one real reach report.
    rows = ended_mix()
    previous = completed_previous_cutoff(rows[1:])
    source = source_with(rows)
    _, result = drain(source, dict(OPTIONS, refresh_ended_within_days="14"), previous)
    by_id = {r["campaign_id"]: r for r in result}
    assert by_id["1"]["coverage_status"] == "COMPLETE"
    assert by_id["1"]["unique_reach_impression_reach"] == "0"
    assert all(r["snapshot_campaign_count"] == 4 for r in result)


def test_changed_period_of_an_ended_campaign_is_refreshed_once():
    # The end date moved from 2026-09-25 to 2026-09-27: the stored value covers
    # another period, so the campaign is refreshed instead of frozen.
    before = ended_mix()
    before[0]["campaign_end_date"] = "2026-09-25"
    previous = completed_previous_cutoff(before)
    _, result = drain(source_with(ended_mix()), dict(OPTIONS, refresh_ended_within_days="14"),
                      previous)
    assert {r["campaign_id"]: r["coverage_status"] for r in result}["1"] == "COMPLETE"


def test_changed_start_of_an_ended_campaign_is_refreshed_once():
    before = ended_mix()
    before[0]["campaign_start_date"] = "2026-08-05"
    previous = completed_previous_cutoff(before)
    _, result = drain(source_with(ended_mix()), dict(OPTIONS, refresh_ended_within_days="14"),
                      previous)
    assert {r["campaign_id"]: r["coverage_status"] for r in result}["1"] == "COMPLETE"


def test_late_campaign_is_refreshed_then_frozen_on_the_next_cutoff():
    options = dict(OPTIONS, refresh_ended_within_days="14")
    previous = completed_previous_cutoff(ended_mix()[1:])
    state, first = drain(source_with(ended_mix()), options, previous)
    assert {r["campaign_id"]: r["coverage_status"] for r in first}["1"] == "COMPLETE"
    _, second = drain(source_with(ended_mix()), dict(options, sync_end_date="2026-10-14"), state)
    assert {r["campaign_id"]: r["coverage_status"] for r in second}["1"] == "NOT_REFRESHED"


def test_frozen_campaign_stays_frozen_on_the_following_cutoff():
    options = dict(OPTIONS, refresh_ended_within_days="14")
    previous = completed_previous_cutoff(ended_mix())
    state, first = drain(source_with(ended_mix()), options, previous)
    assert {r["campaign_id"]: r["coverage_status"] for r in first}["1"] == "NOT_REFRESHED"
    source = source_with(ended_mix())
    _, second = drain(source, dict(options, sync_end_date="2026-10-14"), state)
    assert {r["campaign_id"]: r["coverage_status"] for r in second}["1"] == "NOT_REFRESHED"


def test_window_limited_previous_row_never_lets_a_campaign_freeze():
    rows = ended_mix()
    previous_rows = ended_mix()
    previous_rows[0]["campaign_start_date"] = "2026-01-01"
    previous = completed_previous_cutoff(previous_rows)
    _, result = drain(source_with(rows), dict(OPTIONS, refresh_ended_within_days="14"), previous)
    assert {r["campaign_id"]: r["coverage_status"] for r in result}["1"] == "COMPLETE"


@pytest.mark.parametrize("value", ["-1", "1.5", "abc", "", "3651", " 14"])
def test_invalid_refresh_window_is_rejected(value):
    with pytest.raises(ValueError, match="refresh_ended_within_days"):
        source_with(campaigns(1)).latest_offset(
            TABLE, dict(OPTIONS, refresh_ended_within_days=value), {})


def test_full_snapshot_table_rejects_the_refresh_window():
    source = source_with(campaigns(1))
    options = dict(OPTIONS, refresh_ended_within_days="14")
    with pytest.raises(module.Cm360ConfigurationError, match="refresh_ended_within_days"):
        source.read_table("reach_report_period", None, options)
    source._period_campaigns.assert_not_called()
