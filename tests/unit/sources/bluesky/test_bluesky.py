import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql.types import StringType, StructType, _make_type_verifier

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.libs.utils import parse_value
from databricks.labs.community_connector.sources.bluesky import (
    BlueskyDataSource,
    BlueskyLakeflowConnect,
)
from databricks.labs.community_connector.sources.bluesky.bluesky import ArchiveValidator
from databricks.labs.community_connector.sources.bluesky.errors import (
    ArchiveChanged,
    CursorTooOld,
    ProtocolError,
)
from databricks.labs.community_connector.sources.bluesky.options import MAX_SEQ, Options
from databricks.labs.community_connector.sparkpds.registry import find_data_source


def consume(connector, start=None, options=None):
    rows, end = connector.read_table("events", start, options or {})
    return list(rows), end


def make_connector(options: dict[str, str] | None = None) -> BlueskyLakeflowConnect:
    return BlueskyLakeflowConnect(Options.parse(options or {}, {}))


def grow_fixture_archive(service):
    """Add two real events to the simulator after an availableNow target is pinned."""
    from tests.unit.sources.bluesky.conftest import segment_bytes

    new_events = []
    for seq in (8, 9):
        event = copy.deepcopy(service.events[0])
        event["seq"] = seq
        event["rkey"] = f"post{seq}"
        event["rev"] = f"rev{seq}"
        event["payload"]["text"] = f"new post {seq}"
        new_events.append(event)
    service.events.extend(new_events)
    service.data, service.checksum = segment_bytes(service.events)
    service.tip = 9


def drain_framework_run(reader, start, trigger, service=None):
    """Read successive availableNow offsets from one framework reader instance."""
    reader.prepareForTriggerAvailableNow()
    checkpoint = start
    rows = []
    grew = False
    for _ in range(10):
        end = reader.latestOffset(checkpoint, trigger)
        if end == checkpoint:
            return rows, checkpoint
        batch_rows = [
            row
            for partition in reader.partitions(checkpoint, end)
            for row in reader.read(partition)
        ]
        rows.extend(batch_rows)
        checkpoint = end
        if service is not None and not grew:
            grow_fixture_archive(service)
            grew = True
    raise AssertionError("availableNow reader did not reach its fixed run target")


def test_discovery_schema_metadata(service):
    connector = make_connector({})
    assert isinstance(connector, (LakeflowConnect, SupportsPartitionedStream))
    assert connector.list_tables() == ["events"]
    assert find_data_source("bluesky") is BlueskyDataSource
    schema = connector.get_table_schema("events", {})
    assert isinstance(schema, StructType)
    assert connector.read_table_metadata("events", {}) == {
        "primary_keys": ["seq"],
        "cursor_field": "seq",
        "ingestion_type": "append",
    }
    rows, _ = consume(connector)
    verifier = _make_type_verifier(schema)
    for row in rows:
        verifier(parse_value(row, schema))
    assert json.loads(rows[0]["record"])["text"] == "hello"
    assert rows[0]["cid"].startswith("bafy")
    assert rows[2]["operation"] == "delete" and rows[2]["record"] is None
    assert rows[6]["is_resync"] is True
    assert rows[1]["event_time"] != rows[1]["witnessed_at"]


def test_schema_validation_and_caller_isolation():
    connector = make_connector({})
    schema = connector.get_table_schema("events", {})
    schema.add("caller_only", StringType())

    assert "caller_only" not in connector.get_table_schema("events", {}).fieldNames()
    with pytest.raises(ValueError):
        connector.get_table_schema("events", {"max_retries": "99"})


@pytest.mark.parametrize("method", ["get_table_schema", "read_table_metadata"])
def test_invalid_table(method):
    with pytest.raises(ValueError, match="Unknown table"):
        getattr(make_connector({}), method)("posts", {})


@pytest.mark.parametrize(
    "options",
    [
        {"endpoint": "ws://old.example"},
        {"endpoint": "https://user:secret@example.com"},
        {"endpoint": "https://example.com/?secret=value"},
        {"starting_cursor": "-1"},
        {"starting_cursor": "1.5"},
        {"max_events_per_refresh": "0"},
        {"max_retries": "99"},
        {"request_timeout_seconds": "0"},
        {"include_raw_payload": "yes"},
        {"api_key": "Bearer secret"},
        {"collections": "invalid"},
        {"kinds": ""},
        {"kinds": "commit,unknown"},
        {"kinds": "commit,commit,identity,account,sync,extra"},
        {"collections": "app.bsky.feed.post", "kinds": "account"},
        {"dids": "alice.test"},
        {"starting_cursor": True},
    ],
)
def test_invalid_options(options):
    with pytest.raises(ValueError):
        make_connector(options)


def test_connector_accepts_framework_mapping_and_validates_it():
    assert isinstance(BlueskyLakeflowConnect({}).config, Options)
    assert BlueskyLakeflowConnect({"future_framework_option": "value"}).config.endpoint
    with pytest.raises(TypeError, match="Options instance or a mapping"):
        BlueskyLakeflowConnect(None)


def test_secrets_not_in_repr():
    assert "supersecret" not in repr(Options.parse({"api_key": "supersecret"}, {}))


@pytest.mark.parametrize("value", [True, "1", -1, MAX_SEQ + 1])
def test_archive_sequence_rejects_invalid_values(value):
    with pytest.raises(ProtocolError):
        ArchiveValidator.sequence(value, "checkpoint seq")


def test_archive_sequence_accepts_supported_bounds():
    assert ArchiveValidator.sequence(0, "checkpoint seq") == 0
    assert ArchiveValidator.sequence(MAX_SEQ, "checkpoint seq") == MAX_SEQ


@pytest.mark.parametrize("mode", ["segment", "blocks"])
def test_pagination_and_duplicate_boundary(service, mode):
    service.mode = mode
    connector = make_connector({})
    rows, end = consume(connector)
    assert [r["seq"] for r in rows] == list(range(1, 8))
    plans = [json.loads(r.body) for r in service.calls if r.method == "POST"]
    assert [p["afterSeq"] for p in plans[1:]] == [0, 3, 6]
    assert [p["beforeSeq"] for p in plans[1:]] == [7, 7, 7]
    assert all(p["kinds"] == ["account", "commit", "identity", "sync"] for p in plans)
    assert consume(connector, end) == ([], end)


def test_multiple_bounded_batches_reach_one_fixed_run_target(service):
    config = {"max_events_per_refresh": "2"}
    connector = make_connector(config)
    checkpoint = None
    collected = []
    for _ in range(4):
        rows, checkpoint = consume(connector, checkpoint)
        collected.extend(r["seq"] for r in rows)
        assert len(rows) <= 2
        if collected and service.tip == 7:
            from tests.unit.sources.bluesky.conftest import segment_bytes

            new_events = []
            for seq in (8, 9):
                event = copy.deepcopy(service.events[0])
                event["seq"] = seq
                event["rkey"] = f"post{seq}"
                event["rev"] = f"rev{seq}"
                event["payload"]["text"] = f"new post {seq}"
                new_events.append(event)
            service.events.extend(new_events)
            service.data, service.checksum = segment_bytes(service.events)
            service.tip = 9
    assert collected == list(range(1, 8))
    assert checkpoint["seq"] == 7
    later_rows, later_checkpoint = consume(make_connector(config), checkpoint)
    assert [row["seq"] for row in later_rows] == [8, 9]
    assert later_checkpoint["seq"] == 9


def test_batch_logs_report_sequence_lag_without_credentials(service, caplog):
    caplog.set_level("INFO")
    connector = make_connector({"api_key": "private-token", "max_events_per_refresh": "2"})

    connector.latest_offset("events", {}, {})

    assert "after=0 through=2 target=7 sequence_lag=5" in caplog.text
    assert "private-token" not in caplog.text


def test_exact_filters_keep_account_markers(service):
    rows, end = consume(make_connector({"collections": "app.bsky.feed.post"}))
    assert [r["seq"] for r in rows] == list(range(1, 7))
    assert end["seq"] == 7
    rows, end = consume(make_connector({"dids": "did:plc:unknown"}))
    assert rows == [] and end["seq"] == 7


def test_known_did_and_collection_namespace_include_matches_only(service):
    rows, end = consume(
        make_connector({"dids": "did:plc:alice", "kinds": "commit,identity,account,sync"})
    )
    assert [row["seq"] for row in rows] == [1, 2, 3, 4, 5, 6]
    assert end["seq"] == 7

    rows, _ = consume(make_connector({"collections": "app.bsky.feed.*", "kinds": "commit"}))
    assert [row["collection"] for row in rows] == [
        "app.bsky.feed.post",
        "app.bsky.feed.post",
        "app.bsky.feed.post",
        "app.bsky.feed.like",
    ]


def test_kinds_filter_is_sent_to_planner_and_applied_to_decoded_rows(service):
    rows, end = consume(make_connector({"kinds": "identity,account"}))
    assert [row["kind"] for row in rows] == ["identity", "account"]
    assert end["seq"] == 7
    plans = [json.loads(request.body) for request in service.calls if request.method == "POST"]
    assert plans
    assert all(plan["kinds"] == ["account", "identity"] for plan in plans)


def test_collections_require_commit_kind(service):
    with pytest.raises(ValueError, match="collections.*kinds includes commit"):
        make_connector({"collections": "app.bsky.feed.post", "kinds": "identity"})
    rows, _ = consume(
        make_connector({"collections": "app.bsky.feed.post", "kinds": "commit,identity"})
    )
    assert all(row["kind"] in ("commit", "identity") for row in rows)


def test_namespace_filter_and_raw_disabled(service):
    rows, _ = consume(
        make_connector({"collections": "app.bsky.feed.*", "include_raw_payload": "false"})
    )
    assert len(rows) == 7
    assert all(r["raw_payload"] is None for r in rows)


def test_start_cursor_exclusive_and_input_not_mutated(service):
    config = {"starting_cursor": "3", "max_events_per_refresh": "2"}
    _, checkpoint = consume(make_connector(config))
    saved = copy.deepcopy(checkpoint)
    rows, end = consume(make_connector(config), checkpoint)
    assert checkpoint == saved
    assert [r["seq"] for r in rows] == [6, 7] and end["seq"] == 7


def test_checkpoint_scope_and_regressed_tip(service):
    _, end = consume(make_connector({}))
    with pytest.raises(ValueError, match="changed"):
        consume(make_connector({"collections": "app.bsky.feed.post"}), end)
    with pytest.raises(ValueError, match="changed"):
        consume(make_connector({"kinds": "commit"}), end)
    service.tip = 5
    with pytest.raises(CursorTooOld):
        consume(make_connector({}), end)


def test_spark_retry_uses_recorded_end_even_if_tip_grows(service):
    connector = make_connector({"max_events_per_refresh": "2"})
    end = connector.latest_offset("events", {}, {})
    fresh = make_connector({"max_events_per_refresh": "2"})
    partition = fresh.get_partitions("events", {}, {}, end)[0]
    service.tip = 99
    first = list(fresh.read_partition("events", partition, {}))
    retry = list(fresh.read_partition("events", partition, {}))
    assert first == retry and [r["seq"] for r in first] == [1, 2]
    assert len([r for r in service.calls if r.method == "POST"]) == 2


def test_pending_v1_checkpoint_migrates_without_replanning(service):
    fixture_root = Path(__file__).parent / "fixtures" / "checkpoints"
    pending_v1 = json.loads((fixture_root / "v1-pending-offset.json").read_text())
    saved_pages = json.loads(pending_v1["plan"])
    saved_pages[0]["segments"][0]["checksum"] = service.checksum
    pending_v1["plan"] = json.dumps(saved_pages, separators=(",", ":"))
    request_count = len([request for request in service.calls if request.method == "POST"])

    restored = make_connector({"max_events_per_refresh": "2"})
    partition = restored.get_partitions("events", {}, {}, pending_v1)[0]
    rows = list(restored.read_partition("events", partition, {}))

    assert [row["seq"] for row in rows] == [1, 2]
    assert len([request for request in service.calls if request.method == "POST"]) == request_count


def test_committed_v1_checkpoint_starts_a_new_plan(service):
    fixture_path = Path(__file__).parent / "fixtures" / "checkpoints" / "v1-committed-offset.json"
    committed = json.loads(fixture_path.read_text())
    connector = make_connector({"max_events_per_refresh": "2"})

    rows, end = consume(connector, committed)

    assert [row["seq"] for row in rows] == [3, 4]
    assert end["version"] == 2 and end["after"] == 2


def test_checkpoint_replay_survives_a_python_process_restart(tmp_path):
    """Restore pending offsets in a new interpreter without replanning the batch."""
    fixture_root = Path(__file__).parent / "fixtures" / "checkpoints"
    event_fixture = Path(__file__).parent / "fixtures" / "events.json"
    pending_fixture = fixture_root / "v1-pending-offset.json"
    committed_fixture = fixture_root / "v1-committed-offset.json"
    state_path = tmp_path / "offsets.json"
    result_path = tmp_path / "replay.json"
    failure_path = tmp_path / "failure.json"
    script = r"""import json, pathlib, requests, sys
from conftest import ArchiveService
from databricks.labs.community_connector.sources.bluesky.bluesky import BlueskyLakeflowConnect
from databricks.labs.community_connector.sources.bluesky.options import Options

action, event_path, pending_path, committed_path, state_path, result_path = sys.argv[1:]
events = json.loads(pathlib.Path(event_path).read_text())
service = ArchiveService(events)
requests.Session.send = lambda self, request, **kwargs: service.send(request)
options = {"max_events_per_refresh": "2"}
if action == "produce":
    connector = BlueskyLakeflowConnect(Options.parse(options, {}))
    end = connector.latest_offset("events", {}, {})
    state = {
        "new_pending": end,
        "legacy_pending": json.loads(pathlib.Path(pending_path).read_text()),
        "legacy_committed": json.loads(pathlib.Path(committed_path).read_text()),
    }
    pages = json.loads(state["legacy_pending"]["plan"])
    pages[0]["segments"][0]["checksum"] = service.checksum
    state["legacy_pending"]["plan"] = json.dumps(pages, separators=(",", ":"))
    pathlib.Path(state_path).write_text(json.dumps(state, separators=(",", ":")))
elif action == "consume":
    state = json.loads(pathlib.Path(state_path).read_text())
    restored = {}
    for name in ("legacy_pending", "new_pending", "legacy_committed"):
        before = len(service.calls)
        connector = BlueskyLakeflowConnect(Options.parse(options, {}))
        if name == "legacy_committed":
            end = connector.latest_offset("events", {}, state[name])
            partition = connector.get_partitions("events", {}, state[name], end)[0]
        else:
            partition = connector.get_partitions("events", {}, {}, state[name])[0]
        rows = list(connector.read_partition("events", partition, {}))
        calls = service.calls[before:]
        restored[name] = {
            "seqs": [row["seq"] for row in rows],
            "planner_requests": sum(call.method == "POST" for call in calls),
            "start": partition["start"],
            "end": partition["end"]["seq"],
        }
    pathlib.Path(result_path).write_text(json.dumps(restored, sort_keys=True))
elif action == "failure":
    from databricks.labs.community_connector.sources.bluesky.errors import ArchiveChanged

    state = json.loads(pathlib.Path(state_path).read_text())
    pending = state["legacy_pending"]
    before = len(service.calls)
    scope_mismatch = False
    try:
        connector = BlueskyLakeflowConnect(
            Options.parse({"max_events_per_refresh": "2", "kinds": "account"}, {})
        )
        connector.get_partitions("events", {}, {}, pending)
    except ValueError:
        scope_mismatch = True
    scope_calls = service.calls[before:]

    service.checksum = "0000000000000001"
    before = len(service.calls)
    archive_changed = False
    try:
        connector = BlueskyLakeflowConnect(Options.parse(options, {}))
        partition = connector.get_partitions("events", {}, {}, pending)[0]
        list(connector.read_partition("events", partition, {}))
    except ArchiveChanged:
        archive_changed = True
    generation_calls = service.calls[before:]
    pathlib.Path(result_path).write_text(
        json.dumps(
            {
                "scope_mismatch": scope_mismatch,
                "scope_planner_requests": sum(call.method == "POST" for call in scope_calls),
                "archive_changed": archive_changed,
                "generation_planner_requests": sum(
                    call.method == "POST" for call in generation_calls
                ),
                "replacement_offset": False,
            },
            sort_keys=True,
        )
    )
else:
    raise AssertionError("unknown subprocess action")
"""
    helper_path = str(Path(__file__).parent)
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        value for value in (helper_path, env.get("PYTHONPATH", "")) if value
    )
    args = [
        sys.executable,
        "-c",
        script,
        "produce",
        str(event_fixture),
        str(pending_fixture),
        str(committed_fixture),
        str(state_path),
        str(result_path),
    ]
    subprocess.run(args, cwd=tmp_path, env=env, check=True, capture_output=True, text=True)
    args[3] = "consume"
    subprocess.run(args, cwd=tmp_path, env=env, check=True, capture_output=True, text=True)
    restored = json.loads(result_path.read_text())
    args[3] = "failure"
    args[8] = str(failure_path)
    subprocess.run(args, cwd=tmp_path, env=env, check=True, capture_output=True, text=True)

    assert restored["legacy_pending"] == {
        "seqs": [1, 2],
        "planner_requests": 0,
        "start": 0,
        "end": 2,
    }
    assert restored["new_pending"] == {"seqs": [1, 2], "planner_requests": 0, "start": 0, "end": 2}
    assert restored["legacy_committed"] == {
        "seqs": [3, 4],
        "planner_requests": 2,
        "start": 2,
        "end": 4,
    }
    failures = json.loads(failure_path.read_text())
    assert failures == {
        "archive_changed": True,
        "generation_planner_requests": 0,
        "replacement_offset": False,
        "scope_mismatch": True,
        "scope_planner_requests": 0,
    }

    persisted = json.loads(state_path.read_text())
    fixture_events = json.loads(event_fixture.read_text())
    from tests.unit.sources.bluesky.conftest import segment_bytes

    _, checksum = segment_bytes(fixture_events)
    expected_scope = Options.parse({"max_events_per_refresh": "2"}, {}).fingerprint()
    for name in ("legacy_pending", "new_pending", "legacy_committed"):
        assert persisted[name]["scope"] == expected_scope
    saved_plan = persisted["new_pending"]["plan"]
    descriptors = [
        descriptor for page in json.loads(saved_plan)["pages"] for descriptor in page["segments"]
    ]
    assert descriptors and {descriptor["checksum"] for descriptor in descriptors} == {checksum}


def test_compaction_fails_recorded_retry(service):
    connector = make_connector({})
    end = connector.latest_offset("events", {}, {})
    partition = connector.get_partitions("events", {}, {}, end)[0]
    service.checksum = "0000000000000001"
    with pytest.raises(ArchiveChanged):
        list(connector.read_partition("events", partition, {}))


@pytest.mark.parametrize(
    "damage",
    ["missing_end", "missing_start", "missing_page", "missing_segments", "boolean_boundary"],
)
def test_malformed_saved_partition_fails_as_protocol_error(service, damage):
    connector = make_connector({})
    end = connector.latest_offset("events", {}, {})
    partition = connector.get_partitions("events", {}, {}, end)[0]
    partition = copy.deepcopy(partition)
    if damage == "missing_end":
        del partition["end"]
    elif damage == "missing_start":
        del partition["start"]
    else:
        saved = json.loads(partition["end"]["plan"])
        pages = saved["pages"]
        if damage == "missing_page":
            pages[0] = None
        elif damage == "missing_segments":
            del pages[0]["segments"]
        else:
            pages[0]["afterSeq"] = False
        partition["end"]["plan"] = json.dumps(saved)

    with pytest.raises(ProtocolError):
        list(connector.read_partition("events", partition, {}))


@pytest.mark.parametrize(
    "override",
    [
        lambda p: p.update(plannedThroughSeq=0),
        lambda p: p.update(plannedThroughSeq=999),
        lambda p: p.update(sealedTipSeq="7"),
        lambda p: p["segments"][0].update(mode="unknown"),
    ],
)
def test_bad_plans_fail_closed(service, override):
    service.plan_override = override
    with pytest.raises(ProtocolError):
        consume(make_connector({}))


def test_spark_adapter_end_to_end(service):
    from pyspark.sql.datasource import CaseInsensitiveDict
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    from databricks.labs.community_connector.sparkpds.lakeflow_datasource import (
        LakeflowPartitionedStreamReader,
    )

    source = BlueskyDataSource(
        CaseInsensitiveDict(
            {
                "tableName": "events",
                "sourceName": "bluesky",
                "max_events_per_refresh": "2",
            }
        )
    )
    reader = source.streamReader(source.schema())
    assert isinstance(reader, LakeflowPartitionedStreamReader)
    start = reader.initialOffset()
    rows, end = drain_framework_run(reader, start, ReadAllAvailable(), service)
    assert [row.seq for row in rows] == list(range(1, 8))
    assert reader.latestOffset(end, ReadAllAvailable()) == end
    assert reader.partitions(end, end) == []

    later_source = BlueskyDataSource(
        CaseInsensitiveDict(
            {
                "tableName": "events",
                "sourceName": "bluesky",
                "max_events_per_refresh": "2",
            }
        )
    )
    later_reader = later_source.streamReader(later_source.schema())
    later_rows, later_end = drain_framework_run(later_reader, end, ReadAllAvailable())
    assert [row.seq for row in later_rows] == [8, 9]
    assert later_end != end


def test_generated_single_file_registration_reads_archive(service):
    """The upstream SDP bundle must use the same validated source contract."""
    from pyspark import cloudpickle
    from pyspark.sql.datasource import CaseInsensitiveDict
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    from databricks.labs.community_connector.sources.bluesky import (
        _generated_bluesky_python_source as generated,
    )

    class Registry:
        source = None

        def register(self, source):
            cloudpickle.dumps(source)
            self.source = source

    class DriverOnly:
        def __reduce__(self):
            raise TypeError("driver-only state must not be serialized")

    class Spark:
        dataSource = Registry()
        driver_only = DriverOnly()

    generated.register_lakeflow_source(Spark())
    source = Spark.dataSource.source(
        CaseInsensitiveDict({"tableName": "events", "max_events_per_refresh": "2"})
    )
    reader = source.streamReader(source.schema())
    start = reader.initialOffset()
    rows, end = drain_framework_run(reader, start, ReadAllAvailable(), service)
    assert [row.seq for row in rows] == list(range(1, 8))
    assert reader.latestOffset(end, ReadAllAvailable()) == end
    later_source = Spark.dataSource.source(
        CaseInsensitiveDict({"tableName": "events", "max_events_per_refresh": "2"})
    )
    later_reader = later_source.streamReader(later_source.schema())
    later_rows, later_end = drain_framework_run(later_reader, end, ReadAllAvailable())
    assert [row.seq for row in later_rows] == [8, 9]
    assert later_end != end


def test_empty_archive_terminates(service):
    service.tip = 0
    assert consume(make_connector({}), {}) == ([], {})


def test_no_matching_records_still_checkpoint_progress(service):
    connector = make_connector({"dids": "did:plc:nobody"})
    rows, end = consume(connector)
    assert rows == [] and end["seq"] == 7
    assert consume(connector, end) == ([], end)


def test_corrupt_footer_never_returns_candidate_offset(service):
    service.data = service.data[:-1] + bytes([service.data[-1] ^ 1])
    checkpoint = {}
    with pytest.raises(ProtocolError, match="checksum"):
        _, checkpoint = consume(make_connector({}), checkpoint)
    assert checkpoint == {}


def test_direct_read_memory_budget(service, monkeypatch):
    monkeypatch.setattr(
        "databricks.labs.community_connector.sources.bluesky.bluesky.MAX_BATCH_BYTES", 1024
    )
    with pytest.raises(ProtocolError, match="memory budget"):
        consume(make_connector({}))


def test_plan_generation_can_change_after_committed_range(service):
    config = {"max_events_per_refresh": "2"}
    _, checkpoint = consume(make_connector(config))
    # Changes are safe for a NEW plan after a committed range, never for its retry.
    from tests.unit.sources.bluesky.conftest import segment_bytes

    service.events[0]["payload"]["text"] = "changed fixture generation"
    service.data, service.checksum = segment_bytes(service.events)
    rows, _ = consume(make_connector(config), checkpoint)
    assert [r["seq"] for r in rows] == [3, 4]
