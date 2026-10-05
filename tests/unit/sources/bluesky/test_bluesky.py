import copy
import json

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
        {"typo": "secret"},
        {"starting_cursor": True},
    ],
)
def test_invalid_options(options):
    with pytest.raises(ValueError):
        make_connector(options)


def test_connector_accepts_framework_mapping_and_validates_it():
    assert isinstance(BlueskyLakeflowConnect({}).config, Options)
    with pytest.raises(ValueError, match="Unsupported connector option names"):
        BlueskyLakeflowConnect({"typo": "value"})
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


def test_base36_segment_name_from_live_snapshot():
    ArchiveValidator.validate_segments(
        [
            {
                "name": "seg_000000061t.jss",
                "index": 7841,
                "checksum": "f3779a8a5df3af0d",
                "minSeq": 26552356873,
                "maxSeq": 26553427893,
                "mode": "blocks",
                "blocks": [{"first": 261, "last": 261}],
            }
        ]
    )


@pytest.mark.parametrize("mode", ["segment", "blocks"])
def test_pagination_and_duplicate_boundary(service, mode):
    service.mode = mode
    connector = make_connector({})
    rows, end = consume(connector)
    assert [r["seq"] for r in rows] == list(range(1, 8))
    plans = [json.loads(r.body) for r in service.calls if r.method == "POST"]
    assert [p["afterSeq"] for p in plans] == [0, 3, 6]
    assert [p["beforeSeq"] for p in plans] == [10000, 7, 7]
    assert all(p["kinds"] == ["account", "commit", "identity", "sync"] for p in plans)
    assert consume(connector, end) == ([], end)


def test_refresh_bound_and_resume_new_process(service):
    config = {"max_events_per_refresh": "2"}
    checkpoint = None
    collected = []
    for _ in range(4):
        connector = make_connector(config)
        rows, checkpoint = consume(connector, checkpoint)
        collected.extend(r["seq"] for r in rows)
        assert len(rows) <= 2
        assert consume(connector, checkpoint) == ([], checkpoint)
    assert collected == list(range(1, 8))


def test_exact_filters_keep_account_markers(service):
    rows, end = consume(make_connector({"collections": "app.bsky.feed.post"}))
    assert [r["seq"] for r in rows] == list(range(1, 7))
    assert end["seq"] == 7
    rows, end = consume(make_connector({"dids": "did:plc:unknown"}))
    assert rows == [] and end["seq"] == 7


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
    assert len([r for r in service.calls if r.method == "POST"]) == 1


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
        pages = json.loads(partition["end"]["plan"])
        if damage == "missing_page":
            pages[0] = None
        elif damage == "missing_segments":
            del pages[0]["segments"]
        else:
            pages[0]["after"] = False
        partition["end"]["plan"] = json.dumps(pages)

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
        CaseInsensitiveDict({"tableName": "events", "sourceName": "bluesky"})
    )
    reader = source.streamReader(source.schema())
    assert isinstance(reader, LakeflowPartitionedStreamReader)
    start = reader.initialOffset()
    end = reader.latestOffset(start, ReadAllAvailable())
    rows = [row for partition in reader.partitions(start, end) for row in reader.read(partition)]
    assert [r.seq for r in rows] == list(range(1, 8))
    assert reader.latestOffset(end, ReadAllAvailable()) == end
    assert reader.partitions(end, end) == []


def test_generated_single_file_registration_reads_archive(service):
    """The upstream SDP bundle must use the same validated source contract."""
    from pyspark.sql.datasource import CaseInsensitiveDict
    from pyspark.sql.streaming.datasource import ReadAllAvailable

    from databricks.labs.community_connector.sources.bluesky import (
        _generated_bluesky_python_source as generated,
    )

    class Registry:
        source = None

        def register(self, source):
            self.source = source

    class Spark:
        dataSource = Registry()

    generated.register_lakeflow_source(Spark())
    source = Spark.dataSource.source(CaseInsensitiveDict({"tableName": "events"}))
    reader = source.streamReader(source.schema())
    start = reader.initialOffset()
    end = reader.latestOffset(start, ReadAllAvailable())
    rows = [row for partition in reader.partitions(start, end) for row in reader.read(partition)]
    assert [row.seq for row in rows] == list(range(1, 8))
    assert reader.latestOffset(end, ReadAllAvailable()) == end


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
        "databricks.labs.community_connector.sources.bluesky.bluesky.MAX_BATCH_BYTES", 1
    )
    with pytest.raises(ProtocolError, match="memory budget"):
        consume(make_connector({}))


def test_plan_generation_can_change_after_committed_range(service):
    config = {"max_events_per_refresh": "2"}
    _, checkpoint = consume(make_connector(config))
    # Changes are safe for a NEW plan after a committed range, never for its retry.
    from conftest import segment_bytes

    service.events[0]["payload"]["text"] = "changed fixture generation"
    service.data, service.checksum = segment_bytes(service.events)
    rows, _ = consume(make_connector(config), checkpoint)
    assert [r["seq"] for r in rows] == [3, 4]
