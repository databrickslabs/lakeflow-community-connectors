"""Tests for the Apis REST Service (Hive historian) LakeflowConnect connector.

Runs offline against the in-process source simulator described by
``source_simulator/specs/apis/`` (``endpoints.yaml`` + a corpus bootstrapped
from ``TABLE_SCHEMAS`` and reshaped to the API's wire shape). No credentials
and no network access are involved: the stand-in ``replay_config`` below is
never validated, because the simulator ignores the ``Authorization`` header
the same way it ignores the rest of the auth model.

``TestApisConnector`` inherits the full shared suite twice over:

* ``LakeflowConnectTests`` — discovery, schemas, metadata, invalid-table
  handling.
* ``SupportsPartitionedStreamTests`` — partition discovery/determinism/
  serializability, ``latest_offset``, micro-batch convergence and the
  partitioned read round-trip. All three tables report ``is_partitioned ==
  True``, so the partition mixin (not ``read_table``) is what actually
  exercises the read paths here; the suite's ``read_table`` tests skip
  themselves.

The per-table methods on the class add what the generic suite cannot know:
that ``items`` derives ``module`` client-side, that ``values`` and
``timeseries`` flatten the declared ``single_value_t`` ``(v, q, t)`` triple
onto ``value``/``quality``/``timestamp``, that the ``timeseries`` window is
half-open, and that the init-time cap keeps future rows out.

NOTE ON PAGINATION: the source defines no ``limit``/``offset``/``cursor``/
``nextPageToken`` parameter and no ``Link`` header on any operation, so there
is deliberately no pagination test here and none in the spec. Scope is
narrowed with the ``item`` wildcard and the time window instead. That is a
real limitation of the API, not a gap in this suite — see the header comments
in ``endpoints.yaml`` and ``apis_schemas.py``.
"""

from __future__ import annotations

import pytest

from databricks.labs.community_connector.sources.apis.apis import (
    ApisLakeflowConnect,
    _normalize_items,
    _normalize_timeseries,
    _normalize_values,
)
from tests.unit.sources.test_partition_suite import (
    SupportsPartitionedStreamTests,
)
from tests.unit.sources.test_suite import LakeflowConnectTests

#: The single Hive instance every corpus record names, and therefore the one
#: ``GET /hive`` discovery resolves. Kept in sync with
#: ``specs/apis/corpus/items.json``.
SIM_INSTANCE = "ApisHive"

#: Wire timestamps (``single_value_t.value.t``) in the corpus, in order.
#: Space-separated, no timezone suffix — CONFIRMED live (2026-09-23) as the
#: source's real format, not the ISO-8601 originally assumed here.
VALUES_TIMESTAMPS = [
    "2024-01-01 00:00:00",
    "2024-01-01 01:00:00",
    "2024-01-01 02:00:00",
    "2024-01-01 03:00:00",
    "2024-01-01 04:00:00",
]
TIMESERIES_TIMESTAMPS = [
    "2024-01-01 00:00:00",
    "2024-01-01 00:15:00",
    "2024-01-01 00:30:00",
    "2024-01-01 00:45:00",
    "2024-01-01 01:00:00",
]


class TestApisConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = ApisLakeflowConnect
    simulator_source = "apis"
    sample_records = 50

    # Stand-in credentials. The connector only needs a base URL and a bearer
    # token; the simulator validates neither. The ``item`` table option is
    # deliberately left at its ``*`` default everywhere below: the
    # declarative filter pipeline has no glob operator, so a narrower pattern
    # would still return the whole corpus slice and only inflate row counts.
    replay_config = {
        "base_url": "http://simulator-hive.example.com:8080",
        "token": "simulator-fake-token",
    }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_all(self, table: str, table_options: dict | None = None) -> list[dict]:
        """Read every partition of ``table`` and return the concatenated rows."""
        opts = table_options if table_options is not None else self._opts(table)
        rows: list[dict] = []
        for partition in self.connector.get_partitions(table, opts):
            rows.extend(self.connector.read_partition(table, partition, opts))
        return rows

    # ------------------------------------------------------------------
    # instance discovery — GET /hive is a lookup, not a table
    # ------------------------------------------------------------------

    def test_instance_discovery_is_not_a_table(self):
        """``/hive`` resolves the ``{instance}`` path segment and nothing else."""
        assert "hive" not in self.connector.list_tables()
        # No instance pinned in table options and none in the connection
        # options, so this exercises the GET /hive lookup.
        assert self.connector._resolve_instances({}) == [SIM_INSTANCE]

    def test_instance_option_short_circuits_discovery(self):
        """A pinned table option wins over discovery without any HTTP call."""
        assert self.connector._resolve_instances({"instance": "OtherHive"}) == [
            "OtherHive"
        ]

    # ------------------------------------------------------------------
    # items (snapshot)
    # ------------------------------------------------------------------

    def test_items_rows(self):
        """``items`` re-lists the catalog and derives ``module`` client-side."""
        rows = self._read_all("items")
        assert rows, "items produced no rows"

        for row in rows:
            # ``instance`` is not in any response body — it is stamped from
            # the ``{instance}`` path segment.
            assert row["instance"] == SIM_INSTANCE
            assert row["item_name"]
            # ``module`` is derived by splitting on the first '.', the
            # ``<Module>.<Item>`` convention the source's own examples use.
            assert row["module"] == row["item_name"].split(".", 1)[0]
            assert row["attributes"], f"no attributes on {row['item_name']}"
            for attribute in row["attributes"]:
                assert attribute["attrib_id"] is not None
                assert attribute["value"] is not None

        # Snapshot: primary keys are unique across the full re-list.
        keys = [(row["instance"], row["item_name"]) for row in rows]
        assert len(keys) == len(set(keys)), f"duplicate item PKs: {keys}"

    def test_items_partitions_fan_out_on_instance_and_pattern(self):
        """One partition per (instance x item pattern); no cursor involved."""
        partitions = self.connector.get_partitions("items", {})
        assert partitions == [{"instance": SIM_INSTANCE, "item": "*"}]
        for partition in partitions:
            assert "starttime" not in partition
            assert "updated_since" not in partition

    def test_items_snapshot_offset_is_empty(self):
        """``read_table`` on a snapshot table carries no cursor."""
        _, offset = self.connector.read_table("items", {}, {})
        assert offset == {}

    # ------------------------------------------------------------------
    # values (cdc, upsert-only)
    # ------------------------------------------------------------------

    def test_values_rows_are_flattened(self):
        """The declared ``value: {v,q,t}`` triple lands on flat columns."""
        rows = self._read_all("values")
        assert len(rows) == len(VALUES_TIMESTAMPS)

        for row in rows:
            assert row["instance"] == SIM_INSTANCE
            assert row["item_name"]
            # Flattened, not nested: ``value`` is the scalar point value.
            assert not isinstance(row["value"], dict)
            assert row["value"] is not None
            assert row["quality"] is not None
            assert row["timestamp"] is not None
            assert set(row) == {
                "instance",
                "item_name",
                "value",
                "quality",
                "timestamp",
            }

        assert sorted(row["timestamp"] for row in rows) == VALUES_TIMESTAMPS

        # CDC upserts on (instance, item_name), so the key must be unique
        # within a batch.
        keys = [(row["instance"], row["item_name"]) for row in rows]
        assert len(keys) == len(set(keys)), f"duplicate value PKs: {keys}"

    def test_values_cursor_field_matches_flattened_column(self):
        metadata = self.connector.read_table_metadata("values", {})
        assert metadata["cursor_field"] == "timestamp"
        assert metadata["ingestion_type"] == "cdc"
        rows = self._read_all("values")
        assert all(metadata["cursor_field"] in row for row in rows)

    def test_values_updated_since_narrows_the_batch(self):
        """``updatedSince`` is the source's one genuine incremental signal."""
        since = VALUES_TIMESTAMPS[2]
        partitions = self.connector.get_partitions(
            "values", {}, start_offset={"cursor": since}, end_offset=None
        )
        assert partitions and all(p["updated_since"] == since for p in partitions)

        rows: list[dict] = []
        for partition in partitions:
            rows.extend(self.connector.read_partition("values", partition, {}))

        # Lower-bounded only — there is no "updatedUntil" upstream.
        assert [row["timestamp"] for row in rows] == VALUES_TIMESTAMPS[2:]

    def test_values_read_table_offset_is_the_init_time_cap(self):
        """The offset is the cap, not a record-derived max timestamp.

        The endpoint returns one row per item with no ordering guarantee and
        no upper time bound, so there is no reliable cursor to derive from
        the records themselves.
        """
        records, offset = self.connector.read_table("values", {}, {})
        records = list(records)
        assert records
        assert offset == {"cursor": self.connector._init_time}

        # Feeding the offset back converges without touching the API.
        records2, offset2 = self.connector.read_table("values", offset, {})
        assert list(records2) == []
        assert offset2 == offset

    # ------------------------------------------------------------------
    # timeseries (append)
    # ------------------------------------------------------------------

    def test_timeseries_rows_are_flattened(self):
        rows = self._read_all("timeseries")
        assert len(rows) == len(TIMESERIES_TIMESTAMPS)
        for row in rows:
            assert row["instance"] == SIM_INSTANCE
            assert row["item_name"]
            assert not isinstance(row["value"], dict)
            assert row["value"] is not None
            assert row["quality"] is not None
            assert row["timestamp"] is not None
        assert sorted(row["timestamp"] for row in rows) == TIMESERIES_TIMESTAMPS

    def test_timeseries_partitions_are_contiguous_and_non_overlapping(self):
        """Append-only rows must land in exactly one window."""
        partitions = self.connector.get_partitions("timeseries", {})
        assert len(partitions) > 1
        for previous, current in zip(partitions, partitions[1:]):
            assert previous["endtime"] == current["starttime"]
        assert partitions[-1]["endtime"] == self.connector._init_time

    def test_timeseries_window_is_half_open(self):
        """``[starttime, endtime)`` — a row exactly on the upper bound is out.

        This is what keeps contiguous windows from double-counting a boundary
        row on an append-only table. If a live run shows the server treats
        ``endtime`` as inclusive, this test is where that shows up first.
        """
        partition = {
            "instance": SIM_INSTANCE,
            "item": "*",
            "starttime": TIMESERIES_TIMESTAMPS[0],
            "endtime": TIMESERIES_TIMESTAMPS[2],
        }
        rows = list(self.connector.read_partition("timeseries", partition, {}))
        assert [row["timestamp"] for row in rows] == TIMESERIES_TIMESTAMPS[:2]

    def test_timeseries_respects_the_init_time_cap(self):
        """Rows past the cap are excluded, so the trigger can converge.

        The spec seeds three records dated past wall-clock ``now()``
        (``synthesize_future_records``). They must never be read: the
        ``endtime`` bound is the connector's construction-time instant.
        """
        cap = self.connector._init_time
        rows = self._read_all("timeseries")
        assert rows
        assert all(row["timestamp"] < cap for row in rows)
        assert len(rows) == len(TIMESERIES_TIMESTAMPS)

    def test_timeseries_read_table_walks_windows_and_converges(self):
        """The driver fallback for the one table the shared suite cannot reach.

        ``is_partitioned`` is ``True`` for all three tables, so the suite's
        ``test_read_table`` / ``test_read_terminates`` skip themselves. This
        covers the same contract for ``timeseries``, the only table whose
        ``read_table`` path is a sliding-window walk rather than a single
        call: the cursor must advance to the *window boundary* (not the last
        record's timestamp) so an empty window still moves forward, and the
        walk must converge on the init-time cap.
        """
        opts = {
            # Start just before the corpus so the first call returns rows
            # instead of walking empty windows for years.
            "start_timestamp": "2023-12-31T00:00:00Z",
            "window_seconds": "86400",
        }
        records, offset = self.connector.read_table("timeseries", {}, opts)
        records = list(records)
        assert [row["timestamp"] for row in records] == TIMESERIES_TIMESTAMPS
        # Advanced to a window boundary, not to the newest record.
        assert offset["cursor"] > TIMESERIES_TIMESTAMPS[-1]

        for _ in range(self.read_termination_max_iterations):
            batch, next_offset = self.connector.read_table("timeseries", offset, opts)
            list(batch)
            if next_offset == offset:
                break
            offset = next_offset
        else:
            pytest.fail(f"read_table did not converge (last offset {offset})")

        assert offset == {"cursor": self.connector._init_time}

    def test_timeseries_append_declares_no_primary_keys(self):
        metadata = self.connector.read_table_metadata("timeseries", {})
        assert metadata["ingestion_type"] == "append"
        assert metadata["primary_keys"] == []
        assert metadata["cursor_field"] == "timestamp"

    # ------------------------------------------------------------------
    # no-pagination contract
    # ------------------------------------------------------------------

    def test_no_pagination_parameters_are_ever_sent(self):
        """The source documents no continuation mechanism; don't invent one."""
        seen: list[dict] = []
        original = self.connector._get_json

        def _capture(path, params):
            seen.append(dict(params))
            return original(path, params)

        self.connector._get_json = _capture  # type: ignore[method-assign]
        try:
            for table in self.connector.list_tables():
                self._read_all(table)
        finally:
            self.connector._get_json = original  # type: ignore[method-assign]

        assert seen
        forbidden = {
            "limit",
            "offset",
            "cursor",
            "page",
            "pageSize",
            "page_size",
            "nextPageToken",
            "continuationToken",
            "skip",
            "top",
        }
        for params in seen:
            assert not forbidden & set(params), f"pagination param leaked: {params}"


# ---------------------------------------------------------------------------
# Pure-function tests — no HTTP, so they live outside the simulator context
# ---------------------------------------------------------------------------


def _connector(**overrides) -> ApisLakeflowConnect:
    options = {
        "base_url": "http://hive-host:8080/",
        "token": "fake-token",
        "instance": "PinnedHive",
    }
    options.update(overrides)
    return ApisLakeflowConnect(options)


def test_missing_base_url_is_rejected():
    with pytest.raises(ValueError, match="base_url"):
        ApisLakeflowConnect({"token": "fake-token"})


def test_missing_token_is_rejected():
    with pytest.raises(ValueError, match="token"):
        ApisLakeflowConnect({"base_url": "http://hive-host:8080"})


def test_bearer_scheme_defaults_and_can_be_disabled():
    assert _connector()._headers()["Authorization"] == "Bearer fake-token"
    # The spec models Authorization as a raw apiKey header, so operators must
    # be able to send the bare token.
    assert _connector(auth_scheme="none")._headers()["Authorization"] == "fake-token"


def test_unknown_table_is_rejected_everywhere():
    connector = _connector()
    for call in (
        lambda: connector.get_table_schema("nope", {}),
        lambda: connector.read_table_metadata("nope", {}),
        lambda: connector.latest_offset("nope", {}),
        lambda: connector.get_partitions("nope", {}),
        lambda: connector.read_table("nope", {}, {}),
    ):
        with pytest.raises(ValueError, match="not supported"):
            call()


def test_quality_and_aggregate_values_are_validated_locally():
    """Fail fast with a useful message instead of a bare HTTP 400."""
    connector = _connector()
    with pytest.raises(ValueError, match="quality"):
        connector.read_partition(
            "values", {"instance": "PinnedHive", "item": "*"}, {"quality": "excellent"}
        ).__next__()
    with pytest.raises(ValueError, match="aggregate"):
        connector.read_partition(
            "timeseries",
            {
                "instance": "PinnedHive",
                "item": "*",
                "starttime": "2024-01-01T00:00:00Z",
                "endtime": "2024-01-02T00:00:00Z",
            },
            {"aggregate": "mean"},
        ).__next__()


def test_start_timestamp_option_bounds_the_first_timeseries_run():
    connector = _connector()
    partitions = connector.get_partitions(
        "timeseries", {"start_timestamp": "2024-01-01T00:00:00Z"}
    )
    assert partitions[0]["starttime"] == "2024-01-01T00:00:00Z"
    assert partitions[-1]["endtime"] == connector._init_time


def test_max_partitions_widens_windows_instead_of_exploding_task_count():
    connector = _connector()
    partitions = connector.get_partitions(
        "timeseries",
        {
            "start_timestamp": "2024-01-01T00:00:00Z",
            "window_seconds": "60",
            "max_partitions": "4",
        },
    )
    assert len(partitions) <= 4


def test_equal_offsets_produce_no_partitions_for_every_table():
    connector = _connector()
    offset = {"cursor": connector._init_time}
    for table in connector.list_tables():
        assert (
            connector.get_partitions(table, {}, start_offset=offset, end_offset=offset)
            == []
        )


def test_normalize_items_tolerates_bare_name_strings():
    """The items response shape is INFERRED; a bare-string array is plausible."""
    rows = _normalize_items("H1", ["Worker.Signal1", "Unqualified"])
    assert [r["item_name"] for r in rows] == ["Worker.Signal1", "Unqualified"]
    assert rows[0]["module"] == "Worker"
    # No '.' means no module can be derived — best-effort, so nullable.
    assert rows[1]["module"] is None
    assert rows[0]["attributes"] is None


def test_normalize_values_accepts_the_declared_and_inlined_shapes():
    declared = _normalize_values(
        "H1",
        [{"item_name": "Worker.Signal1", "value": {"v": 42.7, "q": "good", "t": "T0"}}],
    )
    inlined = _normalize_values(
        "H1", [{"item_name": "Worker.Signal1", "v": 42.7, "q": "good", "t": "T0"}]
    )
    assert declared == inlined
    assert declared[0] == {
        "instance": "H1",
        "item_name": "Worker.Signal1",
        "value": 42.7,
        "quality": "good",
        "timestamp": "T0",
    }


def test_normalize_timeseries_accepts_bundled_and_flat_renderings():
    """One row per (item, point in time) whichever way the server bundles."""
    bundled = _normalize_timeseries(
        "H1",
        [
            {
                "item_name": "Worker.Signal1",
                "values": [
                    {"v": 1, "q": "good", "t": "T0"},
                    {"v": 2, "q": "good", "t": "T1"},
                ],
            }
        ],
    )
    flat = _normalize_timeseries(
        "H1",
        [
            {"item_name": "Worker.Signal1", "value": {"v": 1, "q": "good", "t": "T0"}},
            {"item_name": "Worker.Signal1", "value": {"v": 2, "q": "good", "t": "T1"}},
        ],
    )
    assert bundled == flat
    assert [row["timestamp"] for row in bundled] == ["T0", "T1"]
    assert all(row["instance"] == "H1" for row in bundled)


def test_relative_opc_time_expressions_are_rejected():
    """The source accepts 'DAY-1D'; incremental reads must stay deterministic."""
    connector = _connector()
    with pytest.raises(ValueError, match="absolute"):
        connector.get_partitions("timeseries", {"start_timestamp": "DAY-1D"})
