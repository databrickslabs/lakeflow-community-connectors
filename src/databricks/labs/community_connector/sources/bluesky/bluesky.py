"""Bounded raw archive ingestion through the current Lakeflow interfaces."""

import json
import re
from typing import Any, Iterator, Mapping

from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks.labs.community_connector.interface.lakeflow_connect import LakeflowConnect
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.bluesky.archive import segment_events
from databricks.labs.community_connector.sources.bluesky.errors import CursorTooOld, ProtocolError
from databricks.labs.community_connector.sources.bluesky.options import MAX_SEQ, Options, integer
from databricks.labs.community_connector.sources.bluesky.transport import Transport

MAX_PLAN_BYTES = 2 * 1024 * 1024
MAX_BATCH_BYTES = 64 * 1024 * 1024


class ArchiveValidator:
    """Validate untrusted checkpoint values and snapshot planner descriptors.

    Notes
    -----
    Validation failures raise :class:`ProtocolError` so malformed or unsupported
    planner data cannot produce a checkpoint that Lakeflow might commit.
    """

    @staticmethod
    def sequence(value: Any, name: str) -> int:
        # bool subclasses int, but is not a valid archive sequence.
        if type(value) is not int:  # pylint: disable=unidiomatic-typecheck
            raise ProtocolError(f"{name} must be an integer")
        try:
            return integer(value, name)
        except ValueError:
            raise ProtocolError(f"Invalid {name}") from None

    @staticmethod
    def validate_segments(segments: Any) -> None:
        if not isinstance(segments, list):
            raise ProtocolError("Invalid snapshot segments")
        previous_index = -1
        for entry in segments:
            if not isinstance(entry, dict):
                raise ProtocolError("Invalid snapshot segment entry")
            if (
                not re.fullmatch(r"seg_[0-9a-z]+\.jss", str(entry.get("name", "")))
                or not re.fullmatch(r"[0-9a-f]{16}", str(entry.get("checksum", "")))
                or entry.get("mode") not in ("segment", "blocks")
            ):
                raise ProtocolError("Unsupported snapshot segment descriptor")
            idx = ArchiveValidator.sequence(entry.get("index"), "segment index")
            if idx <= previous_index:
                raise ProtocolError("Snapshot segments are not strictly ordered")
            previous_index = idx
            min_seq = ArchiveValidator.sequence(entry.get("minSeq"), "minSeq")
            max_seq = ArchiveValidator.sequence(entry.get("maxSeq"), "maxSeq")
            if min_seq > max_seq:
                raise ProtocolError("Invalid segment sequence bounds")
            if entry["mode"] == "blocks":
                ranges = entry.get("blocks")
                if not isinstance(ranges, list) or not ranges:
                    raise ProtocolError("Missing snapshot block ranges")
                previous = -1
                for block in ranges:
                    if not isinstance(block, dict):
                        raise ProtocolError("Invalid block range")
                    first = ArchiveValidator.sequence(block.get("first"), "first block")
                    last = ArchiveValidator.sequence(block.get("last"), "last block")
                    if first <= previous or first > last:
                        raise ProtocolError("Invalid or overlapping snapshot block ranges")
                    previous = last


class BlueskyLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Read one bounded Jetstream archive refresh through Lakeflow Connect.

    Parameters
    ----------
    config : Options
        Validated connector configuration. Lakeflow's string options are converted
        to this dataclass by :class:`_LakeflowConfigAdapter`.

    Attributes
    ----------
    config : Options
        Configuration used for planning, filtering, and transport setup.
    _refresh_high : int or None
        Sealed sequence tip pinned for this connector instance.
    _refresh_scope : str or None
        Fingerprint of the configuration pinned for this refresh.

    Notes
    -----
    Create a fresh connector instance for each triggered pipeline refresh.
    """

    def __init__(self, config: Options) -> None:
        if not isinstance(config, Options):
            raise TypeError("config must be a validated Options instance")
        super().__init__(config.to_options_dict())
        self.config = config
        self._refresh_high: int | None = None
        self._refresh_scope: str | None = None

    def _config(self, table_name: str, table_options: Mapping[str, Any]) -> Options:
        if table_name != "events":
            raise ValueError("Unknown table; supported table: events")
        return self.config.with_table_options(table_options)

    def list_tables(self) -> list[str]:
        return ["events"]

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._config(table_name, table_options)
        return StructType(
            [
                StructField("seq", LongType(), False),
                StructField("event_time", TimestampType(), False),
                StructField("witnessed_at", TimestampType(), False),
                StructField("did", StringType(), False),
                StructField("kind", StringType(), False),
                StructField("operation", StringType(), True),
                StructField("collection", StringType(), True),
                StructField("rkey", StringType(), True),
                StructField("cid", StringType(), True),
                StructField("rev", StringType(), True),
                StructField("is_resync", BooleanType(), False),
                StructField("record", StringType(), True),
                StructField("event_payload", StringType(), False),
                StructField("raw_payload", BinaryType(), True),
            ]
        )

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict[str, Any]:
        self._config(table_name, table_options)
        return {"primary_keys": ["seq"], "cursor_field": "seq", "ingestion_type": "append"}

    def _cursor(self, offset: dict[str, Any] | None, config: Options) -> int:
        if not offset:
            return config.starting_cursor
        if not isinstance(offset, dict) or offset.get("version") != 1:
            raise ValueError("Unsupported checkpoint version")
        if offset.get("scope") != config.fingerprint():
            raise ValueError(
                "Checkpoint endpoint, start cursor, filters or payload settings changed"
            )
        return ArchiveValidator.sequence(offset.get("seq"), "checkpoint seq")

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        config = self._config(table_name, table_options)
        after = self._cursor(start_offset, config)
        scope = config.fingerprint()
        if self._refresh_scope is not None and self._refresh_scope != scope:
            raise ValueError("A connector instance cannot change its scope during a refresh")
        self._refresh_scope = scope
        if self._refresh_high is not None and after >= self._refresh_high:
            return start_offset or {}
        # A sequence window bounds the number of rows even before exact filtering.
        before = self._refresh_high or min(after + config.max_events_per_refresh, MAX_SEQ)
        if before == after:
            return start_offset or {}
        pages = []
        through = after
        with Transport(config) as transport:
            for _ in range(100):
                page = transport.plan(through, before)
                tip = ArchiveValidator.sequence(page.get("sealedTipSeq"), "sealedTipSeq")
                covered = ArchiveValidator.sequence(
                    page.get("plannedThroughSeq"), "plannedThroughSeq"
                )
                if tip < after:
                    raise CursorTooOld("Archive tip is behind checkpoint; endpoint may have reset")
                if tip > before or covered > tip or covered < through:
                    raise ProtocolError("Snapshot planner returned inconsistent sequence bounds")
                if self._refresh_high is None:
                    self._refresh_high = tip
                if tip != self._refresh_high:
                    raise ProtocolError("Pinned archive tip changed during planning")
                before = tip
                ArchiveValidator.validate_segments(page.get("segments"))
                pages.append({"after": through, "through": covered, "segments": page["segments"]})
                if len(json.dumps(pages)) > MAX_PLAN_BYTES:
                    raise ProtocolError("Snapshot plan too large; reduce max_events_per_refresh")
                if covered == tip:
                    break
                if covered == through:
                    raise ProtocolError("Snapshot planner made no progress")
                through = covered
            else:
                raise ProtocolError("Snapshot planning exceeded the page limit")
        if before == after:
            return start_offset or {}
        return {
            "version": 1,
            "scope": scope,
            "seq": before,
            "after": after,
            "plan": json.dumps(pages, separators=(",", ":")),
        }

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict[str, Any] | None = None,
        end_offset: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        config = self._config(table_name, table_options)
        if end_offset is None:
            end_offset = self.latest_offset(table_name, table_options, start_offset)
        after = self._cursor(start_offset, config)
        through = self._cursor(end_offset, config)
        if through == after:
            return []
        if through < after or end_offset.get("after") != after:
            raise ValueError("Checkpoint range does not match the recorded plan")
        # A single partition keeps ordering and a single overall read time budget.
        # The mixin is used for replay correctness, not parallelism in this MVP.
        return [{"start": after, "end": end_offset}]

    def read_partition(
        self,
        table_name: str,
        partition: dict[str, Any],
        table_options: dict[str, str],
    ) -> Iterator[dict[str, Any]]:
        config = self._config(table_name, table_options)
        end = partition["end"]
        after = ArchiveValidator.sequence(partition["start"], "partition start")
        through = self._cursor(end, config)
        if end.get("after") != after or after >= through:
            raise ProtocolError("Invalid partition range")
        try:
            pages = json.loads(end["plan"])
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("Invalid checkpoint plan") from None
        if not isinstance(pages, list) or not pages or len(pages) > 100:
            raise ProtocolError("Invalid checkpoint pages")
        covered = after
        previous = after
        with Transport(config) as transport:
            for page in pages:
                if page["after"] != covered or not covered <= page["through"] <= through:
                    raise ProtocolError("Discontinuous checkpoint plan")
                ArchiveValidator.validate_segments(page["segments"])
                for entry in page["segments"]:
                    for event in segment_events(
                        transport, entry, covered, page["through"], config.include_raw_payload
                    ):
                        if event["seq"] <= previous:
                            raise ProtocolError("Overlapping or out-of-order archive events")
                        previous = event["seq"]
                        if config.matches(event):
                            yield event
                covered = page["through"]
            if covered != through:
                raise ProtocolError("Incomplete checkpoint plan")

    def read_table(
        self,
        table_name: str,
        start_offset: dict[str, Any] | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict[str, Any]], dict[str, Any]]:
        end = self.latest_offset(table_name, table_options, start_offset)
        # Complete bounded I/O before returning a candidate offset. Only Lakeflow/Spark
        # may commit it, after consuming rows and successfully writing the Delta batch.
        rows = []
        size = 0
        for partition in self.get_partitions(table_name, table_options, start_offset, end):
            for row in self.read_partition(table_name, partition, table_options):
                size += len(json.dumps(row).encode())
                if size > MAX_BATCH_BYTES:
                    raise ProtocolError(
                        "Batch exceeds memory budget; reduce max_events_per_refresh"
                    )
                rows.append(row)
        return iter(rows), end
