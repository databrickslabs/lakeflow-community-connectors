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
from databricks.labs.community_connector.sources.bluesky.errors import CursorTooOld, ProtocolError
from databricks.labs.community_connector.sources.bluesky.jetstream_lakeflow_adapter import (
    pinned_rows,
    pinned_transport,
)
from databricks.labs.community_connector.sources.bluesky.options import MAX_SEQ, Options

MAX_PLAN_BYTES = 2 * 1024 * 1024
MAX_BATCH_BYTES = 64 * 1024 * 1024
# Connector safety cap for paginated plans, not a Jetstream API limit.
MAX_PLAN_PAGES = 100


class ArchiveValidator:
    """Validate untrusted checkpoint values and snapshot planner descriptors.

    Notes
    -----
    Validation failures raise :class:`ProtocolError` so malformed or unsupported
    planner data cannot produce a checkpoint that Lakeflow might commit.
    """

    @staticmethod
    def sequence(value: Any, name: str) -> int:
        """Validate an archive sequence supplied by a checkpoint or planner.

        Parameters
        ----------
        value : Any
            Value to check. Only a Python integer is accepted.
        name : str
            Field name used in a sanitized error message.

        Returns
        -------
        int
            Non-negative sequence within the connector's supported range.

        Raises
        ------
        ProtocolError
            The value is not an integer or is outside the supported range.

        Notes
        -----
        A boolean is rejected even though it is a subclass of ``int``.
        """
        if type(value) is not int:  # pylint: disable=unidiomatic-typecheck
            raise ProtocolError(f"{name} must be an integer")
        if not 0 <= value <= MAX_SEQ:
            raise ProtocolError(f"Invalid {name}")
        return value

    @staticmethod
    def validate_segments(segments: Any) -> None:
        """Check the shape, order, and bounds of planned segment descriptors.

        Parameters
        ----------
        segments : Any
            Untrusted ``segments`` value returned by the snapshot planner or
            loaded from a saved checkpoint.

        Raises
        ------
        ProtocolError
            A descriptor or block range is malformed, unsupported, or out of
            order.

        Notes
        -----
        This checks plan metadata before any segment bytes are requested. The
        package reader validates the sealed segment contents separately.
        """
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
    config : Options or Mapping[str, Any]
        Validated connector configuration or Lakeflow's string options. A mapping
        is validated at the framework boundary before any archive operation.

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

    def __init__(self, config: Options | Mapping[str, Any]) -> None:
        """Create a connector for one bounded refresh.

        Parameters
        ----------
        config : Options or Mapping[str, Any]
            Validated settings or the string options supplied by Lakeflow.

        Raises
        ------
        TypeError
            ``config`` is neither a validated :class:`Options` instance nor a mapping.

        Notes
        -----
        The first planned window pins a sealed tip and configuration scope for
        this instance. A later triggered refresh should use a new instance.
        """
        if isinstance(config, Mapping):
            config = Options.parse(config, {})
        if not isinstance(config, Options):
            raise TypeError("config must be an Options instance or a mapping")
        super().__init__(config.to_options_dict())
        self.config = config
        self._refresh_high: int | None = None
        self._refresh_scope: str | None = None

    def _config(self, table_name: str, table_options: Mapping[str, Any]) -> Options:
        """Resolve and validate options for the single supported table.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        table_options : Mapping[str, Any]
            Per-table overrides layered onto the connection settings.

        Returns
        -------
        Options
            Validated options for this read.

        Raises
        ------
        ValueError
            The table name or an option is invalid.
        """
        if table_name != "events":
            raise ValueError("Unknown table; supported table: events")
        return self.config.with_table_options(table_options)

    def list_tables(self) -> list[str]:
        """List the tables exposed by this connector.

        Returns
        -------
        list[str]
            The single append-only ``events`` table.
        """
        return ["events"]

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        """Return the Spark schema for decoded archive events.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        table_options : dict[str, str]
            Table options validated against the connector configuration.

        Returns
        -------
        StructType
            Event columns, including JSON record text and optional raw bytes.

        Notes
        -----
        Option validation happens here even though the schema itself is fixed.
        """
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
        """Describe the event key, cursor, and append ingestion mode.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        table_options : dict[str, str]
            Table options validated before metadata is returned.

        Returns
        -------
        dict[str, Any]
            Metadata identifying ``seq`` as the key and cursor field.
        """
        self._config(table_name, table_options)
        return {"primary_keys": ["seq"], "cursor_field": "seq", "ingestion_type": "append"}

    @staticmethod
    def _cursor(offset: dict[str, Any] | None, config: Options) -> int:
        """Read a sequence from a checkpoint without changing its scope.

        Parameters
        ----------
        offset : dict[str, Any] or None
            Saved Lakeflow offset, or an empty value on the first read.
        config : Options
            Settings whose fingerprint must match the saved offset.

        Returns
        -------
        int
            Saved sequence, or ``starting_cursor`` for an empty offset.

        Raises
        ------
        ValueError
            The checkpoint version or configuration scope differs.
        ProtocolError
            The saved sequence is invalid.
        """
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
        """Plan one bounded archive window and return its candidate offset.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        table_options : dict[str, str]
            Per-table filters and read limits.
        start_offset : dict[str, Any] or None, optional
            Last committed offset. An empty value starts at ``starting_cursor``.

        Returns
        -------
        dict[str, Any]
            Candidate offset containing the end sequence and serialized plan,
            or the original empty/committed offset when no progress is possible.

        Raises
        ------
        CursorTooOld
            The archive tip is behind the committed checkpoint.
        ProtocolError
            Planner pages are inconsistent, too large, or make no progress.

        Notes
        -----
        The first planner response pins the sealed tip for this instance.
        Subsequent pages must reach that tip without extending the window.
        Returning an offset does not commit it; Spark commits after the batch.
        """
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
        with pinned_transport(config) as transport:
            for _ in range(MAX_PLAN_PAGES):
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
        """Create the single ordered partition for a recorded plan.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        table_options : dict[str, str]
            Settings used to validate both offsets.
        start_offset : dict[str, Any] or None, optional
            Last committed offset.
        end_offset : dict[str, Any] or None, optional
            Planned candidate offset. If absent, a new window is planned.

        Returns
        -------
        list[dict[str, Any]]
            One partition carrying the saved end offset, or an empty list when
            the window contains no new sequence positions.

        Raises
        ------
        ValueError
            The offsets do not describe the same recorded window.

        Notes
        -----
        Partitioning provides Spark replay semantics here, not parallel reads.
        """
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
        # The mixin is used for replay correctness, not parallelism in this version.
        return [{"start": after, "end": end_offset}]

    def read_partition(
        self,
        table_name: str,
        partition: dict[str, Any],
        table_options: dict[str, str],
    ) -> Iterator[dict[str, Any]]:
        """Read exactly the segments pinned in a Lakeflow partition.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        partition : dict[str, Any]
            Start sequence and candidate end offset containing the saved plan.
        table_options : dict[str, str]
            Settings used to validate scope and apply exact event filters.

        Yields
        ------
        dict[str, Any]
            Matching event rows in increasing sequence order.

        Raises
        ------
        ProtocolError
            The saved plan is discontinuous, malformed, or produces unordered
            events.

        Notes
        -----
        This method never calls the planner. A Spark retry reads the same
        checksum of archive generation or fails if that generation is gone.
        """
        config = self._config(table_name, table_options)
        if not isinstance(partition, dict) or not isinstance(partition.get("end"), dict):
            raise ProtocolError("Invalid partition")
        end = partition["end"]
        after = ArchiveValidator.sequence(partition.get("start"), "partition start")
        through = self._cursor(end, config)
        if (
            ArchiveValidator.sequence(end.get("after"), "checkpoint after") != after
            or after >= through
        ):
            raise ProtocolError("Invalid partition range")
        if not isinstance(end.get("plan"), str) or len(end["plan"]) > MAX_PLAN_BYTES:
            raise ProtocolError("Invalid checkpoint plan")
        try:
            pages = json.loads(end["plan"])
        except ValueError:
            raise ProtocolError("Invalid checkpoint plan") from None
        if not isinstance(pages, list) or not pages or len(pages) > MAX_PLAN_PAGES:
            raise ProtocolError("Invalid checkpoint pages")
        covered = after
        previous = after
        with pinned_transport(config) as transport:
            for page in pages:
                if not isinstance(page, dict):
                    raise ProtocolError("Invalid checkpoint page")
                page_after = ArchiveValidator.sequence(page.get("after"), "page after")
                page_through = ArchiveValidator.sequence(page.get("through"), "page through")
                if page_after != covered or not covered < page_through <= through:
                    raise ProtocolError("Discontinuous checkpoint plan")
                segments = page.get("segments")
                ArchiveValidator.validate_segments(segments)
                for entry in segments:
                    for event in pinned_rows(
                        transport, entry, covered, page_through, config.include_raw_payload
                    ):
                        if event["seq"] <= previous:
                            raise ProtocolError("Overlapping or out-of-order archive events")
                        previous = event["seq"]
                        if config.matches(event):
                            yield event
                covered = page_through
            if covered != through:
                raise ProtocolError("Incomplete checkpoint plan")

    def read_table(
        self,
        table_name: str,
        start_offset: dict[str, Any] | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict[str, Any]], dict[str, Any]]:
        """Complete a bounded read for Lakeflow's direct reader interface.

        Parameters
        ----------
        table_name : str
            Table requested by Lakeflow; must be ``events``.
        start_offset : dict[str, Any] or None
            Last committed offset, or an empty value for the first read.
        table_options : dict[str, str]
            Per-table filters and read limits.

        Returns
        -------
        tuple[Iterator[dict[str, Any]], dict[str, Any]]
            Iterator over fully materialized rows and their candidate end
            offset. The caller must commit that offset only after writing rows.

        Raises
        ------
        ProtocolError
            The materialized batch exceeds the memory budget or archive
            validation fails.

        Notes
        -----
        All archive I/O finishes before this method returns. The partitioned
        Spark path instead streams rows from :meth:`read_partition`.
        """
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
