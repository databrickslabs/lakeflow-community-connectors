"""Bounded raw archive ingestion through the current Lakeflow interfaces."""

import json
import logging
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
    client_for_options,
    lakeflow_row,
    migrate_legacy_plan,
    operation_context,
    plan_from_json,
    translated_errors,
)
from databricks.labs.community_connector.sources.bluesky.options import MAX_SEQ, Options

MAX_PLAN_BYTES = 2 * 1024 * 1024
MAX_BATCH_BYTES = 64 * 1024 * 1024
LOGGER = logging.getLogger(__name__)


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


class BlueskyLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Read a fixed Jetstream run target through bounded Lakeflow microbatches.

    Parameters
    ----------
    config : Options or Mapping[str, Any]
        Validated connector configuration or Lakeflow's string options. A mapping
        is validated at the framework boundary before any archive operation.

    Attributes
    ----------
    config : Options
        Configuration used for planning, filtering, and transport setup.
    _run_target : int or None
        Uncapped sealed sequence tip discovered once for this connector instance.
    _run_scope : str or None
        Fingerprint of the configuration pinned for this run.

    Notes
    -----
    Each instance progresses through bounded batches toward its fixed run target.
    """

    def __init__(self, config: Options | Mapping[str, Any]) -> None:
        """Create a connector for one archive run.

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
        The first offset request discovers a sealed tip and configuration scope.
        Later batches on this instance advance toward that fixed tip.
        """
        if isinstance(config, Mapping):
            config = Options.parse(config, {})
        if not isinstance(config, Options):
            raise TypeError("config must be an Options instance or a mapping")
        super().__init__(config.to_options_dict())
        self.config = config
        self._run_target: int | None = None
        self._run_scope: str | None = None

    def requires_trigger_available_now(self) -> bool:
        """Require the shared Spark reader's AvailableNow lifecycle callback."""
        return True

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
        if not isinstance(offset, dict) or offset.get("version") not in (1, 2):
            raise ValueError("Unsupported checkpoint version")
        if offset.get("scope") != config.fingerprint():
            raise ValueError(
                "Checkpoint endpoint, start cursor, filters or payload settings changed"
            )
        return ArchiveValidator.sequence(offset.get("seq"), "checkpoint seq")

    @staticmethod
    def _archive_plan(offset: dict[str, Any], config: Options):
        """Restore a v2 plan or deterministically migrate a pending v1 envelope."""
        if not isinstance(offset.get("plan"), str) or len(offset["plan"]) > MAX_PLAN_BYTES:
            raise ProtocolError("Invalid checkpoint plan")
        after = ArchiveValidator.sequence(offset.get("after"), "checkpoint after")
        through = ArchiveValidator.sequence(offset.get("seq"), "checkpoint seq")
        if offset.get("version") == 1:
            plan = migrate_legacy_plan(offset["plan"], config, after, through)
        elif offset.get("version") == 2:
            plan = plan_from_json(offset["plan"])
        else:
            raise ValueError("Unsupported checkpoint version")
        if plan.after_seq != after or plan.through_seq != through:
            raise ProtocolError("Checkpoint range does not match its saved plan")
        return plan

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
        The connector discovers the uncapped sealed target once. Each later call
        plans one sequence-bounded batch toward it. Returning an offset does not
        commit it; Spark commits after the batch.
        """
        config = self._config(table_name, table_options)
        after = self._cursor(start_offset, config)
        scope = config.fingerprint()
        if self._run_scope is not None and self._run_scope != scope:
            raise ValueError("A connector instance cannot change its scope during a run")
        self._run_scope = scope
        if self._run_target is None:
            with operation_context("Sealed-tip discovery", config, after), translated_errors():
                self._run_target = client_for_options(config, MAX_BATCH_BYTES).sealed_tip(after)
        if self._run_target < after:
            raise CursorTooOld("Archive tip is behind checkpoint; endpoint may have reset")
        if after >= self._run_target:
            return start_offset or {}
        batch_end = min(self._run_target, after + config.max_events_per_refresh)
        with (
            operation_context("Snapshot planning", config, after, batch_end),
            translated_errors(),
        ):
            plan = client_for_options(config, MAX_BATCH_BYTES).plan_snapshot(after, batch_end)
        if plan.through_seq <= after:
            return start_offset or {}
        if plan.through_seq > batch_end:
            raise ProtocolError("Snapshot plan exceeds the requested batch range")
        with (
            operation_context("Plan serialization", config, after, plan.through_seq),
            translated_errors(),
        ):
            plan_json = plan.to_json()
        LOGGER.info(
            "Planned Bluesky batch after=%d through=%d target=%d sequence_lag=%d",
            after,
            plan.through_seq,
            self._run_target,
            self._run_target - plan.through_seq,
        )
        return {
            "version": 2,
            "scope": scope,
            "seq": plan.through_seq,
            "after": after,
            "plan": plan_json,
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
        if (
            through < after
            or end_offset.get("after") != after
            or end_offset.get("version") not in (1, 2)
        ):
            raise ValueError("Checkpoint range does not match the recorded plan")
        self._archive_plan(end_offset, config)
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
        plan = self._archive_plan(end, config)
        LOGGER.info("Reading Bluesky batch after=%d through=%d", after, through)
        try:
            with (
                operation_context("Saved-plan read", config, after, through),
                translated_errors(),
            ):
                batch = client_for_options(config, MAX_BATCH_BYTES).read_plan(plan)
        except ValueError:
            raise ProtocolError(
                "Saved plan source scope does not match connector options"
            ) from None
        LOGGER.info(
            "Read Bluesky batch after=%d through=%d rows=%d",
            batch.after_seq,
            batch.through_seq,
            len(batch.events),
        )
        for event in batch.events:
            yield lakeflow_row(event, config.include_raw_payload)

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
