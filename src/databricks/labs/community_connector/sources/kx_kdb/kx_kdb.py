"""Lakeflow community connector for KX KDB HDB files."""

from __future__ import annotations

import logging
import math
from itertools import chain
from pathlib import Path
from typing import Iterator, Sequence

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    discover_table_partitions,
    discover_tables,
    latest_table_partition,
    normalize_partition_date,
    resolve_table_directory_name,
    table_partition_exists,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    build_runtime_config,
)
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    columns_to_spark_schema,
    infer_schema_from_partition,
)
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
    PartitionShape,
    read_kdb_row_range,
    read_partition_shape,
)

logger = logging.getLogger(__name__)

DEFAULT_MAX_DATES_PER_BATCH = 250
DEFAULT_MAX_TASKS_PER_BATCH = 2000
DEFAULT_MAX_ROWS_PER_TASK = 2_000_000
_MISSING_COLUMN_POLICIES = ("fail", "null")
_REMOVED_TABLE_OPTIONS = ("partition_strategy", "sym_column")


class KxKdbLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Read immutable KDB HDB date partitions through the Lakeflow connector APIs.

    On managed ingestion, pipeline table configuration is merged into
    ``options`` and can fill connection keys that the connection leaves unset;
    the connector cannot tell where a key came from. Keys set on the connection
    take precedence, and the two required paths are always set there. KDB-X
    setup therefore reads only files inside the Unity Catalog Volume of
    ``license_volume_path`` and rejects the removed bootstrap keys (see
    ``build_runtime_config``). Table options are the keys in
    ``external_options_allowlist``.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.hdb_root_path = self._required_absolute_path_option("hdb_root_path")
        self._required_absolute_path_option("license_volume_path")
        # The runtime config is built on the driver and travels with the
        # serialized connector object to executors.
        self.runtime_config = build_runtime_config(self.options)
        self.discovery_sample_dates = self._int_option("discovery_sample_dates", 0)
        self._schema_cache: dict[tuple[str, str], StructType] = {}
        self._column_cache: dict[tuple[str, str], list[dict]] = {}
        self._table_dir_cache: dict[str, str] = {}
        self._shape_cache: dict[tuple[str, str], PartitionShape] = {}

    def list_tables(self) -> list[str]:
        return discover_tables(self.hdb_root_path, self.discovery_sample_dates)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table_options(table_options)
        cache_key = (table_name.lower(), self._ingestion_mode(table_options))
        if cache_key not in self._schema_cache:
            self._schema_cache[cache_key] = columns_to_spark_schema(
                self._column_defs(table_name, table_options)
            )
        return self._schema_cache[cache_key]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._actual_table_name(table_name)
        self._validate_table_options(table_options)
        return {"primary_keys": None, "cursor_field": None, "ingestion_type": "append"}

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict | None]:
        if start_offset is None:
            # A batch read, not a micro-batch: read every complete date.
            partitions = self.get_partitions(table_name, table_options)
            end_offset = {"date_partition": partitions[-1]["date_partition"]} if partitions else {}
        else:
            end_offset = self.latest_offset(table_name, table_options, start_offset)
            if end_offset == start_offset:
                return iter(()), start_offset
            partitions = self.get_partitions(table_name, table_options, start_offset, end_offset)
        records = chain.from_iterable(
            self.read_partition(table_name, partition, table_options) for partition in partitions
        )
        return records, end_offset

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        admitted = self._admitted_partitions(table_name, table_options, start_offset, capped=True)
        if not admitted:
            return dict(start_offset or {})
        return {"date_partition": admitted[-1]}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        if start_offset is None and end_offset is None:
            selected = self._admitted_partitions(table_name, table_options, None, capped=False)
        else:
            self._validate_table_options(table_options)
            end_partition = normalize_partition_date((end_offset or {}).get("date_partition"))
            if not end_partition:
                return []
            start_partition = normalize_partition_date((start_offset or {}).get("date_partition"))
            selected = [
                partition
                for partition in self._available_partitions(table_name, table_options)
                if (start_partition is None or partition > start_partition)
                and partition <= end_partition
            ]
        if not selected:
            return []

        max_rows = self._max_rows_per_task(table_options)
        min_age = self._min_partition_age_seconds(table_options)
        allow_missing = self._allow_missing_columns(table_options)
        newest = latest_table_partition(self.hdb_root_path, table_name)
        descriptors: list[dict] = []
        total_rows = 0
        for partition in selected:
            shape = self._partition_shape(table_name, partition, newest, min_age, allow_missing)
            if not shape.complete:
                raise RuntimeError(
                    f"KX KDB table {table_name!r} date {partition} is in this batch but "
                    f"is no longer complete: {shape.reason}."
                )
            descriptors.extend(_row_ranges(partition, shape.row_count, max_rows))
            total_rows += shape.row_count
        logger.info(
            "KX KDB partition plan table=%s dates=%s first=%s last=%s rows=%s tasks=%s",
            table_name,
            len(selected),
            selected[0],
            selected[-1],
            total_rows,
            len(descriptors),
        )
        return descriptors

    def read_partition(
        self,
        table_name: str,
        partition: dict,
        table_options: dict[str, str],
    ) -> Iterator[dict]:
        date_partition = normalize_partition_date(partition.get("date_partition"))
        if not date_partition:
            return iter(())
        self._validate_table_options(table_options)
        try:
            row_offset = int(partition["row_offset"])
            row_count = int(partition["row_count"])
            total_rows = int(partition["total_rows"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(
                "KX KDB partition descriptors must contain integer 'row_offset', "
                "'row_count', and 'total_rows'."
            ) from None
        return read_kdb_row_range(
            hdb_root_path=self.hdb_root_path,
            kdb_table_name=self._actual_table_name(table_name),
            date_partition=date_partition,
            row_offset=row_offset,
            row_count=row_count,
            total_rows=total_rows,
            runtime_config=self.runtime_config,
            column_defs=self._column_defs(table_name, table_options),
            allow_missing_columns=self._allow_missing_columns(table_options),
        )

    def _admitted_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None,
        *,
        capped: bool,
    ) -> list[str]:
        """Return the complete dates after ``start_offset``, in order.

        Admission stops before the table's newest date while that date is
        still being written, and before any date changed within
        ``min_partition_age_minutes``. Any other incomplete date ends a capped
        batch just before it, so the dates before it are committed, and fails
        once it is the first date to admit; the offset never moves past a date
        that has not been read. With ``capped``, admission also stops at
        ``max_dates_per_batch`` dates or ``max_tasks_per_batch`` tasks; the
        first date is always admitted.
        """
        self._validate_table_options(table_options)
        start_partition = normalize_partition_date((start_offset or {}).get("date_partition"))
        candidates = [
            partition
            for partition in self._available_partitions(table_name, table_options)
            if start_partition is None or partition > start_partition
        ]
        if not candidates:
            return []

        newest = latest_table_partition(self.hdb_root_path, table_name)
        max_dates = self._positive_int_option(
            table_options, "max_dates_per_batch", DEFAULT_MAX_DATES_PER_BATCH
        )
        max_tasks = self._positive_int_option(
            table_options, "max_tasks_per_batch", DEFAULT_MAX_TASKS_PER_BATCH
        )
        max_rows = self._max_rows_per_task(table_options)
        min_age = self._min_partition_age_seconds(table_options)
        allow_missing = self._allow_missing_columns(table_options)

        admitted: list[str] = []
        tasks = 0
        for partition in candidates:
            if capped and len(admitted) >= max_dates:
                break
            shape = self._partition_shape(table_name, partition, newest, min_age, allow_missing)
            if not shape.complete:
                if partition == newest or shape.recent:
                    logger.info(
                        "KX KDB table=%s is waiting for date %s to finish writing: %s",
                        table_name,
                        partition,
                        shape.reason,
                    )
                    break
                if capped and admitted:
                    logger.warning(
                        "KX KDB table=%s ends this batch before incomplete date %s: %s",
                        table_name,
                        partition,
                        shape.reason,
                    )
                    break
                raise RuntimeError(
                    f"KX KDB table {table_name!r} date {partition} is incomplete: "
                    f"{shape.reason}. Only the newest date of a table may still be "
                    "in progress. Repair the partition (for a missing column, "
                    "dbmaint's addcol fills it with nulls), or set the table option "
                    "missing_columns to 'null' to read missing columns of older "
                    "dates as nulls."
                )
            if shape.missing_columns:
                logger.warning(
                    "KX KDB table=%s date %s has no files for columns %s; reading them "
                    "as nulls because missing_columns is 'null'.",
                    table_name,
                    partition,
                    list(shape.missing_columns),
                )
            date_tasks = math.ceil(shape.row_count / max_rows)
            if capped and admitted and tasks + date_tasks > max_tasks:
                break
            admitted.append(partition)
            tasks += date_tasks

        if capped and tasks > max_tasks:
            logger.warning(
                "KX KDB table=%s date %s alone needs %s tasks, more than "
                "max_tasks_per_batch=%s; raise max_rows_per_task to reduce it.",
                table_name,
                admitted[0],
                tasks,
                max_tasks,
            )
        if admitted:
            logger.info(
                "KX KDB admission table=%s start=%s dates=%s first=%s last=%s tasks=%s",
                table_name,
                start_partition,
                len(admitted),
                admitted[0],
                admitted[-1],
                tasks,
            )
        return admitted

    def _partition_shape(
        self,
        table_name: str,
        date_partition: str,
        newest: str,
        min_age_seconds: float,
        allow_missing_columns: bool,
    ) -> PartitionShape:
        is_newest = date_partition == newest
        # The newest date may still be written, so its columns stay required.
        allow_missing = allow_missing_columns and not is_newest
        cache_key = (table_name.lower(), date_partition, allow_missing)
        cached = self._shape_cache.get(cache_key)
        if cached is not None:
            return cached
        shape = read_partition_shape(
            hdb_root_path=self.hdb_root_path,
            kdb_table_name=self._actual_table_name(table_name),
            date_partition=date_partition,
            runtime_config=self.runtime_config,
            check_all_columns=is_newest,
            min_age_seconds=min_age_seconds,
            allow_missing_columns=allow_missing,
        )
        # Older complete dates are immutable. The newest date may still be
        # written, and a sparse date's table directory may still appear, so
        # both are checked again each time.
        if shape.complete and shape.present and not is_newest:
            self._shape_cache[cache_key] = shape
        return shape

    def _required_option(self, key: str) -> str:
        value = str(self.options.get(key, "")).strip()
        if not value:
            raise ValueError(f"KX KDB connector requires {key!r} in connection options")
        return value

    def _required_absolute_path_option(self, key: str) -> str:
        value = self._required_option(key)
        if not Path(value).is_absolute():
            raise ValueError(f"KX KDB connector requires {key!r} to be an absolute filesystem path")
        return value

    def _int_option(self, key: str, default: int) -> int:
        value = str(self.options.get(key, "")).strip()
        if not value:
            return default
        return int(value)

    @staticmethod
    def _positive_int_option(table_options: dict[str, str], key: str, default: int) -> int:
        raw = str(table_options.get(key, "")).strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            raise ValueError(f"{key} must be a positive integer, got {raw!r}") from None
        if value <= 0:
            raise ValueError(f"{key} must be a positive integer, got {raw!r}")
        return value

    def _max_rows_per_task(self, table_options: dict[str, str]) -> int:
        return self._positive_int_option(
            table_options, "max_rows_per_task", DEFAULT_MAX_ROWS_PER_TASK
        )

    @staticmethod
    def _min_partition_age_seconds(table_options: dict[str, str]) -> float:
        raw = str(table_options.get("min_partition_age_minutes", "")).strip()
        if not raw:
            return 0.0
        try:
            minutes = float(raw)
        except ValueError:
            minutes = -1.0
        if not math.isfinite(minutes) or minutes < 0:
            raise ValueError(
                f"min_partition_age_minutes must be a non-negative number, got {raw!r}"
            )
        return minutes * 60

    @staticmethod
    def _allow_missing_columns(table_options: dict[str, str]) -> bool:
        policy = str(table_options.get("missing_columns", "fail")).strip().lower() or "fail"
        if policy not in _MISSING_COLUMN_POLICIES:
            raise ValueError(f"Unsupported missing_columns {policy!r}. Expected 'fail' or 'null'.")
        return policy == "null"

    def _validate_table_options(self, table_options: dict[str, str]) -> None:
        removed = [key for key in _REMOVED_TABLE_OPTIONS if key in table_options]
        if removed:
            raise ValueError(
                f"KX KDB table options {removed} are no longer supported. Each date is "
                "split into row ranges; use max_rows_per_task to size them."
            )
        self._ingestion_mode(table_options)
        self._allow_missing_columns(table_options)

    def _ingestion_mode(self, table_options: dict[str, str]) -> str:
        mode = str(table_options.get("ingestion_mode", "append")).strip().lower()
        if mode != "append":
            raise ValueError(f"Unsupported ingestion_mode {mode!r}. KX KDB supports append only.")
        return mode

    def _actual_table_name(self, table_name: str) -> str:
        cache_key = table_name.lower()
        if cache_key not in self._table_dir_cache:
            self._table_dir_cache[cache_key] = resolve_table_directory_name(
                self.hdb_root_path, table_name
            )
        return self._table_dir_cache[cache_key]

    def _available_partitions(self, table_name: str, table_options: dict[str, str]) -> list[str]:
        # Discovery results are cached per process with a bounded TTL, so a
        # long-lived stream reader still sees new date partitions.
        return discover_table_partitions(
            root_path=self.hdb_root_path,
            table_name=table_name,
            start_date=normalize_partition_date(table_options.get("start_date")),
            end_date=normalize_partition_date(table_options.get("end_date")),
        )

    def _column_defs(self, table_name: str, table_options: dict[str, str]) -> list[dict]:
        cache_key = (table_name.lower(), self._ingestion_mode(table_options))
        if cache_key in self._column_cache:
            return self._column_cache[cache_key]

        partitions = self._available_partitions(table_name, table_options)
        if not partitions:
            raise RuntimeError(
                f"No partitions found for table {table_name!r} under {self.hdb_root_path}"
            )

        actual_table_name = self._actual_table_name(table_name)
        # Interior dates of a sparse table may not contain the table directory.
        schema_partition = next(
            (
                partition
                for partition in partitions
                if table_partition_exists(self.hdb_root_path, partition, actual_table_name)
            ),
            partitions[0],
        )
        columns = infer_schema_from_partition(
            hdb_root_path=self.hdb_root_path,
            kdb_table_name=actual_table_name,
            date_partition=schema_partition,
            runtime_config=self.runtime_config,
        )

        self._column_cache[cache_key] = columns
        return columns


def _row_ranges(date_partition: str, total_rows: int, max_rows_per_task: int) -> list[dict]:
    return [
        {
            "date_partition": date_partition,
            "row_offset": row_offset,
            "row_count": min(max_rows_per_task, total_rows - row_offset),
            "total_rows": total_rows,
        }
        for row_offset in range(0, total_rows, max_rows_per_task)
    ]
