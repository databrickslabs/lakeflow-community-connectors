"""Lakeflow community connector for KX KDB HDB files."""

from __future__ import annotations

import logging
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
    normalize_partition_date,
    resolve_table_directory_name,
    table_partition_exists,
    validate_hdb_name,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    build_runtime_config,
)
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    columns_to_spark_schema,
    infer_schema_from_partition,
)
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
    load_sym_enumeration,
    read_kdb_date_sym_records,
    sym_file_signature,
)

logger = logging.getLogger(__name__)


class KxKdbLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Read immutable KDB HDB date partitions through the Lakeflow connector APIs.

    Connection options (HDB root, license, KDB-X/PyKX bootstrap) are read only
    from the connection-level ``options``. Per-table ``table_options`` are
    limited to the keys in ``external_options_allowlist``.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.hdb_root_path = self._required_absolute_path_option("hdb_root_path")
        self._required_absolute_path_option("license_volume_path")
        # Resolve any KDB-X bootstrap secrets on the driver so the resulting
        # runtime config can travel with the serialized connector object.
        self.runtime_config = build_runtime_config(self.options)
        self.license_path = self.runtime_config.license_directory
        self.discovery_sample_dates = self._int_option("discovery_sample_dates", 0)
        self._schema_cache: dict[tuple[str, str], StructType] = {}
        self._column_cache: dict[tuple[str, str], list[dict]] = {}
        self._table_dir_cache: dict[str, str] = {}
        self._sym_cache: tuple[tuple[int, int], list[str]] | None = None

    def list_tables(self) -> list[str]:
        return discover_tables(self.hdb_root_path, self.discovery_sample_dates)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        cache_key = (table_name.lower(), self._ingestion_mode(table_options))
        if cache_key not in self._schema_cache:
            self._schema_cache[cache_key] = columns_to_spark_schema(
                self._column_defs(table_name, table_options)
            )
        return self._schema_cache[cache_key]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._actual_table_name(table_name)
        self._ingestion_mode(table_options)
        return {"primary_keys": None, "cursor_field": None, "ingestion_type": "append"}

    def read_table(
        self,
        table_name: str,
        start_offset: dict | None,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict | None]:
        self._actual_table_name(table_name)
        partitions = self.get_partitions(table_name, table_options)
        records = chain.from_iterable(
            self.read_partition(table_name, partition, table_options)
            for partition in partitions
        )
        latest_partition = partitions[-1]["date_partition"] if partitions else None
        if latest_partition:
            return records, {"date_partition": latest_partition}
        return records, start_offset or {}

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        self._actual_table_name(table_name)
        partitions = self._available_partitions(table_name, table_options)
        if not partitions:
            return start_offset or {}
        return {"date_partition": partitions[-1]}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        self._actual_table_name(table_name)
        available_partitions = self._available_partitions(table_name, table_options)

        if start_offset is None and end_offset is None:
            selected = available_partitions
        else:
            start_partition = normalize_partition_date(
                (start_offset or {}).get("date_partition")
            )
            end_partition = normalize_partition_date((end_offset or {}).get("date_partition"))
            if not end_partition:
                return []
            selected = [
                partition
                for partition in available_partitions
                if (start_partition is None or partition > start_partition)
                and partition <= end_partition
            ]

        strategy = self._partition_strategy(table_options)
        if not selected:
            logger.info(
                "KX KDB partition plan table=%s strategy=%s selected_dates=0",
                table_name,
                strategy,
            )
            return []

        self._sym_column(table_name, table_options)
        symbols = self._symbols()
        descriptors = []
        for partition in selected:
            descriptors.extend(
                {"date_partition": partition, "sym": symbol, "sym_index": sym_index}
                for sym_index, symbol in enumerate(symbols)
            )
            # Rows whose enumeration index is null or beyond the sym file.
            descriptors.append(
                {
                    "date_partition": partition,
                    "sym": None,
                    "sym_index": None,
                    "sym_count": len(symbols),
                }
            )
        logger.info(
            "KX KDB partition plan table=%s strategy=%s dates=%s symbols=%s tasks=%s",
            table_name,
            strategy,
            len(selected),
            len(symbols),
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

        self._partition_strategy(table_options)
        if "sym_index" not in partition:
            raise ValueError(
                "KX KDB date-only partition descriptors are not supported; "
                "expected a date_sym descriptor with 'sym' and 'sym_index'."
            )

        sym_column = self._sym_column(table_name, table_options)
        actual_table_name = self._actual_table_name(table_name)
        sym_index = partition.get("sym_index")
        sym_count = partition.get("sym_count")
        return read_kdb_date_sym_records(
            hdb_root_path=self.hdb_root_path,
            kdb_table_name=actual_table_name,
            date_partition=date_partition,
            sym_index=None if sym_index is None else int(sym_index),
            sym_count=None if sym_count is None else int(sym_count),
            runtime_config=self.runtime_config,
            column_defs=self._column_defs(table_name, table_options),
            sym_column=sym_column,
        )

    def _required_option(self, key: str) -> str:
        value = str(self.options.get(key, "")).strip()
        if not value:
            raise ValueError(f"KX KDB connector requires {key!r} in connection options")
        return value

    def _required_absolute_path_option(self, key: str) -> str:
        value = self._required_option(key)
        if not Path(value).is_absolute():
            raise ValueError(
                f"KX KDB connector requires {key!r} to be an absolute filesystem path"
            )
        return value

    def _int_option(self, key: str, default: int) -> int:
        value = str(self.options.get(key, "")).strip()
        if not value:
            return default
        return int(value)

    def _ingestion_mode(self, table_options: dict[str, str]) -> str:
        mode = str(table_options.get("ingestion_mode", "append")).strip().lower()
        if mode != "append":
            raise ValueError(
                f"Unsupported ingestion_mode {mode!r}. KX KDB supports append only."
            )
        return mode

    def _partition_strategy(self, table_options: dict[str, str]) -> str:
        strategy = str(table_options.get("partition_strategy", "date_sym")).strip().lower()
        if strategy != "date_sym":
            raise ValueError(
                f"Unsupported partition_strategy {strategy!r}. KX KDB supports 'date_sym' only."
            )
        self._ingestion_mode(table_options)
        return strategy

    def _sym_column(self, table_name: str, table_options: dict[str, str]) -> str:
        value = validate_hdb_name(
            str(table_options.get("sym_column", "sym")).strip(), "sym_column"
        )
        column = next(
            (
                column
                for column in self._column_defs(table_name, table_options)
                if column["name"].lower() == value.lower()
            ),
            None,
        )
        if column is None:
            raise ValueError(f"sym_column {value!r} is not a column of table {table_name!r}")
        if column.get("q_type", "s") != "s":
            raise ValueError(
                f"sym_column {value!r} must be an enumerated symbol column; "
                f"found q type {column.get('q_type')!r}"
            )
        return value

    def _symbols(self) -> list[str]:
        signature = sym_file_signature(self.hdb_root_path)
        if self._sym_cache is None or self._sym_cache[0] != signature:
            symbols = load_sym_enumeration(self.hdb_root_path, self.runtime_config)
            self._sym_cache = (signature, symbols)
        return self._sym_cache[1]

    def _actual_table_name(self, table_name: str) -> str:
        cache_key = table_name.lower()
        if cache_key not in self._table_dir_cache:
            self._table_dir_cache[cache_key] = resolve_table_directory_name(
                self.hdb_root_path, table_name
            )
        return self._table_dir_cache[cache_key]

    def _available_partitions(
        self, table_name: str, table_options: dict[str, str]
    ) -> list[str]:
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
