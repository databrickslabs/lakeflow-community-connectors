"""Lakeflow community connector for KX KDB HDB files."""

from __future__ import annotations

import json
import logging
from itertools import chain
from pathlib import Path
from typing import Iterator, Sequence

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.kx_kdb.conversion import (
    DEFAULT_PARTITION_CONVERSION_MODE,
)
from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    discover_table_partitions,
    discover_tables,
    normalize_partition_date,
    resolve_table_directory_name,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    build_runtime_config,
)
from databricks.labs.community_connector.sources.kx_kdb.schema import (
    columns_to_spark_schema,
    infer_schema_from_partition,
)
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import (
    load_sym_enumeration_with_indices,
    read_kdb_date_sym_records,
)

logger = logging.getLogger(__name__)


class KxKdbLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """Read immutable KDB HDB date partitions through the Lakeflow connector APIs."""

    def __init__(self, options: dict[str, str]) -> None:
        options = self._with_table_config_defaults(options)
        super().__init__(options)
        self.hdb_root_path = self._required_absolute_path_option("hdb_root_path")
        self.license_path = self._required_absolute_path_option("license_volume_path")
        # Resolve any KDB-X bootstrap secrets on the driver so the resulting
        # runtime config can travel with the serialized connector object.
        self.runtime_config = build_runtime_config(self.options)
        self.license_path = self.runtime_config.license_directory
        self.discovery_sample_dates = self._int_option("discovery_sample_dates", 0)
        self._schema_cache: dict[tuple[str, str], StructType] = {}
        self._column_cache: dict[tuple[str, str], list[dict]] = {}
        self._table_dir_cache: dict[str, str] = {}
        self._partition_cache: dict[tuple[str, str | None, str | None], list[str]] = {}
        self._sym_cache: tuple[list[str], dict[str, int]] | None = None

    def _with_table_config_defaults(self, options: dict[str, str]) -> dict[str, str]:
        """Let pipeline tableConfigs override connection-level defaults."""
        raw_configs = options.get("tableConfigs")
        if not raw_configs:
            return options
        try:
            configs = json.loads(raw_configs)
        except Exception:
            return options
        if not isinstance(configs, dict):
            return options

        for config in configs.values():
            if isinstance(config, dict):
                return {**options, **config}
        return options

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

        symbols, sym_indices = self._symbols_with_indices()
        descriptors = [
            {"date_partition": partition, "sym": symbol, "sym_index": sym_indices[symbol]}
            for partition in selected
            for symbol in symbols
            if symbol in sym_indices
        ]
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

        actual_table_name = self._actual_table_name(table_name)
        return read_kdb_date_sym_records(
            hdb_root_path=self.hdb_root_path,
            kdb_table_name=actual_table_name,
            date_partition=date_partition,
            symbol=str(partition.get("sym", "")),
            sym_index=int(partition.get("sym_index", -1)),
            runtime_config=self.runtime_config,
            column_defs=self._column_defs(table_name, table_options),
            sym_column=self._sym_column(table_options),
            conversion_mode=self._partition_conversion_mode(table_options),
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

    def _partition_conversion_mode(self, table_options: dict[str, str]) -> str:
        value = str(
            table_options.get(
                "partition_conversion_mode", DEFAULT_PARTITION_CONVERSION_MODE
            )
        ).strip()
        return value or DEFAULT_PARTITION_CONVERSION_MODE

    def _sym_column(self, table_options: dict[str, str]) -> str:
        value = str(table_options.get("sym_column", "sym")).strip()
        if not value:
            raise ValueError("sym_column must be a non-empty KDB partition column name")
        return value

    def _symbols_with_indices(self) -> tuple[list[str], dict[str, int]]:
        if self._sym_cache is None:
            self._sym_cache = load_sym_enumeration_with_indices(
                self.hdb_root_path,
                self.runtime_config,
            )
        return self._sym_cache

    def _actual_table_name(self, table_name: str) -> str:
        cache_key = table_name.lower()
        if cache_key not in self._table_dir_cache:
            self._table_dir_cache[cache_key] = resolve_table_directory_name(
                self.hdb_root_path,
                table_name,
                self.discovery_sample_dates,
            )
        return self._table_dir_cache[cache_key]

    def _available_partitions(
        self, table_name: str, table_options: dict[str, str]
    ) -> list[str]:
        start_date = normalize_partition_date(table_options.get("start_date"))
        end_date = normalize_partition_date(table_options.get("end_date"))
        cache_key = (table_name.lower(), start_date, end_date)
        if cache_key not in self._partition_cache:
            self._partition_cache[cache_key] = discover_table_partitions(
                root_path=self.hdb_root_path,
                table_name=table_name,
                start_date=start_date,
                end_date=end_date,
            )
        return self._partition_cache[cache_key]

    def _column_defs(self, table_name: str, table_options: dict[str, str]) -> list[dict]:
        cache_key = (table_name.lower(), self._ingestion_mode(table_options))
        if cache_key in self._column_cache:
            return self._column_cache[cache_key]

        partitions = self._available_partitions(table_name, table_options)
        if not partitions:
            raise RuntimeError(
                f"No partitions found for table {table_name!r} under {self.hdb_root_path}"
            )

        columns = infer_schema_from_partition(
            hdb_root_path=self.hdb_root_path,
            kdb_table_name=self._actual_table_name(table_name),
            date_partition=partitions[0],
            runtime_config=self.runtime_config,
        )

        self._column_cache[cache_key] = columns
        return columns
