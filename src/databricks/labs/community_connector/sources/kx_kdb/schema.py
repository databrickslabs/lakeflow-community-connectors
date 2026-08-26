"""Schema inference for KX KDB HDB tables."""

from __future__ import annotations

import json
import logging
from typing import List, Optional

from databricks.labs.community_connector.sources.kx_kdb.filesystem import hdb_child_path
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    PyKxRuntimeConfig,
    prepare_pykx,
)

logger = logging.getLogger(__name__)

Q_META_TYPE_TO_SPARK = {
    "b": "BooleanType",
    "x": "ShortType",
    "h": "ShortType",
    "i": "IntegerType",
    "j": "LongType",
    "e": "FloatType",
    "f": "DoubleType",
    "c": "StringType",
    "s": "StringType",
    "p": "TimestampType",
    "d": "StringType",
    "z": "TimestampType",
    "n": "LongType",
    "u": "StringType",
    "v": "StringType",
    "t": "StringType",
    "g": "StringType",
    " ": "StringType",
    "C": "StringType",
}


def _dedupe_column_defs(columns: List[dict]) -> List[dict]:
    """Return column definitions with unique case-insensitive names."""
    deduped: List[dict] = []
    seen = set()
    for column in columns:
        name = str(column.get("name", "")).strip()
        if not name:
            continue
        lowered = name.lower()
        if lowered in seen:
            logger.warning("Dropping duplicate inferred column: %s", name)
            continue
        seen.add(lowered)
        deduped.append(
            {"name": name, "spark_type": str(column.get("spark_type", "StringType"))}
        )
    return deduped


def _clean_type_char(type_char) -> str:
    value = type_char
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("latin1", "ignore")
    text = str(value or "").strip().strip("`").strip()
    if (
        len(text) >= 3
        and text[0] in {"b", "B"}
        and text[1] in {"'", '"'}
        and text[-1] in {"'", '"'}
    ):
        text = text[2:-1]
    text = text.strip().strip("'").strip('"').strip()
    return text[-1] if text else ""


def _map_meta_type_char(type_char) -> str:
    return Q_META_TYPE_TO_SPARK.get(_clean_type_char(type_char), "StringType")


def infer_schema_from_partition(
    hdb_root_path: str,
    kdb_table_name: str,
    date_partition: str,
    runtime_config: PyKxRuntimeConfig,
) -> List[dict]:
    """Infer column definitions from a single KDB partition."""
    kx = prepare_pykx(runtime_config)
    columns = _try_infer_via_db_query(kx, hdb_root_path, kdb_table_name)
    if columns is None:
        columns = _try_infer_via_get_partition(
            kx, hdb_root_path, kdb_table_name, date_partition
        )
    if columns is None:
        columns = _try_infer_via_splayed_column_files(
            kx, hdb_root_path, kdb_table_name, date_partition
        )

    if columns is None:
        raise RuntimeError(
            f"Could not infer schema for {kdb_table_name} from "
            f"{_partition_table_path(hdb_root_path, kdb_table_name, date_partition)}"
        )

    return _with_date_column(columns)


def _with_date_column(columns: List[dict]) -> List[dict]:
    has_date = any(str(column.get("name", "")).strip().lower() == "date" for column in columns)
    result = []
    if not has_date:
        result.append({"name": "date", "spark_type": "StringType"})
    result.extend(columns)
    return _dedupe_column_defs(result)


def _try_infer_via_splayed_column_files(
    kx,
    hdb_root_path: str,
    table_name: str,
    date_partition: str,
) -> Optional[List[dict]]:
    """Infer schema from a splayed table directory without opening the full HDB."""
    try:
        table_path = _partition_table_path(hdb_root_path, table_name, date_partition)
        columns = []
        for column_name in _column_names_from_splayed_d(kx, table_path):
            columns.append(
                {
                    "name": column_name,
                    "spark_type": _infer_column_file_type(
                        kx, hdb_child_path(table_path, column_name)
                    ),
                }
            )
        return columns if columns else None
    except Exception as exc:
        logger.debug("Splayed file schema inference failed for %s: %s", table_name, exc)
        return None


def _column_names_from_splayed_d(kx, table_path: str) -> list[str]:
    result = kx.q("{[p] get hsym p}", hdb_child_path(table_path, ".d"))
    return _q_list_to_strings(result)


def _infer_column_file_type(kx, column_path: str) -> str:
    type_chars = _q_list_to_strings(kx.q("{[p] string .Q.ty type get hsym p}", column_path))
    return _map_meta_type_char(type_chars[0] if type_chars else "")


def _q_list_to_strings(value) -> list[str]:
    if hasattr(value, "py"):
        try:
            value = value.py()
        except Exception:
            pass
    if hasattr(value, "tolist"):
        try:
            value = value.tolist()
        except Exception:
            pass
    if isinstance(value, (str, bytes)):
        values = [value]
    else:
        values = list(value)
    result = []
    for item in values:
        text = item.decode("utf-8") if isinstance(item, bytes) else str(item)
        text = text.strip().strip("`")
        if text:
            result.append(text)
    return result


def _columns_from_meta(kx, table_expr: str) -> Optional[List[dict]]:
    names = _q_list_to_strings(kx.q(f"string (0!meta {table_expr})`c"))
    type_chars = _q_list_to_strings(kx.q(f"string (0!meta {table_expr})`t"))
    if not names:
        return None
    return [
        {
            "name": names[index],
            "spark_type": _map_meta_type_char(type_chars[index] if index < len(type_chars) else ""),
        }
        for index in range(len(names))
    ]


def _try_infer_via_db_query(kx, hdb_root_path: str, table_name: str) -> Optional[List[dict]]:
    try:
        kx.DB(path=hdb_root_path)
        return _columns_from_meta(kx, table_name)
    except Exception as exc:
        logger.debug("DB schema inference failed for %s: %s", table_name, exc)
        return None


def _try_infer_via_get_partition(
    kx,
    hdb_root_path: str,
    table_name: str,
    date_partition: str,
) -> Optional[List[dict]]:
    try:
        partition_path = _partition_table_path(hdb_root_path, table_name, date_partition)
        kx.q("{[p] __lakeflow_schema_tmp: get hsym p}", partition_path)
        columns = _columns_from_meta(kx, "__lakeflow_schema_tmp")

        try:
            kx.q("delete __lakeflow_schema_tmp from `.")
        except Exception:
            pass
        return columns if columns else None
    except Exception as exc:
        logger.debug("Partition schema inference failed for %s: %s", table_name, exc)
        return None


def _partition_table_path(
    hdb_root_path: str,
    table_name: str,
    date_partition: str,
) -> str:
    return hdb_child_path(hdb_root_path, date_partition, table_name)


def columns_to_spark_schema(columns: List[dict]):
    """Convert a list of column definitions to a StructType."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_map = {
        "BooleanType": BooleanType(),
        "ShortType": ShortType(),
        "IntegerType": IntegerType(),
        "LongType": LongType(),
        "FloatType": FloatType(),
        "DoubleType": DoubleType(),
        "StringType": StringType(),
        "TimestampType": TimestampType(),
    }

    fields = []
    for column in _dedupe_column_defs(columns):
        fields.append(
            StructField(
                column["name"],
                type_map.get(column["spark_type"], StringType()),
                nullable=True,
            )
        )
    return StructType(fields)


def serialize_columns(columns: List[dict]) -> str:
    return json.dumps(columns)


def deserialize_columns(columns_json: str) -> List[dict]:
    return json.loads(columns_json)
