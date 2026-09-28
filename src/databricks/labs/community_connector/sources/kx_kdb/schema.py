"""Schema inference for KX KDB HDB tables."""

from __future__ import annotations

import logging
from typing import List

from databricks.labs.community_connector.sources.kx_kdb.filesystem import (
    hdb_child_path,
    validate_hdb_name,
)
from databricks.labs.community_connector.sources.kx_kdb.runtime import (
    PyKxRuntimeConfig,
    prepare_pykx,
)
from databricks.labs.community_connector.sources.kx_kdb.sym_reader import _ensure_sym_domain

logger = logging.getLogger(__name__)

# Reads one splayed partition directly. Loading the whole HDB would also run
# any q scripts stored in the HDB root.
_SPLAYED_META_Q = "{[p] m:0!meta get hsym p; (string m`c; m`t)}"

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
    """Infer column definitions from one splayed KDB date partition."""
    table_path = hdb_child_path(hdb_root_path, date_partition, kdb_table_name)
    kx = prepare_pykx(runtime_config)
    try:
        _ensure_sym_domain(kx, hdb_root_path)
        meta = kx.q(_SPLAYED_META_Q, f"{table_path}/")
    except Exception as exc:
        raise RuntimeError(
            f"Could not infer schema for {kdb_table_name} from {table_path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    return _with_date_column(_meta_columns(meta))


def _meta_columns(meta) -> List[dict]:
    value = _to_py(meta)
    try:
        raw_names, raw_types = value
    except (TypeError, ValueError):
        raise RuntimeError("KDB meta returned an unexpected shape.") from None

    names = [_text(item) for item in _to_py(raw_names)]
    types = _type_chars(_to_py(raw_types))
    if len(names) != len(types):
        raise RuntimeError(
            f"KDB meta returned {len(names)} column names and {len(types)} type codes."
        )
    return [
        {
            "name": validate_hdb_name(name, "column name"),
            "spark_type": _map_meta_type_char(type_char),
        }
        for name, type_char in zip(names, types)
    ]


def _to_py(value):
    if hasattr(value, "py"):
        try:
            return value.py()
        except Exception:
            return value
    return value


def _text(value) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(value)


def _type_chars(value) -> list[str]:
    if isinstance(value, (bytes, bytearray)):
        return list(value.decode("latin1"))
    if isinstance(value, str):
        return list(value)
    return [_text(item) for item in value]


def _with_date_column(columns: List[dict]) -> List[dict]:
    has_date = any(str(column.get("name", "")).strip().lower() == "date" for column in columns)
    result = []
    if not has_date:
        result.append({"name": "date", "spark_type": "StringType"})
    result.extend(columns)
    return _dedupe_column_defs(result)


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
