"""Schemas, metadata, constants, and Arrow→Spark type mapping for LanceDB.

LanceDB Cloud tables are **user-defined and fully dynamic** — there is no
fixed object list.  In production, :meth:`list_tables` discovers tables via
``GET /v1/table/`` and :meth:`get_table_schema` derives the Spark schema from
``POST /v1/table/{name}/describe/``.

The ``TABLES`` / ``TABLE_SCHEMAS`` maps below therefore describe a small set of
*example* tables used only as:

* a fallback when discovery / describe returns nothing (e.g. offline
  simulate-mode tests, mirroring the HubSpot connector's minimal-schema
  fallback), and
* the seed the simulator's ``corpus_from_schema`` tool uses to synthesize a
  record corpus for those tables.

Reads are snapshot-only: LanceDB's REST API exposes no primary keys, no
cursor/watermark, and no change/delete feed, so every table is read as a full
snapshot and incremental (cdc) reads are not supported.
"""

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ----- HTTP / retry policy ---------------------------------------------------

# Retried with exponential backoff (see ``_request_with_retry``).  LanceDB does
# not publish rate limits, so we retry generic transient errors and honour the
# ``Retry-After`` header when present.
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
DEFAULT_TIMEOUT = 60  # seconds; every request sets an explicit timeout

# LanceDB's query API is top-K; ``k`` doubles as the page size for full scans.
# Production usage caps a single request near ~10k rows.
DEFAULT_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000
# Page size for the List Tables endpoint (no documented default/max; 100 is a
# previously-validated value carried over from the reference implementation).
LIST_TABLES_LIMIT = 100

# ----- example table catalogue (fallback + corpus seed) ----------------------

TABLES = [
    "documents",
    "embeddings",
]


def _documents_schema() -> StructType:
    """A text-plus-embedding table — the canonical LanceDB RAG shape."""
    return StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("text", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("embedding", ArrayType(FloatType()), nullable=True),
            StructField("updated_at", TimestampType(), nullable=True),
        ]
    )


def _embeddings_schema() -> StructType:
    """A bare vector table keyed by an integer id."""
    return StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("source", StringType(), nullable=True),
            StructField("vector", ArrayType(FloatType()), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
        ]
    )


TABLE_SCHEMAS = {
    "documents": _documents_schema(),
    "embeddings": _embeddings_schema(),
}


# ----- Arrow → Spark type mapping --------------------------------------------


def arrow_type_to_spark_type(arrow_type) -> DataType:
    """Map a LanceDB ``describe`` field type to a Spark ``DataType``.

    The ``type`` value may arrive as a structured object
    (``{"type": "int64"}``, ``{"type": "fixed_size_list", "length": 384}``) or,
    on some SDK/tooling paths, as a flattened string
    (``"fixed_size_list<float32>[384]"``).  Both shapes are handled.
    """
    if isinstance(arrow_type, dict):
        return _dict_arrow_to_spark(arrow_type)
    if isinstance(arrow_type, str):
        return _string_arrow_to_spark(arrow_type)
    return StringType()


def _dict_arrow_to_spark(arrow_type: dict) -> DataType:
    type_name = str(arrow_type.get("type", "")).lower()

    if type_name in ("fixed_size_list", "list", "large_list"):
        element = _list_element_type(arrow_type)
        return ArrayType(element, True)
    if type_name == "struct":
        children = arrow_type.get("fields") or []
        spark_fields = [
            StructField(
                child.get("name", ""),
                arrow_type_to_spark_type(child.get("type", {})),
                child.get("nullable", True),
            )
            for child in children
            if isinstance(child, dict)
        ]
        # Prefer an explicit StructType over MapType (house style); fall back to
        # StringType when the nested field list is unavailable.
        return StructType(spark_fields) if spark_fields else StringType()
    return _scalar_arrow_to_spark(type_name)


def _list_element_type(arrow_type: dict) -> DataType:
    """Resolve the element type of a (fixed-size) list field.

    Embedding columns are numeric; default to ``FloatType`` when the inner type
    is unknown (LanceDB's structured form omits it for ``fixed_size_list``).
    """
    inner = (
        arrow_type.get("value_type")
        or arrow_type.get("element_type")
        or arrow_type.get("field")
    )
    if isinstance(inner, dict):
        return arrow_type_to_spark_type(inner.get("type", inner))
    if isinstance(inner, str):
        return _scalar_arrow_to_spark(inner.lower())
    return FloatType()


def _scalar_arrow_to_spark(type_name: str) -> DataType:
    """Map a scalar Arrow type name to a Spark type.

    Integer widths all map to ``LongType`` to avoid overflow (house style).
    """
    if type_name in ("string", "utf8", "large_string", "large_utf8"):
        return StringType()
    if type_name in ("int64", "long", "int32", "int16", "int8", "int", "uint32",
                      "uint64", "uint16", "uint8"):
        return LongType()
    if type_name in ("float64", "double"):
        return DoubleType()
    if type_name in ("float32", "float", "float16", "halffloat"):
        return FloatType()
    if type_name in ("bool", "boolean"):
        return BooleanType()
    if type_name in ("binary", "large_binary", "fixed_size_binary"):
        return BinaryType()
    if type_name.startswith("timestamp"):
        return TimestampType()
    if type_name in ("date", "date32", "date64"):
        return DateType()
    # Safe fallback for unrecognized types.
    return StringType()


def _string_arrow_to_spark(arrow_type: str) -> DataType:
    """Map a flattened string Arrow type (e.g. ``fixed_size_list<float32>[384]``)."""
    lowered = arrow_type.lower()
    if "fixed_size_list" in lowered or "list" in lowered:
        if "float64" in lowered or "double" in lowered:
            return ArrayType(DoubleType(), True)
        if "int" in lowered:
            return ArrayType(LongType(), True)
        return ArrayType(FloatType(), True)
    if lowered.startswith("timestamp"):
        return TimestampType()
    return _scalar_arrow_to_spark(lowered)


def vector_dimension(field_type) -> int | None:
    """Return the fixed-size-list dimension for a ``describe`` field type.

    Used to size the all-zero dummy vector for full-table scans.  Handles the
    structured (``length``) and flattened (``[N]``) forms; returns ``None`` when
    the field is not a fixed-size list.
    """
    if isinstance(field_type, dict):
        if str(field_type.get("type", "")).lower() == "fixed_size_list":
            length = field_type.get("length")
            if isinstance(length, int) and length > 0:
                return length
        return None
    if isinstance(field_type, str):
        lowered = field_type.lower()
        if "fixed_size_list" in lowered and "[" in lowered and "]" in lowered:
            try:
                return int(lowered.rsplit("[", 1)[1].split("]", 1)[0])
            except (ValueError, IndexError):
                return None
    return None
