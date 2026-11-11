"""Fixed schema for every MongoDB collection.

MongoDB collections are schemaless, so the connector does not sample
documents to infer a schema (which is non-deterministic and breaks when a
new field appears). Every collection gets the same columns:

``_id``                 string     primary key (see ``mongodb_utils.encode_id``)
``document``            variant    the full document as Extended JSON (relaxed);
                                    string when ``document_format=string``
``_cdc_operation``      string     snapshot | insert | update | replace | delete
``_cdc_sequence``       string     ordering key: the change stream resume token
                                    (hex, totally ordered); snapshot rows carry the
                                    token taken just before the snapshot started
``_cdc_cluster_time``   timestamp  cluster time of the change (null for snapshot rows)
``_cdc_wall_time``      timestamp  wall time of the change, MongoDB 6.0+ (null otherwise)
``_cdc_namespace``      string     "<database>.<collection>"
"""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

DOCUMENT_FORMATS = ("variant", "string")

PRIMARY_KEY = "_id"
CURSOR_FIELD = "_cdc_sequence"


def table_schema(document_format: str = "variant") -> StructType:
    document_type = VariantType() if document_format == "variant" else StringType()
    return StructType(
        [
            StructField("_id", StringType(), False),
            StructField("document", document_type, True),
            StructField("_cdc_operation", StringType(), False),
            StructField("_cdc_sequence", StringType(), False),
            StructField("_cdc_cluster_time", TimestampType(), True),
            StructField("_cdc_wall_time", TimestampType(), True),
            StructField("_cdc_namespace", StringType(), False),
        ]
    )
