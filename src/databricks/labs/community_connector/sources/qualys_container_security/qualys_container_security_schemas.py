"""Static schemas/metadata for qualys_container_security."""

from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructField, StructType

CONTAINERS_SCHEMA = StructType([
    StructField("container_sha", StringType(), False),
    StructField("container_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("image_sha", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("host_name", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CONTAINER_IMAGES_SCHEMA = StructType([
    StructField("image_sha", StringType(), False),
    StructField("image_id", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("in_use", BooleanType(), True),
    StructField("registry", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CONTAINER_VULNERABILITIES_SCHEMA = StructType([
    StructField("finding_id", StringType(), False),
    StructField("image_sha", StringType(), True),
    StructField("container_sha", StringType(), True),
    StructField("qid", LongType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("first_found", StringType(), True),
    StructField("last_found", StringType(), True),
    StructField("cve", ArrayType(StringType(), True), True),
    StructField("result", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

REGISTRIES_SCHEMA = StructType([
    StructField("registry_id", StringType(), False),
    StructField("registry_name", StringType(), True),
    StructField("registry_type", StringType(), True),
    StructField("registry_uri", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SENSORS_SCHEMA = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("hostname", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("sensor_version", StringType(), True),
    StructField("ipv4", StringType(), True),
    StructField("last_checked_in", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

CONTAINER_SCAN_RESULTS_SCHEMA = StructType([
    StructField("scan_result_id", StringType(), False),
    StructField("image_sha", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("severity_count", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "containers": CONTAINERS_SCHEMA,
    "container_images": CONTAINER_IMAGES_SCHEMA,
    "container_vulnerabilities": CONTAINER_VULNERABILITIES_SCHEMA,
    "registries": REGISTRIES_SCHEMA,
    "sensors": SENSORS_SCHEMA,
    "container_scan_results": CONTAINER_SCAN_RESULTS_SCHEMA,
}

TABLE_METADATA = {
    "containers": {"primary_keys": ["container_sha"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "container_images": {"primary_keys": ["image_sha"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
    "container_vulnerabilities": {"primary_keys": ["finding_id"], "cursor_field": "last_found", "ingestion_type": "cdc"},
    "registries": {"primary_keys": ["registry_id"], "cursor_field": None, "ingestion_type": "snapshot"},
    "sensors": {"primary_keys": ["sensor_id"], "cursor_field": None, "ingestion_type": "snapshot"},
    "container_scan_results": {"primary_keys": ["scan_result_id"], "cursor_field": "updated_at", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
