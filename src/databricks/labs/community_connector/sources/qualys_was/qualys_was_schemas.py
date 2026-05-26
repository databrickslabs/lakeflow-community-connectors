"""Static schemas/metadata for qualys_was."""

from pyspark.sql.types import LongType, StringType, StructField, StructType

WEB_APPS_SCHEMA = StructType([
    StructField("webapp_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True),
    StructField("owner_id", LongType(), True),
    StructField("created_date", StringType(), True),
    StructField("updated_date", StringType(), True),
    StructField("last_scan_date", StringType(), True),
    StructField("last_scan_status", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

WEB_FINDINGS_SCHEMA = StructType([
    StructField("finding_id", LongType(), False),
    StructField("webapp_id", LongType(), True),
    StructField("qid", LongType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("type", StringType(), True),
    StructField("first_found", StringType(), True),
    StructField("last_found", StringType(), True),
    StructField("qds", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

WAS_SCANS_SCHEMA = StructType([
    StructField("scan_id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("reference", StringType(), True),
    StructField("webapp_id", LongType(), True),
    StructField("scan_type", StringType(), True),
    StructField("scan_mode", StringType(), True),
    StructField("status", StringType(), True),
    StructField("launched_date", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

WAS_SCAN_RESULTS_SCHEMA = StructType([
    StructField("scan_id", LongType(), False),
    StructField("reference", StringType(), True),
    StructField("status", StringType(), True),
    StructField("result_date", StringType(), True),
    StructField("finding_count", LongType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "web_apps": WEB_APPS_SCHEMA,
    "web_findings": WEB_FINDINGS_SCHEMA,
    "was_scans": WAS_SCANS_SCHEMA,
    "was_scan_results": WAS_SCAN_RESULTS_SCHEMA,
}

TABLE_METADATA = {
    "web_apps": {"primary_keys": ["webapp_id"], "cursor_field": "updated_date", "ingestion_type": "cdc"},
    "web_findings": {"primary_keys": ["finding_id"], "cursor_field": "last_found", "ingestion_type": "cdc"},
    "was_scans": {"primary_keys": ["scan_id"], "cursor_field": "launched_date", "ingestion_type": "cdc"},
    "was_scan_results": {"primary_keys": ["scan_id"], "cursor_field": "result_date", "ingestion_type": "cdc"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
