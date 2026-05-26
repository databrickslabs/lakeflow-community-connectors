"""Static schemas/metadata for qualys_vmdr."""

from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

ASSETS_SCHEMA = StructType([
    StructField("host_id", LongType(), False),
    StructField("asset_id", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("dns", StringType(), True),
    StructField("netbios", StringType(), True),
    StructField("os", StringType(), True),
    StructField("tracking_method", StringType(), True),
    StructField("last_vuln_scan", StringType(), True),
    StructField("last_compliance_scan", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

HOSTS_SCHEMA = ASSETS_SCHEMA

DETECTIONS_SCHEMA = StructType([
    StructField("detection_id", StringType(), False),
    StructField("host_id", LongType(), True),
    StructField("ip", StringType(), True),
    StructField("qid", LongType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("first_found", StringType(), True),
    StructField("last_found", StringType(), True),
    StructField("last_fixed", StringType(), True),
    StructField("times_found", LongType(), True),
    StructField("qds", StringType(), True),
    StructField("cve", ArrayType(StringType(), True), True),
    StructField("result", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

VULNERABILITIES_SCHEMA = DETECTIONS_SCHEMA
FINDINGS_SCHEMA = DETECTIONS_SCHEMA

KNOWLEDGEBASE_SCHEMA = StructType([
    StructField("qid", LongType(), False),
    StructField("title", StringType(), True),
    StructField("severity_level", StringType(), True),
    StructField("category", StringType(), True),
    StructField("published_date", StringType(), True),
    StructField("last_service_modification_date", StringType(), True),
    StructField("patchable", StringType(), True),
    StructField("cve", ArrayType(StringType(), True), True),
    StructField("vendor_reference", ArrayType(StringType(), True), True),
    StructField("raw_payload", StringType(), True),
])

QIDS_SCHEMA = KNOWLEDGEBASE_SCHEMA

SCANS_SCHEMA = StructType([
    StructField("scan_ref", StringType(), False),
    StructField("scan_id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("type", StringType(), True),
    StructField("state", StringType(), True),
    StructField("launch_date", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("processed", StringType(), True),
    StructField("target", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

SCAN_RESULTS_SCHEMA = StructType([
    StructField("scan_ref", StringType(), False),
    StructField("scan_id", LongType(), True),
    StructField("status", StringType(), True),
    StructField("scan_date", StringType(), True),
    StructField("hosts_scanned", LongType(), True),
    StructField("hosts_alive", LongType(), True),
    StructField("total_vulnerabilities", LongType(), True),
    StructField("raw_payload", StringType(), True),
])

ASSET_GROUPS_SCHEMA = StructType([
    StructField("asset_group_id", LongType(), False),
    StructField("title", StringType(), True),
    StructField("owner_id", LongType(), True),
    StructField("owner", StringType(), True),
    StructField("network_id", LongType(), True),
    StructField("last_update", StringType(), True),
    StructField("raw_payload", StringType(), True),
])

TABLE_SCHEMAS = {
    "assets": ASSETS_SCHEMA,
    "hosts": HOSTS_SCHEMA,
    "detections": DETECTIONS_SCHEMA,
    "vulnerabilities": VULNERABILITIES_SCHEMA,
    "findings": FINDINGS_SCHEMA,
    "knowledgebase": KNOWLEDGEBASE_SCHEMA,
    "qids": QIDS_SCHEMA,
    "scans": SCANS_SCHEMA,
    "scan_results": SCAN_RESULTS_SCHEMA,
    "asset_groups": ASSET_GROUPS_SCHEMA,
}

TABLE_METADATA = {
    "assets": {"primary_keys": ["host_id"], "cursor_field": "last_vuln_scan", "ingestion_type": "cdc"},
    "hosts": {"primary_keys": ["host_id"], "cursor_field": "last_vuln_scan", "ingestion_type": "cdc"},
    "detections": {"primary_keys": ["detection_id"], "cursor_field": "last_found", "ingestion_type": "cdc"},
    "vulnerabilities": {"primary_keys": ["detection_id"], "cursor_field": "last_found", "ingestion_type": "cdc"},
    "findings": {"primary_keys": ["detection_id"], "cursor_field": "last_found", "ingestion_type": "cdc"},
    "knowledgebase": {"primary_keys": ["qid"], "cursor_field": "last_service_modification_date", "ingestion_type": "cdc"},
    "qids": {"primary_keys": ["qid"], "cursor_field": "last_service_modification_date", "ingestion_type": "cdc"},
    "scans": {"primary_keys": ["scan_ref"], "cursor_field": "launch_date", "ingestion_type": "cdc"},
    "scan_results": {"primary_keys": ["scan_ref"], "cursor_field": "scan_date", "ingestion_type": "cdc"},
    "asset_groups": {"primary_keys": ["asset_group_id"], "cursor_field": None, "ingestion_type": "snapshot"},
}

SUPPORTED_TABLES = list(TABLE_SCHEMAS.keys())
