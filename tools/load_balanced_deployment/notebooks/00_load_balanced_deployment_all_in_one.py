# Databricks notebook source
# MAGIC %md
# MAGIC # Load-Balanced Multi-Pipeline Deployment - All-in-One
# MAGIC
# MAGIC This notebook provides a complete end-to-end workflow for deploying load-balanced pipelines.
# MAGIC
# MAGIC **What this notebook does**:
# MAGIC 1. (Optional) Auto-discover tables from connector and classify into groups
# MAGIC 2. Generate multiple Python ingest files (`ingest_*.py`) - one per group
# MAGIC 3. Generate Databricks Asset Bundle (DAB) YAML configuration
# MAGIC 4. Upload ingest files to workspace
# MAGIC 5. Deploy pipelines using DAB
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - UC Connection created: `community-connector create_connection <source> <conn_name> -o '{...}'`
# MAGIC - Connector source code deployed to workspace
# MAGIC - `databricks` CLI configured

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set your configuration parameters below.

# COMMAND ----------

# CONNECTOR SETTINGS
CONNECTOR_NAME = "osipi"  # Change to your connector: osipi, hubspot, github, etc.
CONNECTION_NAME = "osipi_connection_lakeflow"  # UC Connection name
DEST_CATALOG = "osipi"
DEST_SCHEMA = "bronze"

# DISCOVERY OPTIONS (set USE_PRESET=True to skip discovery)
USE_PRESET = False  # Set to True to use preset CSV instead of discovery
PRESET_CSV_PATH = f"../examples/{CONNECTOR_NAME}/preset_by_category_and_ingestion.csv"

# GROUPING STRATEGY (only used if USE_PRESET=False)
GROUP_BY = "category_and_ingestion_type"  # Options: category_and_ingestion_type, ingestion_type, category, none

# SCHEDULES (cron expressions, leave empty for no scheduled jobs)
SCHEDULE_SNAPSHOT = "0 0 * * *"      # Daily at midnight
SCHEDULE_APPEND = "*/15 * * * *"     # Every 15 minutes
SCHEDULE_CDC = "*/5 * * * *"         # Every 5 minutes
SCHEDULE_UNKNOWN = ""                # No schedule

# DEPLOYMENT SETTINGS
import os
import uuid

USERNAME = spark.sql("SELECT current_user()").collect()[0][0]
DATABRICKS_PROFILE = "dogfood"  # Change this to your Databricks CLI profile name

# OUTPUT PATHS (using DBFS paths accessed via dbfs: scheme)
# Note: Use dbfs:/ scheme for DBFS operations, /dbfs/ for local file access
WORK_DIR_DBFS = f"dbfs:/tmp/{USERNAME.replace('@', '_').replace('.', '_')}/load_balanced_deployment_{uuid.uuid4().hex[:8]}"
WORK_DIR_LOCAL = WORK_DIR_DBFS.replace("dbfs:", "/dbfs")  # Local mount path for file operations

CSV_PATH = f"{WORK_DIR_LOCAL}/{CONNECTOR_NAME}_tables.csv"
INGEST_FILES_DIR = f"{WORK_DIR_LOCAL}/{CONNECTOR_NAME}_ingest_files"
DAB_YAML_PATH = f"{WORK_DIR_LOCAL}/{CONNECTOR_NAME}_bundle/databricks.yml"
WORKSPACE_INGEST_PATH = f"/Workspace/Users/{USERNAME}/{CONNECTOR_NAME}_ingest"
CLUSTER_NUM_WORKERS = 2
EMIT_SCHEDULED_JOBS = True
PAUSE_JOBS = True  # Create jobs in PAUSED state

# Tool scripts workspace path - will be copied to DBFS in Step 1 for subprocess access
TOOLS_DIR_WORKSPACE = f"/Workspace/Users/{USERNAME}/lakeflow-community-connectors/tools/load_balanced_deployment"
# Connector source path in workspace - needed for discovery
CONNECTOR_SOURCE_WORKSPACE = f"/Workspace/Users/{USERNAME}/lakeflow-community-connectors/sources/{CONNECTOR_NAME}/{CONNECTOR_NAME}.py"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover and Classify Tables (Optional)
# MAGIC
# MAGIC **Skip this step** if you're using a preset CSV.

# COMMAND ----------

import subprocess
import os
import shutil
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
import base64

# Create work directories using dbutils.fs for DBFS
dbutils.fs.mkdirs(WORK_DIR_DBFS)
dbutils.fs.mkdirs(f"{WORK_DIR_DBFS}/{CONNECTOR_NAME}_ingest_files")
dbutils.fs.mkdirs(f"{WORK_DIR_DBFS}/{CONNECTOR_NAME}_bundle")
print(f"Using work directory: {WORK_DIR_LOCAL}")

# Copy tool scripts to DBFS work directory for subprocess access
# The /Workspace/Repos/ path is not accessible from subprocess in notebooks
SCRIPTS_DIR_DBFS = f"{WORK_DIR_DBFS}/scripts"
SCRIPTS_DIR = f"{WORK_DIR_LOCAL}/scripts"
dbutils.fs.mkdirs(SCRIPTS_DIR_DBFS)

# Get the current notebook's directory to find scripts
# Use workspace API to export scripts from Repos
w = WorkspaceClient()
script_files = [
    "discover_and_classify_tables.py",
    "generate_ingest_files.py",
    "generate_dab_yaml.py",
    "utils.py"
]

print(f"Copying tool scripts to {SCRIPTS_DIR_DBFS}...")
for script_name in script_files:
    workspace_path = f"{TOOLS_DIR_WORKSPACE}/{script_name}"
    dbfs_path = f"{SCRIPTS_DIR_DBFS}/{script_name}"

    try:
        # Export from workspace
        response = w.workspace.export(workspace_path, format=ExportFormat.SOURCE)
        # Decode base64 content
        content = base64.b64decode(response.content).decode('utf-8')
        # Write to DBFS using dbutils.fs.put (overwrites if exists)
        dbutils.fs.put(dbfs_path, content, overwrite=True)
        print(f"  ✓ Copied {script_name}")
    except Exception as e:
        print(f"  ✗ Failed to copy {script_name}: {e}")

# Copy connector source file to DBFS (needed for discovery)
connector_dbfs_path = f"{SCRIPTS_DIR_DBFS}/{CONNECTOR_NAME}.py"
print(f"Copying connector source to {connector_dbfs_path}...")
try:
    response = w.workspace.export(CONNECTOR_SOURCE_WORKSPACE, format=ExportFormat.SOURCE)
    content = base64.b64decode(response.content).decode('utf-8')
    dbutils.fs.put(connector_dbfs_path, content, overwrite=True)
    print(f"  ✓ Copied {CONNECTOR_NAME}.py")
except Exception as e:
    print(f"  ✗ Failed to copy connector source: {e}")
    print(f"  Note: Discovery will attempt to find connector at: {CONNECTOR_SOURCE_WORKSPACE}")

# Point TOOLS_DIR to copied scripts for subprocess calls
TOOLS_DIR = SCRIPTS_DIR
CONNECTOR_PY_FILE = f"{SCRIPTS_DIR}/{CONNECTOR_NAME}.py"

if not USE_PRESET:
    print(f"Discovering tables from {CONNECTOR_NAME} connector...")

    cmd = [
        "python3",
        f"{TOOLS_DIR}/discover_and_classify_tables.py",
        "--connector-name", CONNECTOR_NAME,
        "--connector-python-file", CONNECTOR_PY_FILE,
        "--output-csv", CSV_PATH,
        "--connection-name", CONNECTION_NAME,
        "--dest-catalog", DEST_CATALOG,
        "--dest-schema", DEST_SCHEMA,
        "--group-by", GROUP_BY,
        "--schedule-snapshot", SCHEDULE_SNAPSHOT,
        "--schedule-append", SCHEDULE_APPEND,
        "--schedule-cdc", SCHEDULE_CDC,
        "--schedule-unknown", SCHEDULE_UNKNOWN,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("ERROR during discovery:")
        print(result.stderr)
        raise Exception("Discovery failed")

    print(result.stdout)
    print(f"\nGenerated CSV: {CSV_PATH}")

    # Show first few rows
    with open(CSV_PATH, 'r') as f:
        lines = f.readlines()
        print(f"\nPreview (first 5 rows):")
        print("".join(lines[:6]))
else:
    # Use preset CSV
    CSV_PATH = PRESET_CSV_PATH
    print(f"Using preset CSV: {CSV_PATH}")

    # Show first few rows
    with open(CSV_PATH, 'r') as f:
        lines = f.readlines()
        print(f"\nPreview (first 5 rows):")
        print("".join(lines[:6]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Ingest Files
# MAGIC
# MAGIC Generate Python ingest files for each pipeline group.

# COMMAND ----------

print(f"Generating ingest files from {CSV_PATH}...")

cmd = [
    "python3",
    f"{TOOLS_DIR}/generate_ingest_files.py",
    "--csv", CSV_PATH,
    "--output-dir", INGEST_FILES_DIR,
    "--source-name", CONNECTOR_NAME,
    "--connection-name", CONNECTION_NAME,
    "--catalog", DEST_CATALOG,
    "--schema", DEST_SCHEMA,
    "--common-table-config-json", "{}",
]

result = subprocess.run(cmd, capture_output=True, text=True)

if result.returncode != 0:
    print("ERROR during ingest file generation:")
    print(result.stderr)
    raise Exception("Ingest file generation failed")

print(result.stdout)

# List generated files
ingest_files = list(Path(INGEST_FILES_DIR).glob("ingest_*.py"))
print(f"\nGenerated {len(ingest_files)} ingest files:")
for f in sorted(ingest_files):
    print(f"  - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate DAB YAML
# MAGIC
# MAGIC Generate Databricks Asset Bundle configuration.

# COMMAND ----------

print(f"Generating DAB YAML to {DAB_YAML_PATH}...")

cmd = [
    "python3",
    f"{TOOLS_DIR}/generate_dab_yaml.py",
    "--connector-name", CONNECTOR_NAME,
    "--input-csv", CSV_PATH,
    "--output-yaml", DAB_YAML_PATH,
    "--ingest-files-path", WORKSPACE_INGEST_PATH,
    "--connection-name", CONNECTION_NAME,
    "--catalog", DEST_CATALOG,
    "--schema", DEST_SCHEMA,
    "--num-workers", str(CLUSTER_NUM_WORKERS),
]

if EMIT_SCHEDULED_JOBS:
    cmd.append("--emit-jobs")

if PAUSE_JOBS:
    cmd.append("--pause-jobs")

result = subprocess.run(cmd, capture_output=True, text=True)

if result.returncode != 0:
    print("ERROR during DAB YAML generation:")
    print(result.stderr)
    raise Exception("DAB YAML generation failed")

print(result.stdout)

# Show YAML preview
print(f"\nPreview of generated YAML:")
with open(DAB_YAML_PATH, 'r') as f:
    lines = f.readlines()
    print("".join(lines[:30]))
    if len(lines) > 30:
        print(f"... ({len(lines) - 30} more lines)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Upload Ingest Files to Workspace
# MAGIC
# MAGIC Upload generated ingest files using workspace API.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
import base64

w = WorkspaceClient()

print(f"Uploading ingest files to {WORKSPACE_INGEST_PATH}...")

# Ensure target directory exists
try:
    w.workspace.mkdirs(WORKSPACE_INGEST_PATH)
except Exception:
    pass  # Directory may already exist

uploaded_count = 0
for ingest_file in sorted(Path(INGEST_FILES_DIR).glob("ingest_*.py")):
    target_path = f"{WORKSPACE_INGEST_PATH}/{ingest_file.name}"

    with open(ingest_file, 'r') as f:
        content = f.read()
        encoded_content = base64.b64encode(content.encode()).decode()

    # Delete existing file if present to enable overwrite
    try:
        w.workspace.delete(target_path)
    except Exception:
        pass  # File may not exist yet

    w.workspace.import_(
        path=target_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=encoded_content
    )

    print(f"  ✓ Uploaded {ingest_file.name} -> {target_path}")
    uploaded_count += 1

print(f"\n✓ Uploaded {uploaded_count} ingest files to workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deploy with Databricks Asset Bundle
# MAGIC
# MAGIC Deploy pipelines using the generated DAB configuration.

# COMMAND ----------

import os

bundle_dir = str(Path(DAB_YAML_PATH).parent)

print(f"Deploying bundle from {bundle_dir}...")
print(f"Bundle directory contents:")

# List files in bundle directory
for f in Path(bundle_dir).iterdir():
    print(f"  - {f.name}")

# Deploy using databricks CLI
os.chdir(bundle_dir)

print("\nRunning: databricks bundle validate...")
result = subprocess.run(
    ["databricks", "--profile", DATABRICKS_PROFILE, "bundle", "validate"],
    capture_output=True,
    text=True
)

print(f"Return code: {result.returncode}")
if result.stdout:
    print(f"STDOUT:\n{result.stdout}")
if result.stderr:
    print(f"STDERR:\n{result.stderr}")

if result.returncode != 0:
    print("\nERROR: Bundle validation failed")
    raise Exception("Bundle validation failed")

print("\nRunning: databricks bundle deploy...")
result = subprocess.run(
    ["databricks", "--profile", DATABRICKS_PROFILE, "bundle", "deploy"],
    capture_output=True,
    text=True
)

if result.returncode != 0:
    print("ERROR during bundle deployment:")
    print(result.stderr)
    raise Exception("Bundle deployment failed")

print(result.stdout)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Deployment
# MAGIC
# MAGIC List deployed pipelines and jobs.

# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineStateInfo

# List pipelines matching connector name
pipelines = w.pipelines.list_pipelines()

print(f"Deployed pipelines for {CONNECTOR_NAME}:")
connector_pipelines = [p for p in pipelines if CONNECTOR_NAME in (p.name or "").lower()]

for pipeline in connector_pipelines:
    state = pipeline.state or PipelineStateInfo.UNKNOWN
    print(f"  - {pipeline.name}")
    print(f"    ID: {pipeline.pipeline_id}")
    print(f"    State: {state}")
    print()

if EMIT_SCHEDULED_JOBS:
    # List jobs matching connector name
    jobs = w.jobs.list()

    print(f"\nDeployed scheduled jobs for {CONNECTOR_NAME}:")
    connector_jobs = [j for j in jobs if CONNECTOR_NAME in (j.settings.name or "").lower()]

    for job in connector_jobs:
        schedule_info = job.settings.schedule
        schedule_str = "No schedule"
        if schedule_info:
            schedule_str = f"{schedule_info.quartz_cron_expression} ({schedule_info.pause_status})"

        print(f"  - {job.settings.name}")
        print(f"    ID: {job.job_id}")
        print(f"    Schedule: {schedule_str}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Your load-balanced pipelines are now deployed!
# MAGIC
# MAGIC ### Manual Trigger
# MAGIC
# MAGIC Run pipelines manually:
# MAGIC ```bash
# MAGIC databricks bundle run <pipeline_key>
# MAGIC ```
# MAGIC
# MAGIC Or trigger via SDK:
# MAGIC ```python
# MAGIC w.pipelines.start_update(pipeline_id="<id>", full_refresh=False)
# MAGIC ```
# MAGIC
# MAGIC ### Enable Scheduled Jobs
# MAGIC
# MAGIC If jobs were created in PAUSED state, enable them:
# MAGIC ```python
# MAGIC from databricks.sdk.service.jobs import PauseStatus
# MAGIC
# MAGIC for job in connector_jobs:
# MAGIC     w.jobs.update(
# MAGIC         job_id=job.job_id,
# MAGIC         new_settings={
# MAGIC             "schedule": {
# MAGIC                 "quartz_cron_expression": job.settings.schedule.quartz_cron_expression,
# MAGIC                 "timezone_id": job.settings.schedule.timezone_id,
# MAGIC                 "pause_status": PauseStatus.UNPAUSED
# MAGIC             }
# MAGIC         }
# MAGIC     )
# MAGIC     print(f"Enabled job: {job.settings.name}")
# MAGIC ```
# MAGIC
# MAGIC ### Monitor Pipelines
# MAGIC
# MAGIC - Navigate to **Workflows > Delta Live Tables** to see all pipelines
# MAGIC - Each pipeline group can be monitored independently
# MAGIC - Check pipeline runs, data quality metrics, and lineage

# COMMAND ----------

print("="*60)
print("DEPLOYMENT COMPLETE")
print("="*60)
print(f"\nConnector: {CONNECTOR_NAME}")
print(f"Pipelines deployed: {len(connector_pipelines)}")
if EMIT_SCHEDULED_JOBS:
    print(f"Scheduled jobs created: {len(connector_jobs)}")
print(f"\nIngest files location: {WORKSPACE_INGEST_PATH}")
print(f"DAB config: {DAB_YAML_PATH}")
