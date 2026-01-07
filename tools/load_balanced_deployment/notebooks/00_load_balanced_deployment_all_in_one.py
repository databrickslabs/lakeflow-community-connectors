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
# MAGIC 5. Deploy pipelines and jobs using Databricks SDK
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - **UC Connection created** with proper credentials:
# MAGIC   ```bash
# MAGIC   community-connector create_connection osipi osipi_connection_lakeflow \
# MAGIC     -o '{"pi_base_url": "https://mock-osipi-server.example.com", "access_token": "your-token"}'
# MAGIC   ```
# MAGIC - Connector source code synced to workspace at `/Workspace/Users/{user}/lakeflow-community-connectors/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set your configuration parameters below.

# COMMAND ----------

# CONNECTOR SETTINGS
CONNECTOR_NAME = "osipi"  # Change to your connector: osipi, hubspot, github, etc.
CONNECTION_NAME = "osipi_connection_lakeflow"  # UC Connection name (must exist)
DEST_CATALOG = "osipi"
DEST_SCHEMA = "bronze"

# DISCOVERY OPTIONS (set USE_PRESET=True to skip discovery)
USE_PRESET = False  # Set to True to use preset CSV instead of discovery
# PRESET_CSV_PATH will be set after USERNAME is defined (see below)

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
from datetime import datetime

USERNAME = spark.sql("SELECT current_user()").collect()[0][0]
DATABRICKS_PROFILE = "dogfood"  # Change this to your Databricks CLI profile name

# OUTPUT PATHS (using local /tmp directory to avoid DBFS I/O issues)
# Files are overwritable - same location each time for easy iteration
WORK_DIR = f"/tmp/{USERNAME.replace('@', '_').replace('.', '_')}/load_balanced_deployment_{CONNECTOR_NAME}"

CSV_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_tables.csv"
INGEST_FILES_DIR = f"{WORK_DIR}/{CONNECTOR_NAME}_ingest_files"
DAB_YAML_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_bundle/databricks.yml"
# Upload ingest files to repo directory so they can import from pipeline/ and libs/
WORKSPACE_INGEST_PATH = f"/Workspace/Users/{USERNAME}/lakeflow-community-connectors/ingest/{CONNECTOR_NAME}"
CLUSTER_NUM_WORKERS = 2
EMIT_SCHEDULED_JOBS = True
PAUSE_JOBS = True  # Create jobs in PAUSED state

# Tool scripts workspace path - will be copied to local temp directory in Step 1 for subprocess access
TOOLS_DIR_WORKSPACE = f"/Workspace/Users/{USERNAME}/lakeflow-community-connectors/tools/load_balanced_deployment"
# Connector source path in workspace - needed for discovery
CONNECTOR_SOURCE_WORKSPACE = f"/Workspace/Users/{USERNAME}/lakeflow-community-connectors/sources/{CONNECTOR_NAME}/{CONNECTOR_NAME}.py"
# Preset CSV path (workspace location)
PRESET_CSV_PATH = f"/Workspace/Users/{USERNAME}/lakeflow-community-connectors/tools/load_balanced_deployment/examples/{CONNECTOR_NAME}/preset_by_category_and_ingestion.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1a: Quick CSV Generator (No Connector Required)
# MAGIC
# MAGIC **Use this if you just want to provide a list of table names** without running the connector's discovery.
# MAGIC
# MAGIC Generates a CSV with default values. You can manually edit the CSV afterwards to:
# MAGIC - Set ingestion_type (snapshot/append/cdc)
# MAGIC - Assign pipeline_group
# MAGIC - Set primary_keys and cursor_field

# COMMAND ----------

# Set ENABLE_QUICK_CSV = True to generate CSV from table list
ENABLE_QUICK_CSV = False

# Provide your table names here (or set to None to skip)
QUICK_TABLE_LIST = [
    "pi_dataservers",
    "pi_points",
    "pi_timeseries",
    "pi_assetservers",
    "pi_af_databases",
    # Add more tables...
]

if ENABLE_QUICK_CSV and QUICK_TABLE_LIST:
    import csv
    from pathlib import Path

    print("="*60)
    print("QUICK CSV GENERATOR")
    print("="*60)
    print(f"Generating CSV for {len(QUICK_TABLE_LIST)} tables...")

    Path(WORK_DIR).mkdir(parents=True, exist_ok=True)

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "connection_name", "source_table", "destination_catalog", "destination_schema",
            "destination_table", "pipeline_group", "schedule", "table_options_json",
            "weight", "ingestion_type", "primary_keys", "cursor_field", "category"
        ])
        writer.writeheader()

        for table in QUICK_TABLE_LIST:
            # Extract category from table name prefix (e.g., "pi_af_databases" -> "af")
            parts = table.split('_')
            category = parts[1] if len(parts) > 1 else "default"

            writer.writerow({
                "connection_name": CONNECTION_NAME,
                "source_table": table,
                "destination_catalog": DEST_CATALOG,
                "destination_schema": DEST_SCHEMA,
                "destination_table": table,
                "pipeline_group": f"{category}_snapshot",  # Default grouping
                "schedule": SCHEDULE_SNAPSHOT,             # Default schedule
                "table_options_json": "{}",
                "weight": "1",                              # Default weight
                "ingestion_type": "snapshot",               # Default to snapshot
                "primary_keys": "",                         # Will be inferred by connector
                "cursor_field": "",                         # Will be inferred by connector
                "category": category
            })

    print(f"✓ Generated CSV: {CSV_PATH}")
    print(f"\nNext steps:")
    print(f"  1. Review/edit CSV if needed (change ingestion_type, pipeline_group, etc.)")
    print(f"  2. Set USE_PRESET = False (or skip discovery step)")
    print(f"  3. Continue to Step 2 (Generate Ingest Files)")

    # Preview
    with open(CSV_PATH, 'r') as f:
        lines = f.readlines()
        print(f"\nPreview (first 5 rows):")
        print("".join(lines[:6]))
elif ENABLE_QUICK_CSV:
    print("Quick CSV generation skipped: QUICK_TABLE_LIST is empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1b: Full Discovery with Connector (Requires Credentials)
# MAGIC
# MAGIC **Skip this if you used Quick CSV Generator or preset CSV.**
# MAGIC
# MAGIC This calls the connector's `list_tables()` and `read_table_metadata()` to automatically detect:
# MAGIC - All available tables
# MAGIC - Ingestion type (snapshot/append/cdc)
# MAGIC - Primary keys and cursor fields

# COMMAND ----------

import subprocess
import os
import shutil
import tempfile
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
import base64

# Create work directories using standard Python (no DBFS needed)
Path(WORK_DIR).mkdir(parents=True, exist_ok=True)
Path(INGEST_FILES_DIR).mkdir(parents=True, exist_ok=True)
Path(DAB_YAML_PATH).parent.mkdir(parents=True, exist_ok=True)
print(f"Using work directory: {WORK_DIR}")

# Copy tool scripts to local temp directory for subprocess execution
SCRIPTS_DIR = tempfile.mkdtemp(prefix="lakeflow_scripts_")
print(f"Created local scripts directory: {SCRIPTS_DIR}")

# Get the current notebook's directory to find scripts
# Use workspace API to export scripts from Repos
w = WorkspaceClient()
script_files = [
    "discover_and_classify_tables.py",
    "generate_ingest_files.py",
    "generate_dab_yaml.py",
    "utils.py"
]

print(f"Copying tool scripts to {SCRIPTS_DIR}...")
for script_name in script_files:
    workspace_path = f"{TOOLS_DIR_WORKSPACE}/{script_name}"
    local_path = f"{SCRIPTS_DIR}/{script_name}"

    try:
        # Export from workspace
        response = w.workspace.export(workspace_path, format=ExportFormat.SOURCE)
        # Decode base64 content
        content = base64.b64decode(response.content).decode('utf-8')
        # Write to local temp directory using standard Python file I/O
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ Copied {script_name}")
    except Exception as e:
        print(f"  ✗ Failed to copy {script_name}: {e}")

# Copy connector source file to local temp directory (needed for discovery)
CONNECTOR_PY_FILE = f"{SCRIPTS_DIR}/{CONNECTOR_NAME}.py"
print(f"Copying connector source to {CONNECTOR_PY_FILE}...")
try:
    response = w.workspace.export(CONNECTOR_SOURCE_WORKSPACE, format=ExportFormat.SOURCE)
    content = base64.b64decode(response.content).decode('utf-8')
    with open(CONNECTOR_PY_FILE, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"  ✓ Copied {CONNECTOR_NAME}.py")
except Exception as e:
    print(f"  ✗ Failed to copy connector source: {e}")
    print(f"  Note: Discovery may fail without connector source file")

# Point TOOLS_DIR to copied scripts for subprocess calls
TOOLS_DIR = SCRIPTS_DIR

if not USE_PRESET:
    print(f"Discovering tables from {CONNECTOR_NAME} connector...")
    print(f"Using UC Connection: {CONNECTION_NAME}")

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
# MAGIC ## Step 5: Deploy Pipelines and Jobs
# MAGIC
# MAGIC Deploy pipelines and jobs directly using Databricks SDK (CLI not available in notebooks).

# COMMAND ----------

import yaml
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary, FileLibrary
from databricks.sdk.service.jobs import Task, PipelineTask, JobSettings, CronSchedule, PauseStatus

# Read the generated DAB YAML to get configuration
with open(DAB_YAML_PATH, 'r') as f:
    dab_config = yaml.safe_load(f)

print(f"Deploying pipelines and jobs from {DAB_YAML_PATH}...")
print(f"\nConfiguration loaded:")
print(f"  Bundle: {dab_config['bundle']['name']}")
print(f"  Pipelines: {len(dab_config['resources']['pipelines'])}")
print(f"  Jobs: {len(dab_config.get('resources', {}).get('jobs', {}))}")

# Get variables
variables = dab_config.get('variables', {})
catalog = variables.get('catalog', {}).get('default', DEST_CATALOG)
schema = variables.get('schema', {}).get('default', DEST_SCHEMA)
connection_name = variables.get('connection_name', {}).get('default', CONNECTION_NAME)
ingest_files_path = variables.get('ingest_files_path', {}).get('default', WORKSPACE_INGEST_PATH)

# Check for existing pipelines
print("\n" + "="*60)
print("CHECKING FOR EXISTING PIPELINES")
print("="*60)

pipeline_names_to_deploy = {spec['name']: key for key, spec in dab_config['resources']['pipelines'].items()}
existing_pipelines = {}

try:
    # Filter by connector name pattern to avoid listing all workspace pipelines
    # This reduces API calls and only checks relevant pipelines
    filter_pattern = f"name LIKE '%{CONNECTOR_NAME.upper()}%'"
    filtered_pipelines = list(w.pipelines.list_pipelines(filter=filter_pattern))

    for p in filtered_pipelines:
        if p.name in pipeline_names_to_deploy:
            existing_pipelines[p.name] = p.pipeline_id
            print(f"  ✓ Found existing: {p.name} (ID: {p.pipeline_id})")

    print(f"  Checked {len(filtered_pipelines)} pipeline(s) matching '{CONNECTOR_NAME.upper()}'")
except Exception as e:
    print(f"  ! Error listing pipelines: {e}")

if not existing_pipelines:
    print("  No existing pipelines found - will create new ones")

# Deploy pipelines (create or update)
deployed_pipelines = {}
print("\n" + "="*60)
print("DEPLOYING PIPELINES")
print("="*60)

for pipeline_key, pipeline_config in dab_config['resources']['pipelines'].items():
    pipeline_name = pipeline_config['name']

    # Extract library path from config
    library_config = pipeline_config['libraries'][0]
    file_path = library_config['file']['path']
    # Replace variable placeholders
    file_path = file_path.replace('${var.ingest_files_path}', ingest_files_path)

    # Prepare library configuration
    libraries = [PipelineLibrary(file=FileLibrary(path=file_path))]

    try:
        if pipeline_name in existing_pipelines:
            # Update existing pipeline
            pipeline_id = existing_pipelines[pipeline_name]
            print(f"\n  Updating: {pipeline_name}")

            w.pipelines.update(
                pipeline_id=pipeline_id,
                name=pipeline_name,
                catalog=catalog,
                target=schema,
                channel=pipeline_config.get('channel', 'PREVIEW'),
                serverless=pipeline_config.get('serverless', True),
                development=pipeline_config.get('development', True),
                continuous=pipeline_config.get('continuous', False),
                libraries=libraries
            )

            deployed_pipelines[pipeline_key] = pipeline_id
            print(f"    ✓ Updated successfully")
            print(f"    ID: {pipeline_id}")
        else:
            # Create new pipeline
            print(f"\n  Creating: {pipeline_name}")

            pipeline = w.pipelines.create(
                name=pipeline_name,
                catalog=catalog,
                target=schema,
                channel=pipeline_config.get('channel', 'PREVIEW'),
                serverless=pipeline_config.get('serverless', True),
                development=pipeline_config.get('development', True),
                continuous=pipeline_config.get('continuous', False),
                libraries=libraries
            )

            deployed_pipelines[pipeline_key] = pipeline.pipeline_id
            print(f"    ✓ Created successfully")
            print(f"    ID: {pipeline.pipeline_id}")
    except Exception as e:
        print(f"    ✗ Failed: {e}")

# Deploy jobs (if any)
if 'jobs' in dab_config.get('resources', {}) and EMIT_SCHEDULED_JOBS:
    # Check for existing jobs
    print("\n" + "="*60)
    print("CHECKING FOR EXISTING JOBS")
    print("="*60)

    job_names_to_deploy = {spec['name']: key for key, spec in dab_config['resources']['jobs'].items()}
    existing_jobs = {}

    try:
        # Note: jobs.list() doesn't support pattern matching, only exact name
        # So we iterate through job_names_to_deploy and check each one individually
        # This is more efficient than listing all jobs in the workspace
        checked_count = 0
        for job_name in job_names_to_deploy.keys():
            try:
                # Try to find job by exact name (case insensitive)
                matching_jobs = list(w.jobs.list(name=job_name, limit=1))
                if matching_jobs:
                    job = matching_jobs[0]
                    existing_jobs[job_name] = job.job_id
                    print(f"  ✓ Found existing: {job_name} (ID: {job.job_id})")
                checked_count += 1
            except Exception:
                # Job doesn't exist, will create new
                pass

        print(f"  Checked {checked_count} job(s) by name")
    except Exception as e:
        print(f"  ! Error checking jobs: {e}")

    if not existing_jobs:
        print("  No existing jobs found - will create new ones")

    deployed_jobs = {}
    print("\n" + "="*60)
    print("DEPLOYING SCHEDULED JOBS")
    print("="*60)

    for job_key, job_config in dab_config['resources']['jobs'].items():
        job_name = job_config['name']

        # Get the pipeline reference
        task_config = job_config['tasks'][0]
        pipeline_task_config = task_config['pipeline_task']
        pipeline_ref = pipeline_task_config['pipeline_id']

        # Extract pipeline key from reference like "${resources.pipelines.osipi_snapshot.id}"
        import re
        match = re.search(r'resources\.pipelines\.([^.]+)\.id', pipeline_ref)
        if not match:
            print(f"\n  ✗ Could not parse pipeline reference: {pipeline_ref}")
            continue

        referenced_pipeline_key = match.group(1)
        if referenced_pipeline_key not in deployed_pipelines:
            print(f"\n  ✗ Referenced pipeline not found: {referenced_pipeline_key}")
            continue

        pipeline_id = deployed_pipelines[referenced_pipeline_key]

        # Get schedule info
        schedule_config = job_config.get('schedule', {})
        cron_expr = schedule_config.get('quartz_cron_expression')
        timezone = schedule_config.get('timezone_id', 'UTC')
        pause_status_str = schedule_config.get('pause_status', 'PAUSED')
        # Convert string to enum
        pause_status = PauseStatus.PAUSED if pause_status_str == 'PAUSED' else PauseStatus.UNPAUSED

        try:
            if job_name in existing_jobs:
                # Update existing job
                job_id = existing_jobs[job_name]
                print(f"\n  Updating: {job_name}")

                w.jobs.update(
                    job_id=job_id,
                    new_settings=JobSettings(
                        name=job_name,
                        tasks=[
                            Task(
                                task_key=task_config['task_key'],
                                pipeline_task=PipelineTask(pipeline_id=pipeline_id)
                            )
                        ],
                        schedule=CronSchedule(
                            quartz_cron_expression=cron_expr,
                            timezone_id=timezone,
                            pause_status=pause_status
                        )
                    )
                )

                deployed_jobs[job_key] = job_id
                print(f"    ✓ Updated successfully")
                print(f"    ID: {job_id}")
                print(f"    Schedule: {cron_expr} ({pause_status})")
            else:
                # Create new job
                print(f"\n  Creating: {job_name}")

                job = w.jobs.create(
                    name=job_name,
                    tasks=[
                        Task(
                            task_key=task_config['task_key'],
                            pipeline_task=PipelineTask(pipeline_id=pipeline_id)
                        )
                    ],
                    schedule=CronSchedule(
                        quartz_cron_expression=cron_expr,
                        timezone_id=timezone,
                        pause_status=pause_status
                    )
                )

                deployed_jobs[job_key] = job.job_id
                print(f"    ✓ Created successfully")
                print(f"    ID: {job.job_id}")
                print(f"    Schedule: {cron_expr} ({pause_status})")
        except Exception as e:
            print(f"    ✗ Failed: {e}")

print("\n" + "="*60)
print("DEPLOYMENT SUMMARY")
print("="*60)
print(f"Pipelines created: {len(deployed_pipelines)}")
if EMIT_SCHEDULED_JOBS and 'jobs' in dab_config.get('resources', {}):
    print(f"Jobs created: {len(deployed_jobs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Deployment
# MAGIC
# MAGIC Check the deployed pipelines and jobs.

# COMMAND ----------

print("="*60)
print("DEPLOYMENT VERIFICATION")
print("="*60)

print(f"\n✓ Deployed {len(deployed_pipelines)} pipeline(s):")
for pipeline_key, pipeline_id in deployed_pipelines.items():
    pipeline_info = w.pipelines.get(pipeline_id)
    print(f"  - {pipeline_info.name}")
    print(f"    ID: {pipeline_id}")
    print(f"    State: {pipeline_info.state}")
    print()

if EMIT_SCHEDULED_JOBS and 'deployed_jobs' in locals():
    print(f"✓ Deployed {len(deployed_jobs)} scheduled job(s):")
    for job_key, job_id in deployed_jobs.items():
        job_info = w.jobs.get(job_id)
        schedule_info = job_info.settings.schedule
        schedule_str = "No schedule"
        if schedule_info:
            schedule_str = f"{schedule_info.quartz_cron_expression} ({schedule_info.pause_status})"

        print(f"  - {job_info.settings.name}")
        print(f"    ID: {job_id}")
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
print(f"Pipelines deployed: {len(deployed_pipelines)}")
if EMIT_SCHEDULED_JOBS and 'deployed_jobs' in locals():
    print(f"Scheduled jobs deployed: {len(deployed_jobs)}")
print(f"\nGenerated files (overwritable):")
print(f"  CSV: {CSV_PATH}")
print(f"  Ingest files: {INGEST_FILES_DIR}")
print(f"  DAB YAML: {DAB_YAML_PATH}")
print(f"  Workspace: {WORKSPACE_INGEST_PATH}")
print(f"\nRe-running this notebook will:")
print(f"  ✓ Overwrite CSV/YAML/ingest files with latest")
print(f"  ✓ Update existing pipelines and jobs (no duplicates)")
print(f"\nTo trigger a pipeline update:")
print(f"  w.pipelines.start_update(pipeline_id='<pipeline_id>', full_refresh=False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Start All Pipelines (Optional)
# MAGIC
# MAGIC Start all deployed pipelines to begin ingestion.

# COMMAND ----------

print("="*60)
print("STARTING ALL PIPELINES")
print("="*60)

for name, pipeline_id in deployed_pipelines.items():
    print(f"\nStarting pipeline: {name}")
    print(f"  Pipeline ID: {pipeline_id}")

    try:
        update = w.pipelines.start_update(
            pipeline_id=pipeline_id,
            full_refresh=False  # Set to True for full refresh
        )
        print(f"  ✓ Started (Update ID: {update.update_id})")
        print(f"  Monitor at: https://dogfood.cloud.databricks.com/#joblist/pipelines/{pipeline_id}")
    except Exception as e:
        print(f"  ✗ Failed to start: {e}")

print(f"\n✓ Started {len(deployed_pipelines)} pipelines")
print(f"\nMonitor all pipelines at:")
print(f"  https://dogfood.cloud.databricks.com/#joblist/pipelines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Cleanup (Optional)
# MAGIC
# MAGIC **WARNING**: This will delete all deployed pipelines and jobs. Use with caution!

# COMMAND ----------

# Uncomment to enable cleanup
# ENABLE_CLEANUP = True

if 'ENABLE_CLEANUP' in locals() and ENABLE_CLEANUP:
    print("="*60)
    print("CLEANING UP DEPLOYED RESOURCES")
    print("="*60)

    # Delete pipelines
    if 'deployed_pipelines' in locals():
        print(f"\nDeleting {len(deployed_pipelines)} pipelines...")
        for name, pipeline_id in deployed_pipelines.items():
            try:
                w.pipelines.delete(pipeline_id=pipeline_id)
                print(f"  ✓ Deleted pipeline: {name}")
            except Exception as e:
                print(f"  ✗ Failed to delete {name}: {e}")

    # Delete jobs
    if 'deployed_jobs' in locals():
        print(f"\nDeleting {len(deployed_jobs)} jobs...")
        for name, job_id in deployed_jobs.items():
            try:
                w.jobs.delete(job_id=job_id)
                print(f"  ✓ Deleted job: {name}")
            except Exception as e:
                print(f"  ✗ Failed to delete {name}: {e}")

    print(f"\n✓ Cleanup complete")
else:
    print("Cleanup disabled. Set ENABLE_CLEANUP = True to enable.")
