# Databricks notebook source
# MAGIC %md
# MAGIC # Load-Balanced Multi-Pipeline Deployment
# MAGIC
# MAGIC Automatically deploy multiple DLT pipelines for any community connector with load balancing across table groups.
# MAGIC
# MAGIC **What this does**:
# MAGIC 1. Discover tables (auto or preset CSV) and classify into groups
# MAGIC 2. Generate Python ingest files for each pipeline group
# MAGIC 3. Generate DAB YAML configuration
# MAGIC 4. Upload files and deploy pipelines using Databricks SDK
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - UC Connection created with credentials and `externalOptionsAllowList` configured
# MAGIC - Connector source synced to workspace at `/Workspace/Users/{user}/lakeflow-community-connectors/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies
# MAGIC
# MAGIC Install PyYAML for DAB YAML generation.

# COMMAND ----------

%pip install pyyaml --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC **Basic Settings:**
# MAGIC - **Connector Name**: Source connector (`osipi`, `zendesk`, `github`, etc.)
# MAGIC - **Connection Name**: UC connection with credentials
# MAGIC - **Use Preset CSV**: `true` = preset tables, `false` = auto-discover
# MAGIC - **Preset CSV File**: Which preset to use (only if Use Preset = true)
# MAGIC
# MAGIC **Destination:**
# MAGIC - **Catalog**: Target catalog (must exist)
# MAGIC - **Schema**: Target schema (created if missing)
# MAGIC
# MAGIC **Pipeline Configuration:**
# MAGIC - **Group By**: How to split tables into pipelines
# MAGIC - **Create Scheduled Jobs**: Auto-create scheduled jobs
# MAGIC - **Pause Jobs Initially**: Start jobs in PAUSED state (recommended)

# COMMAND ----------

# Basic Settings
dbutils.widgets.text("connector_name", "osipi", "Connector Name")
dbutils.widgets.text("connection_name", "osipi_connection_lakeflow", "UC Connection Name")
dbutils.widgets.dropdown("use_preset", "true", ["true", "false"], "Use Preset CSV")
dbutils.widgets.dropdown("preset_csv_file", "preset_by_category_and_ingestion.csv",
                        ["preset_by_category_and_ingestion.csv",
                         "preset_by_ingestion_type.csv",
                         "preset_all_tables.csv"],
                        "Preset CSV File")

# Destination
dbutils.widgets.text("dest_catalog", "osipi", "Destination Catalog")
dbutils.widgets.text("dest_schema", "bronze", "Destination Schema")

# Pipeline Configuration
dbutils.widgets.dropdown("group_by", "category_and_ingestion_type",
                        ["category_and_ingestion_type", "ingestion_type", "category", "none"],
                        "Group Tables By")
dbutils.widgets.dropdown("emit_scheduled_jobs", "true", ["true", "false"], "Create Scheduled Jobs")
dbutils.widgets.dropdown("pause_jobs", "true", ["true", "false"], "Pause Jobs Initially")

# Get values
CONNECTOR_NAME = dbutils.widgets.get("connector_name")
CONNECTION_NAME = dbutils.widgets.get("connection_name")
USE_PRESET = dbutils.widgets.get("use_preset") == "true"
PRESET_CSV_FILE = dbutils.widgets.get("preset_csv_file")
DEST_CATALOG = dbutils.widgets.get("dest_catalog")
DEST_SCHEMA = dbutils.widgets.get("dest_schema")
GROUP_BY = dbutils.widgets.get("group_by")
EMIT_SCHEDULED_JOBS = dbutils.widgets.get("emit_scheduled_jobs") == "true"
PAUSE_JOBS = dbutils.widgets.get("pause_jobs") == "true"

# Auto-detect repo path from notebook location
import os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# Extract repo base path: /Workspace/Users/{user}/{repo}/ from notebook path
# Notebook is at: {repo}/tools/load_balanced_deployment/notebooks/deploy_load_balanced_pipelines
# So repo base is 4 levels up from tools/load_balanced_deployment/notebooks/deploy_load_balanced_pipelines
REPO_BASE_PATH = "/".join(notebook_path.split("/")[:-4])
# Ensure path starts with /Workspace (notebookPath doesn't include it)
if not REPO_BASE_PATH.startswith("/Workspace"):
    REPO_BASE_PATH = f"/Workspace{REPO_BASE_PATH}"

# Hardcoded schedule defaults (sensible defaults for most use cases)
# Users can customize by editing generated DAB YAML if needed
SCHEDULE_SNAPSHOT = "0 0 * * *"      # Daily at midnight
SCHEDULE_APPEND = "*/15 * * * *"     # Every 15 minutes
SCHEDULE_CDC = "*/5 * * * *"         # Every 5 minutes
SCHEDULE_UNKNOWN = ""                # No schedule for unknown types

# Auto-discovery credentials (only if not using preset)
# Load from secrets if using auto-discovery mode
SECRETS_SCOPE = "sp-osipi"  # Change this to your secrets scope
SECRETS_MAPPING = '{"access_token": "access_token"}'  # Change this to match your connector

print("="*70)
print("CONFIGURATION")
print("="*70)
print(f"Connector:        {CONNECTOR_NAME}")
print(f"Connection:       {CONNECTION_NAME}")
print(f"Discovery Mode:   {'Preset CSV' if USE_PRESET else 'Auto-Discovery'}")
print(f"Destination:      {DEST_CATALOG}.{DEST_SCHEMA}")
print(f"Group By:         {GROUP_BY}")
print(f"Scheduled Jobs:   {EMIT_SCHEDULED_JOBS}")
print(f"Repo Path:        {REPO_BASE_PATH}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Paths

# COMMAND ----------

import os
from pathlib import Path
from datetime import datetime

# Get current user
USERNAME = spark.sql("SELECT current_user()").collect()[0][0]

# Work directories (local /tmp for subprocess execution)
# Use timestamped directory to avoid caching issues
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
WORK_DIR = f"/tmp/{USERNAME.replace('@', '_').replace('.', '_')}/load_balanced_deployment_{CONNECTOR_NAME}_{TIMESTAMP}"
CSV_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_tables.csv"
INGEST_FILES_DIR = f"{WORK_DIR}/{CONNECTOR_NAME}_ingest_files"
DAB_YAML_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_bundle/databricks.yml"

# Workspace paths
WORKSPACE_INGEST_PATH = f"{REPO_BASE_PATH}/ingest/{CONNECTOR_NAME}"
TOOLS_DIR_WORKSPACE = f"{REPO_BASE_PATH}/tools/load_balanced_deployment"
CONNECTOR_SOURCE_WORKSPACE = f"{REPO_BASE_PATH}/sources/{CONNECTOR_NAME}/{CONNECTOR_NAME}.py"
PRESET_CSV_PATH = f"{TOOLS_DIR_WORKSPACE}/examples/{CONNECTOR_NAME}/{PRESET_CSV_FILE}"

print(f"Work Directory:   {WORK_DIR}")
print(f"Workspace Path:   {WORKSPACE_INGEST_PATH}")
if USE_PRESET:
    print(f"Preset CSV:       {PRESET_CSV_FILE}")

# Clean up old timestamped work directories to avoid filling /tmp
import glob
import shutil
base_work_dir = f"/tmp/{USERNAME.replace('@', '_').replace('.', '_')}"
old_dirs = glob.glob(f"{base_work_dir}/load_balanced_deployment_{CONNECTOR_NAME}_*")
for old_dir in old_dirs:
    try:
        shutil.rmtree(old_dir)
        print(f"Cleaned up old work directory: {old_dir}")
    except Exception as e:
        print(f"Warning: Could not clean up {old_dir}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover Tables
# MAGIC
# MAGIC Discovers tables using either:
# MAGIC - **Preset CSV**: Pre-defined table list from `tools/load_balanced_deployment/examples/{connector}/`
# MAGIC - **Auto-Discovery**: Calls connector's `list_tables()` and `read_table_metadata()`

# COMMAND ----------

import subprocess
import shutil
import tempfile
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat
import base64

# Clean up existing work directory
if Path(WORK_DIR).exists():
    try:
        shutil.rmtree(WORK_DIR)
        print(f"Cleaned up existing work directory: {WORK_DIR}")
    except PermissionError:
        subprocess.run(["rm", "-rf", WORK_DIR], check=True, capture_output=True)
        print(f"Cleaned up existing work directory (via rm -rf): {WORK_DIR}")

# Create work directories
Path(WORK_DIR).mkdir(parents=True, exist_ok=True)
Path(INGEST_FILES_DIR).mkdir(parents=True, exist_ok=True)
Path(DAB_YAML_PATH).parent.mkdir(parents=True, exist_ok=True)
print(f"Created work directory: {WORK_DIR}")

# Create scripts directory in /tmp for subprocess execution
SCRIPTS_DIR = tempfile.mkdtemp(prefix="lakeflow_scripts_")
print(f"Created scripts directory: {SCRIPTS_DIR}")

# Export tool scripts from workspace to local /tmp
w = WorkspaceClient()
script_files = [
    "discover_and_classify_tables.py",
    "generate_ingest_files.py",
    "generate_dab_yaml.py",
    "utils.py"
]

print(f"Copying tool scripts...")
for script_name in script_files:
    workspace_path = f"{TOOLS_DIR_WORKSPACE}/{script_name}"
    local_path = f"{SCRIPTS_DIR}/{script_name}"

    try:
        response = w.workspace.export(workspace_path, format=ExportFormat.SOURCE)
        content = base64.b64decode(response.content).decode('utf-8')
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ {script_name}")
    except Exception as e:
        print(f"  ✗ Failed to copy {script_name}: {e}")

# Copy connector source file
CONNECTOR_PY_FILE = f"{SCRIPTS_DIR}/{CONNECTOR_NAME}.py"
try:
    response = w.workspace.export(CONNECTOR_SOURCE_WORKSPACE, format=ExportFormat.SOURCE)
    content = base64.b64decode(response.content).decode('utf-8')
    with open(CONNECTOR_PY_FILE, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"  ✓ {CONNECTOR_NAME}.py")
except Exception as e:
    print(f"  ✗ Failed to copy connector source: {e}")

TOOLS_DIR = SCRIPTS_DIR

if not USE_PRESET:
    print(f"\nDiscovering tables from {CONNECTOR_NAME} connector...")

    # Load credentials from secrets
    import json
    try:
        secrets_map = json.loads(SECRETS_MAPPING)
        init_options = {}
        for init_key, secret_key in secrets_map.items():
            secret_value = dbutils.secrets.get(SECRETS_SCOPE, secret_key)
            init_options[init_key] = secret_value
            print(f"  ✓ Loaded {init_key} from {SECRETS_SCOPE}/{secret_key}")
        init_options_json = json.dumps(init_options)
    except Exception as e:
        print(f"  ✗ Failed to load credentials: {e}")
        init_options_json = "{}"

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

    if init_options_json != "{}":
        cmd.extend(["--init-options-json", init_options_json])

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print("ERROR during discovery:")
        print(result.stderr)
        raise Exception("Discovery failed")

    print(result.stdout)
    print(f"\nGenerated CSV: {CSV_PATH}")

    with open(CSV_PATH, 'r') as f:
        lines = f.readlines()
        print(f"\nPreview (first 5 rows):")
        print("".join(lines[:6]))
else:
    # Copy preset CSV from workspace to local filesystem
    print(f"Using preset CSV from workspace: {PRESET_CSV_PATH}")
    try:
        response = w.workspace.export(PRESET_CSV_PATH, format=ExportFormat.SOURCE)
        content = base64.b64decode(response.content).decode('utf-8')
        CSV_PATH = f"{WORK_DIR}/{CONNECTOR_NAME}_preset.csv"
        with open(CSV_PATH, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ Copied to: {CSV_PATH}")
    except Exception as e:
        raise Exception(f"Failed to read preset CSV from {PRESET_CSV_PATH}: {e}")

    with open(CSV_PATH, 'r') as f:
        lines = f.readlines()
        print(f"\nPreview (first 5 rows):")
        print("".join(lines[:6]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Ingest Files
# MAGIC
# MAGIC Creates Python ingest files for each pipeline group.

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

ingest_files = list(Path(INGEST_FILES_DIR).glob("ingest_*.py"))
print(f"\nGenerated {len(ingest_files)} ingest files:")
for f in sorted(ingest_files):
    print(f"  - {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate DAB YAML
# MAGIC
# MAGIC Creates Databricks Asset Bundle configuration for pipelines and jobs.
# MAGIC
# MAGIC **Pipeline Configuration:**
# MAGIC - `serverless: true` - Uses serverless compute (auto-scaling, no cluster config needed)
# MAGIC - `channel: PREVIEW` - Required for community connector support

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

print(f"\nPreview of generated YAML:")
with open(DAB_YAML_PATH, 'r') as f:
    lines = f.readlines()
    print("".join(lines[:30]))
    if len(lines) > 30:
        print(f"... ({len(lines) - 30} more lines)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Upload Ingest Files

# COMMAND ----------

from databricks.sdk.service.workspace import ImportFormat, Language

print(f"Uploading ingest files to {WORKSPACE_INGEST_PATH}...")

try:
    w.workspace.mkdirs(WORKSPACE_INGEST_PATH)
except Exception:
    pass

uploaded_count = 0
for ingest_file in sorted(Path(INGEST_FILES_DIR).glob("ingest_*.py")):
    target_path = f"{WORKSPACE_INGEST_PATH}/{ingest_file.name}"

    with open(ingest_file, 'r') as f:
        content = f.read()
        encoded_content = base64.b64encode(content.encode()).decode()

    try:
        w.workspace.delete(target_path)
    except Exception:
        pass

    w.workspace.import_(
        path=target_path,
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=encoded_content
    )

    print(f"  ✓ {ingest_file.name} -> {target_path}")
    uploaded_count += 1

print(f"\n✓ Uploaded {uploaded_count} ingest files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deploy Pipelines and Jobs
# MAGIC
# MAGIC Deploys DLT pipelines and scheduled jobs using Databricks SDK.

# COMMAND ----------

import yaml
from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary
from databricks.sdk.service.jobs import Task, PipelineTask, JobSettings, CronSchedule, PauseStatus

# Read DAB configuration
with open(DAB_YAML_PATH, 'r') as f:
    dab_config = yaml.safe_load(f)

print(f"Deploying from {DAB_YAML_PATH}...")
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
    filter_pattern = f"name LIKE '%{CONNECTOR_NAME.upper()}%'"
    filtered_pipelines = list(w.pipelines.list_pipelines(filter=filter_pattern))

    for p in filtered_pipelines:
        if p.name in pipeline_names_to_deploy:
            existing_pipelines[p.name] = p.pipeline_id
            print(f"  ✓ Found existing: {p.name} (ID: {p.pipeline_id})")

    print(f"  Checked {len(filtered_pipelines)} pipeline(s)")
except Exception as e:
    print(f"  ! Error listing pipelines: {e}")

if not existing_pipelines:
    print("  No existing pipelines found - will create new ones")

# Deploy pipelines
deployed_pipelines = {}
print("\n" + "="*60)
print("DEPLOYING PIPELINES")
print("="*60)

for pipeline_key, pipeline_config in dab_config['resources']['pipelines'].items():
    pipeline_name = pipeline_config['name']

    library_config = pipeline_config['libraries'][0]
    file_path = library_config['file']['path'].replace('${var.ingest_files_path}', ingest_files_path)
    libraries = [PipelineLibrary(file=FileLibrary(path=file_path))]

    try:
        if pipeline_name in existing_pipelines:
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
            print(f"    ✓ Updated (ID: {pipeline_id})")
        else:
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
            print(f"    ✓ Created (ID: {pipeline.pipeline_id})")
    except Exception as e:
        print(f"    ✗ Failed: {e}")

# Deploy jobs
if 'jobs' in dab_config.get('resources', {}) and EMIT_SCHEDULED_JOBS:
    print("\n" + "="*60)
    print("CHECKING FOR EXISTING JOBS")
    print("="*60)

    job_names_to_deploy = {spec['name']: key for key, spec in dab_config['resources']['jobs'].items()}
    existing_jobs = {}

    try:
        checked_count = 0
        for job_name in job_names_to_deploy.keys():
            try:
                matching_jobs = list(w.jobs.list(name=job_name, limit=1))
                if matching_jobs:
                    job = matching_jobs[0]
                    existing_jobs[job_name] = job.job_id
                    print(f"  ✓ Found existing: {job_name} (ID: {job.job_id})")
                checked_count += 1
            except Exception:
                pass

        print(f"  Checked {checked_count} job(s)")
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

        task_config = job_config['tasks'][0]
        pipeline_task_config = task_config['pipeline_task']
        pipeline_ref = pipeline_task_config['pipeline_id']

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

        schedule_config = job_config.get('schedule', {})
        cron_expr = schedule_config.get('quartz_cron_expression')
        timezone = schedule_config.get('timezone_id', 'UTC')
        pause_status_str = schedule_config.get('pause_status', 'PAUSED')
        pause_status = PauseStatus.PAUSED if pause_status_str == 'PAUSED' else PauseStatus.UNPAUSED

        try:
            if job_name in existing_jobs:
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
                print(f"    ✓ Updated (ID: {job_id}, Schedule: {cron_expr} [{pause_status}])")
            else:
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
                print(f"    ✓ Created (ID: {job.job_id}, Schedule: {cron_expr} [{pause_status}])")
        except Exception as e:
            print(f"    ✗ Failed: {e}")

print("\n" + "="*60)
print("DEPLOYMENT SUMMARY")
print("="*60)
print(f"Pipelines deployed: {len(deployed_pipelines)}")
if EMIT_SCHEDULED_JOBS and 'jobs' in dab_config.get('resources', {}):
    print(f"Jobs deployed: {len(deployed_jobs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC Review deployed pipelines and jobs.

# COMMAND ----------

print("="*60)
print("DEPLOYED RESOURCES")
print("="*60)

print(f"\n✓ {len(deployed_pipelines)} Pipeline(s):")
for pipeline_key, pipeline_id in deployed_pipelines.items():
    pipeline_info = w.pipelines.get(pipeline_id)
    print(f"  - {pipeline_info.name}")
    print(f"    ID: {pipeline_id}")
    print(f"    State: {pipeline_info.state}")
    print()

if EMIT_SCHEDULED_JOBS and 'deployed_jobs' in locals():
    print(f"✓ {len(deployed_jobs)} Scheduled Job(s):")
    for job_key, job_id in deployed_jobs.items():
        job_info = w.jobs.get(job_id)
        schedule_info = job_info.settings.schedule
        schedule_str = f"{schedule_info.quartz_cron_expression} ({schedule_info.pause_status})" if schedule_info else "No schedule"

        print(f"  - {job_info.settings.name}")
        print(f"    ID: {job_id}")
        print(f"    Schedule: {schedule_str}")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Start Pipelines
# MAGIC
# MAGIC Trigger all deployed pipelines to start ingestion.

# COMMAND ----------

# Uncomment to start all pipelines
# for pipeline_id in deployed_pipelines.values():
#     print(f"Starting pipeline: {pipeline_id}")
#     w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
# print("All pipelines started!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Cleanup
# MAGIC
# MAGIC Delete deployed pipelines and jobs.

# COMMAND ----------

# Uncomment to delete all deployed pipelines and jobs
# print("Deleting pipelines and jobs...")
#
# # Delete jobs first (they depend on pipelines)
# if EMIT_SCHEDULED_JOBS and 'deployed_jobs' in dir():
#     for job_name, job_id in deployed_jobs.items():
#         try:
#             w.jobs.delete(job_id=job_id)
#             print(f"  ✓ Deleted job: {job_name} (ID: {job_id})")
#         except Exception as e:
#             print(f"  ✗ Failed to delete job {job_name}: {e}")
#
# # Delete pipelines
# if 'deployed_pipelines' in dir():
#     for pipeline_key, pipeline_id in deployed_pipelines.items():
#         try:
#             w.pipelines.delete(pipeline_id=pipeline_id)
#             print(f"  ✓ Deleted pipeline: {pipeline_key} (ID: {pipeline_id})")
#         except Exception as e:
#             print(f"  ✗ Failed to delete pipeline {pipeline_key}: {e}")
#
# # Clean up work directories
# import shutil
# if Path(WORK_DIR).exists():
#     shutil.rmtree(WORK_DIR)
#     print(f"  ✓ Cleaned up work directory: {WORK_DIR}")
#
# print("\nCleanup complete!")
