#!/bin/bash
# Generic Deployment Script for Lakeflow Connect Connectors
# Works for any connector: OSI PI, HubSpot, Zendesk, Zoho CRM, etc.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
    echo
    echo "=========================================="
    echo "$1"
    echo "=========================================="
    echo
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Parse arguments
CONNECTOR_NAME=""
CSV_FILE=""
WORKSPACE_PATH=""
DEST_CATALOG="main"
DEST_SCHEMA="bronze"
EMIT_JOBS="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --connector-name)
            CONNECTOR_NAME="$2"
            shift 2
            ;;
        --input-csv)
            CSV_FILE="$2"
            shift 2
            ;;
        --workspace-path)
            WORKSPACE_PATH="$2"
            shift 2
            ;;
        --dest-catalog)
            DEST_CATALOG="$2"
            shift 2
            ;;
        --dest-schema)
            DEST_SCHEMA="$2"
            shift 2
            ;;
        --emit-jobs)
            EMIT_JOBS="true"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$CONNECTOR_NAME" ] || [ -z "$CSV_FILE" ] || [ -z "$WORKSPACE_PATH" ]; then
    echo "Usage: $0 \\"
    echo "  --connector-name <name> \\"
    echo "  --input-csv <path> \\"
    echo "  --workspace-path <databricks-workspace-path> \\"
    echo "  [--dest-catalog <catalog>] \\"
    echo "  [--dest-schema <schema>] \\"
    echo "  [--emit-jobs]"
    exit 1
fi

print_header "$CONNECTOR_NAME Connector - Automated Deployment"

echo "Configuration:"
echo "  Connector: $CONNECTOR_NAME"
echo "  CSV: $CSV_FILE"
echo "  Workspace Path: $WORKSPACE_PATH"
echo "  Destination: $DEST_CATALOG.$DEST_SCHEMA"
echo "  Emit Jobs: $EMIT_JOBS"
echo

# Step 1: Generate DLT Notebooks
print_header "Step 1: Generate DLT Notebooks"

NOTEBOOKS_DIR="dlt_notebooks/$CONNECTOR_NAME"
mkdir -p "$NOTEBOOKS_DIR"

python tools/notebook_based_deployment/generate_dlt_notebooks_generic.py \
    --connector-name "$CONNECTOR_NAME" \
    --input-csv "$CSV_FILE" \
    --output-dir "$NOTEBOOKS_DIR" \
    --connector-path "$WORKSPACE_PATH/connectors" \
    --generate-all-groups

if [ $? -eq 0 ]; then
    print_success "Notebooks generated in $NOTEBOOKS_DIR"
else
    print_error "Failed to generate notebooks"
    exit 1
fi

# Step 2: Upload Notebooks to Databricks
print_header "Step 2: Upload Notebooks to Databricks"

NOTEBOOK_UPLOAD_PATH="$WORKSPACE_PATH/${CONNECTOR_NAME}_dlt_pipelines"

databricks workspace upload-dir \
    "$NOTEBOOKS_DIR" \
    "$NOTEBOOK_UPLOAD_PATH" \
    --overwrite-existing

if [ $? -eq 0 ]; then
    print_success "Notebooks uploaded to $NOTEBOOK_UPLOAD_PATH"
else
    print_error "Failed to upload notebooks"
    exit 1
fi

# Step 3: Generate DAB YAML
print_header "Step 3: Generate DAB YAML"

DAB_DIR="tools/ingestion_dab_generator/dab_template"
YAML_OUTPUT="$DAB_DIR/resources/${CONNECTOR_NAME}_pipelines.yml"

mkdir -p "$DAB_DIR/resources"

JOBS_FLAG=""
if [ "$EMIT_JOBS" = "true" ]; then
    JOBS_FLAG="--emit-jobs"
fi

python tools/notebook_based_deployment/generate_dab_yaml_notebooks.py \
    --connector-name "$CONNECTOR_NAME" \
    --input-csv "$CSV_FILE" \
    --output-yaml "$YAML_OUTPUT" \
    --notebook-base-path "$NOTEBOOK_UPLOAD_PATH" \
    --dest-catalog "$DEST_CATALOG" \
    --dest-schema "$DEST_SCHEMA" \
    $JOBS_FLAG

if [ $? -eq 0 ]; then
    print_success "DAB YAML generated: $YAML_OUTPUT"
else
    print_error "Failed to generate DAB YAML"
    exit 1
fi

# Step 4: Validate DAB Bundle
print_header "Step 4: Validate DAB Bundle"

cd "$DAB_DIR"

if databricks bundle validate -t dev > /dev/null 2>&1; then
    print_success "DAB bundle validated successfully"
else
    print_warning "DAB validation failed - check databricks.yml exists"
    echo "  Creating minimal databricks.yml..."
    
    cat > databricks.yml << EOF
bundle:
  name: ${CONNECTOR_NAME}_ingestion

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    workspace:
      host: https://e2-demo-field-eng.cloud.databricks.com
EOF
    
    databricks bundle validate -t dev
fi

# Step 5: Deploy
print_header "Step 5: Deploy to Databricks"

echo "Ready to deploy!"
echo
echo "Pipelines to be created:"
grep "^  pipeline_" "$YAML_OUTPUT" | sed 's/://' | sed 's/^/  - /'
echo

if [ "$EMIT_JOBS" = "true" ]; then
    echo "Jobs to be created:"
    grep "^  job_" "$YAML_OUTPUT" | sed 's/://' | sed 's/^/  - /'
    echo
fi

read -p "Deploy to dev environment? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    databricks bundle deploy -t dev
    
    if [ $? -eq 0 ]; then
        print_success "Deployment complete!"
        echo
        echo "Next steps:"
        echo "  1. Go to Workflows → Delta Live Tables"
        echo "  2. Find pipelines starting with '$CONNECTOR_NAME'"
        echo "  3. Start a pipeline to test"
        echo "  4. If successful, enable scheduled jobs in Workflows → Jobs"
    else
        print_error "Deployment failed"
        exit 1
    fi
else
    echo "Deployment cancelled"
    echo
    echo "To deploy later, run:"
    echo "  cd $DAB_DIR"
    echo "  databricks bundle deploy -t dev"
fi

cd - > /dev/null
