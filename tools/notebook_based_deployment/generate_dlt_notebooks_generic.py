#!/usr/bin/env python3
"""
Generic DLT Notebook Generator for Lakeflow Connect Connectors.

Generates DLT notebooks with proper secret handling for connectors where
the framework doesn't auto-inject credentials from connection properties.

Supports: OSI PI, HubSpot, Zendesk, Zoho CRM, and any custom HTTP-based connector.

Usage:
    python generate_dlt_notebooks_generic.py \
        --connector-config configs/osipi_connector_config.json \
        --input-csv metadata.csv \
        --output-dir dlt_notebooks \
        --connector-path /Workspace/Users/user@company.com/connectors
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Any


# Connector-specific configurations
CONNECTOR_CONFIGS = {
    "osipi": {
        "secrets_scope": "sp-osipi",
        "secret_mappings": {
            "client_id": ["client_id", "sp-client-id"],
            "client_secret": ["client_secret", "sp-client-secret"]
        },
        "static_options": {
            "pi_base_url": "https://osipi-webserver-xxx.aws.databricksapps.com",
            "verify_ssl": "false"
        },
        "dynamic_options": {
            "workspace_host": 'f"https://{spark.conf.get(\'spark.databricks.workspaceUrl\')}"'
        }
    },
    "hubspot": {
        "secrets_scope": "hubspot",
        "secret_mappings": {
            "access_token": ["access_token"]
        },
        "static_options": {},
        "dynamic_options": {}
    },
    "zendesk": {
        "secrets_scope": "zendesk",
        "secret_mappings": {
            "api_token": ["api_token"],
            "subdomain": ["subdomain"]
        },
        "static_options": {},
        "dynamic_options": {}
    },
    "zoho_crm": {
        "secrets_scope": "zoho-crm",
        "secret_mappings": {
            "client_id": ["client_id"],
            "client_secret": ["client_secret"],
            "refresh_token": ["refresh_token"]
        },
        "static_options": {
            "base_url": "https://accounts.zoho.com"
        },
        "dynamic_options": {}
    }
}


def load_connector_config(connector_name: str, config_file: Path = None) -> Dict[str, Any]:
    """Load connector-specific configuration."""
    
    # Try custom config file first
    if config_file and config_file.exists():
        with open(config_file, 'r') as f:
            return json.load(f)
    
    # Fall back to built-in configs
    if connector_name in CONNECTOR_CONFIGS:
        return CONNECTOR_CONFIGS[connector_name]
    
    raise ValueError(
        f"Unknown connector: {connector_name}. "
        f"Provide --connector-config with configuration, or use one of: "
        f"{', '.join(CONNECTOR_CONFIGS.keys())}"
    )


def generate_secret_retrieval_code(config: Dict[str, Any]) -> str:
    """Generate code to retrieve secrets from Databricks Secrets."""
    
    secrets_scope = config["secrets_scope"]
    secret_mappings = config["secret_mappings"]
    
    code = f'''# Read connector credentials from Databricks Secrets
# Secrets are read in driver context where dbutils is available
SECRETS_SCOPE = "{secrets_scope}"

'''
    
    for var_name, key_variants in secret_mappings.items():
        var_upper = var_name.upper()
        
        if len(key_variants) == 1:
            # Simple case - one key
            code += f'{var_upper} = dbutils.secrets.get(SECRETS_SCOPE, "{key_variants[0]}")\n'
        else:
            # Try multiple key names (for flexibility)
            code += f'try:\n'
            code += f'    {var_upper} = dbutils.secrets.get(SECRETS_SCOPE, "{key_variants[0]}")\n'
            for alt_key in key_variants[1:]:
                code += f'except:\n'
                code += f'    {var_upper} = dbutils.secrets.get(SECRETS_SCOPE, "{alt_key}")\n'
    
    code += '\nprint("✓ Connector credentials loaded from secrets")\n'
    return code


def generate_helper_function(connector_name: str, config: Dict[str, Any]) -> str:
    """Generate helper function to create streams with credentials."""
    
    secret_mappings = config["secret_mappings"]
    static_options = config.get("static_options", {})
    dynamic_options = config.get("dynamic_options", {})
    
    code = '''def get_connector_stream(table_name: str, **table_opts):
    """
    Create a streaming DataFrame for a connector table with proper authentication.
    
    Args:
        table_name: Connector table name
        **table_opts: Table-specific options
    
    Returns:
        Streaming DataFrame
    """
    reader = (
        spark.readStream.format("lakeflow_connect")
        .option("tableName", table_name)
'''
    
    # Add secret-based options
    for var_name in secret_mappings.keys():
        var_upper = var_name.upper()
        code += f'        .option("{var_name}", {var_upper})\n'
    
    # Add static options
    for key, value in static_options.items():
        code += f'        .option("{key}", "{value}")\n'
    
    # Add dynamic options
    for key, value_expr in dynamic_options.items():
        code += f'        .option("{key}", {value_expr})\n'
    
    code += '''    )
    
    # Add table-specific options
    for k, v in table_opts.items():
        reader = reader.option(k, str(v))
    
    return reader.load()
'''
    
    return code


def generate_dlt_notebook(
    connector_name: str,
    config: Dict[str, Any],
    tables: List[Dict[str, str]],
    output_path: Path,
    connector_path: str,
    pipeline_group: str = None
) -> None:
    """Generate DLT notebook for a pipeline group."""
    
    if not tables:
        raise ValueError(f"No tables provided for pipeline group: {pipeline_group}")
    
    group_name = pipeline_group or "all_tables"
    
    # Start notebook
    notebook = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # {connector_name.upper()} DLT Pipeline - {group_name}
# MAGIC 
# MAGIC Auto-generated DLT pipeline with proper credential handling.
# MAGIC 
# MAGIC **Connector:** {connector_name}
# MAGIC **Pipeline Group:** {group_name}
# MAGIC **Tables:** {len(tables)}
# MAGIC 
# MAGIC This notebook properly handles credentials by reading secrets in the driver
# MAGIC context and passing them as options to the connector (which runs in executors).

# COMMAND ----------

# DBTITLE 1,Setup
import dlt
import urllib3

# Silence SSL warnings (if needed for development)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
CONNECTOR_NAME = "{connector_name}"
CONNECTOR_PATH = "{connector_path}/{connector_name}_generated_source.py"

print(f"Pipeline: {{CONNECTOR_NAME}} - {group_name}")
print(f"Tables: {len(tables)}")

# COMMAND ----------

# DBTITLE 1,Register Connector
# Load and register the Lakeflow connector
exec(open(CONNECTOR_PATH).read())
register_lakeflow_source(spark)

print(f"✓ {{CONNECTOR_NAME}} connector registered")

# COMMAND ----------

# DBTITLE 1,Load Credentials from Secrets
{generate_secret_retrieval_code(config)}

# COMMAND ----------

# DBTITLE 1,Helper Function
{generate_helper_function(connector_name, config)}

# COMMAND ----------

# DBTITLE 1,Table Definitions
# This section defines all DLT tables for pipeline group: {group_name}

'''

    # Generate @dlt.table for each table
    for i, table_row in enumerate(tables, 1):
        source_table = table_row['source_table']
        dest_table = table_row.get('destination_table', '').strip() or source_table
        
        # Parse table_options_json
        table_opts = {}
        if table_row.get('table_options_json', '').strip():
            try:
                table_opts = json.loads(table_row['table_options_json'])
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON for {source_table}: {e}")
        
        # Generate safe function name
        func_name = dest_table.replace('-', '_').replace('.', '_').replace(' ', '_').lower()
        
        # Add table definition
        notebook += f'''
@dlt.table(
    name="{dest_table}",
    comment="{connector_name.upper()} - {source_table}"
)
def {func_name}():
    """Ingest {source_table} from {connector_name.upper()}."""
    return get_connector_stream(
        "{source_table}"'''
        
        # Add table options
        if table_opts:
            for k, v in table_opts.items():
                # Escape quotes in values
                v_str = str(v).replace('"', '\\"')
                notebook += f',\n        {k}="{v_str}"'
        
        notebook += '''
    )
'''
    
    # Add summary
    notebook += f'''
# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary
# MAGIC 
# MAGIC **Connector:** {connector_name}
# MAGIC **Group:** {group_name}
# MAGIC **Tables:** {len(tables)}
# MAGIC 
# MAGIC ### Tables in This Pipeline:
'''
    
    for table_row in tables:
        source = table_row['source_table']
        dest = table_row.get('destination_table', '').strip() or source
        opts = table_row.get('table_options_json', '').strip()
        
        notebook += f"\n# MAGIC - `{source}` → `{dest}`"
        if opts:
            notebook += f"\n# MAGIC   - Options: `{opts[:100]}{'...' if len(opts) > 100 else ''}`"
    
    notebook += '''
# MAGIC 
# MAGIC ### Authentication:
# MAGIC - Credentials read from Databricks Secrets in driver context
# MAGIC - Passed as options to connector (runs in executors)
# MAGIC - Supports OAuth token refresh for long-running pipelines
'''
    
    # Write notebook
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(notebook, encoding='utf-8')
    
    print(f"✓ Generated: {output_path.name}")
    print(f"  Group: {group_name}")
    print(f"  Tables: {len(tables)}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate DLT notebooks for Lakeflow Connect connectors with proper secret handling",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  # OSI PI - Generate notebooks for all groups
  python generate_dlt_notebooks_generic.py \\
    --connector-name osipi \\
    --input-csv examples/osipi/osipi_tables_metadata.csv \\
    --output-dir dlt_notebooks/osipi \\
    --connector-path /Workspace/Users/user@company.com/connectors

  # HubSpot - Generate single notebook
  python generate_dlt_notebooks_generic.py \\
    --connector-name hubspot \\
    --input-csv examples/hubspot/tables.csv \\
    --output-dir dlt_notebooks/hubspot \\
    --pipeline-group main

  # Custom connector with config file
  python generate_dlt_notebooks_generic.py \\
    --connector-name mycustom \\
    --connector-config configs/mycustom_config.json \\
    --input-csv tables.csv \\
    --output-dir dlt_notebooks/mycustom

Connector Config JSON Format:
{
  "secrets_scope": "my-connector",
  "secret_mappings": {
    "api_key": ["api_key", "token"],
    "base_url": ["base_url"]
  },
  "static_options": {
    "verify_ssl": "true"
  },
  "dynamic_options": {
    "workspace_host": "f\\"https://{spark.conf.get('spark.databricks.workspaceUrl')}\\""
  }
}
        """
    )
    
    parser.add_argument(
        "--connector-name",
        required=True,
        help="Connector name (osipi, hubspot, zendesk, zoho_crm, etc.)"
    )
    
    parser.add_argument(
        "--input-csv",
        required=True,
        help="Metadata CSV with table definitions"
    )
    
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for generated notebooks"
    )
    
    parser.add_argument(
        "--connector-path",
        required=True,
        help="Workspace path where connector files are uploaded (without filename)"
    )
    
    parser.add_argument(
        "--connector-config",
        help="Path to JSON config file for custom connectors (optional for known connectors)"
    )
    
    parser.add_argument(
        "--pipeline-group",
        help="Generate notebook for specific pipeline group only (optional)"
    )
    
    parser.add_argument(
        "--generate-all-groups",
        action="store_true",
        help="Generate one notebook per unique pipeline_group in CSV"
    )
    
    args = parser.parse_args()
    
    # Load connector config
    config_file = Path(args.connector_config) if args.connector_config else None
    config = load_connector_config(args.connector_name, config_file)
    
    # Read CSV
    csv_path = Path(args.input_csv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        all_tables = list(reader)
    
    if not all_tables:
        raise ValueError("No tables found in CSV")
    
    # Determine which notebooks to generate
    if args.generate_all_groups:
        # Generate one notebook per unique pipeline_group
        groups = {}
        for table in all_tables:
            group = table.get('pipeline_group', '').strip() or 'default'
            if group not in groups:
                groups[group] = []
            groups[group].append(table)
        
        print(f"Generating {len(groups)} notebooks (one per pipeline group):")
        print()
        
        for group, tables in sorted(groups.items()):
            output_file = output_dir / f"{args.connector_name}_{group}.py"
            generate_dlt_notebook(
                args.connector_name,
                config,
                tables,
                output_file,
                args.connector_path,
                group
            )
            print()
        
        print(f"✓ Generated {len(groups)} notebooks in {output_dir}")
        
    elif args.pipeline_group:
        # Generate for specific group
        tables = [t for t in all_tables 
                 if t.get('pipeline_group', '').strip() == args.pipeline_group]
        
        if not tables:
            raise ValueError(f"No tables found for group: {args.pipeline_group}")
        
        output_file = output_dir / f"{args.connector_name}_{args.pipeline_group}.py"
        generate_dlt_notebook(
            args.connector_name,
            config,
            tables,
            output_file,
            args.connector_path,
            args.pipeline_group
        )
        
    else:
        # Generate single notebook with all tables
        output_file = output_dir / f"{args.connector_name}_all.py"
        generate_dlt_notebook(
            args.connector_name,
            config,
            all_tables,
            output_file,
            args.connector_path,
            None
        )


if __name__ == "__main__":
    main()
