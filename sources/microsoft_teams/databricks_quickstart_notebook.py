# Databricks notebook source
# MAGIC %md
# MAGIC # Microsoft Teams Connector - Quick Test
# MAGIC
# MAGIC This notebook demonstrates how to use the Microsoft Teams connector in Databricks.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Upload `_generated_microsoft_teams_python_source.py` to Databricks (DBFS or Workspace)
# MAGIC 2. Have Azure AD credentials ready (tenant_id, client_id, client_secret)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load the Connector

# COMMAND ----------

# Option A: If you uploaded to DBFS
# %run /dbfs/path/to/_generated_microsoft_teams_python_source.py

# Option B: If you uploaded to Workspace
# %run /Workspace/Users/your.email@company.com/microsoft_teams/_generated_microsoft_teams_python_source

# Option C: Install from file
import sys
sys.path.append('/dbfs/FileStore/connectors/')  # Adjust path as needed

# For this example, we'll assume the file is accessible
from sources.microsoft_teams.microsoft_teams import LakeflowConnect

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Initialize Connector with Credentials

# COMMAND ----------

# Replace with your actual credentials
options = {
    "tenant_id": "YOUR_TENANT_ID",
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET"
}

connector = LakeflowConnect(options)
print("✅ Connector initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test - List Available Tables

# COMMAND ----------

tables = connector.list_tables()
print(f"Available tables: {tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Read Teams Data

# COMMAND ----------

# Get teams schema
schema = connector.get_table_schema("teams", {})
print(f"Teams schema: {schema}")

# Read teams data
records, offset = connector.read_table("teams", start_offset={}, table_options={})
teams_list = list(records)

print(f"\n✅ Found {len(teams_list)} teams")
for team in teams_list[:3]:  # Show first 3
    print(f"  - {team['displayName']} (ID: {team['id']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Convert to Spark DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession

# Create DataFrame from teams data
teams_df = spark.createDataFrame(teams_list, schema)

# Display the data
display(teams_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Write to Delta Table

# COMMAND ----------

# Write to Unity Catalog
teams_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("main.default.microsoft_teams_raw")

print("✅ Data written to main.default.microsoft_teams_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Query the Data with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.default.microsoft_teams_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Read Messages (CDC Mode)

# COMMAND ----------

# First, get a team_id and channel_id from the teams data above
team_id = teams_list[0]['id']  # Use first team

# Get channels for this team
channels_records, _ = connector.read_table("channels", {}, {"team_id": team_id})
channels_list = list(channels_records)

if channels_list:
    channel_id = channels_list[0]['id']
    print(f"Testing with Team: {teams_list[0]['displayName']}")
    print(f"Channel: {channels_list[0]['displayName']}")

    # Read messages with CDC
    messages_records, messages_offset = connector.read_table(
        "messages",
        start_offset={},
        table_options={
            "team_id": team_id,
            "channel_id": channel_id,
            "start_date": "2024-01-01T00:00:00Z",
            "top": "10"
        }
    )

    messages_list = list(messages_records)
    print(f"\n✅ Found {len(messages_list)} messages")

    # Convert to DataFrame
    if messages_list:
        messages_schema = connector.get_table_schema("messages", {})
        messages_df = spark.createDataFrame(messages_list, messages_schema)
        display(messages_df)
else:
    print("No channels found in this team")
