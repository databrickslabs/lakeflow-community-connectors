"""Example ingestion using the OSIPI Lakeflow connector.

This is a simple example you can run in a Databricks notebook.
Secrets must be read on the driver (dbutils) and passed as options.
"""

# Example (Databricks notebook)

# 1) Register the connector (merged file is typically uploaded to workspace)
# exec(open("/Workspace/Users/<you>@company.com/connectors/_generated_osipi_python_source.py").read())
# register_lakeflow_source(spark)

# 2) Read OAuth credentials from secrets in the notebook (driver)
# client_id = dbutils.secrets.get("sp-osipi", "sp-client-id")
# client_secret = dbutils.secrets.get("sp-osipi", "sp-client-secret")
# workspace_host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

# 3) Read a table
# df = (
#     spark.read.format("lakeflow_connect")
#     .option("databricks.connection", "<YOUR_UC_CONNECTION_NAME>")
#     .option("tableName", "pi_dataservers")
#     .option("pi_base_url", "https://<your-osipi-host>")
#     .option("workspace_host", workspace_host)
#     .option("client_id", client_id)
#     .option("client_secret", client_secret)
#     .option("verify_ssl", "false")
#     .load()
# )
# df.show()
