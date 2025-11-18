from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

"""
Please update the spec below to configure your ingestion pipeline.
"""
source_name = "<YOUR_SOURCE_NAME>"

pipeline_spec = {
    "connection_name": "<YOUR_CONNECTION_NAME>",
    "objects": [
        {
            "table": {
                "source_table": "<YOUR_TABLE_NAME>",
            }
        },
        {
            "table": {
                "source_table": "<YOUR_TABLE_NAME>",
                "table_configuration": {"scd_type": "SCD_TYPE_2"},
            }
        },
    ],
}


# dynamically import and register the lakeflow source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
