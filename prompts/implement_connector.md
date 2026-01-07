# Implement the Connector 

## Goal
Implement the Python connector for **{{source_name}}** that conforms exactly to the interface defined in  
[lakeflow_connect.py](../sources/interface/lakeflow_connect.py). The implementation should be based on the source API documentation in `sources/{source_name}/{source_name}_api_doc.md` produced by "understand-source".

## Implementation Requirements
- Implement all methods declared in the interface.
- At the beginning of each function, check if the provided `table_name` exists in the list of supported tables. If it does not, raise an explicit exception to inform that the table is not supported.
- When returning the schema in the `get_table_schema` function, prefer using StructType over MapType to enforce explicit typing of sub-columns.
- Avoid flattening nested fields when parsing JSON data.
- Prefer using `LongType` over `IntegerType` to avoid overflow.
- If `ingestion_type` returned from `read_table_metadata` is `cdc` or `cdc_with_deletes`, then `primary_keys` and `cursor_field` are both required.
- If `ingestion_type` is `cdc_with_deletes`, you must also implement `read_table_deletes()` to fetch deleted records. This method should return records with at minimum the primary key fields and cursor field populated. Refer to `hubspot/hubspot.py` for an example implementation.
- In logic of processing records, if a StructType field is absent in the response, assign None as the default value instead of an empty dictionary {}.
- Avoid creating mock objects in the implementation.
- Do not add an extra main function - only implement the defined functions within the LakeflowConnect class.
- The functions `get_table_schema`, `read_table_metadata`, and `read_table` accept a dictionary argument that may contain additional parameters for customizing how a particular table is read. Using these extra parameters is optional.
- Do not include parameters and options required by individual tables in the connection settings; instead, assume these will be provided through the table_options.
- Do not convert the JSON into dictionary based on the `get_table_schema` before returning in `read_table`. 
- If a data source provides both a list API and a get API for the same object, always use the list API as the connector is expected to produce a table of objects. Only expand entries by calling the get API when the user explicitly requests this behavior and schema needs to match the read behavior.
- Some objects exist under a parent object, treat the parent object's identifier(s) as required parameters when listing the child objects. If the user wants these parent parameters to be optional, the correct pattern is:
  - list the parent objects
  - for each parent object, list the child objects
  - combine the results into a single output table with the parent object identifier as the extra field.
- Refer to `example/example.py` or other connectors under `connector_sources` as examples

