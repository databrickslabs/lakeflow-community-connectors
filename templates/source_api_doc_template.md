# **{{Source_Name}} API Documentation**

## **Authorization**
- Supported methods; pick only one preferred method if multiple exist.
- Exact parameters and where they go (headers/query/body).
- OAuth note: connector **stores** `client_id`, `client_secret`, `refresh_token` and exchanges for access token at runtime; it **does not** run user-facing OAuth flows.
- Include example API requests that demonstrate authentication.

## **Object List**
- Describe the objects that can be retrieved from the source system.
- Provide a list of all objects or tables accessible.
- If an object is layered under another object, please describe that clearly.
- Indicate whether the object list is static or retrievable using an API call.
- If the object list is static, present the complete list of objects.
- If retrievable via an API, describe the endpoint for obtaining object names and include an example API request.


## **Object Schema**
- Explain how to retrieve the schema for a specific object.
- Provide details on how to retrieve table schemas using the API.
- If there is no API and the schema (properties) of the source tables and objects are static, please include these static descriptions.
- Include example API requests and responses to demonstrate this functionality.


## **Get Object Primary Keys**
- Explain how to get the primary key columns for a specific object.
- Provide details on how to get the primary key columns using the API.
- If there is no API and the primary key columns of the source tables and objects is static, please include the static primary key columns.
- Include example API requests and responses to demonstrate this functionality.

## **Object's ingestion type**
- Get information about whether a given object is to be incrementally ingested or not.
- There are 4 types: `cdc`, `cdc_with_deletes`, `snapshot`, `append`.
- `cdc` means the object can be read incrementally with upserts only.
- `cdc_with_deletes` means the object can be read incrementally with upserts AND supports fetching deleted records for delete synchronization.
- `snapshot` means the object can only be read with a snapshot, no incremental read.
- `append` means the object can be read incrementally but only new data, no change feeds. 

## **Read API for Data Retrieval**
- Describe how to efficiently read tabular data using the API.
- Explain the methods available to retrieve data (e.g., GET, POST).
- Provide guidance on incremental data retrieval using techniques like cursors or pagination.
- List required and optional query parameters.
- Provide details on implementing pagination and tracking data changes.
- Provide guidance on how to pull deleted records, either marked as a special field or from a completely different API endpoint.
- Include example API requests and responses for data retrieval.
- Compare different API options for reading the same data (if applicable).
- Include the extra parameters needed to read the table.
- Highlight the rate limits of the API. Source shouldn't exceed this limit.


## **Field Type Mapping**
- Define and describe the data types for fields within objects.
- Provide a mapping of API-specific field types to standard data types (e.g., string, integer, datetime).
- Explain special field behaviors such as auto-generated values, enumerations, or relationships.
- Highlight any constraints or validation rules associated with fields.

## Sources and References**
- Document all sources used to create this API documentation.
- Include URLs to official API documentation.
- List existing connector implementations referenced (e.g., Airbyte, Singer, dlthub).
- Note any user-provided documentation used.
- Include confidence level for each source (e.g., "Official API docs - highest confidence", "Airbyte implementation - high confidence", "Community implementation - medium confidence").
- If information conflicts between sources, explain which source was prioritized and why.
