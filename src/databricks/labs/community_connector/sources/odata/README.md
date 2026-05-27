# OData v4 Community Connector

A Lakeflow Connect community connector that ingests any OData v4 service
into Databricks. Schemas, table lists, and primary keys are discovered
automatically from the service's `$metadata` endpoint.

## Capabilities

- Discovers all entity sets via `$metadata`.
- Maps EDM primitive types (`Edm.String`, `Edm.Int32`, `Edm.DateTimeOffset`,
  `Edm.Decimal`, etc.) to Spark types.
- Snapshot ingest when no cursor is configured.
- Incremental CDC ingest when a per-table `cursor_field` is set
  (`field gt <last> and field le <now>`).
- Four auth methods: bearer, basic, api_key, oauth2 (client credentials).

## Setting up the connection (UC)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType, CreateConnection

w = WorkspaceClient()
w.connections.create(
    CreateConnection(
        name="northwind_odata",
        connection_type=ConnectionType.LAKEFLOW_COMMUNITY,
        options={
            "connector": "odata",
            "service_url": "https://services.odata.org/V4/Northwind/Northwind.svc/",
            "auth_method": "bearer",
            "token": "<token>",
            "externalOptionsAllowList": "cursor_field,select,filter,page_size,max_records_per_batch",
        },
    )
)
```

## Pipeline (ingest.py)

```python
from databricks.labs.community_connector.pipeline import build_pipeline
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect

build_pipeline(
    connector_cls=ODataLakeflowConnect,
    tables=[
        {
            "name": "Customers",
            "options": {},  # snapshot
        },
        {
            "name": "Orders",
            "options": {
                "cursor_field": "OrderDate",
                "max_records_per_batch": "5000",
            },
        },
    ],
)
```

## Per-table options

| Option                  | Default | Description |
| ----------------------- | ------- | ----------- |
| `namespace`             |         | OData schema namespace (e.g. `Sales`, `HR`). Required only when two schemas declare an entity set with the same name. |
| `cursor_field`          |         | Drives incremental reads. Omit for snapshot. |
| `select`                | all     | Comma-separated `$select` projection. |
| `filter`                |         | Extra OData `$filter` expression. |
| `page_size`             | 1000    | `$top` per HTTP request. |
| `max_records_per_batch` | 5000    | Cap on rows per `read_table` call. |

## Multi-tenant / multi-schema services

Services like SAP S/4HANA OData publish more than one `<Schema>` block in
`$metadata`. The connector implements `SupportsNamespaces`, so each schema
shows up as its own namespace in the catalog browser. When the same
entity set name appears in two schemas, set the `namespace` table option:

```python
{"name": "Customers", "options": {"namespace": "Sales"}}
{"name": "Customers", "options": {"namespace": "HR"}}
```

## Limitations

- Single-partition pagination via `@odata.nextLink`. Skiptokens are opaque,
  so we can't safely parallelize. Throughput is bounded by the source.
- Delete tombstones aren't synthesized — `ingestion_type` is never
  `cdc_with_deletes`. OData services don't expose deletions uniformly.
- Cursor field is assumed to be monotonically non-decreasing and naturally
  orderable by `$orderby`. Timestamps and monotonic IDs work; arbitrary
  fields don't.
