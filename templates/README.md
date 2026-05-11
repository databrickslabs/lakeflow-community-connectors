# Templates

These templates standardize the artifacts produced during connector development. Each template is consumed by a specific AI skill to ensure consistent, high-quality output across all community connectors.

- **[Source API Document Template](templates/source_api_doc_template.md)** — Structures the research output for a source system's API. Covers authorization, object discovery, schema retrieval, primary keys, ingestion types (CDC, snapshot, append), data retrieval with pagination, field type mapping, and source references. Used by the `research-source-api` skill.

- **[Community Connector Documentation Template](templates/community_connector_doc_template.md)** — Provides the skeleton for public-facing connector documentation. Includes sections for prerequisites, connection parameters, supported objects, data type mapping, pipeline configuration, and troubleshooting. Used by the `create-connector-document` skill.

- **[Connector Spec Template](templates/connector_spec_template.yaml)** — Defines the YAML specification for a connector's Unity Catalog connection. Supports single or multiple authentication methods, OAuth 2.0 flows, and an external options allowlist for table-specific parameters. Used by the `generate-connector-spec` skill.
