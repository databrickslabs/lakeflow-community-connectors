# Lakeflow Community Connectors — Knowledge Transfer

A single-document walkthrough of how the Lakeflow Community Connectors project
works: the architecture, the data flow, the developer and end-user journeys,
the interface a connector implements, and how a connector runs both as a plain
workspace pipeline and as a first-class **managed ingestion pipeline** (with the
"browse source objects" UI) inside Databricks.

This doc spans three repos:

| Repo | Role | Where |
|------|------|-------|
| `lakeflow-community-connectors` | The connector framework + connectors themselves (Python). | this repo |
| `universe` | Control/data plane for managed ingestion pipelines, schema-exploration UI backend, UC connection model. | `~/universe` |
| `runtime` | Spark engine: Python Data Source V2 execution, UC connection option/credential injection. | `~/runtime` |

> **Audience:** engineers picking up the project. It assumes familiarity with
> Spark and Databricks but not with this codebase.

---

## 1. What this project is

Lakeflow Community Connectors let users ingest data from arbitrary source
systems (REST APIs, healthcare protocols, SaaS products) into Databricks. They
are built on two public Spark/Databricks primitives:

- **[Spark Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html)** — a connector is ultimately a Python `DataSource` that Spark reads from with `spark.read.format(...)` / `spark.readStream.format(...)`.
- **[Spark Declarative Pipelines (SDP)](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines)** — the connector runs inside a declarative pipeline that materializes source tables into Unity Catalog Delta tables with CDC / snapshot / append semantics.

Unlike Databricks' first-party ("managed") connectors, these are
community-maintained and carry no SLA. The value proposition of the framework:
**a connector author writes one Python class with a handful of methods, and the
shared library handles all the Spark Data Source plumbing, streaming, offset
management, partitioning, and pipeline generation.**

### Two implementation approaches

1. **`LakeflowConnect` interface (recommended).** Implement `list_tables`,
   `get_table_schema`, `read_table_metadata`, `read_table` (+ optional mixins).
   The framework adapts this to Spark. All AI-assisted tooling targets this
   path.
2. **Direct Python Data Source API (escape hatch).** Implement Spark's
   `DataSource`/`DataSourceReader`/`DataSourceStreamReader` yourself for full
   control over partitioning/schema. Must still speak the framework's virtual-
   table contract (see §4) so it integrates with the shared SDP.

This doc focuses on approach #1.

---

## 2. Repository layout

```
lakeflow-community-connectors/
├── src/databricks/labs/community_connector/
│   ├── interface/          # The contract a connector implements
│   │   ├── lakeflow_connect.py       # LakeflowConnect ABC (core)
│   │   ├── supports_partition.py     # SupportsPartition / SupportsPartitionedStream
│   │   ├── supports_namespaces.py    # SupportsNamespaces
│   │   └── supports_ingestion_agent.py
│   ├── sources/            # One directory per connector (github, stripe, gmail, ...)
│   │   └── example/        # Reference connector — read this first
│   ├── sparkpds/           # Adapter: LakeflowConnect -> Spark Python Data Source
│   │   ├── lakeflow_datasource.py    # LakeflowSource + reader classes
│   │   └── registry.py               # Dynamic source lookup
│   ├── pipeline/           # SDP orchestration (turns a spec into declarative tables)
│   │   └── ingestion_pipeline.py     # ingest(spark, pipeline_spec)
│   ├── libs/               # Shared utils: spec_parser, type conversion
│   └── source_simulator/   # Offline test infra (simulate / replay / live modes)
├── tests/unit/sources/
│   ├── test_suite.py       # LakeflowConnectTests — generic auto-generated test harness
│   └── <source>/           # Per-connector test classes
├── tools/community_connector/    # `community-connector` CLI (deploy / connect / run)
├── templates/              # Templates for AI-assisted development
└── .claude/                # Slash commands, skills, and agents for AI-assisted dev
```

Each `sources/<source>/` directory holds: the connector `.py`, `__init__.py`
(the `DataSource` subclass), `connector_spec.yaml` (connection params + option
allowlist), `README.md` (end-user docs), and an API doc.

---

## 3. The connector interface (the contract)

**File:** `src/databricks/labs/community_connector/interface/lakeflow_connect.py`

A connector subclasses `LakeflowConnect` and implements four abstract methods.
The framework instantiates it with a single `options: dict[str, str]` (which
carries credentials injected from the UC connection plus per-table options).

| Method | Signature | Purpose |
|--------|-----------|---------|
| `list_tables` | `() -> list[str]` | Enumerate table names (static or fetched from the source). |
| `get_table_schema` | `(table_name, table_options) -> StructType` | The Spark schema of a table. |
| `read_table_metadata` | `(table_name, table_options) -> dict` | Returns `primary_keys`, `cursor_field`, `ingestion_type`. |
| `read_table` | `(table_name, start_offset, table_options) -> (Iterator[dict], dict)` | The paginated read loop. Returns `(records, end_offset)`. |
| `read_table_deletes` | `(table_name, start_offset, table_options) -> (Iterator[dict], dict)` | *Optional.* Required only for `cdc_with_deletes`. |

### `ingestion_type` — the four modes

`read_table_metadata` declares how each table is ingested:

- **`snapshot`** — full re-read every run; no cursor. The pipeline uses
  `apply_changes_from_snapshot`.
- **`append`** — incremental append-only; new rows each run.
- **`cdc`** — incremental change capture keyed by `primary_keys` + `cursor_field`;
  upserts via `apply_changes`. No deletes.
- **`cdc_with_deletes`** — like `cdc` plus a delete flow. Requires implementing
  `read_table_deletes()`, which the framework drives with `isDeleteFlow=true`.

### The offset / pagination protocol (the crux of `read_table`)

The framework calls `read_table` repeatedly to paginate:

- `start_offset` is `None` **only** on the very first call of the very first
  run. On later runs it carries the last checkpointed offset.
- Each call returns `(records, end_offset)`. The framework feeds `end_offset`
  back in as the next `start_offset`.
- **Pagination stops when the returned offset equals the input offset** (a
  fixed point = "no more data").
- Records are **JSON-compatible dicts** — do *not* pre-convert to Spark types;
  the framework converts against `get_table_schema()` for you.
- For a table that can't be read incrementally, return `None` as the offset to
  read everything in one batch.

An important real-world subtlety (from the example connector): connectors cap
the cursor at an **init-time timestamp** so the cursor doesn't drift forward
during a long run, which would skip records.

### Optional mixins

These are additive capabilities. A connector composes them alongside
`LakeflowConnect`:

- **`SupportsPartition`** (`supports_partition.py`) — split a read across Spark
  executors. Implement `get_partitions(table, opts) -> Sequence[dict]` and
  `read_partition(table, partition, opts) -> Iterator[dict]`. Each partition
  dict must be JSON-serializable (it's shipped to an executor).
- **`SupportsPartitionedStream(SupportsPartition)`** — partitioned *streaming*.
  Adds `latest_offset(...)` (Spark calls it every micro-batch to discover new
  data) and an offset-aware `get_partitions(..., start_offset, end_offset)`.
  `is_partitioned(table)` can return `False` to fall back to the simple stream
  reader per-table. Micro-batch sizing is the connector's job via
  `table_options` (e.g. `window_days`, `max_records_per_batch`).
- **`SupportsNamespaces`** (`supports_namespaces.py`) — for hierarchical
  catalogs (GitHub `["org","repo"]`, Azure DevOps `["tenant","project"]`).
  Implement `list_namespaces(prefix) -> list[list[str]]` (one level of children)
  and `list_tables_in_namespace(namespace) -> list[str]`. Flat connectors skip
  this and the framework reports each table under an empty namespace. This is
  what powers the **browse UI's tree navigation** (§8).
- **`SupportsIngestionAgent`** (`supports_ingestion_agent.py`) — expose custom
  agent operations. Niche; skip on a first pass.

### Reference connector

**File:** `sources/example/example.py` (`ExampleLakeflowConnect`). Read this
first — it demonstrates every pattern against an in-process simulated REST API:

- Lazy `_api` resolution via a `@property` — connector instances are **pickled**
  and shipped to executors, so non-picklable state (sockets, locks) must be
  built lazily, not in `__init__`.
- `_request_with_retry` — exponential backoff on 429/500/503.
- Multiple incremental strategies: record-count pagination
  (`_read_incremental`), server-limit pagination (`_read_incremental_by_limit`),
  and fixed time windows (`_read_incremental_by_window`).
- A delete flow for the `orders` table (`read_table_deletes`).

---

## 4. From connector to Spark Data Source (`sparkpds/`)

**File:** `src/databricks/labs/community_connector/sparkpds/lakeflow_datasource.py`

The framework adapts a `LakeflowConnect` into Spark's Python Data Source API.
The Spark format name is **`lakeflow_connect`** (see §7 for why that matters for
UC option injection). `LakeflowSource(DataSource)` is the entry point; each
source ships a thin subclass:

```python
# sources/example/__init__.py
class ExampleDataSource(LakeflowSource):
    _lakeflow_connect_cls = ExampleLakeflowConnect

spark.dataSource.register(ExampleDataSource)   # (done by the framework)
```

`LakeflowSource` dispatches to one of these readers:

| Reader | Spark base | When |
|--------|-----------|------|
| `LakeflowBatchReader` | `DataSourceReader` | `spark.read` (snapshot / batch). |
| `LakeflowStreamReader` | `SimpleDataSourceStreamReader` | Streaming, non-partitioned. |
| `LakeflowPartitionedStreamReader` | `DataSourceStreamReader` | Streaming, connector implements `SupportsPartitionedStream`. |

Key mappings:
- `LakeflowSource.schema()` → `connector.get_table_schema()` (or a framework
  schema for the virtual tables below).
- `partitions()` → `connector.get_partitions()` (falls back to a single
  partition for non-partitioned connectors).
- `read(partition)` → `connector.read_partition()` or `connector.read_table()`.
- `LakeflowStreamReader.read(start)` routes to `read_table` or
  `read_table_deletes` depending on the `isDeleteFlow` option.

### Virtual tables (the metadata/discovery contract)

The framework exposes reserved "table names" that return **metadata**, not data.
These are what the pipeline, the CLI, and the managed-ingestion schema-explorer
all query to discover a source. This contract is what a direct Python Data
Source implementation must also honor.

| Virtual table | Option(s) | Returns |
|---------------|-----------|---------|
| `_community_table_metadata` | `tableNameList` (JSON list) | `tableName, primary_keys[], cursor_field, ingestion_type` per table. |
| `_community_namespaces` | `namespacePrefix` (JSON list, optional) | Immediate child namespaces under the prefix. Empty for flat connectors. |
| `_community_tables` | `namespace` (JSON list) | `(namespace[], tableName)` rows in one namespace. |

Example (from the interface README):

```python
spark.read.format("lakeflow_connect") \
     .option("databricks.connection", connection_name) \
     .option("tableName", "_community_table_metadata") \
     .option("tableNameList", json.dumps(["users", "orders"])) \
     .load()
```

`registry.py` does dynamic lookup: `find_data_source("github")` imports
`...sources.github` and returns its `LakeflowSource` subclass, matching on the
package boundary so `github` never resolves to `github_enterprise`.

---

## 5. From Data Source to declarative pipeline (`pipeline/`)

**File:** `src/databricks/labs/community_connector/pipeline/ingestion_pipeline.py`

`ingest(spark, pipeline_spec)` turns a **pipeline spec** into SDP tables:

1. Parse the spec with `SpecParser` (`libs/spec_parser.py`).
2. Query `_community_table_metadata` to learn each table's `primary_keys`,
   `cursor_field`, and `ingestion_type`.
3. For each table build an `SdpTableConfig` and route by ingestion type:
   - **cdc** → `_create_cdc_table`: a streaming `@sdp.view` reading
     `readStream.format("lakeflow_connect")` + `apply_changes(...)` with
     `stored_as_scd_type`. If `with_deletes`, a second delete-flow view
     (`isDeleteFlow="true"`) + `apply_changes(apply_as_deletes=expr("true"))`.
   - **snapshot** → `_create_snapshot_table`: batch view +
     `apply_changes_from_snapshot`.
   - **append** → `_create_append_table`: streaming `@sdp.append_flow`.

All reads inject `.option("databricks.connection", connection_name)` and the
per-table `table_config` options.

### The pipeline spec

The user-facing config that says *what* to ingest and *where* to land it:

```json
{
  "connection_name": "my_github_conn",
  "objects": [
    { "table": { "source_table": "issues",
                 "destination_catalog": "main",
                 "destination_schema": "raw_github",
                 "destination_table": "issues",
                 "table_configuration": {
                   "scd_type": "SCD_TYPE_1",
                   "primary_keys": ["id"],
                   "owner": "databricks", "repo": "spark"   // -> external options
                 } } }
  ]
}
```

`SpecParser` splits the special keys (`scd_type`, `primary_keys`, `sequence_by`,
`cluster_by`) out from the rest; everything else becomes `table_options` passed
through to the connector — but only if allowlisted (see §6).

---

## 6. The connector spec (`connector_spec.yaml`)

**Files:** `sources/<source>/connector_spec.yaml`

Distinct from the *pipeline* spec, the *connector* spec declares (a) what
credentials/params a UC connection needs, and (b) the **allowlist** of
per-table options that may be passed through to the connector. Example
(`github`):

```yaml
display_name: GitHub
connection:
  parameters:
    - name: token
      type: string
      required: true
      secret: true
      description: Personal access token for the GitHub REST API.
    - name: base_url
      type: string
      required: false
      description: Override for GitHub Enterprise Server.
external_options_allowlist: "owner,repo,state,per_page,start_date,org,pull_number,max_records_per_batch"
```

- `connection.parameters` → the fields a user fills when creating the UC
  connection; `secret: true` values are masked/encrypted.
- `external_options_allowlist` → a security boundary: only these table-option
  keys are injected into the connector, preventing arbitrary option passthrough.
- OAuth connectors add an `oauth:` block (`flow`, `pkce`, `scopes`,
  `authorization_url`, `token_url`, ...). The `flow` maps to UC's
  `community_oauth_flow`. See `sources/gmail/connector_spec.yaml`.

---

## 7. How a connector actually runs — the runtime engine (`~/runtime`)

At the bottom of the stack, the Databricks Spark engine executes the connector.
Community connectors have **no special code in runtime** — they ride the
generic **Python Data Source V2** + **UC connection injection** machinery.

### Python Data Source V2 execution

- Python side: `python/pyspark/sql/datasource.py` — the `DataSource`,
  `DataSourceReader`, `DataSourceStreamReader` base classes the framework
  subclasses. Registered via `spark.dataSource.register(...)` /
  `registerPython`.
- Scala side: `sql/core/.../datasources/v2/python/` — `PythonDataSourceV2`
  (`TableProvider`), `PythonTable` (BATCH_READ / MICRO_BATCH_READ / ...),
  `PythonScan`, `PythonPartitionReaderFactory`.
- Execution flows through Python workers: `create_data_source.py` instantiates
  `DataSource(options)` and calls `schema()`; `plan_data_source_read.py` calls
  `reader(schema).partitions()` then builds the per-partition `read()` function;
  each partition is read on an executor and converted to Arrow batches.

### UC connection → options + credential injection

This is the mechanism that gets credentials into the connector without the
connector ever handling secrets directly.

- The read uses `.option("databricks.connection", <name>)`.
- `sql/core/.../datasources/DataSourceUtils.scala` resolves the UC connection,
  extracts its key/value options, and **injects them into the DataFrame
  options** at planning time (`getUcConnectionOptions` /
  `getUcConnectionForDataSourceInjection`). Sensitive values are filtered/vended
  appropriately.
- The connection types that permit this live in
  `sql/core/.../managedcatalog/connections/models.scala`:
  - `Community` (and legacy `GenericLakeflowConnect`) — `allowArbitraryOptions =
    true`, required option `sourceName`.
  - Injection is allowed when `format` matches the connection's configured
    `sourceName` **or** the literal `"lakeflow_connect"` — which is why the
    framework registers under `lakeflow_connect` today.
- The ingest template sets
  `spark.databricks.unityCatalog.connectionDfOptionInjection.enabled = true` to
  turn this on.

So: **UC connection holds the creds → runtime injects them as options →
`LakeflowSource(options)` → `connector.__init__(options)`.** The connector just
reads `self.options["token"]`.

---

## 8. Running as a managed ingestion pipeline (`~/universe`)

A community connector can run two ways. Both execute the *same* connector code
through the *same* `lakeflow_connect` Spark format; they differ in how the
pipeline is defined, where credentials live, and whether there's a browse UI.

| | Workspace pipeline (SDP) | Managed ingestion pipeline |
|--|--------------------------|----------------------------|
| Pipeline definition | Python source files (`ingest.py`) cloned into the workspace; calls `ingest(spark, spec)`. | Server-side `ingestion_definition` (connection + objects + options) on the pipeline object. |
| Connector code delivery | Files in `/Repos` (or a repo). | Wheels uploaded to a UC Volume, referenced from `environment.dependencies`. |
| Credentials | UC connection (still). | UC connection (`COMMUNITY` type). |
| Browse-source UI | No. | **Yes** — same UI surface as first-party Lakeflow Connect. |
| Shares arch with 1P connectors | No. | **Yes** — goes through the Conduit framework. |

### The managed-ingestion architecture in universe

- **Pipeline definition proto:** `spark/pipelines/api/protos/extensions/ingestion.proto`
  — `IngestionPipelineDefinition` with `oneof source { connection_name; ingestion_gateway_id; ... }`,
  a list of `objects` (Schema/Table/Catalog specs), and `ConnectorOptions`
  whose oneof includes `community_connector_options` (arbitrary key/values).
- **UC connection type:** `managed-catalog/api/messages/connection.proto` —
  `ConnectionType.COMMUNITY` (= 73; legacy `GENERIC_LAKEFLOW_CONNECT` = 45).
  The connection stores `sourceName` (e.g. `github`) plus connector options.
- **Conduit integration:** community connectors plug into the shared **Conduit**
  connector framework used by first-party connectors:
  - `spark/pipelines/execution/conduit/community-connector/.../CommunityConnectorSource.scala`
    — a `ConduitSource` that drives the `lakeflow_connect` Python data source,
    including the `_community_*` virtual tables for discovery.
  - `spark/pipelines/execution/conduit/core/.../ConduitSourceFactory.scala`
    — maps `ConduitSourceType.COMMUNITY` → `CommunityConnectorSource(options)`.
    First-party connectors (Salesforce, Workday, ...) are peers in the same
    factory. This is what "shares architecture with other Lakeflow connectors"
    means concretely.
  - Registration is once-per-(session, connection); a Spark conf marker
    (`spark.databricks.internal.communityConnector.registered.<name>`) guards it.
- **Execution routing:** `ManagedIngestionPipelineExecutionExtension.scala`
  routes by pipeline type; community/SaaS connectors run through
  `SaasConnectorPipeline.scala`.

### The browse UI (object discovery)

The "browse source objects" experience is powered by a schema-exploration
service:

- **Proto/RPC:** `spark/pipelines/api/protos/schemaexploration.proto` —
  `SchemaExplorationService` with:
  - `listSourceObjects` → `GET /pipelines/{id}/source/objects` (hierarchical
    `path[]`, pagination, optional search `query`) → `SourceObject[]`
    (catalog/schema/table).
  - `getSourceObjectDetails` → `GET /pipelines/{id}/source/object_details` →
    `SourceTableDetails` (columns, types, primary keys, sequence_by,
    ingestion_type).
- **Backend:** `spark/pipelines/execution/service/schemaexploration/ExplorationBackend.scala`
  creates a `ConduitSource` (via the factory), caches iterators per session, and
  calls into `CommunityConnectorSource`.
- **Mapping to the connector:** `listSourceObjects` walks the namespace tree by
  reading `_community_namespaces` and `_community_tables`; details come from
  `_community_table_metadata` + `get_table_schema`. **This is exactly what
  `SupportsNamespaces` exists to feed** — a connector that implements it gets a
  navigable tree in the UI; a flat connector shows a single flat table list.

### End-to-end flow (managed)

```
User creates UC connection (type COMMUNITY, sourceName="github", token=...)
        │
User starts a managed ingestion pipeline referencing that connection
        │
UI: listSourceObjects(path=[])  ──►  ExplorationBackend  ──►  ConduitSourceFactory
        │                                                         │
        │                                              CommunityConnectorSource
        │                                                         │
        │                          spark.read.format("lakeflow_connect")
        │                            .option("databricks.connection","github")
        │                            .option("tableName","_community_namespaces") ...
        │
User picks tables ─► getSourceObjectDetails ─► _community_table_metadata + schema
        │
Pipeline deployed with ingestion_definition { connection_name, objects[...] }
        │
At runtime: SaasConnectorPipeline ─► readStream/read format("lakeflow_connect")
        │                              (UC injects creds; connector paginates)
        ▼
Delta tables in Unity Catalog (CDC / snapshot / append via apply_changes)
```

Contrast with the workspace flow, where the deployed `ingest.py` calls
`ingest(spark, pipeline_spec)` directly (§5) and there's no schema-exploration
service in front.

---

## 9. Testing (`source_simulator/` + `test_suite.py`)

Tests run **offline by default** — no creds, no network. Live runs happen on
demand to refresh recorded data or triage regressions.

### Modes (`source_simulator/modes.py`)

Selected by `CONNECTOR_TEST_MODE`:

- **`simulate`** (default for spec-based sources) — serve responses from an
  endpoint **spec** + a **corpus** of sample records, with full query/pagination
  semantics. Fully offline.
- **`replay`** — serve recorded interactions from a **cassette**. (Aliases to
  `simulate` for spec-based sources for back-compat.)
- **`live`** (a.k.a. `record`) — forward to the real source, optionally recording
  responses and tracking endpoint coverage. Needs credentials.

The `Simulator` (`source_simulator/simulator.py`) patches
`requests.Session.send()` to intercept HTTP. Supporting pieces: `cassette.py`
(recorded interactions), `endpoint_spec.py` (per-endpoint YAML: paths, params
with roles like `filter`/`page`/`per_page`, pagination style, response wrapper),
`corpus.py` (sample records per endpoint), `coverage.py` (which endpoints were
exercised).

### The generic harness (`tests/unit/sources/test_suite.py`)

Every connector's test class inherits `LakeflowConnectTests` and sets a few
class attributes:

```python
class TestGithub(LakeflowConnectTests):
    connector_class = GithubLakeflowConnect
    simulator_source = "github"          # -> simulate mode by default
    replay_config = {"token": "fake"}    # stand-in creds; simulator never validates
    sample_records = 200
    allow_empty_first_read = frozenset({"some_table"})
```

The harness auto-generates a broad suite: `list_tables` validity,
schema/metadata correctness, snapshot reads, incremental reads with offset
convergence, delete flows, "every column populated by at least one record",
namespace-tree invariants, and partition invariants. Credential precedence:
`CONNECTOR_TEST_CONFIG_JSON` → `CONNECTOR_TEST_CONFIG_PATH` → class
`replay_config` → local gitignored file.

**Write-back testing** (recommended, `lakeflow_connect_test_utils.py`) writes
data to the real source, reads it back, and verifies incremental reads and
deletes end-to-end.

---

## 10. The CLI (`tools/community_connector/`)

Installed as `community-connector` (`pip install -e tools/community_connector`).
It mirrors the Databricks "Add Data" UI flow and uses the Databricks SDK for
auth (PAT or service principal; `DATABRICKS_CONFIG_PROFILE` selects a profile).

| Command | What it does |
|---------|--------------|
| `create_connection <source> <name> -o '{...}'` | Create the UC `COMMUNITY` connection. Validates options against `connector_spec.yaml` and auto-sets `externalOptionsAllowList`. Supports `--auth-type static\|m2m\|u2m\|u2m_per_user` (defaults from the spec's `oauth.flow`). |
| `update_connection` | Update a connection; re-runs the browser OAuth flow for `u2m`. |
| `create_pipeline <source> <name> -n <conn>` | Create a pipeline. Workspace mode clones source files; managed mode sets `ingestion_definition`. Accepts `-ps` (pipeline spec file/JSON), `--catalog`, `--target`. |
| `update_pipeline <name> -ps <spec>` | Replace the pipeline's spec (`ingest.py`), preserving other settings. |
| `run_pipeline <name> [--full-refresh]` | Trigger a run. |
| `show_pipeline <name>` | Status. |
| `upload <source> --volume-path ...` | Build + upload the framework wheel **and** the connector wheel to a UC Volume (managed mode installs from there). `--skip-framework` when iterating. |

The **managed** vs **workspace** distinction lives in
`managed_pipeline.py`: managed pipelines send raw dict bodies to
`POST/PUT /api/2.0/pipelines` (the installed `databricks-sdk` has no `COMMUNITY`
`IngestionSourceType`), set `pipelines.managedIngestion.registerPythonDataSource`,
and reference the uploaded wheels from `environment.dependencies`.

### Authentication modes

The `COMMUNITY` connection supports static credentials plus three OAuth flows:
`static` (arbitrary key/values), `m2m` (client-credentials), `u2m` (one human
authorizes once via a loopback browser flow at connection creation), and
`u2m_per_user` (each end user authorizes; UC resolves the right token per query).
OAuth-issued tokens are injected at query time, never entered by hand.

---

## 11. Developer & end-user journeys (CUJs)

### Building a connector (developer, AI-assisted)

The recommended flow is two-phase, split at the credential boundary so Phase 1
needs no secrets and is fully automatable:

```
# Phase 1 — research, implement, simulator spec, simulate-mode tests, docs, PR
/develop-connector <source> [tables=t1,t2,...] [doc=<url_or_path>]

# Phase 2 — collect creds, run live tests, optionally deploy
/validate-connector <source>
```

Phase 1 orchestrates subagents: `source-api-researcher` (produces
`<source>_api_doc.md`) → `connector-dev` (the `.py` + simulator
`endpoints.yaml`) → `connector-spec-generator` (`connector_spec.yaml`) →
`connector-doc-writer` (`README.md`) → `connector-tester` (simulate-mode
tests). It opens a PR labeled `needs-live-testing`. `/batch-develop-connectors`
runs Phase 1 across many sources unattended.

Before human review, `/self-review-connector for <source>` writes a scored
`SELF_REVIEW.md`, posts it on the PR, and adds the `connector-self-reviewed`
label that CI requires to merge. The individual skills
(`/research-source-api`, `/implement-connector`, `/test-and-fix-connector`,
`/generate-connector-spec`, `/deploy-connector`, `/write-back-testing`) are the
building blocks these commands orchestrate.

The legacy `/create-connector` runs everything in one interactive session; it's
**not** recommended for developers because it blocks on credentials between
research and implementation.

> **Repo constraint (from `CLAUDE.md`):** when developing a connector, only
> modify files under `sources/<source>/`. Don't touch library, pipeline, or
> interface code unless explicitly asked.

### CI on fork PRs (from `CONTRIBUTING.md`)

Fork PRs don't run CI until a maintainer applies the `safe-to-test` label
(auto-stripped on every new push, so each commit needs a fresh sign-off). This
gates arbitrary-code-execution vectors — `.github/**`, `pyproject.toml`,
`conftest.py`, entry points — that would otherwise run on the privileged
runner. Internal-branch PRs run CI automatically.

### Consuming a connector (end user)

1. **UI:** *+New → Add or upload data → Community connectors* (or *+ Add
   Community Connector* for a custom repo). Create a connection, browse source
   objects, pick tables, create the pipeline.
2. **CLI:** `create_connection` → `create_pipeline` → `run_pipeline`.

Either way the data lands as Delta tables in Unity Catalog with the CDC /
snapshot / append semantics the connector declared.

---

## 12. Contributing a new connector (developer deep-dive)

§11 gives the command-level flow; this section is the "what actually lands on
disk and why" view for someone contributing a connector to the repo.

### The anatomy of a connector directory

Everything a connector needs lives under `sources/<source>/`. A complete
contribution produces:

| File | Produced by | Purpose |
|------|-------------|---------|
| `<source>.py` | you / `implement-connector` | The `LakeflowConnect` subclass (§3). |
| `__init__.py` | you | The `LakeflowSource` subclass wiring `_lakeflow_connect_cls` (§4). |
| `connector_spec.yaml` | `generate-connector-spec` | Connection params + `external_options_allowlist` (§6). |
| `README.md` | `create-connector-document` | Public end-user docs (uses `templates/community_connector_doc_template.md`). |
| `<source>_api_doc.md` | `research-source-api` | Research notes on the source API (uses `templates/source_api_doc_template.md`). |
| `source_simulator/specs/<source>/endpoints.yaml` + corpus | `implement-connector`/`connector-tester` | Offline test fixtures (§9). |
| `tests/unit/sources/<source>/test_*.py` | `connector-tester` | Test class extending `LakeflowConnectTests`. |

The `templates/` directory standardizes these artifacts, and each template is
consumed by a specific skill — so hand-authoring and AI-assisted authoring
produce the same shapes.

### Manual authoring loop (without the agents)

The AI commands are orchestration on top of an ordinary dev loop. To build a
connector by hand:

1. **Scaffold** — create `sources/<source>/`, subclass `LakeflowConnect` in
   `<source>.py`, and expose a `LakeflowSource` subclass in `__init__.py`:
   ```python
   # sources/foo/__init__.py
   from databricks.labs.community_connector.sources.foo.foo import FooLakeflowConnect
   from databricks.labs.community_connector.sparkpds import LakeflowSource

   class FooDataSource(LakeflowSource):
       _lakeflow_connect_cls = FooLakeflowConnect
   ```
2. **Implement the four methods** (`list_tables`, `get_table_schema`,
   `read_table_metadata`, `read_table`), copying patterns from
   `sources/example/example.py`. Add mixins only if the source needs them
   (hierarchical catalog → `SupportsNamespaces`; large parallel reads →
   `SupportsPartition`).
3. **Write the connector spec** — declare connection params and the
   `external_options_allowlist`. Any table option the connector reads from
   `table_options` must be allowlisted here or it won't be injected.
4. **Add offline test fixtures** — write `endpoints.yaml` describing the source
   API's paths/params/pagination and a corpus of sample records, so the test
   suite runs in `simulate` mode with no creds.
5. **Add the test class** — subclass `LakeflowConnectTests`, set
   `connector_class`, `simulator_source`, and `replay_config`. The harness
   auto-generates the suite (§9).
6. **Run** `pytest tests/unit/sources/<source>/` (offline by default). Iterate
   until green, then optionally run live with `CONNECTOR_TEST_MODE=live` and
   real creds to validate the connector against the real API and refresh the
   corpus.

### Design rules that keep a contribution mergeable

- **Only touch `sources/<source>/`.** The repo `CLAUDE.md` forbids changing
  interface/library/pipeline code as part of a connector PR — those are shared
  and reviewed separately. If the framework is genuinely missing something,
  raise it as its own change.
- **Pickle-safety.** Connector instances are serialized to Spark executors.
  Keep non-picklable state (HTTP sessions, locks) behind lazy properties, as
  `example.py` does with `_api`.
- **Return plain JSON dicts** from `read_table`; let the framework coerce to the
  declared schema. Don't build Spark `Row`s yourself.
- **Honor the offset fixed-point** (§3): emit a stable `end_offset` and return
  it unchanged when there's no new data, or the pipeline will loop.
- **Retries and rate limits** belong in the connector (see
  `_request_with_retry` in `example.py`), not the framework.
- **Idempotency** — reads must be safe to re-run from a checkpointed offset;
  cap cursors at an init-time timestamp to avoid drift.

### Getting a PR merged

- Run `/self-review-connector for <source>` for a scored audit; it adds the
  `connector-self-reviewed` label that **CI requires to merge**.
- Fork PRs need a maintainer's `safe-to-test` label (re-applied per push) before
  CI runs — see §11 and `CONTRIBUTING.md`.
- **Write-back tests** (`write-back-testing` skill,
  `lakeflow_connect_test_utils.py`) are recommended: they write to the real
  source and verify the full read/incremental/delete cycle.

### Migrating an existing implementation

If a source was written against the raw Python Data Source API (approach #2),
the `migrate-legacy-implementation` skill helps refactor it onto
`LakeflowConnect` so it benefits from the shared streaming/offset/partition
machinery and the generic test harness.

---

## 13. Customizing as a customer

Customers don't have to fork the framework to adapt or extend connectors. There
are three escalating levels of customization, from config-only to full BYO.

### Level 1 — Configure an existing connector (no code)

Everything about *what* and *how* to ingest is data, not code:

- **Pipeline spec** (§5) — choose tables, destination catalog/schema/table,
  `scd_type` (`SCD_TYPE_1` / `SCD_TYPE_2` / `APPEND_ONLY`), `primary_keys`,
  `sequence_by`, `cluster_by`.
- **Table options** — any key in the connector's `external_options_allowlist`
  (e.g. GitHub `owner`/`repo`/`state`, or `max_records_per_batch` to size
  micro-batches). Set them under `table_configuration` in the pipeline spec or
  as connection options.
- **Connection** — pick the auth mode the connector's spec supports (`static`,
  `m2m`, `u2m`, `u2m_per_user`) and supply credentials via
  `create_connection`.

This covers most customer needs and requires only the UI or the CLI.

### Level 2 — Bring your own connector repo

The framework can run connectors from a **customer-owned repository** — no
change to the upstream repo:

- **UI:** *+New → Add or upload data → + Add Community Connector*, pointing at
  your repo.
- **CLI:** the `--repo-url` / `-r` flag on `create_pipeline`, and `--spec`/`-s`
  on `create_connection`/`update_connection` accepting either a local
  `connector_spec.yaml` path **or a GitHub repo URL** — the CLI fetches
  `src/databricks/labs/community_connector/sources/<source>/connector_spec.yaml`
  from that repo:
  ```bash
  community-connector create_connection github my_conn \
    -o '{"token":"ghp_xxx"}' --spec https://github.com/myorg/my-connectors
  ```
- For **managed** pipelines, `upload` builds and ships your connector wheel (and
  the framework wheel) to a UC Volume, and the pipeline installs from there via
  `environment.dependencies` (§10) — so a private connector runs with the same
  managed-ingestion experience as a built-in one.

This is the path for a customer who has written a connector for an internal or
proprietary system and wants to keep it in their own repo.

### Level 3 — Fork and modify a connector

Because a connector is a self-contained directory, a customer can copy
`sources/<source>/` into their own repo, tweak the Python (add tables, change
pagination, map extra fields), and deploy it via Level 2. Keep the
`external_options_allowlist` in sync with any new options the modified connector
reads, and keep the offline test fixtures so the change stays verifiable.

> The same repo constraint applies for maintainability: customize inside the
> connector directory rather than the shared framework, so upstream framework
> upgrades remain drop-in.

---

## 14. End-to-end mental model

```
                 Connector author writes ONE class
   ┌──────────────────────────────────────────────────────────┐
   │  class FooConnect(LakeflowConnect[, SupportsNamespaces…]): │
   │     list_tables / get_table_schema /                       │
   │     read_table_metadata / read_table[ _deletes]            │
   └──────────────────────────────────────────────────────────┘
                              │  (sparkpds adapter)
                              ▼
        Spark Python Data Source  format("lakeflow_connect")
        + virtual tables: _community_table_metadata / _namespaces / _tables
                              │
              ┌───────────────┴───────────────────────┐
              ▼                                        ▼
   Workspace SDP pipeline                   Managed ingestion pipeline (universe)
   ingest(spark, pipeline_spec)             Conduit CommunityConnectorSource
   apply_changes / _from_snapshot           schema-exploration browse UI
              │                                        │
              └───────────────┬───────────────────────┘
                              ▼
      runtime: Python DataSource V2 execution
      + UC connection option/credential injection (DataSourceUtils)
                              ▼
                Delta tables in Unity Catalog
```

---

## 15. File index (jump-off points)

**This repo**
- Interface: `src/databricks/labs/community_connector/interface/lakeflow_connect.py`
- Mixins: `interface/supports_partition.py`, `interface/supports_namespaces.py`
- Reference connector: `sources/example/example.py`
- Spark adapter: `sparkpds/lakeflow_datasource.py`, `sparkpds/registry.py`
- SDP orchestration: `pipeline/ingestion_pipeline.py`
- Spec parsing: `libs/spec_parser.py`; type conversion: `libs/utils.py`
- Simulator: `source_simulator/{simulator,modes,endpoint_spec,cassette,corpus}.py`
- Test harness: `tests/unit/sources/test_suite.py`
- CLI: `tools/community_connector/src/databricks/labs/community_connector_cli/{cli,managed_pipeline,connector_spec}.py`
- Interface contract doc: `src/databricks/labs/community_connector/interface/README.md`

**universe (managed ingestion)**
- `spark/pipelines/api/protos/extensions/ingestion.proto` — `IngestionPipelineDefinition`, `CommunityConnectorOptions`
- `managed-catalog/api/messages/connection.proto` — `ConnectionType.COMMUNITY`
- `spark/pipelines/execution/conduit/community-connector/.../CommunityConnectorSource.scala`
- `spark/pipelines/execution/conduit/core/.../ConduitSourceFactory.scala`
- `spark/pipelines/api/protos/schemaexploration.proto` + `execution/service/schemaexploration/ExplorationBackend.scala`
- `spark/pipelines/execution/extensions/managedingestion/{ManagedIngestionPipelineExecutionExtension,saas/SaasConnectorPipeline}.scala`

**runtime (engine / UC)**
- `python/pyspark/sql/datasource.py` — Python Data Source base classes
- `python/pyspark/sql/worker/{create_data_source,plan_data_source_read}.py`
- `sql/core/.../datasources/v2/python/{PythonDataSourceV2,PythonTable,PythonScan,PythonPartitionReaderFactory}.scala`
- `sql/core/.../datasources/DataSourceUtils.scala` — UC connection option/credential injection
- `sql/core/.../managedcatalog/connections/models.scala` — `Community` / `GenericLakeflowConnect` connection types
