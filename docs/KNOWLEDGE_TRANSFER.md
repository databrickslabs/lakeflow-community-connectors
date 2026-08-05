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
>
> **Start with §2** for the two user journeys the whole system exists to serve,
> **§13** for how quality is enforced when contributors are outside the core
> team, and **§17** for what is still unfinished.

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
   table contract (see §5) so it integrates with the shared SDP.

This doc focuses on approach #1.

---

## 2. The two main CUJs

Before any of the internals, it helps to know **what a user actually does with
this project**. There are two consumption journeys. They run the *same connector
code* through the *same* Spark format, and differ in how the connector code
reaches the cluster and how the pipeline is defined.

| | **CUJ 1 — Repo → Python SDP** | **CUJ 2 — Packages → managed ingestion** |
|--|-------------------------------|------------------------------------------|
| How code arrives | Import/clone the **repo** into the workspace | Install connector **Python packages** (wheels) |
| Pipeline definition | A Python source file you own (`ingest.py`) | Server-side `ingestion_definition` on the pipeline |
| What the user writes | Python (a `pipeline_spec` dict) | Nothing — UI wizard or a CLI spec |
| Browse-source UI | No | **Yes** |
| Same UX as 1P connectors | No | **Yes** — identical steps to every other ingestion connector |
| Status | The original path; still supported | **The default and strategic direction** |

Everything later in this doc is, in some sense, an explanation of one of these
two columns.

### CUJ 1 — Import the repo into the workspace to create a Python SDP

The user brings the connector code into their workspace as source files and
drives ingestion from a Python pipeline they can read and edit.

1. **Get the code into the workspace** — clone/import
   `lakeflow-community-connectors` as a Git folder, or let
   `create_pipeline --use-workspace-pipeline` do it (that flag is the legacy
   workspace mode; see §11).
2. **Create a UC connection** holding the credentials (`COMMUNITY` type), via
   the UI or `create_connection`.
3. **Author `ingest.py`** from `ingest_template.py`. This is the whole contract
   the user sees:
   ```python
   spark.conf.set(
       "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

   source_name = "github"
   pipeline_spec = {
       "connection_name": "my_github_conn",
       "objects": [{"table": {"source_table": "issues"}}],
   }

   register(spark, source_name)      # register the Spark data source
   ingest(spark, pipeline_spec)      # build the SDP tables
   ```
4. **Create and run the SDP pipeline** pointing at that file.

**Why it matters:** it is fully transparent and customizable — the user can
inspect and modify the pipeline logic, and it works today with no dependency on
managed-ingestion rollout. **The cost:** the user writes and maintains Python,
gets no browse UI, and this is the path that drags in the `_generated_*` merged
source files (`register(spark, name)` resolves a merged module and is explicitly
a legacy shim — see §17.1), because SDP could not import multi-file Python data
sources.

### CUJ 2 — Import the source Python packages to create a managed ingestion pipeline

The user installs the connector as **packages** and then follows **exactly the
same steps as every other Lakeflow ingestion connector** — this is the point of
the journey, and the reason it shares architecture with first-party connectors
(§9).

1. **Make the packages available** — the framework wheel and the connector
   wheel, uploaded to a UC Volume (`upload`, or built automatically by
   `create_pipeline`). Optionally `publish` the connector once so it appears as
   a **"Custom" tile in *Add Data*** (§11), after which the user's experience is
   pure point-and-click.
2. **Create a UC connection** (`COMMUNITY` type) — same wizard as any connector.
3. **Browse source objects** in the UI and pick tables. This is the
   schema-exploration service calling the connector's `_community_*` virtual
   tables (§9) — the step CUJ 1 cannot offer.
4. **Create and run the pipeline.** No Python is authored: the selection becomes
   a server-side `ingestion_definition`, and the wheels are referenced from
   `environment.dependencies`.

**Why it matters:** the user does not learn anything connector-specific — a
community connector looks and behaves like a managed one, monitoring and all.
Because the connector is a versioned package rather than source files, it also
needs no merged-file hack. **The current cost:** the wheels must be built and
uploaded first; publishing to PyPI would remove that step entirely and is the
highest-leverage open item (§17.2).

---

## 3. Repository layout

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

## 4. The connector interface (the contract)

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
  what powers the **browse UI's tree navigation** (§9).
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

## 5. From connector to Spark Data Source (`sparkpds/`)

**File:** `src/databricks/labs/community_connector/sparkpds/lakeflow_datasource.py`

The framework adapts a `LakeflowConnect` into Spark's Python Data Source API.
The Spark format name is **`lakeflow_connect`** (see §8 for why that matters for
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

## 6. From Data Source to declarative pipeline (`pipeline/`)

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
through to the connector — but only if allowlisted (see §7).

---

## 7. The connector spec (`connector_spec.yaml`)

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

## 8. How a connector actually runs — the runtime engine (`~/runtime`)

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

## 9. Running as a managed ingestion pipeline (`~/universe`)

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
  - `listSourceObjects` → `GET /pipelines/{pipeline_id}/source/objects`
    (hierarchical `path[]`, pagination, optional search `query`) →
    `SourceObject[]` (catalog/schema/table).
  - `getSourceObjectDetails` → `GET /pipelines/{pipeline_id}/source/object_details` →
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
`ingest(spark, pipeline_spec)` directly (§6) and there's no schema-exploration
service in front.

---

## 10. Testing (`source_simulator/` + `test_suite.py`)

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

The `Simulator` (`source_simulator/simulator.py`) installs an `Interceptor`
(`interceptor.py`) that patches `requests.sessions.Session.send` — and
urllib3's `urlopen`, for connectors that bypass `requests` — to intercept HTTP.
Supporting pieces: `cassette.py`
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
`replay_config` → `configs/replay_config.json` next to the test file
(gitignored, not committed).

**Write-back testing** (recommended, `lakeflow_connect_test_utils.py`) writes
data to the real source, reads it back, and verifies incremental reads and
deletes end-to-end.

---

## 11. The CLI (`tools/community_connector/`)

Installed as `community-connector` (`pip install -e tools/community_connector`).
It mirrors the Databricks "Add Data" UI flow and uses the Databricks SDK for
auth (PAT or service principal; `DATABRICKS_CONFIG_PROFILE` selects a profile).

| Command | What it does |
|---------|--------------|
| `create_connection <source> <name> -o '{...}'` | Create the UC `COMMUNITY` connection. Validates options against `connector_spec.yaml` and auto-sets `externalOptionsAllowList`. Supports `--auth-type static\|m2m\|u2m\|u2m_per_user` (defaults from the spec's `oauth.flow`) and `--spec/-s` (local path **or** GitHub repo URL). |
| `update_connection` | Update a connection; re-runs the browser OAuth flow for `u2m`. |
| `create_pipeline <source> <name> -n <conn>` | Create a pipeline. **Managed ingestion is the default**; `--use-workspace-pipeline` opts into the legacy clone-and-run-`ingest.py` mode. Accepts `-ps` (pipeline spec file/JSON), `--catalog/-c`, `--schema/-t`, `--package/-p` (pre-built wheel, repeatable), `--volume-path/-v`. |
| `update_pipeline <name> -ps <spec>` | Update an existing pipeline's spec, preserving other settings. With `--package` and no `-ps`, updates only the wheels. |
| `run_pipeline <name> [--full-refresh]` | Trigger a run. |
| `show_pipeline <name>` | Status. |
| `upload <source> --volume-path ...` | Build + upload the framework wheel **and** the connector wheel to a UC Volume (managed mode installs from there). `--skip-framework` when iterating. |
| `publish <source>` | Save the connector as a **workspace asset** so it shows up as a "Custom" tile in *Add Data*. See below. |
| `unpublish <source>` | Delete that asset again (`-d` display name, `-y` to skip the prompt). |

The **managed** vs **workspace** distinction lives in
`managed_pipeline.py`: managed pipelines send raw dict bodies to
`POST/PUT /api/2.0/pipelines` (the installed `databricks-sdk` has no `COMMUNITY`
`IngestionSourceType`), inject `ingestion_definition.source_type = COMMUNITY`,
set `pipelines.managedIngestion.registerPythonDataSource`, and reference the
uploaded wheels from `environment.dependencies`.

### `publish` — the connector as a workspace asset

`publish` is the bridge between a connector living in a repo and a connector
being **discoverable in the Databricks UI** without any repo wiring:

- It writes a JSON manifest to
  `/Users/<you>/.community-connectors/<display_name>.connector.json`. The server
  infers the `CommunityConnector` asset type from the `.connector.json`
  extension — the same call the webapp makes — so the connector appears as a
  "Custom" tile under *Add Data → Community connectors*.
- The manifest (`_build_community_connector_manifest`) mirrors the webapp's
  `buildCommunityConnectorAuthoringManifest` shape: `id`, `sourceName`,
  `displayName`, `connectionSpec` (the connector spec YAML verbatim), and
  `dependencies` (the uploaded wheel volume paths).
- By default it builds the framework + connector wheels from local source and
  uploads them to a UC Volume; `--package/-p` reuses pre-built wheels and
  `--volume-path/-v` picks the volume. `--overwrite` replaces an existing asset.
- Display name resolution (shared with `unpublish`): `--display-name` → the
  spec's `display_name` → the source name.

This is what makes a **customer-authored connector a first-class UI citizen**
(§15, Level 2) — the user creates a connection and browses source objects from
the tile, never touching the CLI again.

### Authentication modes

The `COMMUNITY` connection supports static credentials plus three OAuth flows:
`static` (arbitrary key/values), `m2m` (client-credentials), `u2m` (one human
authorizes once via a loopback browser flow at connection creation), and
`u2m_per_user` (each end user authorizes; UC resolves the right token per query).
OAuth-issued tokens are injected at query time, never entered by hand.

---

## 12. The developer journey (building a connector)

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
label that CI requires to merge — §13 covers that audit and the rest of the
quality machinery in detail. The individual skills
(`/research-source-api`, `/implement-connector`, `/test-and-fix-connector`,
`/generate-connector-spec`, `/deploy-connector`, `/write-back-testing`) are the
building blocks these commands orchestrate.

The legacy `/create-connector` runs everything in one interactive session; it's
**not** recommended for developers because it blocks on credentials between
research and implementation.

> **Repo constraint (from `CLAUDE.md`):** when developing a connector, only
> modify files under `sources/<source>/`. Don't touch library, pipeline, or
> interface code unless explicitly asked.

### Consuming a connector (end user)

§2 covers the two consumption journeys in full. In short:

1. **UI:** *+New → Add or upload data → Community connectors* (or *+ Add
   Community Connector* for a custom repo). Create a connection, browse source
   objects, pick tables, create the pipeline.
2. **CLI:** `create_connection` → `create_pipeline` → `run_pipeline`.

Either way the data lands as Delta tables in Unity Catalog with the CDC /
snapshot / append semantics the connector declared.

---

## 13. Enabling field & partners: how quality is enforced

The strategic bet of this project is that **people outside the core team — field
engineers, partners, customers — can build production connectors.** That only
works if quality is enforced by *machinery* rather than by a reviewer who
happens to know the source API. This section describes that machinery.

The core problem: a reviewer cannot meaningfully verify a Stripe connector
without Stripe credentials, and asking every contributor to hand over live
credentials does not scale and is not acceptable. So the framework is built so
that **correctness is demonstrable without credentials, and drift from reality
is detected separately.**

Five mechanisms, layered:

| Layer | Enforces | Needs creds? | Blocks merge? |
|-------|----------|--------------|---------------|
| Simulator-based tests | The connector actually works | No | Yes |
| Generic test harness | Contract compliance, uniformly | No | Yes |
| Live validator + coverage | The simulation matches reality | Yes, once | Via self-review |
| Self-review audit (54 checks) | Everything a reviewer would check | No | Yes (label) |
| CI workflows + fork gating | All of the above, per-source | No | Yes |

### 13.1 Simulation-based testing: correctness without credentials

The pivotal design decision. Instead of mocks (which encode the author's
assumptions and prove nothing) or live-only tests (which need credentials and
are flaky), the framework runs connectors against an **in-process simulation of
the source API** (§10).

A contributor declares the API's shape once, as data:

- `endpoints.yaml` — paths, params and their **roles** (`filter`, `page`,
  `per_page`), pagination style, response wrapper.
- `corpus/*.json` — sample records per endpoint.

The `Simulator` then serves requests with **real query and pagination
semantics** — so filtering, paging, and cursor advancement are genuinely
exercised, not stubbed. This is why offline tests are meaningful: a connector
that paginates incorrectly fails in `simulate` mode, with no credentials
anywhere.

For enablement this is the whole ballgame:

- A contributor can develop and prove a connector **without production access**
  to the source, and reviewers can verify it the same way.
- CI runs the full suite on every PR with **no secrets in the CI environment** —
  which is what makes accepting fork PRs from partners tractable at all.
- Phase 1 of the developer flow (`/develop-connector`) needs no credentials,
  so it is fully automatable and batchable across many sources.

### 13.2 The generic harness: contract compliance for free

A contributor writes a test class with a handful of attributes (§10) and
inherits **~9 auto-generated test families** covering `list_tables` validity,
invalid-table handling, schema and metadata correctness, snapshot and
incremental reads, delete flows, offset **termination**, and "every declared
column is populated by at least one record."

Two things this buys for enablement:

1. **Uniform quality floor.** Every connector is held to the same contract
   regardless of who wrote it or how much Spark they know. A partner cannot
   accidentally ship a connector that never terminates — `test_read_terminates`
   is generated for them.
2. **Contract violations surface as test failures, not review comments.** The
   offset fixed-point rule (§4) is the single easiest thing to get wrong and the
   most damaging in production; it is machine-checked.

### 13.3 Closing the loop: detecting simulation drift

The obvious objection to simulation is: *what if the spec is wrong, or the API
changed?* Then tests pass and production breaks. The framework answers this
directly.

In live mode the `Simulator` doubles as a **proxy + spec validator**
(`source_simulator/validator.py`). For every request it: forwards to the real
source, runs the *same* request through the simulate handler, and **diffs the
two responses**, writing a JSON report at the end of the run. Severity is
graduated by how decisive the mismatch is:

- **status_code** — live 404 vs simulated 200: the spec is fundamentally wrong
  about the endpoint.
- **shape** — dict vs array: wrapper config is wrong.
- **field set** — keys in live but not the corpus: corpus needs refresh.
- **type** — live `id` is an int, corpus has a string: schema drift.

Alongside it, `coverage.py` tracks which endpoints were actually exercised,
catching **spec endpoints never hit by any test** and **endpoints the connector
hits with no spec entry**.

This is the honest framing of the quality story: **offline tests prove the
connector obeys its contract; the live validator proves the contract matches
reality.** The two are separable, which is exactly why credentials are needed
once (Phase 2, `/validate-connector`) rather than continuously. The self-review
audit then checks that a clean live run actually happened (B11) and that
coverage was complete (B12) — so "I only ever ran it offline" is a detectable
state, not an invisible one.

### 13.4 The self-review audit: encoding reviewer expertise

`/self-review-connector` runs **54 checks** in five sections and writes a scored
`SELF_REVIEW.md`:

| Section | Checks | Covers |
|---------|--------|--------|
| A — Implementation | 13 | Interface compliance, read-pattern choice, `_init_time` termination cap, admission control, request timeouts, schema types, import hygiene, pylint |
| B — Testing & simulator | 14 | Test class wiring, spec/corpus presence, every hit URL has a spec entry, offline tests pass, **live run happened and validated cleanly**, coverage complete |
| C — Artifacts | 10 | Implementation, API doc, connector spec, README, package metadata, simulator spec/corpus — each parseable and consistent |
| D — Security | 10 | Hardcoded secrets, `eval`/`exec`, `subprocess`/`shell=True` |
| E — Cross-doc consistency | 7 | README, spec, and code agree on tables and parameters |

Findings are severity-weighted (BLOCKER ±3/−10, MAJOR +2/−3, MINOR +1/−1),
normalized to 0–100, and bucketed `READY` (90+) / `ALMOST` (75–89) /
`NEEDS WORK` (50–74) / `NOT READY`. **Any BLOCKER failure caps the verdict at
`NOT READY` regardless of score** — missing abstract methods or failing tests
mean it cannot ship even if everything else is perfect.

Why this matters for enablement: it **transfers reviewer expertise into a
checklist a contributor can run themselves, before a human looks at the PR.**
The hard-won lessons — cap the cursor at `_init_time` or reads never terminate;
always pass `timeout=`; use `LongType` not `IntegerType`; don't declare
`pyspark` as a runtime dependency because it conflicts with the cluster's — are
encoded as checks with exact remediation text rather than living in a
reviewer's head. A partner gets that feedback in minutes instead of a review
round-trip, and the maintainer's scarce attention goes to design rather than
mechanics.

The skill is **read-mostly**: it audits and reports, and does not modify
connector code, so its verdict means something.

### 13.5 CI: the merge gates

Five workflows, all running on a hardened runner group
(`databrickslabs-protected-runner-group`) whose only PyPI egress is a JFrog
proxy authenticated by GitHub OIDC:

| Workflow | Gate |
|----------|------|
| **Tests** | `changes` detects which sources a PR touches, then a **per-source matrix** fans out `pytest tests/unit/sources/<source>/` with `fail-fast: false`. Plus `test-libs`, `test-pipeline`, `test-example`, `test-community-connector`. `Tests / summary` is the required check. |
| **Pylint** | Lint gate on changed code, mirrored by the A11 self-review check so a contributor sees it before CI does. |
| **Connector Self-Review** | Detects significant connector-source changes and requires the `connector-self-reviewed` label. **Removed automatically on every new push**, so each revision needs a fresh audit. |
| **Verify Dependency Locks** | Regenerates locks and fails on drift, so `requirements/` always matches the declared deps. |
| **Generate Merged Source File** | Keeps the `_generated_*` files in sync (§17.1 — this one exists to be deleted). |

Two structural choices worth noting:

- **Per-source matrix isolation.** One partner's broken connector fails only its
  own matrix leg. With `fail-fast: false`, unrelated connectors still report
  green — essential when many contributors work in one repo.
- **`summary` treats `skipped` as pass.** Path-filtering legitimately skips
  whole jobs, so the aggregate gate must not confuse "nothing to do" with
  failure.

### 13.6 Accepting code from outside: the fork-PR gate

CI runs on a privileged runner with an OIDC token in scope, which GitHub only
issues in base-repo context. So fork PRs cannot run CI unmediated, and the
naive fix — clicking "approve and run" — turns the approval into an
arbitrary-code-execution channel.

Instead (`CONTRIBUTING.md`): fork-PR CI is gated on an explicit **`safe-to-test`
label**, applied by a maintainer after scanning the diff, and **auto-stripped on
every new push** so each commit needs fresh sign-off. The label model makes the
approval **auditable** in a way a UI click is not. Maintainers are told to check
the classic pwn-request vectors before labeling — `.github/**`,
`pyproject.toml`/`requirements/**`, `conftest.py` (pytest auto-loads it),
`setup.py` entry points, new top-level `__init__.py` — and to treat the click
like merging unreviewed code into a privileged context, because that is what it
is. Internal-branch PRs run automatically.

This is the security counterpart to enablement: the same openness that lets a
partner contribute is what makes CI an attack surface, so the trust boundary is
drawn explicitly at the label rather than left implicit.

### 13.7 Where the model is weakest

Stated plainly, since these are the gaps a new maintainer should know:

- **Live validation is point-in-time.** A clean validator run proves the spec
  matched reality *that day*. Nothing re-runs it on a schedule, so a source API
  that changes after merge is not detected until someone runs live mode again.
  The self-review checks recency (it flags a stale run — e.g. "last record-mode
  run was 23 days ago"), but recency is not freshness.
- **The corpus is synthetic.** Field *values* are never compared, only shapes
  and types, so semantic drift is invisible.
- **Coverage is spec-relative.** It catches endpoints in the spec that no test
  hit, but cannot know about a source endpoint the author never declared.
- **Self-review is self-attested.** It is a skill a contributor runs, and the
  CI gate checks for the *label*, not for an independently reproduced audit.
  It raises the floor; it is not an adversarial control.
- **One reviewer.** All of the above still funnels through a single CODEOWNER
  (§17.7).

---

## 14. Contributing a new connector (developer deep-dive)

§12 gives the command-level flow; this section is the "what actually lands on
disk and why" view for someone contributing a connector to the repo.

### The anatomy of a connector directory

Everything a connector needs lives under `sources/<source>/`. A complete
contribution produces:

| File | Produced by | Purpose |
|------|-------------|---------|
| `<source>.py` | you / `implement-connector` | The `LakeflowConnect` subclass (§4). |
| `__init__.py` | you | The `LakeflowSource` subclass wiring `_lakeflow_connect_cls` (§5). |
| `connector_spec.yaml` | `generate-connector-spec` | Connection params + `external_options_allowlist` (§7). |
| `README.md` | `create-connector-document` | Public end-user docs (uses `templates/community_connector_doc_template.md`). |
| `<source>_api_doc.md` | `research-source-api` | Research notes on the source API (uses `templates/source_api_doc_template.md`). |
| `src/.../community_connector/source_simulator/specs/<source>/endpoints.yaml` + corpus | `implement-connector`/`connector-tester` | Offline test fixtures (§10). |
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
   auto-generates the suite (§10).
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
- **Honor the offset fixed-point** (§4): emit a stable `end_offset` and return
  it unchanged when there's no new data, or the pipeline will loop.
- **Retries and rate limits** belong in the connector (see
  `_request_with_retry` in `example.py`), not the framework.
- **Idempotency** — reads must be safe to re-run from a checkpointed offset;
  cap cursors at an init-time timestamp to avoid drift.

### Getting a PR merged

- Run `/self-review-connector for <source>` for a scored audit; it adds the
  `connector-self-reviewed` label that **CI requires to merge** (§13.4).
- Fork PRs need a maintainer's `safe-to-test` label (re-applied per push) before
  CI runs — see §13.6 and `CONTRIBUTING.md`.
- **Write-back tests** (`write-back-testing` skill,
  `lakeflow_connect_test_utils.py`) are recommended: they write to the real
  source and verify the full read/incremental/delete cycle.

### Migrating an existing implementation

If a source was written against the raw Python Data Source API (approach #2),
the `migrate-legacy-implementation` skill helps refactor it onto
`LakeflowConnect` so it benefits from the shared streaming/offset/partition
machinery and the generic test harness.

---

## 15. Customizing as a customer

Customers don't have to fork the framework to adapt or extend connectors. There
are three escalating levels of customization, from config-only to full BYO.

### Level 1 — Configure an existing connector (no code)

Everything about *what* and *how* to ingest is data, not code:

- **Pipeline spec** (§6) — choose tables, destination catalog/schema/table,
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
  `environment.dependencies` (§11) — so a private connector runs with the same
  managed-ingestion experience as a built-in one.
- **`publish`** (§11) goes one step further: it saves the connector as a
  workspace asset so it appears as a "Custom" tile in *Add Data*, wheels and
  connection spec included. After a one-time `community-connector publish
  my_source`, your users get the same point-and-click experience as a built-in
  connector — create a connection, browse objects, pick tables — with no CLI and
  no repo URL to paste. `unpublish` removes the tile.

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

## 16. End-to-end mental model

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

## 17. Unfinished work and future directions

Everything above describes the system as it is. This section is the honest
counterpart: known debt, deliberate hacks, and directions worth taking. It is
written for whoever picks the project up, so the reasoning behind each item is
recorded rather than just the task.

Ordered roughly by a combination of pain and leverage.

### 17.1 Retire the `_generated_*` merged source files (biggest pain point)

**The hack.** Every connector ships a machine-merged single file —
`sources/<source>/_generated_<source>_python_source.py` — that inlines the
connector, the interface, the Spark adapter, and the utils into one module.
There are currently **29 of them totalling ~84k lines** of duplicated code,
produced by a **990-line** merge script (`tools/scripts/merge_python_source.py`)
with its own exclusion config, a dedicated CI workflow
(`.github/workflows/generate-merged-source-file.yml`), and a
branch-protection-required status check. `sparkpds/registry.py` knows how to
import these modules by name.

**Why it exists.** SDP / Python Data Source did not support module imports for
Python Data Source implementations, so a connector could not be deployed as a
normal package — everything had to be one file. See
`#es-1607732-pds-multi-file-import` for the history. This was a platform gap
that never got fixed on that side.

**Why it should go.** It is the most confusing part of the framework and the
least defensible. It duplicates every connector's code, guarantees the copy
will drift from the original, forces contributors through a regeneration step
they routinely forget (the self-review checklist has a whole item, C6, dedicated
to catching stale merged files), and inflates diffs on every PR.

**What unblocks it.** The **managed ingestion flow does not need it** — managed
pipelines install real wheels from a UC Volume via
`environment.dependencies` (§9, §11). The blocker is purely rollout: managed
ingestion is the CLI default but is not fully launched yet. Once it is, the
merge script, the generated files, the CI workflow, the registry's generated-
module path, and the C6 review item can all be deleted together.

**Secondary consequence.** This hack is also what would block "have an agent
generate a Python data source and run it directly in an SDP pipeline." Worth
noting that Genie is likely the wrong tool for that job — a specialized coding
agent is a much better fit for code generation of this shape.

### 17.2 Publish the packages to PyPI

**Current state.** There is no publish workflow — `.github/workflows/` has no
PyPI job. Users (and the CLI) build wheels locally and upload them to a UC
Volume before a managed pipeline can install them.

**Why it isn't done.** In response to supply-chain security issues,
`databrickslabs` repos now have stricter rules for workflows that publish Python
packages to PyPI, and the org has not yet set up an approved workflow for this.
**We could be the first repo to pioneer it** — which means the work is partly
technical and partly getting org-level sign-off on a trusted-publishing setup.

**Why it is high leverage.** This is arguably the single biggest unlock in the
list:

- For pre-built connectors, users **skip building and uploading wheels
  entirely** during setup — the pipeline just installs from PyPI.
- **Release becomes fully independent of SDP/DBR.** Bug fixes and features ship
  on their own cadence: turnaround measured in **minutes or hours instead of
  days**. This is the framework's core advantage over first-party connectors and
  it is currently unrealized.
- The packages become directly usable, so a user can consume a connector source
  from a **workspace Python SDP pipeline** without any of the volume plumbing.

Together this gives the community connector framework the **fastest dev loop for
building managed connectors, with full customization** — worth stating plainly
because it is the strategic case for the whole project.

### 17.3 Let connectors declare their own Spark format name

**Current state.** Every connector is read via `format("lakeflow_connect")`,
with the actual source selected by a separate option. The single generic format
name was chosen on the belief that unifying would be simpler.

**Why it should change.** In hindsight this was the wrong call. Forcing
`format("lakeflow_connect")` on users is confusing and poor public-API design —
`format("github")` is what anyone would expect, and the generic name leaks an
internal framework detail into the user-facing surface. It also makes the
published packages (§17.2) less useful and harder to explain.

**What unblocks it.** [runtime#229659][rt] — "Allow Python data source UC
connection injection to use the connection `sourceName` as format" — **merged
2026-07-01**. The engine now permits injection when the format matches the
connection's `sourceName` *or* the literal `lakeflow_connect` (§8), so the
platform side is done and this is now a **framework-side follow-up**: register
each source under its own format name and keep `lakeflow_connect` as a
deprecated alias for back-compat.

[rt]: https://github.com/databricks-eng/runtime/pull/229659

### 17.4 Deprecate the legacy `GENERIC_LAKEFLOW_CONNECT` connection type

The project has moved to the `COMMUNITY` UC connection type; the older
`GENERIC_LAKEFLOW_CONNECT` type remains only for back-compat.

**Scope.** Encouragingly, **this repo has no remaining references** to the
legacy type — the cleanup is entirely on the platform side (`models.scala` in
runtime, `connection.proto` in universe, where both types are still defined; see
§8 and §9). The work is to deprecate and remove the old path there, to avoid
confusion and the bug risk of two parallel connection types that must be kept in
sync.

### 17.5 Decide the fate of the ingestion-agent interface

**Current state.** The interface changes needed to support the ingestion-agent
APIs, including **dynamic tool discovery**, are already implemented:
`interface/supports_ingestion_agent.py`, `interface/agent_protocol.py`, and
`sparkpds/ingestion_agent_datasource.py`. But it is **undocumented and
unsupported by any skill** — no connector implements the mixin, and nothing in
`.claude/` or `templates/` mentions it. §4 flags it as "niche; skip on a first
pass," which is the current reality.

**This is a fork in the road, and it should be resolved deliberately:**

- **Pursue it.** Worth exploring once the ingestion agent itself is ready. The
  appeal is letting users **build and customize tools quickly** — dynamic tool
  discovery means a connector can expose source-specific operations to an agent
  without framework changes.
- **Or remove it.** If there is no plan to invest, the code should be deleted.
  Dormant, undocumented interface code pollutes the surrounding modules and
  actively confuses developers reading the interface for the first time.

Leaving it in limbo is the one option with no upside.

### 17.6 Add higher-level abstractions for common connector patterns

Because a connector has the full expressiveness of the Python Data Source API,
there is room for **specialized abstractions above `LakeflowConnect`** that
collapse recurring patterns into far less code — file-based sources are the
clearest example, where much of each implementation is boilerplate. The existing
mixins (§4) are the precedent; the idea is pattern-specific base classes rather
than another general-purpose layer.

### 17.7 Add more reviewers

`.github/CODEOWNERS` currently has a **single owner** (`* @yyoli-db`) and
already carries a TODO to replace it with a team handle once one exists. This is
a bus-factor and throughput risk: one person's unavailability blocks every
merge, since branch protection requires code-owner review. Growing the reviewer
pool — ideally behind a
`@databrickslabs/lakeflow-community-connectors-maintainers` team so load
rotates — is straightforward and overdue.

---

## 18. File index (jump-off points)

**This repo**
- Interface: `src/databricks/labs/community_connector/interface/lakeflow_connect.py`
- Mixins: `interface/supports_partition.py`, `interface/supports_namespaces.py`
- Reference connector: `sources/example/example.py`
- Spark adapter: `sparkpds/lakeflow_datasource.py`, `sparkpds/registry.py`
- SDP orchestration: `pipeline/ingestion_pipeline.py`
- Spec parsing: `libs/spec_parser.py`; type conversion: `libs/utils.py`
- Simulator: `source_simulator/{simulator,interceptor,handler,modes,endpoint_spec,cassette,corpus,pagination}.py` (+ `DESIGN.md`)
- Test harness: `tests/unit/sources/test_suite.py`
- CLI: `tools/community_connector/src/databricks/labs/community_connector_cli/{cli,managed_pipeline,connector_spec,oauth_flow,pipeline_spec_validator}.py`
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
