---
title: Lakeflow Community Connectors
subtitle: A Template-Driven, AI-Assisted Framework for Data Ingestion
author: Yong Li
---

# Lakeflow Community Connectors

### A Template-Driven, AI-Assisted Framework
### for Building Data Ingestion Connectors

---

## The Problem

Building a new data source connector is **repetitive and error-prone**:

- Every source needs the same plumbing — pagination, offsets, checkpointing, schema, retry, streaming
- Every source needs the same scaffolding — packaging, testing, docs, deployment
- Every connector author reinvents the same patterns in slightly different ways
- Ingesting from **long-tail SaaS sources** never makes the roadmap for a managed connector team

**Result:** slow delivery, inconsistent quality, high maintenance cost.

---

## Our Approach

Turn connector development into a **fill-in-the-blanks** exercise:

1. **Template / Framework** — a thin interface + shared libraries that absorb all the boilerplate
2. **AI Agent Plugin** — Claude Code / Cursor skills, subagents, and commands that drive the full workflow
3. **One-command creation** — `/create-connector <source>` takes a source from zero to a deployed pipeline

Built on top of:
- **Spark Python Data Source API** — the execution substrate
- **Spark Declarative Pipeline (SDP)** — the orchestration layer
- **Claude Code plugin model** — skills, subagents, commands

---

## Part 1 — The Template

---

## The Architecture Stack

```
   ┌──────────────────────────────────────────────────────────┐
   │  Spark Declarative Pipeline (SDP)                        │  ← orchestrates
   │  @sdp.view / apply_changes / apply_changes_from_snapshot │
   └──────────────────────┬───────────────────────────────────┘
                          │  spark.readStream.format("lakeflow_connect")
                          ▼
   ┌──────────────────────────────────────────────────────────┐
   │  Spark Python Data Source API   (contract)               │  ← abstract interface
   │  DataSource / DataSourceReader / (Simple)StreamReader    │     Spark defines these
   └──────────────────────▲───────────────────────────────────┘
                          │  implements
                          │
   ┌──────────────────────┴───────────────────────────────────┐
   │  LakeflowSource — specialized PDS implementation         │  ← WE write this, once
   │  • serves _lakeflow_table_list  (virtual table: tables)  │
   │  • serves _lakeflow_metadata    (PKs, cursor, ingest type)
   │  • batch + streaming reads with offset management        │
   │  • delete-flow routing for cdc_with_deletes              │
   │  • injects UC connection options into the connector      │
   └──────────────────────┬───────────────────────────────────┘
                          │  calls (plug-in point)
                          ▼
   ┌──────────────────────────────────────────────────────────┐
   │  LakeflowConnect  (user-written, per-source)             │  ← the only thing you write
   │  list_tables / get_table_schema / read_table_metadata /  │     per connector
   │  read_table / read_table_deletes                         │
   └──────────────────────────────────────────────────────────┘
```

- **The PDS API is a contract**, not a layer. Spark defines it; anyone can implement it.
- **`LakeflowSource` is a specialized implementation** of that contract — it knows how to serve the two virtual tables (`_lakeflow_table_list`, `_lakeflow_metadata`) and how to stream with offsets.
- **`LakeflowConnect` is the plug-in point** `LakeflowSource` calls into for source-specific behavior.

One specialized PDS implementation powers every connector. The author never touches Spark internals.

---

## Registration → End-to-End Ingestion

A connector becomes a running pipeline with one `register` + one `readStream`:

```python
# 1. Register — wraps LakeflowConnect as a Spark PDS named "lakeflow_connect"
register(spark, "github")   # or register(spark, MyLakeflowConnect)

# 2. Read — SDP pipeline calls the PDS by name, passing only the UC connection
@sdp.view
def issues_cdc():
    return (spark.readStream.format("lakeflow_connect")
              .option("databricks.connection", "my_github_conn")
              .option("tableName", "issues")
              .load())

# 3. Merge — SDP applies CDC into a Delta table
sdp.apply_changes(target="issues", source="issues_cdc",
                  keys=["id"], sequence_by=col("updated_at"))
```

The runtime handles the rest:

```
  UC connection options ──► LakeflowConnect.__init__
  tableName == "issues" ──► LakeflowConnect.read_table(...)
  pagination + offset checkpointing  ──►  SDP  ──►  Delta table (SCD, deletes)
```

Same registration path works for the UI, the CLI, and local tests.

---

## The UC Connection: Credentials as Infrastructure

We built a **generic Unity Catalog connection** that stores arbitrary key-value pairs — any source's auth shape fits in one object:

```
Workspace User ──►  UC Connection "my_github_conn"
                       { token: ***, api_url: ..., org: ... }
                                │
                                │  governed, ACL'd, audited
                                ▼
                       granted to pipeline principal
```

- **One connection type** covers API keys, tokens, basic auth, and custom schemes
- **Being extended to OAuth** — both User-to-Machine (U2M) and Machine-to-Machine (M2M) flows, so refresh-token handling moves out of the connector and into the platform
- Secrets **never appear in notebook code or pipeline spec** — only the connection *name*

---

## How Credentials Reach the Connector

The UC connection is **integrated into the Spark runtime**. Connector code never fetches or parses credentials — it just reads them from `options`.

```python
# Pipeline code — user passes the connection NAME only
spark.readStream.format("lakeflow_connect")
     .option("databricks.connection", "my_github_conn")   # ← name only
     .option("tableName", "issues")
     .load()
```

```python
# Connector code — receives creds already injected
class GithubLakeflowConnect(LakeflowConnect):
    def __init__(self, options):
        self._token = options["token"]      # ← injected by runtime
        self._api_url = options["api_url"]
```

- Runtime resolves the connection, checks permissions, injects key-values as `options`
- Same pattern for every connector — **no per-source credential plumbing**
- OAuth tokens are resolved/refreshed by the runtime; the connector just sees a valid token

---

## Template Design Goal

> A connector author should write **only source-specific logic**.
> Everything else is provided by the framework.

What "source-specific" means in practice:
- How do I list tables?
- What's the schema?
- How do I read a batch with an offset?

What the framework owns:
- Spark Python Data Source contract
- Streaming + batch reader implementations
- Offset management and checkpointing
- UC connection → options wiring
- SDP pipeline orchestration
- Packaging, registration, deployment

---

## The `LakeflowConnect` Interface

A single abstract class. Four required methods.

```python
class LakeflowConnect(ABC):
    def list_tables(self) -> list[str]: ...

    def get_table_schema(self, table_name, table_options) -> StructType: ...

    def read_table_metadata(self, table_name, table_options) -> dict:
        # returns: primary_keys, cursor_field, ingestion_type
        # ingestion_type ∈ {snapshot, cdc, cdc_with_deletes, append}
        ...

    def read_table(self, table_name, start_offset, table_options
                   ) -> tuple[Iterator[dict], dict]:
        # returns (records, end_offset)
        ...

    # Optional — only for cdc_with_deletes
    def read_table_deletes(self, ...): ...
```

No Spark imports. No streaming protocol. No checkpoint logic.

---

## How the Framework Amplifies Those 4 Methods

```
   Connector author writes              Framework provides
   ─────────────────────                ──────────────────
   list_tables()          ──►           lakeflow_datasource.py
   get_table_schema()                   (Spark PDS reader, stream reader,
   read_table_metadata()                 partitioned stream reader)
   read_table()           ──►           Offset / checkpoint management
   read_table_deletes()                 UC connection → options injection
                                        Metadata & table-list virtual tables
                                        SDP ingestion pipeline
                                        CLI deployment
```

The framework re-casts the 4 methods into:
`spark.read.format("lakeflow_connect").option(...).load()`

---

## Ingestion Types Supported Out-of-the-Box

| Ingestion Type       | What framework does                    | What author provides            |
| -------------------- | -------------------------------------- | ------------------------------- |
| `snapshot`           | Full refresh each run                  | `read_table` returns all rows   |
| `cdc`                | Merge on primary key, advance cursor   | Records sorted by cursor        |
| `cdc_with_deletes`   | Merge + apply tombstones               | Also implement `read_table_deletes` |
| `append`             | Insert-only, strict offset advancement | Small server-side page limit    |

Author picks a type per-table. Framework handles the rest.

---

## Batch Boundary & Admission Control

Every incremental table supports `max_records_per_batch`.

```
start_offset      ──►   read_table   ──►   (iter, end_offset)
                                              │
                           ┌──────────────────┘
                           ▼
                  end_offset == start_offset ?
                     │               │
                   yes              no
                     │               │
                   stop         call again with end_offset as start_offset
```

- Pagination, termination, and checkpointing are **framework-driven**
- Author only advances the cursor honestly

---

## Scaling Up: Partitioned Reads

For large sources, the template offers an opt-in extension:

```python
class MyConnect(LakeflowConnect, SupportsPartitionedStream):
    def get_partitions(self, table, start, end): ...
    def latest_offset(self, table, table_options): ...
```

- Sliding time-window read → parallel partitions across Spark executors
- Same interface; just add one mixin and two methods
- Used for high-volume sources where sequential reads are too slow

The **sliding window** pattern in `LakeflowConnect` is a natural on-ramp to `SupportsPartitionedStream`.

---

## Reference Implementation

`sources/example/example.py` — a simulated source that demonstrates every pattern:

- Snapshot, CDC, CDC + deletes, append
- Three incremental strategies (record-count, server-side limit, sliding window)
- Retry with exponential backoff
- Cursor capping at init time to prevent "chasing new data" mid-run

New connectors are written **by following this example**, not by reading framework internals.

---

## Part 2 — The AI Agent Plugin

---

## Why AI-Assist Belongs Here

Even with a great template, a human still has to:
- Read hundreds of pages of source API docs
- Map REST endpoints to tables
- Decide ingestion types per table
- Write + debug the connector against a live source
- Generate a spec, docs, packaging, and deployment config

**None of this is hard — but it's slow and mechanical.** Perfect target for an agent.

The template makes the agent's job possible: the surface area is narrow, the conventions are strict, and a reference implementation exists.

---

## Plugin Anatomy

Everything lives under `.claude/` and is auto-discovered by **Claude Code** and **Cursor**.

```
.claude/
├── commands/                 # User-facing entry points
│   └── create-connector.md       (/create-connector)
├── agents/                   # Specialized subagents
│   ├── source-api-researcher.md
│   ├── connector-dev.md
│   ├── connector-tester.md
│   ├── connector-doc-writer.md
│   ├── connector-spec-generator.md
│   ├── connector-package-builder.md
│   └── connector-auth-validator.md
└── skills/                   # Reusable step-level instructions
    ├── research-source-api/
    ├── implement-connector/
    ├── test-and-fix-connector/
    ├── deploy-connector/
    └── ...
```

Three layers: **commands orchestrate**, **subagents specialize**, **skills instruct**.

**Portable by design** — auto-discovered by both Claude Code and Cursor with zero config. No vendor lock-in to one IDE.

---

## The Three Building Blocks

### Skill
A self-contained, reusable **instruction document** (`SKILL.md`) for one step.
Stateless. Composable. Invoked by humans or agents.

### Subagent
A **specialized Claude instance** with its own model, tools, and loaded skills.
Runs in isolation — doesn't pollute the parent conversation's context.

### Slash command
A **user-facing orchestrator** that sequences subagents and skills into a workflow.
`/create-connector github` invokes the full 6-step pipeline.

---

## The `/create-connector` Workflow

```
/create-connector <source_name> [tables=...] [doc=...]

   │
   ├─ Step 1 — API research          [ subagent: source-api-researcher ]
   │                                   → writes {source}_api_doc.md
   ├─ Step 2 — Auth setup             [ skill: authenticate-source ]
   │                                   → collects + validates credentials
   ├─ Step 3 — Implementation         [ subagent: connector-dev ]
   │                                   → writes {source}.py (+ schemas)
   ├─ Step 4 — Testing & fixes        [ subagent: connector-tester ]
   │                                   → writes + runs test_{source}.py
   ├─ Step 5a — Docs                  [ subagent: connector-doc-writer ]
   │         5b — Spec                [ subagent: connector-spec-generator ]
   └─ Step 6 — Deployment             [ skill: deploy-connector ]
                                       → creates UC connection + SDP pipeline
```

Human confirmation gate between steps. Each step commits its own artifacts.

---

## Human-in-the-Loop by Design

Long agentic runs are scary. This workflow stays controllable:

- **Confirmation gate after every step** — agent summarizes what was produced, human picks `Continue` or `Review first`
- **Per-step git commits** — `feat({source}): step N - <summary>` after each step
- **Failure is localized** — a bad step doesn't poison the rest
- **Trivial rollback** — `git reset` to any prior gate
- **Resumable** — skip earlier steps and pick up mid-workflow

The agent does the toil. The human keeps the steering wheel.

---

## Why Subagents (Not One Big Prompt)

| Concern                  | Single-prompt approach    | Subagent approach             |
| ------------------------ | ------------------------- | ----------------------------- |
| Context window           | Bloats with every step    | Each agent starts fresh       |
| Tool scope               | All tools always on       | Least-privilege per agent     |
| Model choice             | One model for everything  | `sonnet` for research, `opus` for code |
| Failure isolation        | One bad step poisons all  | Failures are local            |
| Prompt maintainability   | One huge prompt           | Small focused prompts         |

Subagents = **parallelism, isolation, and specialization**.

---

## Example: `connector-dev` Subagent

```yaml
---
name: connector-dev
description: Develop a Python community connector...
model: opus
permissionMode: bypassPermissions
skills:
  - implement-connector
  - implement-partitioned-connector
---

You are an expert Python developer specializing in
Lakeflow Community Connectors.

## Your Mission
Implement the connector using the implement-connector skill
or the implement-partitioned-connector skill, depending
on the source API characteristics.
```

- Narrow mission
- Two skills loaded into context
- Chooses between standard and partitioned implementation based on API traits

---

## Progressive Disclosure of Context

Subagents load **only the skills they need** via frontmatter:

```yaml
skills:
  - implement-connector
  - implement-partitioned-connector
```

- `source-api-researcher` loads research skills, not implementation skills
- `connector-dev` loads implementation skills, not deployment skills
- `connector-tester` loads testing skills, not doc-writing skills

**Context stays lean even as the plugin grows.**
Adding the 20th skill doesn't make existing subagents slower or dumber.

This is the single most important engineering choice in the plugin.

---

## Skill Example: `implement-connector/SKILL.md`

A skill is a **dense, opinionated playbook** for one step:

- File organization rules
- Interface compliance checklist
- Schema conventions (`StructType` over `MapType`, `LongType` over `IntegerType`)
- Incremental read strategies (sliding window, server-side limit)
- Batch boundary handling for CDC vs. append-only
- Termination guarantees
- API usage rules (list vs. get, parent-child traversal)

**This is where institutional knowledge lives.** Every new connector gets it for free.

---

## Skills = The Other Half of the Framework

The `src/` framework is the **runtime**. The `.claude/skills/` are the **wisdom**.

Every skill captures a lesson learned the hard way across many connectors:

- "Records must be sorted ascending by cursor — otherwise infinite loop on resume"
- "Append-only tables cannot client-side truncate — duplicates or data loss"
- "Cap cursors at init time so a trigger doesn't chase new data mid-run"
- "Use `StructType` over `MapType`; `LongType` over `IntegerType`"

A new author writing their first connector inherits all of this automatically.
**Deleting the skills would hurt us more than deleting half the code.**

---

## Why This Plugin Pattern Generalizes

The framework + plugin pattern is not specific to connectors:

| Layer              | Connectors                     | Any domain                          |
| ------------------ | ------------------------------ | ----------------------------------- |
| Narrow interface   | `LakeflowConnect`              | Your domain's abstract class        |
| Reference impl     | `example/example.py`           | A canonical example                 |
| Shared libs        | `sparkpds/`, `pipeline/`       | Your runtime                        |
| Skills             | Research, implement, test, ... | Your workflow steps                 |
| Subagents          | Specialized roles              | Specialized roles                   |
| Slash command      | `/create-connector`            | `/create-<your-thing>`              |

**Any repetitive engineering task with a narrow surface can be collapsed into a one-command plugin.**

---

## Live Tests, Not Codegen

Most "AI writes your X" demos ship **plausible-looking code**.

This workflow runs Step 4 against the **real source API**:

```
connector-tester subagent
   │
   ├─ generates test file
   ├─ runs pytest against live credentials
   ├─ reads failure output
   ├─ patches the connector
   └─ loops until all tests pass
```

- No mocks. No synthetic fixtures.
- If the API returns something unexpected, the test catches it now
- Output is **verified**, not just generated

This is what separates an agentic workflow from a fancy autocomplete.

---

## Write-Back Testing: a First-Class Concern

Most connector frameworks quietly skip this. We make it a dedicated step.

```
/research-write-api-of-source   →  documents POST/PATCH/DELETE endpoints
/write-back-testing             →  generates write-back test utilities
```

The test then:
1. **Writes** new records into the source (e.g., create a GitHub issue)
2. **Reads** via the connector and verifies the record appears
3. **Updates** the record — verifies incremental sync catches the change
4. **Deletes** the record — verifies `cdc_with_deletes` produces the tombstone

End-to-end proof that the connector correctly models the source's lifecycle.

---

## What This Unlocks

- **Time to first connector**: days → hours
- **Long-tail source coverage**: community authors can ship without deep Spark expertise
- **Consistency**: every connector follows the same patterns, same docs, same tests
- **Live tests, not mocks**: tests run against real source APIs — write-back testing optional
- **Two deployment paths**: Databricks UI ("+ Add Community Connector") or CLI

---

## Part 3 — The UI Experience

---

## Entry Point: The Tile Gallery

```
  + New  ─►  Add or upload data  ─►  Community connectors

  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌──────────┐
  │ GitHub  │ │ Zendesk │ │ HubSpot │ │  Gmail  │ │   FHIR   │
  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └──────────┘
  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌──────────────────────┐
  │ Mixpanel│ │Qualtrics│ │   ...   │ │ + Add Community      │
  └─────────┘ └─────────┘ └─────────┘ │   Connector          │
                                      └──────────────────────┘
```

One click → guided flow, no notebooks, no CLI, no code.

---

## The Core User Journey

```
1. Click a tile  ────────────────►  source selected

2. Connection  ──►  ○ Create new UC connection
                    ○ Select existing connection

3. Pipeline setup
   ├─ Pipeline name
   ├─ Root path (where connector code will live as a workspace repo)
   └─ Default catalog + schema

4. Pipeline spec  ──►  tables, destinations, extra params per table

5. Run ingestion  ──►  SDP pipeline executes
```

What the UI does behind the scenes at step 3:
- **Sparse-checkout clone** of the connector code into a workspace repo under the chosen root path
- **Creates a workspace pipeline** that uses that repo as its source
- User can edit the pipeline spec later; connector code is right there in the workspace

---

## Config-Driven UI: One Connection, Many Faces

The insight that makes this scale:

```
         What the user sees                  What actually exists
         ─────────────────                   ────────────────────

   ┌─────────────────────────┐
   │  New GitHub Connection  │
   │  ─────────────────────  │
   │  Personal Access Token  │           ┌──────────────────────┐
   │  Organization           │   ──►     │                      │
   │  API Base URL           │           │   Generic UC         │
   └─────────────────────────┘           │   Connection         │
                                         │   (arbitrary         │
   ┌─────────────────────────┐           │    key-value pairs)  │
   │  New Zendesk Connection │   ──►     │                      │
   │  ─────────────────────  │           └──────────────────────┘
   │  Subdomain              │
   │  API Token              │
   │  Email                  │
   └─────────────────────────┘
```

Each connector's **`connector_spec.yaml`** describes which parameters to prompt for. The UI reads the spec and renders the form. **Adding a connector = adding a spec file.** No UI code changes.

---

## Custom Connectors: Bring Your Own Repo

Same flow, one difference — the user supplies **their own git repo path**:

```
Community connectors gallery
      │
      ▼
 ┌────────────────────────────┐
 │ + Add Community Connector  │
 └────────────┬───────────────┘
              ▼
        Paste git repo URL
              │
              ▼
   Sparse-checkout clone → workspace repo
   Read their connector_spec.yaml → render form
   Create pipeline → run ingestion
```

The typical path:

1. **Fork / clone** `lakeflow-community-connectors`
2. Run **`/create-connector <my_source>`** — the agent writes code, tests, docs, spec
3. Push to the user's own repo
4. Deploy via **"+ Add Community Connector"** pointing at that repo

**The same agents and skills power both first-party and custom connectors.**
A connector anywhere on GitHub becomes a deployable tile in minutes.

---

## Two Other Deployment Paths

The UI is primary, but there are escape hatches:

- **CLI** — `community-connector create_pipeline <source> <pipeline> -n <conn>` — same outcome, scriptable
- **Agent** — `/deploy-connector` — interactive, walks through the same steps as the UI

All three paths converge on the same artifacts: a UC connection, a workspace repo (sparse checkout), and an SDP pipeline.

---

## Part 4 — The Road Ahead

Current vs Future, one topic at a time.

---

## #1 — Architecture: Community → Managed Ingestion

We are merging community connectors into the **managed ingestion connector** architecture. The Python code stays the implementation; the pipeline around it changes.

```
 ┌────────────────────────────────┐      ┌────────────────────────────────┐
 │  Today                         │      │  Future                        │
 ├────────────────────────────────┤      ├────────────────────────────────┤
 │ Workspace repo                 │      │ Connector shipped as a wheel   │
 │  (sparse-checkout of the       │  ──► │  package                       │
 │   connector code)              │      │                                │
 │                                │      │ Managed ingestion pipeline     │
 │ SDP pipeline imports the repo, │      │  includes the wheel and runs   │
 │ runs register() at pipeline    │      │  register() in its init        │
 │ init                           │      │                                │
 │                                │      │ Same LakeflowConnect contract; │
 │ Community-only CUJ, separate   │      │ unified CUJ with managed       │
 │ from managed connectors        │      │ connectors                     │
 └────────────────────────────────┘      └────────────────────────────────┘
```

**What stays the same:** the `LakeflowConnect` interface, the agents and skills, the `connector_spec.yaml`.
**What changes:** how the connector code is packaged and how the pipeline picks it up.

---

## What the Convergence Buys Us

Four concrete wins from moving onto the managed ingestion architecture:

1. **Unified architecture** — one ingestion substrate for first-party and community connectors. One codebase, one mental model.

2. **Advanced features for free** — schema evolution, column selection, and everything else the managed ingestion pipeline already supports, inherited with no per-connector work.

3. **UI to explore the source and pick tables** — the managed flow already has this. Community connectors gain it automatically.

4. **Escapes SDP-editor limits for Python data sources** — SDP's editor doesn't support multi-file imports, which has been a persistent pain. Wheel-packaged connectors sidestep this entirely.

---

## Why Now: The Original Bet, Reconsidered

We chose the repo-based path originally for two reasons. Both reasons have since weakened.

| Original concern                                             | Why it held then                                               | Why it no longer blocks us                                     |
| ------------------------------------------------------------ | -------------------------------------------------------------- | -------------------------------------------------------------- |
| Let customers **customize the Python code** (Genie-style)    | Direct repo access was the easy path for user edits             | Wheel packages can still be built from a user repo; edits flow through the agent workflow, not in-place notebook hacks |
| **Maintenance burden** of "managed"-labeled connectors at scale | Hand-maintaining 20+ connectors under a managed SLA was risky  | **LLM agents** now do the mechanical maintenance — research, implement, test, fix — so scale is no longer a headcount problem |

**Net:** the constraints that justified the split have dissolved. Unifying is now the right call.

---

## #2 — Customization: Primitive → First-Class

Today, *prebuilt* connectors look polished and *custom* connectors look rough.
Tomorrow, both get the same UI — and custom ones become **publishable** in the workspace.

```
 ┌────────────────────────────────┐      ┌────────────────────────────────┐
 │  Today                         │      │  Future                        │
 ├────────────────────────────────┤      ├────────────────────────────────┤
 │ Prebuilt connectors            │      │ Prebuilt path: unchanged       │
 │  connector_spec.yaml synced    │      │                                │
 │  from this repo into universe  │  ──► │ Custom connectors: same UI     │
 │  → rich, source-specific form  │      │  as prebuilt                   │
 │                                │      │                                │
 │ Custom connectors              │      │ "Publish" action stores the    │
 │  primitive form, generic       │      │  connector metadata as a       │
 │  fields only                   │      │  Workspace Asset               │
 │  no way to "publish" to the    │      │                                │
 │  workspace                     │      │ → workspace-scoped custom      │
 │                                │      │  community connector,          │
 │                                │      │  discoverable by coworkers     │
 └────────────────────────────────┘      └────────────────────────────────┘
```

**Mechanism:** a new **Workspace Asset** type holds the connector's metadata (spec, repo pointer, version). The same front-end that reads universe-synced specs for prebuilt connectors reads workspace-asset specs for custom ones — one rendering path, two sources of spec.

---

## What Workspace Assets Unlock

- **Team sharing** — one person builds a custom connector; the whole workspace gets it as a tile
- **Governance** — custom connectors become first-class workspace objects (permissions, audit, lifecycle)
- **No more "first-party vs custom" gap** — UX quality stops depending on whether the connector lives in our repo
- **Lower bar for contribution** — publish locally first, promote to the main repo later if useful

The AI plugin ships the code; the workspace asset ships the **product experience**.

---

## #3 — Agents: One-Shot → Self-Evolving

Today the agent produces a connector in one shot. Tomorrow, a network of agents keeps it **correct, current, and evolving** without human babysitting.

```
 ┌────────────────────────────────┐      ┌────────────────────────────────┐
 │  Today                         │      │  Future                        │
 ├────────────────────────────────┤      ├────────────────────────────────┤
 │ One agent flow (customer-      │      │ Two flows:                     │
 │  oriented, connection-first)   │      │  • Customer: connection-first  │
 │                                │  ──► │  • Developer: repo-first,      │
 │ One-shot /create-connector     │      │    iterative, test-driven      │
 │                                │      │                                │
 │ Tests only against live source │      │ Live source + agent-seeded     │
 │                                │      │  mocks → real CI/CD coverage   │
 │ Human maintains the repo       │      │                                │
 │                                │      │ Support agents:                │
 │                                │      │  eval, review, API-sync, debug │
 │                                │      │                                │
 │                                │      │ Template evolves; agents       │
 │                                │      │  adapt in lockstep             │
 └────────────────────────────────┘      └────────────────────────────────┘
```

**Goal:** a repo that self-evolves. Super-super-low human maintenance.

---

## Testing: Real Instance + Agent-Seeded Mocks

Today's testing: **real source instance only.**

- ✅ Validates against a real API and real data — high confidence
- ❌ Can't run in CI/CD (credentials, rate limits, instability)
- ❌ Coverage limited to whatever the live account happens to contain
- ❌ Can't test failure modes, edge cases, or large-volume scenarios

Tomorrow's testing: **a combined model.**

```
   live source  ──►  agent captures representative traffic
                                 │
                                 ▼
                      seeds a shared mock fixture
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
       agent expands mock with          runs extended test matrix
       more data / edge cases /         in CI/CD — every PR
       failure modes
```

Live tests remain the ground truth. Mocks give us **breadth + CI safety**.

---

## Two Agent Flows: Customer vs Developer

Different audiences, different entry points.

| Flow         | Customer-oriented (today)                    | Developer-oriented (new)                           |
| ------------ | -------------------------------------------- | -------------------------------------------------- |
| Goal         | Deploy a working pipeline                    | Evolve a connector or the framework                |
| Start point  | UC connection first                          | Repo / branch first                                |
| Iteration    | Linear — 6 steps, gates, done                | Iterative — test, debug, refactor, re-test         |
| Test posture | Real live source                             | Mocks primarily, live for final validation         |
| Output       | Deployed SDP pipeline                        | PR with code + tests + docs                        |
| Audience     | Data engineers setting up ingestion          | Connector maintainers + contributors               |

Both flows share skills and subagents; they differ in **orchestration** and **defaults**.

---

## Support Agents: the Self-Evolving Repo

Beyond `/create-connector`, a portfolio of specialized agents keeps the codebase honest:

- **Eval agent** — scores connector output quality against a benchmark; regression-detects after template changes
- **Review agent** — does first-pass PR review on connector changes before humans see them
- **API-sync agent** — periodically crawls each source's API docs; opens PRs when endpoints, schemas, or auth flows drift
- **Debug agent** — ingested a failed test run, isolates root cause, proposes a fix PR

**Template ↔ agent co-evolution:** when a new connector pattern forces a template or PDS change, the **same PR updates the affected skills and subagents** so the next `/create-connector` already knows about it.

The repo stops being a codebase we maintain and becomes a system that maintains itself.

---

## Summary

- A **thin interface** (`LakeflowConnect`) hides Spark PDS + SDP plumbing
- A **reference implementation** teaches the patterns
- **Shared libraries** absorb pagination, offsets, streaming
- **Skills + subagents + slash commands** turn the workflow into `/create-connector`
- Works in **Claude Code** and **Cursor** out of the box

> Narrow interface + shared runtime + AI plugin
> = connectors become a fill-in-the-blanks exercise

---

## Q & A

Repo: [databrickslabs/lakeflow-community-connectors](https://github.com/databrickslabs/lakeflow-community-connectors)

Key paths:
- Interface — `src/databricks/labs/community_connector/interface/`
- Reference — `sources/example/example.py`
- Plugin — `.claude/{commands,agents,skills}/`
- CLI — `tools/community_connector/`
