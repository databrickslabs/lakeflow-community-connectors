---
title: Lakeflow Community Connectors
subtitle: A Template-Driven, AI-Assisted Framework for Data Ingestion
author: Yong Li
---

# Lakeflow Community Connectors

### A Template-Driven, AI-Assisted Framework
### for Building Data Ingestion Connectors
### by Community (Databricks Eng & SA, Customers and Third Parties)

---

## The Problem

**Long-tail data sources never make the roadmap of a managed connector team.**

- A managed connector team can only build + support a handful of sources well
- The market has **thousands** of SaaS and industry-specific sources
- Every customer has "that one weird system" that blocks their migration to Databricks
- Building a one-off connector is repetitive, error-prone, and expensive to maintain
- Managed ingestion connectors are **tightly coupled with the engine** — they ship on the engine's release train
- **No customization path** for anyone outside Databricks engineering — SAs and customers cannot extend or fork

**Result:** low coverage compared to Fivetran and others, slow delivery and release, high maintenance cost.

We need a way to let **anyone** (Databricks Eng, SAs, customers, third parties) ship a production-grade connector quickly.

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
   └──────────────────────┬───────────────────────────────────┘
                          │  spark.readStream.format("lakeflow_connect")
                          ▼
   ┌──────────────────────────────────────────────────────────┐
   │  Spark Python Data Source API   (contract)               │  ← Spark defines
   └──────────────────────▲───────────────────────────────────┘
                          │  implements
   ┌──────────────────────┴───────────────────────────────────┐
   │  LakeflowSource — specialized PDS implementation         │  ← we write, once
   │     metadata  +  data reads  +  offset management        │
   └──────────────────────┬───────────────────────────────────┘
                          │  calls (plug-in point)
                          ▼
   ┌──────────────────────────────────────────────────────────┐
   │  LakeflowConnect  — per-source plug-in                   │  ← you write this
   └──────────────────────────────────────────────────────────┘
```

- **PDS API** — Spark's abstract contract
- **LakeflowSource** — one specialized impl, handles metadata + data uniformly
- **LakeflowConnect** — the per-source plug-in

> One PDS implementation powers every connector.

---

## One `register` + One `ingest`

```python
register(spark, "github")

pipeline_spec = {
    "connection_name": "my_github_conn",
    "objects": [
        {"table": {"source_table": "issues"}},
        {"table": {"source_table": "pull_requests",
                   "table_configuration": {"scd_type": "SCD_TYPE_2"}}},
    ],
}

ingest(spark, pipeline_spec)
```

Inside `ingest`:

- Look up metadata per table
- Generate views + `apply_changes`
- Wire delete flows for `cdc_with_deletes`

> **The user writes a spec. The library writes the pipeline.**

---

## The UC Connection

```
UC Connection "my_github_conn"
  { token: ***, api_url: ..., org: ... }
               │
               │  governed · ACL'd · audited
               ▼
      granted to pipeline principal
```

- **Generic** — arbitrary key-value pairs; any auth shape fits
- **OAuth coming** — U2M + M2M, refresh handled by the platform
- Pipeline code holds only the connection **name**, never the secrets

---

## Credentials: Injected by the Runtime

```python
# Pipeline code
.option("databricks.connection", "my_github_conn")   # name only
```

```python
# Connector code
class GithubLakeflowConnect(LakeflowConnect):
    def __init__(self, options):
        self._token = options["token"]               # already injected
```

- Runtime resolves · permission-checks · injects
- Zero credential plumbing in the connector
- OAuth refresh lives in the runtime

---

## Template Design Goal

> **Write only source-specific logic.**

| You write                     | Framework owns                            |
| ----------------------------- | ----------------------------------------- |
| `list_tables`                 | Spark PDS contract                        |
| `get_table_schema`            | Batch + streaming readers                 |
| `read_table_metadata`         | Offsets + checkpointing                   |
| `read_table`                  | UC connection → options                   |
| `read_table_deletes` *(opt.)* | SDP orchestration, packaging, deployment  |

---

## The `LakeflowConnect` Interface

```python
class LakeflowConnect(ABC):
    def list_tables(self) -> list[str]: ...
    def get_table_schema(self, table, opts) -> StructType: ...
    def read_table_metadata(self, table, opts) -> dict: ...
    def read_table(self, table, offset, opts
                   ) -> tuple[Iterator[dict], dict]: ...

    # Optional — only for cdc_with_deletes
    def read_table_deletes(self, ...): ...
```

> No Spark imports. No streaming protocol. No checkpoint logic.

---

## Ingestion Types Out of the Box

| Type                 | Framework does                     | Author provides             |
| -------------------- | ---------------------------------- | --------------------------- |
| `snapshot`           | Full refresh                       | Return all rows             |
| `cdc`                | Merge on PK, advance cursor        | Records sorted by cursor    |
| `cdc_with_deletes`   | + apply tombstones                 | + `read_table_deletes`      |
| `append`             | Insert-only, strict offsets        | Small server-side page size |

> Pick a type per table. The framework handles the rest.

---

## Batch Boundaries, For Free

```
start_offset  ──►  read_table  ──►  (iter, end_offset)
                                         │
                         end_offset == start_offset ?
                             │               │
                            yes             no
                             │               │
                            stop    call again with end_offset
```

- Pagination · termination · checkpointing → **framework-driven**
- Author's job: advance the cursor honestly

---

## Scaling Up: Partitioned Reads

```python
class MyConnect(LakeflowConnect, SupportsPartitionedStream):
    def get_partitions(self, table, start, end): ...
    def latest_offset(self, table, opts): ...
```

- Time-window partitions run in parallel across executors
- One mixin + two methods — same interface
- For high-volume sources where sequential is too slow

---

## Reference Implementation

`sources/example/example.py` — every pattern in one place:

- Snapshot · CDC · CDC + deletes · append
- Three incremental strategies (count, server-limit, sliding window)
- Retry with exponential backoff
- Cursor capping to prevent "chasing new data"

> New connectors are written by following this example.

---

## Part 2 — The UI Experience

---

## Demo: The UI in Action

> *Live walkthrough — from the Community Connectors gallery to a running pipeline.*

`+ New` → `Add or upload data` → **Community connectors**

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

## Auto-Sync: Community Repo → UI

The UI doesn't bundle any connector-specific code. Connector metadata flows from the community repo into the universe repo (where the UI lives) through an automated sync.

```
  lakeflow-community-connectors                     universe (UI)
  ──────────────────────────────                    ─────────────
  sources/<connector>/
    ├─ connector_spec.yaml    ──┐
    ├─ <connector>.svg        ──┤   auto-sync job   ┌─ spec registry
    ├─ README.md              ──┼────────────────►  ├─ logos
    └─ ...                      │                   └─ docs
                                │
                                ▼
                         UI renders tiles + forms
```

- **One source of truth** — edit the spec / logo in the community repo, the UI picks it up
- **Adding a connector = one PR in this repo** — no UI code review, no front-end release
- **The spec is the contract** between the framework and the UI
- Same path covers logos, descriptions, and parameter forms

For custom connectors (next slide), the same contract is satisfied without the sync — the user's repo is read directly.

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

## Part 3 — The AI Agent Plugin

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

Long agentic runs *used to be* scary — today's agents are much more reliable, but humans still want visibility into multi-step work. This workflow stays controllable:

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

## Part 4 — The Road Ahead

Current vs Future (in progress), one topic at a time.

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

4. **No more SDP-editor limits** — the SDP editor doesn't support multi-file imports for Python data sources, which has been a persistent pain. Wheel-packaged connectors sidestep this entirely.

---

## Why Now: The Original Bet, Reconsidered

We chose the repo-based path originally for two reasons. Both reasons have since weakened.

| Original concern                                             | Why it held then                                               | Why it no longer blocks us                                     |
| ------------------------------------------------------------ | -------------------------------------------------------------- | -------------------------------------------------------------- |
| Let customers **customize the Python code** (Genie-style)    | Direct repo access was the easy path for user edits             | Wheel packages can still be built from a user repo; edits flow through the agent workflow, not in-place notebook hacks |
| **Maintenance burden** of "managed"-labeled connectors at scale | Hand-maintaining 100+ connectors under a managed SLA was risky  | **LLM agents** now do the mechanical maintenance — research, implement, test, fix — so scale is no longer a headcount problem |

**Net:** the constraints that justified the split have dissolved — and the repo-based workspace pipeline was a sub-optimal experience to begin with (users managing Git repos just to run an ingestion pipeline). Unifying is now the right call.

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
- **Private by default** — many users only want their connector available inside their own workspace, not shared with every Databricks customer. Publishing locally is the end state, not a staging step.
- **Lower bar for contribution** — those who do want to contribute can publish locally first and promote to the main repo later if useful

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

**Goal:** a self-evolving repo with minimal human maintenance.

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

## Support Agents: The Self-Evolving Repo

Beyond `/create-connector`, a portfolio of specialized agents keeps the codebase honest:

- **Eval agent** — scores connector output quality against a benchmark; regression-detects after template changes
- **Review agent** — does first-pass PR review on connector changes before humans see them
- **API-sync agent** — periodically crawls each source's API docs; opens PRs when endpoints, schemas, or auth flows drift
- **Debug agent** — given a failed test run, isolates the root cause and proposes a fix PR

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
