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

## The Long Tail Never Makes the Roadmap

- Thousands of SaaS sources · only a handful get managed support
- Every migration hits "that one weird system"
- Managed connectors tightly coupled to the engine — slow release train
- No extensibility path outside Databricks engineering

**Result:** low coverage · slow release · high maintenance

> Anyone (Eng, SA, customer, third party) should be able to ship a connector.

---

## Our Approach

**Fill-in-the-blanks connector development.**

1. **Template** — thin interface; shared libraries absorb the boilerplate
2. **AI Agent Plugin** — skills · subagents · slash commands
3. **One command** — `/create-connector <source>` → deployed pipeline

> Built on Spark Python Data Source · Spark Declarative Pipeline · Claude Code plugins

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
1. Click a tile  ──►  source selected
2. Connection    ──►  create or select UC connection
3. Pipeline setup — name · root path · default catalog/schema
4. Pipeline spec ──►  tables · destinations · per-table params
5. Run ingestion ──►  SDP pipeline executes
```

Behind the scenes at step 3:

- Sparse-checkout clone → workspace repo
- Workspace pipeline wired to that repo
- Spec editable later; code lives in the workspace

---

## Config-Driven UI: One Connection, Many Faces

```
     What the user sees              What actually exists
     ─────────────────               ────────────────────

   ┌─────────────────────────┐
   │  New GitHub Connection  │
   │  Personal Access Token  │         ┌──────────────────────┐
   │  Organization           │  ──►    │   Generic UC         │
   │  API Base URL           │         │   Connection         │
   └─────────────────────────┘         │   (arbitrary k-v)    │
                                       │                      │
   ┌─────────────────────────┐  ──►    └──────────────────────┘
   │  New Zendesk Connection │
   │  Subdomain · Token      │
   └─────────────────────────┘
```

> `connector_spec.yaml` defines the form fields.
> Adding a connector = adding a spec file. No UI code changes.

---

## Auto-Sync: Community Repo → UI

```
  lakeflow-community-connectors                universe (UI)
  ──────────────────────────────               ─────────────
  sources/<connector>/
    ├─ connector_spec.yaml    ──┐
    ├─ <connector>.svg        ──┤  sync job   ┌─ spec registry
    ├─ README.md              ──┼───────────► ├─ logos
    └─ ...                      │             └─ docs
                                ▼
                         UI renders tiles + forms
```

- One source of truth — edit here, UI picks it up
- Adding a connector = one PR in this repo
- The spec is the contract between framework and UI
- Logos · descriptions · form fields all flow through

---

## Custom Connectors: Bring Your Own Repo

```
+ Add Community Connector
          │
          ▼
    Paste git repo URL
          │
          ▼
Sparse-checkout → read connector_spec.yaml
          │
          ▼
 Render form → create pipeline → ingest
```

Typical path:

1. Fork / clone `lakeflow-community-connectors`
2. `/create-connector <my_source>` — agent writes code + tests + spec
3. Push to your repo
4. Deploy via **"+ Add Community Connector"**

> Any repo on GitHub → a deployable tile in minutes.

---

## Two Other Deployment Paths

- **CLI** — `community-connector create_pipeline ...` — scriptable
- **Agent** — `/deploy-connector` — interactive

> All three paths → same UC connection, workspace repo, SDP pipeline.

---

## Part 3 — The AI Agent Plugin

---

## Why AI-Assist Belongs Here

Even with the template, a human still has to:

- Read hundreds of pages of API docs
- Map endpoints to tables
- Pick ingestion types per table
- Debug against a live source
- Generate spec · docs · packaging

> Not hard — just slow and mechanical.
> A perfect target for an agent.

---

## Plugin Anatomy

```
.claude/
├── commands/          # user-facing entry points
│   └── create-connector.md
├── agents/            # specialized subagents
│   ├── source-api-researcher.md
│   ├── connector-dev.md
│   ├── connector-tester.md
│   ├── connector-doc-writer.md
│   └── ...
└── skills/            # reusable step instructions
    ├── research-source-api/
    ├── implement-connector/
    ├── test-and-fix-connector/
    └── ...
```

> Commands orchestrate · Subagents specialize · Skills instruct
> Auto-discovered by Claude Code + Cursor. No vendor lock-in.

---

## The Three Building Blocks

**Skill** — reusable instruction doc (`SKILL.md`) for one step. Stateless.

**Subagent** — specialized Claude instance with its own model + skills. Isolated context.

**Slash command** — user-facing orchestrator. `/create-connector` runs the full 6-step pipeline.

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

> Human gate between steps. Each step commits its own artifacts.

---

## Human-in-the-Loop by Design

Agents are reliable now — but humans still want visibility.

- **Confirmation gate** after each step — summary + Continue / Review
- **Per-step git commits** — clean audit trail
- **Localized failures** — a bad step doesn't poison the rest
- **Trivial rollback** — `git reset` to any prior gate
- **Resumable** — pick up mid-workflow

> The agent does the toil. The human keeps the steering wheel.

---

## Why Subagents (Not One Big Prompt)

| Concern                  | Single-prompt approach    | Subagent approach             |
| ------------------------ | ------------------------- | ----------------------------- |
| Context window           | Bloats with every step    | Each agent starts fresh       |
| Tool scope               | All tools always on       | Least-privilege per agent     |
| Model choice             | One model for everything  | `sonnet` for research, `opus` for code |
| Failure isolation        | One bad step poisons all  | Failures are local            |
| Prompt maintainability   | One huge prompt           | Small focused prompts         |

> Subagents = parallelism · isolation · specialization

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
- Two skills loaded
- Picks standard vs. partitioned based on the API

---

## Progressive Disclosure of Context

Subagents load only the skills they need:

```yaml
skills:
  - implement-connector
  - implement-partitioned-connector
```

- Researcher → research skills only
- Dev → implementation skills only
- Tester → testing skills only

> Context stays lean as the plugin grows.
> Adding the 20th skill doesn't slow anyone down.

---

## What a Skill Contains

`implement-connector/SKILL.md` — a dense playbook for one step:

- File organization rules
- Interface compliance checklist
- Schema conventions (`StructType` > `MapType`; `LongType` > `IntegerType`)
- Incremental read strategies (sliding window, server-side limit)
- Batch boundaries for CDC vs. append-only
- Termination guarantees
- API usage rules (list vs. get, parent-child traversal)

> Institutional knowledge — inherited by every new connector.

---

## Skills = The Other Half of the Framework

`src/` = the **runtime**. `.claude/skills/` = the **wisdom**.

Each skill encodes a lesson learned the hard way:

- Sort ascending by cursor — or infinite loop on resume
- Append-only can't client-truncate — duplicates or data loss
- Cap cursors at init — don't chase new data mid-run
- Prefer `StructType` + `LongType` — avoid type surprises

> Deleting the skills would hurt more than deleting half the code.

---

## Skill in Action: `/deploy-connector`

The skill reads the connector's own README + `connector_spec.yaml`,
then guides the user through pipeline setup as a conversation:

```
User: /deploy-connector github

Agent: (reads github/README.md + connector_spec.yaml)
       Which tables do you want to ingest?
       Available: issues, pull_requests, releases, ...

User:  issues and pull_requests

Agent: issues uses `cdc_with_deletes` — I'll set it up with
       primary key `id` and cursor `updated_at`.
       pull_requests needs an `owner` and `repo` — what values?

User:  databricks / runtime

Agent: Destination catalog + schema?
       ...
       Ready to run `community-connector create_pipeline`?
```

- LLM + connector docs → an expert co-pilot for each source
- No reading docs, no hand-editing YAML, no guessing options
- Setup feels like a conversation, not a form

> Smooth, not mechanical.

---

## This Pattern Generalizes

| Layer             | Connectors                     | Any domain                   |
| ----------------- | ------------------------------ | ---------------------------- |
| Narrow interface  | `LakeflowConnect`              | Your abstract class          |
| Reference impl    | `example/example.py`           | A canonical example          |
| Shared libs       | `sparkpds/`, `pipeline/`       | Your runtime                 |
| Skills            | Research · implement · test    | Your workflow steps          |
| Subagents         | Specialized roles              | Specialized roles            |
| Slash command     | `/create-connector`            | `/create-<your-thing>`       |

> Any repetitive task with a narrow surface → a one-command plugin.

---

## Live Tests, Not Codegen

Most "AI writes your X" demos ship plausible-looking code.
Step 4 runs against the **real source API**:

```
connector-tester
   ├─ generates test file
   ├─ runs pytest against live creds
   ├─ reads failures
   ├─ patches the connector
   └─ loops until green
```

- No mocks · no synthetic fixtures
- Unexpected API behavior → caught now
- Output is **verified**, not just generated

> This is what separates agentic work from fancy autocomplete.

---

## Write-Back Testing: First-Class

Most frameworks skip this. We make it a dedicated step.

```
/research-write-api-of-source  →  docs POST/PATCH/DELETE endpoints
/write-back-testing            →  generates test utilities
```

Round trip per test:

1. **Write** a record into the source
2. **Read** via the connector — verify it appears
3. **Update** — verify incremental sync catches it
4. **Delete** — verify `cdc_with_deletes` tombstones it

> End-to-end proof the connector models the source lifecycle.

---

## What This Unlocks

- **Days → hours** — time to first connector
- **Long-tail coverage** — community ships without deep Spark expertise
- **Consistency** — same patterns, docs, tests every time
- **Live-tested** — real source APIs; write-back optional
- **Two deploy paths** — Databricks UI or CLI

---

## Part 4 — The Road Ahead

Current vs Future (in progress), one topic at a time.

---

## #1 — Architecture: Community → Managed Ingestion

```
 ┌────────────────────────────────┐      ┌────────────────────────────────┐
 │  Today                         │      │  Future                        │
 ├────────────────────────────────┤      ├────────────────────────────────┤
 │ Workspace repo                 │      │ Wheel package                  │
 │  (sparse checkout)             │  ──► │                                │
 │                                │      │ Managed ingestion pipeline     │
 │ SDP pipeline imports the repo  │      │  runs register() in init       │
 │  + runs register() at init     │      │                                │
 │                                │      │ Same contract · unified CUJ    │
 │ Community-only CUJ             │      │                                │
 └────────────────────────────────┘      └────────────────────────────────┘
```

> **Same:** `LakeflowConnect` · agents · skills · `connector_spec.yaml`
> **Different:** packaging + pipeline substrate

---

## What the Convergence Buys Us

1. **Unified architecture** — one substrate, one mental model
2. **Advanced features free** — schema evolution · column selection · etc.
3. **Source-exploration UI** — managed flow has it; community inherits it
4. **No more SDP-editor limits** — wheel packages sidestep multi-file import pain

---

## Why Now: The Original Bet, Reconsidered

We split for two reasons. Both have weakened.

| Original concern                        | Why it held                              | Why it no longer blocks                                                  |
| --------------------------------------- | ---------------------------------------- | ------------------------------------------------------------------------ |
| Customer-customizable Python code       | Direct repo access was the easy path     | Wheels can be built from user repos; edits flow through agent workflows  |
| Maintenance burden at scale             | 100+ connectors under managed SLA = risk | **LLM agents** now do the mechanical maintenance at scale                |

> And the repo-based pipeline was sub-optimal to begin with —
> users shouldn't have to manage Git repos just to run ingestion.

---

## #2 — Customization: Primitive → First-Class

Today: prebuilt = polished · custom = primitive.
Tomorrow: same UI for both · custom becomes publishable.

```
 ┌────────────────────────────────┐      ┌────────────────────────────────┐
 │  Today                         │      │  Future                        │
 ├────────────────────────────────┤      ├────────────────────────────────┤
 │ Prebuilt                       │      │ Prebuilt: unchanged            │
 │  spec synced → universe        │  ──► │                                │
 │  → rich source-specific form   │      │ Custom: same UI as prebuilt    │
 │                                │      │                                │
 │ Custom                         │      │ "Publish" → Workspace Asset    │
 │  primitive, generic fields     │      │                                │
 │  no publish path               │      │ Workspace-scoped connector     │
 └────────────────────────────────┘      └────────────────────────────────┘
```

> Workspace Asset holds custom metadata.
> One rendering path · two spec sources.

---

## What Workspace Assets Unlock

- **Team sharing** — one publish, whole workspace sees the tile
- **Governance** — first-class object (permissions · audit · lifecycle)
- **Close the first-party/custom gap** — UX no longer depends on repo location
- **Private by default** — many users want it workspace-only, not public
- **Lower contribution bar** — publish locally, promote upstream later

> AI plugin ships the code · Workspace Asset ships the product experience.

---

## #3 — Agents: One-Shot → Self-Evolving

```
 ┌────────────────────────────────┐      ┌────────────────────────────────┐
 │  Today                         │      │  Future                        │
 ├────────────────────────────────┤      ├────────────────────────────────┤
 │ One flow (customer-first)      │      │ Two flows:                     │
 │                                │  ──► │   Customer · Developer         │
 │ One-shot /create-connector     │      │                                │
 │                                │      │ Live + agent-seeded mocks      │
 │ Tests only vs. live source     │      │  → real CI/CD                  │
 │                                │      │                                │
 │ Humans maintain the repo       │      │ Support agents:                │
 │                                │      │  eval · review · API-sync ·    │
 │                                │      │  debug                         │
 │                                │      │                                │
 │                                │      │ Template + agents co-evolve    │
 └────────────────────────────────┘      └────────────────────────────────┘
```

> Goal: a self-evolving repo with minimal human maintenance.

---

## Testing: Real + Agent-Seeded Mocks

Today — **live source only:**
- ✅ Real API + real data → high confidence
- ❌ No CI/CD · narrow coverage · no edge-case or failure-mode tests

Tomorrow — **combined:**

```
live source ──► agent captures traffic ──► seeds shared mocks
                                                    │
                         ┌──────────────────────────┴─────┐
                         ▼                                ▼
             agent expands (edge cases,        extended CI matrix,
             failure modes, more data)         every PR
```

> Live = ground truth · Mocks = breadth + CI safety.

---

## Two Agent Flows: Customer vs Developer

| Flow         | Customer (today)               | Developer (new)                       |
| ------------ | ------------------------------ | ------------------------------------- |
| Goal         | Deploy a working pipeline      | Evolve a connector or the framework   |
| Start point  | UC connection first            | Repo / branch first                   |
| Iteration    | Linear — 6 steps, gates, done  | Iterative — test · debug · refactor   |
| Test posture | Live source                    | Mocks primary · live for final pass   |
| Output       | Deployed SDP pipeline          | PR with code + tests + docs           |
| Audience     | Data engineers                 | Connector maintainers + contributors  |

> Both share skills + subagents. They differ in orchestration and defaults.

---

## Support Agents: The Self-Evolving Repo

Beyond `/create-connector`:

- **Eval** — scores output vs. benchmark; catches regressions
- **Review** — first-pass PR review before humans
- **API-sync** — crawls source docs; opens drift PRs
- **Debug** — failed run → root cause → fix PR

**Template ↔ agent co-evolution:**
new pattern → same PR updates template *and* affected skills/subagents.

> The repo becomes a system that maintains itself.

---

## Summary

- Thin interface (`LakeflowConnect`) hides Spark PDS + SDP plumbing
- Reference implementation teaches the patterns
- Shared libraries absorb pagination · offsets · streaming
- Skills + subagents + slash commands → `/create-connector`
- Works in Claude Code + Cursor

> Narrow interface + shared runtime + AI plugin
> = fill-in-the-blanks connectors

---

## Q & A

Repo: [databrickslabs/lakeflow-community-connectors](https://github.com/databrickslabs/lakeflow-community-connectors)

Key paths:
- Interface — `src/databricks/labs/community_connector/interface/`
- Reference — `sources/example/example.py`
- Plugin — `.claude/{commands,agents,skills}/`
- CLI — `tools/community_connector/`
