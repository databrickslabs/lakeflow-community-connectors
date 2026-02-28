---
name: research-source-api 
description: "Single step only: research and document a source API when explicitly asked. Do NOT use for full connector creation — use the create-connector agent instead."
disable-model-invocation: true
---

# Research Source API

## Description

This skill helps you create comprehensive API documentation for a new data source connector. It guides you through researching the source API, documenting endpoints, authentication, schemas, pagination, and incremental sync strategies.

## Goal 

Create a comprehensive API documentation file that enables connector implementation. The document must be **complete, source-cited, and implementation-ready**—covering endpoints, authentication, schemas, pagination, and incremental sync strategy.

## Output

Create `src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md` following the template at [source_api_doc_template](templates/source_api_doc_template.md).

## Core Principles

1. **No invention** — Every claim must be backed by research. Mark gaps with `TBD:` and rationale.
2. **Single auth method** — If multiple exist, choose one preferred method; note alternatives in Known Quirks.
3. **Complete schemas** — List all fields. No hidden fields behind links.
4. **Read operations only** — Document READ endpoints.
5. **Prefer stable APIs** — Use latest stable version. Avoid deprecated endpoints.

## Research Steps 

### Step 1: Gather Sources

- **Never generate from memory** — Always research first
- **Start with one table** — Get it working before adding more
- Research in this priority order:

| Priority | Source Type | Confidence |
|----------|-------------|------------|
| 1 | User-provided documentation | Highest |
| 2 | Official API documentation | High |
| 3 | Airbyte OSS implementation | Medium |
| 4 | Other reference implementations (Singer, dltHub) | Medium |
| 5 | Reputable technical blogs | Low |

### Step 2: Cross-Reference

Verify every endpoint and schema against **at least two sources**.

### Step 3: Resolve Conflicts

When sources disagree:
- **Official docs** > **Actively maintained OSS** > **Other**
- If unresolved: use the safer interpretation, mark `TBD:`, add note in Known Quirks

### Step 4: Document Requirements
- Fill out every section of the documentation template. If any section cannot be completed, add a note to explain.
- Focus on READ operations: endpoints, authentication parameters, object schemas, Python API for reading data, and incremental read strategy.
- Document both list APIs and get APIs.

Record all research in the Research Log table:

```markdown
## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|-------------|-----|----------------|------------|-------------------|
| Official Docs | https://... | 2025-01-15 | High | Auth params, rate limits |
| Airbyte | https://... | 2025-01-15 | High | Cursor field, pagination |
```

## Acceptance Checklist

Before completion, verify:

- [ ] All template sections present and complete
- [ ] Every schema field listed (no omissions)
- [ ] One authentication method documented with actionable steps
- [ ] Endpoints include params, examples, and pagination
- [ ] Incremental strategy defines cursor, order, lookback, delete handling
- [ ] Research Log completed with full URLs
- [ ] No unverifiable claims; gaps marked `TBD:` with rationale

## Git Commit on Completion

After writing (or updating) the API doc, commit it to git before returning:

```bash
git add src/databricks/labs/community_connector/sources/{source_name}/{source_name}_api_doc.md
git commit -m "Add {source_name} API doc"
```

Use the exact source name in the commit message. Do not push — only commit locally.
