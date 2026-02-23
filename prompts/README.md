# Connector Vibe-Coding Prompts

All development prompts have been migrated to Claude skills under `.claude/skills/`.

## Connector Development Workflow 
These steps build a working read-only connector:
- **Step 1: Document Source API** - Research and document READ operations only.
  - See skill: [`.claude/skills/research-source-api/SKILL.md`](../.claude/skills/research-source-api/SKILL.md)
- **Step 2: Auth Setup** - Generate the connection spec, guide credential entry via the authenticate script, and verify connectivity.
  - See skill: [`.claude/skills/connector-auth-guide/SKILL.md`](../.claude/skills/connector-auth-guide/SKILL.md)
- **Step 3: Implement Connector** - Implement the LakeflowConnect interface
  - See skill: [`.claude/skills/implement-connector/SKILL.md`](../.claude/skills/implement-connector/SKILL.md)
- **Step 4: Test and Fix Connector** - Validate read operations work correctly
  - See skill: [`.claude/skills/test-and-fix-connector/SKILL.md`](../.claude/skills/test-and-fix-connector/SKILL.md)

At this point, you have a **production-ready connector** that can ingest data.

### Write-Back Testing (Optional)
If you need comprehensive end-to-end validation:
- **Step 5: Document Write-Back APIs** - Research and document WRITE operations (separate from Step 1)
  - See skill: [`.claude/skills/research-write-api-of-source/SKILL.md`](../.claude/skills/research-write-api-of-source/SKILL.md)
- **Step 6: Implement Write-Back Testing** - Create test utilities that write data and validate ingestion
  - See skill: [`.claude/skills/write-back-testing/SKILL.md`](../.claude/skills/write-back-testing/SKILL.md)

**Skip Steps 5-6 if:**
- You want to ship quickly (read-only testing is sufficient)
- Source is read-only
- Only production access available
- Write operations are expensive/risky

### Final Step (Required)
- **Step 7: Create Public Documentation** - Generate user-facing README
  - See skill: [`.claude/skills/create-connector-document/SKILL.md`](../.claude/skills/create-connector-document/SKILL.md)
- **Step 8: Create connector spec YAML file**
  - See skill: [`.claude/skills/generate-connector-spec/SKILL.md`](../.claude/skills/generate-connector-spec/SKILL.md)
- **(Temporary) Step 9: Run merge scripts**
  - As a temporary workaround for current compatibility issues with Python Data Source and SDP, please run `tools/scripts/merge_python_source.py` on your newly developed source. This will combine the source implementation into a single file.

## Templates
- [Source API Document Template](templates/source_api_doc_template.md): Used in Step 1 to document the researched API endpoints.
- [Community Connector Documentation Template](templates/community_connector_doc_template.md): Used to create consistent, public-facing documentation across different connectors.

## Quality Review & Validation
This skill is useful for both **development** (testing during implementation) and **quality review** (validating existing connectors):

- **Validate Incremental Sync** ([`.claude/skills/validate-incremental-sync/SKILL.md`](../.claude/skills/validate-incremental-sync/SKILL.md)): Manual validation process to verify CDC implementation by checking offset structure, validating offset matches max cursor, and testing incremental filtering.

## Automated End-to-End Agent

In addition to manually triggering individual skills step-by-step, the entire development workflow is wrapped as a **create-connector** Claude agent (`.claude/agents/`) that orchestrates all the steps above autonomously. Developers can invoke this agent directly and let it execute the full pipeline — from API research through authentication, implementation, testing, documentation, and packaging — without manual intervention at each stage.

## Notes & Tips
- The **context window** grows larger as you proceed through each step. Consider starting a new session for each step to maintain explicit context isolation—this way, each step only references the output from previous ones, conserving context space.
- Each step is wrapped as a SKILL for Claude, located under `.claude/skills/`.
