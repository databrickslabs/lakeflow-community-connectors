# Develop Connector via Prompts



## Templates
- [Source API Document Template](templates/source_api_doc_template.md): Used in Step 1 to document the researched API endpoints.
- [Community Connector Documentation Template](templates/community_connector_doc_template.md): Used to create consistent, public-facing documentation across different connectors.

## Quality Review & Validation
This skill is useful for both **development** (testing during implementation) and **quality review** (validating existing connectors):

- **Validate Incremental Sync** ([`.claude/skills/validate-incremental-sync/SKILL.md`](../.claude/skills/validate-incremental-sync/SKILL.md)): Manual validation process to verify CDC implementation by checking offset structure, validating offset matches max cursor, and testing incremental filtering.

## Notes & Tips
- The **context window** grows larger as you proceed through each step. Consider starting a new session for each step to maintain explicit context isolation—this way, each step only references the output from previous ones, conserving context space.
- Each step is wrapped as a SKILL for Claude, located under `.claude/skills/`.
