# Prompts

## Templates
- [Source API Document Template](templates/source_api_doc_template.md): Used by the `research-source-api` skill to document researched API endpoints.
- [Community Connector Documentation Template](templates/community_connector_doc_template.md): Used by the `create-connector-document` skill to create consistent, public-facing documentation.
- [Connector Spec Template](templates/connector_spec_template.yaml): Used by the `generate-connector-spec` skill to define connection parameters and external options.

## Development Workflow

See the main [README](../README.md) for the full development workflow, including both the autonomous agent path (`/create-connector`) and the step-by-step skill commands.

## Quality Review & Validation

- **Validate Incremental Sync** ([`.claude/skills/validate-incremental-sync/SKILL.md`](../.claude/skills/validate-incremental-sync/SKILL.md)): Manual validation process to verify CDC implementation by checking offset structure, validating offset matches max cursor, and testing incremental filtering.
