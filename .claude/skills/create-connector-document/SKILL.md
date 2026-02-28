---
name: create-connector-document
description: Generate public-facing documentation for a connector targeted at end users.
disable-model-invocation: true
---

# Create Public Connector Documentation

## Goal
Generate the **public-facing documentation** for the **{{source_name}}** connector, targeted at end users.

## Output Contract
Produce a Markdown file strictly following the standard template at `prompts/templates/community_connector_doc_template.md` as `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`.

## Documentation Requirements

- Please use the code implementation as the source of truth.
- Use the source API documentation to cover anything missing.
- Always include a section about how to configure the parameters needed to connect to the source system.
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.

## Git Commit on Completion

After writing the public documentation, commit it to git before returning:

```bash
git add src/databricks/labs/community_connector/sources/{source_name}/README.md
git commit -m "Add {source_name} connector documentation"
```

Use the exact source name in the commit message. Do not push — only commit locally.
