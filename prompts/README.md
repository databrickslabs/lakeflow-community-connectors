# Connector Vibe-Coding Prompts

## Connector Development Workflow 
These steps build a working read-only connector:
- **Step 1: Document Source API** - Research and document READ operations only.
  - See details in [understand_and_document_source.md](understand_and_document_source.md)
- **Step 2: Implement Connector** - Implement the LakeflowConnect interface
  - See details in [implement_connector.md](implement_connector.md)
- **Step 3: Test and Fix Connector** - Validate read operations work correctly
  - See details in [test_and_fix_connector.md](test_and_fix_connector.md)

At this point, you have a **production-ready connector** that can ingest data.

### Write-Back Testing (Optional)
If you need comprehensive end-to-end validation:
- **Step 4: Document Write-Back APIs** - Research and document WRITE operations (separate from Step 1)
  - See details in [document_write_back_api_of_source.md](document_write_back_api_of_source.md)
- **Step 5: Implement Write-Back Testing** - Create test utilities that write data and validate ingestion
  - See details in [implement_write_back_testing.md](implement_write_back_testing.md)

**Skip Steps 4-5 if:**
- You want to ship quickly (read-only testing is sufficient)
- Source is read-only
- Only production access available
- Write operations are expensive/risky

### Final Step (Required)
- **Step 6: Create Public Documentation** - Generate user-facing README
  - See details in [create_connector_documentation.md](create_connector_documentation.md)
- **Step 7: Create connector spec YAML file**
  - See details in [generate_connector_spec_yaml.md](generate_connector_spec_yaml.md)
- **(Temporary) Step 8: Run merge scripts**
  - As a temporary workaround for current compatibility issues with Python Data Source and SDP, please run `tools/scripts/merge_python_source.py` on your newly developed source. This will combine the source implementation into a single file.

## Templates
- [Source API Document Template](templates/source_api_doc_template.md): Used in Step 1 to document the researched API endpoints.
- [Community Connector Documentation Template](templates/community_connector_doc_template.md): Used to create consistent, public-facing documentation across different connectors.

## Quality Review & Validation
This prompt is useful for both **development** (testing during implementation) and **quality review** (validating existing connectors):

- **[Validate Incremental Sync](validate_incremental_sync.md)**: Manual validation process to verify CDC implementation by checking offset structure, validating offset matches max cursor, and testing incremental filtering. See the **validate-incremental-sync** Claude skill.

## Notes & Tips
- The **context window** grows larger as you proceed through each step. Consider starting a new session for each step to maintain explicit context isolation—this way, each step only references the output from previous ones, conserving context space.
- Each step is wrapped as a SKILL for Claude, which can be triggered automatically by Claude Code.
