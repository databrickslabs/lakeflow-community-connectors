---
name: create-connector-document
description: Generate public-facing documentation for a connector targeted at end users.
disable-model-invocation: true
---

# Create Public Connector Documentation

## Goal
Generate the **public-facing documentation** for the **{{source_name}}** connector, targeted at end users.

## Output Contract
Produce a Markdown file strictly following the standard template at `templates/community_connector_doc_template.md` as `src/databricks/labs/community_connector/sources/{{source_name}}/README.md`.

## Documentation Requirements

- Please use the code implementation as the source of truth.
- Use the source API documentation to cover anything missing.
- Always include a section about how to configure the parameters needed to connect to the source system.
- If the connector authenticates via an OAuth flow (the connector spec has a `connection.oauth` block), the OAuth-app setup section MUST document the **Authorized redirect URIs** the user has to register with their identity provider, covering both ways a connection can be created:
  - **Databricks UI flow** (creating the connection from the "Add Data" page): the provider redirects back to Databricks, so the user must add `https://<your-workspace-url>/login/oauth/http.html` (substituting their own workspace URL) to the OAuth app's Authorized redirect URIs. Omitting this causes a `redirect_uri_mismatch` error in the consent popup.
  - **`community-connector` CLI flow**: the CLI listens on a local loopback, so registering the loopback host (e.g. `http://localhost`, which covers the `http://127.0.0.1:<port>/callback` the CLI picks at runtime) is sufficient — no workspace URL is involved.
- AVOID mentioning internal implementation terms such as function or argument names from the `LakeflowConnect`.