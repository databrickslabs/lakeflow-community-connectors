We happily welcome contributions to Lakeflow-Community-Connectors. We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## CI on pull requests from forks

CI for this repo runs on hardened runners that exchange a per-job OIDC token for a JFrog Artifactory access token. To make this work for fork PRs, the workflows are triggered with `pull_request_target` and explicitly check out the PR's head commit.

Workflow runs from outside collaborators are held pending until a maintainer approves them ("Require approval for outside collaborators" is enabled in the repo settings). Maintainers should review the diff — including any change under `requirements/`, `pyproject.toml`, or workflow files — before approving, since approved runs execute the PR's code with secrets available.

Internal contributors pushing branches inside `databrickslabs/lakeflow-community-connectors` are not affected — their PRs run CI automatically on every push.
