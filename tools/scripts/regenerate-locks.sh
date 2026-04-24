#!/bin/sh
# Regenerate pinned dependency files used by CI.
#
# Resolves each CI install environment into a locked requirements.txt-style
# file with exact versions for direct and transitive deps. The cutoff date
# establishes the trust horizon: uv only considers package versions
# published before that timestamp, so a post-cutoff compromise cannot be
# resolved into the lock.
#
# No hashes (per the current internal guidance). PyPI's re-upload
# prohibition for existing versions is the assumed integrity guarantee.
#
# RUN ON A LINUX HOST with PyPI access (e.g. arca). Running on macOS will
# resolve different wheels than CI uses. Running off the corp network may
# hit proxy throttling; arca avoids both issues.
#
# Requires: uv (https://docs.astral.sh/uv/).
#   Install: curl -LsSf https://astral.sh/uv/install.sh | sh

set -eu

# Trust horizon. Bump deliberately when pulling in vetted updates — each
# bump means "we accept as trusted anything PyPI had at this timestamp."
CUTOFF="2026-03-19T00:00:00Z"
PYTHON_VERSION="3.10"
REQ_DIR="requirements"

if ! command -v uv >/dev/null 2>&1; then
    printf 'error: uv not found on PATH. Install with:\n' 1>&2
    printf '  curl -LsSf https://astral.sh/uv/install.sh | sh\n' 1>&2
    exit 1
fi

root="$(git rev-parse --show-toplevel)"
cd "${root}"
mkdir -p "${REQ_DIR}"

compile() {
    out="$1"
    shift
    printf '  %s\n' "${out}"
    uv pip compile "$@" \
        --exclude-newer "${CUTOFF}" \
        --python-version "${PYTHON_VERSION}" \
        --output-file "${out}" \
        --quiet
}

printf 'Regenerating locks (cutoff: %s, python: %s)...\n' "${CUTOFF}" "${PYTHON_VERSION}"

# Root package [dev] — test-libs, test-pipeline, test-example.
compile "${REQ_DIR}/root.txt" pyproject.toml --extra dev

# tools/community_connector [dev] — test-community-connector.
compile "${REQ_DIR}/tools.txt" tools/community_connector/pyproject.toml --extra dev

# Pylint runs against root + tools, and also installs pylint itself. The
# extras file is tracked in the repo so the path baked into the generated
# header is deterministic — using `mktemp` here would put a random /tmp path
# into the lock header and cause spurious drift warnings on every regen.
compile "${REQ_DIR}/pylint.txt" \
    pyproject.toml \
    tools/community_connector/pyproject.toml \
    "${REQ_DIR}/_pylint-extras.in" \
    --extra dev

printf '\nDone. Review with: git diff %s/\n' "${REQ_DIR}"
