#!/usr/bin/env bash
# Set up Python 3.14 and a virtual environment for this project.
# Prerequisite: Homebrew must be installed. If not, run:
#   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# Then ensure brew is on PATH (see install output; often: eval "$(/opt/homebrew/bin/brew shellenv)").

set -e
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Prefer Homebrew (Apple Silicon then Intel)
if [[ -x /opt/homebrew/bin/brew ]]; then
  export PATH="/opt/homebrew/bin:$PATH"
elif [[ -x /usr/local/bin/brew ]]; then
  export PATH="/usr/local/bin:$PATH"
fi

if ! command -v brew &>/dev/null; then
  echo "Homebrew not found. Install it first:"
  echo '  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
  echo "Then add brew to PATH (e.g. eval \"\$(/opt/homebrew/bin/brew shellenv)\") and re-run this script."
  exit 1
fi

echo "==> Installing Python 3.14..."
brew install python@3.14

# Homebrew puts versioned Python in prefix/opt/python@3.14/bin
PY314="$(brew --prefix python@3.14)/bin/python3.14"
if [[ ! -x "$PY314" ]]; then
  echo "Could not find python3.14 at $PY314"
  exit 1
fi

echo "==> Using $PY314"
"$PY314" --version

echo "==> Removing existing .venv (if any)..."
rm -rf .venv

echo "==> Creating virtual environment with Python 3.14..."
"$PY314" -m venv .venv

echo "==> Upgrading pip and installing project with [dev]..."
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e ".[dev]"

echo ""
echo "Done. Activate the environment with:"
echo "  source .venv/bin/activate"
