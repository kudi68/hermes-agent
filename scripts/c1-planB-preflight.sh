#!/usr/bin/env bash
# Read-only candidate checks. Never touches launchd or a live profile.
set -euo pipefail

runtime_root=${1:-${C1_CANDIDATE_REPO:-}}
agent_ops_repo=${2:-${C1_AGENT_OPS_REPO:-}}
candidate_sha=${3:-${C1_CANDIDATE_SHA:-}}
agent_ops_sha=${4:-${C1_AGENT_OPS_SHA:-}}
python_bin=${C1_HERMES_PYTHON:-/Users/kudi68-15mba/Projects/c1-runtime/hermes-agent-c1-fe6ab90/.venv/runtime/bin/python}

if [ -z "$runtime_root" ] || [ -z "$agent_ops_repo" ]; then
  printf 'Hermes and agent-ops candidate checkout paths are required.\n' >&2
  exit 2
fi
if [[ ! "$candidate_sha" =~ ^[0-9a-f]{40}$ ]] || [[ ! "$agent_ops_sha" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'Exact 40-character Hermes and agent-ops candidate SHAs are required.\n' >&2
  exit 2
fi

ls -ld "$runtime_root" "$agent_ops_repo" "$python_bin"
test "$(git -C "$runtime_root" rev-parse --verify 'HEAD^{commit}')" = "$candidate_sha"
test "$(git -C "$agent_ops_repo" rev-parse --verify 'HEAD^{commit}')" = "$agent_ops_sha"
for candidate_repo in "$runtime_root" "$agent_ops_repo"; do
  git -C "$candidate_repo" diff --quiet
  git -C "$candidate_repo" diff --cached --quiet
  test -z "$(git -C "$candidate_repo" ls-files --others --exclude-standard)"
done

(
  cd "$agent_ops_repo"
  export PYTHONDONTWRITEBYTECODE=1
  export PYTHONPATH="$agent_ops_repo"
  "$python_bin" -c '
import sys
from tools.agent_ops_c1.operator_ops import _gateway_pin_ok
raise SystemExit(0 if _gateway_pin_ok(sys.argv[1]) else 1)
' "$runtime_root"
  C1_HERMES_REPO="$runtime_root" "$python_bin" \
    -m tools.agent_ops_c1.operator_ops preflight --candidate
)
