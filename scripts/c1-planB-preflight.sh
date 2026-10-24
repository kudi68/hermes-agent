#!/usr/bin/env bash
# Read-only source/runtime checks. Requires an explicit chief path for identity;
# --candidate never resolves or reads that profile, its credentials, or its DB.
# No launchd calls. Non-candidate operator preflight needs separate live authorization.
set -euo pipefail

runtime_root=${1:-${C1_CANDIDATE_REPO:-}}
agent_ops_repo=${2:-${C1_AGENT_OPS_REPO:-}}
candidate_sha=${3:-${C1_CANDIDATE_SHA:-}}
agent_ops_sha=${4:-${C1_AGENT_OPS_SHA:-}}
python_bin=${C1_HERMES_PYTHON:-/Users/kudi68-15mba/Projects/c1-runtime/hermes-agent-c1-fe6ab90/.venv/runtime/bin/python}
chief_profile=${5:-}
hermes_runtime=${C1_HERMES_RUNTIME_REPO:-/Users/kudi68-15mba/Projects/c1-runtime/hermes-agent-c1-fe6ab90}
ops_runtime=${C1_AGENT_OPS_RUNTIME_REPO:-/Users/kudi68-15mba/Projects/c1-runtime/agent-ops-c1-main-de5ba}
hermes_runtime_head=${C1_HERMES_RUNTIME_HEAD:-b6e7d10e8b04fd10e7a1d72563a40c343a11181e}
ops_runtime_head=${C1_AGENT_OPS_RUNTIME_HEAD:-d5f16ed0f7d7839280590545d44d3e6183d20773}
export GIT_OPTIONAL_LOCKS=0

if [ -z "$runtime_root" ] || [ -z "$agent_ops_repo" ]; then
  printf 'Hermes and agent-ops candidate checkout paths are required.\n' >&2
  exit 2
fi
if [[ ! "$candidate_sha" =~ ^[0-9a-f]{40}$ ]] || [[ ! "$agent_ops_sha" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'Exact 40-character Hermes and agent-ops candidate SHAs are required.\n' >&2
  exit 2
fi
if [[ "$chief_profile" != /*/profiles/chief ]] || [[ "$chief_profile/" == *"/../"* ]]; then
  printf 'An explicit absolute profiles/chief path is required as argument 5.\n' >&2
  exit 2
fi
if [[ ! "$hermes_runtime_head" =~ ^[0-9a-f]{40}$ ]] || [[ ! "$ops_runtime_head" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'Exact expected runtime HEADs are required.\n' >&2
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

check_runtime() {
  local repo=$1 expected_head=$2 target=$3
  local actual_head target_head state ignored
  actual_head=$(git -C "$repo" rev-parse --verify 'HEAD^{commit}')
  target_head=$(git -C "$repo" rev-parse --verify "$target^{commit}")
  if [ "$actual_head" != "$expected_head" ]; then
    printf 'FAIL runtime %s HEAD=%s expected=%s\n' "$repo" "$actual_head" "$expected_head" >&2
    exit 1
  fi
  test "$target_head" = "$target"
  state=$(git -C "$repo" -c core.fsmonitor=false status --porcelain=v1 --untracked-files=all)
  if [ -n "$state" ]; then
    printf 'FAIL runtime %s is dirty:\n%s\n' "$repo" "$state" >&2
    exit 1
  fi
  # Ignored code can also shadow imports. Bytecode caches do not block switching.
  # Not `status --ignored`: with exclude pathspecs it lists ignored root dirs (.venv/).
  ignored=$(git -C "$repo" ls-files --others --ignored --exclude-standard -- \
    gateway hermes_cli hermes_constants.py plugins/c1_telegram_personal plugins/platforms/telegram tools \
    ':(exclude,glob)**/__pycache__' ':(exclude,glob)**/__pycache__/**')
  if [ -n "$ignored" ]; then
    printf 'FAIL runtime %s has ignored protected files:\n%s\n' "$repo" "$ignored" >&2
    exit 1
  fi
  printf 'PASS runtime %s HEAD=%s target=%s\n' "$repo" "$actual_head" "$target_head"
}
check_runtime "$hermes_runtime" "$hermes_runtime_head" "$candidate_sha"
check_runtime "$ops_runtime" "$ops_runtime_head" "$agent_ops_sha"

(
  cd "$agent_ops_repo"
  export PYTHONDONTWRITEBYTECODE=1
  export PYTHONPATH="$agent_ops_repo"
  "$python_bin" -c '
import sys
from tools.agent_ops_c1.operator_ops import _gateway_pin_ok
raise SystemExit(0 if _gateway_pin_ok(sys.argv[1]) else 1)
' "$runtime_root"
  C1_PROFILE_HOME="$chief_profile" HERMES_HOME="$chief_profile" \
    C1_HERMES_REPO="$runtime_root" "$python_bin" \
    -m tools.agent_ops_c1.operator_ops preflight --candidate
)
