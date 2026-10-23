"""Exercise the candidate preflight script without launchd or live profile access."""

from pathlib import Path
import subprocess


HARNESS = r"""
set -euo pipefail
scenario=$1
script=$2
calls=$3
record() { printf '%s\n' "$*" >> "$calls"; }
ls() { record "ls $*"; }
git() {
  record "git $*"
  case "$*" in
    *" rev-parse --verify HEAD^{commit}") printf '%s\n' "$C1_CANDIDATE_SHA" ;;
  esac
}
launchctl() {
  record "launchctl $*"
  return 97
}
python() {
  record "python $*"
  case "$*" in
    -c*)
      if [ "$scenario" = pin_failed ]; then return 1; fi
      ;;
    -m\ tools.agent_ops_c1.operator_ops\ preflight\ --candidate*)
      if [ "$scenario" = preflight_failed ]; then return 1; fi
      ;;
  esac
}
source "$script" "$PWD" "$PWD" "$C1_CANDIDATE_SHA" "$C1_AGENT_OPS_SHA"
"""


def _run_preflight(tmp_path, scenario):
    harness = tmp_path / "preflight-harness.sh"
    harness.write_text(HARNESS)
    calls = tmp_path / "calls.log"
    script = Path(__file__).resolve().parents[2] / "scripts" / "c1-planB-preflight.sh"
    env = {
        "PATH": "/nonexistent",
        "C1_CANDIDATE_SHA": "a" * 40,
        "C1_AGENT_OPS_SHA": "a" * 40,
        "C1_HERMES_PYTHON": "python",
    }
    result = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", str(harness), scenario, str(script), str(calls)],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return result, calls.read_text().splitlines()


def test_preflight_runs_pin_check_then_operator_preflight_without_launchd(tmp_path):
    result, calls = _run_preflight(tmp_path, "pass")
    assert result.returncode == 0, result.stdout + result.stderr
    pin = next(i for i, call in enumerate(calls) if " -c " in call)
    preflight = next(
        i for i, call in enumerate(calls)
        if " -m tools.agent_ops_c1.operator_ops preflight" in call
    )
    assert "--candidate" in calls[preflight]
    assert pin < preflight
    assert not any(call.startswith("launchctl ") for call in calls)


def test_preflight_pin_failure_stops_before_operator_preflight(tmp_path):
    result, calls = _run_preflight(tmp_path, "pin_failed")
    assert result.returncode != 0
    assert any(" -c " in call for call in calls)
    assert not any(
        " -m tools.agent_ops_c1.operator_ops preflight" in call for call in calls
    )
    assert not any(call.startswith("launchctl ") for call in calls)


def test_preflight_failure_is_nonzero_without_launchd(tmp_path):
    result, calls = _run_preflight(tmp_path, "preflight_failed")
    assert result.returncode != 0
    assert any(
        " -m tools.agent_ops_c1.operator_ops preflight" in call for call in calls
    )
    assert not any(call.startswith("launchctl ") for call in calls)


def test_preflight_requires_exact_candidate_revisions(tmp_path):
    harness = tmp_path / "preflight-harness.sh"
    harness.write_text(HARNESS)
    calls = tmp_path / "calls.log"
    script = Path(__file__).resolve().parents[2] / "scripts" / "c1-planB-preflight.sh"
    result = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", str(harness), "pass", str(script), str(calls)],
        env={
            "PATH": "/nonexistent",
            "C1_CANDIDATE_SHA": "",
            "C1_AGENT_OPS_SHA": "",
            "C1_HERMES_PYTHON": "python",
        },
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0
    assert not calls.exists()
