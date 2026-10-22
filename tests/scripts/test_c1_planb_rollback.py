"""Exercise the rollback script with shell functions; no live commands are reachable."""

from pathlib import Path
import subprocess

import pytest


HARNESS = r"""
set -euo pipefail
scenario=$1
script=$2
calls=$3
record() { printf '%s\n' "$*" >> "$calls"; }
id() { printf '501\n'; }
ls() { record "ls $*"; }
shasum() { record "shasum $*"; }
plutil() { record "plutil $*"; }
cp() { record "cp $*"; }
cmp() { record "cmp $*"; }
sleep() { record "sleep $*"; }
git() {
  record "git $*"
  case "$*" in
    *" rev-parse HEAD") printf 'b6e7d10e8b04fd10e7a1d72563a40c343a11181e\n' ;;
  esac
}
launchctl() {
  record "launchctl $*"
  case "$1" in
    bootout)
      if [ "$scenario" = unloaded ]; then
        printf 'Boot-out failed: 3: No such process\n' >&2
        return 3
      elif [ "$scenario" = denied ]; then
        printf 'Boot-out failed: 1: Operation not permitted\n' >&2
        return 1
      fi
      ;;
    print) printf 'gui/501/local.hermes.c1-chief = {\n\tpid = 4242\n}\n' ;;
  esac
}
ps() {
  record "ps $*"
  printf '  PID  PPID COMM\n 4242     1 fake-chief\n'
}
source "$script"
"""


def _run_rollback(tmp_path, scenario):
    harness = tmp_path / "harness.sh"
    harness.write_text(HARNESS)
    calls = tmp_path / "calls.log"
    script = Path(__file__).resolve().parents[2] / "scripts" / "c1-planB-rollback.sh"
    result = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", str(harness), scenario, str(script), str(calls)],
        # Every external action must have a fake function, or fail instead of reaching the host.
        env={"PATH": "/nonexistent"},
        capture_output=True, text=True, timeout=10,
    )
    return result, calls.read_text().splitlines()


@pytest.mark.parametrize("scenario", ["unloaded", "loaded"])
def test_rollback_bootstraps_and_prints_pid_for_both_job_states(tmp_path, scenario):
    result, calls = _run_rollback(tmp_path, scenario)
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls.count("launchctl bootstrap gui/501 /Users/kudi68-15mba/Library/LaunchAgents/local.hermes.c1-chief.plist") == 1
    assert "launchctl print gui/501/local.hermes.c1-chief" in calls
    assert "ps -o pid,ppid,comm -p 4242" in calls
    assert "4242     1 fake-chief" in result.stdout


def test_rollback_does_not_hide_unexpected_bootout_failure(tmp_path):
    result, calls = _run_rollback(tmp_path, "denied")
    assert result.returncode != 0
    assert not any(call.startswith("launchctl bootstrap ") for call in calls)
    assert not any(" checkout " in call for call in calls)
    assert "Operation not permitted" in result.stderr
