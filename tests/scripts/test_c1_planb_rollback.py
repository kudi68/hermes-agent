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
    print)
      print_count=0
      while IFS= read -r recorded; do
        case "$recorded" in
          "launchctl print "*) print_count=$((print_count + 1)) ;;
        esac
      done < "$calls"
      case "$scenario" in
        loaded)
          if [ "$print_count" -eq 1 ]; then
            record "print-result loaded"
            printf 'gui/501/local.hermes.c1-chief = {\n\tstate = running\n}\n'
          elif [ "$print_count" -eq 2 ]; then
            record "print-result unloaded"
            printf 'Could not find service "local.hermes.c1-chief" in domain for user gui: 501\n' >&2
            return 113
          else
            record "print-result pid"
            printf 'gui/501/local.hermes.c1-chief = {\n\tpid = 4242\n}\n'
          fi
          ;;
        unloaded)
          if [ "$print_count" -eq 1 ]; then
            record "print-result unloaded"
            printf 'Could not find service "local.hermes.c1-chief" in domain for user gui: 501\n' >&2
            return 113
          else
            record "print-result pid"
            printf 'gui/501/local.hermes.c1-chief = {\n\tpid = 4242\n}\n'
          fi
          ;;
        print_denied)
          printf 'Could not read service: Operation not permitted\n' >&2
          return 1
          ;;
        wrong_service_not_found)
          printf 'Could not find service "other.service" in domain for user gui: 501\n' >&2
          return 113
          ;;
        still_loaded)
          record "print-result loaded"
          printf 'gui/501/local.hermes.c1-chief = {\n\tstate = running\n}\n'
          ;;
      esac
      ;;
  esac
}
pgrep() {
  record "pgrep $*"
  printf '4242 fake-chief\n'
}
print_count=0
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
        check=False,
    )
    return result, calls.read_text().splitlines()


@pytest.mark.parametrize("scenario", ["unloaded", "loaded"])
def test_rollback_waits_for_unload_then_bootstraps_and_prints_pid(tmp_path, scenario):
    result, calls = _run_rollback(tmp_path, scenario)
    assert result.returncode == 0, result.stdout + result.stderr

    bootout = next(i for i, call in enumerate(calls) if call.startswith("launchctl bootout "))
    checkout = next(i for i, call in enumerate(calls) if call.startswith("git -C ") and " checkout " in call)
    bootstrap = next(i for i, call in enumerate(calls) if call.startswith("launchctl bootstrap "))
    assert bootout < checkout < bootstrap

    if scenario == "loaded":
        loaded = calls.index("print-result loaded")
        unloaded = calls.index("print-result unloaded")
        assert bootout < loaded < unloaded < checkout < bootstrap
    else:
        unloaded = calls.index("print-result unloaded")
        assert bootout < unloaded < checkout < bootstrap

    assert calls.count("launchctl bootstrap gui/501 /Users/kudi68-15mba/Library/LaunchAgents/local.hermes.c1-chief.plist") == 1
    assert "launchctl print gui/501/local.hermes.c1-chief" in calls
    assert calls.index("print-result pid") > bootstrap
    assert "pgrep -l ." in calls
    assert "4242 fake-chief" in result.stdout


def test_rollback_does_not_hide_unexpected_bootout_failure(tmp_path):
    result, calls = _run_rollback(tmp_path, "denied")
    assert result.returncode != 0
    assert not any(call.startswith("launchctl bootstrap ") for call in calls)
    assert not any(" checkout " in call for call in calls)
    assert "Operation not permitted" in result.stderr


def test_rollback_stops_on_unexpected_print_error(tmp_path):
    result, calls = _run_rollback(tmp_path, "print_denied")
    assert result.returncode != 0
    assert "Could not read service: Operation not permitted" in result.stderr
    assert not any(call.startswith("launchctl bootstrap ") for call in calls)
    assert not any(" checkout " in call for call in calls)


def test_rollback_does_not_treat_another_service_as_unloaded(tmp_path):
    result, calls = _run_rollback(tmp_path, "wrong_service_not_found")
    assert result.returncode != 0
    assert not any(call.startswith("launchctl bootstrap ") for call in calls)
    assert not any(" checkout " in call for call in calls)
    assert 'Could not find service "other.service"' in result.stderr


def test_rollback_times_out_while_job_remains_loaded(tmp_path):
    result, calls = _run_rollback(tmp_path, "still_loaded")
    assert result.returncode != 0
    assert "remained loaded for 35 checks" in result.stderr
    assert calls.count("launchctl print gui/501/local.hermes.c1-chief") == 35
    assert calls.count("sleep 1") == 34
    assert not any(call.startswith("launchctl bootstrap ") for call in calls)
    assert not any(" checkout " in call for call in calls)
