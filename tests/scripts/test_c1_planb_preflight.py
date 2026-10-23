"""Exercise the candidate preflight script without launchd or live profile access."""

from pathlib import Path
import subprocess

import pytest


HARNESS = r"""
set -euo pipefail
scenario=$1
script=$2
calls=$3
record() { printf '%s\n' "$*" >> "$calls"; }
ls() { record "ls $*"; }
git() {
  record "git $*"
  repo=$2
  role=candidate
  case "$repo" in
    */runtime-hermes) role=hermes ;;
    */runtime-ops) role=ops ;;
  esac
  case "$*" in
    *" rev-parse --verify HEAD^{commit}")
      if [ "$scenario" = "$role-head" ]; then printf '%040d\n' 1
      elif [ "$role" = hermes ]; then printf '%s\n' "$C1_HERMES_RUNTIME_HEAD"
      elif [ "$role" = ops ]; then printf '%s\n' "$C1_AGENT_OPS_RUNTIME_HEAD"
      else printf '%s\n' "$C1_CANDIDATE_SHA"; fi
      ;;
    *" rev-parse --verify "*)
      if [ "$scenario" = "$role-missing" ]; then return 128; fi
      printf '%s\n' "$C1_CANDIDATE_SHA"
      ;;
    *" status "*)
      if [ "$scenario" = "$role-status-error" ]; then return 128; fi
      case "$scenario" in
        "$role-tracked") printf ' M gateway/core.py\n' ;;
        "$role-index") printf 'M  gateway/core.py\n' ;;
        "$role-untracked") printf '?? gateway/injected.py\n' ;;
        "$role-ignored")
          case "$*" in *--ignored=matching*) printf '!! gateway/ignored.py\n' ;; esac
          ;;
      esac
      ;;
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
      record "profile=$C1_PROFILE_HOME home=$HERMES_HOME"
      if [ "$scenario" = preflight_failed ]; then return 1; fi
      ;;
  esac
}
source "$script" "$PWD" "$PWD" "$C1_CANDIDATE_SHA" "$C1_AGENT_OPS_SHA" "${TEST_PROFILE-}"
"""


def _run_preflight(tmp_path, scenario, profile="/explicit/profiles/chief"):
    harness = tmp_path / "preflight-harness.sh"
    harness.write_text(HARNESS)
    calls = tmp_path / "calls.log"
    script = Path(__file__).resolve().parents[2] / "scripts" / "c1-planB-preflight.sh"
    env = {
        "PATH": "/nonexistent",
        "C1_CANDIDATE_SHA": "a" * 40,
        "C1_AGENT_OPS_SHA": "a" * 40,
        "C1_HERMES_PYTHON": "python",
        "C1_HERMES_RUNTIME_REPO": str(tmp_path / "runtime-hermes"),
        "C1_AGENT_OPS_RUNTIME_REPO": str(tmp_path / "runtime-ops"),
        "C1_HERMES_RUNTIME_HEAD": "b" * 40,
        "C1_AGENT_OPS_RUNTIME_HEAD": "c" * 40,
        "TEST_PROFILE": profile,
        "HERMES_HOME": "/wrong/profiles/other",
        "C1_PROFILE_HOME": "/wrong/profiles/other",
    }
    result = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", str(harness), scenario, str(script), str(calls)],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return result, calls.read_text().splitlines() if calls.exists() else []


def test_preflight_runs_pin_check_then_operator_preflight_without_launchd(tmp_path):
    result, calls = _run_preflight(tmp_path, "pass")
    assert result.returncode == 0, result.stdout + result.stderr
    pin = next(i for i, call in enumerate(calls) if call.startswith("python -c "))
    preflight = next(
        i for i, call in enumerate(calls)
        if " -m tools.agent_ops_c1.operator_ops preflight" in call
    )
    assert "--candidate" in calls[preflight]
    assert pin < preflight
    assert "profile=/explicit/profiles/chief home=/explicit/profiles/chief" in calls
    for role in ("hermes", "ops"):
        runtime_checks = [
            i for i, call in enumerate(calls)
            if call.startswith("git ") and f"/runtime-{role} " in call
        ]
        assert len(runtime_checks) == 4
        assert max(runtime_checks) < pin
        assert all(
            "rev-parse --verify" in calls[i] or " status " in calls[i]
            for i in runtime_checks
        )
    assert not any(call.startswith("launchctl ") for call in calls)


@pytest.mark.parametrize("profile", ["", "relative/profiles/chief", "/explicit/profiles/other"])
def test_preflight_refuses_missing_or_wrong_chief_path(tmp_path, profile):
    result, calls = _run_preflight(tmp_path, "pass", profile)
    assert result.returncode == 2
    assert "explicit absolute profiles/chief" in result.stderr
    assert not calls


@pytest.mark.parametrize("role", ["hermes", "ops"])
@pytest.mark.parametrize("fault", ["head", "tracked", "index", "untracked", "ignored", "missing", "status-error"])
def test_runtime_failure_stops_before_pin_profile_or_live_actions(tmp_path, role, fault):
    result, calls = _run_preflight(tmp_path, f"{role}-{fault}")
    assert result.returncode != 0
    assert not any(call.startswith(("python ", "launchctl ", "cp ", "plutil ")) for call in calls)
    assert not any(" checkout " in call for call in calls)


def test_preflight_pin_failure_stops_before_operator_preflight(tmp_path):
    result, calls = _run_preflight(tmp_path, "pin_failed")
    assert result.returncode != 0
    assert any(call.startswith("python -c ") for call in calls)
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


@pytest.mark.parametrize("fault", ["clean", "tracked", "index", "untracked", "ignored", "ops-ignored"])
def test_runtime_gate_with_real_temporary_checkouts(tmp_path, fault):
    runtime = tmp_path / "runtime"
    ops_runtime = tmp_path / "ops-runtime"
    candidate = tmp_path / "candidate"
    runtime.mkdir()

    def git(repo, *args):
        return subprocess.run(
            ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
        ).stdout.strip()

    git(runtime, "init", "-q")
    (runtime / "gateway").mkdir()
    (runtime / "gateway/core.py").write_text("PINNED = True\n")
    (runtime / ".gitignore").write_text("gateway/ignored.py\ntools/agent_ops_c1/ignored.py\n")
    git(runtime, "add", "--", ".gitignore", "gateway/core.py")
    git(runtime, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
        "commit", "-qm", "runtime fixture")
    sha = git(runtime, "rev-parse", "HEAD")
    git(tmp_path, "clone", "-q", str(runtime), str(candidate))
    git(tmp_path, "clone", "-q", str(runtime), str(ops_runtime))
    if fault in ("tracked", "index"):
        (runtime / "gateway/core.py").write_text("PINNED = False\n")
        if fault == "index":
            git(runtime, "add", "--", "gateway/core.py")
    elif fault in ("untracked", "ignored"):
        (runtime / f"gateway/{fault}.py").write_text("INJECTED = True\n")
    elif fault == "ops-ignored":
        (ops_runtime / "tools/agent_ops_c1").mkdir(parents=True)
        (ops_runtime / "tools/agent_ops_c1/ignored.py").write_text("INJECTED = True\n")
    script = Path(__file__).resolve().parents[2] / "scripts/c1-planB-preflight.sh"
    result = subprocess.run(
        ["/bin/bash", "--noprofile", "--norc", "-c",
         # Only the pin interpreter is synthetic; runtime inspection uses real git.
         'python() { :; }; ls() { :; }; source "$@"',
         "test", str(script), str(candidate), str(candidate), sha, sha,
         "/must-not-read/profiles/chief"],
        env={
            "PATH": "/usr/bin:/bin", "HOME": str(tmp_path), "TMPDIR": str(tmp_path),
            "C1_HERMES_PYTHON": "python",
            "C1_HERMES_RUNTIME_REPO": str(runtime),
            "C1_AGENT_OPS_RUNTIME_REPO": str(ops_runtime),
            "C1_HERMES_RUNTIME_HEAD": sha, "C1_AGENT_OPS_RUNTIME_HEAD": sha,
        },
        capture_output=True, text=True, timeout=15, check=False,
    )
    assert (result.returncode == 0) is (fault == "clean"), result.stdout + result.stderr
    assert git(runtime, "rev-parse", "HEAD") == sha
    assert git(ops_runtime, "rev-parse", "HEAD") == sha
