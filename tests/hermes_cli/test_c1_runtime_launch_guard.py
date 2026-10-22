"""C1 chief startup is limited to the launchd-owned c1-runtime checkout."""

import logging
from pathlib import Path

import pytest

from hermes_cli import gateway


def _assert_startup_refused(tmp_path, monkeypatch, *, parent_pid, project_root, reason, caplog):
    monkeypatch.setattr(gateway.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(gateway.os, "getppid", lambda: parent_pid)
    monkeypatch.setattr(gateway, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(
        gateway, "_guard_official_docker_root_gateway",
        lambda: pytest.fail("startup refusal must happen before gateway setup"),
    )

    with caplog.at_level(logging.ERROR, logger="hermes_cli.gateway"):
        with pytest.raises(SystemExit) as exc:
            gateway.run_gateway()

    assert exc.value.code == gateway.GATEWAY_FATAL_CONFIG_EXIT_CODE
    assert reason in caplog.text


def test_gateway_start_refuses_non_launchd_parent(tmp_path, monkeypatch, caplog):
    runtime_root = tmp_path / "Projects" / "c1-runtime"
    _assert_startup_refused(
        tmp_path,
        monkeypatch,
        parent_pid=42,
        project_root=runtime_root / "hermes-agent",
        reason="not launchd",
        caplog=caplog,
    )


def test_gateway_start_refuses_project_outside_c1_runtime(tmp_path, monkeypatch, caplog):
    _assert_startup_refused(
        tmp_path,
        monkeypatch,
        parent_pid=1,
        project_root=tmp_path / "Projects" / "night" / "c1-planB",
        reason="outside c1-runtime",
        caplog=caplog,
    )


def test_launchd_identity_parks_a_refused_start_instead_of_restart_looping(tmp_path, monkeypatch, caplog):
    """Under the C1 launchd job, a permanent refusal must exit 0 so KeepAlive parks it."""
    monkeypatch.setenv("XPC_SERVICE_NAME", "local.hermes.c1-chief")
    monkeypatch.setattr(gateway.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(gateway.os, "getppid", lambda: 42)
    monkeypatch.setattr(gateway, "PROJECT_ROOT", tmp_path / "Projects" / "c1-runtime" / "hermes-agent")
    monkeypatch.setattr(
        gateway, "_guard_official_docker_root_gateway",
        lambda: pytest.fail("startup refusal must happen before gateway setup"),
    )

    with caplog.at_level(logging.ERROR, logger="hermes_cli.gateway"):
        with pytest.raises(SystemExit) as exc:
            gateway.run_gateway()

    assert exc.value.code == 0
    assert "not launchd" in caplog.text


@pytest.mark.parametrize(
    ("enabled", "temporary_home", "allowed"),
    [("1", True, True), (None, True, False), ("1", False, False), ("0", True, False)],
)
def test_test_only_opt_out_requires_flag_and_tmp_home(
    tmp_path, monkeypatch, enabled, temporary_home, allowed,
):
    """Neither the explicit test flag nor a temporary HOME alone bypasses the guard."""
    flag = "HERMES_TEST_ONLY_ALLOW_C1_GATEWAY_BOOT_IN_TMP_HOME"
    if enabled is None:
        monkeypatch.delenv(flag, raising=False)
    else:
        monkeypatch.setenv(flag, enabled)
    home = tmp_path / "home" if temporary_home else Path("/Users/c1-test-outside-tmp")
    monkeypatch.setenv("HOME", str(home))
    reason = gateway._c1_runtime_launch_refusal_reason(
        parent_pid=42, project_root=tmp_path / "outside-runtime",
    )
    assert (reason is None) is allowed
    if not allowed:
        assert "not launchd" in reason


def test_test_only_opt_out_rejects_home_symlink_outside_tmp(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_TEST_ONLY_ALLOW_C1_GATEWAY_BOOT_IN_TMP_HOME", "1")
    home = tmp_path / "home"
    home.symlink_to(Path("/Users"), target_is_directory=True)
    monkeypatch.setenv("HOME", str(home))
    assert gateway._c1_runtime_launch_refusal_reason(parent_pid=42) is not None


@pytest.mark.parametrize(
    ("home", "allowed"),
    [
        ("/private/var/folders/c1-test/T/pytest-1/home", True),
        ("/private/var/folders/c1-test/T", False),
        ("/private/var/folders/c1-test/T-sibling/home", False),
        ("/private/tmp/c1-test/home", True),
    ],
)
def test_test_only_opt_out_accepts_macos_default_tmpdir(monkeypatch, home, allowed):
    import tempfile

    monkeypatch.setattr(tempfile, "gettempdir", lambda: "/private/var/folders/c1-test/T")
    monkeypatch.setenv("HERMES_TEST_ONLY_ALLOW_C1_GATEWAY_BOOT_IN_TMP_HOME", "1")
    monkeypatch.setenv("HOME", home)
    reason = gateway._c1_runtime_launch_refusal_reason(
        parent_pid=42, project_root=Path("/outside-runtime"),
    )
    assert (reason is None) is allowed
