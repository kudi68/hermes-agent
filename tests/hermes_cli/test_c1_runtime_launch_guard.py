"""C1 chief startup is limited to the launchd-owned c1-runtime checkout."""

import logging

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
