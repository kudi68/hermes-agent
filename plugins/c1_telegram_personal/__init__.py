"""Opt-in chief-profile C1 Telegram initializer."""
from __future__ import annotations

import errno
import hashlib
import importlib
import importlib.util
import json
import math
import logging
import os
from pathlib import Path
import re
import stat
import sys

from hermes_constants import get_hermes_home

PLUGIN_ID = "c1_telegram_personal"
HOOK_NAME = "post_gateway_admission"
GATEWAY_PIN = "4f3620276023d46afa7c97b1039c8234c928d027"
_BOT_TOKEN = re.compile(r"[0-9]+:[A-Za-z0-9_-]{30,}\Z")
_OWNER_ID = re.compile(r"[1-9][0-9]{0,19}\Z")
logger = logging.getLogger(__name__)


def _agent_ops_modules(repo_value: object):
    if type(repo_value) is not str or not repo_value:
        raise RuntimeError("C1 Agent Ops checkout is not configured.")
    root = Path(repo_value)
    if not root.is_absolute() or ".." in root.parts or not root.is_dir():
        raise RuntimeError("C1 Agent Ops checkout is invalid.")
    for component in reversed((root, *root.parents)):
        if component.is_symlink():
            raise RuntimeError("C1 Agent Ops checkout is invalid.")
    root = root.resolve(strict=True)
    package_dir = root / "tools" / "agent_ops_c1"
    init_file = package_dir / "__init__.py"
    module_file = package_dir / "hermes_production.py"
    if not init_file.is_file() or not module_file.is_file():
        raise RuntimeError("C1 Agent Ops integration is unavailable.")
    package_name = "_hermes_c1_agent_ops_" + hashlib.sha256(
        str(root).encode("utf-8"),
    ).hexdigest()[:12]
    if package_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            package_name,
            init_file,
            submodule_search_locations=[str(package_dir)],
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("C1 Agent Ops integration is unavailable.")
        package = importlib.util.module_from_spec(spec)
        sys.modules[package_name] = package
        spec.loader.exec_module(package)
    production = importlib.import_module(package_name + ".hermes_production")
    operator_ops = importlib.import_module(package_name + ".operator_ops")
    return production, operator_ops


def _gateway_pid(profile_home: Path) -> tuple[int, float]:
    from gateway.status import (
        get_process_start_time,
        get_running_pid_identity_strict,
    )

    identity = get_running_pid_identity_strict(profile_home / "gateway.pid")
    start_time = (
        get_process_start_time(identity[0]) if identity is not None else None
    )
    if (
        identity is None
        or identity[0] != os.getpid()
        or type(start_time) is not int
        or start_time <= 0
    ):
        raise RuntimeError("C1 initializer requires this profile's live Gateway.")
    return identity[0], float(start_time)


def _read_marker(marker: Path):
    descriptor = None
    try:
        descriptor = os.open(
            marker, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
        ):
            raise RuntimeError("C1 binding marker is unsafe; preserving it.")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = None
            data = stream.read(4097)
        if len(data) > 4096:
            raise RuntimeError("C1 binding marker is invalid; preserving it.")
        record = json.loads(data.decode("utf-8"))
        if (
            type(record) is not dict
            or set(record) != {
                "plugin", "hook", "state", "pid", "start_time", "gateway_pin",
            }
            or record.get("plugin") != PLUGIN_ID
            or record.get("hook") != HOOK_NAME
            or record.get("state") != "registered"
            or type(record.get("pid")) is not int
            or record["pid"] < 2
            or type(record.get("start_time")) not in (int, float)
            or not math.isfinite(record["start_time"])
            or record["start_time"] <= 0
            or record.get("gateway_pin") != GATEWAY_PIN
        ):
            raise RuntimeError("C1 binding marker is invalid; preserving it.")
        return info, record
    except FileNotFoundError:
        return None
    except RuntimeError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise RuntimeError("C1 binding marker is unreadable; preserving it.") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _process_identity_is_live(pid: int, start_time: float) -> bool | None:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        return None
    try:
        from gateway.status import get_process_start_time, start_time_fingerprints_match

        current_start = get_process_start_time(pid)
        if current_start is None:
            return None
        return start_time_fingerprints_match(start_time, current_start)
    except Exception:
        return None


def _unlink_marker(marker: Path, original_info) -> None:
    try:
        current = marker.lstat()
    except FileNotFoundError:
        return
    if (
        not stat.S_ISREG(current.st_mode)
        or stat.S_IMODE(current.st_mode) != 0o600
        or current.st_uid != os.getuid()
        or current.st_nlink != 1
        or (current.st_dev, current.st_ino)
        != (original_info.st_dev, original_info.st_ino)
    ):
        raise RuntimeError("C1 binding marker changed; preserving it.")
    marker.unlink()
    directory_fd = os.open(
        marker.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_marker(profile_home: Path, pid: int, start_time: float) -> Path:
    marker = profile_home / "c1" / (PLUGIN_ID + ".binding.json")
    record = {
        "plugin": PLUGIN_ID,
        "hook": HOOK_NAME,
        "state": "registered",
        "pid": pid,
        "start_time": start_time,
        "gateway_pin": GATEWAY_PIN,
    }
    data = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        descriptor = os.open(
            marker,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        existing = _read_marker(marker)
        if existing is None:
            raise RuntimeError("C1 binding marker changed; retry registration.") from None
        existing_info, existing_record = existing
        owner_live = _process_identity_is_live(
            existing_record["pid"], existing_record["start_time"],
        )
        if owner_live is not False:
            raise RuntimeError(
                "C1 binding marker has an active or unverifiable owner; preserving it."
            )
        _unlink_marker(marker, existing_info)
        try:
            descriptor = os.open(
                marker,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            raise RuntimeError(
                "C1 binding marker changed during stale recovery; preserving it."
            ) from None
    except OSError:
        raise RuntimeError("C1 binding marker could not be written.") from None
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        directory_fd = os.open(
            marker.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return marker
    except OSError:
        raise RuntimeError("C1 binding marker could not be written.") from None


def _remove_marker(marker: Path, pid: int, start_time: float) -> None:
    try:
        existing = _read_marker(marker)
        if existing is None:
            return
        info, record = existing
        if record.get("pid") != pid or record.get("start_time") != start_time:
            raise RuntimeError("C1 binding marker changed; preserving it.")
        _unlink_marker(marker, info)
    except FileNotFoundError:
        return


def register(ctx):
    """Register only when both Hermes opt-in gates and trusted inputs are present."""
    get_config = getattr(ctx, "get_config", None)
    if not callable(get_config) or get_config("enabled") is not True:
        return None
    if getattr(ctx, "profile_name", None) != "chief":
        raise RuntimeError("C1 binding is restricted to the chief profile.")

    source_repo_value = get_config("source_repo")
    if (
        type(source_repo_value) is not str
        or not source_repo_value
        or not Path(source_repo_value).is_absolute()
        or ".." in Path(source_repo_value).parts
        or not Path(source_repo_value).is_dir()
    ):
        raise RuntimeError("C1 pinned source checkout is not configured.")

    profile_home = Path(get_hermes_home())
    if (
        profile_home.name != "chief"
        or profile_home.parent.name != "profiles"
        or not profile_home.is_absolute()
    ):
        raise RuntimeError("C1 chief profile path is invalid.")

    production, operator_ops = _agent_ops_modules(
        get_config("agent_ops_repo"),
    )
    hermes_repo = Path(__file__).resolve().parents[2]
    if not operator_ops._gateway_pin_ok(str(hermes_repo)):
        raise RuntimeError("C1 Gateway source pin is not satisfied.")

    from agent.secret_scope import get_secret

    allowed_users = get_secret("TELEGRAM_ALLOWED_USERS", "")
    bot_token = get_secret("TELEGRAM_BOT_TOKEN", "")
    if (
        type(allowed_users) is not str
        or not _OWNER_ID.fullmatch(allowed_users)
        or type(bot_token) is not str
        or not _BOT_TOKEN.fullmatch(bot_token)
    ):
        raise RuntimeError("C1 Telegram credentials or allowlist are invalid.")

    pid, start_time = _gateway_pid(profile_home)
    binding = production.build_trusted_binding(
        profile_home=profile_home,
        source_repo=source_repo_value,
        owner_user_id=int(allowed_users),
        bot_token=bot_token,
    )

    binding_module = importlib.import_module(
        production.__package__ + ".hermes_gateway_binding",
    )
    register_post_gateway_admission = binding_module.register_post_gateway_admission
    unregister_post_gateway_admission = binding_module.unregister_post_gateway_admission

    registration = register_post_gateway_admission(ctx, binding)
    marker = None
    try:
        marker = _write_marker(profile_home, pid, start_time)

        def cleanup():
            unregister_post_gateway_admission(registration)
            _remove_marker(marker, pid, start_time)

        ctx.on_unload(cleanup)
    except Exception:
        unregister_post_gateway_admission(registration)
        if marker is not None:
            _remove_marker(marker, pid, start_time)
        raise
    logger.info("C1 Telegram binding registered for the chief profile.")
    return registration
