"""``post_gateway_admission``: let a plugin consume an admitted inbound message (#129958).

``GatewayInboundMixin._handle_message`` fires it once per non-internal message that survived
authorization, bot admission, pause/drain, pending-reply intercepts, the running-session lane and
idle slash-command dispatch, inside the claimed session slot. It runs under the message's ROUTED
profile scope: the receiving bot's handler binds the transport home (auth reads its ``.env``), which
differs from the runtime home when that bot serves a chat for another profile.

Fail-open: a callback that raises, times out or returns anything other than
``{"action": "handled"}`` is logged/ignored and the message proceeds to the agent, so one buggy
plugin can never blackhole a profile's traffic. The payload is a plain snapshot, never live
runner/session-store handles.

The fixed C1 request uses a separate fail-closed binding contract below: its
payload also carries a durable, deadline-bound capability for one synchronous effect.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
import hashlib
import inspect
import json
import logging
from pathlib import Path
import sqlite3
import time
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

POST_ADMISSION_HOOK = "post_gateway_admission"
C1_TELEGRAM_PROFILE = "chief"
C1_TELEGRAM_REQUEST = "回傳 kudi68/personal-ai-os 的 NEXT.md 第一行"
C1_TELEGRAM_HOOK_TIMEOUT_SECONDS = 45
# Telegram timestamps have second precision; tolerate a small sender/host clock skew.
C1_TELEGRAM_FUTURE_SKEW_SECONDS = 30
_C1_TELEGRAM_IDENTITY_SCHEMA = "hermes-telegram-message-identity-v1"


def _routed_scope(runner: Any, source: Any):
    if not getattr(getattr(runner, "config", None), "multiplex_profiles", False):
        return contextlib.nullcontext()
    from gateway.run import _async_profile_runtime_scope
    return _async_profile_runtime_scope(runner._resolve_profile_home_for_source(source))


async def run_post_admission_hook(
    runner: Any, event: Any, source: Any, session_key: str
) -> tuple[bool, Optional[str]]:
    """Return ``(handled, reply)``; ``handled=False`` means run the ordinary agent turn."""
    try:
        async with _routed_scope(runner, source):
            from hermes_cli.lifecycle import ainvoke_hook, has_hook
            if not has_hook(POST_ADMISSION_HOOK):
                return False, None
            results = await ainvoke_hook(
                POST_ADMISSION_HOOK, session_key=session_key,
                platform=source.platform.value if source.platform else "",
                source=source.to_dict(), message_id=event.message_id, text=event.text or "",
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("%s failed; continuing with the agent turn", POST_ADMISSION_HOOK, exc_info=True)
        return False, None
    for result in results:
        if isinstance(result, dict) and result.get("action") == "handled":
            reply = result.get("reply")
            logger.info("%s: a plugin handled the message for session %s", POST_ADMISSION_HOOK, session_key)
            return True, reply if isinstance(reply, str) and reply.strip() else None
    return False, None


def _canonical_digest(value: dict) -> str:
    payload = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def is_c1_telegram_personal_target(event: Any, source: Any, runner: Any) -> bool:
    """Use the canonical runtime identity; a primary profile can be None on the wire."""
    platform = getattr(getattr(source, "platform", None), "value", None)
    raw_text = getattr(getattr(event, "raw_message", None), "text", None)
    fixed_request = (
        platform == "telegram"
        and (
            getattr(event, "text", None) == C1_TELEGRAM_REQUEST
            or raw_text == C1_TELEGRAM_REQUEST
        )
    )
    if not fixed_request:
        return False
    from gateway.session_identity import IdentityUnresolved, canonical_identity

    identity = canonical_identity(source, runner=runner)
    if identity is None:
        # An unresolved fixed request must never become an ordinary agent turn.
        raise IdentityUnresolved("C1 Telegram runtime profile is unresolved")
    return identity.runtime_profile == C1_TELEGRAM_PROFILE


def _utc_now() -> datetime:
    return datetime.now(UTC)


class C1DispatchClosed(RuntimeError):
    """The request cannot authorize another effect; read back instead of retrying."""


class C1TelegramDispatch:
    """Durable, at-most-once effect gate for a cooperative C1 binding.

    A binding must explicitly accept ``c1_dispatch``. Check ``check_active`` before
    admitting durable C1 work, and use ``run_effect`` at the actual return-transport
    boundary, after preparation. Never wrap a whole workflow or queue deferred
    work inside the effect: that would authorize a later send too early.
    This is a trusted-plugin contract, not a Python sandbox. An already-started
    transport operation may have an uncertain outcome; abandoning its thread
    never proves that operation was cancelled.
    """

    def __init__(self, runtime_home: Path, identity: dict):
        self.path = Path(runtime_home) / "c1-telegram-dispatch.sqlite3"
        self.request_id = identity["request_id"]
        self.expires_at = datetime.fromisoformat(identity["expires_at"])
        self.deadline = time.monotonic() + C1_TELEGRAM_HOOK_TIMEOUT_SECONDS
        fingerprint = _canonical_digest(identity)
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS requests ("
                "request_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, "
                "status TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            self.claimed = db.execute(
                "INSERT OR IGNORE INTO requests VALUES (?, ?, 'DISPATCHING', ?)",
                (self.request_id, fingerprint, _utc_now().isoformat()),
            ).rowcount == 1
            prior = db.execute(
                "SELECT fingerprint FROM requests WHERE request_id = ?", (self.request_id,)
            ).fetchone()
            if prior[0] != fingerprint:
                raise C1DispatchClosed("request identity conflict")

    @contextlib.contextmanager
    def _connect(self):
        # Only hashes and terminal state are persisted, never Telegram account data or text.
        with contextlib.closing(sqlite3.connect(self.path, timeout=1)) as db:
            with db:
                yield db

    def readback(self) -> str:
        return self.read_request(self.path.parent, self.request_id)["status"]

    @staticmethod
    def read_request(runtime_home: Path, request_id: str) -> dict | None:
        """Read a request in a fresh process without creating or changing its ledger."""
        path = Path(runtime_home) / "c1-telegram-dispatch.sqlite3"
        if not path.is_file():
            return None
        with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            row = db.execute(
                "SELECT status, updated_at FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        return {"request_id": request_id, "status": row[0], "updated_at": row[1]} if row else None

    def uncertain(self) -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE requests SET status = 'UNCERTAIN', updated_at = ? "
                "WHERE request_id = ? AND status IN ('DISPATCHING', 'EFFECT_STARTED', 'EFFECT_FINISHED')",
                (_utc_now().isoformat(), self.request_id),
            )

    def check_active(self) -> None:
        """Reject late preparation before it admits durable work or claims the return."""
        if not self.claimed:
            raise C1DispatchClosed("replayed dispatch")
        if time.monotonic() >= self.deadline or _utc_now() > self.expires_at:
            self.uncertain()
            raise C1DispatchClosed("dispatch deadline expired")
        if self.readback() != "DISPATCHING":
            raise C1DispatchClosed("dispatch already claimed or uncertain")

    def run_effect(self, effect, *args, **kwargs):
        """Authorize one immediate transport operation; late callbacks cannot claim it."""
        self.check_active()
        if inspect.iscoroutinefunction(effect):
            self.uncertain()
            raise C1DispatchClosed("effect must complete synchronously inside the gate")
        with self._connect() as db:
            # BEGIN IMMEDIATE serializes timeout, replay and concurrent callback claims.
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute(
                "UPDATE requests SET status = 'EFFECT_STARTED', updated_at = ? "
                "WHERE request_id = ? AND status = 'DISPATCHING'",
                (_utc_now().isoformat(), self.request_id),
            ).rowcount
        if not changed:
            raise C1DispatchClosed("dispatch already claimed or uncertain")
        try:
            # Recheck after durable intent: acquiring the database may have used the budget.
            if time.monotonic() >= self.deadline or _utc_now() > self.expires_at:
                raise C1DispatchClosed("effect deadline expired")
            result = effect(*args, **kwargs)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise C1DispatchClosed("effect returned deferred work")
        except BaseException:
            self.uncertain()
            raise
        with self._connect() as db:
            # A late completion must never overwrite the timeout's UNCERTAIN state.
            db.execute(
                "UPDATE requests SET status = 'EFFECT_FINISHED', updated_at = ? "
                "WHERE request_id = ? AND status = 'EFFECT_STARTED'",
                (_utc_now().isoformat(), self.request_id),
            )
        return result


def _c1_binding_callback():
    from hermes_cli.plugins import has_hook, iter_hook_callbacks

    if not has_hook(POST_ADMISSION_HOOK):
        return None
    callbacks = iter_hook_callbacks(POST_ADMISSION_HOOK)
    if len(callbacks) != 1:
        return None
    callback = callbacks[0]
    try:
        parameter = inspect.signature(callback).parameters.get("c1_dispatch")
    except (TypeError, ValueError):
        return None
    # **kwargs alone is not evidence that the binding implements the effect-gate contract.
    if parameter is None or parameter.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY
    ):
        return None
    return callback


def _invoke_c1_binding(callback, payload):
    from hermes_cli.plugins_dispatch import PluginDispatchMixin

    try:
        return PluginDispatchMixin._invoke_hook_callback(callback, payload)
    except SystemExit as exc:
        # Preserve the ordinary plugin dispatcher's isolation before reaching asyncio.
        raise RuntimeError("C1 binding exited") from exc


def _telegram_c1_identity(event: Any, source: Any) -> dict | None:
    """Build stable replay identity from the admitted Telegram Message, not update_id."""
    raw = getattr(event, "raw_message", None)
    chat = getattr(raw, "chat", None)
    user = getattr(raw, "from_user", None)
    chat_id = getattr(chat, "id", None)
    user_id = getattr(user, "id", None)
    message_id = getattr(raw, "message_id", None)
    message_date = getattr(raw, "date", None)

    if (
        type(chat_id) is not int or chat_id <= 0
        or type(user_id) is not int or user_id <= 0
        or type(message_id) is not int or message_id <= 0
        or getattr(chat, "type", None) != "private"
        or getattr(user, "is_bot", True) is not False
        or getattr(source, "chat_type", None) != "dm"
        or getattr(source, "is_bot", True) is not False
        or getattr(source, "chat_id", None) != str(chat_id)
        or getattr(source, "user_id", None) != str(user_id)
        or getattr(event, "message_id", None) != str(message_id)
        or getattr(raw, "text", None) != C1_TELEGRAM_REQUEST
        or getattr(event, "text", None) != C1_TELEGRAM_REQUEST
        or not isinstance(message_date, datetime)
        or message_date.tzinfo is None
        or message_date.utcoffset() is None
    ):
        return None

    platform = getattr(getattr(source, "platform", None), "value", None)
    if platform != "telegram":
        return None

    thread_id = getattr(source, "thread_id", None)
    if thread_id is not None and type(thread_id) is not str:
        return None

    # Telegram update_id is a delivery cursor and may change after long inactivity.
    # The per-chat message_id is the stable identity for duplicate delivery/restart.
    request_material = {
        "platform": platform,
        "profile": C1_TELEGRAM_PROFILE,
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "thread_id": thread_id,
        "message_id": str(message_id),
    }
    request_digest = _canonical_digest(request_material)
    conversation_digest = _canonical_digest({
        "platform": platform,
        "profile": C1_TELEGRAM_PROFILE,
        "chat_id": str(chat_id),
        "thread_id": thread_id,
    })
    issued_at = message_date.astimezone(UTC)
    expires_at = issued_at + timedelta(minutes=5)
    now = _utc_now()
    if now > expires_at or issued_at > now + timedelta(seconds=C1_TELEGRAM_FUTURE_SKEW_SECONDS):
        return None
    return {
        "schema": _C1_TELEGRAM_IDENTITY_SCHEMA,
        "event_id": "gateway-" + request_digest,
        "request_id": "request-" + request_digest,
        "conversation_id": "conversation-" + conversation_digest,
        # The downstream C1 principal verifier must allowlist this Telegram
        # user_id to human:kudi68; these platform fields are not proof by themselves.
        "principal": "basic:kudi68",
        "principal_proof_ref": "telegram-proof-" + request_digest,
        "principal_proof": {
            "platform": platform,
            "user_id": str(user_id),
            "chat_id": str(chat_id),
            "thread_id": thread_id,
            "message_id": str(message_id),
        },
        "text": C1_TELEGRAM_REQUEST,
        "issued_at": issued_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "nonce": "telegram-nonce-" + request_digest,
    }


async def run_c1_telegram_personal(
    runner: Any, event: Any, source: Any, session_key: str
) -> tuple[bool, Optional[str]]:
    """Consume only the fixed chief Telegram request; never fall through on failure."""
    if not is_c1_telegram_personal_target(event, source, runner):
        return False, None
    platform = source.platform.value

    identity = _telegram_c1_identity(event, source)
    if identity is None:
        logger.warning("C1 Telegram request rejected before dispatch; consuming without agent fallback")
        return True, None

    dispatch = None
    try:
        async with _routed_scope(runner, source):
            from gateway.session_identity import canonical_identity
            runtime_identity = canonical_identity(source, runner=runner)
            dispatch = C1TelegramDispatch(runtime_identity.runtime_home, identity)
            if not dispatch.claimed:
                logger.warning("C1 Telegram request already recorded: %s", dispatch.readback())
                return True, None
            callback = _c1_binding_callback()
            if callback is None:
                dispatch.uncertain()
                logger.error("C1 Telegram fenced binding is unavailable; consuming without agent fallback")
                return True, None
            hook_source = source.to_dict()
            hook_source["c1_telegram_identity"] = identity
            await asyncio.wait_for(
                asyncio.to_thread(
                    _invoke_c1_binding,
                    callback,
                    dict(
                        session_key=session_key,
                        platform=platform,
                        source=hook_source,
                        message_id=event.message_id,
                        text=C1_TELEGRAM_REQUEST,
                        telegram_identity=identity,
                        c1_dispatch=dispatch,
                    ),
                ),
                timeout=C1_TELEGRAM_HOOK_TIMEOUT_SECONDS,
            )
            # EFFECT_FINISHED only records that the synchronous effect returned;
            # delivery/receipt truth still belongs to the downstream C1 ledger.
            if dispatch.readback() == "DISPATCHING":
                dispatch.uncertain()
    except asyncio.CancelledError:
        if dispatch is not None:
            dispatch.uncertain()
        raise
    except Exception as exc:
        if dispatch is not None:
            dispatch.uncertain()
        logger.warning(
            "C1 Telegram binding failed (%s); consuming without agent fallback",
            type(exc).__name__,
        )
    # The binding sends through its receipt-aware Gateway transport. Returning a
    # reply here would cause Hermes to send a second, unreceipted message.
    return True, None
