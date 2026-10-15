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
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

POST_ADMISSION_HOOK = "post_gateway_admission"
C1_TELEGRAM_PROFILE = "chief"
C1_TELEGRAM_REQUEST = "回傳 kudi68/personal-ai-os 的 NEXT.md 第一行"
C1_TELEGRAM_HOOK_TIMEOUT_SECONDS = 45
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


def is_c1_telegram_personal_target(event: Any, source: Any) -> bool:
    """Identify the fixed chief request before plugins can rewrite its visible text."""
    platform = getattr(getattr(source, "platform", None), "value", None)
    raw_text = getattr(getattr(event, "raw_message", None), "text", None)
    return (
        platform == "telegram"
        and getattr(source, "profile", None) == C1_TELEGRAM_PROFILE
        and (
            getattr(event, "text", None) == C1_TELEGRAM_REQUEST
            or raw_text == C1_TELEGRAM_REQUEST
        )
    )


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
    issued_at = message_date.astimezone(timezone.utc)
    expires_at = issued_at + timedelta(minutes=5)
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
    if not is_c1_telegram_personal_target(event, source):
        return False, None
    platform = source.platform.value

    identity = _telegram_c1_identity(event, source)
    if identity is None:
        logger.warning("C1 Telegram request rejected before dispatch; consuming without agent fallback")
        return True, None

    try:
        async with _routed_scope(runner, source):
            from hermes_cli.lifecycle import has_hook, invoke_hook
            if not has_hook(POST_ADMISSION_HOOK):
                logger.error("C1 Telegram binding is unavailable; consuming without agent fallback")
                return True, None
            hook_source = source.to_dict()
            hook_source["c1_telegram_identity"] = identity
            await asyncio.wait_for(
                asyncio.to_thread(
                    invoke_hook,
                    POST_ADMISSION_HOOK,
                    session_key=session_key,
                    platform=platform,
                    source=hook_source,
                    message_id=event.message_id,
                    text=C1_TELEGRAM_REQUEST,
                    telegram_identity=identity,
                ),
                timeout=C1_TELEGRAM_HOOK_TIMEOUT_SECONDS,
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "C1 Telegram binding failed (%s); consuming without agent fallback",
            type(exc).__name__,
        )
    # The binding sends through its receipt-aware Gateway transport. Returning a
    # reply here would cause Hermes to send a second, unreceipted message.
    return True, None
