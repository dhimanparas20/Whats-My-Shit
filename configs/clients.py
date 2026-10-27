"""
Client singletons — one instance per process, built from configs.config.

Usage:
    from configs.clients import get_redis, get_waha, get_redis_str, get_redis_hash

Rules:
    - Always call the get_*() functions. Never construct clients ad-hoc.
    - get_*() are @lru_cached → same object every call, created lazily
      on first use (so a missing Redis doesn't crash `import`).
    - Huey tasks are sync by design → use the sync clients there.
    - get_redis_str / get_redis_hash cache one instance per (prefix, ttl)
      combination, so pass the prefix once and reuse the returned store.
"""

from functools import lru_cache
from typing import Optional

import redis

from configs.config import get_settings
from modules.redis_util import RedisHashUtil, RedisStringUtil
from modules.wa_util import WhatsAppUtil


# ──────────────────────────────────────────────
# URL (cheap — no connection made)
# ──────────────────────────────────────────────

def get_redis_url() -> str:
    """Redis connection URL from settings (pass to redis.from_url, etc.)."""
    return get_settings().redis.redis_url


# ──────────────────────────────────────────────
# Raw Redis client
# ──────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_redis() -> redis.Redis:
    """
    Shared SYNC Redis client (for scripts / direct commands).

    Note: RedisStringUtil/RedisHashUtil build their own internal clients
    from the URL — use those wrappers for app data, this raw client only
    when you need direct commands.
    """
    return redis.Redis.from_url(get_redis_url(), decode_responses=True)


# ──────────────────────────────────────────────
# Redis utility wrappers
# ──────────────────────────────────────────────

@lru_cache(maxsize=None)
def get_redis_str(
    prefix: str = "WMS",
    default_ttl: Optional[int] = None,
) -> RedisStringUtil:
    """
    Cached RedisStringUtil — one instance per (prefix, default_ttl).

    Example:
        otp = get_redis_str(prefix="WMS:OTP", default_ttl=300)
        otp.set("919876543210", {"code": "482917", "attempts": 0}, overwrite=True)
        data = otp.get("919876543210")
    """
    return RedisStringUtil(url=get_redis_url(), prefix=prefix, default_ttl=default_ttl)


@lru_cache(maxsize=None)
def get_redis_hash(
    prefix: str = "WMS",
    default_ttl: Optional[int] = None,
) -> RedisHashUtil:
    """
    Cached RedisHashUtil — one instance per (prefix, default_ttl).

    Example:
        messages = get_redis_hash(prefix="WMS:MESSAGES")
        msg_id = messages.create({"status": "queued", "chat_id": "919876543210@c.us"})
        messages.update(msg_id, {"status": "sent", "waha_id": "true_919876543210..."})
    """
    return RedisHashUtil(url=get_redis_url(), prefix=prefix, default_ttl=default_ttl)


# ──────────────────────────────────────────────
# WAHA client (sync — for Huey tasks)
# ──────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_waha() -> WhatsAppUtil:
    """
    Shared WAHA HTTP client built from WAHA_* settings.

    Sync (requests-based) by design — call it from Huey tasks or
    FastAPI sync code paths.
    """
    waha = get_settings().waha
    return WhatsAppUtil(
        base_url=waha.api_url,
        api_key=waha.api_key,
        session=waha.session,
        default_country_code=waha.default_country_code,
    )
