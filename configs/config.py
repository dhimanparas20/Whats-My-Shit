"""
Application settings — single source of truth for all configuration.

Resolution order (highest priority wins):
    1. Real environment variables (Docker -e, shell export)
    2. .env file (absolute path — independent of process CWD)
    3. Field defaults

.env keys are matched case-insensitively against field names,
including the env_prefix, e.g.:
    WAHA_API_URL / WAHA_API_KEY / WAHA_SESSION / WAHA_DEFAULT_COUNTRY_CODE
    API_DEBUG / API_PORT / API_HOST
"""

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

# Repo root = parent of configs/ — makes .env lookup CWD-independent
# (works from anywhere: uv scripts, Docker WORKDIR, huey workers, tests)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"


class RedisSettings(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")


class WASettings(BaseSettings):
    # Field name is api_url (NOT base_url) on purpose:
    #   WAHA_API_URL  = how THIS app reaches the WAHA container
    #                   (http://waha:3000 inside Docker, http://localhost:3000 locally)
    #   WAHA_BASE_URL = consumed by the WAHA container itself (public URL for
    #                   webhooks / file URLs) — do NOT read it here.
    # This mirrors modules/wa_util.py WhatsAppClientConfig.from_env()
    # which also prefers WAHA_API_URL.
    api_url: str = "http://localhost:3000"
    api_key: Optional[str] = None
    session: str = "default"
    default_country_code: str = "91"

    # env_prefix means .env keys are WAHA_API_URL, WAHA_API_KEY, etc.
    model_config = SettingsConfigDict(
        env_prefix="waha_", env_file=ENV_FILE, extra="ignore"
    )


class FastAPISettings(BaseSettings):
    debug: bool = False
    port: int = 8000
    host: str = "0.0.0.0"

    # .env keys: API_DEBUG, API_PORT, API_HOST
    model_config = SettingsConfigDict(
        env_prefix="api_", env_file=ENV_FILE, extra="ignore"
    )


class TaskSettings(BaseSettings):
    retry_count: int = 2
    delay: int = 60

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")

class OTPSettings(BaseSettings):
    """OTP generation/verification knobs — env keys OTP_* (env_prefix='otp_')."""

    # Secret used as the HMAC key when hashing OTP codes. REQUIRED for
    # production; empty default triggers a loud warning + insecure fallback.
    pepper: str = ""
    ttl: int = 30 * 60              # seconds an OTP stays valid
    length: int = 6                 # digits (generate_random_number allows 4–12)
    max_verify_attempts: int = 3    # wrong-code tries before the record is locked

    model_config = SettingsConfigDict(env_prefix="otp_", env_file=ENV_FILE, extra="ignore")

class SystemQuotaSettings(BaseSettings):
    """
    SYSTEM-WIDE daily caps — the super cap across ALL users.

    Independent of per-user quotas: when the system total for a window is
    exhausted, NO user can send (even under their own cap) until the window
    resets. Env keys: SYSTEM_DAILY_MESSAGES / SYSTEM_DAILY_OTP.
    """

    daily_messages: int = 1000
    daily_otp: int = 200

    model_config = SettingsConfigDict(env_prefix="system_", env_file=ENV_FILE, extra="ignore")


class RedisKeys(BaseSettings):
    huey_task_tracker: str = "HUEY_TASK_TRACKER"
    otp: str = "OTP"
    # Named `users` (NOT `user`): a prefix-less `user` field would match the
    # standard POSIX $USER env var (login name) → keys differed per machine.
    # Env key is USERS; the VALUE is the Redis key prefix (USER:<user_id>).
    users: str = "USER"
    # POSIX-safe: no standard env var named QUOTA exists.
    quota: str = "QUOTA"

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")


class Settings(BaseSettings):
    """Aggregated settings — access as settings.redis.redis_url, etc."""

    redis: RedisSettings = RedisSettings()
    waha: WASettings = WASettings()
    api: FastAPISettings = FastAPISettings()
    task: TaskSettings = TaskSettings()
    otp: OTPSettings = OTPSettings()
    system: SystemQuotaSettings = SystemQuotaSettings()
    redis_keys: RedisKeys = RedisKeys()  # users → "USER" (POSIX $USER collision renamed)
    # ^ RedisKeys.users → "USER" (renamed from `user`: POSIX $USER collision)
    # ^ RedisKeys.quota → "QUOTA" (quota tracking prefix — see services/quota.py)

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")




@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Cached settings accessor — call this everywhere instead of
    constructing Settings() repeatedly (each construction re-reads
    .env and re-validates).

    In FastAPI, build a dependency:

        def settings_dep() -> Settings:
            return get_settings()
    """
    return Settings()
