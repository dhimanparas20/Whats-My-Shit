"""
services/ — the business-logic layer.

Services own domain logic that spans multiple Redis stores/prefixes or has
lifecycle rules (state machines, cooldowns, rate limits). This keeps tasks/
thin (send + track + raise) and lets the future api/ layer only orchestrate.

Layering (AGENTS.md §2) — dependencies point downward ONLY:

    api/  →  tasks/  →  services/  →  configs/ · modules/ · models/

services/ must NEVER import from tasks/ or api/ (import cycles).
"""

from configs import get_redis_str, get_settings
from models import OTPData, OTPStatus
from modules import get_logger, get_timestamp
from modules.redis_util import RedisHashUtil, RedisStringUtil


logger = get_logger(__name__, show_time=True)


class OTPService:
    """
    Full OTP lifecycle: generation, hashing, persistence, verification.

    Why a class: the service owns per-domain state — its Redis store (own
    prefix + TTL backstop), its settings view, the pepper (warn-once flag),
    and in future phases cooldown/rate-limit counters. Use the process-wide
    singleton ``otp_service``; don't construct ad-hoc.
    """

    # 12h backstop on the store; real per-record TTL is set on save().
    _STORE_DEFAULT_TTL = 12 * 60 * 60
    # Loud-marker fallback used ONLY when OTP_PEPPER is unset (dev).
    _DEV_PEPPER = "wms-DEV-pepper-NOT-safe-do-not-use-in-prod"

    def __init__(self) -> None:
        self.settings = get_settings().otp
        self.store: RedisStringUtil = get_redis_str(
            prefix=get_settings().redis_keys.otp,
            default_ttl=self._STORE_DEFAULT_TTL,
        )
        self._pepper_warned = False

    @staticmethod
    def record_key(
        user_id: str, phone: str, country_code: str = "91"
    ) -> str:
        """
        Bare (pre-prefix) Redis key for a user's active OTP record:
        ``<user_id>:<country_code><phone>`` → full key ``OTP:<user_id>:91<phone>``.

        User id comes FIRST in the key — two users can each hold an active
        code for the SAME phone number, tracked independently (key-scoped
        ownership; user_id is intentionally NOT a model field).
        """
        return f"{user_id}:{country_code}{phone}"

    # ── crypto ──────────────────────────────────────────────────────

    def pepper(self) -> str:
        """
        HMAC secret for hashing codes. Falls back loudly (warn ONCE per
        process, not per call) when OTP_PEPPER is unset — dev convenience,
        never acceptable for production.
        """
        if not self.settings.pepper:
            if not self._pepper_warned:
                logger.warning(
                    "OTP_PEPPER is unset — falling back to an insecure dev "
                    "pepper. Set OTP_PEPPER in .env before real OTP traffic."
                )
                self._pepper_warned = True
            return self._DEV_PEPPER
        return self.settings.pepper

    # NOTE: generate_random_number / generate_uuid7 / hash_sensitive_data_hmac
    # are STATIC methods on RedisHashUtil (RedisStringUtil does NOT have
    # them). They need no redis connection — always called class-style here.

    def generate(
        self,
        phone: str,
        length: int | None = None,
        ttl: int | None = None,
    ) -> tuple[str, OTPData]:
        """
        Create a fresh code for ``phone``: returns ``(plaintext, OTPData)``.

        The record carries the HMAC hash, uuid7 otp_id, phone, timestamps
        and TTL — but NOT the plaintext. The plaintext exists only in the
        return value: never log it, never store it. The caller decides
        when/where to save.
        """
        raw_otp = RedisHashUtil.generate_random_number(
            length or self.settings.length
        )
        record = OTPData(
            otp_id=RedisHashUtil.generate_uuid7(),
            hashed_otp=RedisHashUtil.hash_sensitive_data_hmac(
                data=raw_otp, secret=self.pepper()
            ),
            phone=phone,
            timestamp=get_timestamp(),
            ttl=ttl or self.settings.ttl,
        )
        return raw_otp, record

    # ── persistence ─────────────────────────────────────────────────

    def save(
        self, record: OTPData, user_id: str, country_code: str = "91"
    ) -> None:
        """
        Persist a record as the user's ACTIVE code for that phone.

        One active code per (user, phone): retries and re-requests overwrite
        the same key (overwrite=True) with an explicit per-key TTL — never
        the store's 12h backstop. ``record.phone`` must already be set.
        """
        self.store.set(
            self.record_key(user_id, record.phone, country_code),
            record.model_dump_json(),
            ttl=record.ttl,
            overwrite=True,
        )

    def get(self, user_id: str, phone: str, country_code: str = "91") -> OTPData | None:
        """Load the (user, phone) active record, or None when absent/expired."""
        rec = self.store.get(self.record_key(user_id, phone, country_code))
        return OTPData(**rec) if rec else None

    def delete(self, user_id: str, phone: str, country_code: str = "91") -> None:
        """Drop the (user, phone) active record (e.g. consumed/expired)."""
        self.store.delete(self.record_key(user_id, phone, country_code))

    # ── verification ────────────────────────────────────────────────

    @classmethod
    def verify_otp(
        cls, user_id: str, phone: str, code: str, country_code: str = "91"
    ) -> dict:
        """
        Check a code against the (user, phone) active OTP record (sync — the
        async API layer must not call this on the event loop; wrap it in
        run_in_executor/AnyIO.to_thread per AGENTS §10).

        Ownership is key-scoped: ``OTP:<user_id>:<cc><phone>`` — the same
        phone can have independent active codes for different users.

        Resolution order:
            1. No record           → not_found
            2. Terminal state      → verified / failed (already settled)
            3. Expired             → expired (record deleted)
            4. Attempts exhausted  → failed (record locked)
            5. HMAC equality       → success / wrong_code (+attempts)

        Every mutation rewrites the record with a re-claimed TTL so it can
        never outlive its expiry. Status dicts carry ids/counts, never the
        hash or the code.

        A classmethod (not staticmethod) so it can route through the
        process-wide singleton's store/settings and stay overridable;
        callable as ``OTPService.verify_otp(...)``,
        ``otp_service.verify_otp(...)`` or the flat ``verify_otp`` alias.
        """
        svc = otp_service
        key = svc.record_key(user_id, phone, country_code)
        record = svc.get(user_id, phone, country_code)
        if record is None:
            return {"status": "not_found"}

        base_ttl = max(record.remaining_ttl(), 1)  # never None → a verify in
        # the final second must NOT fall back to the 12h store default and
        # resurrect a dying record (old bug: `remaining_ttl() or None`).

        def rewrite(updated: OTPData) -> None:
            svc.store.set(key, updated.model_dump_json(),
                          ttl=base_ttl, overwrite=True)

        if record.status in (OTPStatus.VERIFIED, OTPStatus.FAILED):
            return {"status": record.status.value, "attempts": record.attempts}

        if record.is_expired():
            svc.delete(user_id, phone, country_code)
            return {"status": "expired"}

        if record.attempts >= svc.settings.max_verify_attempts:
            record.status = OTPStatus.FAILED
            rewrite(record)
            return {"status": "failed", "attempts": record.attempts}

        import hmac as hmac_lib  # local import: only needed on this path

        computed = RedisHashUtil.hash_sensitive_data_hmac(
            data=code, secret=svc.pepper()
        )
        if hmac_lib.compare_digest(computed, record.hashed_otp):
            record.status = OTPStatus.VERIFIED
            rewrite(record)
            logger.info(
                f"OTP verified: user={user_id}, phone={phone}, otp_id={record.otp_id}"
            )
            return {"status": "success", "otp_id": record.otp_id}

        record.attempts += 1
        if record.attempts >= svc.settings.max_verify_attempts:
            record.status = OTPStatus.FAILED
        rewrite(record)
        logger.info(
            f"OTP wrong code: user={user_id}, phone={phone}, attempts={record.attempts}"
        )
        if record.status is OTPStatus.FAILED:
            # this wrong code was ALSO the cap-breaking one — caller must see
            # the terminal state, not "wrong_code" (AGENTS §11 pitfall)
            return {"status": "failed", "attempts": record.attempts}
        return {"status": "wrong_code", "attempts": record.attempts}


# Process-wide singleton. Import-safe: get_redis_str PINGs redis in its
# constructor — the same eager-client pattern tasks/ already uses.
otp_service = OTPService()


# Flat alias for ergonomic import (`from services.otp import verify_otp`);
# the same callable as OTPService.verify_otp (classmethod).
verify_otp = OTPService.verify_otp
