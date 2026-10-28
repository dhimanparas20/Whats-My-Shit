"""
QuotaService — per-user WhatsApp usage caps (messages/OTPs, daily/monthly).

Storage layout (STRING JSON, one key per user+window — RedisStringUtil):
    QUOTA:<user_id>:DAILY    — QuotaUsage JSON, TTL = end of IST day + buffer
    QUOTA:<user_id>:MONTHLY  — QuotaUsage JSON, TTL = end of IST month + buffer

Reset strategy: TTL-driven lazy reset. Each record carries ``window_end``
(epoch — the authority); the Redis TTL is ``window_end - now + buffer``.
When a read lands past ``window_end`` (or the key has expired), the caller
gets a FRESH zeroed record for the new window. No cron, no field surgery.

Flow contract (Phase 1):
    API layer  → quota_service.check(user_id, type) BEFORE enqueue (429 on over)
    Huey task  → quota_service.increment(user_id, type) AFTER successful
                 WAHA send only — failed sends never consume quota

Tracking unit is the USER (api key), NOT the phone number.
Layering: configs/modules/models only — never tasks/api (AGENTS §2).
"""

from datetime import datetime, timedelta

from configs import get_redis_str, get_settings
from models import QuotaUsage, QuotaWindow, WhatsappMessageType
from modules import get_logger
from modules.generics import IST, now_ist


logger = get_logger(__name__, show_time=True)

# Default caps — adjust here until plans/limits become user-configurable
# (Phase 5). Per-user overrides via set_limits().
DEFAULT_DAILY_MESSAGES = 1000
DEFAULT_DAILY_OTP = 100
DEFAULT_MONTHLY_MESSAGES = 10_000
DEFAULT_MONTHLY_OTP = 1_000
# TTL fires this long AFTER window_end — the small race window between
# expiry and a read that lands past window_end is handled by comparing
# window_end, the TTL is just the janitor.
_TTL_BUFFER_S = 60


class QuotaService:
    """
    CRUD + enforcement for per-user quota usage.

    Use the process-wide singleton ``quota_service``. All methods sync
    (Huey-safe); the async API layer must offload calls to a worker thread
    (run_in_executor / AnyIO.to_thread per AGENTS §10).
    """

    def __init__(self) -> None:
        self.store = get_redis_str(
            prefix=get_settings().redis_keys.quota,
            # no default_ttl — every write carries its window-explicit TTL
        )

    # ── keying ──────────────────────────────────────────────────────

    @staticmethod
    @staticmethod
    def _key(user_id: str, window: QuotaWindow | str) -> str:
        """
        Bare (pre-prefix) key, window FIRST:
        ``DAILY:<user_id>`` / ``MONTHLY:<user_id>`` — so every quota of a
        kind sits under one scannable namespace (``SCAN QUOTA:DAILY:*``).
        Accepts a raw ``"daily"``/``"monthly"`` string too.
        """
        w = QuotaWindow(window) if isinstance(window, str) else window
        return f"{w.value.upper()}:{user_id}"

    # ── window math ─────────────────────────────────────────────────

    @staticmethod
    def _window_bounds(window: QuotaWindow) -> tuple[float, float]:
        """
        ``(window_start, window_end)`` epochs for the window containing
        NOW — boundaries at IST midnight (daily) / IST month start (monthly).
        """
        now = now_ist()
        if window is QuotaWindow.DAILY:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
        else:  # MONTHLY
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if start.month == 12:
                end = datetime(start.year + 1, 1, 1, tzinfo=IST)
            else:
                end = datetime(start.year, start.month + 1, 1, tzinfo=IST)
        return start.timestamp(), end.timestamp()

    def _ttl_for(self, window_end: float) -> int:
        """Seconds the key should live: until window end (+ buffer)."""
        import time
        return max(int(window_end - time.time()) + _TTL_BUFFER_S, _TTL_BUFFER_S)

    # ── CRUD ────────────────────────────────────────────────────────

    def get(self, user_id: str, window: QuotaWindow) -> QuotaUsage | None:
        """
        Load the record, or None when absent / lazily expired (past
        window_end). Does NOT create anything.
        """
        rec = self.store.get(self._key(user_id, window))
        if rec is None:
            return None
        record = QuotaUsage(**rec)
        if datetime.now(IST).timestamp() >= record.window_end:
            self.store.delete(self._key(user_id, window))
            return None  # window over — treat as fresh
        return record

    def get_or_create(
        self,
        user_id: str,
        window: QuotaWindow,
        messages_limit: int | None = None,
        otp_limit: int | None = None,
    ) -> QuotaUsage:
        """
        Current-window record, freshly initialized when absent/expired.
        Passing limit args overrides the caps (per-user plans).
        """
        limits = {
            QuotaWindow.DAILY: (DEFAULT_DAILY_MESSAGES, DEFAULT_DAILY_OTP),
            QuotaWindow.MONTHLY: (DEFAULT_MONTHLY_MESSAGES, DEFAULT_MONTHLY_OTP),
        }
        default_msgs, default_otps = limits[window]
        record = self.get(user_id, window)
        if record is not None:
            if messages_limit is not None:
                record.messages_limit = messages_limit
            if otp_limit is not None:
                record.otp_limit = otp_limit
            return record

        start, end = self._window_bounds(window)
        record = QuotaUsage(
            user_id=user_id,
            window=window,
            messages_limit=messages_limit or default_msgs,
            otp_limit=otp_limit or default_otps,
            window_start=start,
            window_end=end,
        )
        self._write(record)
        return record

    def _write(self, record: QuotaUsage) -> None:
        """Persist with a TTL anchored to the record's own window_end."""
        self.store.set(
            self._key(record.user_id, record.window),
            record.model_dump_json(),
            ttl=self._ttl_for(record.window_end),
            overwrite=True,
        )

    def delete(self, user_id: str, window: QuotaWindow) -> None:
        """Drop one window's record (manual reset)."""
        self.store.delete(self._key(user_id, window))

    def reset(self, user_id: str, window: QuotaWindow) -> None:
        """Force a counter reset for one window (admin/support action)."""
        self.delete(user_id, window)

    # ── enforcement ─────────────────────────────────────────────────

    def check(self, user_id: str, message_type: WhatsappMessageType) -> dict:
        """
        Pre-send gate (API layer calls this before enqueueing; does NOT
        mutate counters). Checks BOTH windows — a send is allowed only if
        it fits the daily AND the monthly cap.
        """
        daily = self.get_or_create(user_id, QuotaWindow.DAILY)
        monthly = self.get_or_create(user_id, QuotaWindow.MONTHLY)
        ok_d, ok_m = daily.allows(message_type), monthly.allows(message_type)
        return {
            "allowed": ok_d and ok_m,
            "reason": None if (ok_d and ok_m) else (
                "daily quota exhausted" if not ok_d else "monthly quota exhausted"
            ),
            "daily": daily.usage(message_type),
            "monthly": monthly.usage(message_type),
        }

    def increment(
        self,
        user_id: str,
        message_type: WhatsappMessageType,
        amount: int = 1,
    ) -> dict:
        """
        Count ``amount`` sends of ``message_type`` against BOTH windows.
        Call ONLY after a successful WAHA send (the task does this) —
        failed deliveries must not consume quota. No cap enforcement here:
        over-limit sends count but the API gate rejects new ones.
        """
        out = {}
        now_ts = datetime.now(IST).timestamp()
        for window in (QuotaWindow.DAILY, QuotaWindow.MONTHLY):
            rec = self.get_or_create(user_id, window)
            if message_type is WhatsappMessageType.TEXT:
                if not rec.first_message_sent_at:
                    rec.first_message_sent_at = now_ts
                rec.last_message_sent_at = now_ts
                rec.messages_sent += amount
            else:  # OTP
                if not rec.first_otp_sent_at:
                    rec.first_otp_sent_at = now_ts
                rec.last_otp_sent_at = now_ts
                rec.otp_sent += amount
            self._write(rec)
            out[window.value] = rec.usage(message_type)
        return out

    def set_limits(
        self,
        user_id: str,
        messages_limit: int | None = None,
        otp_limit: int | None = None,
        window: QuotaWindow | None = None,
    ) -> None:
        """
        Adjust caps for a user (per-user plans). ``window=None`` applies to
        BOTH windows — pass limits explicitly for the ones you want changed.
        """
        windows = (
            (QuotaWindow.DAILY, QuotaWindow.MONTHLY)
            if window is None else (window,)
        )
        for w in windows:
            rec = self.get_or_create(user_id, w)
            if messages_limit is not None:
                rec.messages_limit = messages_limit
            if otp_limit is not None:
                rec.otp_limit = otp_limit
            self._write(rec)


# Process-wide singleton — import-safe (same eager-client pattern as the
# other services).
quota_service = QuotaService()
