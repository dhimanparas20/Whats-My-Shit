"""
Quota-usage model — how many messages/OTPs a user has sent per window.

Stored by ``services/quota.py`` as a JSON STRING under
``QUOTA:<user_id>:DAILY`` and ``QUOTA:<user_id>:MONTHLY`` — one key per
(user, window). Pure data + validation (pydantic only).

Why a single model: DAILY and MONTHLY are the same shape — only the window
bounds differ. Two keys of one model beat one key with two sub-objects:
each key's TTL expires exactly at its own boundary, so window reset is
Redis's problem (lazy), not ours.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from models.enums import QuotaWindow, WhatsappMessageType


class QuotaUsage(BaseModel):
    """
    Per-user, per-window usage snapshot.

    Caps are denormalized INTO the record (``messages_limit`` /
    ``otp_limit``) so a quota check is one read per window — no join with
    the user record. ``window_end`` (epoch) is the reset authority; the
    Redis TTL is a backstop that fires slightly after it.

    Counters: ``messages_sent`` / ``otp_sent`` — counted SEPARATELY
    (WhatsApp text messages vs OTP verification codes). Tracking unit is
    the USER (api key), not the phone number.
    """

    user_id: str
    window: QuotaWindow
    # usage counters
    messages_sent: int = 0
    otp_sent: int = 0
    # caps for this window (per user — adjustable via service.set_limits)
    messages_limit: int
    otp_limit: int
    # bookkeeping timestamps (epoch, IST app clock) — SEPARATE per type:
    # OTP and message sends happen at different times.
    first_message_sent_at: float = 0.0
    last_message_sent_at: float = 0.0
    first_otp_sent_at: float = 0.0
    last_otp_sent_at: float = 0.0
    # window boundaries (epoch) — reset authority
    window_start: float = 0.0
    window_end: float = 0.0
    extra_info: Optional[dict] = None

    def remaining_messages(self) -> int:
        return max(self.messages_limit - self.messages_sent, 0)

    def remaining_otp(self) -> int:
        return max(self.otp_limit - self.otp_sent, 0)

    def allows(self, message_type: WhatsappMessageType) -> bool:
        """Whether one more send of ``message_type`` fits under the cap."""
        if message_type is WhatsappMessageType.TEXT:
            return self.remaining_messages() > 0
        if message_type is WhatsappMessageType.OTP:
            return self.remaining_otp() > 0
        return False

    def usage(self, message_type: WhatsappMessageType) -> dict:
        """Read-only view for API responses (no secrets, just numbers)."""
        sent, limit = (
            (self.messages_sent, self.messages_limit)
            if message_type is WhatsappMessageType.TEXT
            else (self.otp_sent, self.otp_limit)
        )
        return {
            "window": self.window.value,
            "used": sent,
            "limit": limit,
            "remaining": max(limit - sent, 0),
            "window_end": self.window_end,
        }
