from __future__ import annotations

import time
from typing import Optional

from pydantic import BaseModel, model_validator

from models.enums import OTPStatus, WhatsappMessageType


class TaskStatus(BaseModel):
    """
    Delivery-tracking record for a Huey job.

    Stored in Redis under ``HUEY_TASK_TRACKER:<job_id>`` as JSON
    (written by the task itself on success/failure, survives Huey's
    result TTL and is readable without unpickling).
    """

    job_id: str
    success: bool = True
    result: dict | None = None
    timestamp: float = 0.0
    extra_info: Optional[dict] = None
    message_type: Optional[WhatsappMessageType] = None

    @classmethod
    def finished(
        cls,
        job_id: str,
        result: dict,
        timestamp: float,
        **extra_info,
    ) -> "TaskStatus":
        """Convenience factory for a successful job."""
        return cls(job_id=job_id, success=True, result=result,
                   timestamp=timestamp, extra_info=extra_info or None)

    @classmethod
    def failed(
        cls,
        job_id: str,
        error: str,
        timestamp: float,
        **extra_info,
    ) -> "TaskStatus":
        """Convenience factory for a failed job."""
        return cls(job_id=job_id, success=False, result={"error": error},
                   timestamp=timestamp, extra_info=extra_info or None)


class OTPData(BaseModel):
    """
    Per-(user, phone) OTP record, stored under
    ``OTP:<user_id>:<country_code><phone>`` as JSON — ownership is
    key-scoped (user_id is NOT a model field), so different users can hold
    independent active codes for the same phone.

    Only stored after the WhatsApp send SUCCEEDS — a code the user never
    received must never be verifiable. The code itself is never stored in
    plaintext; it is an HMAC-SHA256 hash keyed by ``OTPSettings.pepper``.

    One active record per (user, phone): retries and re-requests overwrite
    the same key (latest code wins), so there are no orphan records.
    """

    otp_id: str                       # uuid7, unique per generated code
    hashed_otp: str                   # HMAC-SHA256(code, OTP_PEPPER) — not reversible
    phone: str
    timestamp: float = 0.0            # when the code was sent
    status: OTPStatus = OTPStatus.PENDING
    attempts: int = 0                 # wrong-code verification attempts
    retry_count: int = 0              # send retries consumed by huey (mirrored)
    ttl: int = 30 * 60                # seconds the code stays valid
    expiry: float = 0.0               # timestamp + ttl; computed if omitted
    extra_info: Optional[dict] = None

    @model_validator(mode="after")
    def _compute_expiry(self) -> "OTPData":
        """expiry = timestamp + ttl unless explicitly provided."""
        if not self.expiry:
            object.__setattr__(self, "expiry", self.timestamp + self.ttl)
        return self

    def remaining_ttl(self) -> int:
        """Whole seconds this record should keep living right now (>= 0)."""
        return max(int(self.expiry - time.time()), 0)

    def is_expired(self) -> bool:
        """Instance check — NOT a classmethod; expiry is per record."""
        return time.time() >= self.expiry
